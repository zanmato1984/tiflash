// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <benchmark/benchmark.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Debug/TiFlashTestEnv.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>

#include <Flash/TiForth/ArrowTypeMapping.h>

#include <arrow/builder.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/logging.h>

#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>

#include <tiforth/engine.h>
#include <tiforth/expr.h>
#include <tiforth/operators/arrow_compute_agg.h>
#include <tiforth/pipeline.h>
#include <tiforth/task.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/core.h>

namespace DB::tests {

namespace {

enum class KeyType {
    kInt32,
    kInt64,
    kString,
};

enum class ValueType {
    kInt64,
    kFloat64,
};

enum class KeyDist {
    kSingleGroup,
    kUniformLowCard,
    kUniformHighCard,
    kZipfSkew,
};

struct BenchConfig {
    KeyType key_type;
    ValueType value_type;
    KeyDist dist;
    size_t num_rows;
    size_t rows_per_block;
    size_t groups; // for low-card / zipf; ignored for high-card
};

std::string ToString(KeyType t) {
    switch (t) {
    case KeyType::kInt32:
        return "kInt32";
    case KeyType::kInt64:
        return "kInt64";
    case KeyType::kString:
        return "kString";
    }
    return "kUnknown";
}

std::string ToString(ValueType t) {
    switch (t) {
    case ValueType::kInt64:
        return "vInt64";
    case ValueType::kFloat64:
        return "vFloat64";
    }
    return "vUnknown";
}

std::string ToString(KeyDist d) {
    switch (d) {
    case KeyDist::kSingleGroup:
        return "single";
    case KeyDist::kUniformLowCard:
        return "uniform_low";
    case KeyDist::kUniformHighCard:
        return "uniform_high";
    case KeyDist::kZipfSkew:
        return "zipf";
    }
    return "dist_unknown";
}

std::string MakeCaseId(const BenchConfig & cfg) {
    return fmt::format(
        "{}_{}_{}_rows{}_blk{}_groups{}",
        ToString(cfg.key_type),
        ToString(cfg.value_type),
        ToString(cfg.dist),
        cfg.num_rows,
        cfg.rows_per_block,
        cfg.groups);
}

class ZipfGenerator {
public:
    ZipfGenerator(size_t n, double s) : cdf_(n) {
        double denom = 0.0;
        for (size_t i = 0; i < n; ++i)
            denom += 1.0 / std::pow(static_cast<double>(i + 1), s);
        double accum = 0.0;
        for (size_t i = 0; i < n; ++i) {
            accum += (1.0 / std::pow(static_cast<double>(i + 1), s)) / denom;
            cdf_[i] = accum;
        }
        cdf_.back() = 1.0;
    }

    size_t Sample(std::mt19937_64 & rng) const {
        const double u = std::generate_canonical<double, 64>(rng);
        return static_cast<size_t>(std::lower_bound(cdf_.begin(), cdf_.end(), u) - cdf_.begin());
    }

private:
    std::vector<double> cdf_;
};

struct Dataset {
    std::vector<Block> blocks;
    std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_batches;
    std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_batches_dict_key;
    Block header;
};

std::shared_ptr<Dataset> GetOrCreateDataset(const BenchConfig & cfg) {
    static std::mutex mu;
    static std::unordered_map<std::string, std::shared_ptr<Dataset>> cache;

    const auto id = MakeCaseId(cfg);
    std::lock_guard lock(mu);
    if (auto it = cache.find(id); it != cache.end())
        return it->second;

    auto out = std::make_shared<Dataset>();

    std::mt19937_64 rng(0);
    std::unique_ptr<ZipfGenerator> zipf;
    if (cfg.dist == KeyDist::kZipfSkew) {
        zipf = std::make_unique<ZipfGenerator>(cfg.groups, /*s=*/1.1);
    }

    DataTypePtr key_type;
    if (cfg.key_type == KeyType::kInt32)
        key_type = std::make_shared<DataTypeInt32>();
    else if (cfg.key_type == KeyType::kInt64)
        key_type = std::make_shared<DataTypeInt64>();
    else
        key_type = std::make_shared<DataTypeString>();

    DataTypePtr value_type;
    if (cfg.value_type == ValueType::kInt64)
        value_type = std::make_shared<DataTypeInt64>();
    else
        value_type = std::make_shared<DataTypeFloat64>();

    const size_t blocks = (cfg.num_rows + cfg.rows_per_block - 1) / cfg.rows_per_block;
    out->blocks.reserve(blocks);

    std::vector<std::string> string_key_dictionary;
    if (cfg.key_type == KeyType::kString) {
        const size_t dict_size =
            cfg.dist == KeyDist::kUniformHighCard ? std::min(cfg.num_rows, size_t{1} << 16) : std::max(cfg.groups, size_t{1});
        string_key_dictionary.reserve(dict_size);
        for (size_t i = 0; i < dict_size; ++i) {
            string_key_dictionary.push_back("k" + std::to_string(i));
        }
    }

    auto sample_key = [&](size_t row) -> std::pair<Int64, std::string_view> {
        size_t key_id = 0;
        switch (cfg.dist) {
        case KeyDist::kSingleGroup:
            key_id = 0;
            break;
        case KeyDist::kUniformLowCard:
            key_id = static_cast<size_t>(rng() % std::max(cfg.groups, size_t{1}));
            break;
        case KeyDist::kUniformHighCard:
            key_id = row;
            break;
        case KeyDist::kZipfSkew:
            key_id = zipf->Sample(rng);
            break;
        }
        if (cfg.key_type == KeyType::kString) {
            const auto idx = key_id % string_key_dictionary.size();
            return {static_cast<Int64>(key_id), string_key_dictionary[idx]};
        }
        return {static_cast<Int64>(key_id), {}};
    };

    size_t global_row = 0;
    for (size_t b = 0; b < blocks; ++b) {
        const size_t rows = std::min(cfg.rows_per_block, cfg.num_rows - global_row);

        MutableColumnPtr key_col;
        if (cfg.key_type == KeyType::kInt32)
            key_col = ColumnInt32::create();
        else if (cfg.key_type == KeyType::kInt64)
            key_col = ColumnInt64::create();
        else
            key_col = ColumnString::create();

        MutableColumnPtr value_col;
        if (cfg.value_type == ValueType::kInt64)
            value_col = ColumnInt64::create();
        else
            value_col = ColumnFloat64::create();

        key_col->reserve(rows);
        value_col->reserve(rows);

        for (size_t i = 0; i < rows; ++i) {
            const size_t row = global_row + i;
            const auto [key_id, key_str] = sample_key(row);

            if (cfg.key_type == KeyType::kInt32)
                key_col->insert(Field(static_cast<Int64>(static_cast<Int32>(key_id))));
            else if (cfg.key_type == KeyType::kInt64)
                key_col->insert(Field(static_cast<Int64>(key_id)));
            else
                key_col->insert(Field(key_str.data(), key_str.size()));

            if (cfg.value_type == ValueType::kInt64)
                value_col->insert(Field(static_cast<Int64>(row)));
            else
                value_col->insert(Field(static_cast<Float64>(row) * 0.1));
        }

        Block block;
        block.insert({std::move(key_col), key_type, "k"});
        block.insert({std::move(value_col), value_type, "v"});
        out->blocks.push_back(std::move(block));
        global_row += rows;
    }

    out->header = out->blocks.at(0).cloneEmpty();

    std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
    out->arrow_batches.reserve(out->blocks.size());
    for (const auto & block : out->blocks) {
        auto maybe_batch
            = TiForth::toArrowRecordBatch(block, options_by_name, arrow::default_memory_pool());
        ARROW_CHECK_OK(maybe_batch.status());
        out->arrow_batches.push_back(std::move(maybe_batch).ValueUnsafe());
    }

    if (cfg.key_type == KeyType::kString) {
        std::unordered_map<std::string_view, Int32> dict_index;
        dict_index.reserve(string_key_dictionary.size());
        for (size_t i = 0; i < string_key_dictionary.size(); ++i) {
            dict_index.emplace(string_key_dictionary[i], static_cast<Int32>(i));
        }

        arrow::BinaryBuilder dict_builder(arrow::default_memory_pool());
        for (const auto & s : string_key_dictionary) {
            ARROW_CHECK_OK(dict_builder.Append(s));
        }
        std::shared_ptr<arrow::Array> dict_values;
        ARROW_CHECK_OK(dict_builder.Finish(&dict_values));
        ARROW_CHECK(dict_values != nullptr);

        const auto dict_type = arrow::dictionary(arrow::int32(), dict_values->type());

        out->arrow_batches_dict_key.reserve(out->arrow_batches.size());
        for (const auto & batch : out->arrow_batches) {
            ARROW_CHECK(batch != nullptr);
            ARROW_CHECK(batch->num_columns() == 2);
            auto schema = batch->schema();
            ARROW_CHECK(schema != nullptr);
            ARROW_CHECK(schema->num_fields() == 2);

            const auto & key_array = batch->column(0);
            ARROW_CHECK(key_array != nullptr);
            const auto & key_binary = dynamic_cast<const arrow::BinaryArray &>(*key_array);
            arrow::Int32Builder index_builder(arrow::default_memory_pool());
            ARROW_CHECK_OK(index_builder.Reserve(batch->num_rows()));
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                const auto view = key_binary.GetView(i);
                auto it = dict_index.find(view);
                ARROW_CHECK(it != dict_index.end());
                ARROW_CHECK_OK(index_builder.Append(it->second));
            }
            std::shared_ptr<arrow::Array> indices;
            ARROW_CHECK_OK(index_builder.Finish(&indices));
            ARROW_CHECK(indices != nullptr);

            auto maybe_dict_array = arrow::DictionaryArray::FromArrays(dict_type, indices, dict_values);
            ARROW_CHECK_OK(maybe_dict_array.status());
            auto dict_array = std::move(maybe_dict_array).ValueUnsafe();
            ARROW_CHECK(dict_array != nullptr);

            auto fields = schema->fields();
            fields[0] = fields[0]->WithType(dict_array->type());
            auto dict_schema = arrow::schema(std::move(fields), schema->metadata());
            out->arrow_batches_dict_key.push_back(arrow::RecordBatch::Make(
                std::move(dict_schema), batch->num_rows(), {std::move(dict_array), batch->column(1)}));
        }
    }

    cache.emplace(id, out);
    return out;
}

std::unique_ptr<Aggregator::Params> BuildAggregatorParams(const ContextPtr & context, const Dataset & dataset) {
    ARROW_CHECK(context != nullptr);
    if (!AggregateFunctionFactory::instance().isAggregateFunctionName("sum"))
        ::DB::registerAggregateFunctions();

    const auto value_type = dataset.header.getByName("v").type;

    AggregateDescriptions aggregates;
    aggregates.push_back(AggregateDescription{
        .function = AggregateFunctionFactory::instance().get(*context, "count", {value_type}),
        .parameters = {},
        .arguments = {1},
        .argument_names = {"v"},
        .column_name = "cnt_v",
    });
    aggregates.push_back(AggregateDescription{
        .function = AggregateFunctionFactory::instance().get(*context, "sum", {value_type}),
        .parameters = {},
        .arguments = {1},
        .argument_names = {"v"},
        .column_name = "sum_v",
    });

    ColumnNumbers keys{0};
    KeyRefAggFuncMap key_ref_agg_func;
    AggFuncRefKeyMap agg_func_ref_key;

    SpillConfig spill_config(
        context->getTemporaryPath(),
        "bench_arrow_compute_agg",
        context->getSettingsRef().max_cached_data_bytes_in_spiller,
        context->getSettingsRef().max_spilled_rows_per_file,
        context->getSettingsRef().max_spilled_bytes_per_file,
        context->getFileProvider(),
        context->getSettingsRef().max_threads,
        context->getSettingsRef().max_block_size);

    auto params = std::make_unique<Aggregator::Params>(
        dataset.header,
        keys,
        key_ref_agg_func,
        agg_func_ref_key,
        aggregates,
        /*group_by_two_level_threshold_=*/0,
        /*group_by_two_level_threshold_bytes_=*/0,
        /*max_bytes_before_external_group_by_=*/0, // disable spill/external aggregation
        /*empty_result_for_aggregation_by_empty_set_=*/false,
        spill_config,
        /*max_block_size_=*/context->getSettingsRef().max_block_size,
        /*use_magic_hash_=*/false,
        TiDB::dummy_collators);

    return params;
}

void RunNativeAggregator(const BenchConfig & cfg, benchmark::State & state) {
    const auto dataset = GetOrCreateDataset(cfg);
    const auto context = TiFlashTestEnv::getContext();
    auto params = BuildAggregatorParams(context, *dataset);

    for (const auto & _ : state) {
        (void)_;
        RegisterOperatorSpillContext register_operator_spill_context;
        auto aggregator = std::make_shared<Aggregator>(
            *params,
            "BenchNativeAggregator",
            /*concurrency=*/1,
            register_operator_spill_context,
            /*is_auto_pass_through=*/false,
            params->use_magic_hash);
        auto data_variants = std::make_shared<AggregatedDataVariants>();
        data_variants->aggregator = aggregator.get();

        Aggregator::AggProcessInfo agg_process_info(aggregator.get());
        for (const auto & block : dataset->blocks) {
            agg_process_info.resetBlock(block);
            aggregator->executeOnBlock(agg_process_info, *data_variants, /*thread_num=*/1);
        }

        std::vector<AggregatedDataVariantsPtr> variants{data_variants};
        auto merging_buckets = aggregator->mergeAndConvertToBlocks(variants, /*final=*/true, /*max_threads=*/1);
        size_t rows = 0;
        for (;;) {
            auto out_block = merging_buckets->getData(0);
            if (!out_block)
                break;
            rows += out_block.rows();
            benchmark::DoNotOptimize(rows);
        }
    }

    state.SetItemsProcessed(static_cast<int64_t>(cfg.num_rows) * state.iterations());
}

void RunTiForthArrowComputeAgg(const BenchConfig & cfg, benchmark::State & state) {
    const auto dataset = GetOrCreateDataset(cfg);

    auto maybe_engine = tiforth::Engine::Create(tiforth::EngineOptions{});
    ARROW_CHECK_OK(maybe_engine.status());
    auto engine = std::move(maybe_engine).ValueUnsafe();

    std::unique_ptr<tiforth::Pipeline> pipeline;
    {
        auto maybe_builder = tiforth::PipelineBuilder::Create(engine.get());
        ARROW_CHECK_OK(maybe_builder.status());
        auto builder = std::move(maybe_builder).ValueUnsafe();
        std::vector<tiforth::AggKey> keys = {{"k", tiforth::MakeFieldRef("k")}};
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"cnt_v", "count", tiforth::MakeFieldRef("v")});
        aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef("v")});

        auto status = builder->AppendTransform(
            [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<tiforth::TransformOpPtr> {
                return std::make_unique<tiforth::ArrowComputeAggTransformOp>(engine_ptr, keys, aggs);
            });
        ARROW_CHECK_OK(status);

        auto maybe_pipeline = builder->Finalize();
        ARROW_CHECK_OK(maybe_pipeline.status());
        pipeline = std::move(maybe_pipeline).ValueUnsafe();
    }

    for (const auto & _ : state) {
        (void)_;
        auto maybe_task = pipeline->CreateTask();
        ARROW_CHECK_OK(maybe_task.status());
        auto task = std::move(maybe_task).ValueUnsafe();
        auto maybe_task_state = task->Step();
        ARROW_CHECK_OK(maybe_task_state.status());
        auto task_state = maybe_task_state.ValueUnsafe();
        ARROW_CHECK(task_state == tiforth::TaskState::kNeedInput);

        for (const auto & batch : dataset->arrow_batches) {
            ARROW_CHECK_OK(task->PushInput(batch));
        }
        ARROW_CHECK_OK(task->CloseInput());

        while (true) {
            maybe_task_state = task->Step();
            ARROW_CHECK_OK(maybe_task_state.status());
            task_state = maybe_task_state.ValueUnsafe();
            if (task_state == tiforth::TaskState::kFinished) {
                break;
            }
            if (task_state == tiforth::TaskState::kNeedInput) {
                continue;
            }
            ARROW_CHECK(task_state == tiforth::TaskState::kHasOutput);
            auto maybe_out = task->PullOutput();
            ARROW_CHECK_OK(maybe_out.status());
            auto out = std::move(maybe_out).ValueUnsafe();
            ARROW_CHECK(out != nullptr);
            benchmark::DoNotOptimize(out->num_rows());
        }
    }

    state.SetItemsProcessed(static_cast<int64_t>(cfg.num_rows) * state.iterations());
}

void RunTiForthArrowComputeAggDictKey(const BenchConfig & cfg, benchmark::State & state) {
    ARROW_CHECK(cfg.key_type == KeyType::kString);
    const auto dataset = GetOrCreateDataset(cfg);
    ARROW_CHECK(!dataset->arrow_batches_dict_key.empty());

    auto maybe_engine = tiforth::Engine::Create(tiforth::EngineOptions{});
    ARROW_CHECK_OK(maybe_engine.status());
    auto engine = std::move(maybe_engine).ValueUnsafe();

    std::unique_ptr<tiforth::Pipeline> pipeline;
    {
        auto maybe_builder = tiforth::PipelineBuilder::Create(engine.get());
        ARROW_CHECK_OK(maybe_builder.status());
        auto builder = std::move(maybe_builder).ValueUnsafe();
        std::vector<tiforth::AggKey> keys = {{"k", tiforth::MakeFieldRef("k")}};
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"cnt_v", "count", tiforth::MakeFieldRef("v")});
        aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef("v")});

        auto status = builder->AppendTransform(
            [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<tiforth::TransformOpPtr> {
                return std::make_unique<tiforth::ArrowComputeAggTransformOp>(engine_ptr, keys, aggs);
            });
        ARROW_CHECK_OK(status);

        auto maybe_pipeline = builder->Finalize();
        ARROW_CHECK_OK(maybe_pipeline.status());
        pipeline = std::move(maybe_pipeline).ValueUnsafe();
    }

    for (const auto & _ : state) {
        (void)_;
        auto maybe_task = pipeline->CreateTask();
        ARROW_CHECK_OK(maybe_task.status());
        auto task = std::move(maybe_task).ValueUnsafe();
        auto maybe_task_state = task->Step();
        ARROW_CHECK_OK(maybe_task_state.status());
        auto task_state = maybe_task_state.ValueUnsafe();
        ARROW_CHECK(task_state == tiforth::TaskState::kNeedInput);

        for (const auto & batch : dataset->arrow_batches_dict_key) {
            ARROW_CHECK_OK(task->PushInput(batch));
        }
        ARROW_CHECK_OK(task->CloseInput());

        while (true) {
            maybe_task_state = task->Step();
            ARROW_CHECK_OK(maybe_task_state.status());
            task_state = maybe_task_state.ValueUnsafe();
            if (task_state == tiforth::TaskState::kFinished) {
                break;
            }
            if (task_state == tiforth::TaskState::kNeedInput) {
                continue;
            }
            ARROW_CHECK(task_state == tiforth::TaskState::kHasOutput);
            auto maybe_out = task->PullOutput();
            ARROW_CHECK_OK(maybe_out.status());
            auto out = std::move(maybe_out).ValueUnsafe();
            ARROW_CHECK(out != nullptr);
            benchmark::DoNotOptimize(out->num_rows());
        }
    }

    state.SetItemsProcessed(static_cast<int64_t>(cfg.num_rows) * state.iterations());
}

void RegisterCases() {
    const size_t num_rows_numeric = 1 << 20;
    const size_t rows_per_block = 1 << 16;

    const std::vector<BenchConfig> configs = {
        // int32 keys, int64 values
        {KeyType::kInt32, ValueType::kInt64, KeyDist::kSingleGroup, num_rows_numeric, rows_per_block, 1},
        {KeyType::kInt32, ValueType::kInt64, KeyDist::kUniformLowCard, num_rows_numeric, rows_per_block, 16},
        {KeyType::kInt32, ValueType::kInt64, KeyDist::kUniformHighCard, num_rows_numeric, rows_per_block, 0},
        {KeyType::kInt32, ValueType::kInt64, KeyDist::kZipfSkew, num_rows_numeric, rows_per_block, 1024},

        // int64 keys, float64 values
        {KeyType::kInt64, ValueType::kFloat64, KeyDist::kUniformLowCard, num_rows_numeric, rows_per_block, 16},
        {KeyType::kInt64, ValueType::kFloat64, KeyDist::kZipfSkew, num_rows_numeric, rows_per_block, 1024},

        // string keys, int64 values (smaller to cap memory)
        {KeyType::kString, ValueType::kInt64, KeyDist::kUniformLowCard, 1 << 18, rows_per_block, 16},
        {KeyType::kString, ValueType::kInt64, KeyDist::kZipfSkew, 1 << 18, rows_per_block, 1024},
    };

    for (const auto & cfg : configs) {
        const auto case_name = MakeCaseId(cfg);

        benchmark::RegisterBenchmark(
            fmt::format("ArrowComputeAgg/Native/{}", case_name).c_str(),
            [cfg](benchmark::State & state) { RunNativeAggregator(cfg, state); });
        benchmark::RegisterBenchmark(
            fmt::format("ArrowComputeAgg/TiForth/{}", case_name).c_str(),
            [cfg](benchmark::State & state) { RunTiForthArrowComputeAgg(cfg, state); });

        if (cfg.key_type == KeyType::kString) {
            benchmark::RegisterBenchmark(
                fmt::format("ArrowComputeAgg/TiForthDictKey/{}", case_name).c_str(),
                [cfg](benchmark::State & state) { RunTiForthArrowComputeAggDictKey(cfg, state); });
        }
    }
}

const auto kRegistered = [] {
    RegisterCases();
    return true;
}();

} // namespace

} // namespace DB::tests

#else

static void BenchTiForthArrowComputeAggDisabled(benchmark::State & state) {
    for (const auto & _ : state)
        benchmark::DoNotOptimize(_);
}
BENCHMARK(BenchTiForthArrowComputeAggDisabled)->Name("ArrowComputeAgg/Disabled");

#endif // defined(TIFLASH_ENABLE_TIFORTH)
