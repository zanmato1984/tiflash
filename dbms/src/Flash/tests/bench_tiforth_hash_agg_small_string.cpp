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
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Debug/TiFlashTestEnv.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>

#include <Flash/TiForth/ArrowTypeMapping.h>

#include <arrow/memory_pool.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/logging.h>

#include <tiforth/engine.h>
#include <tiforth/expr.h>
#include <tiforth/operators/hash_agg.h>
#include <tiforth/pipeline/op/op.h>
#include <tiforth/plan.h>
#include <tiforth/task.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/core.h>

namespace DB::tests {

namespace {

enum class KeyDist {
    kSingleGroup,
    kUniformLowCard,
    kUniformHighCard,
    kZipfSkew,
};

struct BenchConfig {
    int32_t key_length;
    KeyDist dist;
    size_t num_rows;
    size_t rows_per_block;
    size_t groups;
    bool null_keys;
    bool null_values;
};

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
        "len{}_{}_rows{}_blk{}_groups{}_nullk{}_nullv{}",
        cfg.key_length,
        ToString(cfg.dist),
        cfg.num_rows,
        cfg.rows_per_block,
        cfg.groups,
        cfg.null_keys ? 1 : 0,
        cfg.null_values ? 1 : 0);
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
    Block header;
};

void AppendNullable(MutableColumnPtr & col, bool is_null, const Field & value) {
    auto * nullable = typeid_cast<ColumnNullable *>(col.get());
    if (nullable == nullptr) {
        if (is_null)
            col->insertDefault();
        else
            col->insert(value);
        return;
    }

    if (is_null) {
        nullable->insertDefault();
        return;
    }
    nullable->insert(value);
}

std::string MakeFixedLenKey(size_t key_id, int32_t length) {
    if (length <= 0)
        return {};
    std::string out(static_cast<size_t>(length), 'a');
    uint64_t x = static_cast<uint64_t>(key_id) * 0x9E3779B97F4A7C15ULL + 0xD1B54A32D192ED03ULL;
    for (int32_t i = 0; i < length; ++i) {
        out[static_cast<size_t>(i)] = static_cast<char>('a' + ((x + static_cast<uint64_t>(i) * 17) % 26));
        x = x * 0xBF58476D1CE4E5B9ULL + 0x94D049BB133111EBULL;
    }
    return out;
}

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
    if (cfg.dist == KeyDist::kZipfSkew)
        zipf = std::make_unique<ZipfGenerator>(std::max(cfg.groups, size_t{1}), /*s=*/1.1);

    DataTypePtr key_type = std::make_shared<DataTypeString>();
    if (cfg.null_keys)
        key_type = makeNullable(key_type);

    DataTypePtr value_type = std::make_shared<DataTypeInt64>();
    if (cfg.null_values)
        value_type = makeNullable(value_type);

    std::vector<std::string> key_dictionary;
    if (cfg.dist != KeyDist::kUniformHighCard) {
        const size_t dict_size = std::max(cfg.groups, size_t{1});
        key_dictionary.reserve(dict_size);
        for (size_t i = 0; i < dict_size; ++i) {
            key_dictionary.push_back(MakeFixedLenKey(i, cfg.key_length));
        }
    }

    const size_t blocks = (cfg.num_rows + cfg.rows_per_block - 1) / cfg.rows_per_block;
    out->blocks.reserve(blocks);

    size_t global_row = 0;
    for (size_t b = 0; b < blocks; ++b) {
        const size_t rows = std::min(cfg.rows_per_block, cfg.num_rows - global_row);

        MutableColumnPtr key_col;
        if (cfg.null_keys)
            key_col = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
        else
            key_col = ColumnString::create();

        MutableColumnPtr value_col;
        if (cfg.null_values)
            value_col = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
        else
            value_col = ColumnInt64::create();

        key_col->reserve(rows);
        value_col->reserve(rows);

        for (size_t i = 0; i < rows; ++i) {
            const size_t row = global_row + i;
            size_t key_id = 0;
            switch (cfg.dist) {
            case KeyDist::kSingleGroup:
                key_id = 0;
                break;
            case KeyDist::kUniformLowCard:
                key_id = row % std::max(cfg.groups, size_t{1});
                break;
            case KeyDist::kUniformHighCard:
                key_id = row;
                break;
            case KeyDist::kZipfSkew:
                key_id = zipf->Sample(rng);
                break;
            }

            std::string key_tmp;
            std::string_view key_str;
            if (cfg.dist == KeyDist::kUniformHighCard) {
                key_tmp = MakeFixedLenKey(key_id, cfg.key_length);
                key_str = key_tmp;
            } else {
                key_str = key_dictionary[key_id % key_dictionary.size()];
            }

            const bool key_is_null = cfg.null_keys && (row % 37 == 0);
            AppendNullable(key_col, key_is_null, Field(key_str.data(), key_str.size()));

            const bool value_is_null = cfg.null_values && (row % 17 == 0);
            AppendNullable(value_col, value_is_null, Field(static_cast<Int64>(row)));
        }

        Block block;
        block.insert({std::move(key_col), key_type, "k"});
        block.insert({std::move(value_col), value_type, "v"});
        out->blocks.push_back(std::move(block));
        global_row += rows;
    }

    out->header = out->blocks.empty() ? Block{} : out->blocks.at(0).cloneEmpty();

    std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
    out->arrow_batches.reserve(out->blocks.size());
    for (const auto & block : out->blocks) {
        auto maybe_batch
            = TiForth::toArrowRecordBatch(block, options_by_name, arrow::default_memory_pool());
        ARROW_CHECK_OK(maybe_batch.status());
        out->arrow_batches.push_back(std::move(maybe_batch).ValueUnsafe());
    }

    cache.emplace(id, out);
    return out;
}

std::unique_ptr<Aggregator::Params> BuildAggregatorParams(
    const ContextPtr & context,
    const Block & header) {
    if (!AggregateFunctionFactory::instance().isAggregateFunctionName("sum"))
        ::DB::registerAggregateFunctions();

    const auto value_type = header.getByName("v").type;
    const auto arg_col = header.getPositionByName("v");

    AggregateDescriptions aggregates;
    aggregates.push_back(AggregateDescription{
        .function = AggregateFunctionFactory::instance().get(*context, "count", {}),
        .parameters = {},
        .arguments = {},
        .argument_names = {},
        .column_name = "cnt_all",
    });
    aggregates.push_back(AggregateDescription{
        .function = AggregateFunctionFactory::instance().get(*context, "count", {value_type}),
        .parameters = {},
        .arguments = {arg_col},
        .argument_names = {"v"},
        .column_name = "cnt_v",
    });
    aggregates.push_back(AggregateDescription{
        .function = AggregateFunctionFactory::instance().get(*context, "sum", {value_type}),
        .parameters = {},
        .arguments = {arg_col},
        .argument_names = {"v"},
        .column_name = "sum_v",
    });

    ColumnNumbers keys{0};
    KeyRefAggFuncMap key_ref_agg_func;
    AggFuncRefKeyMap agg_func_ref_key;

    SpillConfig spill_config(
        context->getTemporaryPath(),
        "bench_tiforth_small_string_hash_agg",
        context->getSettingsRef().max_cached_data_bytes_in_spiller,
        context->getSettingsRef().max_spilled_rows_per_file,
        context->getSettingsRef().max_spilled_bytes_per_file,
        context->getFileProvider(),
        context->getSettingsRef().max_threads,
        context->getSettingsRef().max_block_size);

    auto params = std::make_unique<Aggregator::Params>(
        header,
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

void RunNativeAgg(const BenchConfig & cfg, benchmark::State & state) {
    const auto dataset = GetOrCreateDataset(cfg);
    const auto context = TiFlashTestEnv::getContext();
    auto params = BuildAggregatorParams(context, dataset->header);

    for (const auto & _ : state) {
        (void)_;
        RegisterOperatorSpillContext register_operator_spill_context;
        auto aggregator = std::make_shared<Aggregator>(
            *params,
            "BenchNativeAggSmallString",
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
        if (merging_buckets) {
            for (;;) {
                auto out_block = merging_buckets->getData(0);
                if (!out_block)
                    break;
                rows += out_block.rows();
                benchmark::DoNotOptimize(rows);
            }
        }
    }

    state.SetItemsProcessed(static_cast<int64_t>(cfg.num_rows) * state.iterations());
}

std::unique_ptr<tiforth::Plan> MakeTiForthAggPlan(const tiforth::Engine * engine) {
    ARROW_CHECK(engine != nullptr);

    auto maybe_builder = tiforth::PlanBuilder::Create(engine);
    ARROW_CHECK_OK(maybe_builder.status());
    auto builder = std::move(maybe_builder).ValueUnsafe();

    std::vector<tiforth::AggKey> keys = {{"k", tiforth::MakeFieldRef("k")}};
    std::vector<tiforth::AggFunc> aggs;
    aggs.push_back({"cnt_all", "count_all", nullptr});
    aggs.push_back({"cnt_v", "count", tiforth::MakeFieldRef("v")});
    aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef("v")});

    const tiforth::Engine * engine_ptr = engine;
    auto maybe_ctx_id = builder->AddBreakerState<tiforth::HashAggContext>(
        [engine_ptr, keys, aggs]() -> arrow::Result<std::shared_ptr<tiforth::HashAggContext>> {
            return std::make_shared<tiforth::HashAggContext>(engine_ptr, keys, aggs);
        });
    ARROW_CHECK_OK(maybe_ctx_id.status());
    const auto ctx_id = maybe_ctx_id.ValueUnsafe();

    auto maybe_build_stage = builder->AddStage();
    ARROW_CHECK_OK(maybe_build_stage.status());
    const auto build_stage = maybe_build_stage.ValueUnsafe();

    ARROW_CHECK_OK(builder->AppendPipe(
        build_stage,
        [ctx_id](tiforth::PlanTaskContext * ctx) -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
            ARROW_ASSIGN_OR_RAISE(auto agg_ctx, ctx->GetBreakerState<tiforth::HashAggContext>(ctx_id));
            return std::make_unique<tiforth::HashAggTransformOp>(std::move(agg_ctx));
        }));

    ARROW_CHECK_OK(builder->SetStageSink(
        build_stage,
        [ctx_id](tiforth::PlanTaskContext * ctx) -> arrow::Result<std::unique_ptr<tiforth::pipeline::SinkOp>> {
            ARROW_ASSIGN_OR_RAISE(auto agg_ctx, ctx->GetBreakerState<tiforth::HashAggContext>(ctx_id));
            return std::make_unique<tiforth::HashAggMergeSinkOp>(std::move(agg_ctx));
        }));

    auto maybe_result_stage = builder->AddStage();
    ARROW_CHECK_OK(maybe_result_stage.status());
    const auto result_stage = maybe_result_stage.ValueUnsafe();

    ARROW_CHECK_OK(builder->SetStageSource(
        result_stage,
        [ctx_id](tiforth::PlanTaskContext * ctx) -> arrow::Result<std::unique_ptr<tiforth::pipeline::SourceOp>> {
            ARROW_ASSIGN_OR_RAISE(auto agg_ctx, ctx->GetBreakerState<tiforth::HashAggContext>(ctx_id));
            return std::make_unique<tiforth::HashAggResultSourceOp>(std::move(agg_ctx));
        }));
    ARROW_CHECK_OK(builder->AddDependency(build_stage, result_stage));

    auto maybe_plan = builder->Finalize();
    ARROW_CHECK_OK(maybe_plan.status());
    return std::move(maybe_plan).ValueUnsafe();
}

void RunTiForthHashAgg(const BenchConfig & cfg, benchmark::State & state) {
    const auto dataset = GetOrCreateDataset(cfg);

    auto maybe_engine = tiforth::Engine::Create(tiforth::EngineOptions{});
    ARROW_CHECK_OK(maybe_engine.status());
    auto engine = std::move(maybe_engine).ValueUnsafe();

    auto plan = MakeTiForthAggPlan(engine.get());

    for (const auto & _ : state) {
        (void)_;
        auto maybe_task = plan->CreateTask();
        ARROW_CHECK_OK(maybe_task.status());
        auto task = std::move(maybe_task).ValueUnsafe();

        auto maybe_state = task->Step();
        ARROW_CHECK_OK(maybe_state.status());
        ARROW_CHECK(maybe_state.ValueUnsafe() == tiforth::TaskState::kNeedInput);

        for (const auto & batch : dataset->arrow_batches) {
            ARROW_CHECK(batch != nullptr);
            ARROW_CHECK_OK(task->PushInput(batch));
        }
        ARROW_CHECK_OK(task->CloseInput());

        while (true) {
            maybe_state = task->Step();
            ARROW_CHECK_OK(maybe_state.status());
            const auto st = maybe_state.ValueUnsafe();
            if (st == tiforth::TaskState::kFinished)
                break;
            if (st == tiforth::TaskState::kNeedInput)
                continue;
            ARROW_CHECK(st == tiforth::TaskState::kHasOutput);
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
    const size_t rows_per_block = 1 << 13;
    const size_t num_rows = 1 << 18;

    const std::vector<BenchConfig> configs = {
        // Small strings (vary length + distribution).
        {4, KeyDist::kUniformLowCard, num_rows, rows_per_block, 16, false, false},
        {8, KeyDist::kUniformLowCard, num_rows, rows_per_block, 16, false, false},
        {16, KeyDist::kUniformLowCard, num_rows, rows_per_block, 256, false, false},
        {24, KeyDist::kUniformLowCard, num_rows, rows_per_block, 256, false, false},
        {8, KeyDist::kZipfSkew, num_rows, rows_per_block, 128, false, false},
        {16, KeyDist::kZipfSkew, num_rows, rows_per_block, 256, false, false},

        // Null patterns.
        {8, KeyDist::kUniformLowCard, num_rows, rows_per_block, 16, true, true},
        {24, KeyDist::kZipfSkew, num_rows, rows_per_block, 256, true, true},

        // High-cardinality unique keys (smaller to cap memory).
        {8, KeyDist::kUniformHighCard, 1 << 17, rows_per_block, 0, false, false},
    };

    for (const auto & cfg : configs) {
        const auto case_name = MakeCaseId(cfg);
        benchmark::RegisterBenchmark(
            fmt::format("NativeAgg/SmallStringSingleKey/{}", case_name).c_str(),
            [cfg](benchmark::State & state) { RunNativeAgg(cfg, state); });
        benchmark::RegisterBenchmark(
            fmt::format("HashAgg/SmallStringSingleKey/{}", case_name).c_str(),
            [cfg](benchmark::State & state) { RunTiForthHashAgg(cfg, state); });
    }
}

const auto kRegistered = [] {
    RegisterCases();
    return true;
}();

} // namespace

} // namespace DB::tests

#else

static void BenchTiForthSmallStringHashAggDisabled(benchmark::State & state) {
    for (const auto & _ : state)
        benchmark::DoNotOptimize(_);
}
BENCHMARK(BenchTiForthSmallStringHashAggDisabled)->Name("SmallStringHashAgg/Disabled");

#endif // defined(TIFLASH_ENABLE_TIFORTH)
