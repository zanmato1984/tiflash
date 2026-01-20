#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Common/FieldVisitors.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Debug/TiFlashTestEnv.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Flash/TiForth/BlockPipelineRunner.h>

#include <arrow/memory_pool.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <tiforth/engine.h>
#include <tiforth/expr.h>
#include <tiforth/operators/arrow_hash_agg.h>
#include <tiforth/pipeline.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
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

using DecimalField128 = DecimalField<Decimal128>;

enum class KeyType {
    kInt32,
    kInt64,
};

enum class ValueType {
    kInt64,
    kFloat64,
    kDecimal128, // Decimal(precision=20, scale=2)
};

enum class KeyDist {
    kSingleGroup,
    kUniformLowCard,
    kUniformHighCard,
    kZipfSkew,
};

struct ParityConfig {
    std::string case_id;
    KeyType key_type;
    ValueType value_type;
    KeyDist dist;
    size_t num_rows;
    size_t rows_per_block;
    size_t groups; // for low-card / zipf; ignored for high-card
    bool null_keys;
    bool null_values;
    bool all_null_group; // force one group to have all-null values (for sum/min/max nullability)
};

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
    Block header;
};

std::string FieldToStableString(const Field & field) {
    return applyVisitor(FieldVisitorToString(), field);
}

std::string SerializeKeyRow(const Block & block, const std::vector<std::string_view> & key_names, size_t row) {
    std::string out;
    for (size_t i = 0; i < key_names.size(); ++i) {
        if (i != 0)
            out.push_back('\x1f');
        const auto & col = block.getByName(String(key_names[i])).column;
        Field f;
        col->get(row, f);
        out += FieldToStableString(f);
    }
    return out;
}

bool FieldIsNull(const Field & field) {
    return field.isNull();
}

std::optional<Int64> FieldToInt64(const Field & field) {
    if (FieldIsNull(field))
        return std::nullopt;
    if (field.getType() == Field::Types::Int64)
        return field.safeGet<Int64>();
    if (field.getType() == Field::Types::UInt64) {
        const auto v = field.safeGet<UInt64>();
        if (v > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            return std::nullopt;
        return static_cast<Int64>(v);
    }
    if (field.getType() == Field::Types::Int128)
        return static_cast<Int64>(field.safeGet<Int128>());
    return std::nullopt;
}

std::optional<Float64> FieldToFloat64(const Field & field) {
    if (FieldIsNull(field))
        return std::nullopt;
    if (field.getType() == Field::Types::Float64)
        return field.safeGet<Float64>();
    return std::nullopt;
}

struct CanonicalRow {
    Field cnt_v;
    Field sum_v;
    Field min_v;
    Field max_v;
};

using CanonicalMap = std::unordered_map<std::string, CanonicalRow>;

arrow::Result<CanonicalMap> Canonicalize(
    const std::vector<Block> & blocks,
    const std::vector<std::string_view> & key_names) {
    CanonicalMap out;
    for (const auto & block : blocks) {
        for (size_t row = 0; row < block.rows(); ++row) {
            const auto key = SerializeKeyRow(block, key_names, row);
            auto [it, inserted] = out.emplace(key, CanonicalRow{});
            if (!inserted)
                return arrow::Status::Invalid("duplicate group key in output: ", key);

            {
                Field f;
                block.getByName("cnt_v").column->get(row, f);
                it->second.cnt_v = std::move(f);
            }
            {
                Field f;
                block.getByName("sum_v").column->get(row, f);
                it->second.sum_v = std::move(f);
            }
            {
                Field f;
                block.getByName("min_v").column->get(row, f);
                it->second.min_v = std::move(f);
            }
            {
                Field f;
                block.getByName("max_v").column->get(row, f);
                it->second.max_v = std::move(f);
            }
        }
    }
    return out;
}

arrow::Status CompareCanonical(
    const CanonicalMap & expected,
    const CanonicalMap & actual,
    const ParityConfig & cfg) {
    if (expected.size() != actual.size()) {
        return arrow::Status::Invalid(
            "group count mismatch: expected=", expected.size(), " actual=", actual.size());
    }

    const auto compare_int = [&](const Field & a, const Field & b, std::string_view name) -> arrow::Status {
        const auto va = FieldToInt64(a);
        const auto vb = FieldToInt64(b);
        if (va.has_value() != vb.has_value())
            return arrow::Status::Invalid(fmt::format("{} nullability mismatch", name));
        if (!va.has_value())
            return arrow::Status::OK();
        if (*va != *vb)
            return arrow::Status::Invalid(fmt::format("{} mismatch: {} vs {}", name, *va, *vb));
        return arrow::Status::OK();
    };

    const auto compare_float = [&](const Field & a, const Field & b, std::string_view name) -> arrow::Status {
        const auto va = FieldToFloat64(a);
        const auto vb = FieldToFloat64(b);
        if (va.has_value() != vb.has_value())
            return arrow::Status::Invalid(fmt::format("{} nullability mismatch", name));
        if (!va.has_value())
            return arrow::Status::OK();
        const double diff = std::abs(*va - *vb);
        if (diff > 1e-9)
            return arrow::Status::Invalid(fmt::format("{} mismatch: {} vs {} (diff={})", name, *va, *vb, diff));
        return arrow::Status::OK();
    };

    const auto compare_decimal = [&](const Field & a, const Field & b, std::string_view name) -> arrow::Status {
        const auto sa = FieldToStableString(a);
        const auto sb = FieldToStableString(b);
        if (sa != sb)
            return arrow::Status::Invalid(fmt::format("{} mismatch: {} vs {}", name, sa, sb));
        return arrow::Status::OK();
    };

    for (const auto & [key, exp] : expected) {
        const auto it = actual.find(key);
        if (it == actual.end())
            return arrow::Status::Invalid("missing group key in actual: ", key);

        const auto & got = it->second;
        ARROW_RETURN_NOT_OK(compare_int(exp.cnt_v, got.cnt_v, "cnt_v"));

        switch (cfg.value_type) {
        case ValueType::kInt64:
            ARROW_RETURN_NOT_OK(compare_int(exp.sum_v, got.sum_v, "sum_v"));
            ARROW_RETURN_NOT_OK(compare_int(exp.min_v, got.min_v, "min_v"));
            ARROW_RETURN_NOT_OK(compare_int(exp.max_v, got.max_v, "max_v"));
            break;
        case ValueType::kFloat64:
            ARROW_RETURN_NOT_OK(compare_float(exp.sum_v, got.sum_v, "sum_v"));
            ARROW_RETURN_NOT_OK(compare_float(exp.min_v, got.min_v, "min_v"));
            ARROW_RETURN_NOT_OK(compare_float(exp.max_v, got.max_v, "max_v"));
            break;
        case ValueType::kDecimal128:
            ARROW_RETURN_NOT_OK(compare_decimal(exp.sum_v, got.sum_v, "sum_v"));
            ARROW_RETURN_NOT_OK(compare_decimal(exp.min_v, got.min_v, "min_v"));
            ARROW_RETURN_NOT_OK(compare_decimal(exp.max_v, got.max_v, "max_v"));
            break;
        }
    }

    return arrow::Status::OK();
}

DataTypePtr MakeKeyType(const ParityConfig & cfg) {
    DataTypePtr nested;
    if (cfg.key_type == KeyType::kInt32)
        nested = std::make_shared<DataTypeInt32>();
    else
        nested = std::make_shared<DataTypeInt64>();
    return cfg.null_keys ? makeNullable(nested) : nested;
}

DataTypePtr MakeValueType(const ParityConfig & cfg) {
    DataTypePtr nested;
    switch (cfg.value_type) {
    case ValueType::kInt64:
        nested = std::make_shared<DataTypeInt64>();
        break;
    case ValueType::kFloat64:
        nested = std::make_shared<DataTypeFloat64>();
        break;
    case ValueType::kDecimal128:
        nested = createDecimal(/*precision=*/20, /*scale=*/2);
        break;
    }
    return cfg.null_values ? makeNullable(nested) : nested;
}

DataTypePtr MakeNestedValueType(ValueType t) {
    switch (t) {
    case ValueType::kInt64:
        return std::make_shared<DataTypeInt64>();
    case ValueType::kFloat64:
        return std::make_shared<DataTypeFloat64>();
    case ValueType::kDecimal128:
        return createDecimal(/*precision=*/20, /*scale=*/2);
    }
    return std::make_shared<DataTypeInt64>();
}

MutableColumnPtr MakeKeyColumn(const ParityConfig & cfg) {
    if (!cfg.null_keys) {
        if (cfg.key_type == KeyType::kInt32)
            return ColumnInt32::create();
        return ColumnInt64::create();
    }

    MutableColumnPtr nested;
    if (cfg.key_type == KeyType::kInt32)
        nested = ColumnInt32::create();
    else
        nested = ColumnInt64::create();
    auto null_map = ColumnUInt8::create();
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

MutableColumnPtr MakeValueColumn(const ParityConfig & cfg) {
    const auto nested_type = MakeNestedValueType(cfg.value_type);
    if (!cfg.null_values)
        return nested_type->createColumn();

    auto nested = nested_type->createColumn();
    auto null_map = ColumnUInt8::create();
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

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

Dataset MakeDataset(const ParityConfig & cfg) {
    std::mt19937_64 rng(0);
    std::unique_ptr<ZipfGenerator> zipf;
    if (cfg.dist == KeyDist::kZipfSkew)
        zipf = std::make_unique<ZipfGenerator>(std::max(cfg.groups, size_t{1}), /*s=*/1.1);

    const auto key_type = MakeKeyType(cfg);
    const auto value_type = MakeValueType(cfg);

    const size_t blocks = (cfg.num_rows + cfg.rows_per_block - 1) / cfg.rows_per_block;
    Dataset out;
    out.blocks.reserve(blocks);

    size_t global_row = 0;
    for (size_t b = 0; b < blocks; ++b) {
        const size_t rows = std::min(cfg.rows_per_block, cfg.num_rows - global_row);
        auto key_col = MakeKeyColumn(cfg);
        auto value_col = MakeValueColumn(cfg);
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

            const bool key_is_null = cfg.null_keys && (row % 37 == 0);
            if (cfg.key_type == KeyType::kInt32)
                AppendNullable(key_col, key_is_null, Field(static_cast<Int64>(static_cast<Int32>(key_id))));
            else
                AppendNullable(key_col, key_is_null, Field(static_cast<Int64>(key_id)));

            const bool value_is_null =
                cfg.null_values
                && ((row % 17 == 0) || (cfg.all_null_group && cfg.dist == KeyDist::kUniformLowCard && key_id == 0));

            if (cfg.value_type == ValueType::kInt64) {
                AppendNullable(value_col, value_is_null, Field(static_cast<Int64>(row)));
            } else if (cfg.value_type == ValueType::kFloat64) {
                AppendNullable(value_col, value_is_null, Field(static_cast<Float64>(row) * 0.1));
            } else {
                AppendNullable(value_col, value_is_null, Field(DecimalField128(Int128(static_cast<Int64>(row)), 2)));
            }
        }

        Block block;
        block.insert({std::move(key_col), key_type, "k"});
        block.insert({std::move(value_col), value_type, "v"});
        out.blocks.push_back(std::move(block));
        global_row += rows;
    }

    out.header = out.blocks.at(0).cloneEmpty();
    return out;
}

std::unique_ptr<Aggregator::Params> BuildAggregatorParams(
    const ContextPtr & context,
    const Block & header) {
    if (!AggregateFunctionFactory::instance().isAggregateFunctionName("sum"))
        ::DB::registerAggregateFunctions();

    const auto value_type = header.getByName("v").type;

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
    aggregates.push_back(AggregateDescription{
        .function = AggregateFunctionFactory::instance().get(*context, "min", {value_type}),
        .parameters = {},
        .arguments = {1},
        .argument_names = {"v"},
        .column_name = "min_v",
    });
    aggregates.push_back(AggregateDescription{
        .function = AggregateFunctionFactory::instance().get(*context, "max", {value_type}),
        .parameters = {},
        .arguments = {1},
        .argument_names = {"v"},
        .column_name = "max_v",
    });

    ColumnNumbers keys{0};
    KeyRefAggFuncMap key_ref_agg_func;
    AggFuncRefKeyMap agg_func_ref_key;

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
        SpillConfig(
            context->getTemporaryPath(),
            "gtest_tiforth_arrow_hash_agg_parity",
            context->getSettingsRef().max_cached_data_bytes_in_spiller,
            context->getSettingsRef().max_spilled_rows_per_file,
            context->getSettingsRef().max_spilled_bytes_per_file,
            context->getFileProvider(),
            context->getSettingsRef().max_threads,
            context->getSettingsRef().max_block_size),
        /*max_block_size_=*/context->getSettingsRef().max_block_size,
        /*use_magic_hash_=*/false,
        TiDB::dummy_collators);
    return params;
}

arrow::Result<std::vector<Block>> RunNativeAggregator(const Dataset & dataset) {
    auto context = TiFlashTestEnv::getContext();
    if (context == nullptr)
        return arrow::Status::Invalid("TiFlashTestEnv::getContext returned null");
    auto params = BuildAggregatorParams(context, dataset.header);

    RegisterOperatorSpillContext register_operator_spill_context;
    auto aggregator = std::make_shared<Aggregator>(
        *params,
        "GTestNativeAggregator",
        /*concurrency=*/1,
        register_operator_spill_context,
        /*is_auto_pass_through=*/false,
        params->use_magic_hash);
    auto data_variants = std::make_shared<AggregatedDataVariants>();
    data_variants->aggregator = aggregator.get();

    Aggregator::AggProcessInfo agg_process_info(aggregator.get());
    for (const auto & block : dataset.blocks) {
        agg_process_info.resetBlock(block);
        aggregator->executeOnBlock(agg_process_info, *data_variants, /*thread_num=*/1);
    }

    std::vector<AggregatedDataVariantsPtr> variants{data_variants};
    auto merging_buckets = aggregator->mergeAndConvertToBlocks(variants, /*final=*/true, /*max_threads=*/1);
    std::vector<Block> out;
    for (;;) {
        auto block = merging_buckets->getData(0);
        if (!block)
            break;
        out.push_back(std::move(block));
    }
    return out;
}

arrow::Result<std::vector<Block>> RunTiForthArrowHashAgg(const Dataset & dataset) {
    ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
    ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));

    const std::vector<tiforth::AggKey> keys = {{"k", tiforth::MakeFieldRef("k")}};
    std::vector<tiforth::AggFunc> aggs;
    aggs.push_back({"cnt_v", "count", tiforth::MakeFieldRef("v")});
    aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef("v")});
    aggs.push_back({"min_v", "min", tiforth::MakeFieldRef("v")});
    aggs.push_back({"max_v", "max", tiforth::MakeFieldRef("v")});

    ARROW_RETURN_NOT_OK(builder->AppendTransform(
        [engine_ptr = engine.get(), keys, aggs]() -> arrow::Result<tiforth::TransformOpPtr> {
            return std::make_unique<tiforth::ArrowHashAggTransformOp>(engine_ptr, keys, aggs);
        }));

    ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
    const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
    ARROW_ASSIGN_OR_RAISE(
        auto outputs,
        TiForth::RunTiForthPipelineOnBlocks(*pipeline, dataset.blocks, options_by_name, arrow::default_memory_pool()));

    std::vector<Block> out_blocks;
    out_blocks.reserve(outputs.size());
    for (auto & out : outputs) {
        out_blocks.push_back(std::move(out.block));
    }
    return out_blocks;
}

arrow::Status RunParityCase(const ParityConfig & cfg) {
    const auto dataset = MakeDataset(cfg);
    ARROW_ASSIGN_OR_RAISE(auto native_blocks, RunNativeAggregator(dataset));
    ARROW_ASSIGN_OR_RAISE(auto tiforth_blocks, RunTiForthArrowHashAgg(dataset));

    const std::vector<std::string_view> key_names = {"k"};
    ARROW_ASSIGN_OR_RAISE(auto expected, Canonicalize(native_blocks, key_names));
    ARROW_ASSIGN_OR_RAISE(auto actual, Canonicalize(tiforth_blocks, key_names));
    return CompareCanonical(expected, actual, cfg);
}

TEST(TiForthArrowHashAggParityTest, NativeVsArrowHashAggMatrix)
{
    const std::vector<ParityConfig> cases = {
        // Int64 values (include high-card + skew, and nulls + null-key grouping).
        {"i32_i64_single_no_null", KeyType::kInt32, ValueType::kInt64, KeyDist::kSingleGroup, 4096, 512, 1, false, false, false},
        {"i32_i64_low_nulls", KeyType::kInt32, ValueType::kInt64, KeyDist::kUniformLowCard, 8192, 512, 16, true, true, true},
        {"i32_i64_high_nulls", KeyType::kInt32, ValueType::kInt64, KeyDist::kUniformHighCard, 4096, 512, 0, true, true, false},
        {"i32_i64_zipf_no_null", KeyType::kInt32, ValueType::kInt64, KeyDist::kZipfSkew, 8192, 512, 128, false, false, false},

        // Float64 values (nulls + skew).
        {"i64_f64_low_nulls", KeyType::kInt64, ValueType::kFloat64, KeyDist::kUniformLowCard, 8192, 512, 16, true, true, true},
        {"i64_f64_zipf_nulls", KeyType::kInt64, ValueType::kFloat64, KeyDist::kZipfSkew, 8192, 512, 128, true, true, false},

        // Decimal values (nulls + high-card).
        {"i64_dec_low_nulls", KeyType::kInt64, ValueType::kDecimal128, KeyDist::kUniformLowCard, 4096, 512, 16, true, true, true},
        {"i64_dec_high_no_null", KeyType::kInt64, ValueType::kDecimal128, KeyDist::kUniformHighCard, 4096, 512, 0, false, false, false},
    };

    for (const auto & cfg : cases) {
        SCOPED_TRACE(cfg.case_id);
        const auto st = RunParityCase(cfg);
        ASSERT_TRUE(st.ok()) << st.ToString();
    }
}

} // namespace

} // namespace DB::tests

#else

TEST(TiForthArrowHashAggParityTest, Disabled)
{
    GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
