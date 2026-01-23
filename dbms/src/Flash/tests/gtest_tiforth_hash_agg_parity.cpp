#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Common/FieldVisitors.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
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
#include <tiforth/operators/hash_agg.h>
#include <tiforth/pipeline/op/op.h>
#include <tiforth/plan.h>

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
    kString,
};

enum class ValueType {
    kInt32,
    kInt64,
    kUInt64,
    kFloat32,
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
    Field cnt_all;
    Field cnt_v;
    Field sum_v;
    Field min_v;
    Field max_v;
    Field avg_v;
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
                block.getByName("cnt_all").column->get(row, f);
                it->second.cnt_all = std::move(f);
            }
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
            {
                Field f;
                block.getByName("avg_v").column->get(row, f);
                it->second.avg_v = std::move(f);
            }
        }
    }
    return out;
}

arrow::Status CompareOutputTypes(
    const std::vector<Block> & expected_blocks,
    const std::vector<Block> & actual_blocks,
    const std::vector<std::string_view> & column_names) {
    if (expected_blocks.empty() || actual_blocks.empty())
        return arrow::Status::OK();

    const auto & expected = expected_blocks.front();
    const auto & actual = actual_blocks.front();
    for (const auto & name : column_names) {
        if (!expected.has(String(name)) || !actual.has(String(name)))
            return arrow::Status::Invalid("missing output column: ", name);
        const auto & exp_type = expected.getByName(String(name)).type;
        const auto & got_type = actual.getByName(String(name)).type;
        if (exp_type == nullptr || got_type == nullptr)
            return arrow::Status::Invalid("output column type must not be null: ", name);
        if (exp_type->getName() != got_type->getName()) {
            return arrow::Status::Invalid(
                fmt::format("output type mismatch for {}: {} vs {}", name, exp_type->getName(), got_type->getName()));
        }
    }
    return arrow::Status::OK();
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
        if (std::isnan(*va) && std::isnan(*vb))
            return arrow::Status::OK();
        if (std::isnan(*va) != std::isnan(*vb))
            return arrow::Status::Invalid(fmt::format("{} NaN mismatch", name));
        if (std::isinf(*va) || std::isinf(*vb)) {
            if (*va != *vb)
                return arrow::Status::Invalid(fmt::format("{} inf mismatch: {} vs {}", name, *va, *vb));
            return arrow::Status::OK();
        }
        if (*va == 0.0 && *vb == 0.0 && std::signbit(*va) != std::signbit(*vb))
            return arrow::Status::Invalid(fmt::format("{} signed zero mismatch", name));
        const double diff = std::abs(*va - *vb);
        const double tol = 1e-9;
        if (diff > tol)
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
        {
            const auto st = compare_int(exp.cnt_all, got.cnt_all, "cnt_all");
            if (!st.ok())
                return arrow::Status::Invalid("group=", key, " ", st.ToString());
        }
        {
            const auto st = compare_int(exp.cnt_v, got.cnt_v, "cnt_v");
            if (!st.ok())
                return arrow::Status::Invalid("group=", key, " ", st.ToString());
        }

        switch (cfg.value_type) {
        case ValueType::kInt32:
        case ValueType::kInt64:
        case ValueType::kUInt64:
            {
                const auto st = compare_int(exp.sum_v, got.sum_v, "sum_v");
                if (!st.ok())
                    return arrow::Status::Invalid("group=", key, " ", st.ToString());
            }
            {
                const auto st = compare_int(exp.min_v, got.min_v, "min_v");
                if (!st.ok())
                    return arrow::Status::Invalid("group=", key, " ", st.ToString());
            }
            {
                const auto st = compare_int(exp.max_v, got.max_v, "max_v");
                if (!st.ok())
                    return arrow::Status::Invalid("group=", key, " ", st.ToString());
            }
            {
                const auto st = compare_float(exp.avg_v, got.avg_v, "avg_v");
                if (!st.ok())
                    return arrow::Status::Invalid("group=", key, " ", st.ToString());
            }
            break;
        case ValueType::kFloat32:
        case ValueType::kFloat64:
            {
                const auto st = compare_float(exp.sum_v, got.sum_v, "sum_v");
                if (!st.ok())
                    return arrow::Status::Invalid("group=", key, " ", st.ToString());
            }
            {
                const auto st = compare_float(exp.min_v, got.min_v, "min_v");
                if (!st.ok())
                    return arrow::Status::Invalid("group=", key, " ", st.ToString());
            }
            {
                const auto st = compare_float(exp.max_v, got.max_v, "max_v");
                if (!st.ok())
                    return arrow::Status::Invalid("group=", key, " ", st.ToString());
            }
            {
                const auto st = compare_float(exp.avg_v, got.avg_v, "avg_v");
                if (!st.ok())
                    return arrow::Status::Invalid("group=", key, " ", st.ToString());
            }
            break;
        case ValueType::kDecimal128:
            {
                const auto st = compare_decimal(exp.sum_v, got.sum_v, "sum_v");
                if (!st.ok())
                    return arrow::Status::Invalid("group=", key, " ", st.ToString());
            }
            {
                const auto st = compare_decimal(exp.min_v, got.min_v, "min_v");
                if (!st.ok())
                    return arrow::Status::Invalid("group=", key, " ", st.ToString());
            }
            {
                const auto st = compare_decimal(exp.max_v, got.max_v, "max_v");
                if (!st.ok())
                    return arrow::Status::Invalid("group=", key, " ", st.ToString());
            }
            {
                const auto st = compare_decimal(exp.avg_v, got.avg_v, "avg_v");
                if (!st.ok())
                    return arrow::Status::Invalid("group=", key, " ", st.ToString());
            }
            break;
        }
    }

    return arrow::Status::OK();
}

DataTypePtr MakeKeyType(const ParityConfig & cfg) {
    DataTypePtr nested;
    if (cfg.key_type == KeyType::kInt32)
        nested = std::make_shared<DataTypeInt32>();
    else if (cfg.key_type == KeyType::kInt64)
        nested = std::make_shared<DataTypeInt64>();
    else
        nested = std::make_shared<DataTypeString>();
    return cfg.null_keys ? makeNullable(nested) : nested;
}

DataTypePtr MakeValueType(const ParityConfig & cfg) {
    DataTypePtr nested;
    switch (cfg.value_type) {
    case ValueType::kInt32:
        nested = std::make_shared<DataTypeInt32>();
        break;
    case ValueType::kInt64:
        nested = std::make_shared<DataTypeInt64>();
        break;
    case ValueType::kUInt64:
        nested = std::make_shared<DataTypeUInt64>();
        break;
    case ValueType::kFloat32:
        nested = std::make_shared<DataTypeFloat32>();
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
    case ValueType::kInt32:
        return std::make_shared<DataTypeInt32>();
    case ValueType::kInt64:
        return std::make_shared<DataTypeInt64>();
    case ValueType::kUInt64:
        return std::make_shared<DataTypeUInt64>();
    case ValueType::kFloat32:
        return std::make_shared<DataTypeFloat32>();
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
        if (cfg.key_type == KeyType::kInt64)
            return ColumnInt64::create();
        return ColumnString::create();
    }

    MutableColumnPtr nested;
    if (cfg.key_type == KeyType::kInt32)
        nested = ColumnInt32::create();
    else if (cfg.key_type == KeyType::kInt64)
        nested = ColumnInt64::create();
    else
        nested = ColumnString::create();
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
            {
                AppendNullable(
                    key_col,
                    key_is_null,
                    Field(static_cast<Int64>(static_cast<Int32>(key_id))));
            }
            else if (cfg.key_type == KeyType::kInt64)
            {
                AppendNullable(key_col, key_is_null, Field(static_cast<Int64>(key_id)));
            }
            else
            {
                AppendNullable(key_col, key_is_null, Field(std::string("k") + std::to_string(key_id)));
            }

            const bool value_is_null =
                cfg.null_values
                && ((row % 17 == 0) || (cfg.all_null_group && cfg.dist == KeyDist::kUniformLowCard && key_id == 0));

            switch (cfg.value_type) {
            case ValueType::kInt32:
                AppendNullable(value_col, value_is_null, Field(static_cast<Int64>(static_cast<Int32>(row))));
                break;
            case ValueType::kInt64:
                AppendNullable(value_col, value_is_null, Field(static_cast<Int64>(row)));
                break;
            case ValueType::kUInt64:
                AppendNullable(value_col, value_is_null, Field(static_cast<UInt64>(row)));
                break;
            case ValueType::kFloat32:
                AppendNullable(value_col, value_is_null, Field(static_cast<Float64>(static_cast<Float32>(row) * 0.1f)));
                break;
            case ValueType::kFloat64:
                AppendNullable(value_col, value_is_null, Field(static_cast<Float64>(row) * 0.1));
                break;
            case ValueType::kDecimal128:
                AppendNullable(value_col, value_is_null, Field(DecimalField128(Int128(static_cast<Int64>(row)), 2)));
                break;
            }
        }

        Block block;
        block.insert({std::move(key_col), key_type, "k"});
        block.insert({std::move(value_col), value_type, "v"});
        out.blocks.push_back(std::move(block));
        global_row += rows;
    }

    if (!out.blocks.empty())
        out.header = out.blocks.at(0).cloneEmpty();
    else
    {
        Block header;
        header.insert({key_type->createColumn(), key_type, "k"});
        header.insert({value_type->createColumn(), value_type, "v"});
        out.header = std::move(header);
    }
    return out;
}

std::unique_ptr<Aggregator::Params> BuildAggregatorParams(
    const ContextPtr & context,
    const Block & header,
    const ColumnNumbers & keys) {
    if (!AggregateFunctionFactory::instance().isAggregateFunctionName("sum"))
        ::DB::registerAggregateFunctions();

    const auto arg_col = header.getPositionByName("v");
    const auto value_type = header.getByName("v").type;

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
    aggregates.push_back(AggregateDescription{
        .function = AggregateFunctionFactory::instance().get(*context, "min", {value_type}),
        .parameters = {},
        .arguments = {arg_col},
        .argument_names = {"v"},
        .column_name = "min_v",
    });
    aggregates.push_back(AggregateDescription{
        .function = AggregateFunctionFactory::instance().get(*context, "max", {value_type}),
        .parameters = {},
        .arguments = {arg_col},
        .argument_names = {"v"},
        .column_name = "max_v",
    });
    aggregates.push_back(AggregateDescription{
        .function = AggregateFunctionFactory::instance().get(*context, "avg", {value_type}),
        .parameters = {},
        .arguments = {arg_col},
        .argument_names = {"v"},
        .column_name = "avg_v",
    });

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
            "gtest_tiforth_hash_agg_parity",
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

arrow::Result<std::vector<Block>> RunNativeAggregatorWithKeys(const Dataset & dataset, const ColumnNumbers & keys) {
    auto context = TiFlashTestEnv::getContext();
    if (context == nullptr)
        return arrow::Status::Invalid("TiFlashTestEnv::getContext returned null");
    auto params = BuildAggregatorParams(context, dataset.header, keys);

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
    if (!merging_buckets)
        return std::vector<Block>{};
    std::vector<Block> out;
    for (;;) {
        auto block = merging_buckets->getData(0);
        if (!block)
            break;
        out.push_back(std::move(block));
    }
    return out;
}

arrow::Result<std::vector<Block>> RunNativeAggregator(const Dataset & dataset) {
    return RunNativeAggregatorWithKeys(dataset, ColumnNumbers{0});
}

arrow::Result<std::vector<Block>> RunTiForthHashAgg(const Dataset & dataset) {
    ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
    ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PlanBuilder::Create(engine.get()));

    const std::vector<tiforth::AggKey> keys = {{"k", tiforth::MakeFieldRef("k")}};
    std::vector<tiforth::AggFunc> aggs;
    aggs.push_back({"cnt_all", "count_all", nullptr});
    aggs.push_back({"cnt_v", "count", tiforth::MakeFieldRef("v")});
    aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef("v")});
    aggs.push_back({"min_v", "min", tiforth::MakeFieldRef("v")});
    aggs.push_back({"max_v", "max", tiforth::MakeFieldRef("v")});
    aggs.push_back({"avg_v", "avg", tiforth::MakeFieldRef("v")});

    const tiforth::Engine * engine_ptr = engine.get();
    ARROW_ASSIGN_OR_RAISE(
        const auto ctx_id,
        builder->AddBreakerState<tiforth::HashAggState>(
            [engine_ptr, keys, aggs]() -> arrow::Result<std::shared_ptr<tiforth::HashAggState>> {
                return std::make_shared<tiforth::HashAggState>(engine_ptr, keys, aggs);
            }));

    ARROW_ASSIGN_OR_RAISE(const auto build_stage, builder->AddStage());
    ARROW_RETURN_NOT_OK(builder->SetStageSink(
        build_stage,
        [ctx_id](tiforth::PlanTaskContext * ctx) -> arrow::Result<std::unique_ptr<tiforth::pipeline::SinkOp>> {
            ARROW_ASSIGN_OR_RAISE(auto agg_state, ctx->GetBreakerState<tiforth::HashAggState>(ctx_id));
            return std::make_unique<tiforth::HashAggSinkOp>(std::move(agg_state));
        }));

    ARROW_ASSIGN_OR_RAISE(const auto result_stage, builder->AddStage());
    ARROW_RETURN_NOT_OK(builder->SetStageSource(
        result_stage,
        [ctx_id](tiforth::PlanTaskContext * ctx) -> arrow::Result<std::unique_ptr<tiforth::pipeline::SourceOp>> {
            ARROW_ASSIGN_OR_RAISE(auto agg_state, ctx->GetBreakerState<tiforth::HashAggState>(ctx_id));
            return std::make_unique<tiforth::HashAggResultSourceOp>(std::move(agg_state));
        }));
    ARROW_RETURN_NOT_OK(builder->AddDependency(build_stage, result_stage));

    ARROW_ASSIGN_OR_RAISE(auto plan, builder->Finalize());
    const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
    ARROW_ASSIGN_OR_RAISE(
        auto outputs,
        TiForth::RunTiForthPlanOnBlocks(
            *plan,
            dataset.blocks,
            options_by_name,
            arrow::default_memory_pool(),
            /*sample_block=*/&dataset.header));

    std::vector<Block> out_blocks;
    out_blocks.reserve(outputs.size());
    for (auto & out : outputs) {
        out_blocks.push_back(std::move(out.block));
    }
    return out_blocks;
}

arrow::Result<std::vector<Block>> RunTiForthHashAggWithKeys(
    const Dataset & dataset,
    const std::vector<tiforth::AggKey> & keys) {
    ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
    ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PlanBuilder::Create(engine.get()));

    std::vector<tiforth::AggFunc> aggs;
    aggs.push_back({"cnt_all", "count_all", nullptr});
    aggs.push_back({"cnt_v", "count", tiforth::MakeFieldRef("v")});
    aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef("v")});
    aggs.push_back({"min_v", "min", tiforth::MakeFieldRef("v")});
    aggs.push_back({"max_v", "max", tiforth::MakeFieldRef("v")});
    aggs.push_back({"avg_v", "avg", tiforth::MakeFieldRef("v")});

    const tiforth::Engine * engine_ptr = engine.get();
    ARROW_ASSIGN_OR_RAISE(
        const auto ctx_id,
        builder->AddBreakerState<tiforth::HashAggState>(
            [engine_ptr, keys, aggs]() -> arrow::Result<std::shared_ptr<tiforth::HashAggState>> {
                return std::make_shared<tiforth::HashAggState>(engine_ptr, keys, aggs);
            }));

    ARROW_ASSIGN_OR_RAISE(const auto build_stage, builder->AddStage());
    ARROW_RETURN_NOT_OK(builder->SetStageSink(
        build_stage,
        [ctx_id](tiforth::PlanTaskContext * ctx) -> arrow::Result<std::unique_ptr<tiforth::pipeline::SinkOp>> {
            ARROW_ASSIGN_OR_RAISE(auto agg_state, ctx->GetBreakerState<tiforth::HashAggState>(ctx_id));
            return std::make_unique<tiforth::HashAggSinkOp>(std::move(agg_state));
        }));

    ARROW_ASSIGN_OR_RAISE(const auto result_stage, builder->AddStage());
    ARROW_RETURN_NOT_OK(builder->SetStageSource(
        result_stage,
        [ctx_id](tiforth::PlanTaskContext * ctx) -> arrow::Result<std::unique_ptr<tiforth::pipeline::SourceOp>> {
            ARROW_ASSIGN_OR_RAISE(auto agg_state, ctx->GetBreakerState<tiforth::HashAggState>(ctx_id));
            return std::make_unique<tiforth::HashAggResultSourceOp>(std::move(agg_state));
        }));
    ARROW_RETURN_NOT_OK(builder->AddDependency(build_stage, result_stage));

    ARROW_ASSIGN_OR_RAISE(auto plan, builder->Finalize());
    const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
    ARROW_ASSIGN_OR_RAISE(
        auto outputs,
        TiForth::RunTiForthPlanOnBlocks(
            *plan,
            dataset.blocks,
            options_by_name,
            arrow::default_memory_pool(),
            /*sample_block=*/&dataset.header));

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
    ARROW_ASSIGN_OR_RAISE(auto tiforth_blocks, RunTiForthHashAgg(dataset));

    const std::vector<std::string_view> key_names = {"k"};
    ARROW_RETURN_NOT_OK(CompareOutputTypes(
        native_blocks,
        tiforth_blocks,
        /*column_names=*/{"cnt_all", "cnt_v", "sum_v", "min_v", "max_v", "avg_v", "k"}));
    ARROW_ASSIGN_OR_RAISE(auto expected, Canonicalize(native_blocks, key_names));
    ARROW_ASSIGN_OR_RAISE(auto actual, Canonicalize(tiforth_blocks, key_names));
    return CompareCanonical(expected, actual, cfg);
}

TEST(TiForthHashAggParityTest, NativeVsHashAggMatrix)
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

        // Additional numeric coverage (types + empty input).
        {"i32_i32_low_nulls", KeyType::kInt32, ValueType::kInt32, KeyDist::kUniformLowCard, 4096, 512, 16, true, true, true},
        {"i64_u64_zipf_no_null", KeyType::kInt64, ValueType::kUInt64, KeyDist::kZipfSkew, 8192, 512, 128, false, false, false},
        {"i32_f32_low_no_null", KeyType::kInt32, ValueType::kFloat32, KeyDist::kUniformLowCard, 4096, 512, 16, false, false, false},

        // String keys (binary semantics; collation out of scope).
        {"s_i64_low_nulls", KeyType::kString, ValueType::kInt64, KeyDist::kUniformLowCard, 8192, 512, 16, true, true, true},
        {"s_i64_high_no_null", KeyType::kString, ValueType::kInt64, KeyDist::kUniformHighCard, 4096, 512, 0, false, false, false},

        // Empty input (group-by should return empty output).
        {"i32_i64_empty", KeyType::kInt32, ValueType::kInt64, KeyDist::kUniformLowCard, 0, 512, 16, false, false, false},
        {"s_i64_empty", KeyType::kString, ValueType::kInt64, KeyDist::kUniformLowCard, 0, 512, 16, false, false, false},
    };

    for (const auto & cfg : cases) {
        SCOPED_TRACE(cfg.case_id);
        const auto st = RunParityCase(cfg);
        ASSERT_TRUE(st.ok()) << st.ToString();
    }
}

TEST(TiForthHashAggParityTest, MultiKeyParity)
{
    std::vector<Block> blocks;
    blocks.reserve(4);

    const auto k0_type = makeNullable(std::make_shared<DataTypeInt32>());
    const auto k1_type = makeNullable(std::make_shared<DataTypeInt64>());
    const auto v_type = makeNullable(std::make_shared<DataTypeInt64>());

    size_t global_row = 0;
    for (size_t b = 0; b < 4; ++b)
    {
        MutableColumnPtr k0 = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
        MutableColumnPtr k1 = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
        MutableColumnPtr v = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

        const size_t rows = 512;
        k0->reserve(rows);
        k1->reserve(rows);
        v->reserve(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            const size_t row = global_row + i;
            AppendNullable(k0, /*is_null=*/row % 37 == 0, Field(static_cast<Int64>(row % 16)));
            AppendNullable(k1, /*is_null=*/row % 53 == 0, Field(static_cast<Int64>(row % 7)));
            AppendNullable(v, /*is_null=*/row % 17 == 0, Field(static_cast<Int64>(row)));
        }

        Block block;
        block.insert({std::move(k0), k0_type, "k0"});
        block.insert({std::move(k1), k1_type, "k1"});
        block.insert({std::move(v), v_type, "v"});
        blocks.push_back(std::move(block));
        global_row += rows;
    }

    Dataset dataset;
    dataset.blocks = std::move(blocks);
    dataset.header = dataset.blocks.at(0).cloneEmpty();

    const ColumnNumbers keys{0, 1};
    ASSERT_TRUE(dataset.header.has("k0") && dataset.header.has("k1") && dataset.header.has("v"));

    const auto native_blocks_res = RunNativeAggregatorWithKeys(dataset, keys);
    ASSERT_TRUE(native_blocks_res.ok()) << native_blocks_res.status().ToString();
    const auto & native_blocks = native_blocks_res.ValueOrDie();

    const std::vector<tiforth::AggKey> tiforth_keys = {{"k0", tiforth::MakeFieldRef("k0")},
                                                       {"k1", tiforth::MakeFieldRef("k1")}};
    const auto tiforth_blocks_res = RunTiForthHashAggWithKeys(dataset, tiforth_keys);
    ASSERT_TRUE(tiforth_blocks_res.ok()) << tiforth_blocks_res.status().ToString();
    const auto & tiforth_blocks = tiforth_blocks_res.ValueOrDie();

    const auto type_st = CompareOutputTypes(
        native_blocks,
        tiforth_blocks,
        /*column_names=*/{"cnt_all", "cnt_v", "sum_v", "min_v", "max_v", "avg_v", "k0", "k1"});
    ASSERT_TRUE(type_st.ok()) << type_st.ToString();

    const std::vector<std::string_view> key_names = {"k0", "k1"};
    const auto expected_res = Canonicalize(native_blocks, key_names);
    ASSERT_TRUE(expected_res.ok()) << expected_res.status().ToString();
    const auto actual_res = Canonicalize(tiforth_blocks, key_names);
    ASSERT_TRUE(actual_res.ok()) << actual_res.status().ToString();

    ParityConfig cfg;
    cfg.value_type = ValueType::kInt64;
    const auto st = CompareCanonical(expected_res.ValueOrDie(), actual_res.ValueOrDie(), cfg);
    ASSERT_TRUE(st.ok()) << st.ToString();
}

TEST(TiForthHashAggParityTest, SpecialFloatValues)
{
    auto k_col = ColumnInt32::create();
    auto v_col = ColumnFloat64::create();

    const auto push = [&](Int32 k, Float64 v) {
        k_col->insert(Field(static_cast<Int64>(k)));
        v_col->insert(Field(v));
    };

    push(0, 1.0);
    push(0, 2.0);
    push(0, std::numeric_limits<double>::quiet_NaN());

    push(1, std::numeric_limits<double>::quiet_NaN());
    push(1, 1.0);

    push(2, -0.0);
    push(3, 0.0);

    push(6, 0.0);
    push(6, -0.0);

    push(7, -0.0);
    push(7, 0.0);

    push(4, std::numeric_limits<double>::infinity());
    push(4, 1.0);

    push(5, -std::numeric_limits<double>::infinity());
    push(5, 1.0);

    Block block;
    block.insert({std::move(k_col), std::make_shared<DataTypeInt32>(), "k"});
    block.insert({std::move(v_col), std::make_shared<DataTypeFloat64>(), "v"});
    Dataset dataset;
    dataset.blocks = {std::move(block)};
    dataset.header = dataset.blocks.at(0).cloneEmpty();

    auto native_blocks_res = RunNativeAggregator(dataset);
    ASSERT_TRUE(native_blocks_res.ok()) << native_blocks_res.status().ToString();
    auto tiforth_blocks_res = RunTiForthHashAgg(dataset);
    ASSERT_TRUE(tiforth_blocks_res.ok()) << tiforth_blocks_res.status().ToString();

    const auto & native_blocks = native_blocks_res.ValueOrDie();
    const auto & tiforth_blocks = tiforth_blocks_res.ValueOrDie();
    const auto type_st = CompareOutputTypes(
        native_blocks,
        tiforth_blocks,
        /*column_names=*/{"cnt_all", "cnt_v", "sum_v", "min_v", "max_v", "avg_v", "k"});
    ASSERT_TRUE(type_st.ok()) << type_st.ToString();

    const std::vector<std::string_view> key_names = {"k"};
    const auto expected_res = Canonicalize(native_blocks, key_names);
    ASSERT_TRUE(expected_res.ok()) << expected_res.status().ToString();
    const auto actual_res = Canonicalize(tiforth_blocks, key_names);
    ASSERT_TRUE(actual_res.ok()) << actual_res.status().ToString();

    ParityConfig cfg;
    cfg.value_type = ValueType::kFloat64;
    const auto st = CompareCanonical(expected_res.ValueOrDie(), actual_res.ValueOrDie(), cfg);
    ASSERT_TRUE(st.ok()) << st.ToString();
}

TEST(TiForthHashAggParityTest, Int64Overflow)
{
    auto k_col = ColumnInt32::create();
    auto v_col = ColumnInt64::create();

    const auto push = [&](Int32 k, Int64 v) {
        k_col->insert(Field(static_cast<Int64>(k)));
        v_col->insert(Field(v));
    };

    push(0, std::numeric_limits<Int64>::max());
    push(0, 1);
    push(1, std::numeric_limits<Int64>::min());
    push(1, -1);

    Block block;
    block.insert({std::move(k_col), std::make_shared<DataTypeInt32>(), "k"});
    block.insert({std::move(v_col), std::make_shared<DataTypeInt64>(), "v"});
    Dataset dataset;
    dataset.blocks = {std::move(block)};
    dataset.header = dataset.blocks.at(0).cloneEmpty();

    auto native_blocks_res = RunNativeAggregator(dataset);
    ASSERT_TRUE(native_blocks_res.ok()) << native_blocks_res.status().ToString();
    auto tiforth_blocks_res = RunTiForthHashAgg(dataset);
    ASSERT_TRUE(tiforth_blocks_res.ok()) << tiforth_blocks_res.status().ToString();

    const auto & native_blocks = native_blocks_res.ValueOrDie();
    const auto & tiforth_blocks = tiforth_blocks_res.ValueOrDie();
    const auto type_st = CompareOutputTypes(
        native_blocks,
        tiforth_blocks,
        /*column_names=*/{"cnt_all", "cnt_v", "sum_v", "min_v", "max_v", "avg_v", "k"});
    ASSERT_TRUE(type_st.ok()) << type_st.ToString();

    const std::vector<std::string_view> key_names = {"k"};
    const auto expected_res = Canonicalize(native_blocks, key_names);
    ASSERT_TRUE(expected_res.ok()) << expected_res.status().ToString();
    const auto actual_res = Canonicalize(tiforth_blocks, key_names);
    ASSERT_TRUE(actual_res.ok()) << actual_res.status().ToString();

    ParityConfig cfg;
    cfg.value_type = ValueType::kInt64;
    const auto st = CompareCanonical(expected_res.ValueOrDie(), actual_res.ValueOrDie(), cfg);
    ASSERT_TRUE(st.ok()) << st.ToString();
}

} // namespace

} // namespace DB::tests

#else

TEST(TiForthHashAggParityTest, Disabled)
{
    GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
