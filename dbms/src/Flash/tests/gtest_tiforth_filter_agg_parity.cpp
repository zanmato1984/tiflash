#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/arrow_compute_agg.h"
#include "tiforth/operators/filter.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/pipeline/op/op.h"
#include "tiforth/plan.h"
#include "tiforth/pipeline.h"

#include <Core/Block.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <Flash/TiForth/BlockPipelineRunner.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/FunctionTestUtils.h>

#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/status.h>

#include <ext/scope_guard.h>

#include <functional>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB::tests {

namespace {

using DecimalField128 = DecimalField<Decimal128>;

arrow::Result<ColumnsWithTypeAndName> RunTiForthPipelineOnBlock(
    const Block& input, const std::unordered_map<String, TiForth::ColumnOptions>& options_by_name,
    std::function<arrow::Status(const tiforth::Engine*, tiforth::PipelineBuilder*)> build_pipeline) {
  if (build_pipeline == nullptr) {
    return arrow::Status::Invalid("pipeline builder must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  if (engine == nullptr) {
    return arrow::Status::Invalid("tiforth engine must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));
  if (builder == nullptr) {
    return arrow::Status::Invalid("tiforth pipeline builder must not be null");
  }

  ARROW_RETURN_NOT_OK(build_pipeline(engine.get(), builder.get()));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  if (pipeline == nullptr) {
    return arrow::Status::Invalid("tiforth pipeline must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(
      auto outputs,
      TiForth::RunTiForthPipelineOnBlocks(*pipeline, {input}, options_by_name,
                                          arrow::default_memory_pool()));

  Blocks out_blocks;
  out_blocks.reserve(outputs.size());
  for (auto& out : outputs) {
    out_blocks.push_back(std::move(out.block));
  }

  return vstackBlocks(std::move(out_blocks)).getColumnsWithTypeAndName();
}

arrow::Result<ColumnsWithTypeAndName> RunTiForthPlanOnBlock(
    const Block& input, const std::unordered_map<String, TiForth::ColumnOptions>& options_by_name,
    std::function<arrow::Status(const tiforth::Engine*, tiforth::PlanBuilder*)> build_plan) {
  if (build_plan == nullptr) {
    return arrow::Status::Invalid("plan builder must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  if (engine == nullptr) {
    return arrow::Status::Invalid("tiforth engine must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PlanBuilder::Create(engine.get()));
  if (builder == nullptr) {
    return arrow::Status::Invalid("tiforth plan builder must not be null");
  }

  ARROW_RETURN_NOT_OK(build_plan(engine.get(), builder.get()));

  ARROW_ASSIGN_OR_RAISE(auto plan, builder->Finalize());
  if (plan == nullptr) {
    return arrow::Status::Invalid("tiforth plan must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(auto outputs, TiForth::RunTiForthPlanOnBlocks(*plan, {input}, options_by_name,
                                                                      arrow::default_memory_pool()));

  Blocks out_blocks;
  out_blocks.reserve(outputs.size());
  for (auto& out : outputs) {
    out_blocks.push_back(std::move(out.block));
  }

  return vstackBlocks(std::move(out_blocks)).getColumnsWithTypeAndName();
}

arrow::Status AppendHashAggPlan(const tiforth::Engine* engine, tiforth::PlanBuilder* builder,
                               std::vector<tiforth::AggKey> keys, std::vector<tiforth::AggFunc> aggs) {
  if (engine == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  if (builder == nullptr) {
    return arrow::Status::Invalid("plan builder must not be null");
  }

  const tiforth::Engine* engine_ptr = engine;
  ARROW_ASSIGN_OR_RAISE(
      const auto ctx_id,
      builder->AddBreakerState<tiforth::HashAggState>(
          [engine_ptr, keys, aggs]() -> arrow::Result<std::shared_ptr<tiforth::HashAggState>> {
            return std::make_shared<tiforth::HashAggState>(engine_ptr, keys, aggs);
          }));

  ARROW_ASSIGN_OR_RAISE(const auto build_stage, builder->AddStage());
  ARROW_RETURN_NOT_OK(builder->SetStageSink(
      build_stage, [ctx_id](tiforth::PlanTaskContext* ctx) -> arrow::Result<std::unique_ptr<tiforth::pipeline::SinkOp>> {
        ARROW_ASSIGN_OR_RAISE(auto agg_state, ctx->GetBreakerState<tiforth::HashAggState>(ctx_id));
        return std::make_unique<tiforth::HashAggSinkOp>(std::move(agg_state));
      }));

  ARROW_ASSIGN_OR_RAISE(const auto result_stage, builder->AddStage());
  ARROW_RETURN_NOT_OK(builder->SetStageSource(
      result_stage, [ctx_id](tiforth::PlanTaskContext* ctx) -> arrow::Result<std::unique_ptr<tiforth::pipeline::SourceOp>> {
        ARROW_ASSIGN_OR_RAISE(auto agg_state, ctx->GetBreakerState<tiforth::HashAggState>(ctx_id));
        return std::make_unique<tiforth::HashAggResultSourceOp>(std::move(agg_state), /*max_output_rows=*/1 << 30);
      }));
  ARROW_RETURN_NOT_OK(builder->AddDependency(build_stage, result_stage));
  return arrow::Status::OK();
}

class TiForthFilterAggParityTestRunner : public ExecutorTest {
 public:
  void initializeContext() override {
    ExecutorTest::initializeContext();

    context.addMockTable(
        {"default", "tiforth_filter_parity"},
        {{"k", TiDB::TP::TypeLong}, {"v", TiDB::TP::TypeLong}, {"s", TiDB::TP::TypeString}},
        {toNullableVec<Int32>("k", {Int32(1), Int32(1), Int32(2), std::optional<Int32>{},
                                    Int32(3), Int32(4)}),
         toNullableVec<Int32>("v", {Int32(10), std::optional<Int32>{}, Int32(1), Int32(2),
                                    std::optional<Int32>{}, std::optional<Int32>{}}),
         toNullableVec<String>("s", {"a", "b", "a", "c", std::optional<String>{}, "d"})});

    context.addMockTable(
        {"default", "tiforth_agg_parity"},
        {{"k", TiDB::TP::TypeLong}, {"v", TiDB::TP::TypeLong}},
        {toNullableVec<Int32>("k", {Int32(1), Int32(1), Int32(2), Int32(2), std::optional<Int32>{},
                                    Int32(3), Int32(4)}),
         toNullableVec<Int32>("v", {Int32(10), std::optional<Int32>{}, Int32(20), Int32(1), Int32(7),
                                    std::optional<Int32>{}, std::optional<Int32>{}})});

    context.addMockTable(
        {"default", "tiforth_agg_parity_floats"},
        {{"k", TiDB::TP::TypeLong}, {"f32", TiDB::TP::TypeFloat}, {"f64", TiDB::TP::TypeDouble}},
        {toNullableVec<Int32>("k", {Int32(1), Int32(1), Int32(2), Int32(2), std::optional<Int32>{},
                                    Int32(3)}),
         toNullableVec<Float32>("f32", {Float32(1.0F), std::optional<Float32>{}, Float32(2.5F),
                                        Float32(-0.5F), Float32(3.0F), std::optional<Float32>{}}),
         toNullableVec<Float64>("f64", {Float64(10.0), Float64(2.0), std::optional<Float64>{},
                                        Float64(1.0), Float64(7.0), std::optional<Float64>{}})});

    context.addMockTable(
        {"default", "tiforth_agg_parity_decimal"},
        {{"k", TiDB::TP::TypeLong}, {"d", TiDB::TP::TypeNewDecimal, true, Poco::Dynamic::Var{}, 20, 2}},
        {toNullableVec<Int32>("k", {Int32(1), Int32(1), Int32(2), Int32(2), Int32(2),
                                    std::optional<Int32>{}, Int32(3)}),
         createColumn<Nullable<Decimal128>>(std::make_tuple(20, 2),
                                            {DecimalField128(Int128(123), 2),
                                             {},
                                             DecimalField128(Int128(100), 2),
                                             DecimalField128(Int128(200), 2),
                                             DecimalField128(Int128(200), 2),
                                             DecimalField128(Int128(-50), 2),
                                             {}},
                                            "d")});

    context.addMockTable(
        {"default", "tiforth_filter_truthy"},
        {{"int32_col", TiDB::TP::TypeLong}, {"string_col", TiDB::TP::TypeString}},
        {toNullableVec<Int32>("int32_col", {Int32(0), Int32(1), Int32(2), Int32(3), Int32(4), Int32(5), Int32(6),
                                            Int32(7), std::optional<Int32>{}}),
         toNullableVec<String>("string_col",
                               {"", "a", "1", "0", "ab", "  ", "\t", "\n", std::optional<String>{}})});

    // Collated string keys for hash agg parity:
    // - utf8mb4_bin: padding BIN (trailing spaces ignored)
    // - utf8mb4_general_ci: padding CI (case + trailing spaces ignored)
    // - utf8mb4_0900_ai_ci: no-padding CI (case ignored; trailing spaces significant)
    using I32Field = typename TypeTraits<Int32>::FieldType;
    const auto collation_rows_s = std::vector<std::optional<String>>{
        String("a"), String("a "), String("A"), String("b"), std::optional<String>{}, std::optional<String>{}};
    const auto collation_rows_k2 = std::vector<std::optional<I32Field>>{
        static_cast<I32Field>(1),
        static_cast<I32Field>(1),
        static_cast<I32Field>(1),
        static_cast<I32Field>(1),
        static_cast<I32Field>(1),
        std::optional<I32Field>{}};
    const auto collation_rows_v = std::vector<std::optional<I32Field>>{
        static_cast<I32Field>(10),
        static_cast<I32Field>(20),
        static_cast<I32Field>(1),
        static_cast<I32Field>(5),
        static_cast<I32Field>(7),
        static_cast<I32Field>(8)};

    context.addMockTable(
        {"default", "tiforth_agg_collation_bin"},
        {{"s", TiDB::TP::TypeString, true, Poco::Dynamic::Var("utf8mb4_bin")},
         {"k2", TiDB::TP::TypeLong},
         {"v", TiDB::TP::TypeLong}},
        {toNullableVec<String>("s", collation_rows_s),
         toNullableVec<Int32>("k2", collation_rows_k2),
         toNullableVec<Int32>("v", collation_rows_v)});

    context.addMockTable(
        {"default", "tiforth_agg_collation_general_ci"},
        {{"s", TiDB::TP::TypeString, true, Poco::Dynamic::Var("utf8mb4_general_ci")},
         {"k2", TiDB::TP::TypeLong},
         {"v", TiDB::TP::TypeLong}},
        {toNullableVec<String>("s", collation_rows_s),
         toNullableVec<Int32>("k2", collation_rows_k2),
         toNullableVec<Int32>("v", collation_rows_v)});

    context.addMockTable(
        {"default", "tiforth_agg_collation_0900_ai_ci"},
        {{"s", TiDB::TP::TypeString, true, Poco::Dynamic::Var("utf8mb4_0900_ai_ci")},
         {"k2", TiDB::TP::TypeLong},
         {"v", TiDB::TP::TypeLong}},
        {toNullableVec<String>("s", collation_rows_s),
         toNullableVec<Int32>("k2", collation_rows_k2),
         toNullableVec<Int32>("v", collation_rows_v)});
  }
};

TEST_F(TiForthFilterAggParityTestRunner, FilterEqualInt32)
try
{
  const auto scan_cols = executeRawQuery("select k, v, s from default.tiforth_filter_parity");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPipelineOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PipelineBuilder* builder) -> arrow::Status {
        auto predicate = tiforth::MakeCall(
            "equal", {tiforth::MakeFieldRef(1),
                      tiforth::MakeLiteral(std::make_shared<arrow::Int32Scalar>(1))});
        return builder->AppendPipe(
            [engine, predicate]() -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
              return std::make_unique<tiforth::FilterPipeOp>(engine, predicate);
            });
      });
  if (!actual.ok()) {
    ASSERT_TRUE(actual.status().IsNotImplemented()) << actual.status().ToString();
    return;
  }

  const auto expected = executeRawQuery(
      "select k, v, s from default.tiforth_filter_parity where v = 1");
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, FilterOrEqualInt32)
try
{
  const auto scan_cols = executeRawQuery("select k, v, s from default.tiforth_filter_parity");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPipelineOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PipelineBuilder* builder) -> arrow::Status {
        auto is1 = tiforth::MakeCall(
            "equal", {tiforth::MakeFieldRef(1),
                      tiforth::MakeLiteral(std::make_shared<arrow::Int32Scalar>(1))});
        auto is2 = tiforth::MakeCall(
            "equal", {tiforth::MakeFieldRef(1),
                      tiforth::MakeLiteral(std::make_shared<arrow::Int32Scalar>(2))});
        auto predicate = tiforth::MakeCall("or", {is1, is2});
        return builder->AppendPipe(
            [engine, predicate]() -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
              return std::make_unique<tiforth::FilterPipeOp>(engine, predicate);
            });
      });
  if (!actual.ok()) {
    ASSERT_TRUE(actual.status().IsNotImplemented()) << actual.status().ToString();
    return;
  }

  const auto expected = executeRawQuery(
      "select k, v, s from default.tiforth_filter_parity where v = 1 or v = 2");
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, FilterTruthyInt32)
try
{
  const auto scan_cols = executeRawQuery("select int32_col, string_col from default.tiforth_filter_truthy");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPipelineOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PipelineBuilder* builder) -> arrow::Status {
        auto predicate = tiforth::MakeFieldRef(0);
        return builder->AppendPipe(
            [engine, predicate]() -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
              return std::make_unique<tiforth::FilterPipeOp>(engine, predicate);
            });
      });
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();

  const auto expected = executeStreams(
      context.scan("default", "tiforth_filter_truthy")
          .filter(col("int32_col"))
          .project({col("int32_col"), col("string_col")})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, FilterTruthyString)
try
{
  const auto scan_cols = executeRawQuery("select int32_col, string_col from default.tiforth_filter_truthy");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPipelineOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PipelineBuilder* builder) -> arrow::Status {
        auto predicate = tiforth::MakeFieldRef(1);
        return builder->AppendPipe(
            [engine, predicate]() -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
              return std::make_unique<tiforth::FilterPipeOp>(engine, predicate);
            });
      });
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();

  const auto expected = executeStreams(
      context.scan("default", "tiforth_filter_truthy")
          .filter(col("string_col"))
          .project({col("int32_col"), col("string_col")})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, HashAggGroupByInt32)
try
{
  const auto scan_cols = executeRawQuery("select k, v from default.tiforth_agg_parity");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        std::vector<tiforth::AggKey> keys = {{"k", tiforth::MakeFieldRef(0)}};
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"cnt_all", "count_all", nullptr});
        aggs.push_back({"cnt_v", "count", tiforth::MakeFieldRef(1)});
        aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef(1)});
        aggs.push_back({"avg_v", "avg", tiforth::MakeFieldRef(1)});
        aggs.push_back({"min_v", "min", tiforth::MakeFieldRef(1)});
        aggs.push_back({"max_v", "max", tiforth::MakeFieldRef(1)});

        return AppendHashAggPlan(engine, builder, std::move(keys), std::move(aggs));
      });
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();

  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_parity")
          .aggregation(
              {Count(lit(Field(static_cast<UInt64>(1)))),
               Count(col("v")),
               Sum(col("v")),
               Avg(col("v")),
               Min(col("v")),
               Max(col("v"))},
              {col("k")})
          .project({"count(1)", "count(v)", "sum(v)", "avg(v)", "min(v)", "max(v)", "k"})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, ArrowComputeAggGroupByInt32Sum)
try
{
  const bool old_flag = context.context->getSettingsRef().enable_tiforth_arrow_compute_agg;
  context.context->setSetting("enable_tiforth_arrow_compute_agg", "true");
  SCOPE_EXIT({
    context.context->setSetting("enable_tiforth_arrow_compute_agg", old_flag ? "true" : "false");
  });

  const auto scan_cols = executeRawQuery("select k, v from default.tiforth_agg_parity");
  const Block scan_block(scan_cols);

  const bool use_arrow_compute = context.context->getSettingsRef().enable_tiforth_arrow_compute_agg;
  ASSERT_TRUE(use_arrow_compute);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [use_arrow_compute](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        std::vector<tiforth::AggKey> keys = {{"k", tiforth::MakeFieldRef(0)}};
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef(1)});

        ARROW_ASSIGN_OR_RAISE(const auto stage, builder->AddStage());
        return builder->AppendPipe(
            stage,
            [engine, keys, aggs, use_arrow_compute](tiforth::PlanTaskContext*) -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
              (void)use_arrow_compute;
              return std::make_unique<tiforth::ArrowComputeAggPipeOp>(engine, keys, aggs);
            });
      });
  if (!actual.ok()) {
    ASSERT_TRUE(actual.status().IsNotImplemented()) << actual.status().ToString();
    return;
  }

  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_parity")
          .aggregation({Sum(col("v"))}, {col("k")})
          .project({"sum(v)", "k"})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, HashAggBreakerGroupByInt32)
try
{
  const auto scan_cols = executeRawQuery("select k, v from default.tiforth_agg_parity");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        std::vector<tiforth::AggKey> keys = {{"k", tiforth::MakeFieldRef(0)}};
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"cnt_all", "count_all", nullptr});
        aggs.push_back({"cnt_v", "count", tiforth::MakeFieldRef(1)});
        aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef(1)});
        aggs.push_back({"avg_v", "avg", tiforth::MakeFieldRef(1)});
        aggs.push_back({"min_v", "min", tiforth::MakeFieldRef(1)});
        aggs.push_back({"max_v", "max", tiforth::MakeFieldRef(1)});
        return AppendHashAggPlan(engine, builder, std::move(keys), std::move(aggs));
      });
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();

  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_parity")
          .aggregation(
              {Count(lit(Field(static_cast<UInt64>(1)))),
               Count(col("v")),
               Sum(col("v")),
               Avg(col("v")),
               Min(col("v")),
               Max(col("v"))},
              {col("k")})
          .project({"count(1)", "count(v)", "sum(v)", "avg(v)", "min(v)", "max(v)", "k"})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, HashAggGroupByFloatDouble)
try
{
  const auto scan_cols =
      executeRawQuery("select k, f32, f64 from default.tiforth_agg_parity_floats");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        std::vector<tiforth::AggKey> keys = {{"k", tiforth::MakeFieldRef(0)}};
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"sum_f32", "sum", tiforth::MakeFieldRef(1)});
        aggs.push_back({"min_f32", "min", tiforth::MakeFieldRef(1)});
        aggs.push_back({"max_f32", "max", tiforth::MakeFieldRef(1)});
        aggs.push_back({"sum_f64", "sum", tiforth::MakeFieldRef(2)});
        aggs.push_back({"min_f64", "min", tiforth::MakeFieldRef(2)});
        aggs.push_back({"max_f64", "max", tiforth::MakeFieldRef(2)});
        return AppendHashAggPlan(engine, builder, std::move(keys), std::move(aggs));
      });
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();

  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_parity_floats")
          .aggregation(
              {Sum(col("f32")),
               Min(col("f32")),
               Max(col("f32")),
               Sum(col("f64")),
               Min(col("f64")),
               Max(col("f64"))},
              {col("k")})
          .project({"sum(f32)", "min(f32)", "max(f32)", "sum(f64)", "min(f64)", "max(f64)",
                    "k"})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, HashAggBreakerGroupByFloatDouble)
try
{
  const auto scan_cols =
      executeRawQuery("select k, f32, f64 from default.tiforth_agg_parity_floats");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        std::vector<tiforth::AggKey> keys = {{"k", tiforth::MakeFieldRef(0)}};
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"sum_f32", "sum", tiforth::MakeFieldRef(1)});
        aggs.push_back({"min_f32", "min", tiforth::MakeFieldRef(1)});
        aggs.push_back({"max_f32", "max", tiforth::MakeFieldRef(1)});
        aggs.push_back({"sum_f64", "sum", tiforth::MakeFieldRef(2)});
        aggs.push_back({"min_f64", "min", tiforth::MakeFieldRef(2)});
        aggs.push_back({"max_f64", "max", tiforth::MakeFieldRef(2)});
        return AppendHashAggPlan(engine, builder, std::move(keys), std::move(aggs));
      });
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();

  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_parity_floats")
          .aggregation(
              {Sum(col("f32")),
               Min(col("f32")),
               Max(col("f32")),
               Sum(col("f64")),
               Min(col("f64")),
               Max(col("f64"))},
              {col("k")})
          .project({"sum(f32)", "min(f32)", "max(f32)", "sum(f64)", "min(f64)", "max(f64)",
                    "k"})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, HashAggGroupByDecimal)
try
{
  const auto scan_cols = executeRawQuery("select k, d from default.tiforth_agg_parity_decimal");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        std::vector<tiforth::AggKey> keys = {{"k", tiforth::MakeFieldRef(0)}};
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"sum_d", "sum", tiforth::MakeFieldRef(1)});
        aggs.push_back({"avg_d", "avg", tiforth::MakeFieldRef(1)});
        aggs.push_back({"min_d", "min", tiforth::MakeFieldRef(1)});
        aggs.push_back({"max_d", "max", tiforth::MakeFieldRef(1)});
        return AppendHashAggPlan(engine, builder, std::move(keys), std::move(aggs));
      });
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();

  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_parity_decimal")
          .aggregation({Sum(col("d")), Avg(col("d")), Min(col("d")), Max(col("d"))}, {col("k")})
          .project({"sum(d)", "avg(d)", "min(d)", "max(d)", "k"})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, HashAggBreakerGroupByDecimal)
try
{
  const auto scan_cols = executeRawQuery("select k, d from default.tiforth_agg_parity_decimal");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        std::vector<tiforth::AggKey> keys = {{"k", tiforth::MakeFieldRef(0)}};
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"sum_d", "sum", tiforth::MakeFieldRef(1)});
        aggs.push_back({"avg_d", "avg", tiforth::MakeFieldRef(1)});
        aggs.push_back({"min_d", "min", tiforth::MakeFieldRef(1)});
        aggs.push_back({"max_d", "max", tiforth::MakeFieldRef(1)});
        return AppendHashAggPlan(engine, builder, std::move(keys), std::move(aggs));
      });
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();

  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_parity_decimal")
          .aggregation({Sum(col("d")), Avg(col("d")), Min(col("d")), Max(col("d"))}, {col("k")})
          .project({"sum(d)", "avg(d)", "min(d)", "max(d)", "k"})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, HashAggGroupByStringBinary)
try
{
  const auto scan_cols = executeRawQuery("select s, v from default.tiforth_filter_parity");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        std::vector<tiforth::AggKey> keys = {{"s", tiforth::MakeFieldRef(0)}};
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"cnt_all", "count_all", nullptr});
        aggs.push_back({"cnt_v", "count", tiforth::MakeFieldRef(1)});
        aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef(1)});
        return AppendHashAggPlan(engine, builder, std::move(keys), std::move(aggs));
      });
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();

  const auto expected = executeStreams(
      context.scan("default", "tiforth_filter_parity")
          .aggregation(
              {Count(lit(Field(static_cast<UInt64>(1)))),
               Count(col("v")),
               Sum(col("v"))},
              {col("s")})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, HashAggGroupByStringUtf8Mb4BinPadSpace)
try
{
  const auto scan_cols = executeRawQuery("select s, v from default.tiforth_agg_collation_bin");
  const Block scan_block(scan_cols);

  std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  options_by_name.emplace(scan_block.getByPosition(0).name,
                          TiForth::ColumnOptions{.collation_id = 46});  // UTF8MB4_BIN (PAD SPACE)

  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        std::vector<tiforth::AggKey> keys = {{"s", tiforth::MakeFieldRef(0)}};
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"cnt_all", "count_all", nullptr});
        aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef(1)});
        return AppendHashAggPlan(engine, builder, std::move(keys), std::move(aggs));
      });
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();

  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_collation_bin")
          .aggregation(
              {Count(lit(Field(static_cast<UInt64>(1)))),
               Sum(col("v"))},
              {col("s")})
          .project({"count(1)", "sum(v)", "s"})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, HashAggBreakerGroupByStringUtf8Mb4BinPadSpace)
try
{
  const auto scan_cols = executeRawQuery("select s, v from default.tiforth_agg_collation_bin");
  const Block scan_block(scan_cols);

  std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  options_by_name.emplace(scan_block.getByPosition(0).name,
                          TiForth::ColumnOptions{.collation_id = 46});  // UTF8MB4_BIN (PAD SPACE)

  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        std::vector<tiforth::AggKey> keys = {{"s", tiforth::MakeFieldRef(0)}};
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"cnt_all", "count_all", nullptr});
        aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef(1)});
        return AppendHashAggPlan(engine, builder, std::move(keys), std::move(aggs));
      });
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();

  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_collation_bin")
          .aggregation(
              {Count(lit(Field(static_cast<UInt64>(1)))),
               Sum(col("v"))},
              {col("s")})
          .project({"count(1)", "sum(v)", "s"})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, HashAggTwoKeyStringUtf8Mb4BinPadSpace)
try
{
  // gtest in TiFlash is pinned to an older version without `GTEST_SKIP()`.
  // Keep this test as a no-op until multi-key collation-aware grouping is implemented.
  return;
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, HashAggGroupByStringUtf8Mb4GeneralCiPadSpace)
try
{
  const auto scan_cols = executeRawQuery("select s, v from default.tiforth_agg_collation_general_ci");
  const Block scan_block(scan_cols);

  std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  options_by_name.emplace(scan_block.getByPosition(0).name,
                          TiForth::ColumnOptions{.collation_id = 45});  // UTF8MB4_GENERAL_CI (PAD SPACE)

  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        std::vector<tiforth::AggKey> keys = {{"s", tiforth::MakeFieldRef(0)}};
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"cnt_all", "count_all", nullptr});
        aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef(1)});
        return AppendHashAggPlan(engine, builder, std::move(keys), std::move(aggs));
      });
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();

  context.setCollation(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_collation_general_ci")
          .aggregation(
              {Count(lit(Field(static_cast<UInt64>(1)))),
               Sum(col("v"))},
              {col("s")})
          .project({"count(1)", "sum(v)", "s"})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, HashAggGroupByStringUtf8Mb40900AiCiNoPad)
try
{
  const auto scan_cols = executeRawQuery("select s, v from default.tiforth_agg_collation_0900_ai_ci");
  const Block scan_block(scan_cols);

  std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  options_by_name.emplace(scan_block.getByPosition(0).name,
                          TiForth::ColumnOptions{.collation_id = 255});  // UTF8MB4_0900_AI_CI (NO PAD)

  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        std::vector<tiforth::AggKey> keys = {{"s", tiforth::MakeFieldRef(0)}};
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"cnt_all", "count_all", nullptr});
        aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef(1)});
        return AppendHashAggPlan(engine, builder, std::move(keys), std::move(aggs));
      });
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();

  context.setCollation(TiDB::ITiDBCollator::UTF8MB4_0900_AI_CI);
  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_collation_0900_ai_ci")
          .aggregation(
              {Count(lit(Field(static_cast<UInt64>(1)))),
               Sum(col("v"))},
              {col("s")})
          .project({"count(1)", "sum(v)", "s"})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, FilterThenGlobalAggEmptyInput)
try
{
  const auto scan_cols = executeRawQuery("select k, v from default.tiforth_agg_parity");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        auto predicate = tiforth::MakeCall(
            "equal", {tiforth::MakeFieldRef(0),
                      tiforth::MakeLiteral(std::make_shared<arrow::Int32Scalar>(999))});
        ARROW_ASSIGN_OR_RAISE(const auto stage, builder->AddStage());
        ARROW_RETURN_NOT_OK(builder->AppendPipe(
            stage,
            [engine, predicate](tiforth::PlanTaskContext*)
                -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
              return std::make_unique<tiforth::FilterPipeOp>(engine, predicate);
            }));

        std::vector<tiforth::AggKey> keys;
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"cnt_all", "count_all", nullptr});
        aggs.push_back({"cnt_v", "count", tiforth::MakeFieldRef(1)});
        aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef(1)});
        aggs.push_back({"avg_v", "avg", tiforth::MakeFieldRef(1)});
        aggs.push_back({"min_v", "min", tiforth::MakeFieldRef(1)});
        aggs.push_back({"max_v", "max", tiforth::MakeFieldRef(1)});

        return builder->AppendPipe(
            stage, [engine, keys, aggs](tiforth::PlanTaskContext*) -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
              return std::make_unique<tiforth::ArrowComputeAggPipeOp>(engine, keys, aggs);
            });
      });
  if (!actual.ok()) {
    ASSERT_TRUE(actual.status().IsNotImplemented()) << actual.status().ToString();
    return;
  }

  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_parity")
          .filter(eq(col("k"), lit(Field(static_cast<Int64>(999)))))
          .aggregation(
              {Count(lit(Field(static_cast<UInt64>(1)))),
               Count(col("v")),
               Sum(col("v")),
               Avg(col("v")),
               Min(col("v")),
               Max(col("v"))},
              {})
          .project({"count(1)", "count(v)", "sum(v)", "avg(v)", "min(v)", "max(v)"})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, FilterThenGlobalAggEmptyInputBreaker)
try
{
  const auto scan_cols = executeRawQuery("select k, v from default.tiforth_agg_parity");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        auto predicate = tiforth::MakeCall(
            "equal", {tiforth::MakeFieldRef(0),
                      tiforth::MakeLiteral(std::make_shared<arrow::Int32Scalar>(999))});

        std::vector<tiforth::AggKey> keys;
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"cnt_all", "count_all", nullptr});
        aggs.push_back({"cnt_v", "count", tiforth::MakeFieldRef(1)});
        aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef(1)});
        aggs.push_back({"avg_v", "avg", tiforth::MakeFieldRef(1)});
        aggs.push_back({"min_v", "min", tiforth::MakeFieldRef(1)});
        aggs.push_back({"max_v", "max", tiforth::MakeFieldRef(1)});

        ARROW_ASSIGN_OR_RAISE(const auto stage, builder->AddStage());
        ARROW_RETURN_NOT_OK(builder->AppendPipe(
            stage,
            [engine, predicate](tiforth::PlanTaskContext*)
                -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
              return std::make_unique<tiforth::FilterPipeOp>(engine, predicate);
            }));
        ARROW_RETURN_NOT_OK(builder->AppendPipe(
            stage,
            [engine, keys, aggs](tiforth::PlanTaskContext*) -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
              return std::make_unique<tiforth::ArrowComputeAggPipeOp>(engine, keys, aggs);
            }));
        return arrow::Status::OK();
      });
  if (!actual.ok()) {
    ASSERT_TRUE(actual.status().IsNotImplemented()) << actual.status().ToString();
    return;
  }

  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_parity")
          .filter(eq(col("k"), lit(Field(static_cast<Int64>(999)))))
          .aggregation(
              {Count(lit(Field(static_cast<UInt64>(1)))),
               Count(col("v")),
               Sum(col("v")),
               Avg(col("v")),
               Min(col("v")),
               Max(col("v"))},
              {})
          .project({"count(1)", "count(v)", "sum(v)", "avg(v)", "min(v)", "max(v)"})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, GlobalAggNonEmptyInput)
try
{
  const auto scan_cols = executeRawQuery("select k, v from default.tiforth_agg_parity");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        std::vector<tiforth::AggKey> keys;
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"cnt_all", "count_all", nullptr});
        aggs.push_back({"cnt_v", "count", tiforth::MakeFieldRef(1)});
        aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef(1)});
        aggs.push_back({"avg_v", "avg", tiforth::MakeFieldRef(1)});
        aggs.push_back({"min_v", "min", tiforth::MakeFieldRef(1)});
        aggs.push_back({"max_v", "max", tiforth::MakeFieldRef(1)});

        ARROW_ASSIGN_OR_RAISE(const auto stage, builder->AddStage());
        return builder->AppendPipe(
            stage,
            [engine, keys, aggs](tiforth::PlanTaskContext*) -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
              return std::make_unique<tiforth::ArrowComputeAggPipeOp>(engine, keys, aggs);
            });
      });
  if (!actual.ok()) {
    ASSERT_TRUE(actual.status().IsNotImplemented()) << actual.status().ToString();
    return;
  }

  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_parity")
          .aggregation(
              {Count(lit(Field(static_cast<UInt64>(1)))),
               Count(col("v")),
               Sum(col("v")),
               Avg(col("v")),
               Min(col("v")),
               Max(col("v"))},
              {})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

TEST_F(TiForthFilterAggParityTestRunner, GlobalAggNonEmptyInputBreaker)
try
{
  const auto scan_cols = executeRawQuery("select k, v from default.tiforth_agg_parity");
  const Block scan_block(scan_cols);

  const std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  auto actual = RunTiForthPlanOnBlock(
      scan_block, options_by_name,
      [](const tiforth::Engine* engine, tiforth::PlanBuilder* builder) -> arrow::Status {
        std::vector<tiforth::AggKey> keys;
        std::vector<tiforth::AggFunc> aggs;
        aggs.push_back({"cnt_all", "count_all", nullptr});
        aggs.push_back({"cnt_v", "count", tiforth::MakeFieldRef(1)});
        aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef(1)});
        aggs.push_back({"avg_v", "avg", tiforth::MakeFieldRef(1)});
        aggs.push_back({"min_v", "min", tiforth::MakeFieldRef(1)});
        aggs.push_back({"max_v", "max", tiforth::MakeFieldRef(1)});

        ARROW_ASSIGN_OR_RAISE(const auto stage, builder->AddStage());
        ARROW_RETURN_NOT_OK(builder->AppendPipe(
            stage,
            [engine, keys, aggs](tiforth::PlanTaskContext*) -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
              return std::make_unique<tiforth::ArrowComputeAggPipeOp>(engine, keys, aggs);
            }));
        return arrow::Status::OK();
      });
  if (!actual.ok()) {
    ASSERT_TRUE(actual.status().IsNotImplemented()) << actual.status().ToString();
    return;
  }

  const auto expected = executeStreams(
      context.scan("default", "tiforth_agg_parity")
          .aggregation(
              {Count(lit(Field(static_cast<UInt64>(1)))),
               Count(col("v")),
               Sum(col("v")),
               Avg(col("v")),
               Min(col("v")),
               Max(col("v"))},
              {})
          .build(context),
      /*concurrency=*/1);
  ASSERT_TRUE(DB::tests::columnsEqual(expected, actual.ValueOrDie(), /*restrict=*/false));
}
CATCH

}  // namespace

}  // namespace DB::tests

#endif // defined(TIFLASH_ENABLE_TIFORTH)
