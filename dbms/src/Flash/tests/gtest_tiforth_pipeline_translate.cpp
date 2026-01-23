#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Columns/ColumnVector.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Operators/Operator.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <set>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/arrow_compute_agg.h"
#include "tiforth/operators/filter.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/operators/hash_join.h"
#include "tiforth/operators/pass_through.h"
#include "tiforth/pipeline/op/op.h"
#include "tiforth/plan.h"
#include "tiforth/pipeline.h"
#include "tiforth/task.h"

namespace DB {
namespace tests {

namespace {

class DummySourceOp final : public SourceOp {
 public:
  explicit DummySourceOp(PipelineExecutorContext& exec_context, const String& req_id)
      : SourceOp(exec_context, req_id) {
    auto type = std::make_shared<DataTypeInt32>();
    ColumnsWithTypeAndName cols{ColumnWithTypeAndName(type, "x")};
    setHeader(Block(cols));
  }

  String getName() const override { return "DummySourceOp"; }

 protected:
  OperatorStatus readImpl(Block& block) override {
    // This operator is used only for constructing a TiFlash-shaped DAG in this test.
    (void)block;
    return OperatorStatus::HAS_OUTPUT;
  }
};

class DummyAggSourceOp final : public SourceOp {
 public:
  explicit DummyAggSourceOp(PipelineExecutorContext& exec_context, const String& req_id)
      : SourceOp(exec_context, req_id) {
    auto type = std::make_shared<DataTypeInt32>();
    ColumnsWithTypeAndName cols{ColumnWithTypeAndName(type, "k"), ColumnWithTypeAndName(type, "v")};
    setHeader(Block(cols));
  }

  String getName() const override { return "DummyAggSourceOp"; }

 protected:
  OperatorStatus readImpl(Block& block) override {
    (void)block;
    return OperatorStatus::HAS_OUTPUT;
  }
};

class DummyJoinSourceOp final : public SourceOp {
 public:
  explicit DummyJoinSourceOp(PipelineExecutorContext& exec_context, const String& req_id)
      : SourceOp(exec_context, req_id) {
    auto type = std::make_shared<DataTypeInt32>();
    ColumnsWithTypeAndName cols{ColumnWithTypeAndName(type, "k"), ColumnWithTypeAndName(type, "pv")};
    setHeader(Block(cols));
  }

  String getName() const override { return "DummyJoinSourceOp"; }

 protected:
  OperatorStatus readImpl(Block& block) override {
    (void)block;
    return OperatorStatus::HAS_OUTPUT;
  }
};

class DummyTransformOp final : public TransformOp {
 public:
  explicit DummyTransformOp(PipelineExecutorContext& exec_context, const String& req_id)
      : TransformOp(exec_context, req_id) {}

  String getName() const override { return "DummyTransformOp"; }

 protected:
  OperatorStatus transformImpl(Block& block) override {
    // No-op transform.
    return block ? OperatorStatus::HAS_OUTPUT : OperatorStatus::NEED_INPUT;
  }

  void transformHeaderImpl(Block& header) override { (void)header; }
};

class DummyFilterTransformOp final : public TransformOp {
 public:
  explicit DummyFilterTransformOp(PipelineExecutorContext& exec_context, const String& req_id)
      : TransformOp(exec_context, req_id) {}

  String getName() const override { return "DummyFilterTransformOp"; }

 protected:
  OperatorStatus transformImpl(Block& block) override {
    // This operator is used only for constructing a TiFlash-shaped DAG in this test.
    // TiForth performs the real filtering after translation.
    return block ? OperatorStatus::HAS_OUTPUT : OperatorStatus::NEED_INPUT;
  }

  void transformHeaderImpl(Block& header) override { (void)header; }
};

class DummyHashAggTransformOp final : public TransformOp {
 public:
  explicit DummyHashAggTransformOp(PipelineExecutorContext& exec_context, const String& req_id)
      : TransformOp(exec_context, req_id) {}

  String getName() const override { return "DummyHashAggTransformOp"; }

 protected:
  OperatorStatus transformImpl(Block& block) override {
    // This operator is used only for constructing a TiFlash-shaped DAG in this test.
    // TiForth performs the real aggregation after translation.
    return block ? OperatorStatus::HAS_OUTPUT : OperatorStatus::NEED_INPUT;
  }

  void transformHeaderImpl(Block& header) override { (void)header; }
};

class DummyHashJoinTransformOp final : public TransformOp {
 public:
  explicit DummyHashJoinTransformOp(PipelineExecutorContext& exec_context, const String& req_id)
      : TransformOp(exec_context, req_id) {}

  String getName() const override { return "DummyHashJoinTransformOp"; }

 protected:
  OperatorStatus transformImpl(Block& block) override {
    // This operator is used only for constructing a TiFlash-shaped DAG in this test.
    // TiForth performs the real join after translation.
    return block ? OperatorStatus::HAS_OUTPUT : OperatorStatus::NEED_INPUT;
  }

  void transformHeaderImpl(Block& header) override { (void)header; }
};

class DummySinkOp final : public SinkOp {
 public:
  explicit DummySinkOp(PipelineExecutorContext& exec_context, const String& req_id)
      : SinkOp(exec_context, req_id) {}

  String getName() const override { return "DummySinkOp"; }

 protected:
  OperatorStatus writeImpl(Block&& block) override {
    (void)block;
    return OperatorStatus::NEED_INPUT;
  }
};

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBatch() {
  auto schema = arrow::schema({arrow::field("x", arrow::int32())});

  arrow::Int32Builder builder;
  ARROW_RETURN_NOT_OK(builder.AppendValues({1, 2, 3}));
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));

  return arrow::RecordBatch::Make(schema, /*num_rows=*/3, {array});
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeAggBatch0() {
  auto schema = arrow::schema({arrow::field("k", arrow::int32()), arrow::field("v", arrow::int32())});

  arrow::Int32Builder k_builder;
  ARROW_RETURN_NOT_OK(k_builder.Append(1));
  ARROW_RETURN_NOT_OK(k_builder.Append(2));
  ARROW_RETURN_NOT_OK(k_builder.Append(1));
  ARROW_RETURN_NOT_OK(k_builder.AppendNull());
  std::shared_ptr<arrow::Array> k_array;
  ARROW_RETURN_NOT_OK(k_builder.Finish(&k_array));

  arrow::Int32Builder v_builder;
  ARROW_RETURN_NOT_OK(v_builder.Append(10));
  ARROW_RETURN_NOT_OK(v_builder.Append(20));
  ARROW_RETURN_NOT_OK(v_builder.AppendNull());
  ARROW_RETURN_NOT_OK(v_builder.Append(7));
  std::shared_ptr<arrow::Array> v_array;
  ARROW_RETURN_NOT_OK(v_builder.Finish(&v_array));

  return arrow::RecordBatch::Make(schema, /*num_rows=*/4, {k_array, v_array});
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeAggBatch1() {
  auto schema = arrow::schema({arrow::field("k", arrow::int32()), arrow::field("v", arrow::int32())});

  arrow::Int32Builder k_builder;
  ARROW_RETURN_NOT_OK(k_builder.Append(2));
  ARROW_RETURN_NOT_OK(k_builder.Append(3));
  ARROW_RETURN_NOT_OK(k_builder.AppendNull());
  ARROW_RETURN_NOT_OK(k_builder.Append(4));
  std::shared_ptr<arrow::Array> k_array;
  ARROW_RETURN_NOT_OK(k_builder.Finish(&k_array));

  arrow::Int32Builder v_builder;
  ARROW_RETURN_NOT_OK(v_builder.Append(1));
  ARROW_RETURN_NOT_OK(v_builder.Append(5));
  ARROW_RETURN_NOT_OK(v_builder.AppendNull());
  ARROW_RETURN_NOT_OK(v_builder.AppendNull());
  std::shared_ptr<arrow::Array> v_array;
  ARROW_RETURN_NOT_OK(v_builder.Finish(&v_array));

  return arrow::RecordBatch::Make(schema, /*num_rows=*/4, {k_array, v_array});
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeJoinProbeBatch() {
  auto schema = arrow::schema({arrow::field("k", arrow::int32()), arrow::field("pv", arrow::int32())});

  arrow::Int32Builder k_builder;
  ARROW_RETURN_NOT_OK(k_builder.Append(2));
  ARROW_RETURN_NOT_OK(k_builder.Append(1));
  ARROW_RETURN_NOT_OK(k_builder.Append(3));
  ARROW_RETURN_NOT_OK(k_builder.AppendNull());
  std::shared_ptr<arrow::Array> k_array;
  ARROW_RETURN_NOT_OK(k_builder.Finish(&k_array));

  arrow::Int32Builder pv_builder;
  ARROW_RETURN_NOT_OK(pv_builder.Append(20));
  ARROW_RETURN_NOT_OK(pv_builder.Append(10));
  ARROW_RETURN_NOT_OK(pv_builder.Append(30));
  ARROW_RETURN_NOT_OK(pv_builder.Append(0));
  std::shared_ptr<arrow::Array> pv_array;
  ARROW_RETURN_NOT_OK(pv_builder.Finish(&pv_array));

  return arrow::RecordBatch::Make(schema, /*num_rows=*/4, {k_array, pv_array});
}

arrow::Result<std::multiset<std::vector<std::string>>> RecordBatchRowSet(const arrow::RecordBatch& batch) {
  std::multiset<std::vector<std::string>> rows;
  for (int64_t row = 0; row < batch.num_rows(); ++row) {
    std::vector<std::string> values;
    values.reserve(batch.num_columns());
    for (int c = 0; c < batch.num_columns(); ++c) {
      ARROW_ASSIGN_OR_RAISE(auto scalar, batch.column(c)->GetScalar(row));
      values.push_back(scalar->ToString());
    }
    rows.insert(std::move(values));
  }
  return rows;
}

arrow::Status TranslateDagToTiForthPipeline(const PipelineExecBuilder& dag, const tiforth::Engine* engine,
                                           tiforth::PipelineBuilder* builder,
                                           bool use_arrow_compute_agg = false) {
  if (engine == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  if (builder == nullptr) {
    return arrow::Status::Invalid("builder must not be null");
  }

  // Common path: single source -> N transforms -> single sink.
  // Only translate the simplest cases needed by tests.
  for (const auto& transform : dag.transform_ops) {
    if (dynamic_cast<const DummyFilterTransformOp*>(transform.get()) != nullptr) {
      auto predicate = tiforth::MakeCall(
          "greater", {tiforth::MakeFieldRef("x"),
                      tiforth::MakeLiteral(std::make_shared<arrow::Int32Scalar>(1))});
      ARROW_RETURN_NOT_OK(builder->AppendTransform(
          [engine, predicate]() -> arrow::Result<tiforth::TransformOpPtr> {
            return std::make_unique<tiforth::FilterTransformOp>(engine, predicate);
          }));
      continue;
    }
    if (dynamic_cast<const DummyHashAggTransformOp*>(transform.get()) != nullptr) {
      (void)use_arrow_compute_agg;
      return arrow::Status::NotImplemented("aggregation translation requires PlanBuilder");
    }
    if (dynamic_cast<const DummyHashJoinTransformOp*>(transform.get()) != nullptr) {
      auto build_schema =
          arrow::schema({arrow::field("k", arrow::int32()), arrow::field("bv", arrow::int32())});
      arrow::Int32Builder k_builder;
      ARROW_RETURN_NOT_OK(k_builder.Append(1));
      ARROW_RETURN_NOT_OK(k_builder.Append(2));
      ARROW_RETURN_NOT_OK(k_builder.Append(2));
      ARROW_RETURN_NOT_OK(k_builder.AppendNull());
      std::shared_ptr<arrow::Array> k_array;
      ARROW_RETURN_NOT_OK(k_builder.Finish(&k_array));

      arrow::Int32Builder bv_builder;
      ARROW_RETURN_NOT_OK(bv_builder.Append(100));
      ARROW_RETURN_NOT_OK(bv_builder.Append(200));
      ARROW_RETURN_NOT_OK(bv_builder.Append(201));
      ARROW_RETURN_NOT_OK(bv_builder.Append(999));
      std::shared_ptr<arrow::Array> bv_array;
      ARROW_RETURN_NOT_OK(bv_builder.Finish(&bv_array));

      std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches;
      build_batches.push_back(arrow::RecordBatch::Make(build_schema, /*num_rows=*/4, {k_array, bv_array}));

      tiforth::JoinKey key{.left = {"k"}, .right = {"k"}};
      ARROW_RETURN_NOT_OK(builder->AppendTransform(
          [engine, build_batches, key]() -> arrow::Result<tiforth::TransformOpPtr> {
            return std::make_unique<tiforth::HashJoinTransformOp>(engine, build_batches, key);
          }));
      continue;
    }

    ARROW_RETURN_NOT_OK(builder->AppendTransform(
        []() -> arrow::Result<tiforth::TransformOpPtr> {
          return std::make_unique<tiforth::PassThroughTransformOp>();
        }));
  }
  return arrow::Status::OK();
}

arrow::Status TranslateDagToTiForthAggBreakerPlan(const PipelineExecBuilder& dag, const tiforth::Engine* engine,
                                                  tiforth::PlanBuilder* builder) {
  if (engine == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  if (builder == nullptr) {
    return arrow::Status::Invalid("builder must not be null");
  }

  bool has_hash_agg = false;
  for (const auto& transform : dag.transform_ops) {
    if (dynamic_cast<const DummyHashAggTransformOp*>(transform.get()) != nullptr) {
      has_hash_agg = true;
      break;
    }
  }
  if (!has_hash_agg) {
    return arrow::Status::Invalid("expected a hash agg transform op in the DAG");
  }

  // Minimal translation for the unit test: build a breaker plan (partial transform + merge sink +
  // result source) using a shared HashAggContext.
  std::vector<tiforth::AggKey> keys = {{"k", tiforth::MakeFieldRef("k")}};
  std::vector<tiforth::AggFunc> aggs;
  aggs.push_back({"cnt", "count_all", nullptr});
  aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef("v")});

  ARROW_ASSIGN_OR_RAISE(const auto ctx_id,
                        builder->AddBreakerState<tiforth::HashAggContext>(
                            [engine, keys, aggs]()
                                -> arrow::Result<std::shared_ptr<tiforth::HashAggContext>> {
                              return std::make_shared<tiforth::HashAggContext>(engine, keys, aggs);
                            }));

  ARROW_ASSIGN_OR_RAISE(const auto build_stage, builder->AddStage());
  ARROW_RETURN_NOT_OK(builder->AppendPipe(
      build_stage,
      [ctx_id](tiforth::PlanTaskContext* ctx)
          -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
        ARROW_ASSIGN_OR_RAISE(auto agg_ctx,
                              ctx->GetBreakerState<tiforth::HashAggContext>(ctx_id));
        return std::make_unique<tiforth::HashAggTransformOp>(std::move(agg_ctx));
      }));
  ARROW_RETURN_NOT_OK(builder->SetStageSink(
      build_stage,
      [ctx_id](tiforth::PlanTaskContext* ctx)
          -> arrow::Result<std::unique_ptr<tiforth::pipeline::SinkOp>> {
        ARROW_ASSIGN_OR_RAISE(auto agg_ctx,
                              ctx->GetBreakerState<tiforth::HashAggContext>(ctx_id));
        return std::make_unique<tiforth::HashAggMergeSinkOp>(std::move(agg_ctx));
      }));

  ARROW_ASSIGN_OR_RAISE(const auto convergent_stage, builder->AddStage());
  ARROW_RETURN_NOT_OK(builder->SetStageSource(
      convergent_stage,
      [ctx_id](tiforth::PlanTaskContext* ctx)
          -> arrow::Result<std::unique_ptr<tiforth::pipeline::SourceOp>> {
        ARROW_ASSIGN_OR_RAISE(auto agg_ctx,
                              ctx->GetBreakerState<tiforth::HashAggContext>(ctx_id));
        return std::make_unique<tiforth::HashAggResultSourceOp>(std::move(agg_ctx),
                                                                /*max_output_rows=*/1 << 30);
      }));
  ARROW_RETURN_NOT_OK(builder->AddDependency(build_stage, convergent_stage));

  return arrow::Status::OK();
}

arrow::Status TranslateDagToTiForthArrowComputeAggPlan(const PipelineExecBuilder& dag,
                                                       const tiforth::Engine* engine,
                                                       tiforth::PlanBuilder* builder) {
  if (engine == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  if (builder == nullptr) {
    return arrow::Status::Invalid("builder must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(const auto stage, builder->AddStage());

  for (const auto& transform : dag.transform_ops) {
    if (dynamic_cast<const DummyFilterTransformOp*>(transform.get()) != nullptr) {
      auto predicate = tiforth::MakeCall(
          "greater", {tiforth::MakeFieldRef("x"),
                      tiforth::MakeLiteral(std::make_shared<arrow::Int32Scalar>(1))});
      ARROW_RETURN_NOT_OK(builder->AppendTransform(
          stage,
          [engine, predicate](tiforth::PlanTaskContext*) -> arrow::Result<tiforth::TransformOpPtr> {
            return std::make_unique<tiforth::FilterTransformOp>(engine, predicate);
          }));
      continue;
    }

    if (dynamic_cast<const DummyHashAggTransformOp*>(transform.get()) != nullptr) {
      std::vector<tiforth::AggKey> keys = {{"k", tiforth::MakeFieldRef("k")}};
      std::vector<tiforth::AggFunc> aggs;
      aggs.push_back({"cnt", "count_all", nullptr});
      aggs.push_back({"sum_v", "sum", tiforth::MakeFieldRef("v")});
      ARROW_RETURN_NOT_OK(builder->AppendPipe(
          stage,
          [engine, keys, aggs](tiforth::PlanTaskContext*)
              -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
            return std::make_unique<tiforth::ArrowComputeAggTransformOp>(engine, keys, aggs);
          }));
      continue;
    }

    return arrow::Status::NotImplemented("unsupported TiFlash DAG transform in tiforth plan mode");
  }

  return arrow::Status::OK();
}

arrow::Status RunHashAggTransformTranslationSmoke(bool use_arrow_compute_agg) {
  PipelineExecutorContext exec_context;
  const String req_id = "tiforth_pipeline_translate_hashagg_transform";

  PipelineExecBuilder dag;
  dag.setSourceOp(std::make_unique<DummyAggSourceOp>(exec_context, req_id));
  dag.appendTransformOp(std::make_unique<DummyHashAggTransformOp>(exec_context, req_id));
  dag.setSinkOp(std::make_unique<DummySinkOp>(exec_context, req_id));

  if (!use_arrow_compute_agg) {
    return arrow::Status::NotImplemented("arrow compute agg translation is disabled");
  }

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PlanBuilder::Create(engine.get()));
  ARROW_RETURN_NOT_OK(TranslateDagToTiForthArrowComputeAggPlan(dag, engine.get(), builder.get()));
  ARROW_ASSIGN_OR_RAISE(auto plan, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, plan->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != tiforth::TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeAggBatch0());
  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeAggBatch1());
  ARROW_RETURN_NOT_OK(task->PushInput(batch1));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
    if (state == tiforth::TaskState::kFinished) {
      break;
    }
    if (state == tiforth::TaskState::kNeedInput) {
      continue;
    }
    if (state != tiforth::TaskState::kHasOutput) {
      return arrow::Status::Invalid("unexpected task state");
    }

    ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }
    if (out->num_columns() != 3) {
      return arrow::Status::Invalid("unexpected output schema");
    }
    if (out->schema()->field(0)->name() != "cnt" || out->schema()->field(1)->name() != "sum_v" ||
        out->schema()->field(2)->name() != "k") {
      return arrow::Status::Invalid("unexpected output schema");
    }
    outputs.push_back(std::move(out));
  }

  std::multiset<std::vector<std::string>> row_set;
  for (const auto& batch : outputs) {
    ARROW_ASSIGN_OR_RAISE(auto rows, RecordBatchRowSet(*batch));
    row_set.insert(rows.begin(), rows.end());
  }

  std::multiset<std::vector<std::string>> expected;
  expected.insert({"2", "10", "1"});
  expected.insert({"2", "21", "2"});
  expected.insert({"2", "7", "null"});
  expected.insert({"1", "5", "3"});
  expected.insert({"1", "null", "4"});

  if (row_set != expected) {
    return arrow::Status::Invalid("unexpected output values");
  }

  return arrow::Status::OK();
}

arrow::Status RunTranslationSmoke() {
  PipelineExecutorContext exec_context;
  const String req_id = "tiforth_pipeline_translate";

  PipelineExecBuilder dag;
  dag.setSourceOp(std::make_unique<DummySourceOp>(exec_context, req_id));
  dag.appendTransformOp(std::make_unique<DummyTransformOp>(exec_context, req_id));
  dag.setSinkOp(std::make_unique<DummySinkOp>(exec_context, req_id));

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));
  ARROW_RETURN_NOT_OK(TranslateDagToTiForthPipeline(dag, engine.get(), builder.get()));
  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != tiforth::TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto input, MakeBatch());
  ARROW_RETURN_NOT_OK(task->PushInput(input));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != tiforth::TaskState::kHasOutput) {
    return arrow::Status::Invalid("expected TaskState::kHasOutput");
  }

  ARROW_ASSIGN_OR_RAISE(auto output, task->PullOutput());
  if (output.get() != input.get()) {
    return arrow::Status::Invalid("expected pass-through RecordBatch");
  }

  ARROW_ASSIGN_OR_RAISE(auto final_state, task->Step());
  if (final_state != tiforth::TaskState::kFinished) {
    return arrow::Status::Invalid("expected TaskState::kFinished");
  }

  return arrow::Status::OK();
}

arrow::Status RunFilterTranslationSmoke() {
  PipelineExecutorContext exec_context;
  const String req_id = "tiforth_pipeline_translate_filter";

  PipelineExecBuilder dag;
  dag.setSourceOp(std::make_unique<DummySourceOp>(exec_context, req_id));
  dag.appendTransformOp(std::make_unique<DummyFilterTransformOp>(exec_context, req_id));
  dag.setSinkOp(std::make_unique<DummySinkOp>(exec_context, req_id));

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));
  ARROW_RETURN_NOT_OK(TranslateDagToTiForthPipeline(dag, engine.get(), builder.get()));
  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != tiforth::TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto input, MakeBatch());
  ARROW_RETURN_NOT_OK(task->PushInput(input));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != tiforth::TaskState::kHasOutput) {
    return arrow::Status::Invalid("expected TaskState::kHasOutput");
  }

  ARROW_ASSIGN_OR_RAISE(auto output, task->PullOutput());
  if (output == nullptr) {
    return arrow::Status::Invalid("expected non-null output");
  }
  if (output->num_columns() != 1 || output->num_rows() != 2) {
    return arrow::Status::Invalid("unexpected filtered output shape");
  }

  arrow::Int32Builder builder_x;
  ARROW_RETURN_NOT_OK(builder_x.AppendValues({2, 3}));
  std::shared_ptr<arrow::Array> expect_x;
  ARROW_RETURN_NOT_OK(builder_x.Finish(&expect_x));
  if (!expect_x->Equals(*output->column(0))) {
    return arrow::Status::Invalid("unexpected filtered output values");
  }

  ARROW_ASSIGN_OR_RAISE(auto final_state, task->Step());
  if (final_state != tiforth::TaskState::kFinished) {
    return arrow::Status::Invalid("expected TaskState::kFinished");
  }

  return arrow::Status::OK();
}

arrow::Status RunHashAggTranslationSmoke() {
  PipelineExecutorContext exec_context;
  const String req_id = "tiforth_pipeline_translate_hashagg";

  PipelineExecBuilder dag;
  dag.setSourceOp(std::make_unique<DummyAggSourceOp>(exec_context, req_id));
  dag.appendTransformOp(std::make_unique<DummyHashAggTransformOp>(exec_context, req_id));
  dag.setSinkOp(std::make_unique<DummySinkOp>(exec_context, req_id));

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PlanBuilder::Create(engine.get()));
  ARROW_RETURN_NOT_OK(TranslateDagToTiForthAggBreakerPlan(dag, engine.get(), builder.get()));
  ARROW_ASSIGN_OR_RAISE(auto plan, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, plan->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != tiforth::TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeAggBatch0());
  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeAggBatch1());
  ARROW_RETURN_NOT_OK(task->PushInput(batch1));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
    if (state == tiforth::TaskState::kFinished) {
      break;
    }
    if (state == tiforth::TaskState::kNeedInput) {
      continue;
    }
    if (state != tiforth::TaskState::kHasOutput) {
      return arrow::Status::Invalid("unexpected task state");
    }

    ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }
    outputs.push_back(std::move(out));
  }

  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output batch");
  }

  const auto& out = outputs[0];
  if (out->num_columns() != 3 || out->num_rows() != 5) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "cnt" || out->schema()->field(1)->name() != "sum_v" ||
      out->schema()->field(2)->name() != "k") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  arrow::Int32Builder k_expect_builder;
  ARROW_RETURN_NOT_OK(k_expect_builder.Append(1));
  ARROW_RETURN_NOT_OK(k_expect_builder.Append(2));
  ARROW_RETURN_NOT_OK(k_expect_builder.AppendNull());
  ARROW_RETURN_NOT_OK(k_expect_builder.Append(3));
  ARROW_RETURN_NOT_OK(k_expect_builder.Append(4));
  std::shared_ptr<arrow::Array> k_expect;
  ARROW_RETURN_NOT_OK(k_expect_builder.Finish(&k_expect));

  arrow::UInt64Builder cnt_expect_builder;
  ARROW_RETURN_NOT_OK(cnt_expect_builder.AppendValues({2, 2, 2, 1, 1}));
  std::shared_ptr<arrow::Array> cnt_expect;
  ARROW_RETURN_NOT_OK(cnt_expect_builder.Finish(&cnt_expect));

  arrow::Int64Builder sum_expect_builder;
  ARROW_RETURN_NOT_OK(sum_expect_builder.Append(10));
  ARROW_RETURN_NOT_OK(sum_expect_builder.Append(21));
  ARROW_RETURN_NOT_OK(sum_expect_builder.Append(7));
  ARROW_RETURN_NOT_OK(sum_expect_builder.Append(5));
  ARROW_RETURN_NOT_OK(sum_expect_builder.AppendNull());
  std::shared_ptr<arrow::Array> sum_expect;
  ARROW_RETURN_NOT_OK(sum_expect_builder.Finish(&sum_expect));

  if (!cnt_expect->Equals(*out->column(0)) || !sum_expect->Equals(*out->column(1)) ||
      !k_expect->Equals(*out->column(2))) {
    return arrow::Status::Invalid("unexpected output values");
  }

  return arrow::Status::OK();
}

arrow::Status RunHashJoinTranslationSmoke() {
  PipelineExecutorContext exec_context;
  const String req_id = "tiforth_pipeline_translate_hashjoin";

  PipelineExecBuilder dag;
  dag.setSourceOp(std::make_unique<DummyJoinSourceOp>(exec_context, req_id));
  dag.appendTransformOp(std::make_unique<DummyHashJoinTransformOp>(exec_context, req_id));
  dag.setSinkOp(std::make_unique<DummySinkOp>(exec_context, req_id));

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));
  ARROW_RETURN_NOT_OK(TranslateDagToTiForthPipeline(dag, engine.get(), builder.get()));
  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != tiforth::TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto probe, MakeJoinProbeBatch());
  ARROW_RETURN_NOT_OK(task->PushInput(probe));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != tiforth::TaskState::kHasOutput) {
    return arrow::Status::Invalid("expected TaskState::kHasOutput");
  }

  ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
  if (out == nullptr) {
    return arrow::Status::Invalid("expected non-null output batch");
  }
  if (out->num_columns() != 4 || out->num_rows() != 3) {
    return arrow::Status::Invalid("unexpected join output shape");
  }

  arrow::Int32Builder probe_k_expect_builder;
  ARROW_RETURN_NOT_OK(probe_k_expect_builder.AppendValues({2, 2, 1}));
  std::shared_ptr<arrow::Array> probe_k_expect;
  ARROW_RETURN_NOT_OK(probe_k_expect_builder.Finish(&probe_k_expect));

  arrow::Int32Builder probe_pv_expect_builder;
  ARROW_RETURN_NOT_OK(probe_pv_expect_builder.AppendValues({20, 20, 10}));
  std::shared_ptr<arrow::Array> probe_pv_expect;
  ARROW_RETURN_NOT_OK(probe_pv_expect_builder.Finish(&probe_pv_expect));

  arrow::Int32Builder build_k_expect_builder;
  ARROW_RETURN_NOT_OK(build_k_expect_builder.AppendValues({2, 2, 1}));
  std::shared_ptr<arrow::Array> build_k_expect;
  ARROW_RETURN_NOT_OK(build_k_expect_builder.Finish(&build_k_expect));

  arrow::Int32Builder build_bv_expect_builder;
  ARROW_RETURN_NOT_OK(build_bv_expect_builder.AppendValues({200, 201, 100}));
  std::shared_ptr<arrow::Array> build_bv_expect;
  ARROW_RETURN_NOT_OK(build_bv_expect_builder.Finish(&build_bv_expect));

  if (!probe_k_expect->Equals(*out->column(0)) || !probe_pv_expect->Equals(*out->column(1)) ||
      !build_k_expect->Equals(*out->column(2)) || !build_bv_expect->Equals(*out->column(3))) {
    return arrow::Status::Invalid("unexpected join output values");
  }

  ARROW_ASSIGN_OR_RAISE(auto final_state, task->Step());
  if (final_state != tiforth::TaskState::kFinished) {
    return arrow::Status::Invalid("expected TaskState::kFinished");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthPipelineTranslateTest, TiFlashDagToTiForth) {
  auto status = RunTranslationSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthPipelineTranslateTest, TiFlashDagWithFilterToTiForth) {
  auto status = RunFilterTranslationSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthPipelineTranslateTest, TiFlashDagWithHashAggToTiForth) {
  auto status = RunHashAggTranslationSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthPipelineTranslateTest, TiFlashDagWithArrowComputeAggTransformToTiForth) {
  auto status = RunHashAggTransformTranslationSmoke(/*use_arrow_compute_agg=*/true);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthPipelineTranslateTest, TiFlashDagWithHashJoinToTiForth) {
  auto status = RunHashJoinTranslationSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tests
}  // namespace DB

#else

TEST(TiForthPipelineTranslateTest, Disabled) {
  GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
