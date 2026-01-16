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

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/filter.h"
#include "tiforth/operators/pass_through.h"
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

arrow::Status TranslateDagToTiForthPipeline(const PipelineExecBuilder& dag,
                                           tiforth::PipelineBuilder* builder) {
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
          [predicate]() -> arrow::Result<tiforth::TransformOpPtr> {
            return std::make_unique<tiforth::FilterTransformOp>(predicate);
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

arrow::Status RunTranslationSmoke() {
  PipelineExecutorContext exec_context;
  const String req_id = "tiforth_pipeline_translate";

  PipelineExecBuilder dag;
  dag.setSourceOp(std::make_unique<DummySourceOp>(exec_context, req_id));
  dag.appendTransformOp(std::make_unique<DummyTransformOp>(exec_context, req_id));
  dag.setSinkOp(std::make_unique<DummySinkOp>(exec_context, req_id));

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));
  ARROW_RETURN_NOT_OK(TranslateDagToTiForthPipeline(dag, builder.get()));
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
  ARROW_RETURN_NOT_OK(TranslateDagToTiForthPipeline(dag, builder.get()));
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

}  // namespace

TEST(TiForthPipelineTranslateTest, TiFlashDagToTiForth) {
  auto status = RunTranslationSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthPipelineTranslateTest, TiFlashDagWithFilterToTiForth) {
  auto status = RunFilterTranslationSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tests
}  // namespace DB

#else

TEST(TiForthPipelineTranslateTest, Disabled) {
  GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
