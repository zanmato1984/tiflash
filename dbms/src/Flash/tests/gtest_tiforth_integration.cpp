#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/result.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>

#include <Flash/TiForth/TaskGroupRunner.h>

#include "tiforth/engine.h"
#include "tiforth/pipeline/logical_pipeline.h"
#include "tiforth/pipeline/op/op.h"
#include "tiforth/pipeline/task_groups.h"

namespace {

class VectorSourceOp final : public tiforth::pipeline::SourceOp {
 public:
  explicit VectorSourceOp(std::vector<std::shared_ptr<arrow::RecordBatch>> batches)
      : batches_(std::move(batches)) {}

  tiforth::pipeline::PipelineSource Source(const tiforth::pipeline::PipelineContext&) override {
    return [this](const tiforth::pipeline::PipelineContext&, const tiforth::task::TaskContext&,
                  tiforth::pipeline::ThreadId thread_id) -> tiforth::pipeline::OpResult {
      if (thread_id != 0) {
        return arrow::Status::Invalid("VectorSourceOp only supports thread_id=0");
      }
      if (next_ >= batches_.size()) {
        return tiforth::pipeline::OpOutput::Finished();
      }
      auto batch = batches_[next_++];
      if (batch == nullptr) {
        return arrow::Status::Invalid("source batch must not be null");
      }
      return tiforth::pipeline::OpOutput::SourcePipeHasMore(std::move(batch));
    };
  }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
  std::size_t next_ = 0;
};

class CollectSinkOp final : public tiforth::pipeline::SinkOp {
 public:
  explicit CollectSinkOp(std::vector<std::shared_ptr<arrow::RecordBatch>>* outputs)
      : outputs_(outputs) {}

  tiforth::pipeline::PipelineSink Sink(const tiforth::pipeline::PipelineContext&) override {
    return [this](const tiforth::pipeline::PipelineContext&, const tiforth::task::TaskContext&,
                  tiforth::pipeline::ThreadId thread_id,
                  std::optional<tiforth::pipeline::Batch> input) -> tiforth::pipeline::OpResult {
      if (thread_id != 0) {
        return arrow::Status::Invalid("CollectSinkOp only supports thread_id=0");
      }
      if (outputs_ == nullptr) {
        return arrow::Status::Invalid("outputs must not be null");
      }
      if (!input.has_value()) {
        return tiforth::pipeline::OpOutput::PipeSinkNeedsMore();
      }
      auto batch = std::move(*input);
      if (batch != nullptr) {
        outputs_->push_back(std::move(batch));
      }
      return tiforth::pipeline::OpOutput::PipeSinkNeedsMore();
    };
  }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>>* outputs_ = nullptr;
};

arrow::Status RunTiForthSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));

  auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  arrow::Int32Builder values;
  ARROW_RETURN_NOT_OK(values.AppendValues({1, 2, 3}));
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(values.Finish(&array));
  auto batch = arrow::RecordBatch::Make(schema, /*num_rows=*/3, {array});

  auto source_op = std::make_unique<VectorSourceOp>(std::vector<std::shared_ptr<arrow::RecordBatch>>{batch});
  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  auto sink_op = std::make_unique<CollectSinkOp>(&outputs);

  tiforth::pipeline::LogicalPipeline::Channel channel;
  channel.source_op = source_op.get();
  channel.pipe_ops = {};

  tiforth::pipeline::LogicalPipeline logical_pipeline{"Smoke", {std::move(channel)}, sink_op.get()};
  ARROW_ASSIGN_OR_RAISE(auto task_groups,
                        tiforth::pipeline::CompileToTaskGroups(tiforth::pipeline::PipelineContext{},
                                                              logical_pipeline, /*dop=*/1));
  const auto task_ctx = DB::TiForth::MakeTaskContext();
  ARROW_RETURN_NOT_OK(DB::TiForth::RunTaskGroupsToCompletion(task_groups, task_ctx));

  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output batch");
  }
  if (outputs[0].get() != batch.get()) {
    return arrow::Status::Invalid("expected pass-through output batch");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthIntegrationSmokeTest, Lifecycle) {
  auto status = RunTiForthSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

#else

TEST(TiForthIntegrationSmokeTest, Disabled) {
  GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
