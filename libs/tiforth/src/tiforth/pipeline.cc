#include "tiforth/pipeline.h"

#include <utility>

#include <arrow/record_batch.h>
#include <arrow/status.h>

namespace tiforth {

namespace {

class TaskRecordBatchReader final : public arrow::RecordBatchReader {
 public:
  TaskRecordBatchReader(std::shared_ptr<arrow::Schema> schema, std::shared_ptr<Task> task,
                        std::shared_ptr<arrow::RecordBatchReader> input)
      : schema_(std::move(schema)), task_(std::move(task)), input_(std::move(input)) {}

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    if (batch == nullptr) {
      return arrow::Status::Invalid("batch output must not be null");
    }

    while (true) {
      ARROW_ASSIGN_OR_RAISE(auto state, task_->Step());
      switch (state) {
        case TaskState::kHasOutput: {
          ARROW_ASSIGN_OR_RAISE(*batch, task_->PullOutput());
          return arrow::Status::OK();
        }
        case TaskState::kFinished:
          *batch = nullptr;
          return arrow::Status::OK();
        case TaskState::kNeedInput:
          return arrow::Status::Invalid(
              "unexpected TaskState::kNeedInput when input reader is configured");
        case TaskState::kBlocked:
          return arrow::Status::NotImplemented("blocked tasks are not implemented");
      }
    }
  }

  arrow::Status Close() override {
    if (input_ == nullptr) {
      return arrow::Status::OK();
    }
    return input_->Close();
  }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<Task> task_;
  std::shared_ptr<arrow::RecordBatchReader> input_;
};

}  // namespace

arrow::Result<std::unique_ptr<PipelineBuilder>> PipelineBuilder::Create(const Engine* engine) {
  if (engine == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  return std::unique_ptr<PipelineBuilder>(new PipelineBuilder(engine));
}

PipelineBuilder::PipelineBuilder(const Engine* engine) : engine_(engine) {}

PipelineBuilder::~PipelineBuilder() = default;

arrow::Result<std::unique_ptr<Pipeline>> PipelineBuilder::Finalize() {
  return std::unique_ptr<Pipeline>(new Pipeline(engine_));
}

Pipeline::Pipeline(const Engine* engine) : engine_(engine) {}

arrow::Result<std::unique_ptr<Task>> Pipeline::CreateTask() const {
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("engine must not be null");
  }
  return Task::Create();
}

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> Pipeline::MakeReader(
    std::shared_ptr<arrow::RecordBatchReader> input) const {
  if (input == nullptr) {
    return arrow::Status::Invalid("input must not be null");
  }
  if (input->schema() == nullptr) {
    return arrow::Status::Invalid("input schema must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(auto task_unique, CreateTask());
  ARROW_RETURN_NOT_OK(task_unique->SetInputReader(input));
  std::shared_ptr<Task> task = std::move(task_unique);

  return std::make_shared<TaskRecordBatchReader>(input->schema(), std::move(task), input);
}

}  // namespace tiforth
