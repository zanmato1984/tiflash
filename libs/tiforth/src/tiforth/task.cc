#include "tiforth/task.h"

#include <utility>

#include <arrow/status.h>
#include <arrow/type.h>

namespace tiforth {

arrow::Result<std::unique_ptr<Task>> Task::Create() {
  return Create(TransformOps{});
}

arrow::Result<std::unique_ptr<Task>> Task::Create(TransformOps transforms) {
  auto task = std::unique_ptr<Task>(new Task());
  ARROW_RETURN_NOT_OK(task->Init(std::move(transforms)));
  return task;
}

Task::Task() = default;

Task::~Task() = default;

class Task::InputSourceOp final : public SourceOp {
 public:
  explicit InputSourceOp(Task* task) : task_(task) {}

 protected:
  arrow::Result<OperatorStatus> ReadImpl(std::shared_ptr<arrow::RecordBatch>* batch) override {
    if (task_ == nullptr) {
      return arrow::Status::Invalid("task must not be null");
    }

    if (!task_->input_queue_.empty()) {
      *batch = std::move(task_->input_queue_.front());
      task_->input_queue_.pop_front();
      return OperatorStatus::kHasOutput;
    }

    if (!task_->input_closed_ && task_->input_reader_ != nullptr) {
      ARROW_RETURN_NOT_OK(task_->input_reader_->ReadNext(batch));
      if (*batch == nullptr) {
        task_->input_closed_ = true;
        return OperatorStatus::kFinished;
      }
      ARROW_RETURN_NOT_OK(task_->ValidateOrSetSchema((*batch)->schema()));
      return OperatorStatus::kHasOutput;
    }

    if (task_->input_closed_) {
      batch->reset();
      return OperatorStatus::kFinished;
    }

    batch->reset();
    return OperatorStatus::kNeedInput;
  }

 private:
  Task* task_;
};

class Task::OutputSinkOp final : public SinkOp {
 public:
  explicit OutputSinkOp(Task* task) : task_(task) {}

 protected:
  arrow::Result<OperatorStatus> WriteImpl(std::shared_ptr<arrow::RecordBatch> batch) override {
    if (task_ == nullptr) {
      return arrow::Status::Invalid("task must not be null");
    }

    if (batch == nullptr) {
      return OperatorStatus::kFinished;
    }

    task_->output_queue_.push_back(std::move(batch));
    return OperatorStatus::kNeedInput;
  }

 private:
  Task* task_;
};

arrow::Status Task::Init(TransformOps transforms) {
  PipelineExecBuilder builder;
  builder.SetSourceOp(std::make_unique<InputSourceOp>(this));
  for (auto& transform : transforms) {
    builder.AppendTransformOp(std::move(transform));
  }
  builder.SetSinkOp(std::make_unique<OutputSinkOp>(this));

  ARROW_ASSIGN_OR_RAISE(exec_, builder.Build());
  return arrow::Status::OK();
}

arrow::Status Task::ValidateOrSetSchema(const std::shared_ptr<arrow::Schema>& schema) {
  if (schema == nullptr) {
    return arrow::Status::Invalid("schema must not be null");
  }
  if (schema_ == nullptr) {
    schema_ = schema;
    return arrow::Status::OK();
  }
  if (!schema_->Equals(*schema, /*check_metadata=*/true)) {
    return arrow::Status::Invalid("schema mismatch");
  }
  return arrow::Status::OK();
}

arrow::Status Task::PushInput(std::shared_ptr<arrow::RecordBatch> batch) {
  if (batch == nullptr) {
    return arrow::Status::Invalid("batch must not be null");
  }
  if (input_reader_ != nullptr) {
    return arrow::Status::Invalid("PushInput cannot be used when an input reader is configured");
  }
  if (input_closed_) {
    return arrow::Status::Invalid("PushInput cannot be used after CloseInput");
  }
  ARROW_RETURN_NOT_OK(ValidateOrSetSchema(batch->schema()));
  input_queue_.push_back(std::move(batch));
  return arrow::Status::OK();
}

arrow::Status Task::CloseInput() {
  if (input_reader_ != nullptr) {
    return arrow::Status::Invalid("CloseInput cannot be used when an input reader is configured");
  }
  input_closed_ = true;
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> Task::PullOutput() {
  if (!output_queue_.empty()) {
    auto batch = std::move(output_queue_.front());
    output_queue_.pop_front();
    return batch;
  }
  if (input_closed_ && input_queue_.empty()) {
    return std::shared_ptr<arrow::RecordBatch>();
  }
  return arrow::Status::Invalid("no output is available");
}

arrow::Status Task::SetInputReader(std::shared_ptr<arrow::RecordBatchReader> reader) {
  if (reader == nullptr) {
    return arrow::Status::Invalid("input reader must not be null");
  }
  if (!input_queue_.empty()) {
    return arrow::Status::Invalid("input reader cannot be set after PushInput");
  }
  if (input_closed_) {
    return arrow::Status::Invalid("input reader cannot be set after CloseInput");
  }
  if (input_reader_ != nullptr) {
    return arrow::Status::Invalid("input reader is already set");
  }
  ARROW_RETURN_NOT_OK(ValidateOrSetSchema(reader->schema()));
  input_reader_ = std::move(reader);
  return arrow::Status::OK();
}

arrow::Result<TaskState> Task::Step() {
  if (!output_queue_.empty()) {
    return TaskState::kHasOutput;
  }

  if (exec_ == nullptr) {
    return arrow::Status::Invalid("task is not initialized");
  }

  ARROW_ASSIGN_OR_RAISE(const auto op_status, exec_->Execute());

  if (!output_queue_.empty()) {
    return TaskState::kHasOutput;
  }

  switch (op_status) {
    case OperatorStatus::kNeedInput:
      return TaskState::kNeedInput;
    case OperatorStatus::kFinished:
      return TaskState::kFinished;
    case OperatorStatus::kHasOutput:
      return arrow::Status::Invalid("unexpected operator status kHasOutput without output");
  }
  return arrow::Status::Invalid("unknown operator status");
}

}  // namespace tiforth
