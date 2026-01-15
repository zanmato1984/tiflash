#include "tiforth/task.h"

#include <utility>

#include <arrow/status.h>
#include <arrow/type.h>

namespace tiforth {

arrow::Result<std::unique_ptr<Task>> Task::Create() {
  return std::unique_ptr<Task>(new Task());
}

Task::Task() = default;

Task::~Task() = default;

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

  if (input_queue_.empty() && !input_closed_ && input_reader_ != nullptr) {
    std::shared_ptr<arrow::RecordBatch> batch;
    ARROW_RETURN_NOT_OK(input_reader_->ReadNext(&batch));
    if (batch == nullptr) {
      input_closed_ = true;
    } else {
      ARROW_RETURN_NOT_OK(ValidateOrSetSchema(batch->schema()));
      input_queue_.push_back(std::move(batch));
    }
  }

  if (!input_queue_.empty()) {
    output_queue_.push_back(std::move(input_queue_.front()));
    input_queue_.pop_front();
    return TaskState::kHasOutput;
  }

  if (input_closed_) {
    return TaskState::kFinished;
  }
  return TaskState::kNeedInput;
}

}  // namespace tiforth
