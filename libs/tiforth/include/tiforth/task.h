#pragma once

#include <deque>
#include <memory>

#include <arrow/result.h>
#include <arrow/record_batch.h>

#include "tiforth/operators.h"
#include "tiforth/pipeline_exec.h"

namespace tiforth {

enum class TaskState {
  kNeedInput,
  kHasOutput,
  kFinished,
  kBlocked,
};

class Task {
 public:
  static arrow::Result<std::unique_ptr<Task>> Create();
  static arrow::Result<std::unique_ptr<Task>> Create(TransformOps transforms);

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  ~Task();

  arrow::Status PushInput(std::shared_ptr<arrow::RecordBatch> batch);
  arrow::Status CloseInput();
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> PullOutput();

  arrow::Status SetInputReader(std::shared_ptr<arrow::RecordBatchReader> reader);

  arrow::Result<TaskState> Step();

 private:
  Task();

  class InputSourceOp;
  class OutputSinkOp;

  arrow::Status Init(TransformOps transforms);
  arrow::Status ValidateOrSetSchema(const std::shared_ptr<arrow::Schema>& schema);

  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::RecordBatchReader> input_reader_;
  std::deque<std::shared_ptr<arrow::RecordBatch>> input_queue_;
  std::deque<std::shared_ptr<arrow::RecordBatch>> output_queue_;
  bool input_closed_ = false;

  std::unique_ptr<PipelineExec> exec_;
};

}  // namespace tiforth
