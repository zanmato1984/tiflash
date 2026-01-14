#pragma once

#include <memory>

#include <arrow/result.h>

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

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  ~Task();

  arrow::Result<TaskState> Step();

 private:
  Task();

  TaskState state_ = TaskState::kFinished;
};

}  // namespace tiforth
