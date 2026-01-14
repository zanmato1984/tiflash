#include "tiforth/task.h"

namespace tiforth {

arrow::Result<std::unique_ptr<Task>> Task::Create() {
  return std::unique_ptr<Task>(new Task());
}

Task::Task() = default;

Task::~Task() = default;

arrow::Result<TaskState> Task::Step() { return state_; }

}  // namespace tiforth
