#include "tiforth/pipeline.h"

#include <utility>

#include <arrow/status.h>

namespace tiforth {

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

}  // namespace tiforth
