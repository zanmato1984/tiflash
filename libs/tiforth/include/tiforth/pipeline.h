#pragma once

#include <memory>

#include <arrow/result.h>
#include <arrow/record_batch.h>

#include "tiforth/engine.h"
#include "tiforth/task.h"

namespace tiforth {

class Pipeline;

class PipelineBuilder {
 public:
  static arrow::Result<std::unique_ptr<PipelineBuilder>> Create(const Engine* engine);

  PipelineBuilder(const PipelineBuilder&) = delete;
  PipelineBuilder& operator=(const PipelineBuilder&) = delete;

  ~PipelineBuilder();

  arrow::Result<std::unique_ptr<Pipeline>> Finalize();

 private:
  explicit PipelineBuilder(const Engine* engine);

  const Engine* engine_;
};

class Pipeline {
 public:
  Pipeline(const Pipeline&) = delete;
  Pipeline& operator=(const Pipeline&) = delete;

  arrow::Result<std::unique_ptr<Task>> CreateTask() const;
  arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> MakeReader(
      std::shared_ptr<arrow::RecordBatchReader> input) const;

 private:
  explicit Pipeline(const Engine* engine);

  const Engine* engine_;

  friend class PipelineBuilder;
};

}  // namespace tiforth
