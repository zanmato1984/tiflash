#pragma once

#include <memory>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>

namespace tiforth {

enum class OperatorStatus {
  kFinished,
  kNeedInput,
  kHasOutput,
};

class Operator {
 public:
  virtual ~Operator() = default;
};

class SourceOp : public Operator {
 public:
  arrow::Result<OperatorStatus> Read(std::shared_ptr<arrow::RecordBatch>* batch) {
    if (batch == nullptr) {
      return arrow::Status::Invalid("batch output must not be null");
    }
    return ReadImpl(batch);
  }

 protected:
  virtual arrow::Result<OperatorStatus> ReadImpl(std::shared_ptr<arrow::RecordBatch>* batch) = 0;
};
using SourceOpPtr = std::unique_ptr<SourceOp>;
using SourceOps = std::vector<SourceOpPtr>;

class TransformOp : public Operator {
 public:
  arrow::Result<OperatorStatus> TryOutput(std::shared_ptr<arrow::RecordBatch>* batch) {
    if (batch == nullptr) {
      return arrow::Status::Invalid("batch output must not be null");
    }
    return TryOutputImpl(batch);
  }

  arrow::Result<OperatorStatus> Transform(std::shared_ptr<arrow::RecordBatch>* batch) {
    if (batch == nullptr) {
      return arrow::Status::Invalid("batch input must not be null");
    }
    return TransformImpl(batch);
  }

 protected:
  virtual arrow::Result<OperatorStatus> TryOutputImpl(std::shared_ptr<arrow::RecordBatch>* batch) {
    batch->reset();
    return OperatorStatus::kNeedInput;
  }

  virtual arrow::Result<OperatorStatus> TransformImpl(std::shared_ptr<arrow::RecordBatch>* batch) = 0;
};
using TransformOpPtr = std::unique_ptr<TransformOp>;
using TransformOps = std::vector<TransformOpPtr>;

class SinkOp : public Operator {
 public:
  arrow::Result<OperatorStatus> Prepare() { return PrepareImpl(); }

  arrow::Result<OperatorStatus> Write(std::shared_ptr<arrow::RecordBatch> batch) {
    return WriteImpl(std::move(batch));
  }

 protected:
  virtual arrow::Result<OperatorStatus> PrepareImpl() { return OperatorStatus::kNeedInput; }

  virtual arrow::Result<OperatorStatus> WriteImpl(std::shared_ptr<arrow::RecordBatch> batch) = 0;
};
using SinkOpPtr = std::unique_ptr<SinkOp>;

}  // namespace tiforth
