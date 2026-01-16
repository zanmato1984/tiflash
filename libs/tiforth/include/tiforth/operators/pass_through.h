#pragma once

#include "tiforth/operators.h"

namespace tiforth {

class PassThroughTransformOp final : public TransformOp {
 protected:
  arrow::Result<OperatorStatus> TransformImpl(
      std::shared_ptr<arrow::RecordBatch>* batch) override {
    // Intentionally no-op; the input batch (or end-of-stream marker) is forwarded as-is.
    (void)batch;
    return OperatorStatus::kHasOutput;
  }
};

}  // namespace tiforth

