#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include <arrow/result.h>
#include <arrow/status.h>

namespace arrow {
class RecordBatch;
class RecordBatchReader;
}  // namespace arrow

namespace tiforth {

struct SpillHandle {
  uint64_t id = 0;
};

class SpillManager {
 public:
  virtual ~SpillManager() = default;

  virtual arrow::Result<std::optional<SpillHandle>> RequestSpill(int64_t bytes_hint) = 0;
  virtual arrow::Status WriteSpill(SpillHandle handle,
                                   std::shared_ptr<arrow::RecordBatch> batch) = 0;
  virtual arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ReadSpill(SpillHandle handle) = 0;
};

class DenySpillManager final : public SpillManager {
 public:
  arrow::Result<std::optional<SpillHandle>> RequestSpill(int64_t bytes_hint) override {
    (void)bytes_hint;
    return std::nullopt;
  }

  arrow::Status WriteSpill(SpillHandle handle,
                           std::shared_ptr<arrow::RecordBatch> batch) override {
    (void)handle;
    (void)batch;
    return arrow::Status::NotImplemented("spilling is disabled");
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ReadSpill(SpillHandle handle) override {
    (void)handle;
    return arrow::Status::NotImplemented("spilling is disabled");
  }
};

}  // namespace tiforth

