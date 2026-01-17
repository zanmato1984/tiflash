#pragma once

#include <cstdint>
#include <memory>

#include <arrow/result.h>
#include <arrow/status.h>

namespace arrow {
class Array;
class Datum;
class MemoryPool;
}  // namespace arrow

namespace tiforth::detail {

arrow::Status EnsureArrowComputeInitialized();

arrow::Result<std::shared_ptr<arrow::Array>> DatumToArray(const arrow::Datum& datum,
                                                          arrow::MemoryPool* pool);

arrow::Result<std::shared_ptr<arrow::Array>> DatumToArray(const arrow::Datum& datum, int64_t length,
                                                          arrow::MemoryPool* pool);

}  // namespace tiforth::detail
