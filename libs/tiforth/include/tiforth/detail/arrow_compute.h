#pragma once

#include <arrow/status.h>

namespace tiforth::detail {

arrow::Status EnsureArrowComputeInitialized();

}  // namespace tiforth::detail

