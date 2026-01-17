#pragma once

#include <cstdint>
#include <memory>

#include <arrow/compute/function_options.h>

namespace tiforth::functions {

std::unique_ptr<arrow::compute::FunctionOptions> MakeCollatedCompareOptions(int32_t collation_id);

}  // namespace tiforth::functions
