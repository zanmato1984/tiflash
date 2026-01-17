#pragma once

#include <cstdint>
#include <memory>

#include <arrow/compute/function_options.h>

#include "tiforth/type_metadata.h"

namespace tiforth::functions {

std::unique_ptr<arrow::compute::FunctionOptions> MakeMyTimeOptions(LogicalTypeId type_id,
                                                                   int32_t datetime_fsp);

}  // namespace tiforth::functions

