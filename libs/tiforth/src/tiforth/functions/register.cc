#include "tiforth/functions/register.h"

#include <arrow/compute/registry.h>

namespace tiforth::functions {

arrow::Status RegisterDecimalFunctions(arrow::compute::FunctionRegistry* registry,
                                       arrow::compute::FunctionRegistry* fallback_registry);
arrow::Status RegisterCollatedCompareFunctions(arrow::compute::FunctionRegistry* registry,
                                               arrow::compute::FunctionRegistry* fallback_registry);

arrow::Status RegisterTiforthFunctions(arrow::compute::FunctionRegistry* registry,
                                       arrow::compute::FunctionRegistry* fallback_registry) {
  if (registry == nullptr) {
    return arrow::Status::Invalid("function registry must not be null");
  }
  if (fallback_registry == nullptr) {
    return arrow::Status::Invalid("fallback function registry must not be null");
  }

  ARROW_RETURN_NOT_OK(RegisterDecimalFunctions(registry, fallback_registry));
  ARROW_RETURN_NOT_OK(RegisterCollatedCompareFunctions(registry, fallback_registry));
  return arrow::Status::OK();
}

}  // namespace tiforth::functions

