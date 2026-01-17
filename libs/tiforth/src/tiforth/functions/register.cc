#include "tiforth/functions/register.h"

#include <arrow/compute/registry.h>

namespace tiforth::functions {

arrow::Status RegisterScalarArithmeticFunctions(arrow::compute::FunctionRegistry* registry,
                                                arrow::compute::FunctionRegistry* fallback_registry);
arrow::Status RegisterScalarComparisonFunctions(arrow::compute::FunctionRegistry* registry,
                                                arrow::compute::FunctionRegistry* fallback_registry);
arrow::Status RegisterScalarTemporalFunctions(arrow::compute::FunctionRegistry* registry,
                                              arrow::compute::FunctionRegistry* fallback_registry);

arrow::Status RegisterTiforthFunctions(arrow::compute::FunctionRegistry* registry,
                                       arrow::compute::FunctionRegistry* fallback_registry) {
  if (registry == nullptr) {
    return arrow::Status::Invalid("function registry must not be null");
  }
  if (fallback_registry == nullptr) {
    return arrow::Status::Invalid("fallback function registry must not be null");
  }

  ARROW_RETURN_NOT_OK(RegisterScalarArithmeticFunctions(registry, fallback_registry));
  ARROW_RETURN_NOT_OK(RegisterScalarComparisonFunctions(registry, fallback_registry));
  ARROW_RETURN_NOT_OK(RegisterScalarTemporalFunctions(registry, fallback_registry));
  return arrow::Status::OK();
}

}  // namespace tiforth::functions
