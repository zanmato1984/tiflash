#include "tiforth/detail/arrow_compute.h"

#include <mutex>

#include <arrow/compute/initialize.h>

namespace tiforth::detail {

namespace {

std::once_flag arrow_compute_init_once;
arrow::Status arrow_compute_init_status = arrow::Status::OK();

}  // namespace

arrow::Status EnsureArrowComputeInitialized() {
  std::call_once(arrow_compute_init_once,
                 []() { arrow_compute_init_status = arrow::compute::Initialize(); });
  return arrow_compute_init_status;
}

}  // namespace tiforth::detail

