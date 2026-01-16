#include "tiforth_c/tiforth.h"

#include <cstdlib>
#include <cstring>

namespace {

tiforth_status_t MakeStatus(tiforth_status_code_t code, const char* message) {
  tiforth_status_t status;
  status.code = code;
  status.message = nullptr;
  if (message == nullptr) {
    return status;
  }

  const size_t len = std::strlen(message) + 1;
  auto* buffer = static_cast<char*>(std::malloc(len));
  if (buffer == nullptr) {
    return status;
  }
  std::memcpy(buffer, message, len);
  status.message = buffer;
  return status;
}

tiforth_status_t MakeOk() { return MakeStatus(TIFORTH_STATUS_OK, nullptr); }

tiforth_status_t MakeNotImplemented() {
  return MakeStatus(TIFORTH_STATUS_NOT_IMPLEMENTED, "tiforth_capi is not implemented yet");
}

bool IsZeroedOptions(const tiforth_engine_options_t& options) {
  if (options.reserved_u32 != 0 || options.reserved_u64 != 0) {
    return false;
  }
  for (void* ptr : options.reserved_ptrs) {
    if (ptr != nullptr) {
      return false;
    }
  }
  return true;
}

}  // namespace

struct tiforth_engine_t {};
struct tiforth_pipeline_t {};
struct tiforth_task_t {};

extern "C" {

void tiforth_free(void* ptr) { std::free(ptr); }

tiforth_status_t tiforth_engine_create(const tiforth_engine_options_t* options,
                                       tiforth_engine_t** out_engine) {
  if (out_engine == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "out_engine must not be null");
  }
  *out_engine = nullptr;

  if (options == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "options must not be null");
  }
  if (options->abi_version != TIFORTH_C_ABI_VERSION) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "unsupported tiforth C ABI version");
  }
  if (!IsZeroedOptions(*options)) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "reserved engine options must be zero");
  }

  return MakeNotImplemented();
}

void tiforth_engine_destroy(tiforth_engine_t* engine) { (void)engine; }

tiforth_status_t tiforth_pipeline_create(tiforth_engine_t* engine, tiforth_pipeline_t** out_pipeline) {
  if (out_pipeline == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "out_pipeline must not be null");
  }
  *out_pipeline = nullptr;
  if (engine == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "engine must not be null");
  }
  return MakeNotImplemented();
}

void tiforth_pipeline_destroy(tiforth_pipeline_t* pipeline) { (void)pipeline; }

tiforth_status_t tiforth_pipeline_create_task(tiforth_pipeline_t* pipeline,
                                              tiforth_task_t** out_task) {
  if (out_task == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "out_task must not be null");
  }
  *out_task = nullptr;
  if (pipeline == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "pipeline must not be null");
  }
  return MakeNotImplemented();
}

void tiforth_task_destroy(tiforth_task_t* task) { (void)task; }

tiforth_status_t tiforth_task_step(tiforth_task_t* task, tiforth_task_state_t* out_state) {
  if (out_state == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "out_state must not be null");
  }
  *out_state = TIFORTH_TASK_BLOCKED;
  if (task == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "task must not be null");
  }
  return MakeNotImplemented();
}

tiforth_status_t tiforth_task_close_input(tiforth_task_t* task) {
  if (task == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "task must not be null");
  }
  return MakeNotImplemented();
}

tiforth_status_t tiforth_task_push_input_batch(tiforth_task_t* task,
                                               const struct ArrowSchema* schema,
                                               const struct ArrowArray* array) {
  if (task == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "task must not be null");
  }
  if (schema == nullptr || array == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "schema/array must not be null");
  }
  return MakeNotImplemented();
}

tiforth_status_t tiforth_task_pull_output_batch(tiforth_task_t* task,
                                                struct ArrowSchema* out_schema,
                                                struct ArrowArray* out_array) {
  if (task == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "task must not be null");
  }
  if (out_schema == nullptr || out_array == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "out_schema/out_array must not be null");
  }
  return MakeNotImplemented();
}

}  // extern "C"

