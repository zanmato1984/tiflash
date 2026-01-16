#ifndef TIFORTH_C_TIFORTH_H_
#define TIFORTH_C_TIFORTH_H_

#include <stddef.h>
#include <stdint.h>

#include <arrow/c/abi.h>

#ifdef __cplusplus
extern "C" {
#endif

// MS7C: C ABI skeleton (no implementation yet).
//
// Versioning notes:
// - This header defines the *C ABI* version, not the TiForth library version.
// - Callers should set `tiforth_engine_options_t.abi_version` to `TIFORTH_C_ABI_VERSION`.
// - Future ABI revisions should extend option structs by appending fields.

#define TIFORTH_C_ABI_VERSION 1u

#if defined(_WIN32)
#if defined(TIFORTH_CAPI_BUILDING)
#define TIFORTH_CAPI __declspec(dllexport)
#else
#define TIFORTH_CAPI __declspec(dllimport)
#endif
#elif defined(__GNUC__)
#define TIFORTH_CAPI __attribute__((visibility("default")))
#else
#define TIFORTH_CAPI
#endif

typedef struct tiforth_engine_t tiforth_engine_t;
typedef struct tiforth_pipeline_t tiforth_pipeline_t;
typedef struct tiforth_task_t tiforth_task_t;

typedef enum tiforth_status_code_e {
  TIFORTH_STATUS_OK = 0,
  TIFORTH_STATUS_INVALID_ARGUMENT = 1,
  TIFORTH_STATUS_NOT_IMPLEMENTED = 2,
  TIFORTH_STATUS_INTERNAL_ERROR = 3,
} tiforth_status_code_t;

typedef struct tiforth_status_t {
  tiforth_status_code_t code;
  // Optional heap-allocated error message owned by TiForth; free with `tiforth_free`.
  char* message;
} tiforth_status_t;

typedef struct tiforth_engine_options_t {
  uint32_t abi_version;

  // Reserved for future use; must be zero for now.
  uint32_t reserved_u32;
  uint64_t reserved_u64;
  void* reserved_ptrs[4];
} tiforth_engine_options_t;

typedef enum tiforth_task_state_e {
  TIFORTH_TASK_NEED_INPUT = 0,
  TIFORTH_TASK_HAS_OUTPUT = 1,
  TIFORTH_TASK_FINISHED = 2,
  TIFORTH_TASK_BLOCKED = 3,
} tiforth_task_state_t;

// Memory management helper for `tiforth_status_t.message` and future heap-allocated buffers.
TIFORTH_CAPI void tiforth_free(void* ptr);

// Engine lifecycle.
TIFORTH_CAPI tiforth_status_t tiforth_engine_create(const tiforth_engine_options_t* options,
                                                    tiforth_engine_t** out_engine);
TIFORTH_CAPI void tiforth_engine_destroy(tiforth_engine_t* engine);

// Pipeline lifecycle.
TIFORTH_CAPI tiforth_status_t tiforth_pipeline_create(tiforth_engine_t* engine,
                                                      tiforth_pipeline_t** out_pipeline);
TIFORTH_CAPI void tiforth_pipeline_destroy(tiforth_pipeline_t* pipeline);

// Task lifecycle.
TIFORTH_CAPI tiforth_status_t tiforth_pipeline_create_task(tiforth_pipeline_t* pipeline,
                                                           tiforth_task_t** out_task);
TIFORTH_CAPI void tiforth_task_destroy(tiforth_task_t* task);

// Task execution.
TIFORTH_CAPI tiforth_status_t tiforth_task_step(tiforth_task_t* task, tiforth_task_state_t* out_state);
TIFORTH_CAPI tiforth_status_t tiforth_task_close_input(tiforth_task_t* task);

// Data interchange (planned):
// - Use Arrow C Data Interface (`ArrowSchema` + `ArrowArray`) to represent a RecordBatch.
// - Concrete representation and ownership rules will be specified in a follow-up milestone.
TIFORTH_CAPI tiforth_status_t tiforth_task_push_input_batch(tiforth_task_t* task,
                                                            const struct ArrowSchema* schema,
                                                            const struct ArrowArray* array);
TIFORTH_CAPI tiforth_status_t tiforth_task_pull_output_batch(tiforth_task_t* task,
                                                             struct ArrowSchema* out_schema,
                                                             struct ArrowArray* out_array);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // TIFORTH_C_TIFORTH_H_
