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
typedef struct tiforth_expr_t tiforth_expr_t;

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

// Expression builder (minimal, stable subset).
//
// Ownership:
// - Returned `tiforth_expr_t*` are owned by the caller and must be destroyed with
//   `tiforth_expr_destroy`.
// - Pipelines/operators keep references to the expressions, so callers may destroy
//   their handles after appending operators.
TIFORTH_CAPI tiforth_status_t tiforth_expr_field_ref_index(int32_t index, tiforth_expr_t** out_expr);
TIFORTH_CAPI tiforth_status_t tiforth_expr_literal_int32(int32_t value, tiforth_expr_t** out_expr);
TIFORTH_CAPI tiforth_status_t tiforth_expr_call(const char* function_name,
                                                const tiforth_expr_t* const* args,
                                                size_t num_args,
                                                tiforth_expr_t** out_expr);
TIFORTH_CAPI void tiforth_expr_destroy(tiforth_expr_t* expr);

typedef struct tiforth_projection_expr_t {
  // Null-terminated output field name (UTF-8).
  const char* name;
  // Expression producing the column.
  const tiforth_expr_t* expr;
} tiforth_projection_expr_t;

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

// Pipeline building (minimal).
TIFORTH_CAPI tiforth_status_t tiforth_pipeline_append_filter(tiforth_pipeline_t* pipeline,
                                                             const tiforth_expr_t* predicate);
TIFORTH_CAPI tiforth_status_t tiforth_pipeline_append_projection(tiforth_pipeline_t* pipeline,
                                                                 const tiforth_projection_expr_t* exprs,
                                                                 size_t num_exprs);

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
//
// Ownership rules (current):
// - `tiforth_task_push_input_batch` consumes (`moves`) the provided `schema` and `array` (record batch as a
//   struct array). Callers must not call `schema->release` / `array->release` after a successful call.
// - `tiforth_task_pull_output_batch` produces an owned (`exported`) record batch. Callers must eventually call
//   `out_schema->release(out_schema)` and `out_array->release(out_array)` when done (if non-null).
TIFORTH_CAPI tiforth_status_t tiforth_task_push_input_batch(tiforth_task_t* task,
                                                            struct ArrowSchema* schema,
                                                            struct ArrowArray* array);
TIFORTH_CAPI tiforth_status_t tiforth_task_pull_output_batch(tiforth_task_t* task,
                                                             struct ArrowSchema* out_schema,
                                                             struct ArrowArray* out_array);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // TIFORTH_C_TIFORTH_H_
