#include "tiforth/functions/collated_compare.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/exec.h>
#include <arrow/compute/function.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/registry.h>
#include <arrow/scalar.h>
#include <arrow/status.h>

#include "tiforth/collation.h"

namespace tiforth::functions {

namespace {

constexpr const char* kCollatedCompareOptionsTypeName = "tiforth_collated_compare";

struct CollatedCompareOptions final : public arrow::compute::FunctionOptions {
  CollatedCompareOptions();
  explicit CollatedCompareOptions(int32_t collation_id);

  static constexpr const char* kTypeName = kCollatedCompareOptionsTypeName;

  int32_t collation_id = 63;
};

class CollatedCompareOptionsType final : public arrow::compute::FunctionOptionsType {
 public:
  const char* type_name() const override { return CollatedCompareOptions::kTypeName; }

  std::string Stringify(const arrow::compute::FunctionOptions& options) const override {
    const auto& typed = static_cast<const CollatedCompareOptions&>(options);
    return "CollatedCompareOptions{collation_id=" + std::to_string(typed.collation_id) + "}";
  }

  bool Compare(const arrow::compute::FunctionOptions& left,
               const arrow::compute::FunctionOptions& right) const override {
    const auto& l = static_cast<const CollatedCompareOptions&>(left);
    const auto& r = static_cast<const CollatedCompareOptions&>(right);
    return l.collation_id == r.collation_id;
  }

  std::unique_ptr<arrow::compute::FunctionOptions> Copy(
      const arrow::compute::FunctionOptions& options) const override {
    const auto& typed = static_cast<const CollatedCompareOptions&>(options);
    return std::make_unique<CollatedCompareOptions>(typed.collation_id);
  }
};

const CollatedCompareOptionsType kCollatedCompareOptionsType;

CollatedCompareOptions::CollatedCompareOptions()
    : arrow::compute::FunctionOptions(&kCollatedCompareOptionsType) {}

CollatedCompareOptions::CollatedCompareOptions(int32_t collation_id)
    : arrow::compute::FunctionOptions(&kCollatedCompareOptionsType), collation_id(collation_id) {}

bool IsBinaryLike(arrow::Type::type type_id) {
  return type_id == arrow::Type::BINARY || type_id == arrow::Type::LARGE_BINARY ||
         type_id == arrow::Type::STRING || type_id == arrow::Type::LARGE_STRING;
}

std::string_view GetBinaryViewAt(const arrow::Array& array, int64_t i) {
  const auto type_id = array.type_id();
  if (type_id == arrow::Type::LARGE_BINARY || type_id == arrow::Type::LARGE_STRING) {
    const auto& bin =
        static_cast<const arrow::BaseBinaryArray<arrow::LargeBinaryType>&>(array);
    return bin.GetView(i);
  }
  const auto& bin =
      static_cast<const arrow::BaseBinaryArray<arrow::BinaryType>&>(array);
  return bin.GetView(i);
}

enum class CompareOp {
  kEqual,
  kNotEqual,
  kLess,
  kLessEqual,
  kGreater,
  kGreaterEqual,
};

arrow::Result<bool> ApplyCompare(CompareOp op, int cmp) {
  switch (op) {
    case CompareOp::kEqual:
      return cmp == 0;
    case CompareOp::kNotEqual:
      return cmp != 0;
    case CompareOp::kLess:
      return cmp < 0;
    case CompareOp::kLessEqual:
      return cmp <= 0;
    case CompareOp::kGreater:
      return cmp > 0;
    case CompareOp::kGreaterEqual:
      return cmp >= 0;
  }
  return arrow::Status::Invalid("unexpected compare op");
}

struct CollatedCompareState final : public arrow::compute::KernelState {
  explicit CollatedCompareState(Collation collation) : collation(collation) {}

  Collation collation;
};

arrow::Result<std::unique_ptr<arrow::compute::KernelState>> InitCollatedCompareState(
    arrow::compute::KernelContext*, const arrow::compute::KernelInitArgs& args) {
  int32_t collation_id = 63;
  if (args.options != nullptr) {
    const auto* typed = dynamic_cast<const CollatedCompareOptions*>(args.options);
    if (typed != nullptr) {
      collation_id = typed->collation_id;
    }
  }
  const auto collation = CollationFromId(collation_id);
  if (collation.kind == CollationKind::kUnsupported) {
    return arrow::Status::NotImplemented("unsupported collation id: ", collation_id);
  }
  return std::make_unique<CollatedCompareState>(collation);
}

template <CompareOp op>
arrow::Status ExecCollatedCompare(arrow::compute::KernelContext* ctx,
                                  const arrow::compute::ExecSpan& batch,
                                  arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("string compare requires 2 args");
  }

  const auto* state = static_cast<const CollatedCompareState*>(ctx->state());
  const Collation collation = state != nullptr ? state->collation : CollationFromId(63);

  std::shared_ptr<arrow::Array> lhs_array;
  std::shared_ptr<arrow::Array> rhs_array;
  if (batch[0].is_array()) {
    lhs_array = arrow::MakeArray(batch[0].array.ToArrayData());
  }
  if (batch[1].is_array()) {
    rhs_array = arrow::MakeArray(batch[1].array.ToArrayData());
  }

  const auto* lhs_scalar =
      batch[0].is_scalar() ? dynamic_cast<const arrow::BaseBinaryScalar*>(batch[0].scalar) : nullptr;
  const auto* rhs_scalar =
      batch[1].is_scalar() ? dynamic_cast<const arrow::BaseBinaryScalar*>(batch[1].scalar) : nullptr;

  const int64_t rows = batch.length;
  arrow::BooleanBuilder out_builder(ctx->memory_pool());
  ARROW_RETURN_NOT_OK(out_builder.Reserve(rows));

  const bool lhs_scalar_valid = lhs_scalar != nullptr && lhs_scalar->is_valid;
  const bool rhs_scalar_valid = rhs_scalar != nullptr && rhs_scalar->is_valid;
  const std::string_view lhs_scalar_view =
      lhs_scalar_valid ? lhs_scalar->view() : std::string_view{};
  const std::string_view rhs_scalar_view =
      rhs_scalar_valid ? rhs_scalar->view() : std::string_view{};

  for (int64_t i = 0; i < rows; ++i) {
    const bool lhs_null = lhs_array != nullptr ? lhs_array->IsNull(i) : !lhs_scalar_valid;
    const bool rhs_null = rhs_array != nullptr ? rhs_array->IsNull(i) : !rhs_scalar_valid;
    if (lhs_null || rhs_null) {
      out_builder.UnsafeAppendNull();
      continue;
    }

    const std::string_view lhs_view =
        lhs_array != nullptr ? GetBinaryViewAt(*lhs_array, i) : lhs_scalar_view;
    const std::string_view rhs_view =
        rhs_array != nullptr ? GetBinaryViewAt(*rhs_array, i) : rhs_scalar_view;

    const int cmp = CompareString(collation, lhs_view, rhs_view);
    ARROW_ASSIGN_OR_RAISE(const bool keep, ApplyCompare(op, cmp));
    out_builder.UnsafeAppend(keep);
  }

  std::shared_ptr<arrow::Array> out_array;
  ARROW_RETURN_NOT_OK(out_builder.Finish(&out_array));
  out->value = out_array->data();
  return arrow::Status::OK();
}

class TiforthCompareMetaFunction final : public arrow::compute::MetaFunction {
 public:
  TiforthCompareMetaFunction(std::string name, std::string collated_name,
                             arrow::compute::FunctionRegistry* fallback_registry)
      : arrow::compute::MetaFunction(std::move(name), arrow::compute::Arity::Binary(),
                                     arrow::compute::FunctionDoc::Empty()),
        collated_name_(std::move(collated_name)),
        fallback_registry_(fallback_registry) {}

 protected:
  arrow::Result<arrow::Datum> ExecuteImpl(const std::vector<arrow::Datum>& args,
                                         const arrow::compute::FunctionOptions* options,
                                         arrow::compute::ExecContext* ctx) const override {
    if (args.size() != 2) {
      return arrow::Status::Invalid(name(), " requires 2 args");
    }
    if (fallback_registry_ == nullptr) {
      return arrow::Status::Invalid("fallback function registry must not be null");
    }

    const auto* lhs_type = args[0].type().get();
    const auto* rhs_type = args[1].type().get();
    const bool lhs_binary = lhs_type != nullptr && IsBinaryLike(lhs_type->id());
    const bool rhs_binary = rhs_type != nullptr && IsBinaryLike(rhs_type->id());

    if (lhs_binary && rhs_binary) {
      return arrow::compute::CallFunction(collated_name_, args, options, ctx);
    }

    arrow::compute::ExecContext fallback_ctx(
        ctx != nullptr ? ctx->memory_pool() : arrow::default_memory_pool(),
        ctx != nullptr ? ctx->executor() : nullptr, fallback_registry_);
    return arrow::compute::CallFunction(name(), args, /*options=*/nullptr, &fallback_ctx);
  }

 private:
  std::string collated_name_;
  arrow::compute::FunctionRegistry* fallback_registry_ = nullptr;
};

arrow::compute::ScalarKernel MakeBinaryLikeKernel(arrow::compute::ArrayKernelExec exec) {
  arrow::compute::ScalarKernel kernel(
      {arrow::compute::InputType(arrow::compute::match::BinaryLike()),
       arrow::compute::InputType(arrow::compute::match::BinaryLike())},
      arrow::compute::OutputType(arrow::boolean()), exec, InitCollatedCompareState);
  kernel.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
  kernel.can_write_into_slices = false;
  return kernel;
}

arrow::compute::ScalarKernel MakeLargeBinaryLikeKernel(arrow::compute::ArrayKernelExec exec) {
  arrow::compute::ScalarKernel kernel(
      {arrow::compute::InputType(arrow::compute::match::LargeBinaryLike()),
       arrow::compute::InputType(arrow::compute::match::LargeBinaryLike())},
      arrow::compute::OutputType(arrow::boolean()), exec, InitCollatedCompareState);
  kernel.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
  kernel.can_write_into_slices = false;
  return kernel;
}

arrow::Status RegisterOneCollatedCompare(arrow::compute::FunctionRegistry* registry,
                                        arrow::compute::FunctionRegistry* fallback_registry,
                                        std::string_view name, std::string_view collated_name,
                                        arrow::compute::ArrayKernelExec binarylike_exec,
                                        arrow::compute::ArrayKernelExec large_binarylike_exec) {
  auto func = std::make_shared<arrow::compute::ScalarFunction>(std::string(collated_name),
                                                              arrow::compute::Arity::Binary(),
                                                              arrow::compute::FunctionDoc::Empty(),
                                                              /*default_options=*/nullptr,
                                                              /*is_pure=*/true);
  ARROW_RETURN_NOT_OK(func->AddKernel(MakeBinaryLikeKernel(binarylike_exec)));
  ARROW_RETURN_NOT_OK(func->AddKernel(MakeLargeBinaryLikeKernel(large_binarylike_exec)));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));

  ARROW_RETURN_NOT_OK(registry->AddFunction(
      std::make_shared<TiforthCompareMetaFunction>(std::string(name), std::string(collated_name),
                                                   fallback_registry),
      /*allow_overwrite=*/true));
  return arrow::Status::OK();
}

}  // namespace

std::unique_ptr<arrow::compute::FunctionOptions> MakeCollatedCompareOptions(int32_t collation_id) {
  return std::make_unique<CollatedCompareOptions>(collation_id);
}

arrow::Status RegisterCollatedCompareFunctions(arrow::compute::FunctionRegistry* registry,
                                               arrow::compute::FunctionRegistry* fallback_registry) {
  if (registry == nullptr) {
    return arrow::Status::Invalid("function registry must not be null");
  }
  if (fallback_registry == nullptr) {
    return arrow::Status::Invalid("fallback function registry must not be null");
  }

  ARROW_RETURN_NOT_OK(registry->AddFunctionOptionsType(&kCollatedCompareOptionsType,
                                                       /*allow_overwrite=*/true));

  ARROW_RETURN_NOT_OK(RegisterOneCollatedCompare(
      registry, fallback_registry, "equal", "tiforth.collated_equal",
      ExecCollatedCompare<CompareOp::kEqual>, ExecCollatedCompare<CompareOp::kEqual>));
  ARROW_RETURN_NOT_OK(RegisterOneCollatedCompare(
      registry, fallback_registry, "not_equal", "tiforth.collated_not_equal",
      ExecCollatedCompare<CompareOp::kNotEqual>, ExecCollatedCompare<CompareOp::kNotEqual>));
  ARROW_RETURN_NOT_OK(RegisterOneCollatedCompare(
      registry, fallback_registry, "less", "tiforth.collated_less",
      ExecCollatedCompare<CompareOp::kLess>, ExecCollatedCompare<CompareOp::kLess>));
  ARROW_RETURN_NOT_OK(RegisterOneCollatedCompare(
      registry, fallback_registry, "less_equal", "tiforth.collated_less_equal",
      ExecCollatedCompare<CompareOp::kLessEqual>, ExecCollatedCompare<CompareOp::kLessEqual>));
  ARROW_RETURN_NOT_OK(RegisterOneCollatedCompare(
      registry, fallback_registry, "greater", "tiforth.collated_greater",
      ExecCollatedCompare<CompareOp::kGreater>, ExecCollatedCompare<CompareOp::kGreater>));
  ARROW_RETURN_NOT_OK(RegisterOneCollatedCompare(
      registry, fallback_registry, "greater_equal", "tiforth.collated_greater_equal",
      ExecCollatedCompare<CompareOp::kGreaterEqual>, ExecCollatedCompare<CompareOp::kGreaterEqual>));

  return arrow::Status::OK();
}

}  // namespace tiforth::functions
