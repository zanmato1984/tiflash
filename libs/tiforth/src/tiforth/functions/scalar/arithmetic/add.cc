#include <algorithm>
#include <cstdint>
#include <utility>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/exec.h>
#include <arrow/compute/function.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/registry.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>

namespace tiforth::functions {

namespace {

struct DecimalSpec {
  int32_t precision = -1;
  int32_t scale = -1;
};

arrow::Result<DecimalSpec> GetDecimalSpec(const arrow::DataType& type) {
  if (type.id() != arrow::Type::DECIMAL128 && type.id() != arrow::Type::DECIMAL256) {
    return arrow::Status::Invalid("expected decimal type");
  }
  const auto& dec = static_cast<const arrow::DecimalType&>(type);
  if (dec.precision() <= 0) {
    return arrow::Status::Invalid("decimal precision must be positive");
  }
  if (dec.scale() < 0) {
    return arrow::Status::Invalid("decimal scale must not be negative");
  }
  return DecimalSpec{static_cast<int32_t>(dec.precision()), static_cast<int32_t>(dec.scale())};
}

DecimalSpec InferDecimalResultSpec(const DecimalSpec& lhs, const DecimalSpec& rhs) {
  // Mirror TiFlash's PlusDecimalInferer (Common/Decimal.h).
  //
  // result_scale = max(s1, s2)
  // result_int = max(p1 - s1, p2 - s2)
  // result_prec = min(result_scale + result_int + 1, 65)
  const int32_t result_scale = std::max(lhs.scale, rhs.scale);
  const int32_t lhs_int = lhs.precision - lhs.scale;
  const int32_t rhs_int = rhs.precision - rhs.scale;
  const int32_t result_int = std::max(lhs_int, rhs_int);
  const int32_t result_prec = std::min<int32_t>(result_scale + result_int + 1, 65);
  return DecimalSpec{result_prec, result_scale};
}

arrow::Result<std::shared_ptr<arrow::DataType>> MakeArrowDecimalType(const DecimalSpec& spec) {
  if (spec.precision <= 0 || spec.scale < 0) {
    return arrow::Status::Invalid("invalid decimal precision/scale");
  }
  if (spec.precision <= arrow::Decimal128Type::kMaxPrecision) {
    return arrow::decimal128(spec.precision, spec.scale);
  }
  return arrow::decimal256(spec.precision, spec.scale);
}

template <typename DecimalT>
DecimalT ScaleTo(DecimalT value, int32_t from_scale, int32_t to_scale) {
  if (to_scale <= from_scale) {
    return value;
  }
  return DecimalT(value.IncreaseScaleBy(to_scale - from_scale));
}

template <typename DecimalT>
bool FitsInPrecision(const DecimalT& value, int32_t precision) {
  return value.FitsInPrecision(precision);
}

arrow::Result<arrow::TypeHolder> ResolveDecimalAddOutputType(
    arrow::compute::KernelContext*, const std::vector<arrow::TypeHolder>& types) {
  if (types.size() != 2) {
    return arrow::Status::Invalid("decimal add requires 2 args");
  }
  if (types[0].type == nullptr || types[1].type == nullptr) {
    return arrow::Status::Invalid("decimal add input type must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(const auto lhs_spec, GetDecimalSpec(*types[0].type));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_spec, GetDecimalSpec(*types[1].type));
  const auto out_spec = InferDecimalResultSpec(lhs_spec, rhs_spec);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeArrowDecimalType(out_spec));
  return arrow::TypeHolder(std::move(out_type));
}

arrow::Status ExecDecimalAdd(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                            arrow::compute::ExecResult* out) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }
  if (batch.num_values() != 2) {
    return arrow::Status::Invalid("decimal add requires 2 args");
  }

  const auto* lhs_type = batch[0].type();
  const auto* rhs_type = batch[1].type();
  if (lhs_type == nullptr || rhs_type == nullptr) {
    return arrow::Status::Invalid("decimal add input type must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(const auto lhs_spec, GetDecimalSpec(*lhs_type));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_spec, GetDecimalSpec(*rhs_type));
  const auto out_spec = InferDecimalResultSpec(lhs_spec, rhs_spec);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeArrowDecimalType(out_spec));

  std::shared_ptr<arrow::Array> lhs_array;
  std::shared_ptr<arrow::Array> rhs_array;
  if (batch[0].is_array()) {
    lhs_array = arrow::MakeArray(batch[0].array.ToArrayData());
  }
  if (batch[1].is_array()) {
    rhs_array = arrow::MakeArray(batch[1].array.ToArrayData());
  }

  const arrow::Scalar* lhs_scalar = batch[0].is_scalar() ? batch[0].scalar : nullptr;
  const arrow::Scalar* rhs_scalar = batch[1].is_scalar() ? batch[1].scalar : nullptr;

  const int64_t rows = batch.length;
  if (out_type->id() == arrow::Type::DECIMAL128) {
    arrow::Decimal128Builder builder(out_type, ctx->memory_pool());
    ARROW_RETURN_NOT_OK(builder.Reserve(rows));

    const arrow::FixedSizeBinaryArray* lhs_fixed =
        lhs_array != nullptr ? static_cast<const arrow::FixedSizeBinaryArray*>(lhs_array.get()) : nullptr;
    const arrow::FixedSizeBinaryArray* rhs_fixed =
        rhs_array != nullptr ? static_cast<const arrow::FixedSizeBinaryArray*>(rhs_array.get()) : nullptr;
    if (lhs_fixed != nullptr && lhs_fixed->byte_width() != 16) {
      return arrow::Status::Invalid("unexpected decimal128 byte width");
    }
    if (rhs_fixed != nullptr && rhs_fixed->byte_width() != 16) {
      return arrow::Status::Invalid("unexpected decimal128 byte width");
    }

    const auto* lhs_dec_scalar = lhs_scalar != nullptr ? static_cast<const arrow::Decimal128Scalar*>(lhs_scalar)
                                                       : nullptr;
    const auto* rhs_dec_scalar = rhs_scalar != nullptr ? static_cast<const arrow::Decimal128Scalar*>(rhs_scalar)
                                                       : nullptr;
    const bool lhs_scalar_valid = lhs_dec_scalar != nullptr && lhs_dec_scalar->is_valid;
    const bool rhs_scalar_valid = rhs_dec_scalar != nullptr && rhs_dec_scalar->is_valid;
    const arrow::Decimal128 lhs_scalar_value =
        lhs_scalar_valid ? lhs_dec_scalar->value : arrow::Decimal128{};
    const arrow::Decimal128 rhs_scalar_value =
        rhs_scalar_valid ? rhs_dec_scalar->value : arrow::Decimal128{};

    for (int64_t i = 0; i < rows; ++i) {
      const bool lhs_null = lhs_fixed != nullptr ? lhs_fixed->IsNull(i) : !lhs_scalar_valid;
      const bool rhs_null = rhs_fixed != nullptr ? rhs_fixed->IsNull(i) : !rhs_scalar_valid;
      if (lhs_null || rhs_null) {
        builder.UnsafeAppendNull();
        continue;
      }

      const arrow::Decimal128 lhs_val =
          lhs_fixed != nullptr ? arrow::Decimal128(reinterpret_cast<const uint8_t*>(lhs_fixed->GetValue(i)))
                               : lhs_scalar_value;
      const arrow::Decimal128 rhs_val =
          rhs_fixed != nullptr ? arrow::Decimal128(reinterpret_cast<const uint8_t*>(rhs_fixed->GetValue(i)))
                               : rhs_scalar_value;

      const auto sum = ScaleTo(lhs_val, lhs_spec.scale, out_spec.scale) +
                       ScaleTo(rhs_val, rhs_spec.scale, out_spec.scale);
      if (!FitsInPrecision(sum, out_spec.precision)) {
        return arrow::Status::Invalid("decimal math overflow");
      }
      builder.UnsafeAppend(sum);
    }

    std::shared_ptr<arrow::Array> out_array;
    ARROW_RETURN_NOT_OK(builder.Finish(&out_array));
    out->value = out_array->data();
    return arrow::Status::OK();
  }

  arrow::Decimal256Builder builder(out_type, ctx->memory_pool());
  ARROW_RETURN_NOT_OK(builder.Reserve(rows));

  const arrow::FixedSizeBinaryArray* lhs_fixed =
      lhs_array != nullptr ? static_cast<const arrow::FixedSizeBinaryArray*>(lhs_array.get()) : nullptr;
  const arrow::FixedSizeBinaryArray* rhs_fixed =
      rhs_array != nullptr ? static_cast<const arrow::FixedSizeBinaryArray*>(rhs_array.get()) : nullptr;
  if (lhs_fixed != nullptr) {
    const int expected = lhs_type->id() == arrow::Type::DECIMAL256 ? 32 : 16;
    if (lhs_fixed->byte_width() != expected) {
      return arrow::Status::Invalid("unexpected decimal byte width (lhs)");
    }
  }
  if (rhs_fixed != nullptr) {
    const int expected = rhs_type->id() == arrow::Type::DECIMAL256 ? 32 : 16;
    if (rhs_fixed->byte_width() != expected) {
      return arrow::Status::Invalid("unexpected decimal byte width (rhs)");
    }
  }

  const bool lhs_scalar_valid = lhs_scalar != nullptr && lhs_scalar->is_valid;
  const bool rhs_scalar_valid = rhs_scalar != nullptr && rhs_scalar->is_valid;

  const auto lhs_scalar_value = [&]() -> arrow::Decimal256 {
    if (!lhs_scalar_valid) {
      return arrow::Decimal256{};
    }
    if (lhs_type->id() == arrow::Type::DECIMAL256) {
      return static_cast<const arrow::Decimal256Scalar*>(lhs_scalar)->value;
    }
    return arrow::Decimal256(static_cast<const arrow::Decimal128Scalar*>(lhs_scalar)->value);
  }();
  const auto rhs_scalar_value = [&]() -> arrow::Decimal256 {
    if (!rhs_scalar_valid) {
      return arrow::Decimal256{};
    }
    if (rhs_type->id() == arrow::Type::DECIMAL256) {
      return static_cast<const arrow::Decimal256Scalar*>(rhs_scalar)->value;
    }
    return arrow::Decimal256(static_cast<const arrow::Decimal128Scalar*>(rhs_scalar)->value);
  }();

  for (int64_t i = 0; i < rows; ++i) {
    const bool lhs_null = lhs_fixed != nullptr ? lhs_fixed->IsNull(i) : !lhs_scalar_valid;
    const bool rhs_null = rhs_fixed != nullptr ? rhs_fixed->IsNull(i) : !rhs_scalar_valid;
    if (lhs_null || rhs_null) {
      builder.UnsafeAppendNull();
      continue;
    }

    const auto lhs_val = [&]() -> arrow::Decimal256 {
      if (lhs_fixed == nullptr) {
        return lhs_scalar_value;
      }
      if (lhs_type->id() == arrow::Type::DECIMAL256) {
        return arrow::Decimal256(reinterpret_cast<const uint8_t*>(lhs_fixed->GetValue(i)));
      }
      return arrow::Decimal256(
          arrow::Decimal128(reinterpret_cast<const uint8_t*>(lhs_fixed->GetValue(i))));
    }();
    const auto rhs_val = [&]() -> arrow::Decimal256 {
      if (rhs_fixed == nullptr) {
        return rhs_scalar_value;
      }
      if (rhs_type->id() == arrow::Type::DECIMAL256) {
        return arrow::Decimal256(reinterpret_cast<const uint8_t*>(rhs_fixed->GetValue(i)));
      }
      return arrow::Decimal256(
          arrow::Decimal128(reinterpret_cast<const uint8_t*>(rhs_fixed->GetValue(i))));
    }();

    const auto sum = ScaleTo(lhs_val, lhs_spec.scale, out_spec.scale) +
                     ScaleTo(rhs_val, rhs_spec.scale, out_spec.scale);
    if (!FitsInPrecision(sum, out_spec.precision)) {
      return arrow::Status::Invalid("decimal math overflow");
    }
    builder.UnsafeAppend(sum);
  }

  std::shared_ptr<arrow::Array> out_array;
  ARROW_RETURN_NOT_OK(builder.Finish(&out_array));
  out->value = out_array->data();
  return arrow::Status::OK();
}

class TiforthAddMetaFunction final : public arrow::compute::MetaFunction {
 public:
  explicit TiforthAddMetaFunction(arrow::compute::FunctionRegistry* fallback_registry)
      : arrow::compute::MetaFunction("add", arrow::compute::Arity::Binary(),
                                     arrow::compute::FunctionDoc::Empty()),
        fallback_registry_(fallback_registry) {}

 protected:
  arrow::Result<arrow::Datum> ExecuteImpl(const std::vector<arrow::Datum>& args,
                                         const arrow::compute::FunctionOptions* options,
                                         arrow::compute::ExecContext* ctx) const override {
    if (args.size() != 2) {
      return arrow::Status::Invalid("add requires 2 args");
    }
    if (fallback_registry_ == nullptr) {
      return arrow::Status::Invalid("fallback function registry must not be null");
    }

    const auto* lhs_type = args[0].type().get();
    const auto* rhs_type = args[1].type().get();
    const bool lhs_decimal =
        lhs_type != nullptr &&
        (lhs_type->id() == arrow::Type::DECIMAL128 || lhs_type->id() == arrow::Type::DECIMAL256);
    const bool rhs_decimal =
        rhs_type != nullptr &&
        (rhs_type->id() == arrow::Type::DECIMAL128 || rhs_type->id() == arrow::Type::DECIMAL256);

    if (lhs_decimal && rhs_decimal) {
      return arrow::compute::CallFunction("tiforth.decimal_add", args, /*options=*/nullptr, ctx);
    }

    arrow::compute::ExecContext fallback_ctx(
        ctx != nullptr ? ctx->memory_pool() : arrow::default_memory_pool(),
        ctx != nullptr ? ctx->executor() : nullptr, fallback_registry_);
    return arrow::compute::CallFunction("add", args, options, &fallback_ctx);
  }

 private:
  arrow::compute::FunctionRegistry* fallback_registry_ = nullptr;
};

}  // namespace

arrow::Status RegisterScalarArithmeticFunctions(arrow::compute::FunctionRegistry* registry,
                                                arrow::compute::FunctionRegistry* fallback_registry) {
  if (registry == nullptr) {
    return arrow::Status::Invalid("function registry must not be null");
  }
  if (fallback_registry == nullptr) {
    return arrow::Status::Invalid("fallback function registry must not be null");
  }

  auto decimal_add =
      std::make_shared<arrow::compute::ScalarFunction>("tiforth.decimal_add",
                                                      arrow::compute::Arity::Binary(),
                                                      arrow::compute::FunctionDoc::Empty());

  const auto make_kernel = [&](arrow::Type::type lhs_id,
                               arrow::Type::type rhs_id) -> arrow::compute::ScalarKernel {
    arrow::compute::ScalarKernel kernel(
        {arrow::compute::InputType(lhs_id), arrow::compute::InputType(rhs_id)},
        arrow::compute::OutputType(ResolveDecimalAddOutputType), ExecDecimalAdd);
    kernel.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
    kernel.can_write_into_slices = false;
    return kernel;
  };

  ARROW_RETURN_NOT_OK(decimal_add->AddKernel(make_kernel(arrow::Type::DECIMAL128, arrow::Type::DECIMAL128)));
  ARROW_RETURN_NOT_OK(decimal_add->AddKernel(make_kernel(arrow::Type::DECIMAL128, arrow::Type::DECIMAL256)));
  ARROW_RETURN_NOT_OK(decimal_add->AddKernel(make_kernel(arrow::Type::DECIMAL256, arrow::Type::DECIMAL128)));
  ARROW_RETURN_NOT_OK(decimal_add->AddKernel(make_kernel(arrow::Type::DECIMAL256, arrow::Type::DECIMAL256)));

  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(decimal_add), /*allow_overwrite=*/true));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::make_shared<TiforthAddMetaFunction>(fallback_registry),
                                            /*allow_overwrite=*/true));
  return arrow::Status::OK();
}

}  // namespace tiforth::functions
