#include "tiforth/function_registry.h"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>

#include <arrow/array.h>
#include <arrow/array/concatenate.h>
#include <arrow/builder.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/memory_pool.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>

#include "tiforth/collation.h"
#include "tiforth/detail/arrow_compute.h"
#include "tiforth/type_metadata.h"

namespace tiforth {

namespace {

arrow::Result<std::shared_ptr<arrow::Array>> DatumToArray(const arrow::Datum& datum,
                                                         arrow::MemoryPool* pool) {
  if (datum.is_array()) {
    return datum.make_array();
  }
  if (datum.is_chunked_array()) {
    auto chunked = datum.chunked_array();
    if (chunked == nullptr) {
      return arrow::Status::Invalid("expected non-null chunked array datum");
    }
    if (chunked->num_chunks() == 1) {
      return chunked->chunk(0);
    }
    return arrow::Concatenate(chunked->chunks(), pool);
  }
  return arrow::Status::Invalid("expected array or chunked array datum");
}

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

arrow::Result<Collation> ResolveStringCollation(const std::vector<TypedDatum>& args) {
  std::optional<int32_t> collation_id;
  for (const auto& arg : args) {
    if (arg.logical_type.id != LogicalTypeId::kString) {
      continue;
    }
    const int32_t id = arg.logical_type.collation_id >= 0 ? arg.logical_type.collation_id : 63;
    if (collation_id.has_value() && *collation_id != id) {
      return arrow::Status::NotImplemented("collation mismatch: ", *collation_id, " vs ", id);
    }
    collation_id = id;
  }
  const int32_t id = collation_id.has_value() ? *collation_id : 63;
  const auto collation = CollationFromId(id);
  if (collation.kind == CollationKind::kUnsupported) {
    return arrow::Status::NotImplemented("unsupported collation id: ", id);
  }
  return collation;
}

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

LogicalType MakeDecimalLogicalType(const DecimalSpec& spec) {
  LogicalType out;
  out.id = LogicalTypeId::kDecimal;
  out.decimal_precision = spec.precision;
  out.decimal_scale = spec.scale;
  return out;
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

class DecimalAddFunction final : public Function {
 public:
  bool CanExecute(const std::vector<TypedDatum>& args) const override {
    if (args.size() != 2) {
      return false;
    }
    const auto lhs_type = args[0].datum.type();
    const auto rhs_type = args[1].datum.type();
    if (lhs_type == nullptr || rhs_type == nullptr) {
      return false;
    }
    return (lhs_type->id() == arrow::Type::DECIMAL128 || lhs_type->id() == arrow::Type::DECIMAL256) &&
           (rhs_type->id() == arrow::Type::DECIMAL128 || rhs_type->id() == arrow::Type::DECIMAL256);
  }

  arrow::Result<TypedDatum> Execute(const std::vector<TypedDatum>& args,
                                    arrow::compute::ExecContext* exec_context) const override {
    if (args.size() != 2) {
      return arrow::Status::Invalid("decimal add requires 2 args");
    }
    if (exec_context == nullptr) {
      return arrow::Status::Invalid("exec_context must not be null");
    }

    const auto lhs_type = args[0].datum.type();
    const auto rhs_type = args[1].datum.type();
    if (lhs_type == nullptr || rhs_type == nullptr) {
      return arrow::Status::Invalid("decimal add arg type must not be null");
    }
    ARROW_ASSIGN_OR_RAISE(const auto lhs_spec, GetDecimalSpec(*lhs_type));
    ARROW_ASSIGN_OR_RAISE(const auto rhs_spec, GetDecimalSpec(*rhs_type));
    const auto out_spec = InferDecimalResultSpec(lhs_spec, rhs_spec);
    ARROW_ASSIGN_OR_RAISE(auto out_type, MakeArrowDecimalType(out_spec));

    // The implementation below covers scalar/array/array/scalar in one place by
    // materializing arrays when needed. Scalar-scalar is handled after normalizing
    // chunked arrays.

    const bool lhs_is_scalar = args[0].datum.is_scalar();
    const bool rhs_is_scalar = args[1].datum.is_scalar();
    const bool lhs_is_array = args[0].datum.is_array() || args[0].datum.is_chunked_array();
    const bool rhs_is_array = args[1].datum.is_array() || args[1].datum.is_chunked_array();
    if (!lhs_is_scalar && !lhs_is_array) {
      return arrow::Status::Invalid("unsupported lhs datum kind for decimal add");
    }
    if (!rhs_is_scalar && !rhs_is_array) {
      return arrow::Status::Invalid("unsupported rhs datum kind for decimal add");
    }

    std::shared_ptr<arrow::Array> lhs_array;
    std::shared_ptr<arrow::Array> rhs_array;
    if (lhs_is_array) {
      ARROW_ASSIGN_OR_RAISE(lhs_array, DatumToArray(args[0].datum, exec_context->memory_pool()));
    }
    if (rhs_is_array) {
      ARROW_ASSIGN_OR_RAISE(rhs_array, DatumToArray(args[1].datum, exec_context->memory_pool()));
    }

    int64_t rows = 1;
    if (lhs_array != nullptr) {
      rows = lhs_array->length();
    } else if (rhs_array != nullptr) {
      rows = rhs_array->length();
    }
    if (lhs_array != nullptr && rhs_array != nullptr && lhs_array->length() != rhs_array->length()) {
      return arrow::Status::Invalid("decimal add array length mismatch");
    }

    // Scalar-scalar (re-check after chunked normalization).
    if (lhs_array == nullptr && rhs_array == nullptr) {
      const auto lhs = args[0].datum.scalar();
      const auto rhs = args[1].datum.scalar();
      if (lhs == nullptr || rhs == nullptr) {
        return arrow::Status::Invalid("decimal scalar must not be null");
      }
      if (!lhs->is_valid || !rhs->is_valid) {
        std::shared_ptr<arrow::Scalar> out_scalar;
        if (out_type->id() == arrow::Type::DECIMAL128) {
          out_scalar = std::make_shared<arrow::Decimal128Scalar>(out_type);
        } else {
          out_scalar = std::make_shared<arrow::Decimal256Scalar>(out_type);
        }
        return TypedDatum{arrow::Datum(std::move(out_scalar)), /*field=*/nullptr,
                          MakeDecimalLogicalType(out_spec)};
      }

      if (out_type->id() == arrow::Type::DECIMAL128) {
        const auto& lhs_dec = static_cast<const arrow::Decimal128Scalar&>(*lhs).value;
        const auto& rhs_dec = static_cast<const arrow::Decimal128Scalar&>(*rhs).value;
        auto sum = ScaleTo(lhs_dec, lhs_spec.scale, out_spec.scale) +
                   ScaleTo(rhs_dec, rhs_spec.scale, out_spec.scale);
        if (!FitsInPrecision(sum, out_spec.precision)) {
          return arrow::Status::Invalid("decimal math overflow");
        }
        auto out_scalar = std::make_shared<arrow::Decimal128Scalar>(sum, out_type);
        return TypedDatum{arrow::Datum(std::move(out_scalar)), /*field=*/nullptr,
                          MakeDecimalLogicalType(out_spec)};
      }

      const auto lhs_val = [&]() -> arrow::Decimal256 {
        if (lhs_type->id() == arrow::Type::DECIMAL256) {
          return static_cast<const arrow::Decimal256Scalar&>(*lhs).value;
        }
        return arrow::Decimal256(static_cast<const arrow::Decimal128Scalar&>(*lhs).value);
      }();
      const auto rhs_val = [&]() -> arrow::Decimal256 {
        if (rhs_type->id() == arrow::Type::DECIMAL256) {
          return static_cast<const arrow::Decimal256Scalar&>(*rhs).value;
        }
        return arrow::Decimal256(static_cast<const arrow::Decimal128Scalar&>(*rhs).value);
      }();

      auto sum = ScaleTo(lhs_val, lhs_spec.scale, out_spec.scale) +
                 ScaleTo(rhs_val, rhs_spec.scale, out_spec.scale);
      if (!FitsInPrecision(sum, out_spec.precision)) {
        return arrow::Status::Invalid("decimal math overflow");
      }
      auto out_scalar = std::make_shared<arrow::Decimal256Scalar>(sum, out_type);
      return TypedDatum{arrow::Datum(std::move(out_scalar)), /*field=*/nullptr,
                        MakeDecimalLogicalType(out_spec)};
    }

    // Array path.
    if (out_type->id() == arrow::Type::DECIMAL128) {
      arrow::Decimal128Builder builder(out_type, exec_context->memory_pool());

      const arrow::FixedSizeBinaryArray* lhs_fixed = nullptr;
      const arrow::FixedSizeBinaryArray* rhs_fixed = nullptr;
      if (lhs_array != nullptr) {
        lhs_fixed = static_cast<const arrow::FixedSizeBinaryArray*>(lhs_array.get());
        if (lhs_fixed->byte_width() != 16) {
          return arrow::Status::Invalid("unexpected decimal128 byte width");
        }
      }
      if (rhs_array != nullptr) {
        rhs_fixed = static_cast<const arrow::FixedSizeBinaryArray*>(rhs_array.get());
        if (rhs_fixed->byte_width() != 16) {
          return arrow::Status::Invalid("unexpected decimal128 byte width");
        }
      }

      const auto* lhs_scalar = lhs_is_scalar ? args[0].datum.scalar().get() : nullptr;
      const auto* rhs_scalar = rhs_is_scalar ? args[1].datum.scalar().get() : nullptr;

      const arrow::Decimal128Scalar* lhs_dec_scalar =
          lhs_scalar != nullptr ? static_cast<const arrow::Decimal128Scalar*>(lhs_scalar) : nullptr;
      const arrow::Decimal128Scalar* rhs_dec_scalar =
          rhs_scalar != nullptr ? static_cast<const arrow::Decimal128Scalar*>(rhs_scalar) : nullptr;

      const bool lhs_scalar_valid = lhs_dec_scalar == nullptr ? false : lhs_dec_scalar->is_valid;
      const bool rhs_scalar_valid = rhs_dec_scalar == nullptr ? false : rhs_dec_scalar->is_valid;
      const arrow::Decimal128 lhs_scalar_value =
          lhs_scalar_valid ? lhs_dec_scalar->value : arrow::Decimal128{};
      const arrow::Decimal128 rhs_scalar_value =
          rhs_scalar_valid ? rhs_dec_scalar->value : arrow::Decimal128{};

      ARROW_RETURN_NOT_OK(builder.Reserve(rows));
      for (int64_t i = 0; i < rows; ++i) {
        const bool lhs_null = lhs_fixed != nullptr ? lhs_fixed->IsNull(i) : !lhs_scalar_valid;
        const bool rhs_null = rhs_fixed != nullptr ? rhs_fixed->IsNull(i) : !rhs_scalar_valid;
        if (lhs_null || rhs_null) {
          builder.UnsafeAppendNull();
          continue;
        }

        const arrow::Decimal128 lhs_val = lhs_fixed != nullptr
                                              ? arrow::Decimal128(reinterpret_cast<const uint8_t*>(lhs_fixed->GetValue(i)))
                                              : lhs_scalar_value;
        const arrow::Decimal128 rhs_val = rhs_fixed != nullptr
                                              ? arrow::Decimal128(reinterpret_cast<const uint8_t*>(rhs_fixed->GetValue(i)))
                                              : rhs_scalar_value;

        auto sum = ScaleTo(lhs_val, lhs_spec.scale, out_spec.scale) +
                   ScaleTo(rhs_val, rhs_spec.scale, out_spec.scale);
        if (!FitsInPrecision(sum, out_spec.precision)) {
          return arrow::Status::Invalid("decimal math overflow");
        }
        builder.UnsafeAppend(sum);
      }

      std::shared_ptr<arrow::Array> out;
      ARROW_RETURN_NOT_OK(builder.Finish(&out));
      return TypedDatum{arrow::Datum(std::move(out)), /*field=*/nullptr,
                        MakeDecimalLogicalType(out_spec)};
    }

    arrow::Decimal256Builder builder(out_type, exec_context->memory_pool());

    const arrow::FixedSizeBinaryArray* lhs_fixed = nullptr;
    const arrow::FixedSizeBinaryArray* rhs_fixed = nullptr;
    if (lhs_array != nullptr) {
      lhs_fixed = static_cast<const arrow::FixedSizeBinaryArray*>(lhs_array.get());
      if (lhs_fixed->byte_width() != (lhs_type->id() == arrow::Type::DECIMAL256 ? 32 : 16)) {
        return arrow::Status::Invalid("unexpected decimal byte width (lhs)");
      }
    }
    if (rhs_array != nullptr) {
      rhs_fixed = static_cast<const arrow::FixedSizeBinaryArray*>(rhs_array.get());
      if (rhs_fixed->byte_width() != (rhs_type->id() == arrow::Type::DECIMAL256 ? 32 : 16)) {
        return arrow::Status::Invalid("unexpected decimal byte width (rhs)");
      }
    }

    const auto* lhs_scalar = lhs_is_scalar ? args[0].datum.scalar().get() : nullptr;
    const auto* rhs_scalar = rhs_is_scalar ? args[1].datum.scalar().get() : nullptr;
    const bool lhs_scalar_valid = lhs_scalar != nullptr ? lhs_scalar->is_valid : false;
    const bool rhs_scalar_valid = rhs_scalar != nullptr ? rhs_scalar->is_valid : false;

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

    ARROW_RETURN_NOT_OK(builder.Reserve(rows));
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
        const auto* ptr = reinterpret_cast<const uint8_t*>(lhs_fixed->GetValue(i));
        if (lhs_type->id() == arrow::Type::DECIMAL256) {
          return arrow::Decimal256(ptr);
        }
        return arrow::Decimal256(arrow::Decimal128(ptr));
      }();

      const auto rhs_val = [&]() -> arrow::Decimal256 {
        if (rhs_fixed == nullptr) {
          return rhs_scalar_value;
        }
        const auto* ptr = reinterpret_cast<const uint8_t*>(rhs_fixed->GetValue(i));
        if (rhs_type->id() == arrow::Type::DECIMAL256) {
          return arrow::Decimal256(ptr);
        }
        return arrow::Decimal256(arrow::Decimal128(ptr));
      }();

      auto sum = ScaleTo(lhs_val, lhs_spec.scale, out_spec.scale) +
                 ScaleTo(rhs_val, rhs_spec.scale, out_spec.scale);
      if (!FitsInPrecision(sum, out_spec.precision)) {
        return arrow::Status::Invalid("decimal math overflow");
      }
      builder.UnsafeAppend(sum);
    }

    std::shared_ptr<arrow::Array> out;
    ARROW_RETURN_NOT_OK(builder.Finish(&out));
    return TypedDatum{arrow::Datum(std::move(out)), /*field=*/nullptr,
                      MakeDecimalLogicalType(out_spec)};
  }
};

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

class CollatedBinaryCompareFunction final : public Function {
 public:
  explicit CollatedBinaryCompareFunction(CompareOp op) : op_(op) {}

  bool CanExecute(const std::vector<TypedDatum>& args) const override {
    if (args.size() != 2) {
      return false;
    }
    if (args[0].logical_type.id != LogicalTypeId::kString &&
        args[1].logical_type.id != LogicalTypeId::kString) {
      return false;
    }
    const auto lhs_type = args[0].datum.type();
    const auto rhs_type = args[1].datum.type();
    if (lhs_type == nullptr || rhs_type == nullptr) {
      return false;
    }
    return IsBinaryLike(lhs_type->id()) && IsBinaryLike(rhs_type->id());
  }

  arrow::Result<TypedDatum> Execute(const std::vector<TypedDatum>& args,
                                    arrow::compute::ExecContext* exec_context) const override {
    if (args.size() != 2) {
      return arrow::Status::Invalid("string compare requires 2 args");
    }
    if (exec_context == nullptr) {
      return arrow::Status::Invalid("exec_context must not be null");
    }

    ARROW_ASSIGN_OR_RAISE(const auto collation, ResolveStringCollation(args));

    const auto* lhs_scalar =
        args[0].datum.is_scalar() ? dynamic_cast<const arrow::BaseBinaryScalar*>(args[0].datum.scalar().get())
                                  : nullptr;
    const auto* rhs_scalar =
        args[1].datum.is_scalar() ? dynamic_cast<const arrow::BaseBinaryScalar*>(args[1].datum.scalar().get())
                                  : nullptr;

    std::shared_ptr<arrow::Array> lhs_array;
    std::shared_ptr<arrow::Array> rhs_array;
    if (args[0].datum.is_array() || args[0].datum.is_chunked_array()) {
      ARROW_ASSIGN_OR_RAISE(lhs_array, DatumToArray(args[0].datum, exec_context->memory_pool()));
    }
    if (args[1].datum.is_array() || args[1].datum.is_chunked_array()) {
      ARROW_ASSIGN_OR_RAISE(rhs_array, DatumToArray(args[1].datum, exec_context->memory_pool()));
    }

    if (lhs_array == nullptr && rhs_array == nullptr) {
      if (lhs_scalar == nullptr || rhs_scalar == nullptr) {
        return arrow::Status::Invalid("expected binary-like scalars for string compare");
      }
      if (!lhs_scalar->is_valid || !rhs_scalar->is_valid) {
        return TypedDatum{arrow::Datum(std::make_shared<arrow::BooleanScalar>()),
                          /*field=*/nullptr, LogicalType{}};
      }
      const int cmp = CompareString(collation, lhs_scalar->view(), rhs_scalar->view());
      ARROW_ASSIGN_OR_RAISE(const bool keep, ApplyCompare(op_, cmp));
      return TypedDatum{arrow::Datum(std::make_shared<arrow::BooleanScalar>(keep)),
                        /*field=*/nullptr, LogicalType{}};
    }

    int64_t rows = 0;
    if (lhs_array != nullptr) {
      rows = lhs_array->length();
    } else {
      rows = rhs_array->length();
    }
    if (lhs_array != nullptr && rhs_array != nullptr && lhs_array->length() != rhs_array->length()) {
      return arrow::Status::Invalid("string compare array length mismatch");
    }

    arrow::BooleanBuilder out_builder(exec_context->memory_pool());
    ARROW_RETURN_NOT_OK(out_builder.Reserve(rows));

    const bool lhs_scalar_valid = lhs_scalar != nullptr && lhs_scalar->is_valid;
    const bool rhs_scalar_valid = rhs_scalar != nullptr && rhs_scalar->is_valid;
    const std::string_view lhs_scalar_view = lhs_scalar_valid ? lhs_scalar->view() : std::string_view{};
    const std::string_view rhs_scalar_view = rhs_scalar_valid ? rhs_scalar->view() : std::string_view{};

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
      ARROW_ASSIGN_OR_RAISE(const bool keep, ApplyCompare(op_, cmp));
      out_builder.UnsafeAppend(keep);
    }

    std::shared_ptr<arrow::Array> out;
    ARROW_RETURN_NOT_OK(out_builder.Finish(&out));
    return TypedDatum{arrow::Datum(std::move(out)), /*field=*/nullptr, LogicalType{}};
  }

 private:
  CompareOp op_;
};

}  // namespace

std::shared_ptr<FunctionRegistry> FunctionRegistry::MakeDefault() {
  auto registry = std::make_shared<FunctionRegistry>();
  registry->Register("add", std::make_shared<DecimalAddFunction>());

  registry->Register("equal",
                     std::make_shared<CollatedBinaryCompareFunction>(CompareOp::kEqual));
  registry->Register("not_equal",
                     std::make_shared<CollatedBinaryCompareFunction>(CompareOp::kNotEqual));
  registry->Register("less",
                     std::make_shared<CollatedBinaryCompareFunction>(CompareOp::kLess));
  registry->Register("less_equal",
                     std::make_shared<CollatedBinaryCompareFunction>(CompareOp::kLessEqual));
  registry->Register("greater",
                     std::make_shared<CollatedBinaryCompareFunction>(CompareOp::kGreater));
  registry->Register("greater_equal",
                     std::make_shared<CollatedBinaryCompareFunction>(CompareOp::kGreaterEqual));

  return registry;
}

void FunctionRegistry::Register(std::string name, std::shared_ptr<Function> function) {
  functions_[std::move(name)].overloads.push_back(std::move(function));
}

arrow::Result<TypedDatum> FunctionRegistry::Call(const std::string& name,
                                                 const std::vector<TypedDatum>& args,
                                                 arrow::compute::ExecContext* exec_context) const {
  if (exec_context == nullptr) {
    return arrow::Status::Invalid("exec_context must not be null");
  }

  if (auto it = functions_.find(name); it != functions_.end()) {
    for (const auto& overload : it->second.overloads) {
      if (overload == nullptr) {
        continue;
      }
      if (!overload->CanExecute(args)) {
        continue;
      }
      return overload->Execute(args, exec_context);
    }
  }

  // Default: delegate to Arrow compute (only when no TiForth override matches).
  ARROW_RETURN_NOT_OK(detail::EnsureArrowComputeInitialized());

  std::vector<arrow::Datum> arrow_args;
  arrow_args.reserve(args.size());
  for (const auto& arg : args) {
    arrow_args.push_back(arg.datum);
  }

  ARROW_ASSIGN_OR_RAISE(auto out,
                        arrow::compute::CallFunction(name, arrow_args, /*options=*/nullptr,
                                                     exec_context));
  return TypedDatum{std::move(out), /*field=*/nullptr, LogicalType{}};
}

}  // namespace tiforth
