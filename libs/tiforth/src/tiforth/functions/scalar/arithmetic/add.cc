#include <algorithm>
#include <cstdint>
#include <memory>
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

bool IsIntegerType(arrow::Type::type type_id) {
  switch (type_id) {
    case arrow::Type::INT8:
    case arrow::Type::UINT8:
    case arrow::Type::INT16:
    case arrow::Type::UINT16:
    case arrow::Type::INT32:
    case arrow::Type::UINT32:
    case arrow::Type::INT64:
    case arrow::Type::UINT64:
      return true;
    default:
      return false;
  }
}

arrow::Result<DecimalSpec> GetDecimalOrIntegerSpec(const arrow::DataType& type) {
  if (type.id() == arrow::Type::DECIMAL128 || type.id() == arrow::Type::DECIMAL256) {
    return GetDecimalSpec(type);
  }
  if (!IsIntegerType(type.id())) {
    return arrow::Status::Invalid("expected decimal or integer type");
  }
  ARROW_ASSIGN_OR_RAISE(const auto precision, arrow::MaxDecimalDigitsForInteger(type.id()));
  return DecimalSpec{precision, /*scale=*/0};
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
  const auto lhs_type_id = types[0].type->id();
  const auto rhs_type_id = types[1].type->id();
  if (lhs_type_id != arrow::Type::DECIMAL128 && lhs_type_id != arrow::Type::DECIMAL256 &&
      rhs_type_id != arrow::Type::DECIMAL128 && rhs_type_id != arrow::Type::DECIMAL256) {
    return arrow::Status::Invalid("decimal add requires at least one decimal argument");
  }

  ARROW_ASSIGN_OR_RAISE(const auto lhs_spec, GetDecimalOrIntegerSpec(*types[0].type));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_spec, GetDecimalOrIntegerSpec(*types[1].type));
  const auto out_spec = InferDecimalResultSpec(lhs_spec, rhs_spec);
  ARROW_ASSIGN_OR_RAISE(auto out_type, MakeArrowDecimalType(out_spec));
  return arrow::TypeHolder(std::move(out_type));
}

template <typename OutDecimal, typename InDecimal, typename InScalar, int ExpectedByteWidth>
struct DecimalReader {
  const arrow::FixedSizeBinaryArray* array = nullptr;
  const InScalar* scalar = nullptr;
  bool scalar_valid = false;
  InDecimal scalar_value{};
  int32_t from_scale = 0;
  int32_t to_scale = 0;

  arrow::Status Init(const std::shared_ptr<arrow::Array>& input_array, const arrow::Scalar* input_scalar,
                     int32_t from_scale_, int32_t to_scale_) {
    array = input_array != nullptr ? static_cast<const arrow::FixedSizeBinaryArray*>(input_array.get()) : nullptr;
    if (array != nullptr && array->byte_width() != ExpectedByteWidth) {
      return arrow::Status::Invalid("unexpected decimal byte width");
    }
    scalar = input_scalar != nullptr ? dynamic_cast<const InScalar*>(input_scalar) : nullptr;
    if (input_scalar != nullptr && scalar == nullptr) {
      return arrow::Status::Invalid("unexpected decimal scalar type");
    }
    scalar_valid = scalar != nullptr && scalar->is_valid;
    scalar_value = scalar_valid ? scalar->value : InDecimal{};
    from_scale = from_scale_;
    to_scale = to_scale_;
    return arrow::Status::OK();
  }

  bool IsNull(int64_t i) const { return array != nullptr ? array->IsNull(i) : !scalar_valid; }

  OutDecimal Value(int64_t i) const {
    const InDecimal raw =
        array != nullptr ? InDecimal(reinterpret_cast<const uint8_t*>(array->GetValue(i))) : scalar_value;
    const OutDecimal casted = OutDecimal(raw);
    return ScaleTo(casted, from_scale, to_scale);
  }
};

template <typename OutDecimal, typename InArray, typename InScalar, typename InValue>
struct IntegerReader {
  const InArray* array = nullptr;
  const InScalar* scalar = nullptr;
  bool scalar_valid = false;
  InValue scalar_value{};
  int32_t to_scale = 0;

  arrow::Status Init(const std::shared_ptr<arrow::Array>& input_array, const arrow::Scalar* input_scalar,
                     int32_t to_scale_) {
    array = input_array != nullptr ? static_cast<const InArray*>(input_array.get()) : nullptr;
    scalar = input_scalar != nullptr ? dynamic_cast<const InScalar*>(input_scalar) : nullptr;
    if (input_scalar != nullptr && scalar == nullptr) {
      return arrow::Status::Invalid("unexpected integer scalar type");
    }
    scalar_valid = scalar != nullptr && scalar->is_valid;
    scalar_value = scalar_valid ? scalar->value : InValue{};
    to_scale = to_scale_;
    return arrow::Status::OK();
  }

  bool IsNull(int64_t i) const { return array != nullptr ? array->IsNull(i) : !scalar_valid; }

  OutDecimal Value(int64_t i) const {
    const InValue raw = array != nullptr ? array->Value(i) : scalar_value;
    return ScaleTo(OutDecimal(raw), /*from_scale=*/0, to_scale);
  }
};

template <typename OutDecimal, typename BuilderT, typename LhsReader, typename RhsReader>
arrow::Status ExecDecimalAddLoop(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                 arrow::compute::ExecResult* out, const DecimalSpec& out_spec,
                                 const std::shared_ptr<arrow::DataType>& out_type, LhsReader lhs, RhsReader rhs) {
  if (ctx == nullptr || out == nullptr) {
    return arrow::Status::Invalid("kernel context/result must not be null");
  }

  const int64_t rows = batch.length;
  BuilderT builder(out_type, ctx->memory_pool());
  ARROW_RETURN_NOT_OK(builder.Reserve(rows));

  for (int64_t i = 0; i < rows; ++i) {
    if (lhs.IsNull(i) || rhs.IsNull(i)) {
      builder.UnsafeAppendNull();
      continue;
    }

    const auto sum = lhs.Value(i) + rhs.Value(i);
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
  if (lhs_type->id() != arrow::Type::DECIMAL128 && lhs_type->id() != arrow::Type::DECIMAL256 &&
      rhs_type->id() != arrow::Type::DECIMAL128 && rhs_type->id() != arrow::Type::DECIMAL256) {
    return arrow::Status::Invalid("decimal add requires at least one decimal argument");
  }

  ARROW_ASSIGN_OR_RAISE(const auto lhs_spec, GetDecimalOrIntegerSpec(*lhs_type));
  ARROW_ASSIGN_OR_RAISE(const auto rhs_spec, GetDecimalOrIntegerSpec(*rhs_type));
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

  if (out_type->id() == arrow::Type::DECIMAL128) {
    using OutDecimal = arrow::Decimal128;
    using OutBuilder = arrow::Decimal128Builder;
    using DecReader = DecimalReader<OutDecimal, arrow::Decimal128, arrow::Decimal128Scalar, 16>;

    const auto lhs_id = lhs_type->id();
    const auto rhs_id = rhs_type->id();
    if (lhs_id == arrow::Type::DECIMAL128 && rhs_id == arrow::Type::DECIMAL128) {
      DecReader lhs;
      DecReader rhs;
      ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, lhs_spec.scale, out_spec.scale));
      ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, rhs_spec.scale, out_spec.scale));
      return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
    }

    if (lhs_id == arrow::Type::DECIMAL128 && IsIntegerType(rhs_id)) {
      DecReader lhs;
      ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, lhs_spec.scale, out_spec.scale));

      switch (rhs_id) {
        case arrow::Type::INT8: {
          IntegerReader<OutDecimal, arrow::Int8Array, arrow::Int8Scalar, int8_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT8: {
          IntegerReader<OutDecimal, arrow::UInt8Array, arrow::UInt8Scalar, uint8_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT16: {
          IntegerReader<OutDecimal, arrow::Int16Array, arrow::Int16Scalar, int16_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT16: {
          IntegerReader<OutDecimal, arrow::UInt16Array, arrow::UInt16Scalar, uint16_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT32: {
          IntegerReader<OutDecimal, arrow::Int32Array, arrow::Int32Scalar, int32_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT32: {
          IntegerReader<OutDecimal, arrow::UInt32Array, arrow::UInt32Scalar, uint32_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT64: {
          IntegerReader<OutDecimal, arrow::Int64Array, arrow::Int64Scalar, int64_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT64: {
          IntegerReader<OutDecimal, arrow::UInt64Array, arrow::UInt64Scalar, uint64_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        default:
          break;
      }
    }

    if (rhs_id == arrow::Type::DECIMAL128 && IsIntegerType(lhs_id)) {
      DecReader rhs;
      ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, rhs_spec.scale, out_spec.scale));

      switch (lhs_id) {
        case arrow::Type::INT8: {
          IntegerReader<OutDecimal, arrow::Int8Array, arrow::Int8Scalar, int8_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT8: {
          IntegerReader<OutDecimal, arrow::UInt8Array, arrow::UInt8Scalar, uint8_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT16: {
          IntegerReader<OutDecimal, arrow::Int16Array, arrow::Int16Scalar, int16_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT16: {
          IntegerReader<OutDecimal, arrow::UInt16Array, arrow::UInt16Scalar, uint16_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT32: {
          IntegerReader<OutDecimal, arrow::Int32Array, arrow::Int32Scalar, int32_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT32: {
          IntegerReader<OutDecimal, arrow::UInt32Array, arrow::UInt32Scalar, uint32_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT64: {
          IntegerReader<OutDecimal, arrow::Int64Array, arrow::Int64Scalar, int64_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT64: {
          IntegerReader<OutDecimal, arrow::UInt64Array, arrow::UInt64Scalar, uint64_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        default:
          break;
      }
    }

    return arrow::Status::Invalid("unsupported decimal128 add signature");
  }

  using OutDecimal = arrow::Decimal256;
  using OutBuilder = arrow::Decimal256Builder;
  using Dec128Reader = DecimalReader<OutDecimal, arrow::Decimal128, arrow::Decimal128Scalar, 16>;
  using Dec256Reader = DecimalReader<OutDecimal, arrow::Decimal256, arrow::Decimal256Scalar, 32>;

  const auto lhs_id = lhs_type->id();
  const auto rhs_id = rhs_type->id();

  if (lhs_id == arrow::Type::DECIMAL256) {
    Dec256Reader lhs;
    ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, lhs_spec.scale, out_spec.scale));

    if (rhs_id == arrow::Type::DECIMAL256) {
      Dec256Reader rhs;
      ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, rhs_spec.scale, out_spec.scale));
      return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
    }
    if (rhs_id == arrow::Type::DECIMAL128) {
      Dec128Reader rhs;
      ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, rhs_spec.scale, out_spec.scale));
      return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
    }
    if (IsIntegerType(rhs_id)) {
      switch (rhs_id) {
        case arrow::Type::INT8: {
          IntegerReader<OutDecimal, arrow::Int8Array, arrow::Int8Scalar, int8_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT8: {
          IntegerReader<OutDecimal, arrow::UInt8Array, arrow::UInt8Scalar, uint8_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT16: {
          IntegerReader<OutDecimal, arrow::Int16Array, arrow::Int16Scalar, int16_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT16: {
          IntegerReader<OutDecimal, arrow::UInt16Array, arrow::UInt16Scalar, uint16_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT32: {
          IntegerReader<OutDecimal, arrow::Int32Array, arrow::Int32Scalar, int32_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT32: {
          IntegerReader<OutDecimal, arrow::UInt32Array, arrow::UInt32Scalar, uint32_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT64: {
          IntegerReader<OutDecimal, arrow::Int64Array, arrow::Int64Scalar, int64_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT64: {
          IntegerReader<OutDecimal, arrow::UInt64Array, arrow::UInt64Scalar, uint64_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        default:
          break;
      }
    }
  }

  if (lhs_id == arrow::Type::DECIMAL128) {
    Dec128Reader lhs;
    ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, lhs_spec.scale, out_spec.scale));

    if (rhs_id == arrow::Type::DECIMAL256) {
      Dec256Reader rhs;
      ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, rhs_spec.scale, out_spec.scale));
      return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
    }
    if (rhs_id == arrow::Type::DECIMAL128) {
      Dec128Reader rhs;
      ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, rhs_spec.scale, out_spec.scale));
      return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
    }
    if (IsIntegerType(rhs_id)) {
      switch (rhs_id) {
        case arrow::Type::INT8: {
          IntegerReader<OutDecimal, arrow::Int8Array, arrow::Int8Scalar, int8_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT8: {
          IntegerReader<OutDecimal, arrow::UInt8Array, arrow::UInt8Scalar, uint8_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT16: {
          IntegerReader<OutDecimal, arrow::Int16Array, arrow::Int16Scalar, int16_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT16: {
          IntegerReader<OutDecimal, arrow::UInt16Array, arrow::UInt16Scalar, uint16_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT32: {
          IntegerReader<OutDecimal, arrow::Int32Array, arrow::Int32Scalar, int32_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT32: {
          IntegerReader<OutDecimal, arrow::UInt32Array, arrow::UInt32Scalar, uint32_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT64: {
          IntegerReader<OutDecimal, arrow::Int64Array, arrow::Int64Scalar, int64_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT64: {
          IntegerReader<OutDecimal, arrow::UInt64Array, arrow::UInt64Scalar, uint64_t> rhs;
          ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        default:
          break;
      }
    }
  }

  if (IsIntegerType(lhs_id) && (rhs_id == arrow::Type::DECIMAL128 || rhs_id == arrow::Type::DECIMAL256)) {
    if (rhs_id == arrow::Type::DECIMAL128) {
      Dec128Reader rhs;
      ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, rhs_spec.scale, out_spec.scale));
      switch (lhs_id) {
        case arrow::Type::INT8: {
          IntegerReader<OutDecimal, arrow::Int8Array, arrow::Int8Scalar, int8_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT8: {
          IntegerReader<OutDecimal, arrow::UInt8Array, arrow::UInt8Scalar, uint8_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT16: {
          IntegerReader<OutDecimal, arrow::Int16Array, arrow::Int16Scalar, int16_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT16: {
          IntegerReader<OutDecimal, arrow::UInt16Array, arrow::UInt16Scalar, uint16_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT32: {
          IntegerReader<OutDecimal, arrow::Int32Array, arrow::Int32Scalar, int32_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT32: {
          IntegerReader<OutDecimal, arrow::UInt32Array, arrow::UInt32Scalar, uint32_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT64: {
          IntegerReader<OutDecimal, arrow::Int64Array, arrow::Int64Scalar, int64_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT64: {
          IntegerReader<OutDecimal, arrow::UInt64Array, arrow::UInt64Scalar, uint64_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        default:
          break;
      }
    } else {
      Dec256Reader rhs;
      ARROW_RETURN_NOT_OK(rhs.Init(rhs_array, rhs_scalar, rhs_spec.scale, out_spec.scale));
      switch (lhs_id) {
        case arrow::Type::INT8: {
          IntegerReader<OutDecimal, arrow::Int8Array, arrow::Int8Scalar, int8_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT8: {
          IntegerReader<OutDecimal, arrow::UInt8Array, arrow::UInt8Scalar, uint8_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT16: {
          IntegerReader<OutDecimal, arrow::Int16Array, arrow::Int16Scalar, int16_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT16: {
          IntegerReader<OutDecimal, arrow::UInt16Array, arrow::UInt16Scalar, uint16_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT32: {
          IntegerReader<OutDecimal, arrow::Int32Array, arrow::Int32Scalar, int32_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT32: {
          IntegerReader<OutDecimal, arrow::UInt32Array, arrow::UInt32Scalar, uint32_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::INT64: {
          IntegerReader<OutDecimal, arrow::Int64Array, arrow::Int64Scalar, int64_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        case arrow::Type::UINT64: {
          IntegerReader<OutDecimal, arrow::UInt64Array, arrow::UInt64Scalar, uint64_t> lhs;
          ARROW_RETURN_NOT_OK(lhs.Init(lhs_array, lhs_scalar, out_spec.scale));
          return ExecDecimalAddLoop<OutDecimal, OutBuilder>(ctx, batch, out, out_spec, out_type, lhs, rhs);
        }
        default:
          break;
      }
    }
  }

  return arrow::Status::Invalid("unsupported decimal256 add signature");
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

    const bool lhs_integer = lhs_type != nullptr && IsIntegerType(lhs_type->id());
    const bool rhs_integer = rhs_type != nullptr && IsIntegerType(rhs_type->id());
    if (lhs_decimal && (rhs_decimal || rhs_integer)) {
      return arrow::compute::CallFunction("tiforth.decimal_add", args, /*options=*/nullptr, ctx);
    }
    if (rhs_decimal && lhs_integer) {
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
  for (auto int_id : {arrow::Type::INT8, arrow::Type::UINT8, arrow::Type::INT16, arrow::Type::UINT16,
                      arrow::Type::INT32, arrow::Type::UINT32, arrow::Type::INT64, arrow::Type::UINT64}) {
    ARROW_RETURN_NOT_OK(decimal_add->AddKernel(make_kernel(arrow::Type::DECIMAL128, int_id)));
    ARROW_RETURN_NOT_OK(decimal_add->AddKernel(make_kernel(int_id, arrow::Type::DECIMAL128)));
    ARROW_RETURN_NOT_OK(decimal_add->AddKernel(make_kernel(arrow::Type::DECIMAL256, int_id)));
    ARROW_RETURN_NOT_OK(decimal_add->AddKernel(make_kernel(int_id, arrow::Type::DECIMAL256)));
  }

  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(decimal_add), /*allow_overwrite=*/true));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::make_shared<TiforthAddMetaFunction>(fallback_registry),
                                            /*allow_overwrite=*/true));
  return arrow::Status::OK();
}

}  // namespace tiforth::functions
