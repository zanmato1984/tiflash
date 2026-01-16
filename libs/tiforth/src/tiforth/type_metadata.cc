#include "tiforth/type_metadata.h"

#include <charconv>

#include <arrow/util/key_value_metadata.h>
#include <arrow/status.h>

namespace tiforth {

arrow::Result<int32_t> ParseInt32(std::string_view value, std::string_view key) {
  int32_t out = 0;
  const char* begin = value.data();
  const char* end = value.data() + value.size();
  auto result = std::from_chars(begin, end, out);
  if (result.ec != std::errc{} || result.ptr != end) {
    return arrow::Status::Invalid("invalid int32 metadata for ", std::string(key), ": '",
                                  std::string(value), "'");
  }
  return out;
}

std::optional<std::string_view> GetMetadataValue(const arrow::Field& field,
                                                 std::string_view key) {
  const auto& metadata = field.metadata();
  if (metadata == nullptr) {
    return std::nullopt;
  }
  const int index = metadata->FindKey(std::string(key));
  if (index < 0) {
    return std::nullopt;
  }
  return std::string_view(metadata->value(index));
}

namespace {

LogicalTypeId ParseLogicalTypeId(std::string_view value) {
  if (value == "decimal") {
    return LogicalTypeId::kDecimal;
  }
  if (value == "mydate") {
    return LogicalTypeId::kMyDate;
  }
  if (value == "mydatetime") {
    return LogicalTypeId::kMyDateTime;
  }
  if (value == "string") {
    return LogicalTypeId::kString;
  }
  return LogicalTypeId::kUnknown;
}

arrow::Status AppendMetadata(std::vector<std::string>* keys, std::vector<std::string>* values,
                            std::string_view key, std::string value) {
  if (keys == nullptr || values == nullptr) {
    return arrow::Status::Invalid("internal error: metadata output vectors must not be null");
  }
  keys->push_back(std::string(key));
  values->push_back(std::move(value));
  return arrow::Status::OK();
}

}  // namespace

arrow::Result<LogicalType> GetLogicalType(const arrow::Field& field) {
  LogicalType out;

  const auto logical_type_val = GetMetadataValue(field, kMetaLogicalType);
  if (!logical_type_val.has_value()) {
    return out;
  }

  out.id = ParseLogicalTypeId(*logical_type_val);
  switch (out.id) {
    case LogicalTypeId::kUnknown:
      return out;
    case LogicalTypeId::kDecimal: {
      // Prefer Arrow decimal type params when present; metadata is optional.
      if (field.type() != nullptr &&
          (field.type()->id() == arrow::Type::DECIMAL128 ||
           field.type()->id() == arrow::Type::DECIMAL256)) {
        const auto& dec = static_cast<const arrow::DecimalType&>(*field.type());
        out.decimal_precision = static_cast<int32_t>(dec.precision());
        out.decimal_scale = static_cast<int32_t>(dec.scale());
      }

      if (const auto p = GetMetadataValue(field, kMetaDecimalPrecision); p.has_value()) {
        ARROW_ASSIGN_OR_RAISE(out.decimal_precision, ParseInt32(*p, kMetaDecimalPrecision));
      }
      if (const auto s = GetMetadataValue(field, kMetaDecimalScale); s.has_value()) {
        ARROW_ASSIGN_OR_RAISE(out.decimal_scale, ParseInt32(*s, kMetaDecimalScale));
      }
      return out;
    }
    case LogicalTypeId::kMyDate:
      return out;
    case LogicalTypeId::kMyDateTime: {
      if (const auto fsp = GetMetadataValue(field, kMetaDateTimeFsp); fsp.has_value()) {
        ARROW_ASSIGN_OR_RAISE(out.datetime_fsp, ParseInt32(*fsp, kMetaDateTimeFsp));
      }
      return out;
    }
    case LogicalTypeId::kString: {
      if (const auto coll = GetMetadataValue(field, kMetaStringCollationId); coll.has_value()) {
        ARROW_ASSIGN_OR_RAISE(out.collation_id, ParseInt32(*coll, kMetaStringCollationId));
      }
      return out;
    }
  }
  return out;
}

arrow::Result<std::shared_ptr<arrow::Field>> WithLogicalTypeMetadata(
    const std::shared_ptr<arrow::Field>& field, const LogicalType& type) {
  if (field == nullptr) {
    return arrow::Status::Invalid("field must not be null");
  }

  std::vector<std::string> keys;
  std::vector<std::string> values;

  if (const auto& metadata = field->metadata(); metadata != nullptr) {
    keys.reserve(metadata->size() + 8);
    values.reserve(metadata->size() + 8);
    for (int i = 0; i < metadata->size(); ++i) {
      keys.push_back(metadata->key(i));
      values.push_back(metadata->value(i));
    }
  } else {
    keys.reserve(8);
    values.reserve(8);
  }

  const auto set_logical_type = [&](std::string_view name) -> arrow::Status {
    return AppendMetadata(&keys, &values, kMetaLogicalType, std::string(name));
  };

  switch (type.id) {
    case LogicalTypeId::kUnknown:
      return field;
    case LogicalTypeId::kDecimal: {
      ARROW_RETURN_NOT_OK(set_logical_type("decimal"));
      if (type.decimal_precision >= 0) {
        ARROW_RETURN_NOT_OK(AppendMetadata(&keys, &values, kMetaDecimalPrecision,
                                           std::to_string(type.decimal_precision)));
      }
      if (type.decimal_scale >= 0) {
        ARROW_RETURN_NOT_OK(
            AppendMetadata(&keys, &values, kMetaDecimalScale, std::to_string(type.decimal_scale)));
      }
      break;
    }
    case LogicalTypeId::kMyDate:
      ARROW_RETURN_NOT_OK(set_logical_type("mydate"));
      break;
    case LogicalTypeId::kMyDateTime: {
      ARROW_RETURN_NOT_OK(set_logical_type("mydatetime"));
      if (type.datetime_fsp >= 0) {
        ARROW_RETURN_NOT_OK(
            AppendMetadata(&keys, &values, kMetaDateTimeFsp, std::to_string(type.datetime_fsp)));
      }
      break;
    }
    case LogicalTypeId::kString: {
      ARROW_RETURN_NOT_OK(set_logical_type("string"));
      if (type.collation_id >= 0) {
        ARROW_RETURN_NOT_OK(AppendMetadata(&keys, &values, kMetaStringCollationId,
                                           std::to_string(type.collation_id)));
      }
      break;
    }
  }

  auto metadata = std::make_shared<arrow::KeyValueMetadata>(std::move(keys), std::move(values));
  return field->WithMetadata(std::move(metadata));
}

}  // namespace tiforth
