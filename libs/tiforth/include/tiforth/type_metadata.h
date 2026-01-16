#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <arrow/result.h>
#include <arrow/type.h>

namespace tiforth {

// Arrow Field metadata keys used by TiForth to preserve TiFlash/TiDB logical type semantics.
inline constexpr std::string_view kMetaLogicalType = "tiforth.logical_type";
inline constexpr std::string_view kMetaDecimalPrecision = "tiforth.decimal.precision";
inline constexpr std::string_view kMetaDecimalScale = "tiforth.decimal.scale";
inline constexpr std::string_view kMetaDateTimeFsp = "tiforth.datetime.fsp";
inline constexpr std::string_view kMetaStringCollationId = "tiforth.string.collation_id";

enum class LogicalTypeId {
  kUnknown,
  kDecimal,
  kMyDate,
  kMyDateTime,
  kString,
};

struct LogicalType {
  LogicalTypeId id = LogicalTypeId::kUnknown;

  // Decimal: precision/scale.
  int32_t decimal_precision = -1;
  int32_t decimal_scale = -1;

  // MyDateTime: fsp (0..6).
  int32_t datetime_fsp = -1;

  // String: TiDB collation id.
  int32_t collation_id = -1;
};

arrow::Result<int32_t> ParseInt32(std::string_view value, std::string_view key);
std::optional<std::string_view> GetMetadataValue(const arrow::Field& field, std::string_view key);

arrow::Result<LogicalType> GetLogicalType(const arrow::Field& field);

arrow::Result<std::shared_ptr<arrow::Field>> WithLogicalTypeMetadata(
    const std::shared_ptr<arrow::Field>& field, const LogicalType& type);

}  // namespace tiforth

