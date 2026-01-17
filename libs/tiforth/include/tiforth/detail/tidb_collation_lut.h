#pragma once

#include <array>
#include <cstdint>

namespace tiforth::tidb::GeneralCI {

using WeightType = std::uint16_t;

extern const std::array<WeightType, 256 * 256> weight_lut;

}  // namespace tiforth::tidb::GeneralCI

namespace tiforth::tidb::UnicodeCI {

extern const std::array<std::uint64_t, 256 * 256 + 1> weight_lut_0400;
extern const std::array<std::uint64_t, 0x2CEA1> weight_lut_0900;

}  // namespace tiforth::tidb::UnicodeCI

