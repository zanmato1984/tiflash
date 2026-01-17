#pragma once

#include <array>
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <string_view>

#include "tiforth/detail/tidb_collation_lut.h"

namespace tiforth {

enum class CollationKind {
  kBinary,
  kPaddingBinary,
  kGeneralCi,
  kUnicodeCi0400,
  kUnicodeCi0900,
  kUnsupported,
};

struct Collation {
  int32_t id = -1;
  CollationKind kind = CollationKind::kUnsupported;
};

Collation CollationFromId(int32_t id);

inline std::string_view RightTrimAsciiSpace(std::string_view value) {
  std::size_t end = value.size();
  while (end > 0 && value[end - 1] == ' ') {
    --end;
  }
  return end == value.size() ? value : value.substr(0, end);
}

inline int Signum(int v) { return (0 < v) - (v < 0); }

inline int CompareBinary(std::string_view lhs, std::string_view rhs) {
  const std::size_t min_len = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
  if (min_len > 0) {
    const int cmp = std::memcmp(lhs.data(), rhs.data(), min_len);
    if (cmp != 0) {
      return cmp;
    }
  }
  if (lhs.size() == rhs.size()) {
    return 0;
  }
  return lhs.size() < rhs.size() ? -1 : 1;
}

using Rune = int32_t;

inline Rune DecodeUtf8Char(std::string_view s, std::size_t& offset) {
  if (offset >= s.size()) {
    return 0;
  }
  const auto* data = reinterpret_cast<const unsigned char*>(s.data());
  const uint8_t b0 = data[offset];
  if (b0 < 0x80) {
    offset += 1;
    return static_cast<Rune>(b0);
  }

  constexpr uint8_t b2_mask = 0x1F;
  constexpr uint8_t b3_mask = 0x0F;
  constexpr uint8_t b4_mask = 0x07;
  constexpr uint8_t mb_mask = 0x3F;

  if (b0 < 0xE0) {
    if (offset + 1 >= s.size()) {
      offset = s.size();
      return 0xFFFD;
    }
    const auto c = static_cast<Rune>(b0 & b2_mask) << 6 |
                   static_cast<Rune>(data[offset + 1] & mb_mask);
    offset += 2;
    return c;
  }
  if (b0 < 0xF0) {
    if (offset + 2 >= s.size()) {
      offset = s.size();
      return 0xFFFD;
    }
    const auto c = static_cast<Rune>(b0 & b3_mask) << 12 |
                   static_cast<Rune>(data[offset + 1] & mb_mask) << 6 |
                   static_cast<Rune>(data[offset + 2] & mb_mask);
    offset += 3;
    return c;
  }
  if (offset + 3 >= s.size()) {
    offset = s.size();
    return 0xFFFD;
  }
  const auto c = static_cast<Rune>(b0 & b4_mask) << 18 |
                 static_cast<Rune>(data[offset + 1] & mb_mask) << 12 |
                 static_cast<Rune>(data[offset + 2] & mb_mask) << 6 |
                 static_cast<Rune>(data[offset + 3] & mb_mask);
  offset += 4;
  return c;
}

inline uint16_t GeneralCiWeight(Rune c) {
  if (c > 0xFFFF) {
    return 0xFFFD;
  }
  return tidb::GeneralCI::weight_lut[static_cast<std::size_t>(c) & 0xFFFF];
}

inline int CompareGeneralCi(std::string_view lhs, std::string_view rhs) {
  lhs = RightTrimAsciiSpace(lhs);
  rhs = RightTrimAsciiSpace(rhs);

  std::size_t lhs_offset = 0;
  std::size_t rhs_offset = 0;
  while (lhs_offset < lhs.size() && rhs_offset < rhs.size()) {
    const auto c1 = DecodeUtf8Char(lhs, lhs_offset);
    const auto c2 = DecodeUtf8Char(rhs, rhs_offset);
    const int diff = static_cast<int>(GeneralCiWeight(c1)) - static_cast<int>(GeneralCiWeight(c2));
    if (diff != 0) {
      return Signum(diff);
    }
  }
  return (lhs_offset < lhs.size()) - (rhs_offset < rhs.size());
}

namespace tidb::UnicodeCI {

struct LongWeight {
  uint64_t first;
  uint64_t second;
};

inline constexpr uint64_t kLongWeightRune = 0xFFFD;

inline constexpr std::array<LongWeight, 23> kWeightLutLong0400 = {
    LongWeight{0x1D6E1DC61D6D0288, 0x000002891E031DC2},
    LongWeight{0x1D741DC61D6D0288, 0x0000000002891DCB},
    LongWeight{0x1D621E0F1DBE1D70, 0x0000000000001DC6},
    LongWeight{0x0E0B1E591E5E1E55, 0x0000000000001E65},
    LongWeight{0x1E781E591E7C1E58, 0x0000000000001E72},
    LongWeight{0x0E0B1E731E7C1E58, 0x000000001E7A1E65},
    LongWeight{0x1E631E7D1E7C1E58, 0x0000000000001E65},
    LongWeight{0x1E651E721E781E59, 0x0000000000001E81},
    LongWeight{0x1E531E5F1E7A1E59, 0x0000000000001E7C},
    LongWeight{0x0E0B1E621E811E5C, 0x0000000000001E72},
    LongWeight{0x1E811E5F0E0B1E6B, 0x0000000000001E65},
    LongWeight{0x1E651E5E1E521E6C, 0x0000000000001E7A},
    LongWeight{0x1E631E781E521E6D, 0x0000000000001E65},
    LongWeight{0x1E551E5D1E631E6D, 0x0000000000001E7A},
    LongWeight{0x0E0B1E611E591E6E, 0x0000000000001E7A},
    LongWeight{0x1E771E5D1E811E70, 0x0000000000001E81},
    LongWeight{0x0E0B1E6B1E791E71, 0x0000000000001E7A},
    LongWeight{0x1E5A1E651E811E7B, 0x0000000000001E81},
    LongWeight{0xDF0FFB40E82AFB40, 0xF93EFB40CF1AFB40},
    LongWeight{0x04370E6D0E330FC0, 0x0000000000000FEA},
    LongWeight{0x04370E6D0E330FC0, 0x000000000E2B0FEA},
    LongWeight{0x135E020913AB135E, 0x13B713AB135013AB},
    LongWeight{0x0, 0x0},
};

inline constexpr std::array<LongWeight, 28> kWeightLutLong0900 = {
    LongWeight{0x3C013C7B3C000317, 0x000003183CD43C77},
    LongWeight{0x3C073C7B3C000317, 0x0000000003183C80},
    LongWeight{0x3BF53CE03C733C03, 0x0000000000003C7B},
    LongWeight{0x1C0E3D623D673D5E, 0x0000000000003D6E},
    LongWeight{0x3D823D623D863D61, 0x0000000000003D7B},
    LongWeight{0x1C0E3D7C3D863D61, 0x000000003D843D6E},
    LongWeight{0x3D6C3D873D863D61, 0x0000000000003D6E},
    LongWeight{0x3D6E3D7B3D823D62, 0x0000000000003D8B},
    LongWeight{0x3D5B3D683D843D62, 0x0000000000003D86},
    LongWeight{0x1C0E3D6B3D8B3D65, 0x0000000000003D7B},
    LongWeight{0x3D8B3D681C0E3D74, 0x0000000000003D6E},
    LongWeight{0x3D6E3D673D5A3D75, 0x0000000000003D84},
    LongWeight{0x3D6C3D823D5A3D76, 0x0000000000003D6E},
    LongWeight{0x3D5E3D663D6C3D76, 0x0000000000003D84},
    LongWeight{0x1C0E3D6A3D623D77, 0x0000000000003D84},
    LongWeight{0x3D813D663D8B3D79, 0x0000000000003D8B},
    LongWeight{0x1C0E3D743D833D7A, 0x0000000000003D84},
    LongWeight{0x3D633D6E3D8B3D85, 0x0000000000003D8B},
    LongWeight{0xDF0FFB40E82AFB40, 0xF93EFB40CF1AFB40},
    LongWeight{0x06251C8F1C471E33, 0x0000000000001E71},
    LongWeight{0x06251C8F1C471E33, 0x000000001C3F1E71},
    LongWeight{0x020923C5239C2364, 0x23B1239C239C230B},
    LongWeight{0x23250209239C2325, 0x23B1239C230B239C},
    LongWeight{0x000000000000FFFD, 0x0000000000000000},
    LongWeight{0x02091C8F1DB91C3F, 0x00001E331C7A1E71},
    LongWeight{0x1E3302091D321D18, 0x000000001E711CAA},
    LongWeight{0x1E711E711DDD1D77, 0x1E711E711CAA1D77},
    LongWeight{0x0, 0x0},
};

inline const LongWeight& WeightLutLongMap0400(Rune r) {
  switch (r) {
    case 0x321D:
      return kWeightLutLong0400[0];
    case 0x321E:
      return kWeightLutLong0400[1];
    case 0x327C:
      return kWeightLutLong0400[2];
    case 0x3307:
      return kWeightLutLong0400[3];
    case 0x3315:
      return kWeightLutLong0400[4];
    case 0x3316:
      return kWeightLutLong0400[5];
    case 0x3317:
      return kWeightLutLong0400[6];
    case 0x3319:
      return kWeightLutLong0400[7];
    case 0x331A:
      return kWeightLutLong0400[8];
    case 0x3320:
      return kWeightLutLong0400[9];
    case 0x332B:
      return kWeightLutLong0400[10];
    case 0x332E:
      return kWeightLutLong0400[11];
    case 0x3332:
      return kWeightLutLong0400[12];
    case 0x3334:
      return kWeightLutLong0400[13];
    case 0x3336:
      return kWeightLutLong0400[14];
    case 0x3347:
      return kWeightLutLong0400[15];
    case 0x334A:
      return kWeightLutLong0400[16];
    case 0x3356:
      return kWeightLutLong0400[17];
    case 0x337F:
      return kWeightLutLong0400[18];
    case 0x33AE:
      return kWeightLutLong0400[19];
    case 0x33AF:
      return kWeightLutLong0400[20];
    case 0xFDFB:
      return kWeightLutLong0400[21];
    default:
      return kWeightLutLong0400[22];
  }
}

inline const LongWeight& WeightLutLongMap0900(Rune r) {
  switch (r) {
    case 0x321D:
      return kWeightLutLong0900[0];
    case 0x321E:
      return kWeightLutLong0900[1];
    case 0x327C:
      return kWeightLutLong0900[2];
    case 0x3307:
      return kWeightLutLong0900[3];
    case 0x3315:
      return kWeightLutLong0900[4];
    case 0x3316:
      return kWeightLutLong0900[5];
    case 0x3317:
      return kWeightLutLong0900[6];
    case 0x3319:
      return kWeightLutLong0900[7];
    case 0x331A:
      return kWeightLutLong0900[8];
    case 0x3320:
      return kWeightLutLong0900[9];
    case 0x332B:
      return kWeightLutLong0900[10];
    case 0x332E:
      return kWeightLutLong0900[11];
    case 0x3332:
      return kWeightLutLong0900[12];
    case 0x3334:
      return kWeightLutLong0900[13];
    case 0x3336:
      return kWeightLutLong0900[14];
    case 0x3347:
      return kWeightLutLong0900[15];
    case 0x334A:
      return kWeightLutLong0900[16];
    case 0x3356:
      return kWeightLutLong0900[17];
    case 0x337F:
      return kWeightLutLong0900[18];
    case 0x33AE:
      return kWeightLutLong0900[19];
    case 0x33AF:
      return kWeightLutLong0900[20];
    case 0xFDFA:
      return kWeightLutLong0900[21];
    case 0xFDFB:
      return kWeightLutLong0900[22];
    case 0xFFFD:
      return kWeightLutLong0900[23];
    case 0x1F19C:
      return kWeightLutLong0900[24];
    case 0x1F1A8:
      return kWeightLutLong0900[25];
    case 0x1F1A9:
      return kWeightLutLong0900[26];
    default:
      return kWeightLutLong0900[27];
  }
}

inline bool Weight0400(uint64_t& first, uint64_t& second, Rune r) {
  if (r > 0xFFFF) {
    first = 0xFFFD;
    return true;
  }
  const uint64_t w = tidb::UnicodeCI::weight_lut_0400[static_cast<std::size_t>(r)];
  if (w == 0) {
    return false;
  }
  if (w == kLongWeightRune) {
    const auto& lw = WeightLutLongMap0400(r);
    first = lw.first;
    second = lw.second;
  } else {
    first = w;
  }
  return true;
}

inline bool Weight0900(uint64_t& first, uint64_t& second, Rune r) {
  if (r > 0x2CEA1) {
    first = (static_cast<uint64_t>(r) >> 15) + 0xFBC0 +
            (((static_cast<uint64_t>(r) & 0x7FFF) | 0x8000) << 16);
    return true;
  }

  const uint64_t w = tidb::UnicodeCI::weight_lut_0900[static_cast<std::size_t>(r)];
  if (w == 0) {
    return false;
  }
  if (w == kLongWeightRune) {
    const auto& lw = WeightLutLongMap0900(r);
    first = lw.first;
    second = lw.second;
  } else {
    first = w;
  }
  return true;
}

template <bool unicode_0900, bool padding>
inline int CompareUnicodeCiImpl(std::string_view lhs, std::string_view rhs) {
  if constexpr (padding) {
    lhs = RightTrimAsciiSpace(lhs);
    rhs = RightTrimAsciiSpace(rhs);
  }

  std::size_t lhs_offset = 0;
  std::size_t rhs_offset = 0;
  const std::size_t lhs_len = lhs.size();
  const std::size_t rhs_len = rhs.size();

  uint64_t lhs_first = 0;
  uint64_t lhs_second = 0;
  uint64_t rhs_first = 0;
  uint64_t rhs_second = 0;

  const auto weight_one = [&](uint64_t& first, uint64_t& second, std::size_t& offset,
                              std::size_t len, std::string_view data) {
    if (first != 0) {
      return;
    }
    if (second != 0) {
      first = second;
      second = 0;
      return;
    }
    while (offset < len) {
      const auto r = DecodeUtf8Char(data, offset);
      const bool ok =
          unicode_0900 ? Weight0900(first, second, r) : Weight0400(first, second, r);
      if (ok) {
        break;
      }
    }
  };

  while (true) {
    weight_one(lhs_first, lhs_second, lhs_offset, lhs_len, lhs);
    weight_one(rhs_first, rhs_second, rhs_offset, rhs_len, rhs);

    if (lhs_first == 0 || rhs_first == 0) {
      if (lhs_first < rhs_first) {
        return -1;
      }
      if (lhs_first > rhs_first) {
        return 1;
      }
      return 0;
    }

    if (lhs_first == rhs_first) {
      lhs_first = 0;
      rhs_first = 0;
      continue;
    }

    while (lhs_first != 0 && rhs_first != 0) {
      if (((lhs_first ^ rhs_first) & 0xFFFF) == 0) {
        lhs_first >>= 16;
        rhs_first >>= 16;
      } else {
        return Signum(static_cast<int>(lhs_first & 0xFFFF) - static_cast<int>(rhs_first & 0xFFFF));
      }
    }
  }
}

}  // namespace tidb::UnicodeCI

template <CollationKind kind>
inline int CompareString(std::string_view lhs, std::string_view rhs) {
  if constexpr (kind == CollationKind::kBinary) {
    return CompareBinary(lhs, rhs);
  } else if constexpr (kind == CollationKind::kPaddingBinary) {
    lhs = RightTrimAsciiSpace(lhs);
    rhs = RightTrimAsciiSpace(rhs);
    return CompareBinary(lhs, rhs);
  } else if constexpr (kind == CollationKind::kGeneralCi) {
    return CompareGeneralCi(lhs, rhs);
  } else if constexpr (kind == CollationKind::kUnicodeCi0400) {
    return tidb::UnicodeCI::CompareUnicodeCiImpl</*unicode_0900=*/false, /*padding=*/true>(lhs, rhs);
  } else if constexpr (kind == CollationKind::kUnicodeCi0900) {
    return tidb::UnicodeCI::CompareUnicodeCiImpl</*unicode_0900=*/true, /*padding=*/false>(lhs, rhs);
  } else {
    return CompareBinary(lhs, rhs);
  }
}

inline int CompareString(CollationKind kind, std::string_view lhs, std::string_view rhs) {
  switch (kind) {
    case CollationKind::kBinary:
      return CompareString<CollationKind::kBinary>(lhs, rhs);
    case CollationKind::kPaddingBinary:
      return CompareString<CollationKind::kPaddingBinary>(lhs, rhs);
    case CollationKind::kGeneralCi:
      return CompareString<CollationKind::kGeneralCi>(lhs, rhs);
    case CollationKind::kUnicodeCi0400:
      return CompareString<CollationKind::kUnicodeCi0400>(lhs, rhs);
    case CollationKind::kUnicodeCi0900:
      return CompareString<CollationKind::kUnicodeCi0900>(lhs, rhs);
    case CollationKind::kUnsupported:
      break;
  }
  return CompareString<CollationKind::kBinary>(lhs, rhs);
}

// Returns:
// - negative if lhs < rhs
// - zero if lhs == rhs
// - positive if lhs > rhs
inline int CompareString(Collation collation, std::string_view lhs, std::string_view rhs) {
  return CompareString(collation.kind, lhs, rhs);
}

}  // namespace tiforth
