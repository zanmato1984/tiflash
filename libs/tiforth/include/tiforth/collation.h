#pragma once

#include <cstdint>
#include <cstring>
#include <string_view>

namespace tiforth {

enum class CollationKind {
  kBinary,
  kPaddingBinary,
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

template <CollationKind kind>
inline int CompareString(std::string_view lhs, std::string_view rhs) {
  if constexpr (kind == CollationKind::kPaddingBinary) {
    lhs = RightTrimAsciiSpace(lhs);
    rhs = RightTrimAsciiSpace(rhs);
  }
  return CompareBinary(lhs, rhs);
}

inline int CompareString(CollationKind kind, std::string_view lhs, std::string_view rhs) {
  switch (kind) {
    case CollationKind::kBinary:
      return CompareString<CollationKind::kBinary>(lhs, rhs);
    case CollationKind::kPaddingBinary:
      return CompareString<CollationKind::kPaddingBinary>(lhs, rhs);
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
