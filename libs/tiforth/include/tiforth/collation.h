#pragma once

#include <cstdint>
#include <string_view>

#include <arrow/result.h>

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

// Returns:
// - negative if lhs < rhs
// - zero if lhs == rhs
// - positive if lhs > rhs
int CompareString(Collation collation, std::string_view lhs, std::string_view rhs);

std::string_view RightTrimAsciiSpace(std::string_view value);

}  // namespace tiforth

