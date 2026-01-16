#include "tiforth/collation.h"

#include <cstring>

namespace tiforth {

Collation CollationFromId(int32_t id) {
  // Keep the id list local to TiForth (no dependency on TiFlash headers).
  //
  // TiDB collation ids (commonly used):
  // - 63: BINARY
  // - 46: UTF8MB4_BIN (PAD SPACE)
  // - 83: UTF8_BIN (PAD SPACE)
  // - 47: LATIN1_BIN (PAD SPACE)
  // - 65: ASCII_BIN (PAD SPACE)
  switch (id) {
    case 63:
      return Collation{.id = id, .kind = CollationKind::kBinary};
    case 46:
    case 83:
    case 47:
    case 65:
      return Collation{.id = id, .kind = CollationKind::kPaddingBinary};
    default:
      return Collation{.id = id, .kind = CollationKind::kUnsupported};
  }
}

std::string_view RightTrimAsciiSpace(std::string_view value) {
  if (value.empty() || value.back() != ' ') {
    return value;
  }
  std::size_t end = value.find_last_not_of(' ');
  if (end == std::string_view::npos) {
    return std::string_view{};
  }
  return value.substr(0, end + 1);
}

int CompareString(Collation collation, std::string_view lhs, std::string_view rhs) {
  if (collation.kind == CollationKind::kPaddingBinary) {
    lhs = RightTrimAsciiSpace(lhs);
    rhs = RightTrimAsciiSpace(rhs);
  } else if (collation.kind != CollationKind::kBinary) {
    // Caller should gate unsupported collations; keep a deterministic fallback.
    collation = Collation{.id = collation.id, .kind = CollationKind::kBinary};
  }

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

}  // namespace tiforth

