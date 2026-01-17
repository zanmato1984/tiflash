#include "tiforth/collation.h"

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

}  // namespace tiforth
