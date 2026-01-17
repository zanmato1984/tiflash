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
  // - 33: UTF8_GENERAL_CI (PAD SPACE)
  // - 45: UTF8MB4_GENERAL_CI (PAD SPACE)
  // - 192: UTF8_UNICODE_CI (PAD SPACE)
  // - 224: UTF8MB4_UNICODE_CI (PAD SPACE)
  // - 255: UTF8MB4_0900_AI_CI (NO PAD)
  // - 309: UTF8MB4_0900_BIN (NO PAD)
  switch (id) {
    case 63:
    case 309:
      return Collation{.id = id, .kind = CollationKind::kBinary};
    case 46:
    case 83:
    case 47:
    case 65:
      return Collation{.id = id, .kind = CollationKind::kPaddingBinary};
    case 33:
    case 45:
      return Collation{.id = id, .kind = CollationKind::kGeneralCi};
    case 192:
    case 224:
      return Collation{.id = id, .kind = CollationKind::kUnicodeCi0400};
    case 255:
      return Collation{.id = id, .kind = CollationKind::kUnicodeCi0900};
    default:
      return Collation{.id = id, .kind = CollationKind::kUnsupported};
  }
}

}  // namespace tiforth
