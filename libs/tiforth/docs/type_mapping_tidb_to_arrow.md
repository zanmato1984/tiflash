# TiDB/MySQL -> Arrow type mapping (TiForth contract)

TiForth executes on Arrow `RecordBatch`, but TiDB/MySQL types often have **extra semantics** that Arrow physical
types do not represent (collations, decimal parameters, packed temporal formats, …).

TiForth’s contract is:

1. Choose an Arrow **physical type** for each column.
2. For “tricky” columns, attach **Arrow field metadata** under the reserved `tiforth.*` namespace.
3. TiForth operators and expression compilation consult that metadata to preserve TiDB/MySQL behavior where
   implemented.

This document specifies the mapping rules and metadata in detail.

## 1) Metadata keys (reserved namespace)

Defined in `libs/tiforth/include/tiforth/type_metadata.h`:

- `tiforth.logical_type`
- `tiforth.decimal.precision`
- `tiforth.decimal.scale`
- `tiforth.datetime.fsp`
- `tiforth.string.collation_id`

### `tiforth.logical_type`

String value identifying the logical type carried by the field. Current values:

- `decimal`
- `mydate`
- `mydatetime`
- `string`

If absent, TiForth treats the field as “no special logical type” (except for Arrow decimal physical types, which
are recognized as decimal logical types automatically during expression compilation).

### Integer metadata values

For `precision`, `scale`, `fsp`, and `collation_id`, TiForth stores values as **base-10 strings** and parses them
as `int32`.

Parsing is strict:

- invalid values (non-integers / overflow / trailing bytes) are errors

## 2) Mapping overview

### What is “supported”?

This doc distinguishes:

- **Contract**: a recommended Arrow representation for a TiDB/MySQL type.
- **Implemented behavior**: what current TiForth operators/functions actually honor.

If you follow the contract for an unimplemented type, TiForth will generally treat it as a plain Arrow physical
type (or reject it in operators that only support a subset of key types).

## 3) Numeric types

### Signed/unsigned integers

TiDB/MySQL integer families map directly to Arrow integer physical types:

- `TINYINT` -> `arrow::int8()`, `TINYINT UNSIGNED` -> `arrow::uint8()`
- `SMALLINT` -> `arrow::int16()`, `SMALLINT UNSIGNED` -> `arrow::uint16()`
- `MEDIUMINT` -> `arrow::int32()`, `MEDIUMINT UNSIGNED` -> `arrow::uint32()`
- `INT` -> `arrow::int32()`, `INT UNSIGNED` -> `arrow::uint32()`
- `BIGINT` -> `arrow::int64()`, `BIGINT UNSIGNED` -> `arrow::uint64()`

No TiForth metadata is required for these types.

Notes:

- TiForth key-based operators currently accept only a limited key set; see `operators_and_functions.md`.
- `NULL` is represented by Arrow’s validity bitmap (field `nullable=true`).

### Floating point

- `FLOAT` -> `arrow::float32()`
- `DOUBLE` -> `arrow::float64()`

No TiForth metadata is required.

## 4) Decimal

### Physical type

Use Arrow decimal physical types:

- `DECIMAL(P,S)`:
  - if `P <= 38`: `arrow::decimal128(P, S)`
  - else: `arrow::decimal256(P, S)` (TiForth’s common-path semantics cap precision at 65)

### Metadata

Attach:

- `tiforth.logical_type=decimal`
- optionally (recommended when the producer wants redundancy / cross-checking):
  - `tiforth.decimal.precision=<P>`
  - `tiforth.decimal.scale=<S>`

How TiForth interprets this:

- `GetLogicalType(field)` treats Arrow `decimal128/256(P,S)` as authoritative and will also read metadata if
  present (metadata can override if it is set).
- Expression compilation treats Arrow decimal physical types as “decimal logical” even if metadata is missing.

### Decimal behavior hooks

TiForth registers a decimal-add kernel (`tiforth.decimal_add`) and expression compilation rewrites
`add(x,y)` into `tiforth.decimal_add(x,y)` when at least one argument is a decimal logical type.

This is how TiForth preserves TiDB/MySQL decimal inference behavior where implemented.

## 5) Packed date/time (MyDate/MyDateTime)

TiForth’s contract for TiDB/MySQL `DATE` / `DATETIME` (and commonly `TIMESTAMP` when represented in the same
packed form) is to preserve the host’s **packed** representation rather than converting to Arrow’s canonical
`date32` / `timestamp`.

### Physical type

- `arrow::uint64()`

### Metadata

- For `DATE`-like values:
  - `tiforth.logical_type=mydate`
- For `DATETIME`-like values:
  - `tiforth.logical_type=mydatetime`
  - `tiforth.datetime.fsp=<0..6>` (fractional seconds precision; optional but recommended)

### Packed layout (as consumed by TiForth functions)

TiForth’s temporal extraction kernels interpret the `uint64` value as:

- `ym = packed >> 46` (year-month combined)
  - `year  = ym / 13`
  - `month = ym % 13`
- `day    = (packed >> 41) & 0x1f`
- `hour   = (packed >> 36) & 0x1f`
- `minute = (packed >> 30) & 0x3f`
- `second = (packed >> 24) & 0x3f`
- `micro  = packed & ((1 << 24) - 1)`

`toMyDate(x)` masks away the time portion:

- `packed & ~((1ULL << 41) - 1)`

Notes:

- Some kernels treat “zero” dates (month/day == 0) as NULL results for TiDB-compatible behavior (e.g.
  `tidbDayOfWeek`, `tidbWeekOfYear`, `yearWeek`). Others (`toYear`, `toMonth`, `toDayOfMonth`) simply extract
  fields without validation.
- `tiforth.datetime.fsp` is carried through compilation into function options, even if some kernels do not
  currently use it (reserved for future rounding/cast behavior).

## 6) Strings, bytes, and collations

### Why `binary` and not `utf8`?

TiDB string columns may contain arbitrary bytes (depending on charset/collation and data origin). TiForth’s
common path treats values as **byte strings** and relies on collation metadata to apply comparison and
normalization rules.

Contract:

- Use `arrow::binary()` (32-bit offsets) for TiDB/MySQL string-like columns that participate in collation-aware
  comparisons or as join/agg/sort keys.

### Physical type

- `CHAR/VARCHAR/TEXT/...` -> `arrow::binary()`
- `BINARY/VARBINARY/BLOB/...` -> `arrow::binary()`

### Metadata

Attach:

- `tiforth.logical_type=string`
- `tiforth.string.collation_id=<tidb collation id>`

If `tiforth.string.collation_id` is missing, TiForth treats the column as collation id `63` (BINARY).

### Supported collation ids (common path)

`libs/tiforth/src/tiforth/collation.cc` maps a TiDB collation id to a TiForth `CollationKind`:

| Collation id | Example TiDB name (informal) | Kind | PAD SPACE | Case-insensitive |
| --- | --- | --- | --- | --- |
| `63` | `BINARY` | `kBinary` | no | no |
| `309` | `UTF8MB4_0900_BIN` | `kBinary` | no | no |
| `46` | `UTF8MB4_BIN` | `kPaddingBinary` | yes (right-trim ASCII space) | no |
| `83` | `UTF8_BIN` | `kPaddingBinary` | yes | no |
| `47` | `LATIN1_BIN` | `kPaddingBinary` | yes | no |
| `65` | `ASCII_BIN` | `kPaddingBinary` | yes | no |
| `33` | `UTF8_GENERAL_CI` | `kGeneralCi` | yes | yes |
| `45` | `UTF8MB4_GENERAL_CI` | `kGeneralCi` | yes | yes |
| `192` | `UTF8_UNICODE_CI` | `kUnicodeCi0400` | yes | yes |
| `224` | `UTF8MB4_UNICODE_CI` | `kUnicodeCi0400` | yes | yes |
| `255` | `UTF8MB4_0900_AI_CI` | `kUnicodeCi0900` | no | yes |

If an id is not in this table, TiForth currently treats it as unsupported.

### Normalization and sort keys (hashing/grouping/join/sort)

For key-based operators, TiForth normalizes collated strings into **sort-key bytes** to make hashing and
equality consistent with TiDB collation semantics.

Defined in `libs/tiforth/include/tiforth/collation.h`:

- `SortKeyStringTo(CollationKind, std::string_view, out)`

Rules (common path):

- `kBinary`: sort key is the raw bytes.
- `kPaddingBinary`: sort key is raw bytes after right-trimming ASCII space (`0x20`).
- `kGeneralCi`: right-trim ASCII space, then map UTF-8 runes to 16-bit weights (`GeneralCI::weight_lut`),
  encoded big-endian (two bytes per rune weight).
- `kUnicodeCi0400` / `kUnicodeCi0900`: map UTF-8 runes to (possibly multi-part) packed weights and encode them
  as a big-endian sequence of 16-bit weights.

This approach avoids calling a collation comparator for every hash/equality check in inner loops.

## 7) Unsupported / future types (contract notes)

The following TiDB/MySQL types are **not** fully implemented in TiForth’s current common path, but are listed
here to make future extensions explicit:

- `JSON`, `GEOMETRY`: likely `arrow::binary()` with logical metadata for semantics-sensitive functions.
- `ENUM`, `SET`: could map to `arrow::int32()` or `arrow::dictionary(int32, binary)` depending on host needs.
- `BIT`: typically `arrow::binary()` or `arrow::fixed_size_binary(N)` depending on width.
- `TIME` / `DURATION`: could map to `arrow::int64()` (microseconds) or keep a packed representation with a new
  `tiforth.logical_type` value.

Until implemented, operators that depend on key semantics may reject these as unsupported key types.

