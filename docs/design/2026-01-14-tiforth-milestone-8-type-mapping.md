# TiForth Milestone 8: TiFlash Type -> Arrow Mapping + Semantics Hooks

- Author(s): TBD
- Last Updated: 2026-01-16
- Status: Draft
- Related design: `docs/design/2026-01-14-tiforth.md`
- Depends on: MS1-7 (pipeline/task framework + basic operators)

## Problem

Current TiForth operators/tests treat Arrow *physical* types as the full contract. This is insufficient for TiDB/TiFlash
types where:

- the in-memory representation is not the same as Arrow canonical logical types (MyDate/MyDateTime packed UInt64)
- semantics require extra parameters (Decimal precision/scale; MyDateTime fsp; string collation)
- Arrow compute kernels do not match TiFlash semantics (collation-aware string compare/sort/group, MySQL zero-date rules, etc)

## Goals

- Define a stable mapping contract from **TiFlash logical types** to **Arrow physical types + field metadata**.
- Preserve TiFlash semantics for tricky types:
  - Decimal: precision/scale, overflow behavior remains TiFlash-defined
  - MyDate/MyDateTime: keep packed representation (no lossy conversion)
  - String: preserve collation id; provide a hook for collation-aware compare/hash/sort
- Keep TiForth independent: no `dbms/` headers/types in `libs/tiforth/`.
- Implement only the **common path** initially:
  - mapping + decode of metadata
  - collation-aware predicate comparisons for BINARY + padding-BIN collations
  - key handling for join/agg/sort extended beyond int32 in follow-ups

## Non-goals (MS8)

- Full TiDB type system (enums/sets/json/bit/geometry, etc).
- Full collation coverage (all ICU/Unicode collations).
- Full MySQL datetime arithmetic/casts (DATE_ADD, timezone conversion, etc).

## Arrow Mapping Contract

### Metadata keys (reserved namespace)

Use Arrow `Field::metadata()` and reserve a TiForth prefix:

- `tiforth.logical_type`: string identifier (`"decimal"`, `"mydate"`, `"mydatetime"`, `"string"`, ...)
- `tiforth.decimal.precision`: decimal precision (base-10 digits)
- `tiforth.decimal.scale`: decimal scale
- `tiforth.datetime.fsp`: fractional seconds precision for MyDateTime
- `tiforth.string.collation_id`: TiDB collation id (int32 as string)

Notes:

- Only non-trivial types require metadata; plain numeric types can rely on Arrow type alone.
- Metadata is additive; TiForth must remain robust if some optional keys are missing (use safe defaults or reject).

### Type mapping table (initial)

- `Int*/UInt*`, `Float*`: map to matching Arrow primitive types, no extra metadata.
- `Decimal(P,S)`:
  - Arrow type: `decimal128(P,S)` if `P <= 38`, else `decimal256(P,S)` (TiFlash supports up to 65).
  - Metadata: `tiforth.logical_type=decimal`, and optionally `tiforth.decimal.{precision,scale}` for cross-checking.
- `MyDate`:
  - Arrow type: `uint64` (TiFlash packed format per `DB::MyTimeBase`).
  - Metadata: `tiforth.logical_type=mydate`.
- `MyDateTime(fsp)`:
  - Arrow type: `uint64` (TiFlash packed format per `DB::MyTimeBase`).
  - Metadata: `tiforth.logical_type=mydatetime`, `tiforth.datetime.fsp=<0..6>`.
- `String` / `FixedString`:
  - Arrow type: `binary` (treat as raw bytes; avoids assuming valid UTF-8).
  - Metadata: `tiforth.logical_type=string`, `tiforth.string.collation_id=<tidb id>`.

Rationale:

- For MyDate/MyDateTime: preserve packed value so TiFlash edge cases (zero date, fsp/type bits) round-trip.
- For strings: treat as bytes; collation is handled by TiForth logic (not Arrow kernels) when required.

## Semantics Hooks

### Collation-aware string compare (MS8C)

Arrow compute `equal/less/greater` on `binary` is bytewise and ignores TiDB collation rules.

MS8 introduces a TiForth compare hook for predicates:

- functions: `=,!=,<,<=,>,>=` (implemented as internal handling of Arrow compute call names)
- supported collations (initial):
  - `BINARY` (id 63): raw byte compare (no trimming)
  - padding BIN collations (ids 46/83/47/65): right-trim ASCII space before compare

Future: add CI/unicode collations by porting TiFlash LUT-based collators into TiForth.

### Date/time and decimal functions

MS8 does not fully port MySQL datetime/decimal functions. It only ensures:

- packed MyDate/MyDateTime values are preserved across the boundary
- comparisons on packed representations are well-defined within a fixed type/fsp column

Future: add explicit TiForth kernels for casts/arithmetic per TiFlash behavior.

## TiFlash Integration (MS8B)

Under `TIFLASH_ENABLE_TIFORTH`, provide:

- `IDataType` -> Arrow `Field` mapping using the contract above
- basic column-to-Arrow conversion for:
  - Decimal columns to Arrow decimal arrays
  - MyDate/MyDateTime columns (UInt64) to Arrow uint64 arrays
  - String columns to Arrow binary arrays

Notes (implemented):

- Numeric primitives (`Int*/UInt*`, `Float*`) also map to Arrow primitives on the common path, since they are
  representation-compatible and unblock mixed-schema batches.

## Definition of Done (MS8 series)

- TiForth: unit tests for metadata parsing + collation compare.
- TiFlash: one guarded gtest that builds a Block with decimal/date-time/collated strings, converts to Arrow using the mapping, runs a TiForth pipeline, and validates output.
- `ninja -C libs/tiforth/build-debug && ctest --test-dir libs/tiforth/build-debug`
- `ninja -C cmake-build-tiflash-tiforth-debug gtests_dbms && gtests_dbms --gtest_filter=TiForth*`

## Follow-up Completed In This Iteration (MS8D/MS8E)

- TiForth operators: hash join / hash agg / sort keys extended to handle `uint64`, `decimal128/256`, and `binary`
  strings with collation metadata (BINARY + PAD SPACE BIN). Implementation notes:
  - join builds a key->row-index map on a concatenated build-side batch and uses Arrow `Take` to materialize output,
    so it naturally supports mixed column types while keeping a deterministic row order.
  - hash agg keeps an output key value (first seen) while using a normalized key for hashing/equality under PAD SPACE.
- TiFlash (guarded) gtest: builds a `DB::Block` containing `Decimal256`, `MyDateTime(6)`, and collated strings,
  converts to Arrow via MS8B, then validates TiForth `HashAgg` and `HashJoin` behavior.
