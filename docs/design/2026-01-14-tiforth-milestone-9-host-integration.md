# TiForth Milestone 9: Host Integration Beyond GTests (TiFlash First)

- Author(s): TBD
- Last Updated: 2026-01-17
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Depends on: MS1-8 (TiForth pipeline/operators + Arrow type mapping contract + TiFlash Block->Arrow)

## Problem

Current TiForth integration is validated mostly by:

- TiForth unit tests operating on Arrow `RecordBatch`
- guarded TiFlash gtests that construct Arrow batches (or convert a single `DB::Block` to Arrow) and run TiForth ops

This does not yet provide a practical path to run TiForth inside the real TiFlash execution flow, because TiFlash is
Block-based (`DB::Block`, `DB::IColumn`) while TiForth is Arrow-based.

The missing integration pieces are:

- Arrow `RecordBatch` -> TiFlash `DB::Block` conversion for a common subset of types (the reverse direction of MS8B)
- a small “runner” adapter that can execute a TiForth `Task` while consuming/producing `DB::Block` (tests first)
- (later) translation from a subset of TiFlash pipeline DAG nodes to TiForth operators (beyond dummy gtests)

## Goals

- Provide a **minimal Block-in / Block-out bridge** for the MS8 type mapping set:
  - numeric primitives (Int*/UInt*, Float*)
  - Decimal (128/256)
  - MyDate/MyDateTime packed UInt64
  - String/FixedString as Arrow `binary` with collation metadata
- Keep everything **guarded by** `TIFLASH_ENABLE_TIFORTH` (build option `ENABLE_TIFORTH`).
- Keep TiForth independent: no `dbms/` types or headers added to `libs/tiforth/`.
- Enable rapid validation by adding at least one gtest that runs TiForth end-to-end using **only Block I/O**.

## Non-goals (MS9 initial)

- Full TiFlash executor translation coverage.
- Full type coverage (arrays/tuples/json/enums/low-cardinality, etc).
- Full collation coverage (ICU/Unicode, CI collations).
- Full memory tracker/spill wiring end-to-end (keep existing MS7 pool plumbing; spill stays stubbed).

## Proposed Architecture (TiFlash Side)

Under `dbms/src/Flash/TiForth/` (guarded):

1. **Type mapping (done in MS8B)**:
   - `IDataType` -> Arrow `Field` + metadata
   - `DB::Block` -> `arrow::RecordBatch`

2. **Reverse mapping (MS9B)**:
   - Arrow `Schema`/`Field` (+ metadata) -> `DB::DataTypePtr`
   - Arrow `Array` -> `DB::IColumn` (nullable aware)
   - `arrow::RecordBatch` -> `DB::Block`

3. **Runner adapter (MS9C, tests first)**:
   - Accept: `std::vector<DB::Block>` (or a Block stream)
   - Convert each input block to `RecordBatch`, push into a TiForth `Task`
   - Pull output `RecordBatch`es, convert back to `DB::Block`
   - Return: `std::vector<DB::TiForth::BlockConversionResult>` (`DB::Block` + `options_by_name` side-channel)

This “runner” is not the final production integration, but it validates correctness of:

- type mapping roundtrip
- collation/date/decimal behavior through real operators
- error propagation across the boundary

## Type Roundtrip Rules (Initial)

Use Arrow physical type + metadata as the contract (MS8):

- Decimal: Arrow `decimal128/decimal256` + `tiforth.logical_type=decimal` (+ precision/scale)
- MyDate/MyDateTime: Arrow `uint64` + `tiforth.logical_type=mydate|mydatetime` (+ fsp for mydatetime)
- String: Arrow `binary` + `tiforth.logical_type=string` (+ `tiforth.string.collation_id`)

Roundtrip decision:

- `binary` without `tiforth.logical_type=string` is treated as raw bytes and maps back to `DataTypeString`
  (collation metadata is optional; for MS9 we keep a “collation id by column name” side-channel for callers that need it).
  Note: this side-channel is currently keyed by Arrow field name, so duplicate field names (e.g. join outputs) can overwrite.

## Translation / Execution (Later MS9+)

Once Block<->Arrow bridges exist, translation can move beyond gtests:

- Identify a smallest “real” TiFlash pipeline shape that is stable (source -> filter -> projection -> agg -> sink).
- Translate only that subset into TiForth operators.
- Gate by runtime checks and fallback to the existing engine when unsupported patterns appear.

This part is deliberately deferred until the Block bridge is validated.

## Test Strategy

- MS9B: roundtrip conversion gtest for the supported type set.
- MS9C: end-to-end gtest that:
  - builds input `DB::Block` data and runs TiForth pipelines via the Block runner (no Arrow visible to assertions)
  - validates output `DB::Block` values match expectations
  - current coverage includes: collated string filter; 2-key hash agg (collated string + int32); 2-key hash join (collated strings + Decimal256, with MyDateTime carried through output)

## Files / Modules (Planned)

- `dbms/src/Flash/TiForth/ArrowTypeMapping.{h,cpp}` (exists; extend if needed)
- `dbms/src/Flash/TiForth/ArrowBlockConversion.{h,cpp}` (new; RecordBatch <-> Block)
- `dbms/src/Flash/TiForth/BlockPipelineRunner.{h,cpp}` (new; test-only runner initially)
- `dbms/src/Flash/tests/gtest_tiforth_block_roundtrip.cpp` (new)
- `dbms/src/Flash/tests/gtest_tiforth_block_runner.cpp` (new)
