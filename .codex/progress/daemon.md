# Work in Progress

# (none)

# To Do

- C ABI follow-ups (steps: add ArrowArrayStream I/O or schema-only push; expand options/versioning rules; add misuse/negative tests for ownership + error mapping).

# Completed
- Core TiForth + TiFlash guarded integration: Arrow-native pipeline framework + operators (Projection/Filter/HashAgg/HashJoin/Sort) plus Block<->Arrow conversion + gtests; functions integrated via Arrow compute registry overlay + MetaFunction overrides; milestone docs kept in `docs/design/2026-01-14-tiforth-milestone-*.md`.
- Semantics parity expansion (common path): TiDB collations (binary/pad/general_ci/unicode_ci 0400/0900) with vectorized compare + minimal coercion; decimal `add` parity (decimal+decimal + decimal+int) with TiFlash infer/overflow; packed MyTime temporal scalars including week/yearweek.
- External host surface: minimal `tiforth_capi` C ABI with engine/pipeline/task, expression builder, and Arrow C Data Interface push/pull; smoke test runs filter+projection pipeline via C.
- Collation-aware hashing: port TiDB collator sort-key generation into TiForth (`SortKeyString`), use it to normalize HashAgg/HashJoin string keys for `GeneralCI` + Unicode CI (0400/0900, incl 0900 no-pad); hash uses stable FNV on normalized bytes; tests added (TiForth: `libs/tiforth/tests/tiforth_hash_agg_test.cpp`, `libs/tiforth/tests/tiforth_hash_join_test.cpp`; TiFlash: `dbms/src/Flash/tests/gtest_tiforth_block_runner.cpp`).
- Packed MyTime TiDB scalars: add Arrow scalar funcs `tidbDayOfWeek`/`tidbWeekOfYear`/`yearWeek` for packed MyDate/MyDateTime (UInt64 + `MyTimeOptions`), return null on invalid (month/day zero) and match TiFlash week modes (`week(3)` / `yearWeek(mode=2)`); wire `tiforth::Expr` options for these names; tests (TiForth: `libs/tiforth/tests/tiforth_mytime_temporal_test.cpp`; TiFlash: `dbms/src/Flash/tests/gtest_tiforth_block_runner.cpp`).
