# Work in Progress

- API packaging: stop installing internal-only headers under `libs/tiforth/include/tiforth/detail/` (keep only what public headers require); update CMake install rules accordingly; ensure standalone install + `find_package(tiforth)` still works; add a CI-ish build check locally (`cmake --install` + minimal consumer compile).

# To Do

- Shared helpers: de-duplicate `DatumToArray`/chunked-array concat helpers across operators (filter/sort/hash_join) into one utility; keep behavior identical; run TiForth + TiFlash TiForth gtests.
- Memory accounting gaps: route large temporary normalized-key allocations (collated sort keys, hash key normalization) through Arrow `MemoryPool` where feasible or document explicit exceptions; add a ProxyMemoryPool test for collated sort path.

# Completed
- TiForth core + TiFlash guarded integration (MS1-6, common path): Arrow-native pipeline/task framework + operators (pass-through/projection/filter/hash agg/hash join/sort) with Block<->Arrow bridge + TiFlash translation/block-runner gtests under `TIFLASH_ENABLE_TIFORTH`.
- Semantics parity + type contract (MS8 + follow-ups): TiDB collations (binary/padding/general_ci/unicode_ci 0400/0900) with vectorized compare + hash normalization + sort-key generation; decimal add parity; packed MyTime temporal scalars; Arrow field metadata as logical-type side channel.
- Host surface + perf (MS7/MS10 + follow-ups): Arrow compute registry integration + expr compilation to bound `Expression`/`ExecuteScalarExpression` (and public `tiforth::CompiledExpr`); `tiforth_capi` C ABI using Arrow C Data/Stream interfaces; memory pool propagation across operators; follow-up To Do list derived via objective review.
- Docs reconciliation: update design docs + C ABI header comments to match current implementation (MS7/MS9/MS10) and refresh main design doc header/milestone wording. Files: `docs/design/2026-01-14-tiforth.md`, `docs/design/2026-01-14-tiforth-milestone-{7,9,10}-*.md`, `libs/tiforth/include/tiforth_c/tiforth.h`. Builds: `ninja -C libs/tiforth/build-debug && ctest`.
