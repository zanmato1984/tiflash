# Work in Progress

- Memory accounting gaps: route large temporary normalized-key allocations (collated sort keys, hash key normalization) through Arrow `MemoryPool` where feasible or document explicit exceptions; add a ProxyMemoryPool test for collated sort path.

# To Do

# (none)

# Completed
- TiForth core + TiFlash guarded integration (MS1-6, common path): Arrow-native pipeline/task framework + operators (pass-through/projection/filter/hash agg/hash join/sort) with Block<->Arrow bridge + TiFlash translation/block-runner gtests under `TIFLASH_ENABLE_TIFORTH`.
- Semantics parity + type contract (MS8 + follow-ups): TiDB collations (binary/padding/general_ci/unicode_ci 0400/0900) with vectorized compare + hash normalization + sort-key generation; decimal add parity; packed MyTime temporal scalars; Arrow field metadata as logical-type side channel.
- Host surface + perf (MS7/MS10 + follow-ups): Arrow compute registry integration + expr compilation to bound `Expression`/`ExecuteScalarExpression` (and public `tiforth::CompiledExpr`); `tiforth_capi` C ABI using Arrow C Data/Stream interfaces; memory pool propagation across operators; follow-up To Do list derived via objective review.
- Docs reconciliation: update design docs + C ABI header comments to match current implementation (MS7/MS9/MS10) and refresh main design doc header/milestone wording. Files: `docs/design/2026-01-14-tiforth.md`, `docs/design/2026-01-14-tiforth-milestone-{7,9,10}-*.md`, `libs/tiforth/include/tiforth_c/tiforth.h`. Builds: `ninja -C libs/tiforth/build-debug && ctest`.
- API packaging: exclude internal-only `tiforth/detail/{arrow_compute,expr_compiler}.h` from `cmake --install`, keep only public-dependency `tidb_collation_lut.h`; add `libs/tiforth/scripts/verify_install.sh` to validate install tree + `find_package(tiforth)` consumer compile. Files: `libs/tiforth/CMakeLists.txt`, `libs/tiforth/scripts/verify_install.sh`. Checks: `bash libs/tiforth/scripts/verify_install.sh libs/tiforth/build-debug`, `ninja -C libs/tiforth/build-debug && ctest`.
- Shared helpers: move duplicated Datum->Array conversion logic into `tiforth::detail::DatumToArray` (handles array/chunked/scalar) and reuse it across filter/sort/hash_join/compiled expr/expr compiler. Files: `libs/tiforth/include/tiforth/detail/arrow_compute.h`, `libs/tiforth/src/tiforth/detail/arrow_compute.cc`, `libs/tiforth/src/tiforth/{compiled_expr,detail/expr_compiler,operators/{filter,hash_join,sort}}.cc`. Builds: `ninja -C libs/tiforth/build-debug && ctest`, `ninja -C cmake-build-tiflash-tiforth-debug gtests_dbms && gtests_dbms --gtest_filter=TiForth*`.
