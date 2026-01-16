# Work in Progress
- MS7A: plumb `arrow::MemoryPool` through `Engine`/operators; add unit tests (allocation routing).

# To Do
- MS7B: add spill hook interfaces (no spill implementation yet); thread through `EngineOptions`.
- MS7C: add initial C ABI header skeleton + versioning notes (no bindings yet).

# Completed
- MS7 design: memory pool + spill hooks + ABI/bindings plan. Files: docs/design/2026-01-14-tiforth-milestone-7-memory-pool-spill-abi.md, .codex/progress/daemon.md.
- MS2-MS4: TiForth foundation + first compute ops (RecordBatch task/pipeline framework, Expr IR, Projection, Filter) + TiFlash translation gtests (pass-through, filter). Key decisions: Arrow C++ boundary, host-driven task stepping, Arrow compute delegation + lazy init, schema stability via cached schema pointers. Files: libs/tiforth/include/tiforth/*, libs/tiforth/src/tiforth/*, docs/design/2026-01-14-tiforth-milestone-{1,2,3,4}-*.md, dbms/src/Flash/tests/gtest_tiforth_pipeline_translate.cpp, .codex/progress/daemon.md.
- MS5: Hash aggregation (blocking end-of-stream output; single int32 key; `count_all` + `sum_int32`) + TiFlash translation gtest (dummy agg -> TiForth hash agg). Files: docs/design/2026-01-14-tiforth-milestone-5-hash-aggregation.md, libs/tiforth/include/tiforth/operators/hash_agg.h, libs/tiforth/src/tiforth/operators/hash_agg.cc, libs/tiforth/tests/tiforth_hash_agg_test.cpp, dbms/src/Flash/tests/gtest_tiforth_pipeline_translate.cpp, .codex/progress/daemon.md.
- MS6: Sort + hash join (Sort: blocking single-key ASC nulls-last via Arrow `SortIndices`/`Take`; Join: inner join on int32 key with preloaded build side) + TiFlash translation gtests (dummy hash agg/join -> TiForth ops). Files: docs/design/2026-01-14-tiforth-milestone-6-join-sort.md, libs/tiforth/include/tiforth/operators/{sort.h,hash_join.h}, libs/tiforth/src/tiforth/operators/{sort.cc,hash_join.cc}, libs/tiforth/tests/{tiforth_sort_test.cpp,tiforth_hash_join_test.cpp}, dbms/src/Flash/tests/gtest_tiforth_pipeline_translate.cpp, .codex/progress/daemon.md.
