# Work in Progress
- MS7: write milestone design doc (memory pool integration + spill hooks + ABI/bindings plan).

# To Do
- (none)

# Completed
- MS2-MS4: TiForth foundation + first compute ops (RecordBatch task/pipeline framework, Expr IR, Projection, Filter) + TiFlash translation gtests (pass-through, filter). Key decisions: Arrow C++ boundary, host-driven task stepping, Arrow compute delegation + lazy init, schema stability via cached schema pointers. Files: libs/tiforth/include/tiforth/*, libs/tiforth/src/tiforth/*, docs/design/2026-01-14-tiforth-milestone-{1,2,3,4}-*.md, dbms/src/Flash/tests/gtest_tiforth_pipeline_translate.cpp, .codex/progress/daemon.md.
- MS5: Hash aggregation (blocking end-of-stream output; single int32 key; `count_all` + `sum_int32`) + TiFlash translation gtest (dummy agg -> TiForth hash agg). Files: docs/design/2026-01-14-tiforth-milestone-5-hash-aggregation.md, libs/tiforth/include/tiforth/operators/hash_agg.h, libs/tiforth/src/tiforth/operators/hash_agg.cc, libs/tiforth/tests/tiforth_hash_agg_test.cpp, dbms/src/Flash/tests/gtest_tiforth_pipeline_translate.cpp, .codex/progress/daemon.md.
- MS6: Sort + hash join (Sort: blocking single-key ASC nulls-last via Arrow `SortIndices`/`Take`; Join: inner join on int32 key with preloaded build side) + TiFlash translation gtests (dummy hash agg/join -> TiForth ops). Files: docs/design/2026-01-14-tiforth-milestone-6-join-sort.md, libs/tiforth/include/tiforth/operators/{sort.h,hash_join.h}, libs/tiforth/src/tiforth/operators/{sort.cc,hash_join.cc}, libs/tiforth/tests/{tiforth_sort_test.cpp,tiforth_hash_join_test.cpp}, dbms/src/Flash/tests/gtest_tiforth_pipeline_translate.cpp, .codex/progress/daemon.md.
