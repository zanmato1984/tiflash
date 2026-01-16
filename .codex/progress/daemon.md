# Work in Progress
- MS4A: implement `FilterTransformOp` + minimal predicate evaluation (single-input path); add TiForth unit tests.

# To Do
- MS4B: add TiFlash gtest (guarded by `TIFLASH_ENABLE_TIFORTH`) translating a TiFlash DAG with filter into a TiForth pipeline; validate output (start with hardcoded expected, later diff vs TiFlash exec).

# Completed
- MS4 design: Filter + predicate evaluation plan (SQL WHERE semantics via Arrow compute filter; minimal boolean/comparison kernels; initial differential testing plan). Files: docs/design/2026-01-14-tiforth-milestone-4-filter-expression-evaluation.md, .codex/progress/daemon.md.
- MS2-MS3 foundation: Arrow boundary + build/integration (always require ArrowCompute; simplify gtest discovery; add consumer examples matrix; remove in-tree Arrow; TiFlash option `ENABLE_TIFORTH` + macro guard `TIFLASH_ENABLE_TIFORTH`). Files: libs/tiforth/CMakeLists.txt, libs/tiforth/cmake/find/*, libs/tiforth/examples/*, top-level CMakeLists.txt and dbms CMakeLists/tests.
- MS3 pipeline + compute: TiFlash-shaped pipeline exec in TiForth (Task push/pull, PipelineBuilder/PipelineExec, PassThroughTransformOp; TiFlash DAG→TiForth translation gtest) + first compute operator set (Expr IR + ProjectionTransformOp + unit test). Files: docs/design/2026-01-14-tiforth-milestone-2-recordbatch-plumbing.md, docs/design/2026-01-14-tiforth-milestone-3-pipeline-framework.md, docs/design/2026-01-14-tiforth-milestone-3-projection-scalar-functions.md, libs/tiforth/include/tiforth/*, libs/tiforth/src/tiforth/*, dbms/src/Flash/tests/gtest_tiforth_*.cpp, .codex/progress/daemon.md.
