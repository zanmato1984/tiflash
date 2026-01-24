# TiForth MS19: Benchmark Small-String Single-Key HashAgg (HashAgg vs TiFlash Native)

- Author(s): zanmato
- Last Updated: 2026-01-21
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Related milestone: `docs/design/2026-01-14-tiforth-milestone-18-small-string-single-key-grouper.md`
- Discussion PR: TBD
- Tracking Issue: TBD

## Summary

Add a dedicated benchmark suite for **small-string single-key GROUP BY** comparing:

- TiFlash native aggregation (baseline)
- TiForth HashAgg (Arrow grouper + grouped `hash_*` kernels + MS18 small-string grouper)

Then update the existing benchmark report doc with these results and analysis.

## Motivation / Problem

Previous Arrow-compute aggregation benchmarks focused on numeric keys and general distributions; string keys were
identified as a hotspot and required special handling.

After MS18, TiForth should have a tailored grouper fast path for small strings. MS19 validates:

- performance wins (or regressions) on the exact target workload,
- scalability vs cardinality/skew,
- memory behavior.

## Goals

- Add `bench_dbms` benchmarks that isolate the **GROUP BY small-string single-key** case.
- Produce a clear comparison against TiFlash native semantics/performance.
- Update `docs/design/2026-01-19-arrow-compute-agg-benchmark-report.md` with:
  - new benchmark tables/plots,
  - a short analysis and decision (default selection justification),
  - any remaining gaps (collation, long strings, multi-key).

## Non-goals

- Collation-aware benchmarking (separate milestone once collation grouping exists).
- End-to-end SQL benchmarks; this milestone targets the aggregation core.

## Benchmark Design

### Dataset matrix

Generate synthetic string keys (deterministic) with knobs:

- key length buckets: `{1..4}`, `{5..8}`, `{9..16}`, `{17..32}` (small-string boundary sensitivity)
- cardinality: `{1, 16, 256, 4096, N}` (high-card / unique)
- distribution: uniform vs Zipf-like skew
- null patterns: no nulls vs null keys vs null values
- batching: single batch vs multi-batch streaming

Values:

- use `Int64` and/or `Float64` payloads with a small aggregate set matching HashAgg supported subset.

### Operators compared

- TiFlash native: `DB::Aggregator` path
- TiForth HashAgg (Arrow-based)

### Metrics

- throughput (rows/s)
- latency (ns/row)
- peak memory (if available in existing bench harness)

## Implementation Plan (Checklist)

- [x] Add new `bench_dbms` benchmark cases for small-string single-key grouping (`dbms/src/Flash/tests/bench_tiforth_hash_agg_small_string.cpp`).
- [x] Compare TiFlash native vs TiForth HashAgg.
- [x] Run benchmark matrix and capture results.
- [x] Update `docs/design/2026-01-19-arrow-compute-agg-benchmark-report.md` with:
  - new section “Small-string single-key GROUP BY (HashAgg)”
  - raw numbers + key takeaways

## Validation / Repro

- Build benchmark: `ninja -C cmake-build-release bench_dbms`
- Run: `bench_dbms --benchmark_filter '^(HashAgg|NativeAgg)/SmallStringSingleKey/.*'`

## Notes

- The benchmark should treat strings as **binary semantics** (collation out of scope).
- If results are mixed, keep HashAgg behind a setting until confidence is higher.
