# TiForth MS19: Benchmark Small-String Single-Key HashAgg (ArrowHashAgg vs TiFlash Native)

- Author(s): zanmato
- Last Updated: 2026-01-20
- Status: Planned
- Related design: `docs/design/2026-01-14-tiforth.md`
- Related milestone: `docs/design/2026-01-14-tiforth-milestone-18-small-string-single-key-grouper.md`
- Discussion PR: TBD
- Tracking Issue: TBD

## Summary

Add a dedicated benchmark suite for **small-string single-key GROUP BY** comparing:

- TiFlash native aggregation (baseline)
- TiForth default aggregation after MS17+MS18 (ArrowHashAgg + small-string grouper)
- TiForth legacy hash agg (for context / regression detection)

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

- use `Int64` and/or `Float64` payloads with a small aggregate set matching ArrowHashAgg supported subset.

### Operators compared

- TiFlash native: `DB::Aggregator` path
- TiForth:
  - default hash agg after MS17: ArrowHashAgg path
  - legacy hash agg: LegacyHashAggTransformOp (explicitly selected)

### Metrics

- throughput (rows/s)
- latency (ns/row)
- peak memory (if available in existing bench harness)

## Implementation Plan (Checklist)

- [ ] Add new `bench_dbms` benchmark cases for small-string single-key grouping.
- [ ] Ensure selection toggles exist to force:
  - TiForth default (ArrowHashAgg)
  - TiForth legacy
  - TiFlash native
- [ ] Run benchmark matrix and capture results.
- [ ] Update `docs/design/2026-01-19-arrow-compute-agg-benchmark-report.md` with:
  - new section “Small-string single-key GROUP BY”
  - raw numbers + key takeaways
  - decision on default selection + known limitations

## Validation / Repro

- Build benchmark: `ninja -C cmake-build-release bench_dbms`
- Run: `bench_dbms --benchmark_filter '^(ArrowHashAgg|HashAgg|NativeAgg)/SmallStringSingleKey/.*'`

## Notes

- The benchmark should treat strings as **binary semantics** (collation out of scope).
- If results are mixed, keep ArrowHashAgg default behind a setting until confidence is higher.

