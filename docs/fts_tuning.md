# Full-text tokenization tuning

## Scope and evidence

The reference is commit `7445bd3d823b45980ea626902111af77b3ec8e76`.
This change optimizes document/query tokenization without changing dictionaries,
token boundaries, filtering rules, frequencies, ranking, or the benchmark.

On the reference binary, a separate tokenization workload sampled with Linux
`perf record -e cycles:u -F 199` shows bucket locking and collation hashing on
the hot path. In the efficiency-core sample group, `TCRWLock::wrlock` accounts
for 4.76% and `ob_hash_sort_utf8mb4` for 3.32% of sampled cycles. These are
workload-specific samples, not fractions of the complete contest benchmark.
Source inspection identifies repeated Unicode decoding in IK classification,
repeated access to the already decoded character in each IK processor, and a
hash-table read followed by an overwrite for every aggregated token.

## Changes and invariants

* Decode one IK character once before applying the original ordered Unicode
  predicates. Retain UTF-16-only surrogate classification and decoder errors.
* Pass the validated character, length and type to all four IK processors in
  their original order, rather than fetching them again in each processor.
* Aggregate frequencies with `set_or_update`, which hashes and searches one
  bucket. Preserve both the original first-occurrence count and replacement
  of the representative key for collation-equivalent spellings. For example,
  preserving the last representative matters for `café` versus `cafe`.
* Use `NoPthreadDefendMode` for the temporary word map. Every construction site
  is local to synchronous expression evaluation, index-row generation, query
  planning or query-token extraction. None publishes the map to other workers.
  Shared dictionaries and dictionary-cache synchronization are unchanged.
  Both retrieval implementations and the optimizer use the common map alias.

## Full benchmark comparison

Both binaries use the same release build settings, `cpu_count=4`, a 4 GiB memory
budget, and CPU affinity `8,10,12,14` (four physical performance cores). Two
complete runs per binary alternate reference/candidate. Each run uses the
unchanged benchmark with `ROWS=20000 BATCH=500 ROUNDS=3000 QUERY_ROUNDS=200
SAMPLES=3 WARMUP=30 SKIP_LOAD=0`. Every run reloads the data and rebuilds indexes
and writes a fresh report. The following values are arithmetic means of those
two complete reports per binary; positive percentages mean lower elapsed time.

| Metric | Reference | Candidate | Reduction |
| --- | ---: | ---: | ---: |
| build_ik_all_sec | 7.80850 | 7.55850 | 3.20% |
| build_ik_content_sec | 6.13400 | 6.08450 | 0.81% |
| build_beng_en_sec | 3.23000 | 3.12750 | 3.17% |
| tokenize_ik_avg_ms | 0.10515 | 0.10155 | 3.42% |
| tokenize_beng_avg_ms | 0.05910 | 0.05705 | 3.47% |
| query_cn_avg_ms | 1.66260 | 1.69545 | -1.98% |
| query_beng_avg_ms | 2.51620 | 2.59415 | -3.10% |
| query_mixed_avg_ms | 1.91845 | 1.96100 | -2.22% |
| query_limit_avg_ms | 2.23920 | 2.23830 | 0.04% |

Applying the category-first, three-way equal weighting gives a **1.34%** local
aggregate reduction, including query regressions. The change does not achieve
the 50% reduction threshold relative to its immediate parent. Improvements are
small and should be rechecked in the official environment; do not infer a
significant query improvement from this experiment. No query execution change
is included.

The unmodified fixed-baseline scorer reports 100/100 for the candidate on this
machine, but the reference is already far faster than that historical CI
baseline. That cross-machine score is not evidence of an 84% patch improvement
and is not an official competition result.

## Correctness and reproduction

`fts_index.tokenize_hot_path` contains expected outputs captured from the
reference binary. It checks complete token frequencies, collation-equivalent
representatives, mixed Chinese/English and supplementary Unicode, a long input
crossing segmentation boundaries, other parsers, index updates, rollback and
deletion. Existing full-text regression cases cover index construction, parser
properties, natural-language and Boolean queries, DML and partitioned tables.

The differential test sends 2,004 generated statements to each binary and
compares complete outputs and error diagnostics, with four concurrent clients
on the candidate:

```sh
python3 unittest/sql/engine/expr/test_fts_tokenize_equivalence.py \
  --client /path/to/obclient --reference-port 12892 --port 12893 --workers 4
```

This build accepts the 668 UTF-8 cases. Its SQL layer rejects the 1,336 UTF-16
and UTF-16LE cases; the comparison verifies those errors remain unchanged and
does not claim those are successful UTF-16 tokenization tests.

Local evidence is retained under `/home/zhoupengcheng/fts-tuning-evidence`:
`before-repeat-{1,2}.txt`, `after-final-{1,2}.txt`, `comparison.json`,
`token-profile.txt`, `build7.log`, `final-equivalence.log` and
`final-regression.log`. Initial exploratory reports use different CPU-affinity
conditions and are not included in the table above. The benchmark scripts,
generation SQL, timer, score calculation and fixed baseline remain unchanged.
