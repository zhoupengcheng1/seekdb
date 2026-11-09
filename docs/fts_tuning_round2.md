# Second full-text tuning experiment

Reference: `94bf9be0f151034f994ae56e37dcf337f73b0881` (the first FTS tuning plus
the file SQL compatibility fixes). The contest reported 4.123/50 for the first
FTS version: an aggregate reduction of 4.122857%, not a submission error.

## Implementation

* Parameterize the existing sorted-array merger, retaining capacity three for
  ordinary storage callers. Sparse retrieval and block-stat merging use it up
  to eight input streams; larger sets retain the loser tree. All comparisons,
  equal-key handling and stream processing still execute.
* Compare equal-length, non-null, in-row binary VARCHAR document IDs directly.
  Other types, collations, lengths, nulls and out-of-row values retain the
  generic comparator. There are no assumptions about a particular ID length.
* Build one immutable view per built-in IK dictionary in the server dictionary
  hub, rather than reconstructing views and acquiring cache handles per document.
  Successful initialization is published with acquire/release atomics; failed
  initialization is discarded and can be retried. Match cursors and parser
  buffers remain private. Three bounded views pin their underlying dictionary
  data until shutdown, increasing persistent residency in exchange for reuse.
  This applies only to the existing built-in dictionaries, not mutable custom
  dictionaries. Release parser data before destroying its backing cache.

## Final measurements

Two alternating complete runs per binary, four identical physical performance
cores (8,10,12,14), cpu_count=4, memory_budget=4G, identical release builds.
The benchmark, dataset generator, SQL and score scripts are unchanged. All runs
use 20,000 documents, batch 500, tokenize rounds 3,000, query rounds 200,
three samples, warmup 30 and SKIP_LOAD=0, with fresh reports and index rebuilds.
Exploratory measurements concurrent with compilation are excluded.

| Metric | Reference mean | Candidate mean | Reduction |
| --- | ---: | ---: | ---: |
| build_ik_all_sec | 7.50950 | 7.56250 | -0.71% |
| build_ik_content_sec | 5.93600 | 5.93300 | 0.05% |
| build_beng_en_sec | 3.12850 | 3.12950 | -0.03% |
| tokenize_ik_avg_ms | 0.09995 | 0.09630 | 3.65% |
| tokenize_beng_avg_ms | 0.05640 | 0.05770 | -2.30% |
| query_cn_avg_ms | 1.69510 | 1.67235 | 1.34% |
| query_beng_avg_ms | 2.58205 | 2.40550 | 6.84% |
| query_mixed_avg_ms | 1.92760 | 1.82860 | 5.14% |
| query_limit_avg_ms | 2.25775 | 2.20825 | 2.19% |

Category-first equal weighting gives an additional **1.44046%** reduction on
this machine, including regressions. This is a modest improvement, not a
solution to the 50% reduction target. These measurements cannot predict the
official score: the current official per-metric reference values were not
provided, and the repository's historical CI baseline is not comparable to
this machine. Do not report its local 100/100 score as a contest result.

## Validation

* Final release build succeeded.
* Twelve FTS SQL cases passed on each binary, including parser properties,
  dictionary cache flush, DML, rollback, index construction and partitioned
  queries.
* The concurrent differential TOKENIZE test passed.
* `test_fts_query_equivalence.py` passed 21 complete ID/ranking/score comparisons
  across 153,612 returned rows, including queries on both sides of the merger
  crossover and larger streams. It uses the generated benchmark database and
  compares scores rounded to 12 decimal places, not just hit counts.
* AI function integration and ordinary SQL minitest passed.

Final evidence: `/home/zhoupengcheng/fts-final-evidence`; final build log:
`/home/zhoupengcheng/fts-dict-evidence/final-build.log`. Earlier experiments are
retained separately under `fts-query-evidence` and `fts-dict-evidence` and are
not included in the final table.
