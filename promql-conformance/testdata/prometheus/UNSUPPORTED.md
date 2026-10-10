# What the engine is missing

Generated — do not edit by hand. Regenerated alongside `SUPPORTED.toml` by:

```sh
PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest
```

Nothing here gates CI. This file is the roadmap, and the core line below is the target: it reads 100% when the engine passes every eval that is neither native histograms, subquery nor experimental syntax. An eval is flag-gated if a stock parser refuses its query at a feature gate, or by its file where the grammar has no productions to reach the gate with (`docs/feature-flags.md`, Conformance). An eval is native-histogram if an expected row is a native histogram sample or a selector in its query matches, by its label matchers, a series the active load blocks load with native histogram samples (`{{…}}`, or derived by `load_with_nhcb`). Each eval is in exactly one category, first match wins: flag-gated, native histograms, subquery, core. The category comes from the eval, never from whether it passes, so the denominators hold still.

**Core: 678 of 871 evals pass (77.8%).**

- Native histograms: 2 of 805 pass.
- Subqueries: 0 of 65 pass.
- Experimental (flag-gated): 65 of 357 pass, run with every gate open and never in `SUPPORTED.toml`; `docs/feature-flags.md` lists the flags.
  - `promql-experimental-functions`: 13 of 174
  - `promql-duration-expr`: 0 of 51
  - `promql-extended-range-selectors`: 8 of 88
  - `promql-binop-fill-modifiers`: 44 of 44

All but the flag-gated: **680 of 1741 evals pass** (39.1%).

## Core evals

| | evals |
|---|---:|
| total | 871 |
| passing | 678 |
| not passing | 193 |

The not-passing ones are the missing features and the unattributed failures below. Fixing those is what takes the core to 100%.

## Why core evals fail

Each failing core eval is filed under a missing feature the engine names, or left unattributed: what is left there is the work nobody has named yet.

| failing evals | reason |
|---:|---|
| 172 | missing features |
| 21 | unattributed |
| 193 | **total** |

## Missing features

| evals blocked | feature |
|---:|---|
| 49 | the histogram_quantile function |
| 29 | the histogram_fraction function |
| 21 | the label_replace function |
| 19 | the topk aggregation |
| 12 | the label_join function |
| 8 | the predict_linear function |
| 8 | the quantile_over_time function |
| 7 | the bottomk aggregation |
| 6 | the quantile aggregation |
| 3 | a range selector |
| 3 | the deriv function |
| 3 | the stddev_over_time function |
| 2 | the stdvar_over_time function |
| 1 | the changes function over a parenthesized expression |
| 1 | the rate function over a parenthesized expression |

## Unattributed failures

Core evals the engine does not name a missing feature for: wrong results, panics, errors nobody has classified. Ids are spelled as in `SUPPORTED.toml`.

### at_modifier (2)

- `metric @ 100s @ 10000`
- `metric @ 1m40s @ 10000`

### name_label_dropping (3)

- `sum by (__name__) (rate({env="1"}[10m])) @ 600000`
- `sum by (__name__) (metric_total{env="1"} or rate(metric_total{env="2"}[5m])) @ 600000`
- `sum by (__name__) (rate(metric_total{env="2"}[5m]) or metric_total{env="1"}) @ 600000`

### operators (1)

- `-{job="api"} @ 0..1200000/1200000`

### type_and_unit (15)

- `-http_requests_total{job="api-server",instance="0",group="production"} @ 3000000`
- `http_requests_total{group="canary", instance="0", job="api-server"} / 0 @ 3000000`
- `0 * http_requests_total{group="canary", instance="0", job="api-server"} % 0 @ 3000000`
- `rate(http_requests_total[25m]) * 25 * 60 @ 3000000`
- `(http_requests_total{group="canary"} + 1) and http_requests_total{instance="0"} @ 3000000`
- `(http_requests_total{group="canary"} + 1) or http_requests_total{instance="1"} @ 3000000`
- `http_requests_total{group="canary"} / ignoring(group) http_requests_total{group="production"} @ 3000000`
- `http_requests_total{job="api-server", instance="0", group="production"} == bool 100 @ 3000000`
- `-http_requests_total{job="api-server",instance="0",group="production"} @ 3000000 #2`
- `http_requests_total{group="canary", instance="0", job="api-server"} / 0 @ 3000000 #2`
- `0 * http_requests_total{group="canary", instance="0", job="api-server"} % 0 @ 3000000 #2`
- `rate(http_requests_total[25m]) * 25 * 60 @ 3000000 #2`
- `(http_requests_total{group="canary"} + 1) and http_requests_total{instance="0"} @ 3000000 #2`
- `(http_requests_total{group="canary"} + 1) or http_requests_total{instance="1"} @ 3000000 #2`
- `http_requests_total{job="api-server", instance="0", group="production"} == bool 100 @ 3000000 #2`

## By file

| file | evals | native histograms | subquery | flag-gated | passing | passing core | |
|---|---:|---:|---:|---:|---:|---:|---|
| selectors | 31 | 0 | 0 | 0 | 31 (100%) | 31 of 31 (100%) | core green |
| extended_vectors | 30 | 0 | 0 | 88 | 30 (100%) | 30 of 30 (100%) | core green |
| literals | 25 | 0 | 0 | 0 | 25 (100%) | 25 of 25 (100%) | core green |
| trig_functions | 19 | 0 | 0 | 0 | 19 (100%) | 19 of 19 (100%) | core green |
| staleness | 17 | 0 | 0 | 0 | 17 (100%) | 17 of 17 (100%) | core green |
| duration_expression | 8 | 0 | 0 | 51 | 8 (100%) | 8 of 8 (100%) | core green |
| collision | 2 | 0 | 0 | 0 | 2 (100%) | 2 of 2 (100%) | core green |
| subquery | 34 | 2 | 30 | 0 | 2 (6%) | 2 of 2 (100%) | core green |
| fill-modifier | 1 | 0 | 0 | 44 | 1 (100%) | 1 of 1 (100%) | core green |
| operators | 213 | 72 | 0 | 0 | 141 (66%) | 139 of 141 (99%) |  |
| at_modifier | 69 | 0 | 29 | 2 | 35 (51%) | 35 of 40 (88%) |  |
| functions | 350 | 99 | 5 | 63 | 196 (56%) | 196 of 246 (80%) |  |
| range_queries | 16 | 2 | 0 | 2 | 11 (69%) | 11 of 14 (79%) |  |
| aggregators | 158 | 35 | 0 | 2 | 93 (59%) | 93 of 123 (76%) |  |
| type_and_unit | 58 | 0 | 0 | 0 | 43 (74%) | 43 of 58 (74%) |  |
| name_label_dropping | 29 | 0 | 1 | 1 | 18 (62%) | 18 of 28 (64%) |  |
| histograms | 167 | 84 | 0 | 18 | 8 (5%) | 8 of 83 (10%) |  |
| native_histograms | 514 | 511 | 0 | 7 | 0 (0%) | 0 of 3 (0%) |  |
| info | 0 | 0 | 0 | 42 | 0 (0%) | 0 of 0 (0%) | no core evals |
| limit | 0 | 0 | 0 | 37 | 0 (0%) | 0 of 0 (0%) | no core evals |

---

Some failures are one cause wearing many hats — a single lexer gap can account for hundreds of rows above. Read a handful before picking, with:

```sh
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- <file>/
```
