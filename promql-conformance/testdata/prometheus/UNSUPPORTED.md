# What the engine is missing

Generated — do not edit by hand. Regenerated alongside `SUPPORTED.toml` by:

```sh
PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest
```

Nothing here gates CI. This file is the roadmap, and the second line below is the target: when every eval that is neither native-histogram nor subquery passes, it reads 100%. Every failing eval is filed under exactly one reason, tried in the order of the table, so what is left under *unattributed* is the work nobody has named yet.

Against the vendored corpus: **677 of 1825 evals pass** (37.1%), and **186** are blocked on the features below.

Excluding native histograms and subqueries: **467 of 760** pass (61.4%). An eval is excluded if any series in its load blocks or its expected rows is a native histogram, or its query contains a subquery, whether it passes or not, so the denominator holds still.

## Core evals

| | evals |
|---|---:|
| total | 760 |
| passing | 467 |
| not passing | 293 |

The not-passing ones are the missing features and the unattributed failures below. Fixing those is what takes the core to 100%.

## Why evals fail

| failing evals | reason |
|---:|---|
| 792 | native histograms |
| 63 | subquery |
| 186 | missing features |
| 107 | unattributed |
| 1148 | **total** |

## Missing features

| evals blocked | feature |
|---:|---|
| 83 | the histogram_quantile function |
| 56 | the histogram_fraction function |
| 21 | the label_replace function |
| 10 | the topk aggregation |
| 6 | the label_join function |
| 2 | the histogram_count function |
| 2 | the histogram_sum function |
| 1 | a range selector |
| 1 | the histogram_avg function |
| 1 | the histogram_stddev function |
| 1 | the histogram_stdvar function |
| 1 | the stddev_over_time function |
| 1 | the stdvar_over_time function |

## Unattributed failures

Not native histograms, not a subquery, and the engine does not name a missing feature: wrong results, panics, errors nobody has classified. Ids are spelled as in `SUPPORTED.toml`.

### at_modifier (2)

- `metric @ 100s @ 10000`
- `metric @ 1m40s @ 10000`

### duration_expression (51)

- `changes(http_requests[26m+4m]) @ 3000000`
- `changes(http_requests[30m+0s]) @ 3000000`
- `changes(http_requests[60*30]) @ 3000000`
- `changes(http_requests[2m*15]) @ 3000000`
- `changes(http_requests[2m*(10+5)]) @ 3000000`
- `changes(http_requests[29m+60s]) @ 3000000`
- `changes(http_requests[24m+((1.5*2m)+2m)]) @ 3000000`
- `changes(http_requests[-5m+35m]) @ 3000000`
- `changes(http_requests[1h/2]) @ 3000000`
- `changes(http_requests[1h30m % 1h]) @ 3000000`
- `changes(http_requests[30m1s-30m1s % 1m]) @ 3000000`
- `changes(http_requests[(9m30s+30s)*3]) @ 3000000`
- `sum_over_time(metric1_total[29s+1s:5s+5s]) @ 1000000`
- `sum_over_time(metric1_total[29s+1s:((((8 - 2) / 3) * 7s) % 4) + 8000ms]) @ 1000000`
- `sum_over_time(metric1_total[29s+1s:20*500ms] offset (20*(((((8 - 2) / 3) * 7s) % 4) + 8000ms))) @ 1200000`
- `sum_over_time(metric1_total[29s+1s:20*500ms] offset -(20*(((((8 - 2) / 3) * 7s) % 4) + 8000ms))) @ 800000`
- `metric1_total offset (100 + 2) @ 1000000`
- `metric1_total offset (2 ^ 2) @ 1000000`
- `metric1_total offset (2 * 2) @ 1000000`
- `metric1_total offset (-2 * 2) @ 1000000`
- `metric1_total offset (-2 ^ 2) @ 1000000`
- `count_over_time(metric1_total[step()]) @ 50000..60000/10000`
- `count_over_time(metric1_total[step()+1ms]) @ 50000..60000/10000`
- `count_over_time(metric1_total[(step())+1]) @ 50000..60000/10000`
- `count_over_time(metric1_total[1+(STep()-5)*2]) @ 50000..60000/10000`
- `count_over_time(metric1_total[step()+1]) @ 50000..60000/5000`
- `count_over_time(metric1_total[min(step()+1,1h)]) @ 50000..60000/5000`
- `count_over_time(metric1_total[max(min(step()+1,1h),1ms)]) @ 50000..60000/5000`
- `count_over_time(metric1_total[((max(min((step()+1),((1h))),1ms)))]) @ 50000..60000/5000`
- `metric1_total offset STEP() @ 50000..60000/5000`
- `metric1_total offset step() @ 50000..60000/5000`
- `metric1_total offset step()*0 @ 50000..60000/5000`
- `metric1_total offset (-step()*2) @ 50000..60000/5000`
- `metric1_total offset -step()*2 @ 50000..60000/5000`
- `metric1_total offset step()^0 @ 50000..60000/5000`
- `metric1_total offset (STEP()/10) @ 50000..60000/5000`
- `metric1_total offset (step()) @ 50000..60000/5000`
- `metric1_total offset min(step(), 1s) @ 50000..60000/5000`
- `metric1_total offset min(step(), 1s)+8000 @ 50000..60000/5000`
- `metric1_total offset -min(step(), 1s)+8000 @ 50000..60000/5000`
- `metric1_total offset -(min(step(), 1s))+8000 @ 50000..60000/5000`
- `metric1_total offset -min(step(), 1s)^0 @ 50000..60000/5000`
- `metric1_total offset +min(step(), 1s)^0 @ 50000..60000/5000`
- `metric1_total offset min(step(), 1s)^0 @ 50000..60000/5000`
- `metric1_total offset max(3s,min(step(), 1s))+8000 @ 50000..60000/5000`
- `metric1_total offset -(min(step(), 2s)-5)+8000 @ 50000..60000/5000`
- `count_over_time(metric1_total[range()]) @ 50000..60000/10000`
- `count_over_time(metric1_total[range()]) @ 50000..60000/5000`
- `metric1_total offset range() @ 50000..60000/5000`
- `metric1_total offset min(range(), 8s) @ 50000..60000/5000`
- `metric1_total offset range() @ 50000`

### functions (33)

- `step() @ 0..10000/5000`
- `step() @ 0..20000/10000`
- `step() @ 0..30000/15000`
- `range() @ 0..30000/10000`
- `range() @ 0..60000/20000`
- `range() @ 10000..50000/10000`
- `step() @ 0`
- `range() @ 0`
- `range() @ 100000`
- `vector(step()) @ 0..10000/5000`
- `vector(range()) @ 0..20000/10000`
- `metric * step() @ 0..4000/1000`
- `metric + range() @ 0..2000/1000`
- `step() @ 100000..110000/5000`
- `range() @ 100000..130000/10000`
- `start() @ 0..30000/10000`
- `start() @ 100000..130000/10000`
- `start() @ 50000..60000/5000`
- `end() @ 0..30000/10000`
- `end() @ 100000..140000/10000`
- `end() @ 20000..50000/10000`
- `start() @ 0`
- `start() @ 100000`
- `end() @ 0`
- `end() @ 50000`
- `vector(start()) @ 0..20000/10000`
- `vector(end()) @ 10000..30000/10000`
- `start() @ 100000..130000/10000 #2`
- `end() @ 100000..130000/10000`
- `end() - start() @ 10000..20000/5000`
- `(end() + start()) / 2 @ 0..30000/10000`
- `start() + range() @ 0..20000/10000`
- `end() - range() @ 10000..50000/10000`

### histograms (2)

- `count(request_duration_seconds) @ 3000000`
- `resets(histogram_with_reset[15m]) @ 600000`

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

## Left out: experimental syntax

**273** more evals use syntax that stock Prometheus refuses unless an `--enable-feature` value is on. They are not part of the core language yet, so they are in no total above, in no row of the tables, and not in `SUPPORTED.toml`. The harness still evaluates them with every gate open; `docs/feature-flags.md` lists the flags.

| evals | needs |
|---:|---|
| 141 | `promql-experimental-functions` |
| 88 | `promql-extended-range-selectors` |
| 44 | `promql-binop-fill-modifiers` |

## By file

| file | evals | native histograms | subquery | passing | passing core | |
|---|---:|---:|---:|---:|---:|---|
| selectors | 31 | 0 | 0 | 31 (100%) | 31 of 31 (100%) | fully green |
| extended_vectors | 30 | 0 | 0 | 30 (100%) | 30 of 30 (100%) | fully green |
| literals | 25 | 0 | 0 | 25 (100%) | 25 of 25 (100%) | fully green |
| trig_functions | 19 | 0 | 0 | 19 (100%) | 19 of 19 (100%) | fully green |
| staleness | 17 | 0 | 0 | 17 (100%) | 17 of 17 (100%) | fully green |
| collision | 2 | 0 | 0 | 2 (100%) | 2 of 2 (100%) | fully green |
| fill-modifier | 1 | 0 | 0 | 1 (100%) | 1 of 1 (100%) | fully green |
| type_and_unit | 58 | 0 | 0 | 43 (74%) | 43 of 58 (74%) |  |
| range_queries | 16 | 7 | 0 | 11 (69%) | 8 of 9 (89%) |  |
| operators | 213 | 165 | 0 | 141 (66%) | 47 of 48 (98%) |  |
| name_label_dropping | 29 | 0 | 1 | 18 (62%) | 18 of 28 (64%) |  |
| aggregators | 158 | 62 | 0 | 93 (59%) | 88 of 96 (92%) |  |
| at_modifier | 69 | 0 | 29 | 35 (51%) | 35 of 40 (88%) |  |
| functions | 383 | 240 | 4 | 193 (50%) | 85 of 139 (61%) |  |
| duration_expression | 59 | 0 | 0 | 8 (14%) | 8 of 59 (14%) |  |
| subquery | 34 | 3 | 29 | 2 (6%) | 2 of 2 (100%) |  |
| histograms | 167 | 11 | 0 | 8 (5%) | 8 of 156 (5%) |  |
| native_histograms | 514 | 514 | 0 | 0 (0%) | 0 of 0 (0%) |  |
| info | 0 | 0 | 0 | 0 (0%) | 0 of 0 (0%) | all gated — experimental syntax |
| limit | 0 | 0 | 0 | 0 (0%) | 0 of 0 (0%) | all gated — experimental syntax |

---

Some failures are one cause wearing many hats — a single lexer gap can account for hundreds of rows above. Read a handful before picking, with:

```sh
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- <file>/
```
