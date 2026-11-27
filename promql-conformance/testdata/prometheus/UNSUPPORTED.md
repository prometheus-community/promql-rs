# What the engine is missing

Generated — do not edit by hand. Regenerated alongside `SUPPORTED.toml` by:

```sh
PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest
```

Nothing here gates CI. This file is the roadmap, and the core line below is the target: it reads 100% when the engine passes every eval that is neither native histograms, subquery nor experimental syntax. An eval is flag-gated if a stock parser refuses its query at a feature gate, or by its file where the grammar has no productions to reach the gate with (`docs/feature-flags.md`, Conformance). An eval is native-histogram if an expected row is a native histogram sample or a selector in its query matches, by its label matchers, a series the active load blocks load with native histogram samples (`{{…}}`, or derived by `load_with_nhcb`). Each eval is in exactly one category, first match wins: flag-gated, native histograms, subquery, core. The category comes from the eval, never from whether it passes, so the denominators hold still.

**Core: 754 of 1056 evals pass (71.4%).**

- Native histograms: 2 of 876 pass.
- Subqueries: 0 of 79 pass.
- Experimental (flag-gated): 67 of 541 pass, run with every gate open and never in `SUPPORTED.toml`; `docs/feature-flags.md` lists the flags.
  - `promql-experimental-functions`: 13 of 282
  - `promql-duration-expr`: 2 of 57
  - `promql-extended-range-selectors`: 8 of 153
  - `promql-binop-fill-modifiers`: 44 of 49

All but the flag-gated: **756 of 2011 evals pass** (37.6%).

## Core evals

| | evals |
|---|---:|
| total | 1056 |
| passing | 754 |
| not passing | 302 |

The not-passing ones are the missing features and the unattributed failures below. Fixing those is what takes the core to 100%.

## Why core evals fail

Each failing core eval is filed under a missing feature the engine names, or left unattributed: what is left there is the work nobody has named yet.

| failing evals | reason |
|---:|---|
| 146 | missing features |
| 156 | unattributed |
| 302 | **total** |

## Missing features

| evals blocked | feature |
|---:|---|
| 54 | the histogram_quantile function |
| 36 | the histogram_fraction function |
| 19 | the topk aggregation |
| 12 | the quantile_over_time function |
| 9 | the quantile aggregation |
| 7 | the bottomk aggregation |
| 6 | the count_values aggregation |
| 1 | the first_over_time function |
| 1 | the predict_linear function with a scalar argument that changes between steps |
| 1 | the scalar function |

## Unattributed failures

Core evals the engine does not name a missing feature for: wrong results, panics, errors nobody has classified. Ids are spelled as in `SUPPORTED.toml`.

### at_modifier (8)

- `metric @ 100s @ 10000`
- `metric @ 1m40s @ 10000`
- `quantile_over_time(scalar(arg), metric[2m1s] @ 2m) @ 0..90000/30000`
- `quantile_over_time(scalar(arg), metric[2m1s] @ 2m) @ 0`
- `quantile_over_time(scalar(arg), metric[2m1s:30s] @ 2m) @ 0..90000/30000`
- `quantile_over_time(scalar(arg), metric[2m1s:30s] @ 2m) @ 0`
- `quantile_over_time(0.5, metric[2m1s:30s] @ 2m) @ 0..90000/30000`
- `quantile_over_time(0.5, metric[2m1s:30s] @ 2m) @ 0`

### bitwise (69)

- `5 bitand 3 @ 0`
- `wide_status bitand 4294967296 @ 0`
- `4294967296 bitand wide_status @ 0`
- `NaN bitand 1 @ 0`
- `1 bitand NaN @ 0`
- `Inf bitand 1 @ 0`
- `1 bitand -Inf @ 0`
- `-1 bitand 1 @ 0`
- `1 bitand -1 @ 0`
- `1.5 bitand 1 @ 0`
- `1 bitand 1.5 @ 0`
- `9007199254740992 bitand 1 @ 0`
- `1 bitand 9007199254740992 @ 0`
- `fractional_status bitand 1 @ 0`
- `5 bitor 3 @ 0`
- `wide_status bitor 4294967296 @ 0`
- `4294967296 bitor wide_status @ 0`
- `NaN bitor 1 @ 0`
- `1 bitor NaN @ 0`
- `Inf bitor 1 @ 0`
- `1 bitor -Inf @ 0`
- `-1 bitor 1 @ 0`
- `1 bitor -1 @ 0`
- `1.5 bitor 1 @ 0`
- `1 bitor 1.5 @ 0`
- `9007199254740992 bitor 1 @ 0`
- `1 bitor 9007199254740992 @ 0`
- `fractional_status bitor 1 @ 0`
- `5 bitxor 3 @ 0`
- `wide_status bitxor 4294967296 @ 0`
- `4294967296 bitxor wide_status @ 0`
- `NaN bitxor 1 @ 0`
- `1 bitxor NaN @ 0`
- `Inf bitxor 1 @ 0`
- `1 bitxor -Inf @ 0`
- `-1 bitxor 1 @ 0`
- `1 bitxor -1 @ 0`
- `1.5 bitxor 1 @ 0`
- `1 bitxor 1.5 @ 0`
- `9007199254740992 bitxor 1 @ 0`
- `1 bitxor 9007199254740992 @ 0`
- `fractional_status bitxor 1 @ 0`
- `9007199254740991 bitand 4503599627370496 @ 0`
- `4503599627370496 bitor 4503599627370495 @ 0`
- `9007199254740991 bitxor 4503599627370496 @ 0`
- `-0 bitor 1 @ 0`
- `device_status bitand on(instance) group_left(model) alarm_mask @ 0`
- `alarm_mask bitor on(instance) group_right(model) device_status @ 0`
- `device_status bitxor (device_status offset 1m) @ 60000`
- `1 + 6 bitand 3 * 2 ^ 3 @ 0`
- `4 bitor 1 bitxor 3 bitand 2 @ 0`
- `device_status bitand on(instance) group_left(model) alarm_mask @ 0..60000/60000`
- `alarm_mask bitor on(instance) group_right(model) device_status @ 0..60000/60000`
- `device_status bitxor 64 @ 0..60000/60000`
- `status bitand mask @ 0..420000/60000`
- `mask bitand status @ 0..420000/60000`
- `status bitor mask @ 0..420000/60000`
- `mask bitor status @ 0..420000/60000`
- `status bitxor mask @ 0..420000/60000`
- `mask bitxor status @ 0..420000/60000`
- `status bitand 3 @ 0`
- `3 bitand status @ 0`
- `status bitand mask @ 0`
- `status bitor 3 @ 0`
- `3 bitor status @ 0`
- `status bitor mask @ 0`
- `status bitxor 3 @ 0`
- `3 bitxor status @ 0`
- `status bitxor mask @ 0`

### functions (9)

- `year(vector(-0.5)) @ 0`
- `month(vector(-0.5)) @ 0`
- `day_of_month(vector(-0.5)) @ 0`
- `day_of_week(vector(-0.5)) @ 0`
- `day_of_year(vector(-0.5)) @ 0`
- `hour(vector(-0.5)) @ 0`
- `minute(vector(-60.5)) @ 0`
- `minute(vector(-0.001)) @ 0`
- `hour(vector(-3600.001)) @ 0`

### name_label_dropping (8)

- `label_replace(rate({env="1"}[10m]), "my_name", "rate_$1", "__name__", "(.+)") @ 600000`
- `label_replace(rate({env="1"}[10m]), "__name__", "rate_$1", "__name__", "(.+)") @ 600000`
- `label_join(rate({env="1"}[10m]), "my_name", "_", "__name__") @ 600000`
- `label_join(rate({env="1"}[10m]), "__name__", "_", "__name__", "env") @ 600000`
- `sum by (__name__) (rate({env="1"}[10m])) @ 600000`
- `label_replace(sum by (__name__) (rate(metric_total{env="2"}[5m])), "__name__", "$1", "__name__", "(.+)") @ 600000`
- `sum by (__name__) (metric_total{env="1"} or rate(metric_total{env="2"}[5m])) @ 600000`
- `sum by (__name__) (rate(metric_total{env="2"}[5m]) or metric_total{env="1"}) @ 600000`

### start_timestamps (14)

- `increase(cumulative[5m]) @ 420000..900000/60000`
- `rate(cumulative[5m]) @ 420000..900000/60000`
- `irate(cumulative[5m]) @ 420000..900000/60000`
- `round(increase(cumulative[5m1ms])) @ 420000..900000/60000`
- `resets(cumulative[5m]) @ 420000..900000/60000`
- `increase(delta[5m]) @ 600000..900000/60000`
- `rate(delta[5m]) @ 600000..900000/60000`
- `irate(delta[5m]) @ 600000..900000/60000`
- `increase(series[5m]) @ 360000`
- `increase(series[5m]) @ 300000`
- `round(increase(series[1m1ms])) @ 180000`
- `rate(counter_total{type!="overlap"}[3m]) @ 240000`
- `rate(counter_total{type="overlap"}[3m]) @ 240000`
- `rate(counter_total[3m]) @ 240000`

### trig_functions (2)

- `rad(vector(1e308)) @ 60000`
- `deg(vector(1e306)) @ 60000`

### type_and_unit (21)

- `SUM(http_requests_total) WITHOUT (instance, group) @ 3000000`
- `absent(http_requests_total{__type__="counter", __unit__="request", job="nonexistent"}) @ 3000000`
- `-http_requests_total{job="api-server",instance="0",group="production"} @ 3000000`
- `http_requests_total{group="canary", instance="0", job="api-server"} / 0 @ 3000000`
- `0 * http_requests_total{group="canary", instance="0", job="api-server"} % 0 @ 3000000`
- `rate(http_requests_total[25m]) * 25 * 60 @ 3000000`
- `(http_requests_total{group="canary"} + 1) and http_requests_total{instance="0"} @ 3000000`
- `(http_requests_total{group="canary"} + 1) or http_requests_total{instance="1"} @ 3000000`
- `http_requests_total{group="canary"} / ignoring(group) http_requests_total{group="production"} @ 3000000`
- `http_requests_total{job="api-server", instance="0", group="production"} == bool 100 @ 3000000`
- `SUM(http_requests_total) WITHOUT (instance, group) @ 3000000 #2`
- `-http_requests_total{job="api-server",instance="0",group="production"} @ 3000000 #2`
- `http_requests_total{group="canary", instance="0", job="api-server"} / 0 @ 3000000 #2`
- `0 * http_requests_total{group="canary", instance="0", job="api-server"} % 0 @ 3000000 #2`
- `rate(http_requests_total[25m]) * 25 * 60 @ 3000000 #2`
- `(http_requests_total{group="canary"} + 1) and http_requests_total{instance="0"} @ 3000000 #2`
- `(http_requests_total{group="canary"} + 1) or http_requests_total{instance="1"} @ 3000000 #2`
- `http_requests_total{group="canary"} / ignoring(group) http_requests_total{group="production"} @ 3000000 #2`
- `http_requests_total{job="api-server", instance="0", group="production"} == bool 100 @ 3000000 #2`
- `kubelet_volume_stats_capacity_bytes - kubelet_volume_stats_available_bytes @ 0`
- `(kubelet_volume_stats_capacity_bytes - kubelet_volume_stats_available_bytes) / kubelet_volume_stats_capacity_bytes * 100 @ 0`

### unquoted_utf8_names (25)

- `http.server.requests{service.name="api"} @ 120000`
- `{"http.server.requests", "service.name"="api"} @ 120000`
- `http.server.requests{resource.k8s.namespace="system", service.name=~"w.*"} @ 120000`
- `{"http.server.requests", "resource.k8s.namespace"="system", "service.name"=~"w.*"} @ 120000`
- `http.server.requests{service.name="web"} @ 120000`
- `温度{場所="東京"} @ 120000`
- `{"温度", "場所"="東京"} @ 120000`
- `Björn @ 120000`
- `sum by (service.name) (http.server.requests) @ 120000`
- `sum by ("service.name") ({"http.server.requests"}) @ 120000`
- `sum without (service.name) (http.server.requests) @ 120000`
- `sum without ("service.name") ({"http.server.requests"}) @ 120000`
- `max_over_time(http.server.requests{service.name="api"}[2m]) @ 120000`
- `http.server.requests * on (service.name) group_left (équipe) service_info @ 120000`
- `{"http.server.requests"} * on ("service.name") group_left ("équipe") service_info @ 120000`
- `service_info * on (service.name) group_right (équipe) http.server.requests @ 120000`
- `http.server.requests * ignoring (resource.k8s.namespace, équipe) service_info @ 120000`
- `histogram.count @ 0`
- `target.info @ 0`
- `foo..bar. @ 0`
- `Inf.total + Inf @ 0`
- `bool.total @ 0`
- `rate.total @ 0`
- `а @ 0`
- `ö @ 0`

## By file

| file | evals | native histograms | subquery | flag-gated | passing | passing core | |
|---|---:|---:|---:|---:|---:|---:|---|
| operators | 214 | 72 | 0 | 0 | 144 (67%) | 142 of 142 (100%) | core green |
| extended_vectors | 35 | 4 | 0 | 139 | 31 (89%) | 31 of 31 (100%) | core green |
| selectors | 31 | 0 | 0 | 0 | 31 (100%) | 31 of 31 (100%) | core green |
| literals | 25 | 0 | 0 | 0 | 25 (100%) | 25 of 25 (100%) | core green |
| staleness | 17 | 0 | 0 | 0 | 17 (100%) | 17 of 17 (100%) | core green |
| range_queries | 16 | 2 | 0 | 2 | 14 (88%) | 14 of 14 (100%) | core green |
| duration_expression | 8 | 0 | 0 | 57 | 8 (100%) | 8 of 8 (100%) | core green |
| collision | 2 | 0 | 0 | 0 | 2 (100%) | 2 of 2 (100%) | core green |
| subquery | 41 | 2 | 37 | 0 | 2 (5%) | 2 of 2 (100%) | core green |
| fill-modifier | 1 | 0 | 0 | 49 | 1 (100%) | 1 of 1 (100%) | core green |
| functions | 399 | 107 | 8 | 142 | 261 (65%) | 261 of 284 (92%) |  |
| trig_functions | 21 | 0 | 0 | 0 | 19 (90%) | 19 of 21 (90%) |  |
| at_modifier | 77 | 0 | 31 | 1 | 38 (49%) | 38 of 46 (83%) |  |
| aggregators | 165 | 35 | 0 | 2 | 93 (56%) | 93 of 130 (72%) |  |
| type_and_unit | 65 | 0 | 0 | 0 | 42 (65%) | 42 of 65 (65%) |  |
| name_label_dropping | 30 | 0 | 1 | 0 | 18 (60%) | 18 of 29 (62%) |  |
| histograms | 176 | 87 | 0 | 19 | 8 (5%) | 8 of 89 (9%) |  |
| unquoted_utf8_names | 27 | 0 | 0 | 0 | 2 (7%) | 2 of 27 (7%) |  |
| bitwise | 87 | 18 | 0 | 0 | 0 (0%) | 0 of 69 (0%) |  |
| start_timestamps | 24 | 8 | 2 | 16 | 0 (0%) | 0 of 14 (0%) |  |
| native_histograms | 550 | 541 | 0 | 7 | 0 (0%) | 0 of 9 (0%) |  |
| info | 0 | 0 | 0 | 70 | 0 (0%) | 0 of 0 (0%) | no core evals |
| limit | 0 | 0 | 0 | 37 | 0 (0%) | 0 of 0 (0%) | no core evals |

---

Some failures are one cause wearing many hats — a single lexer gap can account for hundreds of rows above. Read a handful before picking, with:

```sh
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- <file>/
```
