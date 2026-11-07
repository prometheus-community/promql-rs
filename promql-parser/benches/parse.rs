//! Parser throughput and allocation benchmarks: `parse_expr` over representative
//! queries and `parse_series_desc` over a promqltest `load` line. Throughput is
//! reported in decimal `MB/s` of input text.
//!
//! Recommended CLI invocation:
//! ```sh
//! cargo bench -p promql-parser --bench parse -- -bench '^BenchmarkParse' -benchtime 1s -count 6 -cpu 2 -benchmem | tee ${bench}.txt
//! ```

use std::hint::black_box;

use benchfmt::{CountingAlloc, Suite};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc::new();

const QUERIES: [(&str, &str); 10] = [
    ("selector", "http_requests_total"),
    (
        "matchers",
        r#"http_requests_total{job="api", instance=~"10\\.0\\..*:9090", code!="500", method=~"GET|POST"}"#,
    ),
    ("sum_by", "sum by (job, instance) (http_requests_total)"),
    ("rate", "rate(http_requests_total[5m])"),
    (
        "nested",
        "max(sum by (route) (increase(http_requests_total[5m])))",
    ),
    (
        "ratio",
        r#"sum(rate(http_requests_total{code=~"5.."}[5m])) / sum(rate(http_requests_total[5m])) > 0.01"#,
    ),
    (
        "subquery",
        "max_over_time(rate(http_requests_total[5m])[1h:1m])",
    ),
    ("offset_at", "http_requests_total offset 5m @ 1609459200"),
    (
        "histogram_quantile",
        "histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))",
    ),
    (
        "arithmetic",
        "(a + b) * (c - d) / (e + f) and (g or h) unless i",
    ),
];

fn main() {
    let mut suite = Suite::from_env("promql_parser").with_alloc(&ALLOC);

    for (name, q) in QUERIES {
        if let Err(e) = promql_parser::parse_expr(q) {
            panic!("{name}: {e:?}");
        }
        suite = suite.bench(format!("ParseExpr/case={name}"), move |b| {
            b.set_bytes(q.len() as u64);
            b.report_allocs();
            while b.keep_running() {
                black_box(promql_parser::parse_expr(black_box(q)).unwrap());
            }
        });
    }

    // A `load` line whose values expand to a thousand samples, the cost of
    // seeding one series in the conformance suite.
    let line = r#"http_requests_total{pod="nginx-1", route="/api", code="200"} 0+10x1000"#;
    promql_parser::parse_series_desc(line).unwrap();
    suite = suite.bench("ParseSeriesDesc/case=counter_1000", move |b| {
        b.set_bytes(line.len() as u64);
        b.report_allocs();
        while b.keep_running() {
            black_box(promql_parser::parse_series_desc(black_box(line)).unwrap());
        }
    });

    suite.run();
}
