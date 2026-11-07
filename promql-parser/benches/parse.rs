//! Parser throughput and allocation benchmarks across `parse_expr`,
//! experimental `ParserOptions`, error recovery, `parse_metric_selector`,
//! `parse_metric`, `parse_series_desc`, and AST `Display` formatting.
//!
//! Recommended CLI invocation:
//! ```sh
//! cargo bench -p promql-parser --bench parse -- -bench '^Benchmark' -benchtime 1s -count 6 -cpu 2 -benchmem | tee ${bench}.txt
//! ```

use std::hint::black_box;

use benchfmt::{CountingAlloc, Suite};
use promql_parser::ast::Expr;
use promql_parser::{Parser, ParserOptions};

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

const EXPERIMENTAL_QUERIES: [(&str, &str); 4] = [
    (
        "fill_modifier",
        "http_requests_total / on(route) group_left() fill(0) sum by (route) (http_requests_total)",
    ),
    (
        "extended_range_smoothed",
        "rate(http_requests_total[5m] smoothed)",
    ),
    (
        "limitk_mad_anchored",
        "limitk(1, mad_over_time(http_requests_total[5m] anchored))",
    ),
    (
        "sort_by_label",
        r#"sort_by_label(http_requests_total, "job", "instance")"#,
    ),
];

const ERROR_QUERIES: [(&str, &str); 3] = [
    (
        "unclosed_paren",
        "sum by (job) (rate(http_requests_total[5m])",
    ),
    ("unclosed_braces", r#"http_requests_total{job="api""#),
    (
        "gated_experimental",
        "mad_over_time(http_requests_total[5m])",
    ),
];

fn main() {
    let mut suite = Suite::from_env("promql_parser").with_alloc(&ALLOC);

    for (name, q) in QUERIES {
        let mut checked = false;
        suite = suite.bench(format!("ParseExpr/case={name}"), move |b| {
            if !checked {
                promql_parser::parse_expr(q).unwrap_or_else(|e| panic!("{name}: {e:?}"));
                checked = true;
            }
            b.set_bytes(q.len() as u64);
            b.report_allocs();
            while b.keep_running() {
                black_box(promql_parser::parse_expr(black_box(q)).unwrap());
            }
        });
    }

    for (name, q) in EXPERIMENTAL_QUERIES {
        let parser = Parser::new(ParserOptions::all());
        let mut checked = false;
        suite = suite.bench(format!("ParseExprExperimental/case={name}"), move |b| {
            if !checked {
                assert!(
                    promql_parser::parse_expr(q).is_err(),
                    "{name} should require experimental options"
                );
                parser
                    .parse_expr(q)
                    .unwrap_or_else(|e| panic!("{name}: {e:?}"));
                checked = true;
            }
            b.set_bytes(q.len() as u64);
            b.report_allocs();
            while b.keep_running() {
                black_box(parser.parse_expr(black_box(q)).unwrap());
            }
        });
    }

    for (name, q) in ERROR_QUERIES {
        let mut checked = false;
        suite = suite.bench(format!("ParseExprError/case={name}"), move |b| {
            if !checked {
                assert!(promql_parser::parse_expr(q).is_err(), "{name} should fail");
                checked = true;
            }
            b.set_bytes(q.len() as u64);
            b.report_allocs();
            while b.keep_running() {
                black_box(promql_parser::parse_expr(black_box(q)).unwrap_err());
            }
        });
    }

    for (name, sel) in [
        ("bare", "http_requests_total"),
        (
            "four_matchers",
            r#"http_requests_total{job="api", instance=~"10\\.0\\..*:9090", code!="500", method=~"GET|POST"}"#,
        ),
    ] {
        let mut checked = false;
        suite = suite.bench(format!("ParseMetricSelector/case={name}"), move |b| {
            if !checked {
                promql_parser::parse_metric_selector(sel).unwrap();
                checked = true;
            }
            b.set_bytes(sel.len() as u64);
            b.report_allocs();
            while b.keep_running() {
                black_box(promql_parser::parse_metric_selector(black_box(sel)).unwrap());
            }
        });
    }

    let metric_text = r#"http_requests_total{job="api", instance="10.0.0.1:9090", route="/v1/query", code="200"}"#;
    let mut metric_checked = false;
    suite = suite.bench("ParseMetric/case=five_labels", move |b| {
        if !metric_checked {
            promql_parser::parse_metric(metric_text).unwrap();
            metric_checked = true;
        }
        b.set_bytes(metric_text.len() as u64);
        b.report_allocs();
        while b.keep_running() {
            black_box(promql_parser::parse_metric(black_box(metric_text)).unwrap());
        }
    });

    // `load` lines expanding to ~1000 samples, covering both uniform counter
    // increments and sparse/reset/stale sequences from the conformance corpus.
    for (name, line) in [
        (
            "counter_1000",
            r#"http_requests_total{pod="nginx-1", route="/api", code="200"} 0+10x1000"#,
        ),
        (
            "sparse_resets_stale",
            r#"http_requests_total{pod="nginx-1", route="/api", code="200"} 0+10x250 _x50 100-2x250 stale 5+5x400"#,
        ),
    ] {
        let mut checked = false;
        suite = suite.bench(format!("ParseSeriesDesc/case={name}"), move |b| {
            if !checked {
                promql_parser::parse_series_desc(line).unwrap();
                checked = true;
            }
            b.set_bytes(line.len() as u64);
            b.report_allocs();
            while b.keep_running() {
                black_box(promql_parser::parse_series_desc(black_box(line)).unwrap());
            }
        });
    }

    let mut matcher_opt = None;
    suite = suite.bench("AstDisplay/case=label_matcher_escaped", move |b| {
        let matcher = matcher_opt.get_or_insert_with(|| {
            let Expr::VectorSelector(vs) =
                promql_parser::parse_expr(QUERIES[1].1).expect("valid selector")
            else {
                panic!("expected vector selector");
            };
            vs.label_matchers[1].clone()
        });
        b.report_allocs();
        while b.keep_running() {
            black_box(black_box(&*matcher).to_string());
        }
    });

    suite.run();
}
