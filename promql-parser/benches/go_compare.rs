//! Direct `benchstat` counterpart to `BenchmarkParser` in upstream Prometheus
//! (`promql/bench_test.go` at `83962c35a4ab0c9988bc469aa8165014fc065d34`).
//!
//! Benchmark names and the default `pkg` header (`github.com/prometheus/prometheus/promql`)
//! match Go's `testing.B` output so `benchstat go_parser.txt rust_parser.txt`
//! pairs every expression directly without renaming.
//!
//! NOTE: Two upstream differences apply under default `ParserOptions` (`parser.Options{}`):
//! 1. `sort_by_label(metric, "foo", "bar")` is gated by `enable_experimental_functions`,
//!    so both Go's `testParser.ParseExpr` and `promql_parser::parse_expr` return a
//!    gate error and discard it.
//! 2. Go's `ParseExpr` runs post-parse semantic validation (`checkAST`), whereas
//!    `promql-rs` performs type and vector-matching checks during `promql-engine`
//!    planning; only syntactic errors fail inside `promql_parser::parse_expr`.
//!
//! Recommended CLI invocation:
//! ```sh
//! cargo bench -p promql-parser --bench go_compare -- -bench '^BenchmarkParser/' -benchtime 500ms -count 6 -cpu 2 -benchmem | tee ${bench}.txt
//! ```

use std::hint::black_box;

use benchfmt::{CountingAlloc, Suite};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc::new();

const CASES: [&str; 14] = [
    "a",
    "metric",
    "1",
    "1 >= bool 1",
    "1 + 2/(3*1)",
    "foo or bar",
    "foo and bar unless baz or qux",
    "bar + on(foo) bla / on(baz, buz) group_right(test) blub",
    "foo / ignoring(test,blub) group_left(blub) bar",
    "foo - ignoring(test,blub) group_right(bar,foo) bar",
    r#"foo{a="b", foo!="bar", test=~"test", bar!~"baz"}"#,
    r#"min_over_time(rate(foo{bar="baz"}[2s])[5m:])[4m:3s]"#,
    "sum without(and, by, avg, count, alert, annotations)(some_metric) [30m:10s]",
    r#"sort_by_label(metric, "foo", "bar")"#,
];

const ERR_CASES: [&str; 6] = [
    "(",
    "}",
    "1 or 1",
    "1 or on(bar) foo",
    "foo unless on(bar) group_left(baz) bar",
    "test[5d] OFFSET 10s [10m:5s]",
];

fn main() {
    let mut suite = Suite::from_env("github.com/prometheus/prometheus/promql").with_alloc(&ALLOC);

    for expr in CASES {
        suite = suite.bench(format!("Parser/{expr}"), move |b| {
            b.report_allocs();
            while b.keep_running() {
                let _ = black_box(promql_parser::parse_expr(black_box(expr)));
            }
        });
    }

    for expr in ERR_CASES {
        suite = suite.bench(format!("Parser/{expr} (should fail)"), move |b| {
            b.report_allocs();
            while b.keep_running() {
                let _ = black_box(promql_parser::parse_expr(black_box(expr)));
            }
        });
    }

    suite.run();
}
