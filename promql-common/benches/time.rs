//! Duration parsing, formatting, and millisecond-conversion benchmarks for
//! `promql_common::model`.
//!
//! Recommended CLI invocation:
//! ```sh
//! cargo bench -p promql-common --bench time -- -bench '^Benchmark' -benchtime 1s -count 6 -cpu 2 -benchmem | tee ${bench}.txt
//! ```

use std::hint::black_box;

use benchfmt::{CountingAlloc, Suite};
use promql_common::model::{secs_to_millis, Duration};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc::new();

fn main() {
    let mut suite = Suite::from_env("promql_common").with_alloc(&ALLOC);

    for (name, input) in [
        ("5m", "5m"),
        ("1s118ms", "1s118ms"),
        ("3w2d1h", "3w2d1h"),
        ("289y", "289y"),
    ] {
        Duration::parse(input).unwrap();
        suite = suite.bench(format!("DurationParse/case={name}"), move |b| {
            b.set_bytes(input.len() as u64);
            b.report_allocs();
            while b.keep_running() {
                let d = Duration::parse(black_box(input)).unwrap();
                black_box(d.as_secs_f64());
            }
        });
    }

    let invalid = "1y1m1d";
    assert!(Duration::parse(invalid).is_err());
    suite = suite.bench("DurationParse/case=invalid_order", move |b| {
        b.set_bytes(invalid.len() as u64);
        b.report_allocs();
        while b.keep_running() {
            black_box(Duration::parse(black_box(invalid)).unwrap_err());
        }
    });

    let neg_input = "-3w2d1h";
    Duration::parse_allow_negative(neg_input).unwrap();
    suite = suite.bench("DurationParseAllowNegative/case=-3w2d1h", move |b| {
        b.set_bytes(neg_input.len() as u64);
        b.report_allocs();
        while b.keep_running() {
            black_box(Duration::parse_allow_negative(black_box(neg_input)).unwrap());
        }
    });

    for (name, input) in [("1ms500us", "1ms500us"), ("1s1ns", "1s1ns")] {
        Duration::parse_nanos(input).unwrap();
        suite = suite.bench(format!("DurationParseNanos/case={name}"), move |b| {
            b.set_bytes(input.len() as u64);
            b.report_allocs();
            while b.keep_running() {
                black_box(Duration::parse_nanos(black_box(input)).unwrap());
            }
        });
    }

    for (name, dur) in [
        ("5m", Duration::parse("5m").unwrap()),
        ("23d1h", Duration::parse("23d1h").unwrap()),
        ("1s1ns", Duration::parse_nanos("1s1ns").unwrap()),
    ] {
        suite = suite.bench(format!("DurationDisplay/case={name}"), move |b| {
            b.report_allocs();
            while b.keep_running() {
                black_box(black_box(dur).to_string());
            }
        });
    }

    suite = suite
        .bench("DurationFromMillis/case=300000", |b| {
            b.report_allocs();
            while b.keep_running() {
                black_box(Duration::from_millis(black_box(300_000)).unwrap());
            }
        })
        .bench("SecsToMillis/case=1.0015", |b| {
            b.report_allocs();
            while b.keep_running() {
                black_box(secs_to_millis(black_box(1.0015)).unwrap());
            }
        });

    suite.run();
}
