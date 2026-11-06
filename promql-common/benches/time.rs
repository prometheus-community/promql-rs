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
        let mut checked = false;
        suite = suite.bench(format!("DurationParse/case={name}"), move |b| {
            if !checked {
                Duration::parse(input).unwrap();
                checked = true;
            }
            b.set_bytes(input.len() as u64);
            b.report_allocs();
            while b.keep_running() {
                black_box(Duration::parse(black_box(input)).unwrap());
            }
        });
    }

    let invalid = "1y1m1d";
    let mut invalid_checked = false;
    suite = suite.bench("DurationParse/case=invalid_order", move |b| {
        if !invalid_checked {
            assert!(Duration::parse(invalid).is_err());
            invalid_checked = true;
        }
        b.set_bytes(invalid.len() as u64);
        b.report_allocs();
        while b.keep_running() {
            black_box(Duration::parse(black_box(invalid)).unwrap_err());
        }
    });

    let neg_input = "-3w2d1h";
    let mut neg_checked = false;
    suite = suite.bench("DurationParseAllowNegative/case=-3w2d1h", move |b| {
        if !neg_checked {
            Duration::parse_allow_negative(neg_input).unwrap();
            neg_checked = true;
        }
        b.set_bytes(neg_input.len() as u64);
        b.report_allocs();
        while b.keep_running() {
            black_box(Duration::parse_allow_negative(black_box(neg_input)).unwrap());
        }
    });

    for (name, input) in [("1ms500us", "1ms500us"), ("1s1ns", "1s1ns")] {
        let mut checked = false;
        suite = suite.bench(format!("DurationParseNanos/case={name}"), move |b| {
            if !checked {
                Duration::parse_nanos(input).unwrap();
                checked = true;
            }
            b.set_bytes(input.len() as u64);
            b.report_allocs();
            while b.keep_running() {
                black_box(Duration::parse_nanos(black_box(input)).unwrap());
            }
        });
    }

    for (name, input) in [("5m", "5m"), ("23d1h", "23d1h"), ("1s1ns", "1s1ns")] {
        let mut dur_opt = None;
        suite = suite.bench(format!("DurationDisplay/case={name}"), move |b| {
            let dur = *dur_opt.get_or_insert_with(|| Duration::parse_nanos(input).unwrap());
            b.report_allocs();
            while b.keep_running() {
                black_box(black_box(dur).to_string());
            }
        });
    }

    let mut split_dur_opt = None;
    suite = suite
        .bench("DurationAsSecsF64/case=1s118ms", move |b| {
            let dur = *split_dur_opt.get_or_insert_with(|| Duration::parse("1s118ms").unwrap());
            b.report_allocs();
            while b.keep_running() {
                black_box(black_box(dur).as_secs_f64());
            }
        })
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
