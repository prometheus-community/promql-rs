//! Native-histogram kernels with 256 buckets per side or 256 custom bounds.
//!
//! Recommended CLI invocation:
//! ```sh
//! cargo bench -p promql-histogram --bench histogram -- -bench '^BenchmarkHistogram' -benchtime 1s -count 6 -cpu 2 -benchmem | tee ${bench}.txt
//! ```

use std::hint::black_box;

use benchfmt::{CountingAlloc, Suite};
use promql_histogram::statistics::{fraction, quantile};
use promql_histogram::{FloatHistogram, Span, TrimDirection, CUSTOM_SCHEMA};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc::new();

const SIDE_BUCKETS: usize = 256;
const CUSTOM_BOUNDS: usize = 256;

fn bucket_counts(length: usize, seed: usize) -> Vec<f64> {
    (0..length)
        .map(|index| ((index * 17 + seed) % 23 + 1) as f64)
        .collect()
}

fn exponential_spans(offset: i32) -> Vec<Span> {
    (0..4)
        .map(|span| Span {
            offset: if span == 0 { offset } else { 8 },
            length: (SIDE_BUCKETS / 4) as u32,
        })
        .collect()
}

fn exponential_fixture(schema: i32, seed: usize, zero_threshold: f64) -> FloatHistogram {
    let negative_buckets = bucket_counts(SIDE_BUCKETS, seed);
    let positive_buckets = bucket_counts(SIDE_BUCKETS, seed + 7);
    let zero_count = 11.0;
    let count =
        negative_buckets.iter().sum::<f64>() + zero_count + positive_buckets.iter().sum::<f64>();
    FloatHistogram {
        schema,
        count,
        sum: count * 1.5,
        zero_threshold,
        zero_count,
        negative_spans: exponential_spans(-128),
        negative_buckets,
        positive_spans: exponential_spans(-128),
        positive_buckets,
        ..FloatHistogram::default()
    }
}

fn custom_fixture(step: f64, seed: usize) -> FloatHistogram {
    let custom_values = (0..CUSTOM_BOUNDS)
        .map(|index| -64.0 + index as f64 * step)
        .collect::<Vec<_>>();
    let positive_buckets = bucket_counts(custom_values.len() + 1, seed);
    let count = positive_buckets.iter().sum();
    FloatHistogram {
        schema: CUSTOM_SCHEMA,
        count,
        sum: count * 2.0,
        positive_spans: vec![Span {
            offset: 0,
            length: positive_buckets.len() as u32,
        }],
        positive_buckets,
        custom_values,
        ..FloatHistogram::default()
    }
}

fn main() {
    let mut suite = Suite::from_env("promql_histogram").with_alloc(&ALLOC);
    for shape in ["exponential", "custom"] {
        for operation in [
            "validate", "traverse", "quantile", "fraction", "trim", "add", "reset",
        ] {
            let mut fixtures = None;
            suite = suite.bench(
                format!("Histogram/op={operation}/shape={shape}/buckets=256"),
                move |b| {
                    let (input, rhs) = fixtures.get_or_insert_with(|| {
                        let pair = if shape == "exponential" {
                            (
                                exponential_fixture(4, 3, 0.0001),
                                exponential_fixture(3, 11, 0.01),
                            )
                        } else {
                            (custom_fixture(0.5, 5), custom_fixture(1.0, 13))
                        };
                        pair.0.validate_ingress().unwrap();
                        pair.1.validate_ingress().unwrap();
                        pair
                    });
                    b.report_allocs();
                    while b.keep_running() {
                        let input = black_box(&*input);
                        match operation {
                            "validate" => {
                                black_box(input.validate_ingress()).unwrap();
                            }
                            "traverse" => {
                                black_box(
                                    input
                                        .all_buckets()
                                        .fold(0.0, |sum, bucket| sum + bucket.count),
                                );
                            }
                            "quantile" => {
                                black_box(quantile(black_box(0.73), input));
                            }
                            "fraction" => {
                                black_box(fraction(black_box(-3.0), black_box(20.0), input));
                            }
                            // Include the owned working copy: repeated mutation of the same
                            // histogram would eventually benchmark an empty or overflowed input.
                            "trim" => {
                                let output =
                                    input.trim_buckets(black_box(3.0), TrimDirection::Upper);
                                black_box(&output);
                            }
                            "add" => {
                                let mut output = input.clone();
                                output.add(black_box(rhs)).unwrap();
                                black_box(output);
                            }
                            "reset" => {
                                black_box(input.detect_reset(black_box(rhs)));
                            }
                            _ => unreachable!(),
                        }
                    }
                },
            );
        }
    }
    suite.run();
}
