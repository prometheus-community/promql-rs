//! Series-description parsing (`parse_series_desc`), the promqltest
//! load-block line format.
//!
//! Expected values were cross-checked against Go's
//! `parser.ParseSeriesDesc` over the full
//! `promql/promqltest/testdata` corpus: of 1015 distinct series lines,
//! all 787 non-histogram lines produce identical labels and values.
//! The 228 native-histogram lines were not part of that cross-check;
//! their coverage here is hand-written against `buildHistogramFromMap`
//! and `histogramsSeries` in upstream `parse.go`.

use promql_histogram::{CounterResetHint, Span};
use promql_parser::{parse_series_desc, SeriesDescription};

fn parse(input: &str) -> SeriesDescription {
    parse_series_desc(input).expect("parses")
}

/// Label name/value pairs, in source order.
fn labels(sd: &SeriesDescription) -> Vec<(&str, &str)> {
    sd.labels
        .iter()
        .map(|m| (m.name.as_str(), m.value.as_str()))
        .collect()
}

/// The value sequence with the omitted flag dropped. Only meaningful
/// for inputs without `_` gaps.
fn floats(sd: &SeriesDescription) -> Vec<f64> {
    sd.values.iter().map(|v| v.value).collect()
}

#[test]
fn arithmetic_run() {
    let sd = parse(r#"http_requests_total{pod="envoy-1", route="/"} 46.00+13.00x40"#);

    assert_eq!(
        labels(&sd),
        vec![
            ("__name__", "http_requests_total"),
            ("pod", "envoy-1"),
            ("route", "/"),
        ]
    );

    // Upstream's loop is `i <= count`, adding "an additional value for
    // time 0, which we ignore in tests" — so x40 is 41 points.
    let vals = floats(&sd);
    assert_eq!(vals.len(), 41);
    assert_eq!(vals[0], 46.0);
    assert_eq!(vals[1], 59.0);
    assert_eq!(vals[40], 566.0);
}

#[test]
fn fractional_step() {
    let vals = floats(&parse(
        r#"http_requests_total{pod="envoy-2", route="/"}  2+5.25x40"#,
    ));
    assert_eq!(vals.len(), 41);
    assert_eq!(vals[0], 2.0);
    assert_eq!(vals[1], 7.25);
    assert_eq!(vals[40], 212.0);
}

#[test]
fn repeat_without_step() {
    assert_eq!(floats(&parse("metric{} 1x2")), vec![1.0, 1.0, 1.0]);
}

#[test]
fn plain_sequence() {
    assert_eq!(floats(&parse("metric 1 2 3")), vec![1.0, 2.0, 3.0]);
}

#[test]
fn negative_step() {
    assert_eq!(floats(&parse("metric 10-2x3")), vec![10.0, 8.0, 6.0, 4.0]);
}

#[test]
fn blank_is_a_gap() {
    let vals = parse("metric 1 _ 3").values;
    assert_eq!(vals.len(), 3);
    assert!(!vals[0].omitted);
    assert!(vals[1].omitted);
    assert!(!vals[2].omitted);
}

#[test]
fn blank_run_has_no_extra_point() {
    // `BLANK TIMES uint` loops `i < count` upstream, unlike the value
    // forms which loop `i <= count`. `_x3` is exactly 3 gaps.
    let vals = parse("metric _x3 1").values;
    assert_eq!(vals.len(), 4);
    assert!(vals[..3].iter().all(|v| v.omitted));
    assert!(!vals[3].omitted);
}

#[test]
fn stale_marker() {
    let vals = parse("metric 1 stale").values;
    assert_eq!(vals.len(), 2);
    assert!(vals[1].value.is_nan());
    // Upstream uses value.StaleNaN, a NaN with a distinguishing low bit.
    assert_eq!(vals[1].value.to_bits(), 0x7ff0_0000_0000_0002);
}

#[test]
fn bare_metric_without_label_set() {
    let sd = parse("metric 1");
    assert_eq!(labels(&sd), vec![("__name__", "metric")]);
    assert_eq!(floats(&sd), vec![1.0]);
}

#[test]
fn empty_value_label_is_dropped() {
    // `metric : metric_identifier label_set` runs through a
    // labels.Builder upstream, and Builder.Reset treats an empty value
    // as a deletion.
    let sd = parse(r#"metric{__unit__="", job="app"} 1"#);
    assert_eq!(labels(&sd), vec![("__name__", "metric"), ("job", "app")]);
}

#[test]
fn tabs_separate_values() {
    let sd = parse("metric{job=\"api\"}\t\t1 2");
    assert_eq!(labels(&sd), vec![("__name__", "metric"), ("job", "api")]);
    assert_eq!(floats(&sd), vec![1.0, 2.0]);
}

#[test]
fn trailing_space_is_allowed() {
    assert_eq!(floats(&parse("metric 1 2 ")), vec![1.0, 2.0]);
}

#[test]
fn metric_with_no_values() {
    let sd = parse("metric");
    assert_eq!(labels(&sd), vec![("__name__", "metric")]);
    assert!(sd.values.is_empty());
}

#[test]
fn special_floats() {
    let vals = floats(&parse("metric NaN Inf -Inf"));
    assert!(vals[0].is_nan());
    assert_eq!(vals[1], f64::INFINITY);
    assert_eq!(vals[2], f64::NEG_INFINITY);
}

#[test]
fn native_histogram_descriptor() {
    let sd = parse(
        "h {{schema:1 sum:-3.5 count:6 z_bucket:1 z_bucket_w:0.001 buckets:[1 2] offset:-2 \
         n_buckets:[3] n_offset:4 counter_reset_hint:gauge}}",
    );
    assert_eq!(sd.values.len(), 1);
    let v = &sd.values[0];
    assert!(v.counter_reset_hint_set && !v.omitted);
    let h = v.histogram.as_ref().expect("a histogram");
    assert_eq!(h.counter_reset_hint, CounterResetHint::Gauge);
    assert_eq!((h.schema, h.sum, h.count), (1, -3.5, 6.0));
    assert_eq!((h.zero_count, h.zero_threshold), (1.0, 0.001));
    assert_eq!(h.positive_buckets, [1.0, 2.0]);
    assert_eq!(
        h.positive_spans,
        [Span {
            offset: -2,
            length: 2
        }]
    );
    assert_eq!(h.negative_buckets, [3.0]);
    assert_eq!(
        h.negative_spans,
        [Span {
            offset: 4,
            length: 1
        }]
    );
}

#[test]
fn native_histogram_runs_and_mixed_sequences() {
    // `x2` is three points, as for floats; an unspelled hint is "don't care".
    let sd = parse("h {{schema:0 sum:1 count:1}}x2 _ 5 stale {{}} {{ }}");
    assert_eq!(sd.values.len(), 8);
    assert!(sd.values[..3]
        .iter()
        .all(|v| v.histogram.is_some() && !v.counter_reset_hint_set));
    assert!(sd.values[3].omitted);
    assert_eq!(sd.values[4].value, 5.0);
    assert!(sd.values[4].histogram.is_none() && sd.values[5].histogram.is_none());
    assert_eq!(
        sd.values[6].histogram,
        Some(promql_histogram::FloatHistogram::default())
    );
    assert_eq!(sd.values[6], sd.values[7]);
}

#[test]
fn native_histogram_arithmetic_series() {
    let sd = parse(
        "h {{schema:0 sum:1 count:2 buckets:[1 1]}}+{{schema:0 sum:2 count:1 buckets:[0 1]}}x2",
    );
    let sums: Vec<_> = sd
        .values
        .iter()
        .map(|v| {
            let h = v.histogram.as_ref().unwrap();
            (h.sum, h.count, h.positive_buckets.clone())
        })
        .collect();
    assert_eq!(
        sums,
        [
            (1.0, 2.0, vec![1.0, 1.0]),
            (3.0, 3.0, vec![1.0, 2.0]),
            (5.0, 4.0, vec![1.0, 3.0]),
        ]
    );

    let sd = parse("h {{sum:5 count:5 buckets:[5]}}-{{sum:1 count:1 buckets:[1]}}x1");
    let last = sd.values[1].histogram.as_ref().unwrap();
    assert_eq!((last.sum, last.count), (4.0, 4.0));
    assert_eq!(last.positive_buckets, [4.0]);
}

#[test]
fn native_histogram_custom_buckets_and_special_numbers() {
    let sd = parse("h {{schema:-53 sum:Inf count:NaN custom_values:[5 10] buckets:[1 4 0]}}");
    let h = sd.values[0].histogram.as_ref().unwrap();
    assert_eq!(h.schema, -53);
    assert!(h.sum.is_infinite() && h.count.is_nan());
    assert_eq!(h.custom_values[..2], [5.0, 10.0]);
}

#[test]
fn rejects_malformed_native_histograms() {
    for bad in [
        "h {{sum:1 sum:2}}",
        "h {{bogus:1}}",
        "h {{schema:1.5}}",
        "h {{sum:1",
        // Upstream `histogramsSeries`: the increment cannot be coarser.
        "h {{schema:1}}+{{schema:0}}x2",
    ] {
        assert!(parse_series_desc(bad).is_err(), "{bad}");
    }
}

#[test]
fn rejects_non_stale_identifier() {
    assert!(parse_series_desc("metric 1 bogus").is_err());
}

#[test]
fn expression_parsing_is_unaffected() {
    // The series lexer must not leak into the expression entry point:
    // `x` and `_` are ordinary identifier characters there, and space
    // is insignificant.
    assert!(promql_parser::parse_expr("x + 1").is_ok());
    assert!(promql_parser::parse_expr("x_total{a=\"b\"} offset 5m").is_ok());
    assert!(promql_parser::parse_expr("sum by (x) (rate(foo[5m]))").is_ok());
    // Conversely, an expression is not a series description.
    assert!(parse_series_desc("rate(foo[5m])").is_err());
}

// -------- repetition counts --------

#[test]
fn rejects_repeat_count_at_u64_max() {
    // `u64::MAX` is the largest value that still parses as the count, so
    // it is what reaches the expansion: one more digit and `parse::<u64>`
    // fails on its own. Unbounded, `series_repeat` computed
    // `count as usize + 1` and panicked with "attempt to add with
    // overflow" in debug, or wrapped to a zero-capacity Vec and looped
    // 2^64 times in release. It must be an ordinary parse error.
    assert!(parse_series_desc("metric 1x18446744073709551615").is_err());
    assert!(parse_series_desc("metric _x18446744073709551615").is_err());
}

#[test]
fn rejects_repeat_count_above_limit() {
    assert!(parse_series_desc("metric 1x1000001").is_err());
    assert!(parse_series_desc("metric _x1000001").is_err());
}

#[test]
fn accepts_repeat_count_at_limit() {
    // The bound is inclusive, and `1x<count>` yields `count + 1` points.
    let sd = parse("metric 1x1000000");
    assert_eq!(sd.values.len(), 1_000_001);
}

#[test]
fn accepts_chained_runs_summing_to_the_description_limit() {
    // Neither run is near MAX_REPEAT_COUNT, so this exercises the
    // accumulated bound. `1x<count>` emits `count + 1` points, so the
    // pair lands exactly on the inclusive per-description limit.
    let sd = parse("metric 1x500000 1x499999");
    assert_eq!(sd.values.len(), 1_000_001);
}

#[test]
fn rejects_chained_runs_above_the_description_limit() {
    // One point more than the test above.
    assert!(parse_series_desc("metric 1x500000 1x500000").is_err());
}

#[test]
fn rejects_repeat_count_wider_than_u64() {
    // Already an error before the bound existed — `parse::<u64>` refuses
    // it — but worth pinning so the two rejection paths stay distinct.
    assert!(parse_series_desc("metric 1x18446744073709551616").is_err());
}

// -------- parse_metric --------

#[test]
fn parses_bare_label_set() {
    let matchers = promql_parser::parse_metric(r#"{foo="bar", baz="qux"}"#).expect("parses");
    let pairs: Vec<(&str, &str)> = matchers
        .iter()
        .map(|m| (m.name.as_str(), m.value.as_str()))
        .collect();
    assert_eq!(pairs, vec![("foo", "bar"), ("baz", "qux")]);
}

#[test]
fn parses_metric_name_with_labels() {
    let matchers =
        promql_parser::parse_metric(r#"http_requests_total{job="api"}"#).expect("parses");
    let pairs: Vec<(&str, &str)> = matchers
        .iter()
        .map(|m| (m.name.as_str(), m.value.as_str()))
        .collect();
    assert_eq!(
        pairs,
        vec![("__name__", "http_requests_total"), ("job", "api")]
    );
}

#[test]
fn parse_metric_rejects_a_value_sequence() {
    // A metric is the label set alone; the trailing values belong to
    // `parse_series_desc`.
    assert!(promql_parser::parse_metric("metric 1 2 3").is_err());
}
