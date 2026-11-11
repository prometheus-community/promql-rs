//! Native histograms end to end: parse, plan, execute, decode over the
//! in-memory source.
//!
//! The conformance suite holds the histogram arithmetic to Prometheus.
//! These hold the engine to what that suite cannot see: that a float
//! query carries nothing for histograms, that a series may change type,
//! that the two sample lists are one series in time order, and that the
//! answer does not depend on how the store cuts chunks, batches, blocks
//! and partitions.

use std::num::{NonZeroU64, NonZeroUsize};

use promql_engine::histogram::HISTOGRAMS;
use promql_engine::series::{coalesce, decode};
use promql_engine::{
    explain_plan, Engine, EngineError, MemorySeriesSource, RangeQuery, Series, SeriesIdEncoding,
};
use promql_histogram::{CounterResetHint, FloatHistogram, Span};
use promql_parser::SeriesDescription;

const M: i64 = 60_000;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

fn source(lines: &[&str]) -> MemorySeriesSource {
    MemorySeriesSource::from_descriptions(&load(lines), 60.0)
}

/// A histogram counter and its count as a float counter, a series that
/// changes type twice, and two gauges to add: every shape the queries
/// below read.
fn fixture() -> Vec<&'static str> {
    vec![
        r#"h{job="a"} {{schema:0 sum:2 count:1 buckets:[1]}}+{{schema:0 sum:2 count:1 buckets:[1]}}x20"#,
        r#"h{job="b"} {{schema:0 sum:9 count:3 buckets:[1 2]}}+{{schema:0 sum:3 count:2 buckets:[1 1]}}x20"#,
        r#"count{job="a"} 1+1x20"#,
        r#"count{job="b"} 3+2x20"#,
        r#"mixed 1 2 {{schema:0 sum:3 count:3 buckets:[3]}} {{schema:0 sum:4 count:4 buckets:[4]}} 5 6 _ {{schema:0 sum:8 count:8 buckets:[8]}}x13"#,
        r#"g{job="a"} {{schema:1 sum:5 count:4 buckets:[1 3] counter_reset_hint:gauge}}x20"#,
        r#"g{job="b"} 7x20"#,
    ]
}

fn run(source: &MemorySeriesSource, query: &str, range: &RangeQuery) -> Vec<Series> {
    try_run(source, query, range).unwrap_or_else(|e| panic!("{query}: {e}"))
}

fn try_run(
    source: &MemorySeriesSource,
    query: &str,
    range: &RangeQuery,
) -> Result<Vec<Series>, EngineError> {
    let engine = Engine::blocking().unwrap();
    let batches = engine.range_query(source, query, range)?;
    Ok(coalesce(decode(&batches).unwrap()).unwrap())
}

fn labels(s: &Series) -> Vec<(String, String)> {
    s.labels()
        .map(|(n, v)| (n.to_string(), v.to_string()))
        .collect()
}

fn floats(s: &Series) -> Vec<(i64, f64)> {
    s.timestamps()
        .iter()
        .copied()
        .zip(s.values().iter().copied())
        .collect()
}

fn counts(s: &Series) -> Vec<(i64, f64)> {
    s.histograms().iter().map(|(t, h)| (*t, h.count)).collect()
}

#[test]
fn a_selector_answers_a_histogram_series_with_histograms() {
    let source = source(&fixture());
    let range = RangeQuery::new(0, 4 * M, M);
    let out = run(&source, r#"h{job="a"}"#, &range);
    assert_eq!(out.len(), 1);
    assert!(floats(&out[0]).is_empty());
    assert_eq!(
        counts(&out[0]),
        (0..5).map(|i| (i * M, (i + 1) as f64)).collect::<Vec<_>>()
    );
    let (_, last) = &out[0].histograms()[4];
    assert_eq!((last.sum, last.schema), (10.0, 0));
    assert_eq!(last.positive_buckets, [5.0]);
    assert_eq!(
        last.positive_spans,
        [Span {
            offset: 0,
            length: 1
        }]
    );
}

#[test]
fn a_float_query_carries_no_histograms_column() {
    let source = source(&fixture());
    let engine = Engine::blocking().unwrap();
    let range = RangeQuery::new(0, 4 * M, M);
    for query in [
        "count",
        "sum by (job) (rate(count[2m]))",
        "count + count",
        // The functions that answer a histogram with a float, too.
        "histogram_count(h)",
        "count_over_time(h[2m])",
        "abs(h)",
    ] {
        let batches = engine.range_query(&source, query, &range).unwrap();
        assert!(!batches.is_empty(), "{query}");
        for batch in &batches {
            assert!(
                batch.column_by_name(HISTOGRAMS).is_none(),
                "{query}: {:?}",
                batch.schema()
            );
        }
    }
    // A selection of only floats plans as it did before histograms existed.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let plan = rt
        .block_on(Engine::new().plan_async(&source, "sum by (job) (rate(count[2m]))", &range))
        .unwrap();
    let rendered = explain_plan(&plan);
    assert!(!rendered.contains("histogram"), "{rendered}");
    // And a histogram query's batches do carry the column.
    let batches = engine.range_query(&source, "h", &range).unwrap();
    assert!(batches
        .iter()
        .all(|b| b.column_by_name(HISTOGRAMS).is_some()));
}

#[test]
fn a_series_changes_type_and_the_selector_follows_it() {
    let source = source(&fixture());
    // Half-minute steps: each sample answers its own step and the next.
    let out = run(&source, "mixed", &RangeQuery::new(0, 5 * M, M / 2));
    assert_eq!(out.len(), 1);
    assert_eq!(
        floats(&out[0]),
        vec![
            (0, 1.0),
            (M / 2, 1.0),
            (M, 2.0),
            (3 * M / 2, 2.0),
            (4 * M, 5.0),
            (9 * M / 2, 5.0),
            (5 * M, 6.0),
        ]
    );
    // The latest sample at 2m30s is the histogram at 2m, not the float
    // before it: one series in time order, not two lists looked up apart.
    assert_eq!(
        counts(&out[0]),
        vec![
            (2 * M, 3.0),
            (5 * M / 2, 3.0),
            (3 * M, 4.0),
            (7 * M / 2, 4.0)
        ]
    );
}

#[test]
fn a_stale_histogram_ends_the_series_like_a_stale_float() {
    let h = |count: f64, sum: f64| FloatHistogram {
        count,
        sum,
        ..FloatHistogram::default()
    };
    let stale = f64::from_bits(0x7ff0_0000_0000_0002);
    let series = Series::new(&[("__name__", "s")], vec![], vec![])
        .unwrap()
        .with_histograms(vec![
            (0, h(1.0, 1.0)),
            (M, h(0.0, stale)),
            (3 * M, h(3.0, 3.0)),
        ])
        .unwrap();
    let source = MemorySeriesSource::try_new(vec![series]).unwrap();
    let out = run(&source, "s", &RangeQuery::new(0, 4 * M, M));
    // 1m is the marker itself and 2m looks back onto it.
    assert_eq!(counts(&out[0]), vec![(0, 1.0), (3 * M, 3.0), (4 * M, 3.0)]);
    // A range function never sees the marker.
    let out = run(
        &source,
        "count_over_time(s[5m])",
        &RangeQuery::new(4 * M, 4 * M, M),
    );
    assert_eq!(floats(&out[0]), vec![(4 * M, 2.0)]);
}

#[test]
fn a_timestamp_holding_both_a_float_and_a_histogram_is_refused() {
    let err = Series::new(&[("__name__", "s")], vec![0, M], vec![1.0, 2.0])
        .unwrap()
        .with_histograms(vec![(M, FloatHistogram::default())])
        .unwrap_err();
    assert!(
        err.to_string().contains("both a float and a histogram"),
        "{err}"
    );
}

#[test]
fn a_histogram_counter_rates_as_its_count_does() {
    let source = source(&fixture());
    let range = RangeQuery::new(0, 20 * M, M);
    for func in ["rate", "increase", "delta", "irate", "idelta"] {
        let hist = run(&source, &format!("histogram_count({func}(h[5m]))"), &range);
        let float = run(&source, &format!("{func}(count[5m])"), &range);
        assert_eq!(hist.len(), 2, "{func}");
        for (h, f) in hist.iter().zip(&float) {
            assert_eq!(labels(h), labels(f), "{func}");
            let (h, f) = (floats(h), floats(f));
            assert_eq!(h.len(), f.len(), "{func}");
            for ((ht, hv), (ft, fv)) in h.iter().zip(&f) {
                assert_eq!(ht, ft, "{func}");
                assert!(
                    (hv - fv).abs() <= 1e-9 * fv.abs(),
                    "{func} @{ht}: {hv} != {fv}"
                );
            }
        }
    }
    // The rate of a counter histogram is a gauge histogram.
    let out = run(
        &source,
        r#"rate(h{job="a"}[5m])"#,
        &RangeQuery::new(5 * M, 5 * M, M),
    );
    assert_eq!(
        out[0].histograms()[0].1.counter_reset_hint,
        CounterResetHint::Gauge
    );
}

#[test]
fn a_window_mixing_floats_and_histograms_answers_what_upstream_does() {
    let source = source(&fixture());
    // (0, 5m] holds floats at 1m, 4m and 5m and histograms at 2m and 3m.
    let at = RangeQuery::new(5 * M, 5 * M, M);
    for query in [
        "rate(mixed[5m])",
        "sum_over_time(mixed[5m])",
        "avg_over_time(mixed[5m])",
    ] {
        assert!(run(&source, query, &at).is_empty(), "{query}");
    }
    let value = |query: &str| floats(&run(&source, query, &at)[0])[0].1;
    assert_eq!(value("count_over_time(mixed[5m])"), 5.0);
    assert_eq!(value("max_over_time(mixed[5m])"), 6.0);
    assert_eq!(value("min_over_time(mixed[5m])"), 2.0);
    // float→histogram, histogram→histogram, histogram→float, float→float.
    assert_eq!(value("changes(mixed[5m])"), 4.0);
    assert_eq!(value("last_over_time(mixed[5m])"), 6.0);
    // The last two samples at 3m are both histograms; at 4m they are not.
    let irate = run(
        &source,
        "irate(mixed[5m])",
        &RangeQuery::new(3 * M, 4 * M, M),
    );
    assert_eq!(counts(&irate[0]), vec![(3 * M, 1.0 / 60.0)]);
    assert!(floats(&irate[0]).is_empty());
}

#[test]
fn sum_adds_each_type_and_gives_way_where_a_group_holds_both() {
    let source = source(&fixture());
    let range = RangeQuery::new(0, 2 * M, M);
    let out = run(&source, "sum(h)", &range);
    assert_eq!(counts(&out[0]), vec![(0, 4.0), (M, 7.0), (2 * M, 10.0)]);
    let out = run(&source, "avg(h)", &range);
    assert_eq!(counts(&out[0]), vec![(0, 2.0), (M, 3.5), (2 * M, 5.0)]);
    // `g` is a histogram for job a and a float for job b.
    assert!(run(&source, "sum(g)", &range).is_empty());
    let by_job = run(&source, "sum by (job) (g)", &range);
    assert_eq!(by_job.len(), 2);
    assert_eq!(counts(&by_job[0]).len(), 3);
    assert_eq!(floats(&by_job[1]), vec![(0, 7.0), (M, 7.0), (2 * M, 7.0)]);
    // `count` takes either type as one more element; `max` passes over a
    // histogram.
    assert_eq!(floats(&run(&source, "count(g)", &range)[0])[0], (0, 2.0));
    assert_eq!(floats(&run(&source, "max(g)", &range)[0])[0], (0, 7.0));
}

#[test]
fn binary_operators_know_both_types() {
    let source = source(&fixture());
    let at = RangeQuery::new(2 * M, 2 * M, M);
    let one = |query: &str| run(&source, query, &at);
    // histogram + histogram, paired by label set.
    let sum = one("h + h");
    assert_eq!(sum.len(), 2);
    assert_eq!(counts(&sum[0]), vec![(2 * M, 6.0)]);
    assert_eq!(counts(&sum[1]), vec![(2 * M, 14.0)]);
    // A histogram against a scalar, and against a float vector.
    assert_eq!(counts(&one(r#"h{job="a"} * 2"#)[0]), vec![(2 * M, 6.0)]);
    assert_eq!(counts(&one(r#"2 * h{job="a"}"#)[0]), vec![(2 * M, 6.0)]);
    assert_eq!(counts(&one(r#"h{job="a"} / 3"#)[0]), vec![(2 * M, 1.0)]);
    assert_eq!(
        counts(&one(r#"h{job="a"} * on(job) count{job="a"}"#)[0]),
        vec![(2 * M, 9.0)]
    );
    assert_eq!(counts(&one(r#"-h{job="a"}"#)[0]), vec![(2 * M, -3.0)]);
    // The pairings upstream leaves out of the result.
    for query in [
        r#"h{job="a"} + 1"#,
        r#"1 / h{job="a"}"#,
        r#"h{job="a"} > 0"#,
        r#"h{job="a"} + on(job) count{job="a"}"#,
        r#"h{job="a"} > on(job) h{job="a"}"#,
    ] {
        assert!(one(query).is_empty(), "{query}");
    }
    // Equality keeps the left histogram; `bool` scores it as a float.
    assert_eq!(counts(&one("h == h")[0]), vec![(2 * M, 3.0)]);
    assert!(one("h != h").is_empty());
    assert_eq!(floats(&one("h == bool h")[0]), vec![(2 * M, 1.0)]);
    // Set operators hand a histogram back as it is.
    assert_eq!(
        counts(&one(r#"h and on(job) count{job="a"}"#)[0]),
        vec![(2 * M, 3.0)]
    );
    // `count` shares both of `h`'s signatures; `mixed` has its own.
    assert_eq!(one("h or count").len(), 2);
    assert_eq!(one("h or mixed").len(), 3);
    assert_eq!(one(r#"h unless on(job) count{job="a"}"#).len(), 1);
}

#[test]
fn trims_cut_a_histogram_and_drop_a_float() {
    let source = source(&[
        "t {{schema:0 sum:10 count:6 buckets:[1 2 3]}}x4",
        "f 3x4",
        "x 2x4",
    ]);
    let at = RangeQuery::new(M, M, M);
    // Buckets (0.5,1], (1,2], (2,4]: at 2 the first two stay below.
    let upper = run(&source, "t </ 2", &at);
    assert_eq!(counts(&upper[0]), vec![(M, 3.0)]);
    assert_eq!(
        labels(&upper[0]),
        vec![("__name__".to_string(), "t".to_string())]
    );
    let lower = run(&source, "t >/ 2", &at);
    assert_eq!(counts(&lower[0]), vec![(M, 3.0)]);
    assert!(run(&source, "f </ 2", &at).is_empty());
    // Between vectors the float comes from the matched series.
    let paired = run(&source, "t </ on() x", &at);
    assert_eq!(counts(&paired[0]), vec![(M, 3.0)]);
    assert!(run(&source, "f </ on() x", &at).is_empty());
}

#[test]
fn histogram_quantile_reads_native_and_classic_histograms() {
    let source = source(&[
        // One native, one classic, and one label set that is both.
        r#"native{kind="n"} {{schema:0 sum:10 count:4 buckets:[2 2]}}x4"#,
        r#"classic_bucket{kind="c", le="1"} 2x4"#,
        r#"classic_bucket{kind="c", le="2"} 4x4"#,
        r#"classic_bucket{kind="c", le="+Inf"} 4x4"#,
        r#"both{le="1"} 1x4"#,
        r#"both{le="+Inf"} 2x4"#,
        r#"both {{schema:0 sum:1 count:1 buckets:[1]}}x4"#,
    ]);
    let at = RangeQuery::new(M, M, M);
    let value = |query: &str| {
        let out = run(&source, query, &at);
        assert_eq!(out.len(), 1, "{query}");
        // Neither the name nor `le` is in the answer.
        assert_eq!(labels(&out[0]).len(), 1, "{query}");
        assert_eq!(labels(&out[0])[0].0, "kind", "{query}");
        floats(&out[0])[0].1
    };
    assert_eq!(value("histogram_quantile(0.5, classic_bucket)"), 1.0);
    assert_eq!(value("histogram_fraction(0, 1, classic_bucket)"), 0.5);
    // Half of four observations in (0.5,1] and (1,2]: the top of the first.
    assert_eq!(value("histogram_quantile(0.5, native)"), 1.0);
    assert_eq!(value("histogram_count(native)"), 4.0);
    // A native and a classic histogram of one label set answer nothing.
    assert!(run(&source, "histogram_quantile(0.5, both)", &at).is_empty());
    // Both kinds in one vector, each under its own labels.
    let all = run(
        &source,
        r#"histogram_quantile(0.5, {__name__=~"native|classic_bucket"})"#,
        &at,
    );
    assert_eq!(all.len(), 2);
    assert!(all.iter().all(|s| floats(s) == vec![(M, 1.0)]));
    // Two series with one label set once the name is gone answer the
    // same step twice, which the result refuses as it does for floats.
    let twice = source_of_two_names();
    let err = try_run(&twice, r#"histogram_count({__name__=~"p|q"})"#, &at).unwrap_err();
    assert!(err.to_string().contains("same labelset"), "{err}");
}

fn source_of_two_names() -> MemorySeriesSource {
    source(&[
        "p {{schema:0 sum:1 count:1 buckets:[1]}}x4",
        "q {{schema:0 sum:1 count:1 buckets:[1]}}x4",
    ])
}

#[test]
fn an_instant_range_selector_returns_both_sample_lists() {
    let source = source(&fixture());
    let engine = Engine::blocking().unwrap();
    let result = engine.instant_query(&source, "mixed[3m]", 4 * M).unwrap();
    let promql_engine::InstantResult::Matrix(batches) = result else {
        panic!("expected a matrix");
    };
    let out = coalesce(decode(&batches).unwrap()).unwrap();
    // (1m, 4m]: the float at 1m is out, the one at 4m is in.
    assert_eq!(floats(&out[0]), vec![(4 * M, 5.0)]);
    assert_eq!(counts(&out[0]), vec![(2 * M, 3.0), (3 * M, 4.0)]);
}

type Cut = Box<dyn Fn(MemorySeriesSource) -> MemorySeriesSource>;

/// Every way the reference store can cut one selection.
fn cuts() -> Vec<(&'static str, Cut)> {
    let nz = |n: usize| NonZeroUsize::new(n).unwrap();
    vec![
        (
            "one sample per chunk",
            Box::new(|s: MemorySeriesSource| s.chunked(0)),
        ),
        (
            "three-minute chunks",
            Box::new(|s: MemorySeriesSource| s.chunked(3 * M)),
        ),
        (
            "blocks between steps",
            Box::new(|s: MemorySeriesSource| s.blocks(NonZeroU64::new(200_000).unwrap())),
        ),
        (
            "four partitions",
            Box::new(move |s: MemorySeriesSource| s.partitions(nz(4))),
        ),
        (
            "everything at once",
            Box::new(move |s: MemorySeriesSource| {
                s.with_ids(SeriesIdEncoding::FixedSizeBinary)
                    .chunked(0)
                    .blocks(NonZeroU64::new(M as u64).unwrap())
                    .partitions(nz(3))
                    .rows_per_batch(nz(1))
            }),
        ),
    ]
}

#[test]
fn the_answer_does_not_depend_on_how_the_store_cuts_it() {
    let range = RangeQuery::new(0, 20 * M, M / 2);
    let plain = source(&fixture());
    let comparable = |series: Vec<Series>| -> Vec<_> {
        let mut out: Vec<_> = series
            .iter()
            .map(|s| {
                let histograms: Vec<(i64, String)> = s
                    .histograms()
                    .iter()
                    .map(|(t, h)| (*t, format!("{h:?}")))
                    .collect();
                let floats: Vec<(i64, u64)> =
                    floats(s).iter().map(|(t, v)| (*t, v.to_bits())).collect();
                (labels(s), floats, histograms)
            })
            .collect();
        out.sort();
        out
    };
    for query in [
        "h",
        "mixed",
        "g",
        "mixed offset 90s",
        "h @ 600",
        "rate(h[5m])",
        "increase(mixed[4m])",
        "irate(mixed[3m])",
        "sum_over_time(h[3m])",
        "avg_over_time(g[3m])",
        "count_over_time(mixed[3m])",
        "last_over_time(mixed[3m])",
        "changes(mixed[10m])",
        "resets(mixed[10m])",
        "max_over_time(mixed[10m])",
        "histogram_count(h)",
        "histogram_sum(rate(h[5m]))",
        "histogram_quantile(0.9, rate(h[5m]))",
        "timestamp(mixed)",
        "sum(h)",
        "avg by (job) (h)",
        "sum(g)",
        "count(mixed)",
        "h + h",
        "h * on(job) count",
        "h or mixed",
        "-h",
        "h </ 1.5",
        "absent(mixed)",
    ] {
        let want = comparable(run(&plain, query, &range));
        // A group of both types answers nothing, and `mixed` is never absent.
        let empty = ["sum(g)", "absent(mixed)"].contains(&query);
        assert_eq!(want.is_empty(), empty, "{query}");
        for (name, cut) in cuts() {
            let got = comparable(run(&cut(source(&fixture())), query, &range));
            assert_eq!(got, want, "{query} over {name}");
        }
    }
}

/// One raw row far longer than the evaluator's per-poll work budget, its
/// floats and histograms alternating in runs: the row is consumed over
/// many polls, each resuming inside a run of one type or between two.
#[test]
fn a_long_row_of_both_types_resumes_where_its_poll_left_off() {
    const SAMPLES: i64 = 90_000;
    const RUN: i64 = 700;
    let h = |count: f64| FloatHistogram {
        count,
        sum: count,
        ..FloatHistogram::default()
    };
    let mut raw = (Vec::new(), Vec::new());
    let mut histograms = Vec::new();
    for i in 0..SAMPLES {
        match (i / RUN) % 2 {
            0 => {
                raw.0.push(i * 1000);
                raw.1.push(i as f64);
            }
            _ => histograms.push((i * 1000, h(i as f64))),
        }
    }
    let build = || {
        let series = Series::new(&[("__name__", "long")], raw.0.clone(), raw.1.clone())
            .unwrap()
            .with_histograms(histograms.clone())
            .unwrap();
        MemorySeriesSource::try_new(vec![series]).unwrap()
    };
    let end = (SAMPLES - 1) * 1000;
    let range = RangeQuery::new(0, end, 250_000);
    // Each step is the latest sample, a float or a histogram by its run.
    let selected = run(&build(), "long", &range);
    let steps = (end / 250_000 + 1) as usize;
    assert_eq!(
        floats_of(&selected).len() + counts(&selected[0]).len(),
        steps
    );
    for (t, v) in floats_of(&selected) {
        assert_eq!((v as i64, (t / 1000 / RUN) % 2), (t / 1000, 0));
    }
    for (t, count) in counts(&selected[0]) {
        assert_eq!((count as i64, (t / 1000 / RUN) % 2), (t / 1000, 1));
    }
    // Every sample is counted once, whatever poll it arrived in.
    let all = RangeQuery::new(end, end, 1000);
    let total = run(&build(), "count_over_time(long[2d])", &all);
    assert_eq!(floats(&total[0]), vec![(end, SAMPLES as f64)]);
    // And cut into chunks the answers are the same.
    let chunked = build().chunked(10_000);
    assert_eq!(
        counts(&run(&chunked, "long", &range)[0]),
        counts(&selected[0])
    );
    assert_eq!(
        floats_of(&run(&chunked, "long", &range)),
        floats_of(&selected)
    );
}

fn floats_of(series: &[Series]) -> Vec<(i64, f64)> {
    floats(&series[0])
}
