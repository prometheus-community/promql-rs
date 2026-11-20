//! End to end: the arithmetic binary operators over the in-memory
//! source.
//!
//! What is under test is the matching — which series pair up, which
//! labels survive, what an unmatched side leaves behind — rather than
//! the arithmetic, which `binary.rs`'s unit tests hold to Go's `math`
//! value by value.
//!
//! A one-step range stands in for an instant query, which this engine
//! does not have its own entry point for yet: with `start == end` every
//! series carries exactly one value, so a row reads as a vector element.

use std::sync::Arc;

use promql_engine::{Engine, EngineError, EngineOptions, MemorySeriesSource, RangeQuery};
use promql_parser::{ParserOptions, SeriesDescription};

/// `fill`, `fill_left` and `fill_right` are experimental syntax: a stock
/// engine refuses them while parsing, so the tests that evaluate one switch
/// on the gate that admits it.
fn engine() -> Engine {
    Engine::blocking_with_options(EngineOptions {
        parser: ParserOptions {
            enable_binop_fill_modifiers: true,
            ..Default::default()
        },
    })
    .unwrap()
}

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Two metrics over the same two pods, plus one pod only `requests`
/// knows about — the series with nothing to match against.
fn source() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"requests{pod="a"} 10+10x10"#,
            r#"requests{pod="b"} 20+20x10"#,
            r#"requests{pod="c"} 30+30x10"#,
            r#"errors{pod="a"} 1+1x10"#,
            r#"errors{pod="b"} 2+2x10"#,
        ]),
        30.0,
    ))
}

/// The one step every assertion below is read at.
fn at_150s() -> RangeQuery {
    RangeQuery::new(150_000, 150_000, 30_000)
}

/// One vector element: its labels, then its value.
type Row = (Vec<(String, String)>, f64);

/// The result of `query` over `source` at one step, sorted by label set
/// — a vector result has no promised order, and DataFusion's grouping
/// order is not one either.
fn vector_of(
    source: &dyn promql_engine::SeriesSource,
    query: &str,
    range: &RangeQuery,
) -> Vec<Row> {
    let batches = engine()
        .range_query(source, query, range)
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut rows: Vec<Row> = promql_engine::series::decode(&batches)
        .expect("the canonical shape decodes")
        .iter()
        .map(|s| {
            assert_eq!(s.values().len(), 1, "{query}: one step, one value");
            (
                s.labels()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                s.values()[0],
            )
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

fn vector(query: &str) -> Vec<Row> {
    vector_of(source().as_ref(), query, &at_150s())
}

fn pod(name: &str) -> Vec<(String, String)> {
    vec![("pod".to_string(), name.to_string())]
}

/// At 150s: requests are 60, 120, 180 and errors 6, 12.
#[test]
fn a_scalar_on_either_side_applies_to_every_sample() {
    assert_eq!(
        vector("requests * 2"),
        [(pod("a"), 120.0), (pod("b"), 240.0), (pod("c"), 360.0),]
    );
    // The operands keep their order when the scalar is on the left,
    // which is the whole of upstream's `swap`.
    assert_eq!(
        vector("600 / requests"),
        [(pod("a"), 10.0), (pod("b"), 5.0), (pod("c"), 10.0 / 3.0)]
    );
    assert_eq!(
        vector("requests - 600"),
        [(pod("a"), -540.0), (pod("b"), -480.0), (pod("c"), -420.0),]
    );
}

/// `changesMetricSchema` holds for every arithmetic operator, so the
/// result is never the metric it was computed from.
#[test]
fn arithmetic_drops_the_metric_name() {
    for query in ["requests * 2", "2 * requests", "requests / errors"] {
        let names: Vec<_> = vector(query)
            .iter()
            .flat_map(|(labels, _)| labels.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>())
            .collect();
        assert!(
            !names.contains(&"__name__".to_string()),
            "{query}: {names:?}"
        );
    }
}

/// Default matching pairs on every label but `__name__`, so `pod="c"`
/// — which only one side has — is simply not in the result.
#[test]
fn one_to_one_matching_drops_the_series_with_no_partner() {
    assert_eq!(
        vector("requests / errors"),
        [(pod("a"), 10.0), (pod("b"), 10.0)]
    );
    assert_eq!(
        vector("requests + errors"),
        [(pod("a"), 66.0), (pod("b"), 132.0)]
    );
}

/// A label one side does not carry is part of the signature all the
/// same: `{pod="a"}` and `{pod="a", zone="eu"}` are different match
/// groups, so neither of them pairs.
#[test]
fn an_extra_label_on_one_side_is_a_different_match_group() {
    let source = MemorySeriesSource::from_descriptions(
        &load(&[r#"requests{pod="a"} 10"#, r#"errors{pod="a", zone="eu"} 1"#]),
        30.0,
    );
    let range = RangeQuery::new(0, 0, 30_000);
    assert!(vector_of(&source, "requests / errors", &range).is_empty());
}

/// An operand that selects nothing has nothing to pair with, and the
/// answer is the empty vector rather than the other side.
#[test]
fn an_empty_side_yields_an_empty_result() {
    for query in ["requests * missing", "missing * requests"] {
        assert!(vector(query).is_empty(), "{query}");
    }
}

/// Two series in one match group are upstream's two matching errors,
/// in upstream's words.
#[test]
fn a_match_group_with_two_series_on_a_side_fails() {
    let source = MemorySeriesSource::from_descriptions(
        &load(&[
            r#"requests{pod="a"} 10"#,
            r#"retries{pod="a"} 5"#,
            r#"errors{pod="a"} 1"#,
        ]),
        30.0,
    );
    let engine = engine();
    let range = RangeQuery::new(0, 0, 30_000);

    let err = engine
        .range_query(
            &source,
            r#"{__name__=~"requests|retries"} + errors"#,
            &range,
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(
            "multiple matches for labels: many-to-one matching must be explicit \
             (group_left/group_right)"
        ),
        "{err}"
    );

    let err = engine
        .range_query(
            &source,
            r#"errors + {__name__=~"requests|retries"}"#,
            &range,
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(
            "found duplicate series for the match group {pod=\"a\"} on the right hand-side of \
             the operation: ["
        ),
        "{err}"
    );
    assert!(
        err.contains(
            ";many-to-many matching not allowed: matching labels must be unique on one side"
        ),
        "{err}"
    );
}

/// One vector element with its `__name__`, which is the whole point of
/// a comparison: it answers with a sample that was already there.
fn named(metric: &str, pod_name: &str) -> Vec<(String, String)> {
    vec![
        ("__name__".to_string(), metric.to_string()),
        ("pod".to_string(), pod_name.to_string()),
    ]
}

/// A filtering comparison feeding another operator. At 150s requests
/// are 60, 120, 180 and errors 6, 12, so `requests > 100` is pods `b`
/// and `c`, still called `requests`.
#[test]
fn a_filtering_comparison_can_be_an_operand() {
    let named = |metric: &str, pod_name: &str| {
        vec![
            ("__name__".to_string(), metric.to_string()),
            ("pod".to_string(), pod_name.to_string()),
        ]
    };

    // Arithmetic on either side: pod `b` is the only one both reach.
    assert_eq!(vector("(requests > 100) + errors"), [(pod("b"), 132.0)]);
    assert_eq!(vector("errors + (requests > 100)"), [(pod("b"), 132.0)]);

    // Set operators, which keep the name the comparison kept.
    assert_eq!(
        vector("(requests > 100) or errors"),
        [
            (named("errors", "a"), 6.0),
            (named("requests", "b"), 120.0),
            (named("requests", "c"), 180.0),
        ]
    );
    assert_eq!(
        vector("(requests > 100) unless errors"),
        [(named("requests", "c"), 180.0)]
    );

    // And a function above it, which is the same shape one stage down.
    assert_eq!(
        vector("abs(requests > 100)"),
        [(pod("b"), 120.0), (pod("c"), 180.0)]
    );
}

/// A comparison against a scalar keeps the samples that pass and the
/// metric they came from; `bool` scores every sample instead and drops
/// the name, because a 1 is no longer that metric.
#[test]
fn a_comparison_against_a_scalar_filters_and_keeps_the_name() {
    assert_eq!(
        vector("requests > 100"),
        [
            (named("requests", "b"), 120.0),
            (named("requests", "c"), 180.0)
        ]
    );
    assert_eq!(
        vector("requests > bool 100"),
        [(pod("a"), 0.0), (pod("b"), 1.0), (pod("c"), 1.0)]
    );
    // Upstream keeps the vector element's value whichever side the
    // scalar was on, so the answer is the series' own number, not 100.
    assert_eq!(
        vector("100 < requests"),
        [
            (named("requests", "b"), 120.0),
            (named("requests", "c"), 180.0)
        ]
    );
    assert_eq!(
        vector("100 < bool requests"),
        [(pod("a"), 0.0), (pod("b"), 1.0), (pod("c"), 1.0)]
    );
}

/// The same rules between two vectors, over the match groups the
/// arithmetic already pairs: the left sample survives with its own
/// labels, `bool` replaces it with a score and takes the name away.
#[test]
fn a_comparison_between_vectors_answers_with_the_left_sample() {
    assert_eq!(
        vector("requests > errors"),
        [
            (named("requests", "a"), 60.0),
            (named("requests", "b"), 120.0)
        ]
    );
    assert_eq!(vector("requests < errors"), []);
    assert_eq!(
        vector("requests < bool errors"),
        [(pod("a"), 0.0), (pod("b"), 0.0)]
    );
    // `pod="c"` has nothing to compare against, so it is gone either
    // way — matching happens before the filter.
    assert_eq!(
        vector("requests >= bool errors"),
        [(pod("a"), 1.0), (pod("b"), 1.0)]
    );
}

/// Two left series that differ only in `__name__` are one match group,
/// because the signature drops the name — and a filtering comparison
/// gives it back, so upstream answers with both of them. Their samples
/// never meet at a step, so they are no duplicate either.
///
/// With `bool` the name goes away and the two become one label set,
/// which is one series in the answer. Upstream reaches the same place
/// from the other end: `VectorBinop` still emits a sample per left
/// series, and the range evaluation collects them into a series per
/// label set. Were the two to meet at a step, both engines would have
/// failed the match long before that.
#[test]
fn a_kept_name_splits_a_match_group_into_a_series_each() {
    // 350s apart, so that the 5m lookback has let `a` go stale before
    // `b` arrives: two series that overlap at a step are a duplicate,
    // and that is the other test.
    let source = MemorySeriesSource::from_descriptions(
        &load(&[
            r#"a{x="1"} 1 _ _"#,
            r#"b{x="1"} _ _ 4"#,
            r#"c{x="1"} 2 2 2"#,
        ]),
        350.0,
    );
    let engine = engine();
    let range = promql_engine::RangeQuery::new(0, 700_000, 700_000);

    let batches = engine
        .range_query(&source, r#"{__name__=~"a|b"} != c"#, &range)
        .expect("the query runs");
    let mut series = promql_engine::series::decode(&batches).expect("the canonical shape decodes");
    series.sort_by_key(|s| s.label("__name__").to_string());
    let seen: Vec<_> = series
        .iter()
        .map(|s| (s.label("__name__").to_string(), s.timestamps(), s.values()))
        .collect();
    assert_eq!(
        seen,
        [
            ("a".to_string(), &[0i64][..], &[1.0f64][..]),
            ("b".to_string(), &[700_000][..], &[4.0][..]),
        ]
    );

    let batches = engine
        .range_query(&source, r#"{__name__=~"a|b"} != bool c"#, &range)
        .expect("the query runs");
    // One label set once the names are dropped, in rows the consumer
    // merges, as upstream's `mergeSeriesWithSameLabelset` does.
    let series = promql_engine::series::decode(&batches).expect("the canonical shape decodes");
    assert!(series.iter().all(|s| s.label("__name__").is_empty()));
    assert!(series.iter().all(|s| s.label("x") == "1"));
    let mut points: Vec<_> = series
        .iter()
        .flat_map(|s| {
            s.timestamps()
                .iter()
                .copied()
                .zip(s.values().iter().copied())
        })
        .collect();
    points.sort_by_key(|(t, _)| *t);
    assert_eq!(points, [(0, 1.0), (700_000, 1.0)]);
}

/// Two metrics that agree on `pod` and nothing else, plus a per-pod
/// one the fan-out modifiers have something to copy from.
fn zoned() -> MemorySeriesSource {
    MemorySeriesSource::from_descriptions(
        &load(&[
            r#"requests{pod="a", path="/x"} 10"#,
            r#"requests{pod="a", path="/y"} 20"#,
            r#"requests{pod="b", path="/x"} 30"#,
            r#"limit{pod="a", zone="eu"} 2"#,
            r#"limit{pod="b", zone="us"} 3"#,
        ]),
        30.0,
    )
}

/// The result of `query` over `zoned()` at 0, as label sets with their
/// value, sorted.
fn matched(query: &str) -> Vec<Row> {
    vector_of(&zoned(), query, &RangeQuery::new(0, 0, 30_000))
}

fn labels(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// `on(…)` matches on exactly those labels and the result keeps only
/// them; `ignoring(…)` matches on everything else and the result keeps
/// everything the left side had but those.
#[test]
fn on_and_ignoring_decide_the_signature_and_the_result() {
    // `path` would keep the two `requests` series apart, so `on(pod)`
    // is what makes them one match group — and two of them, which is
    // the error below. `pod="b"` is the one that is alone.
    assert_eq!(
        matched(r#"requests{path="/x", pod="b"} / on(pod) limit"#),
        [(labels(&[("pod", "b")]), 10.0)]
    );
    // Without `on`, `ignoring` has to name every label the two sides
    // disagree on, and the result keeps the rest of the left side's.
    assert_eq!(
        matched(r#"requests{path="/x"} / ignoring(path, zone) limit"#),
        [
            (labels(&[("pod", "a")]), 5.0),
            (labels(&[("pod", "b")]), 10.0),
        ]
    );
}

/// `group_left` lets the left side be many: every one of its series
/// pairs with the single right-hand one, keeps all of its own labels,
/// and takes the named ones along from the right.
#[test]
fn group_left_fans_out_and_copies_the_named_labels() {
    assert_eq!(
        matched("requests / on(pod) group_left(zone) limit"),
        [
            (labels(&[("path", "/x"), ("pod", "a"), ("zone", "eu")]), 5.0),
            (
                labels(&[("path", "/x"), ("pod", "b"), ("zone", "us")]),
                10.0
            ),
            (
                labels(&[("path", "/y"), ("pod", "a"), ("zone", "eu")]),
                10.0
            ),
        ]
    );
    // Without a label to copy, the fan-out is the only thing the
    // modifier does.
    assert_eq!(
        matched("requests / on(pod) group_left limit"),
        [
            (labels(&[("path", "/x"), ("pod", "a")]), 5.0),
            (labels(&[("path", "/x"), ("pod", "b")]), 10.0),
            (labels(&[("path", "/y"), ("pod", "a")]), 10.0),
        ]
    );
}

/// `group_right` is the mirror image, and the operands keep the order
/// the query wrote them in: this is limit divided by requests.
#[test]
fn group_right_lets_the_right_side_be_many() {
    assert_eq!(
        matched("limit / on(pod) group_right(zone) requests"),
        [
            (labels(&[("path", "/x"), ("pod", "a"), ("zone", "eu")]), 0.2),
            (labels(&[("path", "/x"), ("pod", "b"), ("zone", "us")]), 0.1),
            (labels(&[("path", "/y"), ("pod", "a"), ("zone", "eu")]), 0.1),
        ]
    );
}

/// A comparison keeps the many side's `__name__` through the fan-out,
/// because `changesMetricSchema` is false for it.
#[test]
fn a_fan_out_comparison_keeps_the_metric_name() {
    assert_eq!(
        matched("requests > on(pod) group_left(zone) limit"),
        [
            (
                labels(&[
                    ("__name__", "requests"),
                    ("path", "/x"),
                    ("pod", "a"),
                    ("zone", "eu")
                ]),
                10.0
            ),
            (
                labels(&[
                    ("__name__", "requests"),
                    ("path", "/x"),
                    ("pod", "b"),
                    ("zone", "us")
                ]),
                30.0
            ),
            (
                labels(&[
                    ("__name__", "requests"),
                    ("path", "/y"),
                    ("pod", "a"),
                    ("zone", "eu")
                ]),
                20.0
            ),
        ]
    );
}

/// Every way a match can fail, in upstream's words.
#[test]
fn the_matching_errors_are_upstreams() {
    let engine = engine();
    let source = zoned();
    // Every one of these is a rejection Prometheus also makes, so it has
    // to reach the caller as `Query` and not as the DataFusion error the
    // UDAF had no choice but to raise.
    let at_0 = RangeQuery::new(0, 0, 30_000);
    let fails = |query: &str| match engine.range_query(&source, query, &at_0).expect_err(query) {
        EngineError::Query(message) => message,
        other => panic!("{query}: expected a query error, got {other:?}"),
    };

    // Two left series in one match group, and no modifier saying so.
    let err = fails("requests / on(pod) limit");
    assert!(
        err.contains(
            "multiple matches for labels: many-to-one matching must be explicit \
             (group_left/group_right)"
        ),
        "{err}"
    );

    // Two right-hand series in one match group is many-to-many.
    let err = fails("limit / on(pod) requests");
    assert!(
        err.contains("found duplicate series for the match group")
            && err.contains("on the right hand-side of the operation: ["),
        "{err}"
    );
    assert!(
        err.contains(
            ";many-to-many matching not allowed: matching labels must be unique on one side"
        ),
        "{err}"
    );

    // group_right moves the "one" to the left, and the message with it.
    let err = fails("requests / on(pod) group_right() limit");
    assert!(
        err.contains("on the left hand-side of the operation: ["),
        "{err}"
    );

    // A fan-out whose result labels collide: the two left series
    // differ only in `__name__`, which the division takes away.
    let same = MemorySeriesSource::from_descriptions(
        &load(&[r#"a{pod="x"} 1"#, r#"b{pod="x"} 2"#, r#"c{pod="x"} 4"#]),
        30.0,
    );
    let err = match engine
        .range_query(
            &same,
            r#"{__name__=~"a|b"} / on(pod) group_left() c"#,
            &at_0,
        )
        .expect_err("the fan-out collides")
    {
        EngineError::Query(message) => message,
        other => panic!("expected a query error, got {other:?}"),
    };
    assert!(
        err.contains("multiple matches for labels: grouping labels must ensure unique matches"),
        "{err}"
    );
}

/// The set operators over `zoned()`: each of them asks only whether
/// the other side has this signature at this step, and answers with
/// the sample it was handed -- labels, `__name__` and value intact.
#[test]
fn a_set_operator_filters_by_signature_and_keeps_the_sample() {
    let requests =
        |pod: &str, path: &str| labels(&[("__name__", "requests"), ("path", path), ("pod", pod)]);

    // Both pods have a limit, so every request series survives -- and
    // `ignoring` names the same signature the other way round.
    for query in [
        "requests and on(pod) limit",
        "requests and ignoring(path, zone) limit",
    ] {
        assert_eq!(
            matched(query),
            [
                (requests("a", "/x"), 10.0),
                (requests("b", "/x"), 30.0),
                (requests("a", "/y"), 20.0),
            ],
            "{query}"
        );
    }
    assert!(matched("requests unless on(pod) limit").is_empty());

    // A label one side does not carry is the empty string, which is a
    // signature of its own: nothing matches on `zone`.
    assert!(matched("requests and on(zone) limit").is_empty());
    assert_eq!(matched("requests unless on(zone) limit").len(), 3);

    // Default matching is every label but `__name__`, so this pairs on
    // `{pod, path}` and only `/y` is left without a partner.
    assert_eq!(
        matched(r#"requests unless requests{path="/x"}"#),
        [(requests("a", "/y"), 20.0)]
    );
}

/// `or` is the left side whole plus the right side's series under the
/// signatures the left side left empty, each keeping its own labels.
#[test]
fn or_fills_in_the_signatures_the_left_side_missed() {
    let limit =
        |pod: &str, zone: &str| labels(&[("__name__", "limit"), ("pod", pod), ("zone", zone)]);
    let requests =
        |pod: &str, path: &str| labels(&[("__name__", "requests"), ("path", path), ("pod", pod)]);

    // Pod `a` is answered by the left side, so only pod `b`'s limit is
    // filled in -- as `limit`, not as `requests`.
    assert_eq!(
        matched(r#"requests{path="/y"} or on(pod) limit"#),
        [(limit("b", "us"), 3.0), (requests("a", "/y"), 20.0)]
    );
    // Every signature the right side has is already on the left.
    assert_eq!(matched("requests or on(pod) limit").len(), 3);
    // And with nothing on the left, `or` is the right side.
    assert_eq!(
        matched("missing or on(pod) limit"),
        [(limit("a", "eu"), 2.0), (limit("b", "us"), 3.0)]
    );
}

/// Upstream refuses both of these while parsing; this engine has to
/// say so itself, in the same words.
#[test]
fn a_set_operator_takes_neither_a_group_modifier_nor_a_scalar() {
    let engine = engine();
    let at_0 = RangeQuery::new(0, 0, 30_000);
    let fails = |query: &str| match engine.range_query(&zoned(), query, &at_0).expect_err(query) {
        EngineError::Query(message) => message,
        other => panic!("{query}: expected a query error, got {other:?}"),
    };

    assert_eq!(
        fails("requests and on(pod) group_left() limit"),
        r#"no grouping allowed for "and" operation"#
    );
    assert_eq!(
        fails("limit or on(pod) group_right(path) requests"),
        r#"no grouping allowed for "or" operation"#
    );
    assert_eq!(
        fails("requests unless 1"),
        r#"set operator "unless" not allowed in binary scalar expression"#
    );
    assert_eq!(
        fails("2 and 3"),
        r#"set operator "and" not allowed in binary scalar expression"#
    );
}

/// A fill value answers for a side that has no series in a match
/// group, where the same query without one drops the step. Over
/// `zoned()`: pod `c` is on neither side, and `path`/`zone` are only on
/// one each, so `on(pod)` is the signature that has something to fill.
#[test]
fn a_fill_value_answers_for_a_side_with_no_series() {
    // Without a fill, only the pods both metrics know about answer.
    assert_eq!(
        matched(r#"requests{path="/x"} - on(pod) limit"#),
        [(pod("a"), 8.0), (pod("b"), 27.0)]
    );

    // `fill_right(0)` stands in for the right operand, so pod `d`'s
    // request -- which has no limit -- answers with its own value.
    let source = MemorySeriesSource::from_descriptions(
        &load(&[
            r#"requests{pod="a", path="/x"} 10"#,
            r#"requests{pod="d", path="/x"} 40"#,
            r#"limit{pod="a", zone="eu"} 2"#,
            r#"limit{pod="e", zone="us"} 5"#,
        ]),
        30.0,
    );
    let at = |query: &str| vector_of(&source, query, &RangeQuery::new(0, 0, 30_000));

    assert_eq!(at("requests - on(pod) limit"), [(pod("a"), 8.0)]);
    assert_eq!(
        at("requests - on(pod) fill_right(0) limit"),
        [(pod("a"), 8.0), (pod("d"), 40.0)]
    );
    // `fill_left(100)` answers for pod `e`, which only `limit` has.
    assert_eq!(
        at("requests - on(pod) fill_left(100) limit"),
        [(pod("a"), 8.0), (pod("e"), 95.0)]
    );
    // `fill(0)` is both at once.
    assert_eq!(
        at("requests - on(pod) fill(0) limit"),
        [(pod("a"), 8.0), (pod("d"), 40.0), (pod("e"), -5.0)]
    );
    // A filled-in side brings only the signature, so the labels that
    // are not matched on are gone from the rows it answers for --
    // `path` is on pod `d`'s request but not on the result.
    assert!(at("requests - on(pod) fill(0) limit")
        .iter()
        .all(|(labels, _)| labels.iter().all(|(k, _)| k == "pod")));
}

/// A match group only one side ever reached is not a match, so its
/// duplicates are nobody's error -- until a fill gives it the other
/// side and it becomes one.
#[test]
fn a_group_with_no_left_side_is_empty_until_a_fill_answers_for_it() {
    let engine = engine();
    // Two `limit` series in one `on(pod)` group and no `requests` at
    // all, which is a duplicate on the right waiting to be noticed.
    let source = MemorySeriesSource::from_descriptions(
        &load(&[
            r#"limit{pod="a", zone="eu"} 2"#,
            r#"limit{pod="a", zone="us"} 3"#,
        ]),
        30.0,
    );

    let at_0 = RangeQuery::new(0, 0, 30_000);
    assert!(vector_of(&source, "requests + on(pod) limit", &at_0).is_empty());

    let err = engine
        .range_query(&source, "requests + on(pod) fill_left(0) limit", &at_0)
        .expect_err("the fill makes it a match group");
    assert!(
        err.to_string()
            .contains("found duplicate series for the match group"),
        "{err}"
    );
}

/// Upstream refuses a fill where there is nothing to fill in, and says
/// so while parsing; this engine has to say it itself.
#[test]
fn a_fill_needs_two_vectors_and_a_value_operator() {
    let engine = engine();
    let at_0 = RangeQuery::new(0, 0, 30_000);
    let fails = |query: &str| match engine.range_query(&zoned(), query, &at_0).expect_err(query) {
        EngineError::Query(message) => message,
        other => panic!("{query}: expected a query error, got {other:?}"),
    };

    assert_eq!(
        fails("requests + fill(0) 1"),
        "filling in missing series only allowed between instant vectors"
    );
    assert_eq!(
        fails("requests and fill(0) limit"),
        "filling in missing series not allowed for set operators"
    );
}

/// Upstream's parser refuses a label that both picks the match and is
/// copied across it; ours has to say so itself.
#[test]
fn a_label_cannot_be_matched_on_and_copied_at_once() {
    let err = engine()
        .range_query(
            &zoned(),
            "requests / on(pod) group_left(pod) limit",
            &RangeQuery::new(0, 0, 30_000),
        )
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::Query(q)
            if q == "label \"pod\" must not occur in ON and GROUP clause at once"),
        "{err}"
    );
}

/// A scalar operand is folded while planning, so one that moves with
/// the step is a gap rather than a wrong answer.
#[test]
fn a_step_varying_scalar_operand_is_unsupported() {
    let range = RangeQuery::new(0, 300_000, 30_000);
    let err = engine()
        .range_query(source().as_ref(), "requests * time()", &range)
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::Unsupported(f)
            if f == "the * operator with a scalar argument that changes between steps"),
        "{err}"
    );
}

/// A range query pairs step by step: a step only one side reached is
/// not in the result, which is what a series that starts late shows.
#[test]
fn matching_is_per_step_not_per_series() {
    let source = MemorySeriesSource::from_descriptions(
        &load(&[r#"requests{pod="a"} 10 20 30"#, r#"errors{pod="a"} _ _ 3"#]),
        30.0,
    );
    // A 30s step over the three scrapes, with the default lookback
    // carrying each sample forward; only the last step has both.
    let range = RangeQuery::new(0, 60_000, 30_000);
    let batches = engine()
        .range_query(&source, "requests / errors", &range)
        .expect("the query runs");
    let series = promql_engine::series::decode(&batches).expect("the canonical shape decodes");
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].timestamps(), [60_000]);
    assert_eq!(series[0].values(), [10.0]);
}
