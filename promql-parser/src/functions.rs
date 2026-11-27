//! Which built-in functions are experimental. Upstream `functions.go`
//! carries a full registry (`Functions`) whose `Experimental` field the
//! `function_call` actions read; the signatures live in
//! `promql-engine/src/function.rs` for this crate's consumers, so only
//! the one bit the parser itself acts on is kept here.

/// The functions `functions.go` marks `Experimental: true`, which
/// `EnableExperimentalFunctions` gates. `start`, `end`, `step` and
/// `range` are among them: their `function_call` arms gate exactly like
/// the identifier arm.
const EXPERIMENTAL: [&str; 18] = [
    "double_exponential_smoothing",
    "end",
    "histogram_quantiles",
    "info",
    "integral",
    "mad_over_time",
    "max_of",
    "min_of",
    "range",
    "sort_by_label",
    "sort_by_label_desc",
    "start",
    "start_timestamp",
    "step",
    "ts_of_first_over_time",
    "ts_of_last_over_time",
    "ts_of_max_over_time",
    "ts_of_min_over_time",
];

/// Whether upstream's registry marks the function `name` experimental.
/// An unknown name is not: upstream gates on `fn != nil && fn.Experimental`.
pub fn is_experimental(name: &str) -> bool {
    EXPERIMENTAL.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The list is the part that can drift from the vendored
    /// `functions.go`, so it is derived from that file here rather than
    /// trusted: a `sync pull` that flips a function's flag fails this.
    #[test]
    fn the_list_is_exactly_what_the_vendored_registry_marks_experimental() {
        let go = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/upstream/functions.go"
        ))
        .expect("vendored functions.go");
        let mut found = Vec::new();
        let mut current = None;
        for line in go.lines() {
            // Registry entries are the only lines indented by exactly one
            // tab that open a map literal: `\t"name": {`.
            if let Some(rest) = line.strip_prefix("\t\"") {
                if let Some((name, tail)) = rest.split_once('"') {
                    if tail == ": {" {
                        current = Some(name.to_string());
                    }
                }
            }
            if line.trim() == "Experimental: true," {
                found.push(current.clone().expect("inside an entry"));
            }
        }
        found.sort();
        let mut ours = EXPERIMENTAL.to_vec();
        ours.sort();
        assert_eq!(ours, found);
    }

    #[test]
    fn unknown_and_stable_names_are_not_experimental() {
        assert!(is_experimental("mad_over_time"));
        assert!(!is_experimental("rate"));
        assert!(!is_experimental("no_such_function"));
    }
}
