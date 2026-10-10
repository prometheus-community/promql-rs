//! Parser feature gates. Upstream `parser.Options` (`upstream/parse.go`).
//!
//! Stock Prometheus parses a smaller language than the grammar accepts:
//! each experimental construct is rejected by the grammar action that
//! builds it unless a `--enable-feature` value switched its gate on
//! (`cmd/prometheus/main.go`). All four gates default to off, here as
//! there, so [`crate::parse_expr`] rejects what `ParseExpr` rejects.
//! Upstream's promqltest turns all four on (`TestParserOpts`), which is
//! what [`ParserOptions::all`] is for.
//!
//! `docs/feature-flags.md` lists the gates against the constructs they
//! guard.

/// Upstream `parser.Options`, field for field.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ParserOptions {
    /// `EnableExperimentalFunctions`, `--enable-feature=promql-experimental-functions`.
    /// Experimental functions (`Experimental: true` in `functions.go`) and
    /// the `limitk` / `limit_ratio` aggregators.
    pub enable_experimental_functions: bool,
    /// `ExperimentalDurationExpr`, `--enable-feature=promql-duration-expr`.
    /// Arithmetic, `step()`, `range()` and `min`/`max` inside durations.
    pub experimental_duration_expr: bool,
    /// `EnableExtendedRangeSelectors`, `--enable-feature=promql-extended-range-selectors`.
    /// The `anchored` and `smoothed` selector modifiers.
    pub enable_extended_range_selectors: bool,
    /// `EnableBinopFillModifiers`, `--enable-feature=promql-binop-fill-modifiers`.
    /// `fill`, `fill_left` and `fill_right` on binary operators.
    pub enable_binop_fill_modifiers: bool,
}

impl ParserOptions {
    /// Every gate on: upstream's `TestParserOpts` in
    /// `promql/promqltest/test.go`.
    pub const fn all() -> Self {
        Self {
            enable_experimental_functions: true,
            experimental_duration_expr: true,
            enable_extended_range_selectors: true,
            enable_binop_fill_modifiers: true,
        }
    }
}

impl ParserOptions {
    /// These options with the gate behind `flag` (an `--enable-feature`
    /// value from [`FEATURE_FLAGS`]) set to `on`, or `None` for a name that
    /// is not one.
    pub fn with_flag(mut self, flag: &str, on: bool) -> Option<Self> {
        let [functions, duration, selectors, fill] = FEATURE_FLAGS.map(|f| f.flag);
        let field = match flag {
            f if f == functions => &mut self.enable_experimental_functions,
            f if f == duration => &mut self.experimental_duration_expr,
            f if f == selectors => &mut self.enable_extended_range_selectors,
            f if f == fill => &mut self.enable_binop_fill_modifiers,
            _ => return None,
        };
        *field = on;
        Some(self)
    }
}

/// One `--enable-feature` value and the option it sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureFlag {
    /// The value after `--enable-feature=`, upstream `cmd/prometheus/main.go`.
    pub flag: &'static str,
    /// Upstream's `parser.Options` field.
    pub upstream_option: &'static str,
    /// This crate's [`ParserOptions`] field.
    pub option: &'static str,
}

/// The four gates, in `parser.Options` order. The conformance harness
/// finds which of them a query needs from this table, so a flag added
/// here is picked up there.
pub const FEATURE_FLAGS: [FeatureFlag; 4] = [
    FeatureFlag {
        flag: "promql-experimental-functions",
        upstream_option: "EnableExperimentalFunctions",
        option: "enable_experimental_functions",
    },
    FeatureFlag {
        flag: "promql-duration-expr",
        upstream_option: "ExperimentalDurationExpr",
        option: "experimental_duration_expr",
    },
    FeatureFlag {
        flag: "promql-extended-range-selectors",
        upstream_option: "EnableExtendedRangeSelectors",
        option: "enable_extended_range_selectors",
    },
    FeatureFlag {
        flag: "promql-binop-fill-modifiers",
        upstream_option: "EnableBinopFillModifiers",
        option: "enable_binop_fill_modifiers",
    },
];

/// The `--enable-feature` values a gate names on the error it raises.
pub(crate) const EXPERIMENTAL_FUNCTIONS: &str = FEATURE_FLAGS[0].flag;
pub(crate) const DURATION_EXPR: &str = FEATURE_FLAGS[1].flag;
pub(crate) const EXTENDED_RANGE_SELECTORS: &str = FEATURE_FLAGS[2].flag;
pub(crate) const BINOP_FILL_MODIFIERS: &str = FEATURE_FLAGS[3].flag;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_gate_defaults_off_and_all_turns_every_gate_on() {
        assert_eq!(
            ParserOptions::default(),
            ParserOptions {
                enable_experimental_functions: false,
                experimental_duration_expr: false,
                enable_extended_range_selectors: false,
                enable_binop_fill_modifiers: false,
            }
        );
        let all = ParserOptions::all();
        assert!(
            all.enable_experimental_functions
                && all.experimental_duration_expr
                && all.enable_extended_range_selectors
                && all.enable_binop_fill_modifiers
        );
    }

    /// `docs/feature-flags.md` is the reviewer's table; a flag renamed
    /// here and not there fails this.
    #[test]
    fn the_doc_lists_every_flag_under_its_names() {
        let doc = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../docs/feature-flags.md"
        ))
        .expect("docs/feature-flags.md");
        for f in FEATURE_FLAGS {
            for name in [f.flag, f.upstream_option, f.option] {
                assert!(
                    doc.contains(&format!("`{name}`")),
                    "{name} is not in the doc"
                );
            }
        }
    }

    #[test]
    fn with_flag_sets_exactly_the_named_gate() {
        let count = |o: ParserOptions| {
            [
                o.enable_experimental_functions,
                o.experimental_duration_expr,
                o.enable_extended_range_selectors,
                o.enable_binop_fill_modifiers,
            ]
            .iter()
            .filter(|on| **on)
            .count()
        };
        for f in FEATURE_FLAGS {
            let one = ParserOptions::default().with_flag(f.flag, true).unwrap();
            assert_eq!(count(one), 1, "{}", f.flag);
            assert_eq!(one.with_flag(f.flag, false), Some(ParserOptions::default()));
            let all_but = ParserOptions::all().with_flag(f.flag, false).unwrap();
            assert_eq!(count(all_but), 3, "{}", f.flag);
            assert_ne!(one, all_but);
        }
        assert_eq!(ParserOptions::default().with_flag("nope", true), None);
    }
}
