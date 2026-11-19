use cfgrammar::yacc::YaccKind;
use lrlex::CTLexerBuilder;
use lrpar::CTParserBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A build script that emits no `rerun-if-changed` is rerun by Cargo
    // on *any* change inside the package, so editing a source file or a
    // test regenerates the grammar and the lexer too. Bound the rerun to
    // the files that actually drive codegen; `build.rs` itself is always
    // tracked by Cargo.
    //
    // These have to stay exhaustive: emitting even one `rerun-if-changed`
    // turns off the rerun-on-any-change fallback, so a file left out here
    // stops being watched altogether.
    println!("cargo:rerun-if-changed=src/grammar.y");
    println!("cargo:rerun-if-changed=src/lexer.l");

    // The PromQL grammar carries the same inherent shift/reduce
    // conflicts as upstream's goyacc grammar: one per binary operator
    // resolved by %left/%right precedence, plus a handful from the
    // postfix [range]/[range:step]/offset/@ productions. The grammar
    // uses `%expect` to opt in to that count; grmtools then accepts
    // those conflicts as designed.
    //
    // The grammar is built once, on its own, so that both lexers can
    // be generated against the same token ids: `grammar.y` has a
    // `start` rule dispatching on an injected START_* pseudo-token
    // (mirroring upstream's `parseGenerated`), and each parse mode
    // pairs it with the lexer for that mode.
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    // `CTParserBuilder::build` skips codegen when `grammar.y.rs` is newer than
    // `src/grammar.y`, which would leave an already-patched file behind if only
    // `build.rs` changed.
    let _ = std::fs::remove_file(out_dir.join("grammar.y.rs"));

    let ctp = CTParserBuilder::<lrlex::DefaultLexerTypes<u32>>::new()
        .yacckind(YaccKind::Grmtools)
        // Upstream goyacc stops at the first syntax error rather than synthesizing
        // token repairs; skipping CPCT+ avoids multi-millisecond repair searches on
        // malformed inputs whose repaired AST `parse_generated` discards anyway.
        .recoverer(lrpar::RecoveryKind::None)
        .grammar_in_src_dir("grammar.y")?
        // The emitted grammar mirrors upstream's, which is ambiguous
        // by design — operator precedence and the matrix/subquery [..]
        // productions produce a known set of shift/reduce conflicts
        // resolved by the `%left`/`%right` declarations. grmtools
        // surfaces them as warnings under this flag; goyacc accepts
        // them silently.
        .error_on_conflicts(false)
        .build()?;

    patch_generated_grammar(&out_dir.join("grammar.y.rs"))?;

    // `lexer.l` — ordinary PromQL expressions. `series.l` — series
    // descriptions (upstream's `seriesDesc` lexer mode). Neither
    // covers the whole token set on its own, and neither has a rule
    // for the START_* pseudo-tokens (those are injected in
    // `parser.rs`, never lexed), so both directions of "missing" are
    // expected. `lexer.l`'s BRACES start condition mirrors Prometheus's
    // `braceOpen` flag: a keyword is only a keyword outside `{...}`.
    // Fixing that in the grammar instead would not survive, since
    // `grammar.y` is regenerated from the vendored upstream file.
    let mut expr_tokens = ctp.token_map().clone();
    for (alias, token) in [
        ("BRACE_STRING", "STRING"),
        ("BRACE_IDENT", "IDENT"),
        ("BRACE_NEQ", "NEQ"),
        ("BRACE_NEQ_REGEX", "NEQ_REGEX"),
        ("BRACE_EQL_REGEX", "EQL_REGEX"),
        ("BRACE_EQ", "EQ"),
        ("BRACE_COMMA", "COMMA"),
    ] {
        let id = *expr_tokens
            .get(token)
            .ok_or_else(|| format!("grammar.y has no token {token} to alias {alias} onto"))?;
        expr_tokens.insert(alias.to_string(), id);
    }
    // `allow_missing_tokens_in_parser` below has to stay on for the
    // generated grammar's sake, and it also swallows a BRACE_* rule
    // whose alias was forgotten: lrlex drops the rule and the first
    // sign is a lex error inside `{...}` at runtime. This is the only
    // place that knows both the rule names and the alias table, so the
    // check belongs here.
    let lexer_l = std::fs::read_to_string(std::path::Path::new("src").join("lexer.l"))?;
    let def = <lrlex::LRNonStreamingLexerDef<lrlex::DefaultLexerTypes<u32>> as lrlex::LexerDef<
        lrlex::DefaultLexerTypes<u32>,
    >>::from_str(&lexer_l)
    .map_err(|e| format!("lexer.l does not parse: {e:?}"))?;
    for rule in lrlex::LexerDef::iter_rules(&def) {
        if let Some(name) = rule.name() {
            if name.starts_with("BRACE_") && !expr_tokens.contains_key(name) {
                return Err(format!("lexer.l rule {name} has no alias in build.rs").into());
            }
        }
    }
    CTLexerBuilder::<lrlex::DefaultLexerTypes<u32>>::new()
        .rule_ids_map(expr_tokens)
        .allow_missing_terms_in_lexer(true)
        .allow_missing_tokens_in_parser(true)
        .lexer_in_src_dir("lexer.l")?
        .build()?;

    // `series.l` needs the same token to be produced from more than one
    // start condition (`IDENT` as a metric name and as a label name;
    // `LBRACE` before and after the metric name). lrlex requires rule
    // names to be unique within a file, but nothing stops two names
    // mapping to the same token id — so the extra spellings are aliased
    // here rather than distorting the grammar.
    let mut series_tokens = ctp.token_map().clone();
    for (alias, token) in [
        ("IDENT_LABEL", "IDENT"),
        ("STALE", "IDENT"),
        ("LBRACE_AFTER_NAME", "LBRACE"),
    ] {
        let id = *series_tokens
            .get(token)
            .ok_or_else(|| format!("grammar.y has no token {token} to alias {alias} onto"))?;
        series_tokens.insert(alias.to_string(), id);
    }
    CTLexerBuilder::<lrlex::DefaultLexerTypes<u32>>::new()
        .rule_ids_map(series_tokens)
        .allow_missing_terms_in_lexer(true)
        .allow_missing_tokens_in_parser(true)
        .lexer_in_src_dir("series.l")?
        .build()?;

    // The START_* pseudo-tokens are never lexed, so `parser.rs` needs
    // their ids to prepend one to the lexeme stream. Upstream reads
    // them straight off the goyacc-generated constants; we write the
    // equivalent out here.
    let mut consts = String::from(
        "// Generated by build.rs from grammar.y's token map. Token ids\n\
         // for the START_* pseudo-tokens injected by `parser.rs`, and\n\
         // for the keywords it demotes to IDENT by lookahead.\n",
    );
    for token in [
        "START_EXPRESSION",
        "START_METRIC",
        "START_METRIC_SELECTOR",
        "START_SERIES_DESCRIPTION",
        "FILL",
        "FILL_LEFT",
        "FILL_RIGHT",
        "IDENT",
    ] {
        let id = ctp
            .token_map()
            .get(token)
            .ok_or_else(|| format!("grammar.y has no {token} token"))?;
        consts.push_str(&format!("pub const {token}: u32 = {id};\n"));
    }
    std::fs::write(out_dir.join("start_tokens.rs"), consts)?;

    Ok(())
}

// `lrpar` emits `_reconstitute(__GRM_DATA, __STABLE_DATA)` and a heap `vec![...]`
// of 248 action wrappers inside `parse()`, deserializing ~94 KiB of bincode tables
// on every parse call. Cache the deserialized tables once via `crate::grammar::cached_tables`
// and borrow the action wrappers from a stack slice.
fn patch_generated_grammar(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let generated = std::fs::read_to_string(path)?;
    const RECONSTITUTE_OLD: &str =
        "let (grm, stable) = ::lrpar::ctbuilder::_reconstitute(__GRM_DATA, __STABLE_DATA);";
    const RECONSTITUTE_NEW: &str = "let (grm, stable) = crate::grammar::cached_tables(|| ::lrpar::ctbuilder::_reconstitute(__GRM_DATA, __STABLE_DATA));";
    if !generated.contains(RECONSTITUTE_OLD) {
        return Err("expected lrpar _reconstitute call in generated grammar.y.rs".into());
    }
    let patched = generated.replacen(RECONSTITUTE_OLD, RECONSTITUTE_NEW, 1);

    const VEC_TYPE_OLD: &str = "let actions: ::std::vec::Vec<&dyn Fn(";
    const VEC_TYPE_NEW: &str = "let actions: &[&dyn Fn(";
    const VEC_INIT_OLD: &str = "-> __GtActionsKind<'input>> = ::std::vec![";
    const VEC_INIT_NEW: &str = "-> __GtActionsKind<'input>] = &[";
    const BUILDER_OLD: &str = "::lrpar::RTParserBuilder::new(&grm, &stable)";
    const BUILDER_NEW: &str = "::lrpar::RTParserBuilder::new(grm, stable)";
    const CALL_OLD: &str = ".parse_actions(lexer, &actions, p)";
    const CALL_NEW: &str = ".parse_actions(lexer, actions, p)";
    if !patched.contains(VEC_TYPE_OLD)
        || !patched.contains(VEC_INIT_OLD)
        || !patched.contains(BUILDER_OLD)
        || !patched.contains(CALL_OLD)
    {
        return Err("expected lrpar actions vec in generated grammar.y.rs".into());
    }
    let patched = patched
        .replacen(VEC_TYPE_OLD, VEC_TYPE_NEW, 1)
        .replacen(VEC_INIT_OLD, VEC_INIT_NEW, 1)
        .replacen(BUILDER_OLD, BUILDER_NEW, 1)
        .replacen(CALL_OLD, CALL_NEW, 1);
    std::fs::write(path, patched)?;
    Ok(())
}
