//! Developer-invoked sync tool for the files we vendor verbatim from
//! prometheus/prometheus.
//!
//! Per `docs/parser-sync.md` this tool:
//!
//! - `check`: verifies that every vendored file matches the sha256
//!   recorded in its set's `MANIFEST.toml`. Runs in CI to catch
//!   accidental edits to files that are meant to be verbatim copies.
//! - `pull --target <sha-or-tag>`: fetches the upstream files at the
//!   target revision, diffs them against the currently-vendored copies,
//!   and writes a Markdown report with a change summary. Mutates only
//!   the vendored files and `sync-meta.toml` — never commits, branches,
//!   or pushes.
//!
//! # Vendored sets
//!
//! There are two, listed in [`VENDOR_SETS`], and they track upstream
//! independently:
//!
//! - `parser` — the Go sources our parser is ported from. Translated
//!   line by line into Rust, so a bump here means human work.
//! - `promqltest` — Prometheus's own `.test` corpus. Data, replayed
//!   verbatim; a bump here means regenerating the conformance baseline.
//!
//! `check` covers every set. `pull` takes one at a time, because
//! adopting an upstream change is a per-set decision with different
//! follow-up work in each case.
//!
//! Structural classification (Green / Yellow / Red per the design doc)
//! is a follow-up. Today the tool gives an honest "what changed, pass
//! to a human" surface rather than attempting mechanical apply of
//! token-table / AST-field additions. The report it produces is the
//! starting point for manual translation.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

mod emitter;
mod sidecar;
mod yacc;

const UPSTREAM_OWNER: &str = "prometheus";
const UPSTREAM_REPO: &str = "prometheus";

/// One directory of files copied verbatim from prometheus/prometheus,
/// with its own manifest and its own pinned revision.
struct VendorSet {
    /// Selector for `--set`.
    name: &'static str,
    /// Where the copies live, relative to the repo root. Holds
    /// `MANIFEST.toml` and `UPSTREAM.md` alongside them.
    vendor_dir: &'static str,
    /// Where this set's pinned revision is recorded.
    sync_meta: &'static str,
    /// The directory these files come from upstream.
    upstream_subdir: &'static str,
    /// The filenames, listed rather than globbed from the upstream
    /// tree: adopting a newly-added upstream file should be a decision
    /// someone makes, not something a sync quietly pulls in.
    files: &'static [&'static str],
}

impl VendorSet {
    fn dir(&self, root: &Path) -> PathBuf {
        root.join(self.vendor_dir)
    }

    fn manifest_path(&self, root: &Path) -> PathBuf {
        self.dir(root).join("MANIFEST.toml")
    }

    fn sync_meta_path(&self, root: &Path) -> PathBuf {
        root.join(self.sync_meta)
    }
}

/// The Go sources our parser is a port of. Keep in sync with
/// `promql-parser/upstream/UPSTREAM.md`.
const PARSER: VendorSet = VendorSet {
    name: "parser",
    vendor_dir: "promql-parser/upstream",
    sync_meta: "promql-parser/sync-meta.toml",
    upstream_subdir: "promql/parser",
    files: &[
        "generated_parser.y",
        "lex.go",
        "ast.go",
        "parse.go",
        "functions.go",
        "printer.go",
    ],
};

/// Prometheus's promqltest corpus. Keep in sync with
/// `promql-conformance/testdata/prometheus/UPSTREAM.md`.
const PROMQLTEST: VendorSet = VendorSet {
    name: "promqltest",
    vendor_dir: "promql-conformance/testdata/prometheus",
    sync_meta: "promql-conformance/sync-meta.toml",
    upstream_subdir: "promql/promqltest/testdata",
    files: &[
        "aggregators.test",
        "at_modifier.test",
        "bitwise.test",
        "collision.test",
        "duration_expression.test",
        "extended_vectors.test",
        "fill-modifier.test",
        "functions.test",
        "histograms.test",
        "info.test",
        "limit.test",
        "literals.test",
        "name_label_dropping.test",
        "native_histograms.test",
        "operators.test",
        "range_queries.test",
        "selectors.test",
        "staleness.test",
        "start_timestamps.test",
        "subquery.test",
        "trig_functions.test",
        "type_and_unit.test",
        "unquoted_utf8_names.test",
    ],
};

const VENDOR_SETS: &[&VendorSet] = &[&PARSER, &PROMQLTEST];

/// Resolve `--set`. `None` means every set, which is what `check`
/// wants and what CI runs.
fn resolve_sets(name: Option<&str>) -> Result<Vec<&'static VendorSet>> {
    let Some(name) = name else {
        return Ok(VENDOR_SETS.to_vec());
    };
    VENDOR_SETS
        .iter()
        .find(|s| s.name == name)
        .map(|s| vec![*s])
        .with_context(|| {
            let known: Vec<_> = VENDOR_SETS.iter().map(|s| s.name).collect();
            format!("unknown --set {name:?}; known sets: {}", known.join(", "))
        })
}

#[derive(Debug, Parser)]
#[command(
    name = "promql-sync",
    about = "Upstream sync tool for files vendored from prometheus/prometheus"
)]
struct Cli {
    /// Repository root. Defaults to the current working directory.
    #[arg(long, value_name = "PATH", global = true, default_value = ".")]
    root: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Verify vendored-file hashes match MANIFEST.toml. No network
    /// access, no mutations. Intended for CI.
    Check {
        /// Limit to one vendored set. Defaults to checking all of them.
        #[arg(long, value_name = "NAME")]
        set: Option<String>,
    },

    /// Fetch upstream at the given git SHA (or tag) and compare with
    /// the currently-vendored files. When not in dry-run mode,
    /// rewrites the vendored files, their `MANIFEST.toml`, and the
    /// set's `sync-meta.toml`; otherwise just reports.
    Pull {
        /// Which vendored set to pull. One at a time: adopting an
        /// upstream change means different follow-up work per set.
        #[arg(long, value_name = "NAME", default_value = "parser")]
        set: String,

        /// Target git SHA or tag.
        #[arg(long, value_name = "SHA_OR_TAG")]
        target: String,

        /// Show the plan but mutate nothing.
        #[arg(long)]
        dry_run: bool,

        /// Report output path. Defaults to
        /// `target/promql-sync-report.md`.
        #[arg(long, value_name = "PATH")]
        report: Option<PathBuf>,
    },

    /// Regenerate `src/grammar.y` from the vendored upstream grammar
    /// (`upstream/generated_parser.y`) plus the `grammar-actions.toml`
    /// + `grammar-tokens.toml` sidecars.
    GenerateGrammar {
        /// Show the generated grammar on stdout instead of writing it.
        #[arg(long)]
        dry_run: bool,

        /// Output path. Defaults to `src/grammar.y` under the crate
        /// root.
        #[arg(long, value_name = "PATH")]
        out: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::from(2)
        }
    }
}

fn run(cli: Cli) -> Result<ExitCode> {
    let root = cli.root.clone();

    match cli.command {
        Command::Check { set } => check_integrity(&root, &resolve_sets(set.as_deref())?),
        Command::Pull {
            set,
            target,
            dry_run,
            report,
        } => {
            let sets = resolve_sets(Some(&set))?;
            let report_path =
                report.unwrap_or_else(|| PathBuf::from("target/promql-sync-report.md"));
            sync(&root, sets[0], &target, &report_path, dry_run)
        }
        // Deliberately bound to the parser set rather than `--set`:
        // there is only one grammar, and pointing this at the test
        // corpus could only ever destroy `src/grammar.y`.
        Command::GenerateGrammar { dry_run, out } => {
            let crate_dir = root.join("promql-parser");
            let out_path = out.unwrap_or_else(|| crate_dir.join("src/grammar.y"));
            generate_grammar(&crate_dir, &PARSER.dir(&root), &out_path, dry_run)
        }
    }
}

fn generate_grammar(
    crate_dir: &Path,
    upstream_dir: &Path,
    out_path: &Path,
    dry_run: bool,
) -> Result<ExitCode> {
    let actions_toml = crate_dir.join("grammar-actions.toml");
    let tokens_toml = crate_dir.join("grammar-tokens.toml");
    let sc = sidecar::Sidecar::load(&actions_toml, &tokens_toml).context("load sidecars")?;
    let upstream_y = upstream_dir.join("generated_parser.y");
    let result = emitter::emit_from_paths(&upstream_y, &sc).context("emit grammar.y")?;

    if dry_run {
        println!("{}", result.text);
    } else {
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent).ok();
        }
        fs::write(out_path, &result.text)
            .with_context(|| format!("write {}", out_path.display()))?;
        eprintln!("wrote {} ({} bytes)", out_path.display(), result.text.len());
    }

    eprintln!(
        "emitted: {} rules; skipped as unreachable/filtered: {}",
        result.rules_emitted.len(),
        result.rules_skipped_unreachable.len(),
    );
    if !result.unmapped.is_empty() {
        eprintln!(
            "\n{} unmapped alt(s) — placeholder emitted:",
            result.unmapped.len()
        );
        for u in &result.unmapped {
            eprintln!("  {}: {}", u.rule, u.upstream_signature);
        }
        eprintln!(
            "\nFill these in `{}` to replace the placeholders.",
            crate_dir.join("grammar-actions.toml").display()
        );
    }

    Ok(ExitCode::SUCCESS)
}

// ---------------- --check mode ----------------

fn check_integrity(root: &Path, sets: &[&VendorSet]) -> Result<ExitCode> {
    let mut drifted = 0usize;

    for set in sets {
        let manifest_path = set.manifest_path(root);
        let manifest = Manifest::load(&manifest_path)
            .with_context(|| format!("load manifest at {}", manifest_path.display()))?;
        let dir = set.dir(root);

        println!("[{}] {}", set.name, set.vendor_dir);
        for file in set.files {
            let disk = sha256_file(&dir.join(file)).with_context(|| format!("hash {file}"))?;
            match manifest.file_hashes.get(*file) {
                Some(recorded) if recorded == &disk => {
                    println!("  ok     {file}");
                }
                Some(recorded) => {
                    println!("  drift  {file}\n         disk={disk}\n         toml={recorded}");
                    drifted += 1;
                }
                None => {
                    println!("  absent {file} (not in MANIFEST)");
                    drifted += 1;
                }
            }
        }

        // A file present on disk but missing from the set's list is
        // invisible to every check above, so say so: either upstream
        // added it and `files` needs updating, or it does not belong.
        for stray in stray_files(&dir, set.files)? {
            println!("  stray  {stray} (on disk, not in the {} set)", set.name);
            drifted += 1;
        }
    }

    if drifted == 0 {
        Ok(ExitCode::SUCCESS)
    } else {
        eprintln!(
            "\n{drifted} vendored file(s) have drifted. Either revert the edit or run \
             `promql-sync pull --set <name> --target <sha>` to adopt the change."
        );
        Ok(ExitCode::from(1))
    }
}

/// Vendored-directory entries that the set does not list. Ignores the
/// bookkeeping files we write ourselves — `go.mod` among them, which is
/// a stub keeping the vendored Go out of any parent module's build.
fn stray_files(dir: &Path, known: &[&str]) -> Result<Vec<String>> {
    const OURS: &[&str] = &[
        "MANIFEST.toml",
        "UPSTREAM.md",
        "SUPPORTED.toml",
        "UNSUPPORTED.md",
        "go.mod",
    ];
    let mut stray = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        if !known.contains(&name.as_str()) && !OURS.contains(&name.as_str()) {
            stray.push(name);
        }
    }
    stray.sort();
    Ok(stray)
}

// ---------------- --target mode ----------------

#[derive(Debug)]
struct FileDelta {
    name: String,
    old_hash: Option<String>,
    new_hash: String,
    bytes_old: usize,
    bytes_new: usize,
}

fn sync(
    root: &Path,
    set: &VendorSet,
    target: &str,
    report_path: &Path,
    dry_run: bool,
) -> Result<ExitCode> {
    let upstream_dir = set.dir(root);
    let manifest_path = set.manifest_path(root);
    let sync_meta_path = set.sync_meta_path(root);
    let subdir = set.upstream_subdir;

    let client = reqwest::blocking::Client::builder()
        .user_agent("promql-sync/0.1")
        .build()?;
    let prior = Manifest::load(&manifest_path).unwrap_or_default();

    let mut deltas = Vec::new();
    let mut new_manifest = Manifest::default();
    for file in set.files {
        let url = format!(
            "https://raw.githubusercontent.com/{UPSTREAM_OWNER}/{UPSTREAM_REPO}/{target}/{subdir}/{file}"
        );
        let body = client
            .get(&url)
            .send()
            .with_context(|| format!("GET {url}"))?
            .error_for_status()
            .with_context(|| format!("status of {url}"))?
            .bytes()
            .with_context(|| format!("read {url}"))?;
        let new_hash = sha256_bytes(&body);
        let old_path = upstream_dir.join(file);
        let old_bytes_len = fs::read(&old_path).map(|b| b.len()).unwrap_or(0);
        let old_hash = prior.file_hashes.get(*file).cloned();
        deltas.push(FileDelta {
            name: (*file).to_string(),
            old_hash: old_hash.clone(),
            new_hash: new_hash.clone(),
            bytes_old: old_bytes_len,
            bytes_new: body.len(),
        });
        new_manifest
            .file_hashes
            .insert((*file).to_string(), new_hash);
        if !dry_run {
            fs::write(&old_path, &body).with_context(|| format!("write {file}"))?;
        }
    }

    // Report.
    fs::create_dir_all(report_path.parent().unwrap_or_else(|| Path::new(".")))?;
    let mut md = String::new();
    md.push_str(&format!(
        "# promql-sync report — set `{}`, target `{target}`\n\n",
        set.name
    ));
    md.push_str(if dry_run {
        "Mode: `--dry-run`. No files were modified.\n\n"
    } else {
        "Mode: apply. Vendored files have been rewritten in place; review `git diff` before committing.\n\n"
    });
    md.push_str("| file | prior sha256 | new sha256 | bytes |\n");
    md.push_str("| --- | --- | --- | --- |\n");
    let mut any_changed = false;
    for d in &deltas {
        let changed = d.old_hash.as_deref() != Some(d.new_hash.as_str());
        if changed {
            any_changed = true;
        }
        md.push_str(&format!(
            "| `{}` | `{}` | `{}`{} | {} → {} |\n",
            d.name,
            d.old_hash.as_deref().unwrap_or("-"),
            d.new_hash,
            if changed { " ⚠️" } else { "" },
            d.bytes_old,
            d.bytes_new,
        ));
    }
    md.push_str("\n## Next steps\n\n");
    if !any_changed {
        md.push_str("No vendored files changed vs the prior manifest.\n");
    } else if set.name == PROMQLTEST.name {
        md.push_str(&format!(
            "1. Inspect the diff: `git diff {}`.\n\
             2. Regenerate the conformance baseline — upstream test content moved, so the old one no longer describes reality:\n   \
                `PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest`\n\
             3. Review the baseline diff. It is the record of what changed upstream: cases that started passing, cases that started failing, cases that vanished.\n\
             4. Commit the vendored bump and the regenerated baseline together.\n\n\
             If upstream added or removed a file, update the `promqltest` entry in `VENDOR_SETS` first — this sync only fetches the files it already knows about.\n",
            set.vendor_dir,
        ));
    } else {
        md.push_str(&format!(
            "1. Inspect the diff: `git diff {}`.\n\
             2. Re-run `cargo test -p promql-parser` to see which translation steps are needed.\n\
             3. Translate any new grammar rules / action bodies / lexer states / AST fields into the corresponding Rust modules.\n\
             4. Update `sync-meta.toml` with the translation hashes (manual for now; structural classification is a follow-up).\n\
             5. Commit the vendored-file bump alongside the Rust translations in a single reviewable change.\n\n\
             Note: structural Green/Yellow/Red classification (design doc) is not yet implemented. This produces a byte-level diff; human handles the translation call.\n",
            set.vendor_dir,
        ));
    }
    fs::write(report_path, &md)
        .with_context(|| format!("write report {}", report_path.display()))?;
    println!("wrote report to {}", report_path.display());

    if !dry_run && any_changed {
        new_manifest.save(&manifest_path)?;
        update_sync_meta(&sync_meta_path, target)?;
        println!(
            "updated MANIFEST and sync-meta.toml for the {} set",
            set.name
        );
    }

    Ok(ExitCode::SUCCESS)
}

// ---------------- MANIFEST + sync-meta I/O ----------------

#[derive(Debug, Default, Serialize, Deserialize)]
struct Manifest {
    #[serde(default)]
    file_hashes: BTreeMap<String, String>,
}

impl Manifest {
    fn load(path: &Path) -> Result<Self> {
        let body = fs::read_to_string(path)?;
        let m: Manifest = toml::from_str(&body).context("parse MANIFEST.toml")?;
        Ok(m)
    }

    fn save(&self, path: &Path) -> Result<()> {
        // Reproduce byte-for-byte what a human would have written, so a
        // sync that changes no hashes leaves no diff. The `=` column is
        // padded for the same reason the committed manifests pad it:
        // the hashes are the part worth scanning.
        let width = self
            .file_hashes
            .keys()
            .map(|k| k.len() + 2)
            .max()
            .unwrap_or(0);
        let mut out = String::new();
        out.push_str(
            "# SHA-256 of every vendored upstream file.\n\
             #\n\
             # The promql-sync tool verifies these hashes on startup and after a sync,\n\
             # catching accidental manual edits to files that are meant to be verbatim\n\
             # copies from prometheus/prometheus. A mismatch is not fatal on its own,\n\
             # but the sync tool refuses to proceed until the discrepancy is resolved\n\
             # (either revert the local edit or re-vendor).\n\n\
             [file_hashes]\n",
        );
        for (k, v) in &self.file_hashes {
            let quoted = format!("\"{k}\"");
            out.push_str(&format!(
                "{quoted:<width$} = \"sha256:{}\"\n",
                v.trim_start_matches("sha256:")
            ));
        }
        fs::write(path, out)?;
        Ok(())
    }
}

fn update_sync_meta(path: &Path, target: &str) -> Result<()> {
    // Minimal update: rewrite pinned_upstream_sha and last_sync_date,
    // preserve existing translations tables. To avoid a fragile TOML
    // edit, read-mutate-write via the toml crate's Value type.
    let existing = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut doc: toml::Value =
        toml::from_str(&existing).with_context(|| format!("parse {}", path.display()))?;
    if let Some(table) = doc.as_table_mut() {
        table.insert(
            "pinned_upstream_sha".into(),
            toml::Value::String(target.into()),
        );
        table.insert(
            "last_sync_date".into(),
            toml::Value::String(chrono_date_utc()),
        );
    } else {
        bail!("sync-meta.toml is not a table");
    }
    let out = toml::to_string(&doc)?;
    fs::write(path, out)?;
    Ok(())
}

/// RFC-3339 date component in UTC, without pulling in the chrono
/// workspace dep. Produces e.g. `2026-04-21`.
fn chrono_date_utc() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Civil-date conversion from POSIX seconds (Howard Hinnant's
    // algorithm, stable-Rust safe).
    let days = (secs / 86_400) as i64;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146_096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

// ---------------- hashing ----------------

fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    Ok(sha256_bytes(&bytes))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    format!("sha256:{:x}", h.finalize())
}
