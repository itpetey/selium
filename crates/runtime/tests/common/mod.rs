//! Shared helpers for the wasm-guest integration tests.
//!
//! The spine-style tests load prebuilt `wasm32-unknown-unknown` guest modules
//! from the target directory. Guest wasm is **not mixed-version safe**
//! against the runtime: the guest ABI uses rkyv enums whose variant indices
//! shift whenever a variant is inserted, so a wasm module built against older
//! sources produces silent, incomprehensible failures at runtime (a guest
//! whose hostcalls decode as the wrong variants — readiness timeouts, traps,
//! or parked entrypoints) instead of a build error.
//!
//! [`read_guest_wasm`] and [`read_guest_wasm_debug`] therefore check the
//! chosen artifact's mtime against the newest source file in the guest
//! crate's transitive path-dependency tree, and fail loudly with the exact
//! rebuild command when the artifact is older than its sources. The check is
//! an mtime heuristic (mirroring cargo's own staleness logic): it can only
//! false-positive into "please rebuild", never false-negative.

// Each integration test binary compiles this module independently and uses
// only the readers its guests need; per-binary, some readers are dead code.
#![allow(
    dead_code,
    reason = "shared test-support module compiled per test binary; readers are used across binaries"
)]

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    time::SystemTime,
};

/// Reads a guest's wasm module, preferring the release profile over debug
/// (release is the recommended build for the interpreted-wasm spine tests).
///
/// Panics with an actionable message when the module is missing or older
/// than its sources.
#[expect(
    clippy::panic,
    reason = "missing/stale build artifact is a hard test failure"
)]
pub fn read_guest_wasm(crate_name: &str, wasm_file: &str) -> Vec<u8> {
    let (bytes, path) = read_profile(target_dir(), "release", wasm_file)
        .or_else(|| read_profile(target_dir(), "debug", wasm_file))
        .unwrap_or_else(|| {
            panic!(
                "{crate_name} guest not found (looked for {wasm_file} in release and debug).\
                 \nBuild it first (release preferred):\
                 \n  cargo build --release --target wasm32-unknown-unknown -p {crate_name}"
            )
        });
    assert_fresh(crate_name, &path);
    bytes
}

/// Reads a guest's wasm module from the debug profile exactly.
///
/// For tests whose documented build recipe produces a debug artifact
/// (e.g. the nightly-atomics guest build); using the release-preferred
/// reader would let a plain release build shadow the special one.
///
/// Panics with an actionable message when the module is missing or older
/// than its sources.
#[expect(
    clippy::panic,
    reason = "missing/stale build artifact is a hard test failure"
)]
pub fn read_guest_wasm_debug(crate_name: &str, wasm_file: &str) -> Vec<u8> {
    let (bytes, path) = read_profile(target_dir(), "debug", wasm_file).unwrap_or_else(|| {
        panic!(
            "{crate_name} guest not found (looked for {wasm_file} in debug).\
                 \nBuild it first:\
                 \n  cargo build --target wasm32-unknown-unknown -p {crate_name}"
        )
    });
    assert_fresh(crate_name, &path);
    bytes
}

fn read_profile(target: PathBuf, profile: &str, wasm_file: &str) -> Option<(Vec<u8>, PathBuf)> {
    let path = target
        .join("wasm32-unknown-unknown")
        .join(profile)
        .join(wasm_file);
    std::fs::read(&path).ok().map(|bytes| (bytes, path))
}

/// Fails loudly when the wasm artifact predates the newest source file in
/// the guest crate's transitive path-dependency tree.
#[expect(
    clippy::panic,
    reason = "a stale guest artifact fails the test with an actionable message"
)]
fn assert_fresh(crate_name: &str, wasm_path: &Path) {
    let root = workspace_root();
    let Some(crate_dir) = resolve_crate_dir(&root, crate_name) else {
        // Cannot locate the sources to compare against; the missing-file
        // case is handled by the readers, so stay permissive here.
        return;
    };
    let (newest_mtime, newest_file) = newest_source_mtime(&root, &crate_dir);
    let Some(wasm_mtime) = std::fs::metadata(wasm_path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
    else {
        return;
    };
    if wasm_mtime < newest_mtime {
        let profile = if wasm_path.to_string_lossy().contains("release") {
            "--release "
        } else {
            ""
        };
        panic!(
            "stale guest artifact: {wasm} is older than the newest source file it was built \
             from ({newest_file}).\
             \nGuest wasm is NOT mixed-version safe against the runtime ABI (rkyv enum \
             variants shift when the ABI changes), so a stale artifact fails at runtime \
             with readiness timeouts or traps — not a build error.\
             \nRebuild it:\
             \n  cargo build {profile}--target wasm32-unknown-unknown -p {crate_name}",
            wasm = wasm_path.display(),
            newest_file = newest_file.display(),
        );
    }
}

fn workspace_root() -> PathBuf {
    let target = target_dir();
    // <root>/target -> <root>
    target.parent().map(Path::to_path_buf).unwrap_or(target)
}

fn target_dir() -> PathBuf {
    std::env::var("CARGO_TARGET_DIR")
        .unwrap_or_else(|_error| concat!(env!("CARGO_MANIFEST_DIR"), "/../../target").to_string())
        .into()
}

/// Locates a workspace crate's source directory from its package name by
/// scanning guest and crate manifests.
fn resolve_crate_dir(root: &Path, crate_name: &str) -> Option<PathBuf> {
    let mut candidates = Vec::new();
    for group in ["guests", "crates"] {
        let entries = std::fs::read_dir(root.join(group)).ok()?;
        for entry in entries.flatten() {
            candidates.push(entry.path());
            // Nested manifests (e.g. crates/guest/macros).
            if let Ok(nested) = std::fs::read_dir(entry.path().join("macros")) {
                candidates.extend(nested.flatten().map(|child| child.path()));
            }
        }
    }
    candidates.into_iter().find(|dir| {
        let manifest = std::fs::read_to_string(dir.join("Cargo.toml")).unwrap_or_default();
        manifest
            .lines()
            .any(|line| line.trim() == format!("name = \"{crate_name}\""))
    })
}

/// Newest source mtime across the crate dir, its transitive path
/// dependencies, and the workspace root manifest + lockfile.
fn newest_source_mtime(root: &Path, crate_dir: &Path) -> (SystemTime, PathBuf) {
    let workspace_paths = workspace_path_entries(root);
    let mut newest = (SystemTime::UNIX_EPOCH, PathBuf::new());
    consider(&mut newest, &root.join("Cargo.toml"));
    consider(&mut newest, &root.join("Cargo.lock"));

    let mut visited = HashSet::new();
    let mut stack = vec![crate_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        if !visited.insert(dir.clone()) {
            continue;
        }
        consider(&mut newest, &dir.join("Cargo.toml"));
        consider_tree(&mut newest, &dir.join("src"));
        let manifest = dir.join("Cargo.toml");
        for dep_dir in parse_dependency_dirs(&manifest, &workspace_paths) {
            stack.push(dep_dir);
        }
    }
    (newest.0, newest.1)
}

/// Records `(dependency name, path)` for every workspace-manifest line
/// carrying a local `path = "…"` (dependency tables and `[patch]` sections
/// alike: patched path sources such as `wasmtiny` and vendored crates also
/// invalidate guest builds).
fn workspace_path_entries(root: &Path) -> Vec<(String, PathBuf)> {
    let manifest = std::fs::read_to_string(root.join("Cargo.toml")).unwrap_or_default();
    manifest
        .lines()
        .filter_map(|line| {
            let name = line.split('=').next()?.trim().to_string();
            let path = extract_path_value(line, root)?;
            Some((name, path))
        })
        .collect()
}

/// Extracts local `path = "…"` targets from a manifest line, resolved
/// against the manifest's directory. Also resolves `name.workspace = true`
/// dependencies through the workspace path entries by name.
///
/// Only build-relevant sections are honoured: `[dependencies]`,
/// `[build-dependencies]`, `[workspace.dependencies]`, and `[patch.*]`.
/// Dev-dependencies (e.g. a crate's native test harness) never affect the
/// compiled guest wasm and would only produce false staleness positives.
fn parse_dependency_dirs(manifest: &Path, workspace_paths: &[(String, PathBuf)]) -> Vec<PathBuf> {
    let text = std::fs::read_to_string(manifest).unwrap_or_default();
    let manifest_dir = manifest.parent().map(Path::to_path_buf).unwrap_or_default();
    let mut dirs = Vec::new();
    let mut in_relevant_section = false;
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('[') {
            in_relevant_section = trimmed.starts_with("[dependencies")
                || trimmed.starts_with("[build-dependencies")
                || trimmed.starts_with("[workspace.dependencies")
                || trimmed.starts_with("[patch");
            continue;
        }
        if !in_relevant_section {
            continue;
        }
        if let Some(path) = extract_path_value(trimmed, &manifest_dir) {
            dirs.push(path);
            continue;
        }
        // `selium-foo.workspace = true` — resolve via the workspace path
        // entries, which are keyed by dependency name.
        if let Some(name) = trimmed.strip_suffix(".workspace = true")
            && let Some((_, path)) = workspace_paths
                .iter()
                .find(|(dep_name, _)| dep_name == name)
        {
            dirs.push(path.clone());
        }
    }
    dirs
}

fn extract_path_value(line: &str, base: &Path) -> Option<PathBuf> {
    let start = line.find("path = \"")? + "path = \"".len();
    let rest = line.get(start..)?;
    let end = rest.find('"')?;
    let relative = rest.get(..end)?;
    if relative.is_empty() {
        return None;
    }
    let path = base.join(relative);
    // Keep only existing directories: path entries may be conditional or
    // target-specific.
    path.is_dir().then_some(path)
}

fn consider(newest: &mut (SystemTime, PathBuf), path: &Path) {
    if let Ok(mtime) = std::fs::metadata(path).and_then(|metadata| metadata.modified())
        && mtime > newest.0
    {
        *newest = (mtime, path.to_path_buf());
    }
}

fn consider_tree(newest: &mut (SystemTime, PathBuf), dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            consider_tree(newest, &path);
        } else {
            consider(newest, &path);
        }
    }
}
