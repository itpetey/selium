---
name: land
description: >-
  Land the changes produced in this thread into the selium repository: run the
  project's pre-commit gate, commit, push to origin/main (the itpetey/selium
  fork), and verify the CI run for the landed commit. Invoke only when the user
  has explicitly requested landing - for example by choosing Land Changes or
  asking to land or merge the change - never for review, preparation, or
  running checks on their own.
disable-model-invocation: true
metadata:
  delta-action: land
---

# Land changes (selium)

Applies to the `selium` Rust workspace: the repository whose root contains
`Cargo.toml`, `AGENTS.md`, `DESIGN-INTENT.md`, and `openspec/`, including
agent-managed worktree checkouts of it. Invoking this skill is the landing
request: do not ask again whether to merge, and do not use it for review or
check-only work.

All commands run from the workspace root of the checkout being landed.

## 1. Establish the change set

```sh
git --no-optional-locks status --short --branch
git log --oneline origin/main..HEAD
```

The change set is every uncommitted or untracked file in this checkout plus any
local commits ahead of `origin/main`. If both are empty, stop and report that
there is nothing to land.

Rules:

- Land only this checkout. The user's primary checkout
  (`/Users/pete/projects/seliumlabs/selium`) may hold unrelated uncommitted
  work; never modify it, never push the `local` remote, and never stage files
  outside this worktree.
- Never push tags. A `v*` tag push triggers `publish.yml`, which publishes every
  crate to crates.io; releases are tagged deliberately, not by this skill.
- Never force-push and never rewrite commits already on `origin/main`.

## 2. Reconcile with origin/main

```sh
git fetch origin
git --no-optional-locks status --short --branch
```

If `origin/main` has moved, rebase this checkout's unpushed commits onto it:

```sh
GIT_EDITOR=true git rebase origin/main
```

Conflict preference (set by the user): resolve conflicts automatically when the
intended result is clear, preserving unrelated work from either side; stop when
the correct outcome is ambiguous. `pull.ff only` is configured for this
repository, so do not run `git pull`. If reconciliation would rewrite commits
that are already on `origin/main`, stop instead.

## 3. Run the project's pre-commit gate

`AGENTS.md` (CRITICAL IMPERATIVES, Pre-commit checks) requires all of the
following before any commit. Run them all and fix failures before continuing.
The first item is run with `cargo refmt` rather than `cargo fmt --all`: it is
the project owner's formatter (the `cargo-refmt` binary on PATH; `--help`
shows only `-h`/`-V`, so it has no check mode), it sorts items consistently in
Rust sources, and it wraps `cargo fmt`. The formatting *check* stays
`cargo fmt --all --check`, which is what CI runs.

```sh
cargo refmt                                              # AGENTS.md pre-commit 1 (owner's formatter, wraps cargo fmt)
cargo fmt --all --check                                  # the check CI runs (.github/workflows/ci.yml, "cargo fmt" step)
cargo clippy --workspace --all-targets -- -D warnings    # AGENTS.md pre-commit 2
cargo test --workspace --all-targets                     # AGENTS.md pre-commit 3
cargo build --target wasm32-unknown-unknown \
  -p selium-spine-demo -p selium-discovery               # AGENTS.md pre-commit 4
cargo test -p selium-runtime --test spine -- --ignored   # AGENTS.md pre-commit 5
scripts/check-wasm-patches.sh                            # AGENTS.md pre-commit 6
```

`scripts/check-wasm-patches.sh` needs `wasm-tools` on PATH
(`cargo install wasm-tools`); it fails with that instruction when missing.

CI runs on Linux and the repository documents that clippy can differ between
macOS and Linux (see the header of `scripts/clippy.sh`), so prefer the
CI-parity wrappers when the Docker daemon is up:

```sh
WASMTINY_DIR=/Users/pete/projects/seliumlabs/wasmtiny scripts/clippy.sh
WASMTINY_DIR=/Users/pete/projects/seliumlabs/wasmtiny scripts/test.sh
```

The scripts default to the CI invocation (`--workspace --all-targets`, plus
`-- -D warnings` for clippy) and mount the wasmtiny checkout named by
`WASMTINY_DIR` at `/work/wasmtiny`, because a worktree checkout has no sibling
`wasmtiny`. The first run seeds Docker volumes and takes several minutes; allow
a generous timeout. On macOS the scripts try to start Docker Desktop and exit
with an error when the daemon cannot be reached; fall back to the native
commands above in that case, and say so in the report.

When the change includes OpenSpec artifacts (files under
`openspec/changes/<name>/`), validate each one as well (`openspec/config.yaml`
defines the repository's spec-driven workflow):

```sh
openspec validate <change-name> --strict
```

## 4. Commit

Stage the change set explicitly (`git add <paths>`), not `git add -A`, so that
unrelated files are never swept in. Commit in the repository's plain style: a
short, capitalised, imperative subject (recent history, e.g. "Update CI to
ubuntu-latest-arm"), with a short body when the subject is not enough. This
repository requires no conventional-commit prefix, changelog entry, or
sign-off, and no attribution trailers appear in its history.

```sh
GIT_EDITOR=true git commit -m "<subject>" -m "<body>"
```

## 5. Push

```sh
git push origin main
```

`branch.main` tracks `origin/main`, so a plain `git push` is equivalent. Push
to `origin` (the `itpetey/selium` fork) only: never to `upstream`
(`seliumlabs/selium`), which is synced separately, and never with `--force`. If
the push is rejected, return to step 2 and rebase; do not force.

## 6. Verify the landed state

Record the pushed commit (`git rev-parse HEAD`) and confirm the remote ref:

```sh
git ls-remote origin refs/heads/main
```

Then verify the CI run for that exact commit. `gh` has no default repository
configured in this worktree, so always pass `-R itpetey/selium`:

```sh
gh run list -R itpetey/selium --branch main -L 10
gh run view <run-id> -R itpetey/selium --json status,conclusion,headSha,jobs
```

The landing gate is the `Test` workflow (`.github/workflows/ci.yml`) whose
`headSha` matches the pushed commit, with every job successful:

- Cargo checks
- Cargo tests
- Golden-path spine (WASM guest)
- Shared-page fast path (atomics guest)
- Shared-page fast path Stage 2 (per-OS wait-words)

Poll with `gh run view ... --json status,conclusion` at bounded intervals until
the run completes - typically minutes; do not stream one blocking watch. Also
check the other push-triggered workflows on `main` and report their outcomes:
`Audit` (`.github/workflows/audit.yml`, runs when `Cargo.toml` or `Cargo.lock`
changed) and `Draft the release` (`.github/workflows/release.yml`).

Notes:

- Pending, missing, or failing jobs are not success. Never report a landing as
  verified from a partial, pending, or stale run.
- `main`'s history includes commits whose `Test` run failed in `Cargo checks`
  (`cargo clippy`). If the run for the landed commit fails, first check whether
  the same job failed for the base commit (`gh run list -R itpetey/selium
  --branch main`; inspect the run whose `headSha` is the parent commit). A
  failure caused by the landed change is fixed forward with a new commit -
  never by rewriting pushed history. A failure that predates the change is
  reported explicitly as pre-existing, and the landing is reported with that
  caveat rather than as fully green.
- The user's primary checkout can fast-forward with `git pull --ff-only` in
  `~/projects/seliumlabs/selium` when they are ready; this skill does not
  update it.

## Stop conditions

Stop and report that the changes have **not landed**, naming the specific
blocker, when:

- the pre-commit gate fails and cannot be fixed within the change;
- a conflict's correct resolution is ambiguous, or reconciliation would require
  a history rewrite;
- the push is rejected and cannot be made fast-forward;
- verification cannot establish a passing `Test` run for the pushed commit.

Never force-push, rewrite pushed history, push tags, bypass a failing check,
touch the user's primary checkout, or claim success from an unverified run.
