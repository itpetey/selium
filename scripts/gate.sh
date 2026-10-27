#!/usr/bin/env bash
# The verification gate: one reproducible entry point for the whole
# verification surface, run by CI and by a developer with the same command.
#
# What it runs (see docs/verification-gate.md):
#
#   1. cargo fmt --all --check
#   2. cargo check --workspace --all-targets
#   3. cargo clippy --workspace --all-targets -- -D warnings
#   4. cargo test --workspace --all-targets
#   5. cargo test --workspace --doc
#   6. scripts/build-all.sh            (every wasm prerequisite, including the
#                                       nightly atomics net-demo and mt-demo)
#   7. the atomics module's shared-memory declaration
#   8. scripts/check-wasm-patches.sh
#   9. scripts/check-traceability.sh   (spec scenario -> test/waiver)
#  10. stage-2 wait-word conformance    (Linux only; the module is cfg'd away
#                                       elsewhere)
#  11. scripts/flake-check.sh          (the full ignored pack, retried:
#                                       every `#[ignore]`d spine plus the
#                                       failure-injection binaries
#                                       `failure_*`; the soak and crash lanes
#                                       are excluded here and run under --soak)
#  12. the known-unknowns register, printed alongside the result
#
# Every step runs even when an earlier one fails, and the exit status is
# non-zero if any step failed, so one run reports every regression it can. The
# soak and crash-consistency lanes are too slow for every push: they run under
# `--soak` (the scheduled CI lane) and are skipped here otherwise.
#
# Usage:
#   scripts/gate.sh                 # the full gate
#   scripts/gate.sh --quick         # everything except the ignored pack
#   scripts/gate.sh --soak          # full gate plus the soak/crash lanes
#   scripts/gate.sh --soak-only     # just the soak/crash lanes (nightly job)
#   scripts/gate.sh --repeat 3      # run the ignored pack 3 times, flag flakes
#   scripts/gate.sh --native        # do not re-exec into the Linux container
#   scripts/gate.sh --list          # list the steps and what they run
#
# On a non-Linux host the gate re-execs itself inside the Linux container CI
# uses (the modules and std behaviour the gate exercises are Linux-only), so a
# developer and CI run the same commands. `--native` opts out for a quick local
# subset; the result then does not assert the Linux-only components.
#
# Environment:
#   GATE_REPEAT      ignored-pack runs for flake detection (default 1)
#   RUST_IMAGE       container image tag (default: rust:latest)
#   GATE_IN_CONTAINER set by the re-exec; do not set by hand
#   GATE_NATIVE      1 is the same as --native
#   CARGO_TARGET_DIR forwarded into the container run

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

QUICK=0
SOAK=0
SOAK_ONLY=0
LIST=0
NATIVE=0
REPEAT="${GATE_REPEAT:-1}"
[ "${GATE_NATIVE:-0}" = "1" ] && NATIVE=1

while [ $# -gt 0 ]; do
  case "$1" in
    --quick) QUICK=1 ;;
    --soak) SOAK=1 ;;
    --soak-only) SOAK_ONLY=1 ;;
    --repeat)
      shift
      [ $# -gt 0 ] || {
        echo "gate.sh: --repeat needs a run count" >&2
        exit 2
      }
      REPEAT="$1"
      ;;
    --native) NATIVE=1 ;;
    --list) LIST=1 ;;
    -h | --help)
      sed -n '2,45p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "gate.sh: unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

# ---------------------------------------------------------------------------
# The Linux re-exec. The gate asserts Linux behaviour (futex wait words, std
# lock destruction differences), so a non-Linux host runs the same command in
# the same container `scripts/test.sh`/`scripts/clippy.sh` use.
# ---------------------------------------------------------------------------
if [ "$NATIVE" = "0" ] && [ -z "${GATE_IN_CONTAINER:-}" ] && [ "$(uname -s)" != "Linux" ]; then
  if ! docker info >/dev/null 2>&1; then
    if [ "$(uname -s)" = "Darwin" ]; then
      echo "gate.sh: starting Docker Desktop..."
      open -a Docker
      for _ in $(seq 1 60); do
        docker info >/dev/null 2>&1 && break
        sleep 2
      done
    fi
    if ! docker info >/dev/null 2>&1; then
      echo "gate.sh: Docker is required to run the Linux gate on $(uname -s)." >&2
      echo "gate.sh: start Docker, or run with --native for the host subset." >&2
      exit 1
    fi
  fi

  GRANDPARENT="$(cd "$ROOT/../.." && pwd -P)"
  PARENT_NAME="$(basename "$(cd "$ROOT/.." && pwd -P)")"
  REPO_NAME="$(basename "$ROOT")"
  CONTAINER_WS="/work/$PARENT_NAME/$REPO_NAME"
  RUST_IMAGE="${RUST_IMAGE:-rust:latest}"

  TTY_ARGS=()
  [ -t 1 ] && TTY_ARGS=(-t)

  echo "gate.sh: re-exec into Linux container ($RUST_IMAGE) at $CONTAINER_WS"
  exec docker run --rm \
    ${TTY_ARGS[@]+"${TTY_ARGS[@]}"} \
    -v "$GRANDPARENT":/work \
    -v selium-clippy-rustup:/usr/local/rustup \
    -v selium-clippy-cargo:/usr/local/cargo \
    -v selium-gate-target:/target \
    -e WS="$CONTAINER_WS" \
    -e GATE_IN_CONTAINER=1 \
    -e CARGO_INCREMENTAL=0 \
    -e CARGO_TARGET_DIR=/target \
    -e CARGO_TERM_COLOR=always \
    "$RUST_IMAGE" \
    bash -c '
      set -euo pipefail
      apt-get update -qq
      apt-get install -y -qq clang cmake >/dev/null
      cd "$WS"
      rustup component add clippy rustfmt >/dev/null
      rustup target add wasm32-unknown-unknown >/dev/null
      rustup toolchain install nightly --component rust-src --target wasm32-unknown-unknown >/dev/null
      command -v wasm-tools >/dev/null 2>&1 ||
        cargo install wasm-tools --locked --no-default-features --features print
      exec scripts/gate.sh "$@"
    ' bash "$@"
fi

export CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}"

# ---------------------------------------------------------------------------
# Steps. Each is a function so `--list` and the failure report stay in sync.
# ---------------------------------------------------------------------------
step_fmt() { cargo fmt --all --check; }
step_check() { cargo check --workspace --all-targets; }
step_clippy() { cargo clippy --workspace --all-targets -- -D warnings; }
step_tests() { cargo test --workspace --all-targets; }
step_doc_tests() { cargo test --workspace --doc; }
step_wasm_prereqs() { scripts/build-all.sh; }
step_shared_memory() {
  # The atomics net-demo the fast-path test loads must declare memory 0 shared
  # with a maximum, or the wasmtiny validator rejects its atomic instructions
  # and the fast path can never activate. This is the assertion the former
  # `shared-page-fastpath-guest` job made; it is kept in the aggregate gate.
  local target_root="${CARGO_TARGET_DIR:-$ROOT/target}"
  local artifact="$target_root/wasm32-unknown-unknown/debug/selium_net_demo_atomics.wasm"
  [ -f "$artifact" ] || {
    echo "missing atomics artifact: $artifact" >&2
    return 1
  }
  wasm-tools print "$artifact" | grep -E '\(memory .*shared'
}
step_wasm_patches() { scripts/check-wasm-patches.sh; }
step_traceability() { scripts/check-traceability.sh; }
step_stage2() {
  cargo test -p selium-kernel --features stage2-wait-words --test wait_notify &&
    cargo test -p selium-kernel -p selium-runtime --features selium-runtime/stage2-wait-words
}
step_ignored_pack() { scripts/flake-check.sh --runs "$REPEAT"; }
step_soak() {
  cargo test -p selium-runtime --test soak -- --ignored &&
    cargo test -p selium-runtime --test crash_consistency -- --ignored
}

STEP_NAMES=()
STEP_FUNCS=()

add_step() {
  STEP_NAMES+=("$1")
  STEP_FUNCS+=("$2")
}

if [ "$SOAK_ONLY" = "1" ]; then
  add_step "soak and crash-consistency lanes" step_soak
else
  add_step "formatting" step_fmt
  add_step "cargo check" step_check
  add_step "clippy" step_clippy
  add_step "workspace tests" step_tests
  add_step "doc tests" step_doc_tests
  add_step "wasm prerequisites" step_wasm_prereqs
  add_step "atomics shared-memory declaration" step_shared_memory
  add_step "wasm patch guard" step_wasm_patches
  add_step "spec traceability" step_traceability
  add_step "stage-2 wait-word conformance" step_stage2
  if [ "$QUICK" = "0" ]; then
    add_step "ignored pack (flake-checked)" step_ignored_pack
  fi
  if [ "$SOAK" = "1" ]; then
    add_step "soak and crash-consistency lanes" step_soak
  fi
fi

if [ "$LIST" = "1" ]; then
  for index in "${!STEP_NAMES[@]}"; do
    printf '%2d. %s\n' "$((index + 1))" "${STEP_NAMES[$index]}"
  done
  exit 0
fi

# ---------------------------------------------------------------------------
# Run every step, collecting failures. `set -e` is deliberately off here.
# ---------------------------------------------------------------------------
# Linux-only steps are skipped (and said to be skipped) on a native non-Linux
# run, because their modules do not exist there. This is the only place the
# gate narrows itself, and only with --native.
FAILED=()
SKIPPED=()
STARTED="$(date +%s)"

for index in "${!STEP_NAMES[@]}"; do
  name="${STEP_NAMES[$index]}"
  func="${STEP_FUNCS[$index]}"

  if [ "$name" = "stage-2 wait-word conformance" ] && [ "$(uname -s)" != "Linux" ]; then
    echo
    echo "==> $name: SKIPPED (Linux-only, native run on $(uname -s))"
    SKIPPED+=("$name")
    continue
  fi

  echo
  echo "==> $name"
  step_started="$(date +%s)"
  if "$func"; then
    echo "<== PASS: $name ($(( $(date +%s) - step_started ))s)"
  else
    echo "<== FAIL: $name ($(( $(date +%s) - step_started ))s)"
    FAILED+=("$name")
  fi
done

# ---------------------------------------------------------------------------
# The known-unknowns register is printed with the result: green means "green,
# with these caveats" (design D7).
# ---------------------------------------------------------------------------
echo
echo "=============================================================================="
if [ "${#FAILED[@]}" -eq 0 ]; then
  echo "gate: PASS ($(( $(date +%s) - STARTED ))s)"
else
  echo "gate: FAIL: ${#FAILED[@]} step(s) failed:"
  for name in "${FAILED[@]}"; do
    echo "  - $name"
  done
fi
if [ "${#SKIPPED[@]}" -gt 0 ]; then
  echo "gate: skipped: ${SKIPPED[*]}"
fi
echo "gate: open known-unknowns (what a green gate does not assert):"
if [ -f docs/known-unknowns.md ]; then
  sed -n 's/^## //p' docs/known-unknowns.md | sed 's/^/  - /'
else
  echo "  (docs/known-unknowns.md is missing; a green gate is unqualified)"
fi
echo "=============================================================================="

[ "${#FAILED[@]}" -eq 0 ]
