#!/usr/bin/env bash
# Spec-scenario traceability check.
#
# The OpenSpec requirements are the contract; a test that no longer exists (or
# a requirement no test ever exercised) should be visible, not implied. This
# script parses every scenario heading in `openspec/specs/**/spec.md`, collects
# the scenarios that tests claim, collects reviewed waivers, and reports any
# scenario with neither.
#
# Annotation format (one scenario per line, anywhere in a Rust test source):
#
#     // scenario: <capability>/<requirement-slug>/<scenario-slug>
#
# The id is the capability directory name, then the requirement and scenario
# headings slugged (lowercase; every run of non-alphanumeric characters becomes
# a single `-`; leading/trailing `-` trimmed). For example:
#
#     ### Requirement: Host-authoritative admission
#     #### Scenario: Authority failure fails closed
#     -> discovery-registration/host-authoritative-admission/authority-failure-fails-closed
#
# A test may carry several annotations; the scenario is covered when any source
# carries it. Prefer annotating the test that *actually asserts* the outcome,
# not the test binary as a whole.
#
# Waiver format (checked-in, reviewed): a Markdown table row in
# `openspec/traceability-waivers.md`
#
#     | <scenario-id> | <reviewer> | <reason the scenario cannot be tested here> |
#
# A waiver with no reason fails the check. An annotation or waiver naming a
# scenario that does not exist fails the check as drift.
#
# Usage:
#   scripts/check-traceability.sh            # strict: fail on any uncovered scenario
#   scripts/check-traceability.sh --report   # report only (exit 0), for adoption
#   scripts/check-traceability.sh --all      # list every uncovered scenario
#
# Written for bash 3.2 (macOS `/bin/bash`).

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

SPECS_DIR="openspec/specs"
WAIVERS_FILE="openspec/traceability-waivers.md"

REPORT_ONLY=0
SHOW_ALL=0

while [ $# -gt 0 ]; do
  case "$1" in
    --report) REPORT_ONLY=1 ;;
    --all) SHOW_ALL=1 ;;
    -h | --help)
      sed -n '2,42p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "check-traceability: unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

if [ ! -d "$SPECS_DIR" ]; then
  echo "check-traceability: FAIL: $SPECS_DIR not found" >&2
  exit 1
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/selium-trace-XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

KNOWN="$TMP/known"
ANNOTATED="$TMP/annotated"
WAIVED="$TMP/waived"

: >"$KNOWN"
: >"$ANNOTATED"
: >"$WAIVED"

# ---------------------------------------------------------------------------
# Every scenario id in the main specs.
# ---------------------------------------------------------------------------
for spec in "$SPECS_DIR"/*/spec.md; do
  [ -f "$spec" ] || continue
  capability="$(basename "$(dirname "$spec")")"
  awk -v capability="$capability" '
    function slug(text,    out, i, c) {
      out = ""
      text = tolower(text)
      for (i = 1; i <= length(text); i++) {
        c = substr(text, i, 1)
        if (c ~ /[a-z0-9]/) {
          out = out c
        } else if (out != "" && substr(out, length(out), 1) != "-") {
          out = out "-"
        }
      }
      sub(/-$/, "", out)
      return out
    }
    /^### Requirement: / {
      requirement = substr($0, length("### Requirement: ") + 1)
      next
    }
    /^#### Scenario: / {
      scenario = substr($0, length("#### Scenario: ") + 1)
      if (requirement == "") next
      print capability "/" slug(requirement) "/" slug(scenario)
    }
  ' "$spec" >>"$KNOWN"
done
sort -u -o "$KNOWN" "$KNOWN"

# ---------------------------------------------------------------------------
# Test annotations: `// scenario: <id>` lines in Rust sources.
# ---------------------------------------------------------------------------
grep -rh --include='*.rs' --exclude-dir=target --exclude-dir=vendor \
  -E '^[[:space:]]*// scenario: [^[:space:]]+' \
  crates guests integration 2>/dev/null |
  sed -E 's#^[[:space:]]*// scenario: ##; s/[[:space:]]+$//' |
  sed '/^$/d' |
  sort -u >"$ANNOTATED"

# ---------------------------------------------------------------------------
# Reviewed waivers: a Markdown table row `| id | reviewer | reason |`.
# ---------------------------------------------------------------------------
if [ -f "$WAIVERS_FILE" ]; then
  awk -F'|' '
    /^[[:space:]]*\|/ {
      id = $2; reviewer = $3; reason = $4
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", id)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", reviewer)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", reason)
      if (id == "Scenario" || id ~ /^-+$/) next
      if (id == "") next
      if (reason == "") {
        printf "check-traceability: FAIL: waiver for %s has no reason\n", id > "/dev/stderr"
        exit 3
      }
      print id
    }
  ' "$WAIVERS_FILE" | sort -u >"$WAIVED"
  waiver_status="${PIPESTATUS[0]}"
  if [ "$waiver_status" -ne 0 ]; then
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
# Drift: names that do not exist in the specs.
# ---------------------------------------------------------------------------
DRIFT=0
while IFS= read -r id; do
  [ -n "$id" ] || continue
  if ! grep -qxF "$id" "$KNOWN"; then
    echo "check-traceability: DRIFT: annotation names unknown scenario: $id" >&2
    DRIFT=1
  fi
done <"$ANNOTATED"

while IFS= read -r id; do
  [ -n "$id" ] || continue
  if ! grep -qxF "$id" "$KNOWN"; then
    echo "check-traceability: DRIFT: waiver names unknown scenario: $id" >&2
    DRIFT=1
  fi
done <"$WAIVED"

# ---------------------------------------------------------------------------
# Coverage.
# ---------------------------------------------------------------------------
COVERED="$TMP/covered"
cat "$ANNOTATED" "$WAIVED" | sort -u >"$COVERED"
comm -23 "$KNOWN" "$COVERED" >"$TMP/missing"

known_count="$(wc -l <"$KNOWN" | tr -d ' ')"
covered_count="$(comm -12 "$KNOWN" "$COVERED" | wc -l | tr -d ' ')"
annotated_count="$(comm -12 "$KNOWN" "$ANNOTATED" | wc -l | tr -d ' ')"
waived_count="$(comm -12 "$KNOWN" "$WAIVED" | wc -l | tr -d ' ')"
missing_count="$(wc -l <"$TMP/missing" | tr -d ' ')"

if [ "$missing_count" -gt 0 ]; then
  # Per-capability summary, so the report points at the next unit of work.
  echo "check-traceability: uncovered scenarios by capability:"
  cut -d/ -f1 "$TMP/missing" | sort | uniq -c | sort -rn |
    while read -r count capability; do
      printf '  %4d  %s\n' "$count" "$capability"
    done

  if [ "$SHOW_ALL" = "1" ] || [ "$REPORT_ONLY" = "1" ]; then
    echo "check-traceability: uncovered scenarios:"
    sed 's/^/  /' "$TMP/missing"
  else
    echo "check-traceability: (pass --all to list every uncovered scenario)"
  fi
fi

echo "check-traceability: $known_count scenarios, $annotated_count annotated, $waived_count waived, $missing_count uncovered"

if [ "$DRIFT" -ne 0 ]; then
  echo "check-traceability: FAIL (drift)"
  exit 1
fi

if [ "$missing_count" -gt 0 ]; then
  if [ "$REPORT_ONLY" = "1" ]; then
    echo "check-traceability: REPORT ($missing_count uncovered)"
    exit 0
  fi
  echo "check-traceability: FAIL ($missing_count uncovered)"
  exit 1
fi

echo "check-traceability: PASS"
