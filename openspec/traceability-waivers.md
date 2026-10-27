# Reviewed traceability waivers

Every scenario annotated in `openspec/specs/**/spec.md` must be covered by a
test annotation (`// scenario: <id>`) or by a row here. A waiver records why the
scenario cannot be tested in this project, and needs a reviewer and a non-empty
reason; `scripts/check-traceability.sh` fails otherwise.

| Scenario | Reviewer | Reason |
| --- | --- | --- |
| shared-page-fastpath/optional-per-os-wait-word-primitives-stage-2/platform-without-stage-2 | @pete | The Stage-1 fallback is selected at compile time for a platform without wait-word primitives; the gate runs on Linux, so only the macOS native subset exercises it. |
