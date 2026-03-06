# Examples

This directory is the home for sample contracts, state snapshots, and runnable workflows.

## Layout

- `idl/`: `.selium` contract examples.
- `state/`: sample state files used by workflow docs.
- `workflows/`: CLI-driven walkthroughs for common scenarios.
- `rust-sdk/`: standalone Rust examples using `selium-sdk-rust`.
- `modules/`: standalone guest module examples (e.g. `modules/user-echo`, `modules/user-io`).

## Migrated legacy examples

These are the first rewrites of examples previously under `selium/main/examples`:

- `echo` -> `idl/echo_v1.selium` + `workflows/echo.md`
- `data-pipeline` -> `idl/data_pipeline_v1.selium` + `workflows/data-pipeline.md`
- `orchestrator` -> `idl/orchestrator_v1.selium` + `workflows/orchestrator.md`

`idl/media_pipeline_v1.selium` is the original sample pipeline contract moved from repository root.
