# Repository Guidelines

## Project Structure & Module Organization

This repo is a Rust workspace monorepo. Core runtime crates live under `crates/{abi,kernel,runtime,guest}`; platform crates are under `crates/control-plane/*`, `crates/io/*`, `crates/io/core`, `crates/io/durability`, and `crates/runtime/adaptors/*`; Rust SDK lives in `crates/sdk/rust`; system modules live under `modules/*` (control-plane reconcile logic); CLI is at `crates/cli`; examples (contracts/workflows/state snapshots) are in `examples/`.

## Build, Test, and Development Commands

Run commands from repo root:
- `cargo check --workspace --all-targets`
- `cargo fmt --all`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --all-targets`

Use `cargo check -p <crate>` / `cargo test -p <crate>` for package-scoped default-feature iteration while developing a focused change. Treat the workspace commands above as the repo-wide baseline mirrored by CI, and keep any non-default feature verification package-scoped and explicit.

For WASM modules, build any module crate with `--target wasm32-unknown-unknown`, e.g. `cargo build --release --target wasm32-unknown-unknown -p selium-module-control-plane`.

## Coding Style & Naming Conventions

Rust 2024 edition is used across the workspace; prefer default rustfmt output and keep clippy clean. Use standard Rust naming (`snake_case` for functions/modules, `CamelCase` for types). Workspace members follow `selium-<domain>-<component>` naming.

All public interfaces must be documented with Rust doc comments. Write repository prose and code naming in International English. Keep comments and doc examples short, factual, and directly tied to the crate API.

Code files have a soft limit of 500 lines. If a file grows beyond that, split it into focused submodules instead of extending a single large source file.

## Testing Guidelines

Unit tests live with each crate. For cross-crate changes, ensure workspace checks/tests pass and document any runtime environment assumptions in your PR.

## Commit & Pull Request Guidelines

Recent commits use short, descriptive, imperative summaries (optionally with a scope). Keep commit messages concise and focused on one change set. For PRs, include a clear summary, rationale, and the exact `cargo` commands you ran; call out affected modules and any runtime requirements (e.g., wasm builds or `selium-runtime` setup).
