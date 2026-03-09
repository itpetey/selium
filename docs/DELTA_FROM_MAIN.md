# Delta From `../main`

This document is the repo-grounded delta between the previous project in
`../main` and the current `newarch` workspace. It is written as a
regression-first report: the focus is on what moved, what replaced it, and what
still lacks direct user-visible parity.

## Baseline

- Baseline repo: sibling checkout at `../main`
- Current repo: `newarch`
- Validation run for both trees: `cargo test --workspace --all-targets`
- Result: both workspaces pass their current test suites

That means the comparison below is healthy-to-healthy, not a migration from a
broken legacy baseline.

## Executive Summary

- The workspace grew from 19 members to 34 members and split core concerns into
  control-plane, runtime adaptors, storage, consensus, tables, SDK, and module
  crates.
- The old channel- and protocol-specific capability model was replaced by
  queue/shared-memory primitives, protocol-neutral network operations, and
  runtime-managed storage.
- The old guest development model (`selium-userland`, singleton lookup,
  Flatbuffers `#[schema]`) was replaced by `selium-guest`, `.selium` IDL, and
  generated `bindings.rs`.
- The old pattern of booting a runtime and layering external runtime modules on
  top of it (`remote-client`, `switchboard`, `atlas`) was replaced by a
  first-party CLI, daemon protocol, control-plane state, and runtime agent
  reconciliation.
- The largest user-visible gaps are not in low-level test health; they are in
  removed or not-yet-replaced example workflows and in the current
  cross-deployment routing boundary.

## Workspace and Package Delta

## Previous workspace shape

- `system/{abi,kernel,runtime,userland,userland/macros}`
- `subsystem/{filesystem-store,messaging,net-hyper,net-quinn,wasmtime}`
- examples centered on messaging/network demos
- a single `tests/request-reply` integration crate

## Current workspace shape

- `crates/{abi,kernel,guest,cli}`
- `crates/control-plane/{api,protocol,runtime,scheduler}`
- `crates/io/{consensus,core,durability,tables}`
- `crates/runtime/{service,network,storage,adaptors/*}`
- `crates/sdk/rust`
- `modules/{control-plane,io-demo}`
- examples expanded around contracts, daemon workflows, and guest-local flows

## Crate-by-crate translation

| Previous crate | Current crate(s) | Status |
| --- | --- | --- |
| `system/abi` | `crates/abi` | retained, expanded |
| `system/kernel` | `crates/kernel` | retained, reorganized |
| `system/runtime` | `crates/runtime/service` | retained, expanded |
| `system/userland` | `crates/guest` | replaced |
| `system/userland/macros` | `crates/guest/macros` | replaced |
| `subsystem/messaging` | `crates/kernel` + `crates/io/core` | replaced |
| `subsystem/net-hyper` + `subsystem/net-quinn` | `crates/runtime/network` | replaced |
| `subsystem/filesystem-store` | `crates/runtime/storage` + `crates/io/durability` | replaced |
| `subsystem/wasmtime` | `crates/runtime/adaptors/wasmtime-adaptor` + runtime Wasmtime internals | replaced |
| no equivalent in `../main` | `crates/control-plane/*` | new |
| no equivalent in `../main` | `crates/io/consensus`, `crates/io/tables` | new |
| no equivalent in `../main` | `crates/sdk/rust` | new |
| no equivalent in `../main` | `crates/cli` | new |

## Public API and Capability Delta

## Capability mapping

| `../main` capability | `newarch` status | Regression classification |
| --- | --- | --- |
| `SessionLifecycle` | still present | no gap |
| `ChannelLifecycle` | replaced by `QueueLifecycle` | replacement |
| `ChannelReader` | replaced by `QueueReader` | replacement |
| `ChannelWriter` | replaced by `QueueWriter` | replacement |
| `ProcessLifecycle` | still present | no gap |
| `NetQuicBind`, `NetHttpBind` | folded into `NetworkLifecycle` | replacement |
| `NetQuicAccept`, `NetHttpAccept` | folded into `NetworkAccept` | replacement |
| `NetQuicConnect`, `NetHttpConnect` | folded into `NetworkConnect` | replacement |
| `NetQuicRead`, `NetHttpRead` | replaced by `NetworkStreamRead` or `NetworkRpcServer` / `NetworkRpcClient` paths | replacement |
| `NetQuicWrite`, `NetHttpWrite` | replaced by `NetworkStreamWrite` or `NetworkRpcServer` / `NetworkRpcClient` paths | replacement |
| `NetTlsServerConfig`, `NetTlsClientConfig` | moved to runtime ingress/egress config, no guest capability | migration required |
| `SingletonRegistry`, `SingletonLookup` | removed | user-visible gap for old dependency-injection model |
| `TimeRead` | still present | no gap |
| no legacy equivalent | `SharedMemory` | new |
| no legacy equivalent | `Storage*` capabilities | new |

## Guest API delta

| `../main` | `newarch` | Status |
| --- | --- | --- |
| `selium-userland` | `selium-guest` | replacement |
| `selium-userland-macros` | `selium-guest-macros` | replacement |
| `Context` + singleton lookup | no direct equivalent | user-visible gap |
| `#[schema]` and Flatbuffers-first payload workflow | `.selium` IDL + generated `bindings.rs` | replacement |
| `logging` guest module | no direct equivalent surface in `selium-guest` | migration required |
| `net` | `network` | replacement |
| `io` | `io`, `queue`, `shm`, `storage`, `durability` | expansion |
| generated `fbs` modules in examples | generated `bindings.rs` | replacement |

## Runtime / CLI Delta

## Removed previous operational pattern

- boot a host runtime directly
- control it from outside through an externally built `remote-client` module
- compose higher-level behavior via external runtime modules such as
  `switchboard` and `atlas`

## Added current operational pattern

- first-party `selium` CLI in the workspace
- daemon-backed runtime protocol over QUIC
- control-plane state mutation and query
- scheduler and agent reconciliation loop
- explicit runtime ingress bindings, egress profiles, log stores, and blob
  stores
- adaptor-aware module specs

## New user-visible commands

- `deploy`
- `connect`
- `scale`
- `observe`
- `replay`
- `nodes`
- `start`
- `stop`
- `list`
- `agent`
- `idl compile`
- `idl publish`

## Workflow Regression Matrix

The matrix below focuses on end-user-visible behavior, not internal design.

| Legacy workflow | `newarch` status | Notes |
| --- | --- | --- |
| Start/stop a guest module | supported | now exposed through daemon + `selium start/stop` |
| Target a specific node for launch | supported | covered by ignored CLI topology tests |
| Run a two-node cluster | supported | covered by ignored cluster harness tests |
| Build a request/reply guest flow | supported | `examples/rpc-echo-service` |
| Build staged pipeline flows | supported | `examples/pipeline-transform`, `examples/scatter-gather` |
| Build guest-supervised child process flows | supported | `examples/process-supervisor` |
| Use HTTP/QUIC guest networking | supported | `examples/network-http-rpc`, `examples/network-quic-stream` |
| Use runtime-managed durable replay/checkpoints | supported | storage, durability, replay CLI, stateful example |
| Use singleton dependency injection from guest code | not supported | no direct singleton capability in current ABI |
| Use first-party load-balancer example | no direct equivalent | user-visible gap |
| Use first-party HTTPS load-balancer example | no direct equivalent | user-visible gap |
| Use first-party WAF example | no direct equivalent | user-visible gap |
| Use first-party log-analyser example | no direct equivalent | user-visible gap |
| Use orchestrator example with the old shape | partial replacement | split across supervisor + typed-entrypoint flows |

## Example-by-Example Delta

| `../main` example | Closest `newarch` replacement | Status |
| --- | --- | --- |
| `examples/echo` | `examples/rpc-echo-service` | replacement |
| `examples/echo-no-deps` | `examples/rpc-echo-service` | replacement |
| `examples/data-pipeline` | `examples/pipeline-transform` | replacement |
| `examples/orchestrator` | `examples/process-supervisor` and `examples/typed-entrypoints` | partial replacement |
| `examples/rest-api` | `examples/network-http-rpc` | partial replacement |
| `examples/load-balancer` | none | user-visible gap |
| `examples/load-balancer-https` | none | user-visible gap |
| `examples/log-analyser` | none | user-visible gap |
| `examples/waf` | none | user-visible gap |

## Discovery and Operational Boundary

The current examples are contract-first and demonstrate `idl publish`,
`deploy`, `connect`, and `agent --once` for control-plane-managed workloads.
For that workflow, the public discovery model is tenant-qualified:

- workloads are identified as `tenant/namespace/workload`
- contract-defined event endpoints are identified as
  `tenant/namespace/workload#endpoint`
- pipeline `connect` operations bind those public identities rather than node,
  transport, queue, channel, or replica details

Control-plane pipeline edges now activate runtime-managed event routes. If the
source and target workloads are colocated, delivery stays on-node. If they are
placed on different nodes, the runtime bridges the event route across daemons
without changing the guest-facing endpoint names or event semantics.

The remaining user-facing boundary is operational rather than application
binding. `selium list --node ...`, `start`, and `stop --replica-key ...`
expose replica placement and replica keys for administrative control only; they
are not the supported application-facing discovery or bind identifiers.

Current verification covers public discovery filtering, denied event-endpoint
binds, operational running-process discovery behaviour, local route forwarding,
remote daemon frame delivery, and a dedicated ignored multi-node CLI
integration that proves payload delivery across a full
`deploy` → `connect` → `agent --once` workflow end-to-end.

## `todo!()` / `unimplemented!()` Inventory

Search performed: `rg -n "\\b(todo!|unimplemented!)\\s*\\(" .`

## Current findings

| Path | Item | Impact |
| --- | --- | --- |
| `crates/kernel/src/services/session_service.rs:125` | `todo!()` in `Session::authenticate` | session payload authentication is not implemented; the method currently logs success/failure intent but does not return a real authentication result |

## No current findings

- `unimplemented!()` occurrences: none found

## Documentation and Discoverability Gaps

| Item | Status |
| --- | --- |
| `README.md` linked to `docs/ORIENTATION.md` but the file was missing | fixed by adding `docs/ORIENTATION.md` |
| there was no checked-in full delta report against `../main` | fixed by adding this document |
| control-plane example docs can be read as stronger than current runtime routing support | keep the current boundary explicit in docs and reviews |

## Test and Validation Delta

## `../main`

- passed `cargo test --workspace --all-targets`
- had additional messaging-focused tests under `subsystem/messaging/tests`
- had a standalone `tests/request-reply` workspace member on disk, but it was
  not enabled in the workspace manifest

## `newarch`

- passed `cargo test --workspace --all-targets`
- has broader unit coverage around ABI, control-plane, durability, queue,
  network, storage, guest macros, and runtime parsing
- has ignored integration tests covering:
  - two-node cluster boot
  - one-runtime-per-node topology
  - targeted CLI launch
  - guest-side I/O smoke
  - control-plane topology deploy/connect/agent-once remote delivery

## Priority Follow-Ups

1. Decide whether singleton-style guest dependency injection is intentionally
   gone or needs a first-party replacement.
2. Decide which removed first-party examples need direct replacements:
   load-balancer, HTTPS load-balancer, WAF, and log analysis are the clearest
   user-visible gaps.
3. Implement real session authentication in `Session::authenticate` instead of
   leaving `todo!()`.
