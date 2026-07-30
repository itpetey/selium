# Selium

Selium is a software-defined cloud infrastructure platform: application
stacks are composed entirely in code — typed I/O channels, strict
capabilities, and zero external configuration — and executed as WebAssembly
guests on a minimal host.

The design intention, in one sentence: **developers should build and own the
whole stack without touching traditional infrastructure or networking.**
See [`DESIGN-INTENT.md`](DESIGN-INTENT.md) for the non-negotiables, the
rejected alternatives, and the invariants that guide contributions.

## What runs today

The **spine** and **discovery control-plane** of the platform work and are
continuously tested:

- Real WASM guests (`wasm32-unknown-unknown`, no WASI) executing on wasmtiny
- The hostcall ABI (`selium-abi`, rkyv-encoded): capability-gated shared
  memory alloc/attach, host queues, storage, process lifecycle, activity and
  guest logs
- Shared-memory channels (`selium-shm`): lock-free ring buffers with typed
  pub/sub, RPC, and live-table overlays (`selium-wire`)
- Structured guest logging over a shared-memory channel, drained by the host
- Config-driven bootstrap of WASM system guests with per-process capability
  grants
- Discovery system guest (`crates/guests/discovery`): Tier-1 runtime feed
  registration, Tier-2 guest-driven URI resolution over shm RPC, and
  revocation on process exit

The proof is two integration tests:

**Spine** — a real guest (`crates/guests/spine-demo`) is compiled to WASM,
bootstrapped by the runtime, creates shared-memory channels, completes a
typed pub/sub round trip, and streams structured logs back to the host:

```sh
cargo build --target wasm32-unknown-unknown -p selium-spine-demo
cargo test -p selium-runtime --test spine -- --ignored
```

**Discovery** — the discovery system guest and a probe fixture guest
(`crates/guests/discovery-probe`) are compiled to WASM and bootstrapped
together: the runtime injects discovery wiring (feed ring + RPC listener),
both guests reach readiness, Tier-1 region registration events flow through
the feed, Tier-2 RPC resolution works between two real WASM guests, and URI
revocation fires on process exit:

```sh
cargo build --target wasm32-unknown-unknown -p selium-discovery -p selium-discovery-probe
cargo test -p selium-runtime --test discovery -- --ignored
```

## Deferred (explicitly not working yet)

- Networking (TCP/UDP bridges, QUIC, external clients)
- AAA / tenant enforcement (capability grants exist; tenant/URI
  enforcement is not implemented)
- Multi-host clustering, scheduling, supervision, external API
- Durable storage (current logs/blob stores are in-memory)
- Guest wake/wait for channel I/O (currently spin/poll based)

Frozen crates retained in-tree for later increments: `crates/core/quic`,
`crates/guests/{bridge,cluster,scheduler,supervisor,external-api}` — these
are not in the workspace and do not build.

## Repository layout

| Crate | Role |
| --- | --- |
| `crates/core/abi` | Canonical host↔guest contract: capabilities, scopes, hostcall payloads, framing |
| `crates/core/encoding` | FlatBuffers message encoding, log record types, schema bindings |
| `crates/core/memory` | `RegionMapping`/`RegionProvider` shared-memory abstraction |
| `crates/core/shm` | Shared-memory ring channels (`Channel`, `RingBuf`, blocking/non-blocking readers/writers) |
| `crates/core/wire` | Transport-agnostic framing + pub/sub, RPC, live-table patterns |
| `crates/core/kernel` | Primitive host resources: shared memory, network, storage, processes, activity, metering |
| `crates/core/runtime` | Wasmtiny-backed execution, capability enforcement, system-guest bootstrap, hostcall dispatch |
| `crates/core/guest` | The guest SDK: hostcalls, async reactor, typed handles, tracing integration |
| `crates/core/guest/macros` | `#[entrypoint]`, `#[pattern_interface]`, `#[schema]` proc macros |
| `crates/guests/discovery` | Discovery system guest (URI registration/resolution store + wiring) |
| `crates/guests/discovery-probe` | Discovery probe test fixture guest (exercises Tier-2 RPC against discovery) |
| `crates/guests/spine-demo` | Golden-path demo guest used by the spine test |

## Building

Requires stable Rust and the `wasm32-unknown-unknown` target:

```sh
rustup target add wasm32-unknown-unknown
cargo build --workspace
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

**Note:** the workspace depends on a sibling checkout of
[`wasmtiny`](https://github.com/itpetey/wasmtiny) via a path patch
(`../../wasmtiny`). Both repos must sit side-by-side for the build to
resolve.

## Contributing

See `AGENTS.md` for rules: stable Rust, edition 2024, no WASI, `tracing`
for logging, International English, and the pre-commit gate (fmt, clippy,
tests, wasm32 guest builds, spine test).

## Licence

MPL v2 (see `LICENCE`)
