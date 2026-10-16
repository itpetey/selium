# Spec Delta

## MODIFIED Requirements

### Requirement: Discovery-Enabled Bootstrap

`selium-runtime` SHALL support `start_discovery` in `RuntimeConfig`, creating the Tier-1 feed ring and RPC listener, injecting tagged `WasmValue` entrypoint arguments (feed region id and listener handle into the discovery guest; listener handle into other guests with empty argument lists), and spawning declared guests without gating their startup on another guest's readiness.

#### Scenario: Discovery wiring uses tagged argument encoding

- **WHEN** the runtime injects discovery arguments into a guest descriptor
- **THEN** `decode_wasm_arguments` decodes every injected value without error, for all possible u64 handle values

#### Scenario: Readiness is per-guest

- **WHEN** a bootstrapped guest has not registered its serving route
- **THEN** the runtime SHALL still start it and SHALL NOT roll back the bootstrap on that basis, because readiness is no longer a per-guest gate

#### Scenario: Application guest receives discovery handle

- **WHEN** the runtime bootstraps an application guest
- **THEN** the guest's entrypoint SHALL receive the discovery `shared_id`
  as the first entrypoint argument slot, and the entrypoint macro's
  generated glue SHALL construct the `Context` from it for
  `Context`-leading entrypoints (guests do not call `Context::from_raw`
  themselves)

## ADDED Requirements

### Requirement: Host-Authored Protocol Handler Registration

The runtime SHALL publish a protocol handler registration for each guest
descriptor that declares handlers, on the Tier-1 feed, so the registration is
host-authored and cannot be forged by a guest. Handler resolution SHALL be a
discovery lookup; the runtime SHALL NOT keep a host-side handler registry.

#### Scenario: Handler registration is host-authored

- **WHEN** the runtime spawns a guest whose descriptor declares a protocol handler
- **THEN** it SHALL publish that handler's registration on the Tier-1 feed

#### Scenario: Handler resolution is a discovery lookup

- **WHEN** a serve-side guest resolves the handler for a protocol scheme
- **THEN** it SHALL obtain the owning process from discovery, not from a runtime-maintained map

#### Scenario: Guests cannot forge a handler

- **WHEN** a guest registers a protocol handler route it was not assigned
- **THEN** the registration SHALL NOT be treated as host-authored authority
