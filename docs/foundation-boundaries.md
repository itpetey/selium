# Foundation Boundaries

Selium arch3 now treats the host and guest foundation as five explicit crates with clean boundaries.

## Crate Roles

- `selium-abi`: canonical host/guest contract for capabilities, scopes, handles, hostcalls, completion states, and framing
- `selium-kernel`: primitive host-side resources such as shared memory, signalling, network, storage, process lifecycle, activity hooks, and metering hooks
- `selium-runtime`: Wasmtiny-backed execution, session and capability enforcement, generic config-driven bootstrap of system guests, and the host-side implementation of the guest execution bridge
- `selium-guest`: ergonomic guest SDK with typed handles, codecs, tracing support, and a messaging-pattern layer
- `selium-guest-macros`: procedural macros for guest entrypoints and generated interface metadata

## Design Rules

- Keep `selium-kernel` primitive. Do not move guest policy or orchestration logic into it.
- Keep `selium-runtime` generic. It bootstraps guests from descriptors and readiness rules rather than hard-coded guest names.
- Keep `selium-guest` pattern-oriented. Request/reply is one pattern among peers, not the privileged architectural default.
- Keep `selium-abi` stable and explicit. Host and guest crates meet there first.

## Layering

```text
selium-guest-macros
        |
        v
    selium-guest
        |
        v
     selium-abi
      /      \
     v        v
selium-kernel selium-runtime
                    ^
                    |
             guest-host contract
        \      /
         v    v
         wasmtiny
```

## System Guest Dependency Direction

System guests should depend on the guest SDK and macro layer. They may consume kernel-backed primitives and runtime bootstrap indirectly through those layers, but they should not grow bespoke host contracts outside `selium-abi`.
