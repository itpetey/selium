# Exploration: Remote CLI & Simplified Architecture

**Date:** 2026-03-22  
**Context:** CLI for remote orchestration of Selium hosts, guest-focused rather than host-implemented  
**Status:** Capture for future reference - architecture needs further exploration in greenfield repo

---

## Context

We explored redesigning the CLI for remote orchestration. The old CLI (`old/crates/cli/`) was tightly coupled to host internals, with commands like `deploy`, `connect`, `scale`, `observe`, `discover`, etc. that directly manipulated control plane state.

We wanted to:
- Make CLI host-focused (guest-focused in intent)
- Align with greenfield architecture from the archived OpenSpec change
- Simplify the overall system architecture

---

## Key Insights That Emerged

### 1. Guest-Focused CLI

The CLI should be a **native binary** that connects to hosts via QUIC, but it should not know about Selium's inner architecture. It expresses **intent** ("deploy X", "scale Y"), not implementation details.

```
CLI (native)  ----QUIC---->  selium-host
                                   |
                                   v
                             [guests...]
```

The CLI's job: express intent, receive results.  
The CLI's constraint: no knowledge of guests, channels, SOTs, consensus.

### 2. Service Macro Pattern

Instead of hand-rolling RPC boilerplate, use a procedural macro:

```rust
#[selium_guest::service]
pub trait DiscoveryService {
    fn discover(&self, ...) -> Future;
}
```

This generates:
- **Server side:** channel creation, RPC server, event loop, routing
- **Client side:** metadata for dynamic consumer generation
- **Benefit:** Orchestrator can discover services and build consumers at runtime

### 3. Channel Ownership Pattern

Each guest **owns a channel** - their source of truth (SOT):

```
scheduler (guest)  <======>  scheduler data channel
     |                                    |
     v                                    v
supervisor (guest)  <======>  supervisor data channel
     |                                    |
     v                                    v
discovery (guest)  <======>  discovery data channel
```

- Write operations go through the channel
- Reads query the channel (or materialized views)
- Synchronization handled by I/O layer

### 4. Orchestrator as Intermediary

A privileged guest that:
- Receives CLI intents
- Discovers available services (via metadata)
- Builds RPC consumers dynamically
- Commands guests via RPC
- Aggregates results

```
CLI intent ──► Orchestrator ──► discovers services via metadata
                         ──► builds RPC consumers
                         ──► commands guests via RPC
                         ──► returns result to CLI
```

**Key constraint:** Orchestrator does NOT do coordination/communication itself. It leverages the "tools" (system guest APIs) at hand.

### 5. Simplified Guest Model

- **Scheduler** - owns scheduler data channel, handles placement
- **Supervisor** - owns supervisor data channel, handles health/restart
- **Discovery** - handles host/guest discovery
- **Routing** - I/O channel orchestration
- **Consensus** - possibly embedded in I/O layer rather than separate guest

### 6. I/O Layer as Foundation

The existing `old/crates/io/core/` provides channels with:
- Sequence numbers for ordering
- Durability (in-memory or file-backed)
- Kernel transport (queues + shared memory)
- Subscription model for consumers

Future enhancement: synchronized channels with built-in consensus for multi-node.

---

## Architecture Diagram (Emerging)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SELIUM HOST                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     SYSTEM GUESTS                                      │   │
│  │                                                                      │   │
│  │   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐          │   │
│  │   │  scheduler  │    │  supervisor  │    │  discovery   │          │   │
│  │   │  ─────────  │    │  ─────────  │    │  ─────────  │          │   │
│  │   │ owns channel│    │ owns channel│    │ owns channel│          │   │
│  │   └──────────────┘    └──────────────┘    └──────────────┘          │   │
│  │                                                                      │   │
│  │   Each guest: owns + operates on its own SOT via channel             │   │
│  │                                                                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         ORCHESTRATOR                                 │   │
│  │   (guest, privileged)                                                │   │
│  │                                                                      │   │
│  │   CLI intent ──► Orchestrator ──► discovers services                 │   │
│  │                           │         (via service metadata)           │   │
│  │                           ├──► builds RPC consumers                  │   │
│  │                           └──► commands guests via RPC                │   │
│  │                                                                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         I/O LAYER                                    │   │
│  │   (host-provided, synchronized)                                      │   │
│  │                                                                      │   │
│  │   Channels: publish/subscribe, sequence numbers, durability           │   │
│  │   Multi-node: consensus embedded here? (separate change spec)        │   │
│  │                                                                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                               CLI (native)                                   │
│                                                                             │
│   Purpose: Express intent                                                  │
│   Connects to: nearest host (network tomography)                           │
│   Protocol: QUIC to orchestrator                                           │
│   Views: multiple hosts as equal                                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Open Questions

### Service Discovery
- If every guest exposes a service via `#[selium_guest::service]`, how does the orchestrator know what's available?
- Is there a service registry guest?
- Do guests announce themselves on startup?
- Is discovery built into the service macro?

### Multi-Node Synchronization
- Single-node: mutex-protected data structure?
- Multi-node: Raft in I/O layer? Per-channel consensus?
- Or something in between (gossip + eventual consistency)?
- **Decision:** This is an I/O problem, not an orchestration problem. Handle in separate change spec.

### Orchestrator Service Discovery
- How does orchestrator learn about other services?
- Static configuration? Dynamic discovery?
- If dynamic, how does it bootstrap?

### Orchestrator ↔ Guest Commands
- Via RPC to each guest's service interface?
- Via writing to the guest's channel?
- Via special hostcall?
- **Decision:** RPC. Dogfood the service macro pattern.

### Init Guest Fate
- In greenfield design, init was narrow - deals with startup only
- After spawning system guests, what happens?
- Options:
  - Init exits, host listens for OS termination signal
  - Init persists for lifecycle management
  - Init becomes unnecessary
- **Unresolved:** Needs more exploration

### Guest Startup Order
- Orchestrator depends on other services (scheduler, supervisor, etc.)
- How does it wait for them to be ready?
- Health checks? Dependency declarations?

### CLI Host Discovery
- Single-node: host address provided via stdin/config
- Multi-node: DNS for cluster bootstrap?
- How does CLI find the "nearest" host for network tomography?

### Routing Guest
- Previously designed as message proxy/load balancer
- If orchestrator handles routing decisions, what does routing guest do?
- Is it still needed? Merged with orchestrator?

### Multi-Node Orchestrator
- Every orchestrator capable of handling commands?
- Or primary/secondary model?
- How do they coordinate?
- **Unresolved:** Network tomography for "reconnect to closest node" was suggested

---

## Architectural Tensions

### 1. Narrow vs. Broad Orchestrator
- **Tension:** Orchestrator should be simple (interpret → delegate) vs. needs enough capability to coordinate
- **Resolution needed:** What's the boundary of "interpretation"?

### 2. CLI Simplicity vs. System Complexity
- **Tension:** CLI should be simple (express intent) vs. users may need visibility into what's happening
- **Resolution:** CLI sees hosts as equal; no knowledge of internal architecture. Visibility comes from separate tooling.

### 3. Service Discovery vs. Capability Model
- **Tension:** Services are discovered dynamically vs. capabilities are granted at spawn time
- **Resolution needed:** How does orchestrator get capability to discover services?

### 4. Single-Node vs. Multi-Node
- **Tension:** Architecture should work for both, but requirements differ significantly
- **Resolution:** Design for single-node first; multi-node via I/O layer changes (separate spec)

### 5. Guest Autonomy vs. Centralized Control
- **Tension:** Guests should be autonomous vs. orchestrator needs to command them
- **Resolution:** Orchestrator doesn't command directly - it expresses desired state; guests react to channel updates

---

## Uncertainties

1. **Init's role** - Is it needed? What happens after startup?
2. **Service metadata format** - What does the service macro emit for discovery?
3. **Orchestrator bootstrap** - How does it start with minimal capabilities?
4. **Channel synchronization** - Single-node vs. multi-node semantics not defined
5. **Guest lifecycle** - Who manages guest restarts? Supervisor? Orchestrator?
6. **Intent expression format** - What does CLI send to orchestrator? Structured commands? Domain-specific language?

---

## Decisions That Seemed Solid

1. **CLI is native** - Not a WASM guest, connects via QUIC
2. **CLI expresses intent** - Not implementation details
3. **Orchestrator is a guest** - Not native code, not a single capability
4. **RPC for guest communication** - Dogfood the service macro pattern
5. **Each guest owns a channel** - Their source of truth
6. **Consensus is I/O concern** - Not a separate guest; embed in I/O layer
7. **Multi-node out of scope** - Handle in separate change spec
8. **CLI sees hosts as equal** - No hierarchy, network tomography for proximity

---

## Referenced Artifacts

- `openspec/changes/archive/2026-03-20-greenfield-selium-kernel-userland/` - Greenfield architecture
- `old/crates/cli/` - Previous CLI implementation
- `old/crates/io/core/` - Existing channel implementation
- `old/crates/io/tables/` - Previous table store with versioning

---

## Next Steps

This exploration revealed that the architecture needs further simplification before implementing CLI:

1. **Clarify orchestrator scope** - What does it own? What does it delegate?
2. **Define service discovery** - How do guests announce, how does orchestrator learn?
3. **Resolve init fate** - Is it needed? What replaces it?
4. **Design intent expression** - What format does CLI use to talk to orchestrator?
5. **Separate multi-node work** - Focus single-node first

The suggestion was to explore these first principles in a **separate greenfield repo** before integrating into arch2.
