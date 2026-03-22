## Context

The new kernel/userland architecture lacks AAA (Authentication, Authorization, Accounting) infrastructure. The previous implementation in `old/crates/` had comprehensive security features that must be ported:

- **Authentication**: Session + pubkey identity system
- **Authorization**: Capability-based with resource scoping (None/Some/Any)  
- **Accounting**: Attribution-based metering with external_account_ref, module_id
- **TLS**: Certificate generation infrastructure

Current state in `crates/host/`:
- Basic handle-based isolation (`CapabilityRegistry`)
- Simple usage counters (`UsageMeter`)
- No session or principal system
- No TLS infrastructure

## Goals / Non-Goals

**Goals:**
- Port session/principal system to host
- Enhance capability registry with resource scoping
- Add attribution to metering (external_account_ref, module_id, instance_id)
- Port TLS certificate infrastructure
- Create guest services for rich authorization
- Add 5 missing capabilities to ABI

**Non-Goals:**
- Inter-guest RPC authentication (covered by RPC framework)
- Multi-node gossip/clustering (enterprise init only)
- Schema system for contracts

## Decisions

### 1. Two-Tier AAA Architecture

**Decision**: Split AAA into host tier and guest tier.

```
Host Tier:
├── Handle validation (does guest own this handle?)
├── Basic capability check (does guest have this capability type?)
├── TLS termination (incoming connections)
└── Basic metering (resource consumption, guest-level attribution)

Guest Tier:
├── Session service (principal, authentication)
├── Entitlement service (resource scoping, delegation)
└── Attribution service (external_account_ref mapping)
```

**Rationale**: Host provides sandboxing enforcement (can't bypass). Guest services provide policy and rich semantics.

### 2. Host Handles + Guest Capabilities

**Decision**: Host validates handles; guest services validate resource scopes.

```
Old: session.authorise(Capability::StorageBlobRead, resource_id)
     └── single check in kernel

New: host.validate(handle_id, guest_id)  // handle belongs to guest?
     └── guest service: authorize(capability, resource_id) // resource allowed?
```

**Rationale**: Maintains kernel/userland separation while enabling rich authorization in guests.

### 3. Session Lives in Host

**Decision**: Session is a host-level concept for hostcall verification. Guest services use it for inter-guest RPC.

```
Host maintains:
- guest_id → session mapping
- session.principal for attribution
- session.pubkey for authentication
```

**Rationale**: Host needs to verify caller identity for hostcall enforcement.

### 4. Attribution at Spawn

**Decision**: Attribution (external_account_ref, module_id) is determined at spawn time.

```
spawn(module, handles, attribution: {
    external_account_ref,
    module_id,
    instance_id
})
```

**Rationale**: Simpler than per-call attribution. Guests can still act on behalf of others via delegation, but base attribution is fixed.

### 5. Port Existing Code, Don't Rewrite

**Decision**: Port code directly from `old/crates/` with minimal adaptation.

**Rationale**: The old code is well-tested and secure. Adaptation should be integration, not rewrite.

### 6. Certificates for TLS Termination + Inter-Node mTLS

**Decision**: Port `certs.rs` to host. Use for both:
- TLS termination (incoming connections)
- Inter-node mTLS (enterprise multi-node)

**Rationale**: Same infrastructure serves both. Enterprise multi-node needs node-to-node auth.

## Risks / Trade-offs

- **[Complexity]**: Two-tier AAA is more complex than single-tier
  - **Mitigation**: Clear separation of concerns, well-documented interfaces
  
- **[Performance]**: Guest-level authorization adds latency
  - **Mitigation**: Cache authorizations, use handle-level for hot paths
  
- **[Attribution]**: Attribution at spawn limits flexibility
  - **Mitigation**: Delegation allows guests to act on behalf of others

- **[Wire Compatibility]**: New ABI differs from old
  - **Mitration**: Greenfield - no backwards compatibility needed

## Migration Plan

This is a greenfield project - no migration from existing code. Integration with existing artifacts:

1. **ABI**: Update `crates/abi/src/lib.rs` with new capabilities + principals
2. **Host**: Enhance `crates/host/src/capabilities.rs`, `metering.rs`, add `certs.rs`
3. **Guest services**: Create new guests for session, entitlement, attribution
4. **Existing specs**: Update `host-kernel` and `capability-registry` with new requirements

## Open Questions

~~1. **Handle validation vs capability check**: Host does handle validation, guest does capability check.~~ RESOLVED

~~2. **Session lives where**: Session in host, guest services use it.~~ RESOLVED

~~3. **Attribution at spawn or per-call**: Attribution at spawn.~~ RESOLVED

~~4. **Certificate scope**: TLS termination + inter-node mTLS.~~ RESOLVED

5. **Wire compatibility**: Is clean ABI acceptable, or need backwards compatibility?
   - **Decision needed**: Greenfield suggests clean ABI is fine