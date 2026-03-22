## Why

The new greenfield architecture (kernel/userland split) lacks critical security infrastructure that existed in the previous implementation. Specifically, there is no authentication system, no rich authorization beyond basic handle isolation, and no attribution for usage metering. This is a significant gap that must be addressed before the system can be considered secure for production use.

The previous implementation (`old/crates/`) had a comprehensive AAA system including session management, capability-based authorization with resource scoping, attribution-based metering, and TLS certificate infrastructure. This change ports those security foundations to the new architecture while respecting the kernel/userland separation.

## What Changes

1. **Add 5 missing capabilities to ABI**: SessionLifecycle, ProcessLifecycle, SharedMemory, NetworkAccept, NetworkRpcClient, NetworkRpcServer
2. **Add principal/identity types to ABI**: Principal, PrincipalKind for identifying callers
3. **Implement session management in host**: Track authenticated sessions with public key identity
4. **Enhance capability registry with resource scoping**: Add ResourceScope (None/Some/Any) to capability grants
5. **Add attribution to metering**: Track external_account_ref, module_id, instance_id in usage samples
6. **Port TLS certificate infrastructure**: Certificate generation for TLS termination and inter-node mTLS
7. **Create guest services**: Session service, entitlement service for rich authorization

## Capabilities

### New Capabilities
- `session-management`: Authentication and session lifecycle management
- `entitlement-authorization`: Rich capability authorization with resource scoping
- `metering-attribution`: Usage tracking with attribution to external accounts
- `tls-certificates`: TLS termination and certificate infrastructure
- `principal-identity`: Principal and identity system for callers

### Modified Capabilities
- `host-kernel`: Add authentication enforcement, attribution requirements, TLS support
- `capability-registry`: Add resource scoping (None/Some/Any) requirements

## Impact

- **ABI changes**: `crates/abi/src/lib.rs` - add capabilities, principals, rkyv support
- **Host changes**: `crates/host/src/` - session tracking, enhanced capabilities, certs
- **Guest services**: New guest modules for session, entitlement, attribution services
- **Dependencies**: May need to add `rcgen` for certificate generation