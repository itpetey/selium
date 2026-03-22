## 1. ABI Enhancement

- [ ] 1.1 Add missing capabilities to crates/abi/src/lib.rs (SessionLifecycle, ProcessLifecycle, SharedMemory, NetworkAccept, NetworkRpcClient, NetworkRpcServer)
- [ ] 1.2 Add Principal and PrincipalKind types to ABI
- [ ] 1.3 Add rkyv support for ABI stability
- [ ] 1.4 Add ResourceScope type to ABI

## 2. Host Session Management

- [ ] 2.1 Create session module in crates/host/src/session.rs
- [ ] 2.2 Implement Session struct with pubkey, principal, authn_method
- [ ] 2.3 Implement Session::bootstrap() for init
- [ ] 2.4 Implement Session::create() for delegation with scope enforcement
- [ ] 2.5 Add session tracking to guest spawn flow

## 3. Host Capability Enhancement

- [ ] 3.1 Enhance CapabilityRegistry to support ResourceScope
- [ ] 3.2 Add validate with resource_id parameter
- [ ] 3.3 Implement authorise() method checking capability + resource
- [ ] 3.4 Implement allows_capability() for capability-only check
- [ ] 3.5 Add GuestNamespace for handle isolation

## 4. Host Metering Enhancement

- [ ] 4.1 Enhance UsageMeter to track attribution (external_account_ref, module_id, instance_id)
- [ ] 4.2 Add attribution to spawn parameters
- [ ] 4.3 Add snapshot() method returning GuestUsage with attribution
- [ ] 4.4 Add query method for filtering by attribution

## 5. TLS Certificate Infrastructure

- [ ] 5.1 Port certs.rs from old/crates/runtime/service/src/certs.rs
- [ ] 5.2 Add to crates/host/src/certs.rs
- [ ] 5.3 Implement generate_ca() function
- [ ] 5.4 Implement generate_leaf() with Server, Client, Peer variants
- [ ] 5.5 Add TLS termination to network hostcall
- [ ] 5.6 Implement certificate persistence

## 6. Guest Services

- [ ] 6.1 Create session guest module (if not handled in host)
- [ ] 6.2 Create entitlement guest for rich authorization
- [ ] 6.3 Create attribution guest for extended metering

## 7. Integration and Testing

- [ ] 7.1 Run cargo fmt and cargo clippy
- [ ] 7.2 Run cargo test to verify changes
- [ ] 7.3 Update existing tests for new behavior
- [ ] 7.4 Add integration tests for AAA flow