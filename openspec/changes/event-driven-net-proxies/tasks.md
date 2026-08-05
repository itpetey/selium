# Tasks: Event-Driven Network Proxies

## 1. WaitRegister bridge

- [ ] 1.1 Add `HostcallRequest::WaitRegister { region_id, generation }` to the ABI (rkyv round-trip test)
- [ ] 1.2 Runtime wait registry: `(process, task, region, generation)` entries; `note_generation_advance(region, gen)` matches and calls `wake_process_task`
- [ ] 1.3 Runtime rejects `WaitRegister` for regions the process has not attached (`PermissionDenied`)
- [ ] 1.4 Guest reactor: when parking a task on a host-writable ring, issue `WaitRegister` with the parked `task_id`

## 2. Kernel inbound poller

- [ ] 2.1 Add mio dependency to `selium-kernel`; single poller thread owning `mio::Poll`
- [ ] 2.2 Register proxy sockets (TCP stream inbound, TCP accept, UDP recv) by `shared_id` token
- [ ] 2.3 Readable event → pump available bytes/datagrams to the inbound ring → `note_generation_advance` → guest wake bridge
- [ ] 2.4 Accept loop moves into the poller (listener readable → accept → create stream region → enqueue host queue)

## 3. Kernel outbound condvar waits

- [ ] 3.1 Outbound TCP pump and UDP send pump block on `atomic_wait32` (host condvar registry) on the ring generation word, with a bounded timeout backstop
- [ ] 3.2 Runtime kicks (`waiters::notify`) the guest's network regions on hostcall create, hostcall poll, and reactor stall
- [ ] 3.3 Delete all `thread::sleep` retry loops and `PROXY_POLL_INTERVAL_MS` from network proxy paths

## 4. Verification

- [ ] 4.1 Test: guest reader parked on inbound ring wakes on socket data without any sleep-based polling (assert no 1 ms poll constants remain; assert wake latency < threshold)
- [ ] 4.2 Test: guest write followed by stall is drained to the socket without waiting for the backstop timeout
- [ ] 4.3 Test: EOF/half-close propagation still behaves per `guest-networking` scenarios
- [ ] 4.4 Test: spin detection — proxy threads consume ~0 CPU while idle (best-effort CI assertion or documented manual check)
