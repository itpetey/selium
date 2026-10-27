# Known-unknowns register

A green gate means "green within the coverage below", not "no known unknowns".
This register lists what the gate does **not** exercise. It is deliberately
short and specific: an entry is a gap someone could close, not a disclaimer.

The gate prints the open entries with every result (`scripts/gate.sh`), so a
green run is read as qualified by this list. Record a new gap here when it is
identified rather than leaving it implicit.

## Platform coverage is Linux-only

CI and the default `scripts/gate.sh` run on Linux. Darwin's permanently
Stage-1 wait words, Windows/FreeBSD wait-word builds, and the pthread-backed
std behaviour macOS exposes are not asserted by the gate. `--native` on macOS
covers the host subset only and says so.

## Multi-host and distributed behaviour is not tested

Every spine binds loopback or a single host: no cross-machine networking,
partition, clock skew, or failover is exercised.

## Network conditions are benign

QUIC/TLS spines run over loopback with no packet loss, reordering, or
bandwidth limit. Congestion-control behaviour and retry storms under a hostile
network are unknown.

## Crash consistency is format-level

The crash lane tears the on-disk framed log/manifest at every byte offset (a
short write) and corrupts a written record's checksum, then asserts replay is
the complete prefix. It does not power-cut the machine, fail `fsync`, or
corrupt the storage medium beneath the file, so filesystem-level ordering
guarantees are assumed, not tested.

## Load is bounded

The soak lane exercises repeated spawns, timers, and registrations at a rate a
CI runner can sustain. Sustained multi-tenant load, adversarial scheduling, and
hour-scale runs are not covered, so slow leaks beyond the soak window are
unknown.

## No fuzzing or adversarial wire input

Malformed, truncated, and hostile frames are covered by hand-written codec
tests and some property-shaped cases, but there is no continuous fuzzing of the
wire codecs, hostcall arguments, or the wasm module loader.

## Guest ABI drift is detected by rebuild, not by compatibility

Integration tests rebuild guests from current sources before reading them, so a
stale artifact cannot silently mis-test the runtime. The reverse — an *older*
guest module talking to a *newer* host — is not supported or tested.

## Traceability is claimed, not proven

`scripts/check-traceability.sh` proves that a scenario names a test (or a
reviewed waiver). It cannot prove that the test genuinely asserts the scenario.
Annotations are reviewed when added and can rot when a test changes;
`--report` shows drift, not intent.

## Soak and crash lanes run nightly, not per merge

A regression that only appears in the long lanes is merged before it is
reported. The nightly run is the first evidence, not a pre-merge guarantee.

## A slow blocking writer anchors the ring window by design

A blocking writer's slot bounds how far other writers may run ahead of its last
committed frame (documented Park backpressure). A writer that goes permanently
idle therefore caps the ring window at one capacity past its position, and the
window arithmetic is not fuzzed: a long-idle neighbour, or many writers with
skewed progress, is only covered by the hand-written slot tests in
`selium-shm` and by the spine/soak lanes. Writers no longer pin themselves
(`reserve_tail_for_writer` excludes the caller's own slot, and the slot advances
to the end of each committed frame), so the large-frame and late-join cases the
bridge's acceptance-frame test used to expose are covered; whether the
cross-writer window itself should be reader-relative remains a ring-protocol
design question rather than a fixed race.

## Watch-driven boot convergence still depends on discovery reachability

`Context::resolve_ready` and protocol-handler resolution converge on
route-watch events, but the initial authoritative `lookup` and the re-resolve
after each appearance still ride the discovery RPC path. A permanently stalled
discovery connection surfaces as a typed lookup error or the readiness bound,
not as a converged route: there is no host-side readiness sequencer behind it.

## A spine can miss its convergence bound under sequential load

The identity spine was observed hitting its onboarding convergence bound once
while several long spines ran back to back on this host, then passing on
isolated re-runs (about 2 minutes each). The failure was a bound expiry, not a
wrong result: the deterministic regressions (RPC acceptance, session lifecycle,
route readiness, bridge tables) stayed green in every run. Sustained parallel
spine load therefore remains a timing unknown to track, not a diagnosed race.
