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
