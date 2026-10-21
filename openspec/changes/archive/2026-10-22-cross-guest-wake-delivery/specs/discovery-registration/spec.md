# Spec Delta

## ADDED Requirements

### Requirement: Lease sweep on the monotonic timer
The discovery lease sweep SHALL measure its interval on the monotonic clock the
`Sleep` hostcall compares against, so it ticks on schedule inside a `wasm32`
guest rather than being parked on a deadline in a different clock domain. A
first tick SHALL be observable so a sweep that never fires can be distinguished
from one that fires silently.

#### Scenario: Sweep ticks in a wasm32 guest
- **WHEN** the discovery guest runs with a live lease and nothing else happens in the platform
- **THEN** the sweep SHALL tick on its timer, expire lapsed leases, and emit the resulting revocations

#### Scenario: A dead sweep is observable
- **WHEN** the sweep has not ticked because its deadline is unreachable
- **THEN** the absence of the first-tick marker SHALL reveal that the sweep is parked
