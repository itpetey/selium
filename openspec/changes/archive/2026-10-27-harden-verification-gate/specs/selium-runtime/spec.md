## ADDED Requirements

### Requirement: A concurrent stop is not undone by an in-flight poll

When a process is stopped while one of its reactor passes is executing, the
finishing pass SHALL NOT re-insert the guest into the loaded-guest table. The
stop wins: the guest execution is dropped, so a stopped process cannot leave a
stale loaded guest behind.

#### Scenario: Stop during a poll reclaims the execution

- **WHEN** `stop_process` runs while the guest's poll pass is in flight
- **THEN** the finishing poll SHALL observe the process is no longer running
  and drop the guest instead of re-inserting it

#### Scenario: Repeated stop cycles leave no loaded guests

- **WHEN** a runtime spawns and stops guests repeatedly
- **THEN** after the last stop the loaded-guest table SHALL be empty
