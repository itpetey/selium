## ADDED Requirements

### Requirement: Teardown releases pending sleeps

A process that stops SHALL have its pending guest sleeps removed from the
timer driver. A deadline the process can never consume SHALL NOT remain queued
for its full duration, and the driver SHALL re-arm against the next earliest
deadline after such a removal.

#### Scenario: Stopped guest leaves no pending timer

- **WHEN** a guest parked on a long `Timer` is stopped
- **THEN** the driver SHALL drop its sleep, and `pending_timer_count` SHALL
  return to the baseline of the guests that are still running

#### Scenario: A stopped discovery guest drains its sweep sleep

- **WHEN** the discovery guest stops while its lease-sweep sleep is queued
- **THEN** the sleep SHALL be discarded at its deadline rather than renewed
