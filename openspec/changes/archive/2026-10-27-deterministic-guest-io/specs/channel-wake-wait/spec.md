# Spec Delta

## ADDED Requirements

### Requirement: Writer Attachment Wakes Channel Readers
Successful blocking-writer registration SHALL advance the channel's generation and wake readers registered for generation changes. Failed registration SHALL NOT publish a spurious attachment event.

#### Scenario: Reader wakes on attachment without data
- **WHEN** a reader is parked awaiting a point-to-point peer and a blocking writer successfully registers without writing a frame
- **THEN** the reader SHALL be woken and observe the writer's presence

#### Scenario: Writer registration fails
- **WHEN** writer-slot or writer-id allocation fails during registration
- **THEN** the writer count and generation SHALL NOT retain a partially published attachment

## MODIFIED Requirements

### Requirement: Wakeable Channel Writes (Park Backpressure)
Writers on Park channels SHALL park on a full ring and SHALL be woken when blocking readers or blocking writers advance the minimum slot position. Writers on Drop channels SHALL keep drop-on-full semantics without parking. A blocking writer's own recorded slot position SHALL anchor *other* writers only: it SHALL NOT constrain that writer's own reservation, and it SHALL advance to the end of each committed frame so a writer cannot pin itself behind its own last reservation.

#### Scenario: Parked writer resumes after reader advance
- **WHEN** a writer hits BufferFull on a Park channel and a blocking reader later advances its slot
- **THEN** the writer is woken and its reservation attempt proceeds

#### Scenario: A large frame does not pin its own writer
- **WHEN** a blocking writer commits a frame larger than half the ring and a reader drains it
- **THEN** that writer's next reservation SHALL fit instead of parking on its own recorded slot

#### Scenario: An idle writer still anchors other writers
- **WHEN** a blocking writer commits a frame and then stops writing
- **THEN** other writers SHALL still be bounded by that writer's position rather than running arbitrarily far ahead of it
