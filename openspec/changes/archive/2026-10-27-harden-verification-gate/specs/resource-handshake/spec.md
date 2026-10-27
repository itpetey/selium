## ADDED Requirements

### Requirement: Departed RPC session release

A rendezvous region delivered to a receiver SHALL be released when its client
departs, including a client that dies without a clean close. The host SHALL
mark the session's request ring as peer-closed when the sender is reaped and
wake a receiver parked reading it; the receiver SHALL then exit and free the
region it owns. A live client's session SHALL NOT be released.

#### Scenario: Departed client's region is released

- **WHEN** a client that sent an RPC rendezvous region exits or crashes
- **THEN** the host SHALL mark the session peer-closed, wake the receiver, and
  the receiver SHALL free the connection region

#### Scenario: Live idle client keeps its region

- **WHEN** a client is alive but sends no requests
- **THEN** the receiver SHALL keep the session and its region

#### Scenario: Graceful close releases the region

- **WHEN** a client closes its session and stops
- **THEN** the receiver SHALL observe the close and release the region

## MODIFIED Requirements

### Requirement: Host-mediated Connection Queue (ResourceSender/ResourceListener)
The `ResourceSender` and `ResourceListener` types in `selium-guest` SHALL implement the `Rendezvous` trait. The host SHALL continue to validate capability before enqueuing connections. A host queue SHALL be reclaimed when its last local handle is released: a queue with a live attacher SHALL remain available, and a queue whose owner and every attacher have gone SHALL NOT accumulate in the registry.

#### Scenario: Client sends connection request via ResourceSender
- **WHEN** a client calls `sender.send(shared_id).await`
- **THEN** the system SHALL invoke the `HostQueueSend` hostcall with capability validation

#### Scenario: Server accepts connection via ResourceListener
- **WHEN** a server calls `listener.recv().await`
- **THEN** the system SHALL return an `IncomingConnection` with `client_process_id` and `shared_id`

#### Scenario: Last handle releases the queue
- **WHEN** the final local handle to a host queue is released at process teardown
- **THEN** the queue SHALL be removed from the registry

#### Scenario: Discovery listener queue is released when discovery stops
- **WHEN** the discovery guest stops
- **THEN** the host-created listener queue SHALL be destroyed
