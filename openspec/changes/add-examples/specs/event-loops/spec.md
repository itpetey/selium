## ADDED Requirements

### Requirement: Event binding initialization
The event-loops example SHALL demonstrate setting up event bindings:
- Configure event readers and writers at module start
- Bind to specific endpoint names
- Handle the case where bindings are not yet available

#### Scenario: Module accepts binding configuration
- **WHEN** module starts with event-reader and event-writer flags
- **THEN** it configures internal readers/writers accordingly

#### Scenario: Module waits for bindings
- **WHEN** bindings are not immediately available
- **THEN** module waits or reports appropriate error

### Requirement: Event processing loop
The example SHALL demonstrate processing events:
- Main loop reads incoming events
- Events are deserialized to typed structures
- Processing produces output events on writer
- Loop continues until shutdown

#### Scenario: Events are read and processed
- **WHEN** events arrive on bound reader
- **THEN** they are deserialized and processed in order

#### Scenario: Processed events are emitted
- **WHEN** input event processing completes
- **THEN** output event is written to bound writer

### Requirement: Graceful shutdown on event binding
The example SHALL handle binding loss gracefully:
- If writer binding is lost, module can continue reading
- If reader binding is lost, module can emit final event and exit
- Shutdown is coordinated via selium_guest::shutdown

#### Scenario: Module handles binding loss
- **WHEN** a binding becomes unavailable
- **THEN** module handles the error appropriately

#### Scenario: Module responds to shutdown signal
- **WHEN** selium_guest::shutdown() is called
- **THEN** event loop exits cleanly
