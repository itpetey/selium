## ADDED Requirements

### Requirement: Init guest serves as first guest to boot
The init guest SHALL be the first guest spawned by the host and SHALL orchestrate the startup of all other system services.

#### Scenario: Init boots first
- **WHEN** host starts with init guest
- **THEN** init guest SHALL be spawned before any other guest

#### Scenario: Init completes host lifecycle
- **WHEN** init's `start()` entrypoint returns
- **THEN** the host SHALL terminate with init's exit status

### Requirement: Init guest reads static configuration
The init guest SHALL read InitConfig from static configuration defining enabled services.

#### Scenario: Default configuration
- **WHEN** no explicit config provided
- **THEN** init SHALL use default config enabling consensus, scheduler, discovery, supervisor, routing

#### Scenario: Custom configuration
- **WHEN** InitConfig specifies enabled services
- **THEN** init SHALL only spawn the enabled services

### Requirement: Init guest spawns services in order
The init guest SHALL spawn system services in dependency order: consensus → scheduler → discovery/supervisor/routing.

#### Scenario: Consensus spawned first
- **WHEN** init boots
- **THEN** consensus guest SHALL be spawned before scheduler

#### Scenario: Service order
- **WHEN** init boots with default config
- **THEN** services SHALL be spawned in order: consensus, scheduler, discovery, supervisor, routing

### Requirement: Init guest tracks spawned services
The init guest SHALL maintain a registry of spawned service handles (name, guest_id).

#### Scenario: Service registration
- **WHEN** a service is spawned
- **THEN** init SHALL register ServiceHandle with name and guest_id

#### Scenario: Service lookup
- **WHEN** init.get_service("consensus") is called
- **THEN** it SHALL return the ServiceHandle if consensus was spawned

### Requirement: Init guest uses host spawn capability
The init guest SHALL use host `spawn()` to create guest processes with appropriate handles.

#### Scenario: Spawn with handles
- **WHEN** spawning a service
- **THEN** init SHALL pass appropriate capability handles (storage, network, queues)

#### Scenario: Service readiness
- **WHEN** spawning a service
- **THEN** init SHALL wait for service to report ready before proceeding

### Requirement: Init guest provides run_init async function
The init guest SHALL export an async `run_init()` function as the entrypoint.

#### Scenario: Run init returns result
- **WHEN** run_init() completes successfully
- **THEN** it SHALL return Ok(())

#### Scenario: Run init handles errors
- **WHEN** run_init() encounters an error
- **THEN** it SHALL return Err(GuestError) and propagate to host
