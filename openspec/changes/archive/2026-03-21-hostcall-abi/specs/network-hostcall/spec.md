# Network Hostcall

Network operations for guest modules.

## ADDED Requirements

### Requirement: Network operations namespace

Network hostcalls SHALL be exposed under the import namespace `selium::network`.

### Requirement: NetworkHandle type

The ABI crate SHALL define a `NetworkHandle` type representing an open network connection.

### Requirement: NetworkListenerHandle type

The ABI crate SHALL define a `NetworkListenerHandle` type representing a listening socket.

### Requirement: NetworkConnect driver

The network hostcalls SHALL include a `NetworkConnect` driver that:
- Accepts an address and port
- Returns a `NetworkHandle` or an error

#### Scenario: Successful connection
- **WHEN** a guest calls `network::connect` with a reachable address
- **THEN** the host SHALL establish the connection and return a handle

#### Scenario: Connection refused
- **WHEN** a guest calls `network::connect` but the target refuses
- **THEN** the host SHALL return `GuestError::WouldBlock`

### Requirement: NetworkSend driver

The network hostcalls SHALL include a `NetworkSend` driver that:
- Accepts a `NetworkHandle` and data bytes
- Returns bytes sent or an error

#### Scenario: Successful send
- **WHEN** a guest calls `network::send` with a valid handle
- **THEN** the host SHALL send the data and return the byte count

### Requirement: NetworkRecv driver

The network hostcalls SHALL include a `NetworkRecv` driver that:
- Accepts a `NetworkHandle` and maximum byte count
- Returns received data or an error

#### Scenario: Successful receive
- **WHEN** a guest calls `network::recv` and data is available
- **THEN** the host SHALL return the received data

#### Scenario: Would block
- **WHEN** a guest calls `network::recv` but no data is available
- **THEN** the host SHALL return `GuestError::WouldBlock`

### Requirement: NetworkClose driver

The network hostcalls SHALL include a `NetworkClose` driver that:
- Accepts a `NetworkHandle`
- Returns success or an error

#### Scenario: Successful close
- **WHEN** a guest calls `network::close` with a valid handle
- **THEN** the host SHALL close the connection and return success

### Requirement: NetworkListen driver

The network hostcalls SHALL include a `NetworkListen` driver that:
- Accepts an address and port
- Returns a `NetworkListenerHandle` or an error

#### Scenario: Successful listen
- **WHEN** a guest calls `network::listen` with an available port
- **THEN** the host SHALL start listening and return a handle

### Requirement: NetworkAccept driver

The network hostcalls SHALL include a `NetworkAccept` driver that:
- Accepts a `NetworkListenerHandle`
- Returns a `NetworkHandle` for the accepted connection

#### Scenario: Successful accept
- **WHEN** a guest calls `network::accept` and a connection is pending
- **THEN** the host SHALL accept and return a connection handle

#### Scenario: No pending connections
- **WHEN** a guest calls `network::accept` but no connection is pending
- **THEN** the host SHALL return `GuestError::WouldBlock`

### Requirement: Capability gating

Network hostcalls SHALL require the corresponding capability:
- `NetworkConnect` requires `Capability::NetworkConnect`
- `NetworkSend` requires `Capability::NetworkStreamWrite`
- `NetworkRecv` requires `Capability::NetworkStreamRead`
- `NetworkClose` requires `Capability::NetworkLifecycle`
- `NetworkListen` requires `Capability::NetworkLifecycle`
- `NetworkAccept` requires `Capability::NetworkLifecycle`
