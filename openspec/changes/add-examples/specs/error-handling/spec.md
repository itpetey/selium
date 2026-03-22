## ADDED Requirements

### Requirement: GuestError types
The error-handling example SHALL demonstrate proper error types:
- Use `selium_guest::GuestError` enum variants
- Include context in error messages
- Distinguish recoverable vs fatal errors

#### Scenario: Error with context
- **WHEN** operation fails with reason
- **THEN** error includes descriptive message

#### Scenario: Error propagation
- **WHEN** function returns GuestResult<T>
- **THEN** errors propagate via ? operator

### Requirement: GuestResult usage
The example SHALL demonstrate Result type conventions:
- Functions return `GuestResult<T>` or `GuestResult<()>`
- Use ? for error propagation
- Provide meaningful error messages

#### Scenario: Successful operation
- **WHEN** operation succeeds
- **THEN** Ok(value) is returned

#### Scenario: Failed operation propagates
- **WHEN** nested function returns Err
- **THEN** error propagates to caller

### Requirement: Panic handling
The example SHALL demonstrate panic handling:
- Use `panic-helper` crate for WASM panics
- Panics are caught and reported as errors
- Module can recover from some panic scenarios

#### Scenario: Panic is caught
- **WHEN** code panics during execution
- **THEN** panic is caught and reported as GuestError

#### Scenario: Panic-helper is configured
- **WHEN** module is built
- **THEN** panic hook is set to catch and forward panics
