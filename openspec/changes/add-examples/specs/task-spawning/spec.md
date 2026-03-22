## ADDED Requirements

### Requirement: Spawning concurrent tasks
A task-spawning example SHALL demonstrate cooperative multitasking:
- Use `selium_guest::spawn` to create new tasks
- Each spawned task runs concurrently with the main task
- Tasks can `yield_now` to cooperatively give up execution time
- `JoinHandle` allows waiting for task completion

#### Scenario: Single task spawn
- **WHEN** main task calls `spawn(async { /* work */ })`
- **THEN** a new task is created and begins executing

#### Scenario: Multiple concurrent tasks
- **WHEN** main task spawns three tasks
- **THEN** all three tasks run concurrently

#### Scenario: Joining on spawned task
- **WHEN** main task awaits the JoinHandle
- **THEN** main task blocks until spawned task completes

### Requirement: Cooperative yielding
The example SHALL demonstrate cooperative multitasking:
- Tasks call `yield_now()` to yield execution to the scheduler
- Without yielding, a compute-bound task could starve other tasks
- Yield allows other tasks to make progress

#### Scenario: Task yields control
- **WHEN** a task calls `yield_now()`
- **THEN** the scheduler runs other pending tasks

#### Scenario: Multiple yields per task
- **WHEN** a task yields multiple times
- **THEN** other tasks get opportunities to execute between yields

### Requirement: Task isolation
Tasks SHALL be isolated from each other:
- Panics in one task do not crash the entire module
- Each task has its own execution context
- Task-local state is isolated

#### Scenario: Task panic isolation
- **WHEN** a spawned task panics
- **THEN** other tasks continue executing
