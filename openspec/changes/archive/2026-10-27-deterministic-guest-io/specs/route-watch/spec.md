# Spec Delta

## MODIFIED Requirements

### Requirement: Convergence Without a Readiness Gate
A consumer SHALL converge on a provider through reactive route watches rather than a host-verified readiness signal or timer-based polling. The runtime SHALL NOT gate a guest's startup on another guest's readiness, and consumers SHALL NOT reintroduce retry loops around an available route watch.

#### Scenario: Consumer starts before its provider
- **WHEN** a consumer is started before the provider it depends on
- **THEN** the consumer SHALL converge on the provider's registration write, with no host readiness gate or retry interval

#### Scenario: Provider restarts mid-life
- **WHEN** a running provider exits and is restarted
- **THEN** watching consumers SHALL observe the change and converge on the restarted provider
