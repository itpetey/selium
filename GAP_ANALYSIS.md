# Goal

Turn Selium from its current daemon-plus-control-plane prototype into a paid cloud platform with utility billing for CPU, memory, bandwidth, and block storage.

# Current State

Selium already has a credible execution substrate:

- contract-first control-plane workflows for deployments, pipelines, and node discovery
- a daemon with mTLS for CLI and peer control traffic
- a scheduler that reconciles workloads onto nodes
- runtime-managed network and storage resources
- file-backed durable logs and blob stores
- replayable control-plane events and basic operational CLI commands

The product surface is still much closer to a developer/runtime prototype than to a commercial cloud service:

- workload identity includes a `tenant/namespace/workload` path, but the tenant is just a string in the control-plane model rather than a backed account, organization, or policy boundary
- authentication and authorization are not product-grade yet
- scheduling is based on abstract capacity slots rather than declared CPU, memory, bandwidth, or storage requests
- there is no usage ledger, billing pipeline, pricing model, quota engine, or invoice flow
- observability is primarily logs plus CLI inspection, not a managed metrics/alerts/SLO platform
- storage exists as named file-backed logs and blobs, not as tenant-scoped block volumes with lifecycle, performance classes, and billing

# Delta Assessment

## 1. Tenant model and customer surface

Today Selium has names for workloads and pipelines, but not customers.

- Missing: organizations, projects, users, roles, API tokens, service accounts, invitations, audit history, plan assignment, subscription state.
- Impact: the current `tenant` field is enough to namespace desired state, but not enough to sell, operate, or secure the platform.

## 2. Authentication and authorization

Today Selium has transport security for daemon access, not a hosted identity system.

- Present: daemon mTLS for CLI and peer RPC.
- Missing: user auth, SSO/OIDC, bearer/API tokens, RBAC, service-to-service identities, tenant-aware policy enforcement, customer-facing ingress auth, secret management.
- Impact: this is the largest productization gap after billing.

## 3. Resource model and scheduling

Today Selium schedules with `capacity_slots`.

- Present: workload replicas, isolation profile selection, node liveness, placement, and bridge activation.
- Missing: resource requests/limits, node allocatable accounting, noisy-neighbor controls, quota checks, autoscaling, overcommit policy, preemption, admission control.
- Impact: utility pricing cannot be trusted until the scheduler reasons about billable resources directly.

## 4. Metering and billing

Today Selium has no usage accounting path.

- Missing: measurement collection, aggregation windows, pricing catalog, rated usage, invoice generation, credit handling, payment collection, dispute/reconciliation workflows.
- Impact: this is a net-new subsystem, not an incremental extension of existing code.

## 5. Storage productization

Today Selium offers runtime-managed logs and blob stores.

- Present: named file-backed durable logs, checkpoints, blobs, and manifests.
- Missing: tenant-scoped block volumes, volume attach/detach lifecycle, snapshots, resize, classes, durability SLAs, encryption-at-rest controls, storage quotas, storage billing.
- Impact: block storage is effectively not started yet as a cloud product.

## 6. Observability and cloud operations

Today Selium exposes logs and control-plane inspection.

- Present: tracing output, JSON logs, control-plane `observe`, `replay`, and `nodes`.
- Missing: metrics backend, per-tenant usage dashboards, alerts, SLOs, incident tooling, backups/restores, upgrades, regional rollout playbooks, support tooling.
- Impact: without these, even a single-tenant managed service would be operationally fragile.

# Milestones

## Milestone 0: Operator-Grade Private Preview

### Outcome

Selium can be run by the core team as a managed private preview for a few design partners, but only with manual onboarding and no public self-service.

### Scope

- harden runtime and control-plane lifecycle for long-running clusters
- define the canonical resource model for CPU, memory, bandwidth, and block storage
- introduce cluster/operator observability and backup baselines
- keep tenancy coarse and operator-managed

### Required Specifications

#### Control-plane resource model

- Add resource requests and limits to deployment specs:
  - `cpu_millis`
  - `memory_mib`
  - `ephemeral_storage_mib`
  - `bandwidth_profile`
  - `volume_mounts`
- Replace slot-only placement with allocatable resource accounting per node.
- Reject placements that exceed allocatable resources.
- Persist declared resources in control-plane state and include them in summaries and replay events.

#### Runtime measurement hooks

- Instrument per-process CPU time, memory high-water mark, memory sampled byte-ms, network ingress bytes, network egress bytes, and storage bytes written/read.
- Emit usage samples on a fixed interval and on process termination.
- Make the collector idempotent and resilient to daemon restarts.

#### Operations baseline

- Export machine-consumable health and metrics endpoints.
- Define backup and restore for:
  - control-plane Raft state
  - runtime storage logs/blobs
  - certificate material
- Add structured audit events for operator mutations.

### Exit Criteria

- a node can advertise allocatable CPU and memory, not just slots
- deployments fail admission when resource requests exceed available capacity
- every running workload produces measurable CPU, memory, and bandwidth samples
- operators can restore a cluster from documented backups
- private preview customers can run without direct shell access to the cluster

## Milestone 1: Managed Single-Tenant Service

### Outcome

Selium can run one managed environment per customer with strong isolation at the environment boundary and manual commercial operations.

### Scope

- each customer gets a dedicated control plane and runtime fleet
- onboarding, auth, and quotas remain simple
- billing can be manual or off-platform during this phase

### Required Specifications

#### Customer model

- Introduce first-class `organization`, `project`, and `environment` records.
- Map the current control-plane `tenant` concept to an internal environment identifier instead of a free-form user string.
- Store subscription plan, contract owner, support tier, and region on the environment.

#### Access control

- Support operator-issued API tokens and service credentials.
- Add environment-scoped RBAC with at least:
  - `owner`
  - `admin`
  - `developer`
  - `viewer`
- Require authenticated mutations on all daemon-exposed control-plane APIs.

#### Customer ingress

- Provide HTTPS endpoint management with:
  - managed certificates
  - DNS mapping
  - request authentication hooks
  - rate limiting
- Separate public ingress policy from internal daemon control traffic.

### Exit Criteria

- a customer can be provisioned as an isolated environment with dedicated capacity
- only authenticated principals can mutate that environment
- customer workloads can expose managed public endpoints with TLS and request auth
- support staff can inspect health and usage without direct node login

## Milestone 2: Shared Multi-Tenant Control Plane

### Outcome

Multiple customers can safely share Selium control-plane and runtime infrastructure.

### Scope

- make tenancy a real security and accounting boundary
- add quotas and admission control
- add tenant-aware scheduling and observability

### Required Specifications

#### Tenant-aware identity and policy

- Introduce stable IDs for organizations, projects, environments, users, and service accounts.
- Support OIDC/SSO for user login.
- Support short-lived API tokens for automation.
- Enforce tenant ownership on every control-plane mutation, query, replay, and discovery result.

#### Quotas and limits

- Define hard and soft quotas for:
  - running vCPU
  - memory GiB
  - persistent block storage GiB
  - monthly bandwidth
  - object/blob storage GiB
  - deployment count and endpoint count
- Evaluate quotas before scheduling and before volume creation or resize.
- Emit explicit quota events when requests are rejected or throttled.

#### Isolation model

- Define which workloads may share a node.
- Make the `microvm` profile executable or remove it from supported hosted offerings until implemented.
- Add noisy-neighbor protections for CPU and memory pressure.

### Exit Criteria

- one tenant cannot view or mutate another tenant's control-plane state
- quota exhaustion blocks new usage deterministically
- billing and observability pipelines can attribute all usage to a single tenant and environment
- hosted isolation claims match actual runtime behavior

## Milestone 3: Usage Metering and Rating

### Outcome

Selium can produce accurate, auditable usage records and rate them against a pricing catalog.

### Scope

- build the internal utility ledger before full invoicing automation
- make usage attribution trustworthy enough for customer-visible dashboards

### Required Specifications

#### Canonical billable units

- CPU: vCPU-seconds from process CPU time
- Memory: GiB-hours from sampled resident memory byte-ms
- Bandwidth: ingress bytes and egress bytes at the customer boundary
- Block storage: provisioned GiB-hours

Deferred until later unless required for pricing:

- block IOPS / throughput
- request counts for ingress
- snapshot storage

#### Usage event schema

Every rated usage event must include:

- `event_id`
- `tenant_id`
- `project_id`
- `environment_id`
- `workload_id`
- `instance_id`
- `node_id`
- `resource_kind`
- `usage_start_at`
- `usage_end_at`
- `quantity`
- `unit`
- `source`
- `idempotency_key`
- `pricing_dimension_version`

#### Metering pipeline

- Collect raw samples in the runtime.
- Aggregate into fixed windows in a metering service.
- Deduplicate on `idempotency_key`.
- Freeze closed billing windows.
- Recompute rated totals deterministically from immutable raw usage.

#### Pricing catalog

- Version the pricing catalog.
- Support per-region prices and plan overrides.
- Support included free allowances and overage pricing.
- Keep rating deterministic for historical invoices after catalog changes.

### Exit Criteria

- usage totals reconcile from raw samples to customer dashboard totals
- metering survives daemon restarts without double counting
- every billable dimension can be attributed to tenant, environment, and workload
- finance can re-rate a billing window from stored raw usage alone

## Milestone 4: Billing, Payments, and Customer Controls

### Outcome

Selium can charge customers automatically and expose account-level controls for spend and limits.

### Scope

- convert rated usage into invoices and payments
- add customer-facing spend controls and alerts

### Required Specifications

#### Billing system

- Generate invoices from rated usage windows.
- Support:
  - monthly invoicing
  - prepaid credits
  - postpaid overage
  - taxes and currency handling
  - credit notes and invoice adjustments
- Record invoice line items by resource dimension and environment.

#### Payments integration

- Store subscription state and payment status.
- Support card and invoice-based payment methods.
- Handle failed payment retries, account suspension, and service restoration.

#### Customer controls

- Budgets and spend alerts.
- Hard spend caps that stop new capacity growth.
- Usage dashboards by environment, workload, and resource type.
- Downloadable invoice and usage reports.

### Exit Criteria

- a customer can see near-real-time usage and end-of-period invoice projections
- invoices match rated usage ledger outputs
- spend caps and delinquency states change control-plane admission behavior
- support can explain any invoice from first principles using stored usage records

## Milestone 5: Public Cloud GA

### Outcome

Selium operates as a public cloud product with regional rollout discipline, reliability targets, and a supported customer experience.

### Scope

- self-service onboarding
- regional capacity management
- production SRE practices
- contractual product boundaries and SLAs

### Required Specifications

#### Reliability and operations

- Define SLOs for:
  - control-plane availability
  - workload start latency
  - ingress availability
  - storage durability
  - metering freshness
- Add paging, alert routing, incident management, and postmortem workflows.
- Automate control-plane upgrades and rollback.
- Add disaster recovery drills with region-level recovery objectives.

#### Product surfaces

- Public API for org/project/environment management.
- Web console for deployments, logs, metrics, usage, billing, and support.
- Customer documentation for limits, pricing, SLAs, and security model.

#### Compliance and trust

- Audit trails for customer and operator actions.
- Secret management and key rotation.
- Encryption at rest for persisted customer data.
- Formal data retention and deletion workflows.

### Exit Criteria

- a new customer can sign up, provision an environment, deploy workloads, and see usage without operator intervention
- on-call can detect and remediate control-plane or metering failures inside defined SLOs
- security and billing controls are strong enough to support contractual commitments

# Recommended Build Order

The practical order is:

1. Milestone 0
2. Milestone 1
3. Milestone 3 foundations in parallel with Milestone 2
4. Milestone 2
5. Milestone 4
6. Milestone 5

The reason to start Milestone 3 foundations early is simple: utility pricing depends on trustworthy measurement, and trustworthy measurement is easier to build before the public customer model becomes harder to change.

# Current Repo Evidence Behind This Assessment

- Tenancy exists mostly as a naming concept in control-plane models and CLI flows.
- Scheduling is driven by node `capacity_slots`, not billable resource requests.
- Daemon access uses mTLS, but session authentication is unfinished and public ingress listeners do not require client auth.
- Storage is file-backed logs and blob stores, not block-volume infrastructure.
- Observability is mainly tracing plus CLI inspection and replay.
- The `microvm` adaptor is declared but not executable.
- The repo itself documents that it is still a first functional cut rather than a production cloud service.
