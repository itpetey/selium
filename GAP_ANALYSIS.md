# Goal

Turn Selium from its current operator-oriented distributed runtime into a commercial cloud platform with trustworthy utility billing for compute, memory, network, and storage.

# Current Repo Baseline

As of 2026-03-13, the repo is materially ahead of the older "prototype with slots and logs" assessment. The codebase now has a usable operator/runtime substrate with partial resource accounting, partial attribution, and partial authorization.

## What Is Already Implemented

- A Rust workspace with a runtime daemon, first-party control-plane module, deterministic control-plane engine, scheduler, CLI, guest SDK, and end-to-end examples.
- A control-plane deployment model that already carries:
  - `cpu_millis`
  - `memory_mib`
  - `ephemeral_storage_mib`
  - `bandwidth_profile`
  - `volume_mounts`
  - `external_account_ref`
- Node registration that already advertises:
  - `capacity_slots`
  - `allocatable_cpu_millis`
  - `allocatable_memory_mib`
  - supported isolation profiles
  - daemon address and TLS server name
- A scheduler that places workloads against:
  - isolation compatibility
  - live-node status
  - slot capacity
  - allocatable CPU
  - allocatable memory
- Durable control-plane replay, checkpointing, summary projection, attributed inventory queries, and scoped discovery APIs.
- Runtime usage collection with durable replay/export for:
  - CPU time field
  - memory high watermark
  - memory byte-millis
  - ingress bytes
  - egress bytes
  - storage read bytes
  - storage write bytes
- Attribution plumbing that threads `external_account_ref`, workload key, module id, and instance id through replay, inventory, and runtime usage export.
- Certificate-based daemon authentication that extracts Selium principals from certificate SANs and applies static method/discovery grants.
- Runtime-managed network ingress bindings, network egress profiles, storage logs, and blob stores.
- Managed endpoint bridging across nodes for event, service, and stream endpoints.
- Structured audit metadata in control-plane replay events.

## What The Repo Still Is

Selium is still an operator-facing platform substrate, not a cloud product. The repo can run and reconcile workloads across nodes, but it does not yet implement the customer/account model, billing system, or product operations needed for a hosted commercial service.

# What Changed Since The Previous Analysis

The previous document understated several areas that now exist in code:

- Resource-aware scheduling has started. CPU and memory are first-class deployment and node fields, and the scheduler enforces them.
- Metering has started. Runtime usage samples are durable, attributable, replayable, and exposed through the CLI.
- Authorization has moved beyond "mTLS only". The daemon now resolves principals from certificates and applies scoped access grants.
- Operational introspection is broader than logs alone. The repo now has `observe`, `replay`, `discover`, `inventory`, `usage`, `nodes`, and guest-log subscription flows.

The main remaining gap is not "nothing exists"; it is that several subsystems are only operator-grade foundations and are not yet customer-grade products.

# Updated Gap Assessment

## 1. Customer Model And Tenancy

Today Selium has workload naming and external attribution hooks, but not customer records.

- Present:
  - `tenant/namespace/workload` naming is pervasive in the control-plane model.
  - Pipeline validation already blocks cross-tenant references.
  - `external_account_ref` can tag deployments and pipelines for downstream attribution.
- Missing:
  - organizations
  - projects
  - environments
  - users
  - memberships
  - invitations
  - plans
  - subscriptions
  - account lifecycle state
  - tenant-owned audit history beyond operator mutation logs
- Impact:
  - Selium can attribute infrastructure to an external account key, but it still does not own a first-class customer/account model.

## 2. Authentication And Authorization

Today Selium has certificate-backed principal authentication plus static daemon authorization, but not a hosted identity system.

- Present:
  - principal kinds such as `user`, `machine`, `runtime-peer`, `workload`, and `internal`
  - principal extraction from certificate SAN URIs
  - static `access-grant` policy entries
  - per-method grants
  - scoped discovery permissions by workload and endpoint pattern
- Missing:
  - OIDC or SSO
  - bearer or API token issuance
  - service-account lifecycle
  - customer-facing RBAC model
  - secret management
  - secret rotation workflows
  - customer ingress authentication policies as a product feature
- Impact:
  - the daemon is no longer unauthorised once a client has a cert, but identity and authz are still operator-configured infrastructure rather than product surfaces.

## 3. Resource Model And Scheduling

Resource scheduling is partially implemented, not absent.

- Present:
  - deployment specs declare CPU, memory, ephemeral storage, bandwidth profile, and volume mounts
  - nodes advertise allocatable CPU and memory
  - the scheduler enforces slot, CPU, and memory fit
  - isolation-aware placement exists
- Missing:
  - scheduler enforcement for `ephemeral_storage_mib`
  - scheduler enforcement for `bandwidth_profile`
  - scheduler enforcement for `volume_mounts`
  - quota evaluation
  - preemption
  - overcommit policy
  - autoscaling
  - admission control that rejects under-provisioned requests before reconcile-time failure
  - customer-facing CLI or API ergonomics for setting all resource fields
- Important nuance:
  - the operator CLI still deploys workloads with zeroed resource fields and no `external_account_ref`, even though the control-plane model supports them.
- Impact:
  - Selium now has the beginnings of a billable resource model, but only CPU and memory placement are materially enforced today.

## 4. Metering And Billing

Metering foundations now exist, but billing does not.

- Present:
  - durable runtime usage samples
  - replay cursors and filters
  - attribution by workload, module, instance, and external account reference
  - network and storage byte counters
  - CLI export for machine-readable usage records
- Missing:
  - aggregation windows outside the raw sample log
  - pricing catalog
  - rating engine
  - invoice generation
  - credits
  - payment collection
  - reconciliation workflows
  - finance-grade audit chain
- Important nuance:
  - the current `cpu_time_millis` field is derived from elapsed sample-window time, not host-measured CPU consumption.
- Impact:
  - Selium now has raw usage telemetry, but not trustworthy commercial billing yet.

## 5. Storage Productization

Storage infrastructure exists, but storage product surfaces do not.

- Present:
  - file-backed durable logs
  - file-backed blob stores
  - replay and checkpoint support
  - runtime usage accounting for storage reads and writes
- Missing:
  - tenant-scoped block volumes
  - attach and detach lifecycle
  - resize
  - snapshots as a customer feature
  - storage classes
  - durability tiers
  - encryption-at-rest controls
  - storage quotas
  - storage billing
- Impact:
  - Selium has runtime persistence primitives, not a cloud block-storage product.

## 6. Observability And Cloud Operations

Operational visibility is better than before, but still not cloud-grade.

- Present:
  - control-plane status and metrics RPCs
  - control-plane replay with checkpoints
  - attributed inventory queries
  - scoped discovery
  - node liveness queries
  - guest log subscription and attach flows
  - structured audit fields in replayed control-plane mutations
  - text and JSON tracing output
- Missing:
  - standard scrape/export metrics surface for external observability systems
  - dashboards
  - alerts
  - SLOs
  - backup and restore procedures
  - upgrade orchestration
  - disaster recovery drills
  - support tooling
- Impact:
  - operators can inspect the system, but operating it as a paid managed service would still be fragile.

## 7. Isolation And Runtime Product Boundaries

Isolation is partially modelled, but not fully backed by executable runtime options.

- Present:
  - `Standard`
  - `Hardened`
  - `Microvm`
  - Wasmtime-backed execution for the executable profiles
- Missing:
  - an executable microVM runtime path
  - product-level isolation guarantees tied to billing and tenancy
  - noisy-neighbour controls
  - resource isolation stronger than scheduler placement plus process boundaries
- Impact:
  - the repo exposes a richer isolation model than the runtime can currently deliver for hosted offerings.

## 8. Customer-Facing Product Surface

The current UX is still an operator CLI plus examples.

- Present:
  - CLI commands for deploy, connect, scale, observe, replay, discover, inventory, usage, start, stop, list, and agent flows
  - IDL compile and publish support
  - multiple end-to-end examples
- Missing:
  - self-service control-plane API for customer account management
  - web console
  - onboarding flow
  - support workflow integration
  - customer documentation for limits, pricing, and SLAs
- Impact:
  - Selium is testable and operable by engineers, not yet usable as a self-serve cloud.

# Milestone Status

## Milestone 0: Operator-Grade Private Preview

Status: partially complete.

Already in place:

- multi-node control-plane daemon and scheduler
- allocatable CPU and memory placement
- durable replay and audit metadata
- runtime usage export
- discovery and inventory tooling
- certificate-backed principal authz

Still required to close this milestone cleanly:

- enforce all declared resource dimensions, not just CPU and memory
- surface resource and attribution fields through the operator UX
- define and test backup and restore procedures
- provide a standard observability integration story
- decide whether `microvm` remains declared or is removed from near-term supported offerings

## Milestone 1: Managed Single-Tenant Service

Status: not started as a product milestone.

The code has some prerequisites for this phase, especially attribution hooks and daemon authorization, but there is still no first-class customer, environment, or subscription model.

## Milestone 2: Shared Multi-Tenant Control Plane

Status: not started as a product milestone.

There is no genuine tenant security boundary yet beyond naming, validation rules, and operator-configured access scopes.

## Milestone 3: Usage Metering And Rating

Status: foundation started.

Raw usage collection and export exist. Rating, catalog management, and invoice-grade aggregation do not.

## Milestone 4: Billing, Payments, And Customer Controls

Status: not started.

## Milestone 5: Public Cloud GA

Status: not started.

# Recommended Build Order

The practical order is now:

1. Finish Milestone 0 properly.
2. Build the first-class customer and environment model.
3. Turn raw usage into a proper metering ledger and rating pipeline.
4. Add quota, admission, and tenant isolation.
5. Add billing, spend controls, and payments.
6. Add self-service product surfaces and production operations.

The biggest change from the older plan is that metering no longer needs to start from zero. The immediate priority is to make the existing raw usage path trustworthy and commercially useful.

# Current Repo Evidence Behind This Assessment

- `crates/control-plane/api/src/model.rs`
  - deployment resources, external account attribution, node allocatable CPU and memory, discovery scope model
- `crates/control-plane/scheduler/src/lib.rs`
  - slot, CPU, and memory-aware placement
- `crates/control-plane/runtime/src/inventory.rs`
  - attributed workload, pipeline, module, and node inventory views
- `modules/control-plane/src/lib.rs`
  - control-plane status, metrics, replay, checkpointing, and audit field generation
- `crates/runtime/service/src/auth.rs`
  - principal extraction from certificates and static method/discovery access grants
- `crates/runtime/service/src/usage.rs`
  - durable runtime usage collection and replay
- `crates/runtime/network/src/lib.rs`
  - network usage accounting hooks
- `crates/runtime/storage/src/lib.rs`
  - storage usage accounting hooks
- `crates/runtime/service/src/main.rs`
  - operator-configured ingress bindings, egress profiles, logs, and blob stores
- `crates/runtime/adaptors/microvm-adaptor/src/lib.rs`
  - non-executable microVM adaptor
- `crates/cli/src/main.rs`
  - current operator UX, including the fact that `deploy` still hardcodes zeroed resource fields
