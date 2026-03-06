# Architecture Snapshot

## Objectives

1. Maximise developer experience for deploy/run/scale/communicate workflows.
2. Keep strict capability isolation.
3. Offer first-class RPC, durable events, and byte streams under one programming model.

## Design Pillars

1. Control plane is logically centralised but physically distributed.
2. Node agents reconcile desired state onto local runtimes.
3. Runtime adapters abstract execution engines (Wasmtime default).
4. Shared memory fast-path is an implementation detail, not the primary developer API.

## Delivery Guarantees

1. Durable event channels: at-least-once.
2. Runtime-managed checkpoints.
3. Replay selectors: earliest/latest/sequence/timestamp/checkpoint.
