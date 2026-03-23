# Selium

In a sentence, Selium is a software-defined/pureplay cloud infrastructure. Our goal is to create a fully featured public cloud infrastructure that is configured by the software it runs, rather than through external assets like Terraform or Cloudformation.

Selium will be best-in-class and the developer's advocate in the industry.

The project's north star and guiding principle is DEVELOPER ERGONOMICS. Everything is subservient to this goal, except where it introduces security vulnerabilities.

## Motivation

Our worldview is that the current breed of infrastructure (AWS/Azure/etc., Kubernetes, Docker, serverless/lambda functions, Kafka, Fluvio, ...) are designed to imitate the physical world - physical servers, switches, load balancers etc. And where that isn't quite true (e.g. Fluvio), we are still hammered with complex configuration and orchestration requirements.

These requirements serve to preclude developers from the infrastructure that runs _their_ projects. Thus we have to rely on DevOps teams, creating significant friction, management headaches, project delays, and cost.

In our opinion, developers (either human or AI) should own the _whole_ stack, and Selium is the vehicle empowering them to do so.

## Project context

This is (at least) the third attempt at building the target architecture. In each previous attempt, we found multiple issues that compromised the integrity of the design, such that a new prototype was warranted.

## Target architecture

Core technologies and design choices:
- Multi-tenant from day 1
- Clustered hosts from day 1
- WASM for secure execution
- Data streaming I/O layer with messaging patterns (e.g. pub/sub, RPC, fanout) overlay
- Kubernetes-like architecture: i.e. 'dumb' host, 'smart' guest. In other words, the host should be a minimal hardware interface layer that provides core orchestration and runtime. System guests should own as much of the complexity as possible.
- Capability-driven security model throughout, using resource scopes to limit individual capabilities
- Session-based persistence of capabilities and scopes
- mTLS for authentication of hosts and users ("users" = human developers using the CLI)
- URI-driven addressing and discovery, e.g. `sel://<tenant>/<namespace>[/<subnamespace>..]/<project>/<running process>/<channel>`. NOTE: this is only one example and different hierarchies could exist if needed.
- Each guest process creates a log channel (via proc macro), which is linked to `tracing::Subscriber`
- Dogfooding wherever possible to increase reuse and eliminate tech debt
- `rkyv` for host-guest ABI codec
- `quinn` for QUIC impl
- `hyper` for HTTP impl
- `tracing` for logging
- `clap` for argument parsing
- `tokio` for async
- Raft for distributed consensus

### Crates

`selium-host` binary (crates/host/) is responsible for:
- WASM runtime execution
- Resource (e.g. process, channel, etc.) tracking and capability-driven sharing of resources with WASM processes
- Exposing low-level primitives, e.g. sockets, shared memory, blob storage, append-only log storage, time, async, etc.
- Defining the ABI for host-guest interaction
- Bridging external network protocols
- TLS offload
- Keeping a structured activity log that is both machine and human parseable, and accessible to guests given valid Capability.
- Guest process CPU, memory, storage, and bandwidth metering

`selium-guest` library (crates/guest/) is responsible for:
- Wrapping the host ABI
- Providing ergonomic handles for guests to consume host resources
- Re-export tracing macros: `info!()`, `warn!()`, etc.

`selium-guest-macros` library (crates/guest/macros/) is responsible for:
- Defining procedural macros to assist with guest entrypoint function boilerplate

`selium` CLI binary (crates/cli/) is responsible for:
- Orchestrating hosts
- Starting/stopping guest processes
- Uploading/deleting WASM modules
- Ad hoc reading of channel data, e.g. outputting process logs to stdout
- Uses `selium-client` for heavy lifting

`selium-client` library (crates/client/) is responsible for:
- Wrapping the guest API
- Providing ergonomic handles for external code to interact with guests and hosts

`selium-external-api` guest (modules/external-api/) is responsible for:
- Creating/running the API server using an RPC channel
- Bridging channel to QUIC socket

`selium-supervisor` guest (modules/supervisor/) is responsible for:
- Listening to host activity log for new guest processes
- Monitoring guest processes
- Handling errors and unplanned process termination using a rules-based setup

`selium-scheduler` guest (modules/router/) is responsible for:
- Starting and stopping guest processes (via host ABI)
- Creating and destroying I/O channels
- Deciding on placement in a multi-host environment
- It will operate as a state machine using a cluster-shared I/O table - once a new state is committed, every scheduler will reconcile the new state with their current state.

`selium-discovery` guest (modules/discovery/) is responsible for:
- Owning the URI layout
- Keeping a map of URI => host + resource ID
- Providing a discovery service to guests
- Metadata validation, e.g. type checking for channels to ensure that subscribers are receiving the encoded data they expect (not for day 1)

`selium-cluster` guest (modules/cluster/) is responsible for:
- Keeping track of hosts in the cluster
- Providing a live table to `selium-scheduler` so it can make placement decisions based on host load and latency
- Shutdown/update coordination - migrating processes, data, and channels from one host to others (not required for day 1)
- Shared MPMC channel for data exchange
- Shared consensus channel for coordination
- Update DNS TXT record

NOTE: Other crates can exist outside these core crates. This is just a starting point.

### I/O subsystem

Channel-based byte streaming.

Composable features:
- Messaging pattern overlays
- Consensus algorithm overlay (Raft impl)
- Network bridging
- Codecs for typed read/write handles and protocols (e.g. HTTP, QUIC etc.)
- Rust-native stream/sink impls for maximum composability
- Durable channels with replay
- Delivery guarantees (defaults to "at most once")
- Live tables, which are durable channels, indexed by key (string)

### Clustering

Flat hierarchy with each Selium host acting as a first-class citizen.

For external discovery, `selium-cluster` will maintain a DNS TXT record with n random server addresses. External clients will use that record to randomly choose one host to contact. Because schedulers operate on a shared data model, any host can handle any request.

If a host is too busy, it should reply with the address of a different host to contact. This may happen repeatedly until the `selium-client` lands on a host that is capable of handling the request.

## Prior art

We should learn from previous projects and take whatever code/patterns/etc. that are useful for building this prototype without compromising our architectural goals.

Prototypes in order of age (oldest-newest):
1. ../main/ AND ../../selium-modules/main/ - this is the current public project, spread across 2 repositories
2. ../newkernel/ AND ../../selium-modules/newkernel/ - attempted to consolidate crates by removing subsystems and pushing more logic to guests, e.g. I/O
3. ../newarch/ - a greenfield rewrite that is the most complete architectural design, but complexity ballooned without achieving our goals. Consolidated the `selium-modules` repository into the `selium` repository.
4. ../arch2/ - the previous greenfield prototype that was much closely aligned to our target architecture, but had significant gaps in implementation whilst still suffering from architectural impurity.
5. ../arch3/ - the current greenfield prototype

### Observations about prototypes

Each of these prototypes 'failed' because we tried to achieve ambitious goals with technologies that couldn't quite facilitate them. For example, `newkernel` (and later prototypes) tried introducing shared memory to manage I/O channels, but because `wasmtime` didn't support host-swap shared memories, we had to fall back on a host-managed queue layer that complicated the codebase. Similarly, `arch2` tried to implement guest process migration using `wasmtime`, which doesn't support guest snapshotting, leading to a suboptimal solution.

## Future work and ideas

- Trait-based RPC (see arch2)
- IDL for data types + codec choice for I/O (linked to trait-based RPC)
- Channel replicas
