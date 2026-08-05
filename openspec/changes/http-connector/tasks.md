# Tasks: HTTP Connector

## 1. Protocol types

- [ ] 1.1 Create `selium-proto-http` crate: `HttpRequest`, `HttpResponse`, `HttpBodyChunk`, trailer types via `selium-encoding` schema macros
- [ ] 1.2 Schema round-trip tests incl. chunked body sequences

## 2. Connector guest

- [ ] 2.1 Create `selium-connector-http` guest crate (plain guest on `selium-guest::net`)
- [ ] 2.2 TCP listener + accept loop spawning per-connection channel pairs
- [ ] 2.3 TLS termination with rustls; certificate/key loaded via storage grant; loud failure on missing/invalid cert material
- [ ] 2.4 HTTP/1.1 codec: request parse → `HttpRequest`; typed `HttpResponse` → wire bytes; keep-alive ordering
- [ ] 2.5 Discovery route resolution (Host + path → channel) with cache and invalidation
- [ ] 2.6 In-flight correlation map (connection ordering ↔ frame tags)
- [ ] 2.7 Backpressure: ring-full stops socket reads; resume on generation advance
- [ ] 2.8 Streaming bodies mapped to server-streaming RPC (dependency: `streaming-rpc-patterns`)

## 3. App-guest API

- [ ] 3.1 Typed serve API: register served URI subtree, receive `RpcConnection<HttpRequest, HttpResponse, _>` (+ server-stream variant)
- [ ] 3.2 Document the zero-`Network`-grant capability model and `ExplicitResource` channel hygiene

## 4. Verification

- [ ] 4.1 Golden-path CI: `curl` → connector → app guest → typed response + log line (`cargo test -p selium-runtime`)
- [ ] 4.2 CI: concurrent keep-alive requests preserve ordering/correlation
- [ ] 4.3 CI: slow app guest → connector stops reading (backpressure); no unbounded growth
- [ ] 4.4 CI: app guest with no Network grants serves successfully; attach attempt by an ungranted guest to a connection region is denied
