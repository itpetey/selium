# Tasks: DNS Connector

## 1. Protocol types

- [ ] 1.1 Create `selium-proto-dns`: `DnsQuery`, `DnsResponse`, record enums (A/AAAA/CNAME/…), outcome variants (Ok/NxDomain/Timeout/Truncated) via `selium-encoding`
- [ ] 1.2 Wire-format codec: schema type ↔ real DNS message (encode query, parse response), with tests against known-good packets

## 2. Connector guest

- [ ] 2.1 Create `selium-connector-dns` guest: raw `UdpSocket` bound via grant, upstream resolver address from entrypoint arg/config grant
- [ ] 2.2 Accept typed `DnsQuery` on its well-known channel; allocate txid; emit wire query
- [ ] 2.3 In-flight map `(txid, resolver addr) → (reply channel, tag)`; demux replies; drop unknown txids
- [ ] 2.4 Typed failure mapping: timeout / NXDOMAIN / truncation each produce distinct `DnsResponse` outcomes
- [ ] 2.5 Register well-known URI with discovery at boot

## 3. Guest API

- [ ] 3.1 `selium_guest::net::resolve(name) -> Result<Vec<IpAddr>>`: discovery attach + unary RPC + typed outcome → address list or error

## 4. Verification

- [ ] 4.1 CI: fake upstream UDP DNS server (loopback) → guest resolves `example.test` to `127.0.0.1`, then `TcpConnect`s to the literal
- [ ] 4.2 CI: NXDOMAIN and timeout surface as distinct typed errors
- [ ] 4.3 CI: a guest without a grant for the connector channel cannot resolve
- [ ] 4.4 CI: reply with unknown txid is dropped (no cross-talk between concurrent queries)
