# Tasks: Streaming RPC Pattern Variants

## 1. Frame discipline

- [ ] 1.1 Define stream flag bits (item / end / cancel) on the existing `FrameHeader` flags field, documented in `framed-io` terms
- [ ] 1.2 Confirm tag-correlation invariants: all frames of one stream share the request's tag

## 2. Server-streaming

- [ ] 2.1 `RpcServerStreamClient<Req, Item, M>`: `call(req) -> impl Stream<Item = Result<Item>>`
- [ ] 2.2 Server side: connection handler produces items until end flag; error terminates with error frame
- [ ] 2.3 Cancellation: client cancel frame → server stops producing; client dropping the stream sends cancel

## 3. Bidi-streaming

- [ ] 3.1 `RpcBidiStream<Req, Item, Resp, M>`: independent send/receive halves over one tag
- [ ] 3.2 Half-close semantics per direction (end flag = that direction closed; peer may continue)

## 4. Backpressure + errors

- [ ] 4.1 Writers park on full ring via generation wait (no spin, no unbounded buffer)
- [ ] 4.2 Transport errors map into the stream's error item per the `MessageTransport` error abstraction

## 5. Verification

- [ ] 5.1 Tests over `ShmTransport`: ordered delivery, end-of-stream, cancel, error mid-stream
- [ ] 5.2 Backpressure test: slow consumer + small ring → producer parks, no item loss, no spin
- [ ] 5.3 Overwrite semantics: streams never report `Overwritten` (bounded/park), asserted in tests
