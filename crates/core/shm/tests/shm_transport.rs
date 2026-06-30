//! End-to-end smoke test for `selium-wire` patterns over `selium-shm` transports.

use selium_abi::ResourceKind;
use selium_encoding::FlatMsg;
use selium_shm::{Channel, ChannelBackpressure, ShmTransport};
use selium_wire::{
    FramedRead, FramedWrite,
    rpc::{Rendezvous, RpcClient, RpcConnection},
};

#[derive(Debug, Clone, PartialEq)]
struct Ping(String);

#[derive(Debug, Clone, PartialEq)]
struct Pong(String);

impl FlatMsg for Ping {
    fn encode(value: &Self) -> Vec<u8> {
        value.0.clone().into_bytes()
    }

    fn decode(bytes: &[u8]) -> Result<Self, flatbuffers::InvalidFlatbuffer> {
        Ok(Self(
            String::from_utf8(bytes.to_vec())
                .map_err(|_error| flatbuffers::InvalidFlatbuffer::ApparentSizeTooLarge)?,
        ))
    }
}

impl FlatMsg for Pong {
    fn encode(value: &Self) -> Vec<u8> {
        value.0.clone().into_bytes()
    }

    fn decode(bytes: &[u8]) -> Result<Self, flatbuffers::InvalidFlatbuffer> {
        Ok(Self(
            String::from_utf8(bytes.to_vec())
                .map_err(|_error| flatbuffers::InvalidFlatbuffer::ApparentSizeTooLarge)?,
        ))
    }
}

fn install_heap_provider() {
    drop(selium_memory::set_region_provider(Box::new(selium_memory::HeapRegionProvider::new())));
}

fn make_channel(capacity: u64) -> Channel {
    Channel::create_with_backpressure(
        capacity,
        ChannelBackpressure::Drop,
        ResourceKind::SharedMemory,
    )
    .expect("create channel")
}

#[tokio::test]
async fn rpc_round_trip_over_shm_transport() {
    install_heap_provider();

    // Two unidirectional channels form a full-duplex RPC session.
    let request_channel = make_channel(1024);
    let reply_channel = make_channel(1024);

    let dummy = make_channel(64);

    let client: RpcClient<Ping, Pong, ShmTransport> = RpcClient::new(
        FramedWrite::new(ShmTransport::new(&dummy, &request_channel).expect("client tx")),
        FramedRead::new(ShmTransport::new(&reply_channel, &dummy).expect("client rx")),
    );

    let mut server: RpcConnection<Ping, Pong, ShmTransport> = RpcConnection::new(
        FramedRead::new(ShmTransport::new(&request_channel, &dummy).expect("server rx")),
        FramedWrite::new(ShmTransport::new(&dummy, &reply_channel).expect("server tx")),
        42,
    );

    // Drive the server in a background task.
    let server_handle = tokio::spawn(async move {
        let req = server.recv().await.expect("recv request");
        let ping: Ping = req.payload().expect("decode ping");
        req.reply(Pong(format!("reply to {}", ping.0)))
            .await
            .expect("reply");
    });

    let mut client = client;
    let pong = client.request(Ping("hello".to_string())).await.expect("request");
    assert_eq!(pong, Pong("reply to hello".to_string()));

    server_handle.await.expect("server task");
}

#[tokio::test]
async fn pubsub_round_trip_over_shm_transport() {
    install_heap_provider();

    let channel = make_channel(1024);

    // Dummy channels: each transport only uses one real direction for this test.
    let dummy = make_channel(64);

    let mut publisher = selium_wire::Publisher::<Ping, ShmTransport>::new(
        FramedWrite::new(ShmTransport::new(&dummy, &channel).expect("publisher tx")),
    );
    let mut subscriber = selium_wire::Subscriber::<Ping, ShmTransport>::new(
        FramedRead::new(ShmTransport::new(&channel, &dummy).expect("subscriber rx")),
        None,
    );

    publisher
        .publish(&Ping("broadcast".to_string()))
        .expect("publish");

    let (received, _writer_id) = subscriber.read_with_tag().expect("receive");
    assert_eq!(received, Ping("broadcast".to_string()));
}

#[test]
fn framed_write_read_round_trip_over_shm_transport() {
    install_heap_provider();

    let channel = make_channel(1024);
    let dummy = make_channel(64);

    let mut writer = FramedWrite::<ShmTransport>::new(
        ShmTransport::new(&dummy, &channel).expect("writer"),
    );
    let mut reader = FramedRead::<ShmTransport>::new(
        ShmTransport::new(&channel, &dummy).expect("reader"),
    );

    writer.write_frame(b"hello", 42).expect("write frame");
    let (payload, tag) = reader.read_frame().expect("read frame");
    assert_eq!(payload, b"hello");
    assert_eq!(tag, 42);
}

#[test]
fn rendezvous_passes_region_ids() {
    use selium_shm::ShmRendezvous;
    let rendezvous = ShmRendezvous::new();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        rendezvous.send(12345).await.expect("send");
        let conn = rendezvous.recv().await.expect("recv");
        assert_eq!(conn.shared_id, 12345);
    });
}
