//! Golden-path demo guest.
//!
//! Exercises the spine of the platform end-to-end inside a real WASM guest:
//! log transport initialisation, shared-memory channel creation, a typed
//! pub/sub round trip, and readiness signalling. The `selium-runtime`
//! `spine` integration test deploys this guest and asserts on its output.

use selium_guest::entrypoint;
use selium_shm::{Channel, ChannelBackpressure, transport::ShmTransport};
use selium_wire::{
    framed::{FramedRead, FramedWrite},
    pubsub::{Publisher, Subscriber},
};

/// Channel capacity for the pub/sub round trip.
const DEMO_CHANNEL_CAPACITY: u64 = 4096;

#[entrypoint]
async fn spine_demo() {
    drop(selium_guest::log::init());
    selium_guest::info!("hello spine");

    let channel = match Channel::create(DEMO_CHANNEL_CAPACITY, ChannelBackpressure::Park) {
        Ok(channel) => channel,
        Err(error) => {
            selium_guest::error!("spine: channel create failed: {error}");
            return;
        }
    };

    // Create the subscriber transport before publishing so its reader starts
    // at the current tail and observes the message.
    let subscriber_transport = match ShmTransport::new(&channel, &channel) {
        Ok(transport) => transport,
        Err(error) => {
            selium_guest::error!("spine: subscriber transport failed: {error}");
            return;
        }
    };
    let publisher_transport = match ShmTransport::new(&channel, &channel) {
        Ok(transport) => transport,
        Err(error) => {
            selium_guest::error!("spine: publisher transport failed: {error}");
            return;
        }
    };

    let mut subscriber: Subscriber<String, ShmTransport> =
        Subscriber::new(FramedRead::new(subscriber_transport), None);
    let mut publisher: Publisher<String, ShmTransport> =
        Publisher::new(FramedWrite::new(publisher_transport));

    if let Err(error) = publisher.publish(&"ping".to_string()) {
        selium_guest::error!("spine: publish failed: {error}");
        return;
    }

    match subscriber.read_with_tag() {
        Ok((message, _tag)) if message == "ping" => {
            selium_guest::info!("spine: pubsub ok");
        }
        Ok((message, _tag)) => {
            selium_guest::error!("spine: unexpected message: {message}");
        }
        Err(error) => {
            selium_guest::error!("spine: subscribe failed: {error}");
        }
    }

    selium_guest::mark_ready();
}
