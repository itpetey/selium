//! Queue control-plane SPI contracts.

use std::{future::Future, pin::Pin, sync::Arc};

use selium_abi::{
    QueueAck, QueueAttach, QueueCommit, QueueCreate, QueueFrameRef, QueueReserve,
    QueueReserveResult, QueueStatsData, QueueStatus, QueueWaitResult,
};

use crate::{
    guest_error::GuestError,
    services::queue_service::{QueueEndpoint, QueueState},
};

/// Boxed reserve future for queue capabilities.
pub type QueueReserveFuture<E> =
    Pin<Box<dyn Future<Output = Result<QueueReserveResult, E>> + Send + 'static>>;
/// Boxed wait future for queue capabilities.
pub type QueueWaitFuture<E> =
    Pin<Box<dyn Future<Output = Result<QueueWaitResult, E>> + Send + 'static>>;

/// Capability responsible for queue control-plane operations.
pub trait QueueCapability {
    /// Runtime-specific error type.
    type Error: Into<GuestError>;

    /// Create a queue state object from guest parameters.
    fn create(&self, input: QueueCreate) -> Result<QueueState, Self::Error>;

    /// Attach a queue endpoint for the requested role.
    fn attach(&self, queue: &QueueState, input: QueueAttach) -> Result<QueueEndpoint, Self::Error>;

    /// Reserve capacity for one writer frame.
    fn reserve(
        &self,
        endpoint: &QueueEndpoint,
        input: QueueReserve,
    ) -> QueueReserveFuture<Self::Error>;

    /// Commit a writer reservation.
    fn commit(
        &self,
        endpoint: &QueueEndpoint,
        input: QueueCommit,
    ) -> Result<QueueStatus, Self::Error>;

    /// Cancel a writer reservation.
    fn cancel(
        &self,
        endpoint: &QueueEndpoint,
        reservation_id: u64,
    ) -> Result<QueueStatus, Self::Error>;

    /// Wait for the next readable frame.
    fn wait(&self, endpoint: &QueueEndpoint, timeout_ms: u32) -> QueueWaitFuture<Self::Error>;

    /// Acknowledge a delivered frame.
    fn ack(&self, endpoint: &QueueEndpoint, input: QueueAck) -> Result<QueueStatus, Self::Error>;

    /// Close queue globally.
    fn close_queue(&self, queue: &QueueState) -> Result<QueueStatus, Self::Error>;

    /// Close one endpoint.
    fn close_endpoint(&self, endpoint: QueueEndpoint) -> Result<QueueStatus, Self::Error>;

    /// Return queue stats snapshot.
    fn stats(&self, queue: &QueueState) -> Result<QueueStatsData, Self::Error>;

    /// Validate that a frame payload reference is well-formed for queue rules.
    fn validate_frame_ref(
        &self,
        endpoint: &QueueEndpoint,
        frame: &QueueFrameRef,
    ) -> Result<QueueStatus, Self::Error>;
}

impl<T> QueueCapability for Arc<T>
where
    T: QueueCapability,
{
    type Error = T::Error;

    fn create(&self, input: QueueCreate) -> Result<QueueState, Self::Error> {
        self.as_ref().create(input)
    }

    fn attach(&self, queue: &QueueState, input: QueueAttach) -> Result<QueueEndpoint, Self::Error> {
        self.as_ref().attach(queue, input)
    }

    fn reserve(
        &self,
        endpoint: &QueueEndpoint,
        input: QueueReserve,
    ) -> QueueReserveFuture<Self::Error> {
        self.as_ref().reserve(endpoint, input)
    }

    fn commit(
        &self,
        endpoint: &QueueEndpoint,
        input: QueueCommit,
    ) -> Result<QueueStatus, Self::Error> {
        self.as_ref().commit(endpoint, input)
    }

    fn cancel(
        &self,
        endpoint: &QueueEndpoint,
        reservation_id: u64,
    ) -> Result<QueueStatus, Self::Error> {
        self.as_ref().cancel(endpoint, reservation_id)
    }

    fn wait(&self, endpoint: &QueueEndpoint, timeout_ms: u32) -> QueueWaitFuture<Self::Error> {
        self.as_ref().wait(endpoint, timeout_ms)
    }

    fn ack(&self, endpoint: &QueueEndpoint, input: QueueAck) -> Result<QueueStatus, Self::Error> {
        self.as_ref().ack(endpoint, input)
    }

    fn close_queue(&self, queue: &QueueState) -> Result<QueueStatus, Self::Error> {
        self.as_ref().close_queue(queue)
    }

    fn close_endpoint(&self, endpoint: QueueEndpoint) -> Result<QueueStatus, Self::Error> {
        self.as_ref().close_endpoint(endpoint)
    }

    fn stats(&self, queue: &QueueState) -> Result<QueueStatsData, Self::Error> {
        self.as_ref().stats(queue)
    }

    fn validate_frame_ref(
        &self,
        endpoint: &QueueEndpoint,
        frame: &QueueFrameRef,
    ) -> Result<QueueStatus, Self::Error> {
        self.as_ref().validate_frame_ref(endpoint, frame)
    }
}
