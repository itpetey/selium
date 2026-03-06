//! Queue control-plane service for host-managed messaging.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use selium_abi::{
    QueueAck, QueueAttach, QueueCommit, QueueCreate, QueueDelivery, QueueFrameRef, QueueOverflow,
    QueueReservation, QueueReservationId, QueueReserve, QueueReserveResult, QueueRole, QueueSeq,
    QueueStatsData, QueueStatus, QueueStatusCode, QueueWaitResult,
};
use tokio::{sync::Notify, time::timeout};

use crate::{
    guest_error::GuestError,
    spi::queue::{QueueCapability, QueueReserveFuture, QueueWaitFuture},
};

type ReaderId = u64;

#[derive(Clone)]
struct QueueConfig {
    capacity_frames: u32,
    max_frame_bytes: u32,
    delivery: QueueDelivery,
    overflow: QueueOverflow,
}

struct QueueFrame {
    seq: QueueSeq,
    writer_id: u32,
    shm_shared_id: u64,
    offset: u32,
    len: u32,
}

struct QueueReservationState {
    seq: QueueSeq,
    writer_id: u32,
    len: u32,
}

struct QueueReaderState {
    next_seq: QueueSeq,
}

struct QueueRuntimeState {
    closed: bool,
    next_seq: QueueSeq,
    next_reader_id: ReaderId,
    next_reservation_id: QueueReservationId,
    writers: u32,
    readers: HashMap<ReaderId, QueueReaderState>,
    reservations: HashMap<QueueReservationId, QueueReservationState>,
    frames: VecDeque<QueueFrame>,
}

struct QueueInner {
    config: QueueConfig,
    state: Mutex<QueueRuntimeState>,
    readable: Notify,
    writable: Notify,
}

#[derive(Clone)]
pub struct QueueState {
    inner: Arc<QueueInner>,
}

enum QueueEndpointRole {
    Reader { reader_id: ReaderId },
    Writer { writer_id: u32 },
}

struct QueueEndpointInner {
    queue: Arc<QueueInner>,
    role: QueueEndpointRole,
}

#[derive(Clone)]
pub struct QueueEndpoint {
    inner: Arc<QueueEndpointInner>,
}

/// Kernel-owned queue capability implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueueService;

impl QueueRuntimeState {
    fn has_capacity(&self, limit: u32) -> bool {
        let used = self.frames.len().saturating_add(self.reservations.len());
        used < usize::try_from(limit).unwrap_or(usize::MAX)
    }
}

impl Drop for QueueEndpointInner {
    fn drop(&mut self) {
        if let Ok(mut state) = self.queue.state.lock() {
            match self.role {
                QueueEndpointRole::Reader { reader_id } => {
                    state.readers.remove(&reader_id);
                    prune_frames(&mut state);
                    self.queue.writable.notify_waiters();
                }
                QueueEndpointRole::Writer { .. } => {
                    if state.writers > 0 {
                        state.writers -= 1;
                    }
                    self.queue.writable.notify_waiters();
                }
            }
        }
    }
}

impl QueueService {
    fn reserve_with_timeout(
        endpoint: QueueEndpoint,
        input: QueueReserve,
    ) -> QueueReserveFuture<GuestError> {
        Box::pin(async move {
            let QueueEndpointRole::Writer { writer_id } = endpoint.inner.role else {
                return Ok(QueueReserveResult {
                    code: QueueStatusCode::PermissionDenied,
                    reservation: None,
                });
            };

            if input.len == 0 {
                return Ok(QueueReserveResult {
                    code: QueueStatusCode::InvalidArgument,
                    reservation: None,
                });
            }

            if input.len > endpoint.inner.queue.config.max_frame_bytes {
                return Ok(QueueReserveResult {
                    code: QueueStatusCode::TooLarge,
                    reservation: None,
                });
            }

            let timeout_duration = Duration::from_millis(u64::from(input.timeout_ms));
            loop {
                {
                    let mut state = endpoint.inner.queue.state.lock().map_err(|_| {
                        GuestError::Subsystem("queue state lock poisoned".to_string())
                    })?;

                    if state.closed {
                        return Ok(QueueReserveResult {
                            code: QueueStatusCode::Closed,
                            reservation: None,
                        });
                    }

                    if state.has_capacity(endpoint.inner.queue.config.capacity_frames) {
                        let reservation_id = state.next_reservation_id;
                        state.next_reservation_id = state.next_reservation_id.saturating_add(1);
                        let seq = state.next_seq;
                        state.next_seq = state.next_seq.saturating_add(1);

                        state.reservations.insert(
                            reservation_id,
                            QueueReservationState {
                                seq,
                                writer_id,
                                len: input.len,
                            },
                        );

                        return Ok(QueueReserveResult {
                            code: QueueStatusCode::Ok,
                            reservation: Some(QueueReservation {
                                reservation_id,
                                seq,
                            }),
                        });
                    }

                    match endpoint.inner.queue.config.overflow {
                        QueueOverflow::DropNewest => {
                            return Ok(QueueReserveResult {
                                code: QueueStatusCode::Full,
                                reservation: None,
                            });
                        }
                        QueueOverflow::DropOldest => {
                            if !drop_oldest_frame(&mut state) {
                                return Ok(QueueReserveResult {
                                    code: QueueStatusCode::Full,
                                    reservation: None,
                                });
                            }
                            continue;
                        }
                        QueueOverflow::Block => {}
                    }
                }

                if input.timeout_ms == 0 {
                    return Ok(QueueReserveResult {
                        code: QueueStatusCode::WouldBlock,
                        reservation: None,
                    });
                }

                let wait = endpoint.inner.queue.writable.notified();
                if timeout(timeout_duration, wait).await.is_err() {
                    return Ok(QueueReserveResult {
                        code: QueueStatusCode::Timeout,
                        reservation: None,
                    });
                }
            }
        })
    }

    fn wait_with_timeout(endpoint: QueueEndpoint, timeout_ms: u32) -> QueueWaitFuture<GuestError> {
        Box::pin(async move {
            let QueueEndpointRole::Reader { reader_id } = endpoint.inner.role else {
                return Ok(QueueWaitResult {
                    code: QueueStatusCode::PermissionDenied,
                    frame: None,
                });
            };

            let timeout_duration = Duration::from_millis(u64::from(timeout_ms));
            loop {
                {
                    let mut state = endpoint.inner.queue.state.lock().map_err(|_| {
                        GuestError::Subsystem("queue state lock poisoned".to_string())
                    })?;

                    let Some(next_seq) =
                        state.readers.get(&reader_id).map(|reader| reader.next_seq)
                    else {
                        return Ok(QueueWaitResult {
                            code: QueueStatusCode::NotFound,
                            frame: None,
                        });
                    };
                    let frame_opt =
                        state
                            .frames
                            .iter()
                            .find(|frame| frame.seq >= next_seq)
                            .map(|frame| QueueFrameRef {
                                seq: frame.seq,
                                writer_id: frame.writer_id,
                                shm_shared_id: frame.shm_shared_id,
                                offset: frame.offset,
                                len: frame.len,
                            });
                    if let Some(frame) = frame_opt {
                        if frame.seq > next_seq {
                            match endpoint.inner.queue.config.delivery {
                                QueueDelivery::Lossless => {
                                    if let Some(reader) = state.readers.get_mut(&reader_id) {
                                        reader.next_seq = frame.seq;
                                    }
                                    return Ok(QueueWaitResult {
                                        code: QueueStatusCode::ReaderBehind,
                                        frame: None,
                                    });
                                }
                                QueueDelivery::BestEffort => {
                                    if let Some(reader) = state.readers.get_mut(&reader_id) {
                                        reader.next_seq = frame.seq;
                                    }
                                }
                            }
                        }

                        return Ok(QueueWaitResult {
                            code: QueueStatusCode::Ok,
                            frame: Some(frame),
                        });
                    }

                    if state.closed {
                        return Ok(QueueWaitResult {
                            code: QueueStatusCode::Closed,
                            frame: None,
                        });
                    }
                }

                if timeout_ms == 0 {
                    return Ok(QueueWaitResult {
                        code: QueueStatusCode::WouldBlock,
                        frame: None,
                    });
                }

                let wait = endpoint.inner.queue.readable.notified();
                if timeout(timeout_duration, wait).await.is_err() {
                    return Ok(QueueWaitResult {
                        code: QueueStatusCode::Timeout,
                        frame: None,
                    });
                }
            }
        })
    }
}

impl QueueCapability for QueueService {
    type Error = GuestError;

    fn create(&self, input: QueueCreate) -> Result<QueueState, Self::Error> {
        if input.capacity_frames == 0 || input.max_frame_bytes == 0 {
            return Err(GuestError::InvalidArgument);
        }

        let inner = QueueInner {
            config: QueueConfig {
                capacity_frames: input.capacity_frames,
                max_frame_bytes: input.max_frame_bytes,
                delivery: input.delivery,
                overflow: input.overflow,
            },
            state: Mutex::new(QueueRuntimeState {
                closed: false,
                next_seq: 0,
                next_reader_id: 1,
                next_reservation_id: 1,
                writers: 0,
                readers: HashMap::new(),
                reservations: HashMap::new(),
                frames: VecDeque::new(),
            }),
            readable: Notify::new(),
            writable: Notify::new(),
        };

        Ok(QueueState {
            inner: Arc::new(inner),
        })
    }

    fn attach(&self, queue: &QueueState, input: QueueAttach) -> Result<QueueEndpoint, Self::Error> {
        let mut state = queue
            .inner
            .state
            .lock()
            .map_err(|_| GuestError::Subsystem("queue state lock poisoned".to_string()))?;
        if state.closed {
            return Err(GuestError::NotFound);
        }

        let role = match input.role {
            QueueRole::Reader => {
                let reader_id = state.next_reader_id;
                state.next_reader_id = state.next_reader_id.saturating_add(1);
                let initial_seq = state
                    .frames
                    .front()
                    .map(|f| f.seq)
                    .unwrap_or(state.next_seq);
                state.readers.insert(
                    reader_id,
                    QueueReaderState {
                        next_seq: initial_seq,
                    },
                );
                QueueEndpointRole::Reader { reader_id }
            }
            QueueRole::Writer { writer_id } => {
                state.writers = state.writers.saturating_add(1);
                QueueEndpointRole::Writer { writer_id }
            }
        };

        Ok(QueueEndpoint {
            inner: Arc::new(QueueEndpointInner {
                queue: Arc::clone(&queue.inner),
                role,
            }),
        })
    }

    fn reserve(
        &self,
        endpoint: &QueueEndpoint,
        input: QueueReserve,
    ) -> QueueReserveFuture<Self::Error> {
        Self::reserve_with_timeout(endpoint.clone(), input)
    }

    fn commit(
        &self,
        endpoint: &QueueEndpoint,
        input: QueueCommit,
    ) -> Result<QueueStatus, Self::Error> {
        let QueueEndpointRole::Writer { writer_id } = endpoint.inner.role else {
            return Ok(QueueStatus {
                code: QueueStatusCode::PermissionDenied,
            });
        };

        let mut state = endpoint
            .inner
            .queue
            .state
            .lock()
            .map_err(|_| GuestError::Subsystem("queue state lock poisoned".to_string()))?;
        if state.closed {
            return Ok(QueueStatus {
                code: QueueStatusCode::Closed,
            });
        }

        let Some(reservation) = state.reservations.remove(&input.reservation_id) else {
            return Ok(QueueStatus {
                code: QueueStatusCode::StaleReservation,
            });
        };

        if reservation.writer_id != writer_id || reservation.len != input.len {
            return Ok(QueueStatus {
                code: QueueStatusCode::StaleReservation,
            });
        }

        state.frames.push_back(QueueFrame {
            seq: reservation.seq,
            writer_id,
            shm_shared_id: input.shm_shared_id,
            offset: input.offset,
            len: input.len,
        });
        endpoint.inner.queue.readable.notify_waiters();

        Ok(QueueStatus {
            code: QueueStatusCode::Ok,
        })
    }

    fn cancel(
        &self,
        endpoint: &QueueEndpoint,
        reservation_id: u64,
    ) -> Result<QueueStatus, Self::Error> {
        let QueueEndpointRole::Writer { writer_id } = endpoint.inner.role else {
            return Ok(QueueStatus {
                code: QueueStatusCode::PermissionDenied,
            });
        };

        let mut state = endpoint
            .inner
            .queue
            .state
            .lock()
            .map_err(|_| GuestError::Subsystem("queue state lock poisoned".to_string()))?;
        if state.closed {
            return Ok(QueueStatus {
                code: QueueStatusCode::Closed,
            });
        }

        let Some(reservation) = state.reservations.get(&reservation_id) else {
            return Ok(QueueStatus {
                code: QueueStatusCode::StaleReservation,
            });
        };
        if reservation.writer_id != writer_id {
            return Ok(QueueStatus {
                code: QueueStatusCode::StaleReservation,
            });
        }
        state.reservations.remove(&reservation_id);
        endpoint.inner.queue.writable.notify_waiters();

        Ok(QueueStatus {
            code: QueueStatusCode::Ok,
        })
    }

    fn wait(&self, endpoint: &QueueEndpoint, timeout_ms: u32) -> QueueWaitFuture<Self::Error> {
        Self::wait_with_timeout(endpoint.clone(), timeout_ms)
    }

    fn ack(&self, endpoint: &QueueEndpoint, input: QueueAck) -> Result<QueueStatus, Self::Error> {
        let QueueEndpointRole::Reader { reader_id } = endpoint.inner.role else {
            return Ok(QueueStatus {
                code: QueueStatusCode::PermissionDenied,
            });
        };

        let mut state = endpoint
            .inner
            .queue
            .state
            .lock()
            .map_err(|_| GuestError::Subsystem("queue state lock poisoned".to_string()))?;
        if state.closed {
            return Ok(QueueStatus {
                code: QueueStatusCode::Closed,
            });
        }

        let Some(reader) = state.readers.get_mut(&reader_id) else {
            return Ok(QueueStatus {
                code: QueueStatusCode::NotFound,
            });
        };

        if input.seq > reader.next_seq {
            return Ok(QueueStatus {
                code: QueueStatusCode::InvalidArgument,
            });
        }
        if input.seq == reader.next_seq {
            reader.next_seq = reader.next_seq.saturating_add(1);
        }

        prune_frames(&mut state);
        endpoint.inner.queue.writable.notify_waiters();

        Ok(QueueStatus {
            code: QueueStatusCode::Ok,
        })
    }

    fn close_queue(&self, queue: &QueueState) -> Result<QueueStatus, Self::Error> {
        let mut state = queue
            .inner
            .state
            .lock()
            .map_err(|_| GuestError::Subsystem("queue state lock poisoned".to_string()))?;
        state.closed = true;
        state.reservations.clear();
        queue.inner.readable.notify_waiters();
        queue.inner.writable.notify_waiters();

        Ok(QueueStatus {
            code: QueueStatusCode::Ok,
        })
    }

    fn close_endpoint(&self, endpoint: QueueEndpoint) -> Result<QueueStatus, Self::Error> {
        drop(endpoint);
        Ok(QueueStatus {
            code: QueueStatusCode::Ok,
        })
    }

    fn stats(&self, queue: &QueueState) -> Result<QueueStatsData, Self::Error> {
        let state = queue
            .inner
            .state
            .lock()
            .map_err(|_| GuestError::Subsystem("queue state lock poisoned".to_string()))?;

        Ok(QueueStatsData {
            depth_frames: truncate_to_u32(state.frames.len()),
            reservations: truncate_to_u32(state.reservations.len()),
            readers: truncate_to_u32(state.readers.len()),
            writers: state.writers,
            closed: state.closed,
        })
    }

    fn validate_frame_ref(
        &self,
        endpoint: &QueueEndpoint,
        frame: &QueueFrameRef,
    ) -> Result<QueueStatus, Self::Error> {
        let QueueEndpointRole::Writer { .. } = endpoint.inner.role else {
            return Ok(QueueStatus {
                code: QueueStatusCode::PermissionDenied,
            });
        };

        let max_len = endpoint.inner.queue.config.max_frame_bytes;
        if frame.len == 0 || frame.len > max_len {
            return Ok(QueueStatus {
                code: QueueStatusCode::TooLarge,
            });
        }

        let end = frame.offset.checked_add(frame.len);
        if end.is_none() {
            return Ok(QueueStatus {
                code: QueueStatusCode::PayloadOutOfBounds,
            });
        }

        Ok(QueueStatus {
            code: QueueStatusCode::Ok,
        })
    }
}

fn drop_oldest_frame(state: &mut QueueRuntimeState) -> bool {
    let Some(frame) = state.frames.pop_front() else {
        return false;
    };

    let next = frame.seq.saturating_add(1);
    for reader in state.readers.values_mut() {
        if reader.next_seq <= frame.seq {
            reader.next_seq = next;
        }
    }
    true
}

fn prune_frames(state: &mut QueueRuntimeState) {
    if state.frames.is_empty() {
        return;
    }

    let min_seq = state
        .readers
        .values()
        .map(|reader| reader.next_seq)
        .min()
        .unwrap_or(state.next_seq);

    while let Some(front) = state.frames.front() {
        if front.seq < min_seq {
            let _ = state.frames.pop_front();
        } else {
            break;
        }
    }
}

fn truncate_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_abi::{QueueRole, QueueStatusCode};

    #[tokio::test]
    async fn queue_round_trip_reserve_commit_wait_ack() {
        let service = QueueService;
        let queue = service
            .create(QueueCreate {
                capacity_frames: 8,
                max_frame_bytes: 64,
                delivery: QueueDelivery::Lossless,
                overflow: QueueOverflow::Block,
            })
            .expect("create queue");
        let writer = service
            .attach(
                &queue,
                QueueAttach {
                    shared_id: 0,
                    role: QueueRole::Writer { writer_id: 9 },
                },
            )
            .expect("attach writer");
        let reader = service
            .attach(
                &queue,
                QueueAttach {
                    shared_id: 0,
                    role: QueueRole::Reader,
                },
            )
            .expect("attach reader");

        let reserved = service
            .reserve(
                &writer,
                QueueReserve {
                    endpoint_id: 0,
                    len: 4,
                    timeout_ms: 0,
                },
            )
            .await
            .expect("reserve");
        assert_eq!(reserved.code, QueueStatusCode::Ok);
        let reservation = reserved.reservation.expect("reservation");
        service
            .commit(
                &writer,
                QueueCommit {
                    endpoint_id: 0,
                    reservation_id: reservation.reservation_id,
                    shm_shared_id: 42,
                    offset: 3,
                    len: 4,
                },
            )
            .expect("commit");

        let waited = service.wait(&reader, 0).await.expect("wait");
        assert_eq!(waited.code, QueueStatusCode::Ok);
        let frame = waited.frame.expect("frame");
        assert_eq!(frame.writer_id, 9);
        assert_eq!(frame.shm_shared_id, 42);

        let ack = service
            .ack(
                &reader,
                QueueAck {
                    endpoint_id: 0,
                    seq: frame.seq,
                },
            )
            .expect("ack");
        assert_eq!(ack.code, QueueStatusCode::Ok);
    }
}
