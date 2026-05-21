//! Selium guest SDK.

use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use rkyv::{
    api::high::{HighDeserializer, HighValidator},
    rancor::Error as RancorError,
};
use selium_abi::{
    AbiError, CompletionState, HostcallEnvelope, HostcallOutput, HostcallRequest, SignalDescriptor,
    TaskId, decode_rkyv, deframe_bytes, encode_rkyv, frame_bytes, unpack_hostcall_status,
};
use thiserror::Error;

pub use selium_abi::{
    Capability, CapabilityGrant, EntrypointMetadata, InterfaceMetadata, LocalityScope,
    ResourceClass, ResourceIdentity, ResourceSelector, ScopeContext,
};
pub use selium_guest_macros::{entrypoint, pattern_interface};
pub use tracing::{debug, error, info, trace, warn};

pub type Result<T> = std::result::Result<T, GuestError>;

#[derive(Debug, Error)]
pub enum GuestError {
    #[error("host error: {0}")]
    Host(String),
    #[error("codec error: {0}")]
    Codec(#[from] selium_abi::RkyvError),
    #[error("permission denied for capability {0:?}")]
    PermissionDenied(Capability),
    #[error("unexpected hostcall output")]
    UnexpectedHostcallOutput,
}

#[derive(Clone, Debug)]
pub struct Signal {
    descriptor: SignalDescriptor,
}

struct HostcallFuture {
    request: Option<HostcallRequest>,
    operation_id: Option<u64>,
    task_id: Option<TaskId>,
}

struct BackgroundTask {
    id: TaskId,
    future: Pin<Box<dyn Future<Output = ()>>>,
    runnable: bool,
}

struct JoinState<T> {
    result: Option<T>,
    waker: Option<Waker>,
}

pub struct JoinHandle<T> {
    state: Rc<RefCell<JoinState<T>>>,
}

struct YieldNow {
    yielded: bool,
}

struct TaskWake {
    task_id: TaskId,
}

thread_local! {
    static BACKGROUND: RefCell<Vec<BackgroundTask>> = const { RefCell::new(Vec::new()) };
    static SPAWN_QUEUE: RefCell<Vec<BackgroundTask>> = const { RefCell::new(Vec::new()) };
    static WAKE_QUEUE: RefCell<Vec<TaskId>> = const { RefCell::new(Vec::new()) };
    static CURRENT_TASK: RefCell<Option<TaskId>> = const { RefCell::new(None) };
    static NEXT_TASK_ID: RefCell<TaskId> = const { RefCell::new(1) };
}

impl Signal {
    pub fn create() -> Result<Self> {
        match hostcall_ready(HostcallRequest::SignalCreate)? {
            HostcallOutput::Signal(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn attach(shared_id: u64) -> Result<Self> {
        match hostcall_ready(HostcallRequest::SignalAttach { shared_id })? {
            HostcallOutput::Signal(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn descriptor(&self) -> SignalDescriptor {
        self.descriptor
    }

    pub fn local_id(&self) -> u64 {
        self.descriptor.local_id
    }

    pub fn shared_id(&self) -> u64 {
        self.descriptor.shared_id
    }

    pub fn notify(&self) -> Result<u64> {
        match hostcall_ready(HostcallRequest::SignalNotify {
            local_id: self.descriptor.local_id,
        })? {
            HostcallOutput::SignalGeneration(generation) => Ok(generation),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub async fn wait(&self, observed_generation: u64, timeout_ms: u64) -> Result<u64> {
        match hostcall_async(HostcallRequest::SignalWait {
            local_id: self.descriptor.local_id,
            observed_generation,
            timeout_ms,
        })
        .await?
        {
            HostcallOutput::SignalGeneration(generation) => Ok(generation),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn close(self) -> Result<()> {
        match hostcall_ready(HostcallRequest::SignalClose {
            local_id: self.descriptor.local_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}

impl<T> JoinState<T> {
    fn new() -> Self {
        Self {
            result: None,
            waker: None,
        }
    }

    fn complete(&mut self, value: T) {
        self.result = Some(value);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.borrow_mut();
        if let Some(value) = state.result.take() {
            Poll::Ready(value)
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl futures::task::ArcWake for TaskWake {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        wake_task(arc_self.task_id);
    }
}

impl Future for HostcallFuture {
    type Output = Result<HostcallOutput>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.operation_id.is_none() {
            let Some(task_id) = current_task_id() else {
                return Poll::Ready(Err(GuestError::Host(
                    "async hostcall polled outside Selium guest reactor".to_string(),
                )));
            };
            let request = self
                .request
                .take()
                .ok_or_else(|| GuestError::Host("hostcall request already consumed".to_string()))?;
            let envelope = HostcallEnvelope {
                request,
                task_id: Some(task_id),
            };
            let encoded = match encode_rkyv(&envelope) {
                Ok(encoded) => encoded,
                Err(error) => return Poll::Ready(Err(error.into())),
            };
            let create_status = unsafe { selium_hostcall_create(encoded.as_ptr(), encoded.len()) };
            let (status, operation_id) = unpack_hostcall_status(create_status);
            if status == selium_abi::HOSTCALL_STATUS_FAILED {
                return Poll::Ready(Err(GuestError::Host("hostcall create failed".to_string())));
            }
            self.operation_id = Some(operation_id as u64);
            self.task_id = Some(task_id);
        }

        let operation_id = self.operation_id.expect("operation id set above");
        match poll_operation(operation_id) {
            Ok(Some(output)) => {
                unsafe { selium_hostcall_drop(operation_id) };
                self.operation_id = None;
                self.task_id = None;
                Poll::Ready(Ok(output))
            }
            Ok(None) => Poll::Pending,
            Err(error) => {
                unsafe { selium_hostcall_drop(operation_id) };
                self.operation_id = None;
                self.task_id = None;
                Poll::Ready(Err(error))
            }
        }
    }
}

impl Drop for HostcallFuture {
    fn drop(&mut self) {
        if let Some(operation_id) = self.operation_id {
            unsafe { selium_hostcall_drop(operation_id) };
        }
    }
}

pub fn process_id() -> u64 {
    unsafe { selium_process_id() }
}

pub fn mark_ready() {
    unsafe { selium_mark_ready() }
}

pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
{
    let state = Rc::new(RefCell::new(JoinState::new()));
    let state_for_task = Rc::clone(&state);
    let id = next_task_id();
    let task = BackgroundTask {
        id,
        future: Box::pin(async move {
            let output = future.await;
            state_for_task.borrow_mut().complete(output);
        }),
        runnable: true,
    };

    BACKGROUND.with(|tasks| {
        if let Ok(mut tasks) = tasks.try_borrow_mut() {
            tasks.push(task);
        } else {
            SPAWN_QUEUE.with(|queue| queue.borrow_mut().push(task));
        }
    });

    JoinHandle { state }
}

pub async fn yield_now() {
    YieldNow { yielded: false }.await;
}

pub fn poll_reactor() {
    register_mailbox();

    loop {
        drain_mailbox();
        if poll_backgrounds() {
            continue;
        }
        break;
    }
}

pub fn decode_typed<T>(bytes: &[u8]) -> Result<T>
where
    T: rkyv::Archive + Sized,
    for<'a> T::Archived: rkyv::Deserialize<T, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
{
    let payload = deframe_bytes(bytes).map_err(abi_error_to_guest_error)?;
    Ok(decode_rkyv(payload)?)
}

pub fn encode_typed<T>(value: &T) -> Result<Vec<u8>>
where
    T: selium_abi::RkyvEncode,
{
    frame_bytes(&encode_rkyv(value)?).map_err(abi_error_to_guest_error)
}

pub fn run_entrypoint_safely<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        spawn(future);
        poll_safely();
    }));
    if result.is_err() {
        std::process::abort();
    }
}

pub fn poll_safely() {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(poll_reactor));
    if result.is_err() {
        std::process::abort();
    }
}

fn hostcall_ready(request: HostcallRequest) -> Result<HostcallOutput> {
    let envelope = HostcallEnvelope {
        request,
        task_id: None,
    };
    let request = encode_rkyv(&envelope)?;
    let create_status = unsafe { selium_hostcall_create(request.as_ptr(), request.len()) };
    let (status, operation_id) = unpack_hostcall_status(create_status);
    if status == selium_abi::HOSTCALL_STATUS_FAILED {
        return Err(GuestError::Host("hostcall create failed".to_string()));
    }

    match poll_operation(operation_id as u64) {
        Ok(Some(output)) => {
            unsafe { selium_hostcall_drop(operation_id as u64) };
            Ok(output)
        }
        Ok(None) => {
            unsafe { selium_hostcall_drop(operation_id as u64) };
            Err(GuestError::Host(
                "hostcall returned pending; await the async API instead".to_string(),
            ))
        }
        Err(error) => {
            unsafe { selium_hostcall_drop(operation_id as u64) };
            Err(error)
        }
    }
}

fn hostcall_async(request: HostcallRequest) -> HostcallFuture {
    HostcallFuture {
        request: Some(request),
        operation_id: None,
        task_id: None,
    }
}

fn poll_operation(operation_id: u64) -> Result<Option<HostcallOutput>> {
    let mut output = vec![0_u8; 4096];
    loop {
        let poll_status =
            unsafe { selium_hostcall_poll(operation_id, output.as_mut_ptr(), output.len()) };
        let (status, len) = unpack_hostcall_status(poll_status);
        if status == selium_abi::HOSTCALL_STATUS_OUTPUT_TOO_SMALL {
            output.resize(len as usize, 0);
            continue;
        }
        let state: CompletionState = decode_rkyv(&output[..len as usize])?;
        return match state {
            CompletionState::Ready(output) => Ok(Some(output)),
            CompletionState::Pending { .. } => Ok(None),
            CompletionState::Failed(error) => Err(abi_error_to_guest_error(error)),
        };
    }
}

fn poll_backgrounds() -> bool {
    let mut progressed = merge_spawn_queue();
    progressed |= apply_wake_queue();
    BACKGROUND.with(|tasks| {
        if let Ok(mut tasks) = tasks.try_borrow_mut() {
            let mut index = 0;
            while index < tasks.len() {
                if !tasks[index].runnable {
                    index += 1;
                    continue;
                }

                tasks[index].runnable = false;
                let task_id = tasks[index].id;
                let waker = futures::task::waker(Arc::new(TaskWake { task_id }));
                let mut context = Context::from_waker(&waker);
                CURRENT_TASK.with(|current| *current.borrow_mut() = Some(task_id));
                let poll = tasks[index].future.as_mut().poll(&mut context);
                CURRENT_TASK.with(|current| *current.borrow_mut() = None);

                match poll {
                    Poll::Ready(()) => {
                        tasks.swap_remove(index);
                        progressed = true;
                    }
                    Poll::Pending => index += 1,
                }
            }
        }
    });
    progressed | apply_wake_queue() | merge_spawn_queue()
}

fn merge_spawn_queue() -> bool {
    SPAWN_QUEUE.with(|queue| {
        let mut queue = queue.borrow_mut();
        if queue.is_empty() {
            return false;
        }
        BACKGROUND.with(|tasks| tasks.borrow_mut().extend(queue.drain(..)));
        true
    })
}

fn apply_wake_queue() -> bool {
    let wakeups = WAKE_QUEUE.with(|queue| queue.borrow_mut().drain(..).collect::<Vec<_>>());
    if wakeups.is_empty() {
        return false;
    }

    BACKGROUND.with(|tasks| {
        if let Ok(mut tasks) = tasks.try_borrow_mut() {
            for task_id in &wakeups {
                if let Some(task) = tasks.iter_mut().find(|task| task.id == *task_id) {
                    task.runnable = true;
                }
            }
        }
    });
    SPAWN_QUEUE.with(|tasks| {
        let mut tasks = tasks.borrow_mut();
        for task_id in &wakeups {
            if let Some(task) = tasks.iter_mut().find(|task| task.id == *task_id) {
                task.runnable = true;
            }
        }
    });
    true
}

fn next_task_id() -> TaskId {
    NEXT_TASK_ID.with(|next| {
        let mut next = next.borrow_mut();
        let id = (*next).max(1);
        *next = id.checked_add(1).unwrap_or(1).max(1);
        id
    })
}

fn current_task_id() -> Option<TaskId> {
    CURRENT_TASK.with(|current| *current.borrow())
}

fn wake_task(task_id: TaskId) {
    if task_id != 0 {
        WAKE_QUEUE.with(|queue| queue.borrow_mut().push(task_id));
    }
}

fn abi_error_to_guest_error(error: AbiError) -> GuestError {
    if error.code == selium_abi::AbiErrorCode::PermissionDenied {
        GuestError::PermissionDenied(Capability::Signal)
    } else {
        GuestError::Host(format!("{:?}: {}", error.code, error.message))
    }
}

#[cfg(target_arch = "wasm32")]
const MAILBOX_WORDS: usize = selium_abi::mailbox::BYTE_LEN / 4;

#[cfg(target_arch = "wasm32")]
static mut MAILBOX: [u32; MAILBOX_WORDS] = [0; MAILBOX_WORDS];

#[cfg(target_arch = "wasm32")]
fn mailbox_base() -> *mut u8 {
    core::ptr::addr_of_mut!(MAILBOX).cast::<u8>()
}

#[cfg(target_arch = "wasm32")]
unsafe fn mailbox_cell(offset: usize) -> *mut core::sync::atomic::AtomicU32 {
    unsafe {
        mailbox_base()
            .add(offset)
            .cast::<core::sync::atomic::AtomicU32>()
    }
}

#[cfg(target_arch = "wasm32")]
fn register_mailbox() {
    unsafe {
        (*mailbox_cell(selium_abi::mailbox::CAPACITY_OFFSET)).store(
            selium_abi::mailbox::CAPACITY as u32,
            std::sync::atomic::Ordering::Release,
        );
        selium_mailbox_register(mailbox_base(), selium_abi::mailbox::BYTE_LEN);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn register_mailbox() {}

#[cfg(target_arch = "wasm32")]
fn drain_mailbox() {
    unsafe {
        if (*mailbox_cell(selium_abi::mailbox::FLAG_OFFSET))
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            return;
        }
        let mut head = (*mailbox_cell(selium_abi::mailbox::HEAD_OFFSET))
            .load(std::sync::atomic::Ordering::Acquire);
        let tail = (*mailbox_cell(selium_abi::mailbox::TAIL_OFFSET))
            .load(std::sync::atomic::Ordering::Acquire);
        while head != tail {
            let slot = selium_abi::mailbox::RING_OFFSET
                + (head as usize % selium_abi::mailbox::CAPACITY) * selium_abi::mailbox::SLOT_SIZE;
            let task_id = (*mailbox_cell(slot)).load(std::sync::atomic::Ordering::Relaxed);
            wake_task(task_id);
            head = head.wrapping_add(1);
        }
        (*mailbox_cell(selium_abi::mailbox::HEAD_OFFSET))
            .store(head, std::sync::atomic::Ordering::Release);
        (*mailbox_cell(selium_abi::mailbox::FLAG_OFFSET))
            .store(0, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn drain_mailbox() {}

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "selium")]
unsafe extern "C" {
    #[link_name = "process_id"]
    fn selium_process_id() -> u64;
    #[link_name = "mark_ready"]
    fn selium_mark_ready();
    #[link_name = "hostcall_create"]
    fn selium_hostcall_create(request_ptr: *const u8, request_len: usize) -> u64;
    #[link_name = "hostcall_poll"]
    fn selium_hostcall_poll(operation_id: u64, out_ptr: *mut u8, out_capacity: usize) -> u64;
    #[link_name = "hostcall_drop"]
    fn selium_hostcall_drop(operation_id: u64) -> u32;
    #[link_name = "mailbox_register"]
    fn selium_mailbox_register(mailbox_ptr: *mut u8, mailbox_len: usize);
}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn selium_process_id() -> u64 {
    0
}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn selium_mark_ready() {}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn selium_hostcall_create(_request_ptr: *const u8, _request_len: usize) -> u64 {
    selium_abi::pack_hostcall_status(selium_abi::HOSTCALL_STATUS_FAILED, 0)
}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn selium_hostcall_poll(_operation_id: u64, _out_ptr: *mut u8, _out_capacity: usize) -> u64 {
    selium_abi::pack_hostcall_status(selium_abi::HOSTCALL_STATUS_FAILED, 0)
}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn selium_hostcall_drop(_operation_id: u64) -> u32 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[rkyv(bytecheck())]
    struct DemoPayload {
        message: String,
    }

    #[test]
    fn typed_codec_round_trips() {
        let payload = DemoPayload {
            message: "hello".to_string(),
        };

        let encoded = encode_typed(&payload).expect("encode payload");
        let decoded: DemoPayload = decode_typed(&encoded).expect("decode payload");

        assert_eq!(decoded, payload);
    }

    #[test]
    fn native_hostcalls_are_unavailable() {
        let result = Signal::create();

        assert!(matches!(result, Err(GuestError::Host(_))));
    }

    #[test]
    fn cooperative_yield_allows_spawned_task_progress() {
        let value = Rc::new(RefCell::new(0));
        let value_for_task = Rc::clone(&value);

        let join = spawn(async move {
            yield_now().await;
            *value_for_task.borrow_mut() = 7;
        });
        poll_reactor();

        assert_eq!(*value.borrow(), 7);
        assert_eq!(join.state.borrow().result, Some(()));
    }

    #[test]
    fn reactor_parks_pending_tasks_until_woken() {
        struct ParkUntilWoken {
            polls: Rc<RefCell<u32>>,
            task_id: Rc<RefCell<Option<TaskId>>>,
        }

        impl Future for ParkUntilWoken {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                let task = self.get_mut();
                let mut polls = task.polls.borrow_mut();
                *polls += 1;
                if *polls == 1 {
                    *task.task_id.borrow_mut() = current_task_id();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }
        }

        let polls = Rc::new(RefCell::new(0));
        let task_id = Rc::new(RefCell::new(None));
        let join = spawn(ParkUntilWoken {
            polls: Rc::clone(&polls),
            task_id: Rc::clone(&task_id),
        });

        poll_reactor();
        assert_eq!(*polls.borrow(), 1);
        assert_eq!(join.state.borrow().result, None);

        poll_reactor();
        assert_eq!(*polls.borrow(), 1);

        wake_task(task_id.borrow().expect("task id captured"));
        poll_reactor();

        assert_eq!(*polls.borrow(), 2);
        assert_eq!(join.state.borrow().result, Some(()));
    }
}
