#[cfg(target_arch = "wasm32")]
use crate::async_runtime::wake_task;

/// Returns the current guest process id assigned by the host.
pub fn process_id() -> u64 {
    unsafe { selium_process_id() }
}

/// Marks the current guest as ready for runtime readiness checks.
pub fn mark_ready() {
    unsafe { selium_mark_ready() }
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
pub(crate) fn register_mailbox() {
    unsafe {
        (*mailbox_cell(selium_abi::mailbox::CAPACITY_OFFSET)).store(
            selium_abi::mailbox::CAPACITY as u32,
            std::sync::atomic::Ordering::Release,
        );
        selium_mailbox_register(mailbox_base(), selium_abi::mailbox::BYTE_LEN);
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn register_mailbox() {}

#[cfg(target_arch = "wasm32")]
pub(crate) fn drain_mailbox() {
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
pub(crate) fn drain_mailbox() {}

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "selium")]
unsafe extern "C" {
    #[link_name = "process_id"]
    fn selium_process_id() -> u64;
    #[link_name = "mark_ready"]
    fn selium_mark_ready();
    #[link_name = "hostcall_create"]
    pub(crate) fn selium_hostcall_create(request_ptr: *const u8, request_len: usize) -> u64;
    #[link_name = "hostcall_poll"]
    pub(crate) fn selium_hostcall_poll(
        operation_id: u64,
        out_ptr: *mut u8,
        out_capacity: usize,
    ) -> u64;
    #[link_name = "hostcall_drop"]
    pub(crate) fn selium_hostcall_drop(operation_id: u64) -> u32;
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
pub(crate) unsafe fn selium_hostcall_create(_request_ptr: *const u8, _request_len: usize) -> u64 {
    selium_abi::pack_hostcall_status(selium_abi::HOSTCALL_STATUS_FAILED, 0)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) unsafe fn selium_hostcall_poll(
    _operation_id: u64,
    _out_ptr: *mut u8,
    _out_capacity: usize,
) -> u64 {
    selium_abi::pack_hostcall_status(selium_abi::HOSTCALL_STATUS_FAILED, 0)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) unsafe fn selium_hostcall_drop(_operation_id: u64) -> u32 {
    0
}
