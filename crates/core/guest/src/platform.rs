use crate::async_runtime::wake_task;

static mut MAILBOX: [u32; MAILBOX_WORDS] = [0; MAILBOX_WORDS];
const MAILBOX_WORDS: usize = selium_abi::mailbox::BYTE_LEN / 4;

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

/// Marks the current guest as ready for runtime readiness checks.
pub fn mark_ready() {
    // SAFETY: `selium_mark_ready` is a host import with no safety invariants.
    unsafe { selium_mark_ready() }
}

/// Returns the current guest process id assigned by the host.
pub fn process_id() -> u64 {
    // SAFETY: `selium_process_id` is a host import with no safety invariants.
    unsafe { selium_process_id() }
}

pub(crate) fn drain_mailbox() {
    // SAFETY: The mailbox is a static mutable array. Access is serialised by the
    // flag/head/tail handshake and atomic operations. This function is called
    // from the guest event-loop context where no other code concurrently
    // accesses the mailbox.
    let flag_ptr = unsafe { mailbox_cell(selium_abi::mailbox::FLAG_OFFSET) };
    // SAFETY: `flag_ptr` points to a valid AtomicU32 within the mailbox.
    if unsafe { (*flag_ptr).load(std::sync::atomic::Ordering::Acquire) } == 0 {
        return;
    }

    // SAFETY: Same single-threaded mailbox access.
    let head_ptr = unsafe { mailbox_cell(selium_abi::mailbox::HEAD_OFFSET) };
    // SAFETY: Same as above.
    let mut head = unsafe { (*head_ptr).load(std::sync::atomic::Ordering::Acquire) };

    // SAFETY: Same as above.
    let tail_ptr = unsafe { mailbox_cell(selium_abi::mailbox::TAIL_OFFSET) };
    // SAFETY: Same as above.
    let tail = unsafe { (*tail_ptr).load(std::sync::atomic::Ordering::Acquire) };

    while head != tail {
        let slot = selium_abi::mailbox::RING_OFFSET
            + (head as usize % selium_abi::mailbox::CAPACITY) * selium_abi::mailbox::SLOT_SIZE;
        // SAFETY: `slot` has been bounds-checked against the mailbox capacity.
        let slot_ptr = unsafe { mailbox_cell(slot) };
        // SAFETY: `slot_ptr` points to a valid AtomicU32 within the mailbox.
        let task_id = unsafe { (*slot_ptr).load(std::sync::atomic::Ordering::Relaxed) };
        wake_task(task_id);
        head = head.wrapping_add(1);
    }
    // SAFETY: Same single-threaded mailbox access.
    let head_ptr = unsafe { mailbox_cell(selium_abi::mailbox::HEAD_OFFSET) };
    // SAFETY: Same as above.
    unsafe { (*head_ptr).store(head, std::sync::atomic::Ordering::Release) };

    // SAFETY: Same single-threaded mailbox access.
    let flag_ptr = unsafe { mailbox_cell(selium_abi::mailbox::FLAG_OFFSET) };
    // SAFETY: Same as above.
    unsafe { (*flag_ptr).store(0, std::sync::atomic::Ordering::Release) };
}

pub(crate) fn register_mailbox() {
    // SAFETY: The mailbox is a static mutable array. This function is called
    // once during guest initialisation before any concurrent access occurs.
    let capacity_ptr = unsafe { mailbox_cell(selium_abi::mailbox::CAPACITY_OFFSET) };
    // SAFETY: `capacity_ptr` points to a valid AtomicU32 within the mailbox.
    unsafe {
        (*capacity_ptr).store(
            selium_abi::mailbox::CAPACITY as u32,
            std::sync::atomic::Ordering::Release,
        );
    }
    // SAFETY: `selium_mailbox_register` is a host import that registers the
    // mailbox with the runtime. It is safe to call once during initialisation.
    unsafe {
        selium_mailbox_register(mailbox_base(), selium_abi::mailbox::BYTE_LEN);
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) unsafe fn selium_hostcall_create(_: *const u8, _: usize) -> u64 {
    0
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) unsafe fn selium_hostcall_drop(_: u64) -> u32 {
    0
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) unsafe fn selium_hostcall_poll(_: u64, _: *mut u8, _: usize) -> u64 {
    0
}

fn mailbox_base() -> *mut u8 {
    core::ptr::addr_of_mut!(MAILBOX).cast::<u8>()
}

unsafe fn mailbox_cell(offset: usize) -> *mut core::sync::atomic::AtomicU32 {
    // SAFETY: `offset` is a known mailbox constant within the MAILBOX bounds.
    unsafe {
        mailbox_base()
            .add(offset)
            .cast::<core::sync::atomic::AtomicU32>()
    }
}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn selium_mailbox_register(_: *mut u8, _: usize) {}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn selium_mark_ready() {}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn selium_process_id() -> u64 {
    0
}
