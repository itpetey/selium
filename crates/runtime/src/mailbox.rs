use selium_abi::TaskId;
use wasmtiny::{WasmError, runtime::SharedMemory};

pub(crate) struct GuestMailbox {
    pub(crate) memory: SharedMemory,
    pub(crate) base: u32,
}

impl GuestMailbox {
    pub(crate) fn new(memory: SharedMemory, base: u32) -> Self {
        Self { memory, base }
    }

    pub(crate) fn enqueue(&self, task_id: TaskId) -> wasmtiny::runtime::Result<()> {
        let mut memory = self
            .memory
            .lock()
            .map_err(|_| WasmError::Runtime("guest memory lock poisoned".to_string()))?;
        let tail_offset = self.offset(selium_abi::mailbox::TAIL_OFFSET)?;
        let ring_offset = self.offset(selium_abi::mailbox::RING_OFFSET)?;
        let flag_offset = self.offset(selium_abi::mailbox::FLAG_OFFSET)?;
        let tail = memory.read_u32(tail_offset)?;
        let slot = (tail as usize % selium_abi::mailbox::CAPACITY) * selium_abi::mailbox::SLOT_SIZE;
        let slot_offset = ring_offset
            .checked_add(slot as u32)
            .ok_or_else(|| WasmError::Runtime("mailbox slot offset overflow".to_string()))?;
        memory.write_u32(slot_offset, task_id)?;
        memory.write_u32(tail_offset, tail.wrapping_add(1))?;
        memory.write_u32(flag_offset, 1)?;
        Ok(())
    }

    fn offset(&self, offset: usize) -> wasmtiny::runtime::Result<u32> {
        self.base
            .checked_add(offset as u32)
            .ok_or_else(|| WasmError::Runtime("mailbox offset overflow".to_string()))
    }
}
