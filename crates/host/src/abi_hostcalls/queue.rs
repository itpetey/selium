use selium_abi::{Capability, GuestContext};
use wasmtime::Linker;

pub struct QueueCapability {
    data: std::collections::HashMap<u64, Vec<Vec<u8>>>,
}

impl QueueCapability {
    pub fn new() -> Self {
        Self {
            data: std::collections::HashMap::new(),
        }
    }

    pub fn create(&mut self, capacity: usize) -> Result<u64, selium_abi::GuestError> {
        if capacity == 0 {
            return Err(selium_abi::GuestError::InvalidArgument);
        }
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.data.insert(id, Vec::new());
        Ok(id)
    }

    pub fn enqueue(&mut self, id: u64, msg: Vec<u8>) -> Result<(), selium_abi::GuestError> {
        match self.data.get_mut(&id) {
            Some(queue) => {
                queue.push(msg);
                Ok(())
            }
            None => Err(selium_abi::GuestError::NotFound),
        }
    }

    pub fn dequeue(&mut self, id: u64) -> Result<Vec<u8>, selium_abi::GuestError> {
        match self.data.get_mut(&id) {
            Some(queue) => queue.pop().ok_or(selium_abi::GuestError::WouldBlock),
            None => Err(selium_abi::GuestError::NotFound),
        }
    }

    pub fn len(&self, id: u64) -> Result<usize, selium_abi::GuestError> {
        match self.data.get(&id) {
            Some(queue) => Ok(queue.len()),
            None => Err(selium_abi::GuestError::NotFound),
        }
    }

    pub fn close(&mut self, id: u64) -> Result<(), selium_abi::GuestError> {
        self.data.remove(&id);
        Ok(())
    }
}

impl Default for QueueCapability {
    fn default() -> Self {
        Self::new()
    }
}

use std::sync::OnceLock;
static QUEUE: OnceLock<QueueCapability> = OnceLock::new();

pub fn global_queue() -> &'static QueueCapability {
    QUEUE.get_or_init(QueueCapability::new)
}

pub fn queue_create(_ctx: &GuestContext, capacity: usize) -> Result<u64, selium_abi::GuestError> {
    if !_ctx.has_capability(Capability::QueueLifecycle) {
        return Err(selium_abi::GuestError::PermissionDenied);
    }
    let queue = global_queue() as *const QueueCapability as *mut QueueCapability;
    unsafe { (*queue).create(capacity) }
}

pub fn queue_enqueue(
    _ctx: &GuestContext,
    id: u64,
    msg: Vec<u8>,
) -> Result<(), selium_abi::GuestError> {
    if !_ctx.has_capability(Capability::QueueWriter) {
        return Err(selium_abi::GuestError::PermissionDenied);
    }
    let queue = global_queue() as *const QueueCapability as *mut QueueCapability;
    unsafe { (*queue).enqueue(id, msg) }
}

pub fn queue_dequeue(_ctx: &GuestContext, id: u64) -> Result<Vec<u8>, selium_abi::GuestError> {
    if !_ctx.has_capability(Capability::QueueReader) {
        return Err(selium_abi::GuestError::PermissionDenied);
    }
    let queue = global_queue() as *const QueueCapability as *mut QueueCapability;
    unsafe { (*queue).dequeue(id) }
}

pub fn queue_len(_ctx: &GuestContext, id: u64) -> Result<usize, selium_abi::GuestError> {
    if !_ctx.has_capability(Capability::QueueReader)
        && !_ctx.has_capability(Capability::QueueWriter)
    {
        return Err(selium_abi::GuestError::PermissionDenied);
    }
    let queue = global_queue() as *const QueueCapability as *mut QueueCapability;
    unsafe { (*queue).len(id) }
}

pub fn queue_close(_ctx: &GuestContext, id: u64) -> Result<(), selium_abi::GuestError> {
    if !_ctx.has_capability(Capability::QueueLifecycle) {
        return Err(selium_abi::GuestError::PermissionDenied);
    }
    let queue = global_queue() as *const QueueCapability as *mut QueueCapability;
    unsafe { (*queue).close(id) }
}

pub fn add_to_linker(_linker: &mut Linker<GuestContext>) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx(caps: impl IntoIterator<Item = Capability>) -> GuestContext {
        GuestContext::with_capabilities(1, caps)
    }

    #[test]
    fn test_queue_create() {
        let ctx = test_ctx([Capability::QueueLifecycle]);
        let id = queue_create(&ctx, 10).unwrap();
        assert!(id > 0);
    }

    #[test]
    fn test_queue_enqueue_dequeue() {
        let ctx = test_ctx([
            Capability::QueueLifecycle,
            Capability::QueueWriter,
            Capability::QueueReader,
        ]);
        let id = queue_create(&ctx, 10).unwrap();
        queue_enqueue(&ctx, id, b"hello".to_vec()).unwrap();
        queue_enqueue(&ctx, id, b"world".to_vec()).unwrap();
        assert_eq!(queue_dequeue(&ctx, id).unwrap(), b"world");
        assert_eq!(queue_dequeue(&ctx, id).unwrap(), b"hello");
    }

    #[test]
    fn test_queue_empty() {
        let ctx = test_ctx([Capability::QueueLifecycle, Capability::QueueReader]);
        let id = queue_create(&ctx, 10).unwrap();
        assert!(matches!(
            queue_dequeue(&ctx, id),
            Err(selium_abi::GuestError::WouldBlock)
        ));
    }

    #[test]
    fn test_queue_close() {
        let ctx = test_ctx([Capability::QueueLifecycle, Capability::QueueWriter]);
        let id = queue_create(&ctx, 10).unwrap();
        queue_close(&ctx, id).unwrap();
        assert!(matches!(
            queue_enqueue(&ctx, id, b"test".to_vec()),
            Err(selium_abi::GuestError::NotFound)
        ));
    }
}
