#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StorageHandle(u64);

impl StorageHandle {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn id(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueueHandle(u64);

impl QueueHandle {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn id(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NetworkHandle(u64);

impl NetworkHandle {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn id(&self) -> u64 {
        self.0
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NetworkListenerHandle(u64);

#[allow(dead_code)]
impl NetworkListenerHandle {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn id(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_handle_id() {
        let h = StorageHandle::new(42);
        assert_eq!(h.id(), 42);
    }

    #[test]
    fn queue_handle_id() {
        let h = QueueHandle::new(123);
        assert_eq!(h.id(), 123);
    }

    #[test]
    fn network_handle_id() {
        let h = NetworkHandle::new(99);
        assert_eq!(h.id(), 99);
    }
}
