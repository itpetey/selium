#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuestPtr<T> {
    addr: u32,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> GuestPtr<T> {
    pub fn null() -> Self {
        Self {
            addr: 0,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn from_addr(addr: u32) -> Self {
        Self {
            addr,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn addr(&self) -> u32 {
        self.addr
    }

    pub fn is_null(&self) -> bool {
        self.addr == 0
    }

    pub fn offset(&self, bytes: u32) -> Option<Self> {
        self.addr.checked_add(bytes).map(|addr| Self {
            addr,
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn wrapping_offset(&self, bytes: u32) -> Self {
        Self {
            addr: self.addr.wrapping_add(bytes),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Default for GuestPtr<T> {
    fn default() -> Self {
        Self::null()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_pointer() {
        let p: GuestPtr<u8> = GuestPtr::null();
        assert!(p.is_null());
        assert_eq!(p.addr(), 0);
    }

    #[test]
    fn from_addr() {
        let p: GuestPtr<u8> = GuestPtr::from_addr(0x1000);
        assert!(!p.is_null());
        assert_eq!(p.addr(), 0x1000);
    }

    #[test]
    fn offset() {
        let p: GuestPtr<u8> = GuestPtr::from_addr(0x100);
        let q = p.offset(0x10).unwrap();
        assert_eq!(q.addr(), 0x110);
    }

    #[test]
    fn offset_overflow() {
        let p: GuestPtr<u8> = GuestPtr::from_addr(u32::MAX);
        assert!(p.offset(1).is_none());
    }

    #[test]
    fn wrapping_offset() {
        let p: GuestPtr<u8> = GuestPtr::from_addr(u32::MAX);
        let q = p.wrapping_offset(1);
        assert_eq!(q.addr(), 0);
    }
}
