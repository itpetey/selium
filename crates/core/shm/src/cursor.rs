use selium_wire::error::{Error, Result};

/// A monotonic cursor over a shared-memory ring buffer.
///
/// Cursors track read or write positions in a ring buffer stored in a
/// shared memory region. All cursor operations use the shared memory
/// region mapping for cross-guest visibility.
#[derive(Clone, Copy, Debug)]
pub struct Cursor {
    position: u64,
}

impl Cursor {
    /// Creates a new cursor at the given position.
    pub const fn new(position: u64) -> Self {
        Self { position }
    }

    /// Returns the current cursor position.
    pub fn get(&self) -> u64 {
        self.position
    }

    /// Advances the cursor by `delta` bytes.
    pub fn advance(&mut self, delta: u64) {
        self.position = self.position.wrapping_add(delta);
    }

    /// Returns the number of readable bytes between this cursor and `tail`.
    pub fn readable(&self, tail: u64) -> u64 {
        tail.saturating_sub(self.position)
    }

    /// Returns the number of writable bytes between `head` and this cursor using capacity.
    pub fn writable(&self, head: u64, capacity: u64) -> u64 {
        capacity.saturating_sub(self.position.wrapping_sub(head))
    }

    /// Computes the masked offset into the ring buffer for this position.
    pub fn masked(&self, mask: u64) -> u64 {
        self.position & mask
    }

    /// Computes the length of the tail segment (from masked position to end of buffer).
    pub fn tail_segment_len(&self, mask: u64) -> u64 {
        let masked = self.masked(mask);
        debug_assert!(masked <= mask.wrapping_add(1));
        mask.wrapping_add(1).wrapping_sub(masked)
    }

    /// Splits a write at `pos` of `len` bytes into two segments accounting for wraparound.
    /// Returns (tail_len, head_len) where tail is the amount before wraparound.
    pub fn split_wraparound(&self, len: u64, mask: u64) -> (u64, u64) {
        let tail_seg = self.tail_segment_len(mask).min(len);
        let head_seg = len.wrapping_sub(tail_seg);
        (tail_seg, head_seg)
    }
}

/// Computes a mask for a given capacity (must be a power of two).
pub fn mask_for_capacity(capacity: u64) -> Result<u64> {
    if !capacity.is_power_of_two() {
        return Err(Error::InvalidLayout);
    }
    Ok(capacity - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_tracks_position() {
        let mut c = Cursor::new(0);
        assert_eq!(c.get(), 0);
        c.advance(42);
        assert_eq!(c.get(), 42);
    }

    #[test]
    fn readable_computes_difference() {
        let c = Cursor::new(10);
        assert_eq!(c.readable(25), 15);
        assert_eq!(c.readable(10), 0);
        assert_eq!(c.readable(5), 0);
    }

    #[test]
    fn writable_computes_remaining() {
        let c = Cursor::new(10);
        assert_eq!(c.writable(0, 100), 90);
        assert_eq!(c.writable(5, 20), 15);
    }

    #[test]
    fn mask_rounds_down_capacity() {
        assert_eq!(mask_for_capacity(64).unwrap(), 63);
        assert_eq!(mask_for_capacity(1).unwrap(), 0);
        assert_eq!(mask_for_capacity(3).unwrap_err(), Error::InvalidLayout);
    }

    #[test]
    fn wraparound_splits_correctly() {
        let mask = 15;
        let c = Cursor::new(12);
        let (tail, head) = c.split_wraparound(10, mask);
        assert_eq!(tail, 4);
        assert_eq!(head, 6);
    }

    #[test]
    fn wraparound_within_one_pass() {
        let mask = 15;
        let c = Cursor::new(5);
        let (tail, head) = c.split_wraparound(5, mask);
        assert_eq!(tail, 5);
        assert_eq!(head, 0);
    }

    #[test]
    fn masked_position_is_bounded() {
        let c = Cursor::new(37);
        assert_eq!(c.masked(31), 5);
    }

    #[test]
    fn tail_segment_len_at_buffer_edge() {
        let c = Cursor::new(12);
        assert_eq!(c.tail_segment_len(15), 4);
    }
}
