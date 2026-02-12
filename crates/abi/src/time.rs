use rkyv::{Archive, Deserialize, Serialize};

/// Snapshot of the host clock values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct TimeNow {
    /// Unix timestamp in milliseconds.
    pub unix_ms: u64,
    /// Monotonic timestamp in milliseconds.
    pub monotonic_ms: u64,
}

/// Request to sleep for a duration in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct TimeSleep {
    /// Duration to sleep in milliseconds.
    pub duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{decode_rkyv, encode_rkyv};

    #[test]
    fn time_now_round_trips_with_rkyv() {
        let now = TimeNow {
            unix_ms: 123,
            monotonic_ms: 456,
        };
        let encoded = encode_rkyv(&now).expect("encode");
        let decoded = decode_rkyv::<TimeNow>(&encoded).expect("decode");
        assert_eq!(decoded, now);
    }

    #[test]
    fn time_sleep_round_trips_with_rkyv() {
        let sleep = TimeSleep { duration_ms: 50 };
        let encoded = encode_rkyv(&sleep).expect("encode");
        let decoded = decode_rkyv::<TimeSleep>(&encoded).expect("decode");
        assert_eq!(decoded, sleep);
    }
}
