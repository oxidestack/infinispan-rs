//! Port of `HashFunctionPartitioner` (core 14.0.35.Final,
//! `distribution/ch/impl/HashFunctionPartitioner.java`).

use super::murmur_hash3::{HashKey, hash_object};

/// `Util.getSegmentSize(numSegments)`: `(int) Math.ceil((double) (1L << 31) / numSegments)`.
pub fn get_segment_size(num_segments: i32) -> i32 {
    ((1i64 << 31) as f64 / num_segments as f64).ceil() as i32
}

/// `HashFunctionPartitioner`: maps a key's MurmurHash3 hash to one of `num_segments` equal ranges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashFunctionPartitioner {
    num_segments: i32,
    segment_size: i32,
}

impl HashFunctionPartitioner {
    /// `init(int)` (`:56-62`).
    pub fn new(num_segments: i32) -> Result<Self, String> {
        if num_segments <= 0 {
            return Err("numSegments must be strictly positive".into());
        }
        Ok(Self { num_segments, segment_size: get_segment_size(num_segments) })
    }

    /// The number of segments.
    pub fn num_segments(&self) -> i32 {
        self.num_segments
    }

    /// `getSegment(Object)` (`:64-67`): `(hash & Integer.MAX_VALUE) / segmentSize`.
    pub fn get_segment(&self, key: &HashKey) -> i32 {
        (hash_object(key) & i32::MAX) / self.segment_size
    }

    /// `getSegmentEndHashes()` (`:73-79`).
    pub fn segment_end_hashes(&self) -> Vec<i32> {
        (0..self.num_segments)
            .map(|i| ((i + 1) % self.num_segments).wrapping_mul(self.segment_size))
            .collect()
    }
}
