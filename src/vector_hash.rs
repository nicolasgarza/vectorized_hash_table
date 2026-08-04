use std::simd::{cmp::SimdPartialEq, u8x32};
use ahash::AHasher;
use std::cmp::max;
use std::hash::{Hash, Hasher};
use std::usize;

pub struct VectorHash {
    num_keys: usize,
    num_tombstones: usize,
    buckets: Vec<Bucket>,
}

pub struct Bucket {
    fingerprints: [u8; 32],
    pairs: [(u64, u64); 32],
}

impl Default for Bucket {
    fn default() -> Self {
        let fingerprints = [EMPTY; 32];
        Self {
            fingerprints,
            pairs: Default::default(),
        }
    }
}

const EMPTY: u8 = 0x80;
const TOMBSTONE: u8 = 0xFE;
const BUCKET_SIZE: usize = 32;
const LOAD_FACTOR: f64 = 0.7;

impl VectorHash {
    pub fn new() -> Self {
        VectorHash {
            num_keys: 0,
            num_tombstones: 0,
            buckets: (0..32).map(|_| Bucket::default()).collect(),
        }
    }

    pub fn with_capacity(size: usize) -> Self {
        let size = max(size, 16).next_power_of_two();

        VectorHash {
            num_keys: 0,
            num_tombstones: 0,
            buckets: (0..size).map(|_| Bucket::default()).collect(),
        }
    }

    pub fn get(&self, key: u64) -> Option<u64> {
        let (mut i, hash) = self.index(key);
        let fingerprint = (hash >> 57) as u8;

        loop {
            // never an infinite loop, as there are always empty slots in array
            match self.search_in_bucket(key, fingerprint, i) {
                (Some(idx), _, _) => return Some(self.buckets[i].pairs[idx].1),
                (None, Some(_), _) => return None,
                (None, None, _) => (),
            };
            i = (i + 1) & self.buckets.len() - 1
        }
    }

    pub fn put(&mut self, key: u64, value: u64) -> Option<u64> {
        if (self.num_keys + self.num_tombstones) as f64 / (self.buckets.len() * BUCKET_SIZE) as f64
            > LOAD_FACTOR
        {
            self.resize();
        }

        let (mut i, hash) = self.index(key);
        let fingerprint = (hash >> 57) as u8;
        let (mut tombstone_slot, mut tombstone_bucket) = (usize::MAX, usize::MAX);

        loop {
            let (check, found_empty, found_tombstone_slot) =
                self.search_in_bucket(key, fingerprint, i);
            if found_tombstone_slot.is_some() && tombstone_bucket == usize::MAX {
                tombstone_bucket = i;
                tombstone_slot = found_tombstone_slot.unwrap();
            }

            if let Some(old_index) = check {
                let old_value = self.buckets[i].pairs[old_index].1;
                self.buckets[i].fingerprints[old_index] = fingerprint;
                self.buckets[i].pairs[old_index] = (key, value);

                return Some(old_value);
            } else if let Some(empty_slot) = found_empty {
                let (insert_bucket, insert_slot) = if tombstone_bucket != usize::MAX {
                    self.num_tombstones -= 1;
                    (tombstone_bucket, tombstone_slot)
                } else {
                    (i, empty_slot)
                };

                self.buckets[insert_bucket].fingerprints[insert_slot] = fingerprint;
                self.buckets[insert_bucket].pairs[insert_slot] = (key, value);

                self.num_keys += 1;
                return None;
            }

            i = (i + 1) & self.buckets.len() - 1;
        }
    }

    pub fn delete(&mut self, key: u64) -> Option<u64> {
        let (mut i, hash) = self.index(key);
        let fingerprint = (hash >> 57) as u8;

        loop {
            match self.search_in_bucket(key, fingerprint, i) {
                (Some(idx), _, _) => {
                    let old = self.buckets[i].pairs[idx].1;
                    self.buckets[i].fingerprints[idx] = TOMBSTONE;

                    self.num_keys -= 1;
                    self.num_tombstones += 1;
                    return Some(old);
                }
                (None, Some(_), _) => return None,
                (None, None, _) => (),
            }
            i = (i + 1) & self.buckets.len() - 1;
        }
    }

    fn resize(&mut self) {
        let mut new_map = VectorHash::with_capacity(self.buckets.len() * 4);

        for bucket in &self.buckets {
            for (idx, fp) in bucket.fingerprints.iter().enumerate() {
                if fp >> 7 == 0 {
                    let (k, v) = (bucket.pairs[idx].0, bucket.pairs[idx].1);
                    new_map.put(k, v);
                }
            }
        }

        *self = new_map;
    }

    // returns an option. if Some(), it will be the matching index
    fn search_in_bucket(
        &self,
        key: u64,
        fingerprint: u8,
        bucket_index: usize,
    ) -> (Option<usize>, Option<usize>, Option<usize>) {
        let bucket = &self.buckets[bucket_index];

        let (mut matches, empties, tombstones) = simd_fingerprint_masks(&bucket.fingerprints, fingerprint);

        let found_empty = if empties == 0 {
            None
        } else {
            Some(empties.trailing_zeros() as usize)
        };

        let found_tombstone = if tombstones == 0 {
            None
        } else {
            Some(tombstones.trailing_zeros() as usize)
        };

        while matches != 0 {
            let idx = matches.trailing_zeros() as usize;

            if bucket.pairs[idx].0 == key {
                return (Some(idx), found_empty, found_tombstone);
            }

            matches &= matches - 1;
        }

        (None, found_empty, found_tombstone)
    }

    fn index(&self, key: u64) -> (usize, u64) {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        (hash as usize & (self.buckets.len() - 1), hash)
    }
}


fn simd_fingerprint_masks(
    fingerprints: &[u8; BUCKET_SIZE],
    fingerprint: u8,
) -> (u32, u32, u32) {
    let data = u8x32::from_array(*fingerprints);

    let matches = data
        .simd_eq(u8x32::splat(fingerprint))
        .to_bitmask() as u32;

    let empties = data
        .simd_eq(u8x32::splat(EMPTY))
        .to_bitmask() as u32;

    let tombstones = data
        .simd_eq(u8x32::splat(TOMBSTONE))
        .to_bitmask() as u32;

    (matches, empties, tombstones)
}

#[cfg(test)]
mod tests {
    use super::VectorHash;

    #[test]
    fn get_missing_returns_none() {
        let map = VectorHash::new();
        assert_eq!(map.get(42), None);
    }

    #[test]
    fn put_then_get_returns_value() {
        let mut map = VectorHash::new();
        assert_eq!(map.put(1, 100), None);
        assert_eq!(map.get(1), Some(100));
    }

    #[test]
    fn put_overwrite_returns_old_and_updates_value() {
        let mut map = VectorHash::new();
        assert_eq!(map.put(1, 100), None);
        assert_eq!(map.put(1, 200), Some(100));
        assert_eq!(map.get(1), Some(200));
    }

    #[test]
    fn delete_existing_returns_value() {
        let mut map = VectorHash::new();
        map.put(7, 77);
        assert_eq!(map.delete(7), Some(77));
        assert_eq!(map.get(7), None);
    }

    #[test]
    fn delete_missing_returns_none() {
        let mut map = VectorHash::new();
        assert_eq!(map.delete(99), None);
    }

    #[test]
    fn delete_twice_returns_none_second_time() {
        let mut map = VectorHash::new();
        map.put(5, 50);
        map.delete(5);
        assert_eq!(map.delete(5), None);
    }

    #[test]
    fn tombstone_does_not_break_probe_chain() {
        let mut map = VectorHash::new();
        for i in 0..60 {
            map.put(i, i * 10);
        }
        map.delete(20);
        for i in 0..60 {
            if i == 20 {
                assert_eq!(map.get(i), None);
            } else {
                assert_eq!(map.get(i), Some(i * 10));
            }
        }
    }

    #[test]
    fn put_after_delete_reuses_slot() {
        let mut map = VectorHash::new();
        map.put(1, 10);
        map.put(2, 20);
        map.delete(1);
        assert_eq!(map.put(1, 99), None);
        assert_eq!(map.get(1), Some(99));
        assert_eq!(map.get(2), Some(20));
    }

    #[test]
    fn resize_preserves_all_entries() {
        let mut map = VectorHash::new();
        for i in 0..200 {
            map.put(i, i * 3);
        }
        for i in 0..200 {
            assert_eq!(map.get(i), Some(i * 3));
        }
    }

    #[test]
    fn with_capacity_basic_ops() {
        let mut map = VectorHash::with_capacity(64);
        map.put(1000, 9999);
        assert_eq!(map.get(1000), Some(9999));
    }

    #[test]
    fn zero_key_and_value() {
        let mut map = VectorHash::new();
        assert_eq!(map.put(0, 0), None);
        assert_eq!(map.get(0), Some(0));
        assert_eq!(map.delete(0), Some(0));
        assert_eq!(map.get(0), None);
    }

    #[test]
    fn u64_max_key() {
        let mut map = VectorHash::new();
        map.put(u64::MAX, 42);
        assert_eq!(map.get(u64::MAX), Some(42));
        assert_eq!(map.delete(u64::MAX), Some(42));
        assert_eq!(map.get(u64::MAX), None);
    }

    #[test]
    fn many_deletes_then_inserts() {
        let mut map = VectorHash::new();
        for i in 0..100 {
            map.put(i, i);
        }
        for i in 0..50 {
            map.delete(i);
        }
        for i in 0..50 {
            map.put(i, i * 2);
        }
        for i in 0..100 {
            let expected = if i < 50 { i * 2 } else { i };
            assert_eq!(map.get(i), Some(expected));
        }
    }

    #[test]
    fn stress_sequential_keys() {
        let mut map = VectorHash::new();
        let n = 10_000u64;
        for i in 0..n {
            assert_eq!(map.put(i, i * 7), None);
        }
        for i in 0..n {
            assert_eq!(map.get(i), Some(i * 7));
        }
        for i in 0..n {
            assert_eq!(map.delete(i), Some(i * 7));
            assert_eq!(map.get(i), None);
        }
    }

    #[test]
    fn stress_overwrite_same_key() {
        let mut map = VectorHash::new();
        let mut prev = None;
        for i in 0..10_000u64 {
            let ret = map.put(42, i);
            assert_eq!(ret, prev);
            prev = Some(i);
        }
        assert_eq!(map.get(42), Some(9_999));
    }

    #[test]
    fn stress_interleaved_put_delete() {
        let mut map = VectorHash::new();
        let n = 5_000u64;

        for i in 0..n {
            map.put(i, i);
        }

        for i in 0..n {
            if i % 2 == 0 {
                assert_eq!(map.delete(i), Some(i));
            } else {
                assert_eq!(map.put(i, i * 3), Some(i));
            }
            map.put(n + i, i * 5);
        }

        for i in 0..n {
            if i % 2 == 0 {
                assert_eq!(map.get(i), None);
            } else {
                assert_eq!(map.get(i), Some(i * 3));
            }
            assert_eq!(map.get(n + i), Some(i * 5));
        }
    }

    #[test]
    fn stress_sparse_large_keys() {
        let mut map = VectorHash::new();
        let keys: Vec<u64> = (0..5_000).map(|i| i * 1_000_003).collect();

        for &k in &keys {
            map.put(k, k ^ 0xDEADBEEF);
        }

        for &k in &keys {
            assert_eq!(map.get(k), Some(k ^ 0xDEADBEEF));
        }

        for &k in keys.iter().step_by(2) {
            map.delete(k);
        }

        for (i, &k) in keys.iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(map.get(k), None);
            } else {
                assert_eq!(map.get(k), Some(k ^ 0xDEADBEEF));
            }
        }
    }

    #[test]
    fn stress_multiple_resizes() {
        let mut map = VectorHash::with_capacity(16);
        let n = 50_000u64;
        for i in 0..n {
            map.put(i, i);
        }

        for i in 0..n {
            assert_eq!(map.get(i), Some(i), "missing key {i} after resizes");
        }
    }

    #[test]
    fn stress_delete_all_then_reinsert() {
        let mut map = VectorHash::new();
        let n = 5_000u64;
        for i in 0..n {
            map.put(i, i);
        }
        for i in 0..n {
            assert_eq!(map.delete(i), Some(i));
        }

        for i in 0..n {
            assert_eq!(map.put(i, i * 2), None);
        }
        for i in 0..n {
            assert_eq!(map.get(i), Some(i * 2));
        }
    }
}
