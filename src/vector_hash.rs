use ahash::AHasher;
use std::cmp::max;
use std::hash::{Hash, Hasher};
// use std::simd;
// use std::simd::cmp::SimdPartialEq;

pub struct VectorHash {
    num_keys: usize,
    num_tombstones: usize,
    buckets: Vec<Bucket>,
}

pub struct Bucket {
    fingerprints: [u8; 16],
    pairs: [(u64, u64); 32],
}

impl Default for Bucket {
    fn default() -> Self {
        let fingerprints = [EMPTY; 16];
        Self {
            fingerprints,
            pairs: Default::default(),
        }
    }
}

const EMPTY: u8 = 0x80;
const TOMBSTONE: u8 = 0xFE;
const BUCKET_SIZE: usize = 16;
const LOAD_FACTOR: f64 = 0.7;

impl VectorHash {
    pub fn new() -> Self {
        VectorHash {
            num_keys: 0,
            num_tombstones: 0,
            buckets: (0..16).map(|_| Bucket::default()).collect(),
        }
    }

    pub fn with_capacity(size: usize) -> Self {
        let size = max(size, 16);

        VectorHash {
            num_keys: 0,
            num_tombstones: 0,
            buckets: (0..size).map(|_| Bucket::default()).collect(),
        }
    }

    #[inline(never)] // for flamegraph
    pub fn get(&self, key: u64) -> Option<u64> {
        let (mut i, hash) = self.index(key);
        let fingerprint = (hash & 0x7F) as u8;

        loop {
            // never an infinite loop, as there are always empty slots in array
            match self.search_in_bucket(key, fingerprint, i) {
                (Some(idx), _) => return Some(self.buckets[i].pairs[idx].1),
                (None, Some(_)) => return None,
                (None, None) => (),
            };
            i = (i + 1) & self.buckets.len() - 1
        }
    }

    #[inline(never)] // for flamegraph
    pub fn put(&mut self, key: u64, value: u64) -> Option<u64> {
        if (self.num_keys + self.num_tombstones) as f64 / (self.buckets.len() * BUCKET_SIZE) as f64
            > LOAD_FACTOR
        {
            self.resize();
        }

        let (mut i, hash) = self.index(key);
        let fingerprint = (hash & 0x7F) as u8;

        loop {
            let (check, found_empty) = self.search_in_bucket(key, fingerprint, i);
            if let Some(old_index) = check {
                let old_value = self.buckets[i].pairs[old_index].1;
                self.buckets[i].fingerprints[old_index] = fingerprint;
                self.buckets[i].pairs[old_index] = (key, value);

                return Some(old_value);
            } else if found_empty.is_some() {
                let idx = found_empty.unwrap() as usize;
                self.buckets[i].fingerprints[idx] = fingerprint;
                self.buckets[i].pairs[idx] = (key, value);

                self.num_keys += 1;
                return None;
            }

            i = (i + 1) & self.buckets.len() - 1;
        }
    }

    #[inline(never)] // for flamegraph
    pub fn delete(&mut self, key: u64) -> Option<u64> {
        let (mut i, hash) = self.index(key);
        let fingerprint = (hash & 0x7F) as u8;

        loop {
            match self.search_in_bucket(key, fingerprint, i) {
                (Some(idx), _) => {
                    let old = self.buckets[i].pairs[idx].1;
                    self.buckets[i].fingerprints[idx] = TOMBSTONE;

                    self.num_tombstones += 1;
                    return Some(old);
                }
                (None, Some(_)) => return None,
                (None, None) => (),
            }
            i = (i + 1) & self.buckets.len() - 1;
        }
    }

    #[inline(never)] // for flamegraph
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

    #[inline(never)] // for flamegraph

    // returns an option. if Some(), it will be the matching index
    fn search_in_bucket(
        &self,
        key: u64,
        fingerprint: u8,
        bucket_index: usize,
    ) -> (Option<usize>, Option<usize>) {
        let bucket = &self.buckets[bucket_index];

        let mut found_empty = None;
        let mut candidates = vec![];
        for (idx, print) in bucket.fingerprints.iter().enumerate() {
            if fingerprint == *print {
                candidates.push(idx);
            }
            if *print == EMPTY {
                found_empty = Some(idx);
            }
        }

        if candidates.is_empty() {
            return (None, found_empty);
        }

        for candidate in candidates {
            if key == bucket.pairs[candidate].0 {
                return (Some(candidate), found_empty);
            }
        }

        (None, found_empty)
    }

    #[inline(never)] // for flamegraph
    fn index(&self, key: u64) -> (usize, u64) {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        (hash as usize & (self.buckets.len() - 1), hash)
    }
}

/*
// returns if any element is in the array, and the mask
fn simd_match(fingerprint: u8, buckets: [u8; 16]) -> (bool, [bool; 16]) {
    let cmp_vec: simd::Simd<u8, 16> = simd::Simd::splat(fingerprint);
    let data = simd::Simd::<u8, 16>::from_array(buckets);

    let mask = data.simd_eq(cmp_vec);

    let tomb_vec: simd::Simd<u8, 16> = simd::Simd::splat(EMPTY);
    let tomb_mask = data.simd_eq(tomb_vec);
    (tomb_mask.any(), mask.to_array())
}

*/

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
}
