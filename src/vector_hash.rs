use ahash::AHasher;
use std::cmp::max;
use std::hash::{Hash, Hasher};
// use std::simd;
// use std::simd::cmp::SimdPartialEq;

pub struct VectorHash {
    size: usize,
    keys: Vec<u64>,
    values: Vec<u64>,
    ctrl: Vec<u8>,

    // load factor: 0.5
    elements: usize,
    tombstones: usize,
    resize_threshold: usize,
}

const EMPTY: u8 = 0x80;
const FULL: u8 = 0x00;
const TOMBSTONE: u8 = 0xFE;

impl VectorHash {
    pub fn new() -> Self {
        VectorHash {
            size: 128,
            keys: vec![0; 128],
            values: vec![0; 128],
            ctrl: vec![EMPTY; 128], // 0 => unoccupied, 1 => occupied, 2 => tombstone

            elements: 0,
            tombstones: 0,
            resize_threshold: 64, // size / 2
        }
    }

    pub fn with_capacity(size: usize) -> Self {
        let size = max(size, 128); // TODO: align size arugment

        VectorHash {
            size,
            keys: vec![0; 128],
            values: vec![0; 128],
            ctrl: vec![EMPTY; size],

            elements: 0,
            tombstones: 0,
            resize_threshold: size / 2,
        }
    }

    #[inline(never)] // for flamegraph
    pub fn get(&self, key: u64) -> Option<&u64> {
        let (mut i, hash) = self.index(key);
        let fingerprint = (hash & 0x7F) as u8;

        let mut candidates: Vec<usize> = Vec::with_capacity(10);
        loop {
            // never an infinite loop, as there are always empty slots in array
            match self.ctrl[i] {
                EMPTY => break,
                TOMBSTONE => {}
                _ if self.ctrl[i] & 0x7F == fingerprint => candidates.push(i),
                _ => {}
            };
            i = (i + 1) & self.size - 1
        }

        for i in candidates {
            if key == self.keys[i] {
                return Some(&self.values[i]);
            }
        }

        None
    }

    #[inline(never)] // for flamegraph
    pub fn put(&mut self, key: u64, value: u64) -> Option<u64> {
        let (mut i, hash) = self.index(key);
        let mut first_deleted: Option<usize> = None;

        loop {
            match self.ctrl[i] {
                EMPTY => {
                    // empty, put element here
                    let i = first_deleted.unwrap_or(i);
                    if first_deleted.is_some() {
                        self.tombstones -= 1;
                    }

                    self.ctrl[i] = (hash & 0x7F) as u8;
                    self.keys[i] = key;
                    self.values[i] = value;
                    self.elements += 1;
                    if self.elements + self.tombstones >= self.resize_threshold {
                        self.resize();
                    }
                    return None;
                }
                TOMBSTONE => {
                    // can place at first tombstone we encounter
                    if first_deleted.is_none() {
                        first_deleted = Some(i);
                    }
                }
                _ if self.ctrl[i] & 0x80 == FULL && self.keys[i] == key => {
                    // occupied with same key TODO: use same as get logic
                    let old = self.values[i];
                    self.values[i] = value;
                    return Some(old);
                }
                _ => {}
            }
            i = (i + 1) & self.size - 1;
        }
    }

    #[inline(never)] // for flamegraph
    pub fn delete(&mut self, key: u64) -> Option<u64> {
        let (mut i, _) = self.index(key);

        loop {
            match self.ctrl[i] {
                EMPTY => return None,
                _ if self.ctrl[i] & 0x80 == FULL && self.keys[i] == key => {
                    // TODO: same as get logic
                    self.ctrl[i] = 0xFF;
                    self.tombstones += 1;
                    self.elements -= 1;
                    if self.tombstones > self.size / 3 {
                        self.clear_tombstones();
                    }

                    return Some(self.values[i]);
                }
                _ => i = (i + 1) & self.size - 1,
            }
        }
    }

    #[inline(never)] // for flamegraph
    fn resize(&mut self) {
        let mut new_map = VectorHash::with_capacity(self.size * 4);

        let old_keys = std::mem::take(&mut self.keys);
        let old_values = std::mem::take(&mut self.values);
        let old_ctrl = std::mem::take(&mut self.ctrl);

        for i in 0..self.size {
            if old_ctrl[i] & 0x80 == FULL {
                let k = old_keys[i];
                let v = old_values[i];
                new_map.put(k, v);
            }
        }

        *self = new_map;
    }

    #[inline(never)] // for flamegraph
    fn clear_tombstones(&mut self) {
        let mut new_map = VectorHash::with_capacity(self.size);

        let old_keys = std::mem::take(&mut self.keys);
        let old_values = std::mem::take(&mut self.values);
        let old_ctrl = std::mem::take(&mut self.ctrl);

        for i in 0..self.size {
            if old_ctrl[i] & 0x80 == FULL {
                let k = old_keys[i];
                let v = old_values[i];
                new_map.put(k, v);
            }
        }

        *self = new_map;
    }

    #[inline(never)] // for flamegraph
    fn index(&self, key: u64) -> (usize, u64) {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        (hash as usize & (self.size - 1), hash)
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
