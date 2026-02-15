// use std::collections::hash_map::DefaultHasher;
// use std::hash::{Hash, Hasher};

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

impl VectorHash {
    pub fn new() -> Self {
        VectorHash{
            size: 100,
            keys: vec![0; 100],
            values: vec![0; 100],
            ctrl: vec![0; 100], // 0 => unoccupied, 1 => occupied, 2 => tombstone

            elements: 0,
            tombstones: 0,
            resize_threshold: 50, // size / 2
        }
    }

    pub fn with_capacity(size: usize) -> Self {
        VectorHash{
            size: size,
            keys: vec![0; size],
            values: vec![0; size],
            ctrl: vec![0; size], // 0 => unoccupied, 1 => occupied, 2 => tombstone

            elements: 0,
            tombstones: 0,
            resize_threshold: size / 2,
        }
    }

    pub fn get(&self, key: u64) -> Option<u64> {
        let mut i = self.index(key);

        loop {
            // never an infinite loop, as there are always empty slots in array
            match self.ctrl[i] {
                0 => return None,
                1 if self.keys[i] == key => return Some(self.values[i]),
                2 if self.keys[i] == key => return None,
                _ => i = (i + 1) % self.size,
            }
        }
    }

    pub fn put(&mut self, key: u64, value: u64) -> Option<u64> {
        let mut i = self.index(key);
        let mut first_deleted: Option<usize> = None;

        loop {
            match self.ctrl[i] {
                0 => { // empty, put element here
                    let i = first_deleted.unwrap_or(i);
                    self.ctrl[i] = 1;
                    self.keys[i] = key;
                    self.values[i] = value;
                    self.elements += 1;
                    if self.elements + self.tombstones >= self.resize_threshold {
                        self.resize();
                    }
                    return None;
                }
                1 if self.keys[i] == key => { // occupied with same key
                    let old = self.values[i];
                    self.values[i] = value;
                    return Some(old);
                }
                2 => { // can place at first tombstone we encounter
                    if first_deleted.is_none() {
                        first_deleted = Some(i);
                    }
                    i = (i + 1) % self.size;
                }
                _ => i = (i + 1) % self.size,
            }
        }
    }

    pub fn delete(&mut self, key: u64) -> Option<u64> {
        let mut i = self.index(key);

        loop {
            match self.ctrl[i] {
                0 => return None,
                1 if self.keys[i] == key => {
                    self.ctrl[i] = 2;
                    self.tombstones += 1;
                    self.elements -= 1;
                    return Some(self.values[i]);
                }
                _ => i = (i + 1) % self.size,
            }
        }
    }

    fn resize(&mut self) {
        let mut new_map = VectorHash::with_capacity(self.size * 10);

        for i in 0..self.size {
            if self.ctrl[i] == 1 {
                new_map.put(self.keys[i], self.values[i]);
            }
        }

        *self = new_map;
    }

    fn index(&self, key: u64) -> usize {
        key as usize % self.size
    }
}
