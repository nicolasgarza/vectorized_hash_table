use std::cmp::max;
use ahash::AHasher;
use std::hash::{Hash, Hasher};
use std::mem::MaybeUninit;
use std::ptr;

pub struct VectorHash<K: Hash + Eq, V> {
    size: usize,
    keys: Vec<MaybeUninit<K>>,
    values: Vec<MaybeUninit<V>>,
    ctrl: Vec<u8>,

    // load factor: 0.5
    elements: usize,
    tombstones: usize,
    resize_threshold: usize,
}


const EMPTY: u8 = 0x80;
const FULL: u8 = 0x00;
const TOMBSTONE: u8 = 0xFE;

impl<K: Hash + Eq, V> VectorHash<K, V> {
    pub fn new() -> Self {
        let mut keys = Vec::with_capacity(128);
        let mut values = Vec::with_capacity(128);
        unsafe {
            keys.set_len(128);
            values.set_len(128);
        }

        VectorHash {
            size: 128,
            keys,
            values,
            ctrl: vec![EMPTY; 128], // 0 => unoccupied, 1 => occupied, 2 => tombstone

            elements: 0,
            tombstones: 0,
            resize_threshold: 64, // size / 2
        }
    }

    pub fn with_capacity(size: usize) -> Self {
        let size = max(size, 128);
        let mut keys = Vec::with_capacity(size);
        let mut values = Vec::with_capacity(size);
        unsafe {
            keys.set_len(size);
            values.set_len(size);
        }

        VectorHash{
            size,
            keys,
            values,
            ctrl: vec![EMPTY; size],

            elements: 0,
            tombstones: 0,
            resize_threshold: size / 2,
        }
    }

    #[inline(never)] // for flamegraph
    pub fn get(&self, key: &K) -> Option<&V> {
        let (mut i, hash) = self.index(key);
        let fingerprint = (hash & 0x7F) as u8;

        let mut candidates: Vec<usize> = Vec::with_capacity(10);
        loop {
            // never an infinite loop, as there are always empty slots in array
            match self.ctrl[i] {
                EMPTY => break,
                TOMBSTONE => {},
                _ if self.ctrl[i] & 0x7F == fingerprint => candidates.push(i),
                _ => {},
            };
            i = (i + 1) & self.size - 1
        }

        for i in candidates {
            if key == unsafe { self.keys[i].assume_init_ref() } {
                return Some(unsafe { self.values[i].assume_init_ref() })
            }
        }

        None
    }

    #[inline(never)] // for flamegraph
    pub fn put(&mut self, key: K, value: V) -> Option<V> {
        let (mut i, hash) = self.index(&key);
        let mut first_deleted: Option<usize> = None;

        loop {
            match self.ctrl[i] {
                EMPTY => { // empty, put element here
                    let i = first_deleted.unwrap_or(i);
                    if first_deleted.is_some() {
                        self.tombstones -= 1;
                    }

                    self.ctrl[i] = (hash & 0x7F) as u8;
                    self.keys[i].write(key);
                    self.values[i].write(value);
                    self.elements += 1;
                    if self.elements + self.tombstones >= self.resize_threshold {
                        self.resize();
                    }
                    return None;
                }
                TOMBSTONE => { // can place at first tombstone we encounter
                    if first_deleted.is_none() {
                        first_deleted = Some(i);
                    }
                }
                _ if self.ctrl[i] & 0x80 == FULL && unsafe { self.keys[i].assume_init_ref() } == &key => { // occupied with same key TODO: use same as get logic
                    let old = unsafe { ptr::read(self.values[i].as_ptr()) };
                    self.values[i].write(value);
                    return Some(old);
                }
                _ => {},
            }
            i = (i + 1) & self.size - 1;
        }
    }

    #[inline(never)] // for flamegraph
    pub fn delete(&mut self, key: &K) -> Option<V> {
        let (mut i, _) = self.index(key);

        loop {
            match self.ctrl[i] {
                EMPTY => return None,
                _ if self.ctrl[i] & 0x80 == FULL && unsafe { self.keys[i].assume_init_ref() } == key => { // TODO: same as get logic
                    self.ctrl[i] = 0xFF;
                    self.tombstones += 1;
                    self.elements -= 1;
                    if self.tombstones > self.size / 3 {
                        self.clear_tombstones();
                    }

                    unsafe { std::ptr::drop_in_place(self.keys[i].as_mut_ptr()) };
                    return Some(unsafe { ptr::read(self.values[i].as_ptr()) });
                }
                _ => i = (i + 1) & self.size - 1,
            }
        }
    }

    #[inline(never)] // for flamegraph
    fn resize(&mut self) {
        let mut new_map = VectorHash::<K, V>::with_capacity(self.size * 4);

        let old_keys = std::mem::take(&mut self.keys);
        let old_values = std::mem::take(&mut self.values);
        let old_ctrl = std::mem::take(&mut self.ctrl);

        for i in 0..self.size {
            if old_ctrl[i] & 0x80 == FULL {
                unsafe {
                    let k = ptr::read(old_keys[i].as_ptr());
                    let v = ptr::read(old_values[i].as_ptr());
                    new_map.put(k, v);
                }
            }
        }

        *self = new_map;
    }

    #[inline(never)] // for flamegraph
    fn clear_tombstones(&mut self) {
        let mut new_map = VectorHash::<K, V>::with_capacity(self.size);

        let old_keys = std::mem::take(&mut self.keys);
        let old_values = std::mem::take(&mut self.values);
        let old_ctrl = std::mem::take(&mut self.ctrl);

        for i in 0..self.size {
            if old_ctrl[i] & 0x80 == FULL {
                unsafe {
                    let k = ptr::read(old_keys[i].as_ptr());
                    let v = ptr::read(old_values[i].as_ptr());
                    new_map.put(k, v);
                }
            }
        }

        *self = new_map;
    }

    #[inline(never)] // for flamegraph
    fn index(&self, key: &K) -> (usize, u64) {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        (hash as usize & (self.size - 1), hash)
    }
}

impl<K: Hash + Eq, V> Drop for VectorHash<K, V> {
    fn drop(&mut self) {
        let mut old_keys = std::mem::take(&mut self.keys);
        let mut old_values = std::mem::take(&mut self.values);
        let old_ctrl = std::mem::take(&mut self.ctrl);

        for i in 0..old_ctrl.len() {
            if old_ctrl[i] & 0x80 == FULL {
                unsafe {
                    ptr::drop_in_place(old_keys[i].as_mut_ptr());
                    ptr::drop_in_place(old_values[i].as_mut_ptr());
                }
            }
        }
    }
}
