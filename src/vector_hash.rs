use std::cmp::max;
use std::collections::hash_map::DefaultHasher;
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


const EMPTY: u8 = 0;
const FULL: u8 = 1;
const TOMBSTONE: u8 = 2;

impl<K: Hash + Eq, V> VectorHash<K, V> {
    pub fn new() -> Self {
        let mut keys = Vec::with_capacity(100);
        let mut values = Vec::with_capacity(100);
        unsafe {
            keys.set_len(100);
            values.set_len(100);
        }

        VectorHash {
            size: 100,
            keys: keys,
            values: values,
            ctrl: vec![EMPTY; 100], // 0 => unoccupied, 1 => occupied, 2 => tombstone

            elements: 0,
            tombstones: 0,
            resize_threshold: 50, // size / 2
        }
    }

    pub fn with_capacity(size: usize) -> Self {
        let size = max(size, 100);
        let mut keys = Vec::with_capacity(size);
        let mut values = Vec::with_capacity(size);
        unsafe {
            keys.set_len(size);
            values.set_len(size);
        }

        VectorHash{
            size: size,
            keys: keys,
            values: values,
            ctrl: vec![EMPTY; size], // 0 => unoccupied, 1 => occupied, 2 => tombstone

            elements: 0,
            tombstones: 0,
            resize_threshold: size / 2,
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let mut i = self.index(&key);

        loop {
            // never an infinite loop, as there are always empty slots in array
            match self.ctrl[i] {
                EMPTY => return None,
                FULL if unsafe { self.keys[i].assume_init_ref() } == key => return Some(unsafe { self.values[i].assume_init_ref() }),
                _ => i = (i + 1) % self.size,
            }
        }
    }

    pub fn put(&mut self, key: K, value: V) -> Option<V> {
        let mut i = self.index(&key);
        let mut first_deleted: Option<usize> = None;

        loop {
            match self.ctrl[i] {
                EMPTY => { // empty, put element here
                    let i = first_deleted.unwrap_or(i);
                    if first_deleted.is_some() {
                        self.tombstones -= 1;
                    }

                    self.ctrl[i] = FULL;
                    self.keys[i].write(key);
                    self.values[i].write(value);
                    self.elements += 1;
                    if self.elements + self.tombstones >= self.resize_threshold {
                        self.resize();
                    }
                    return None;
                }
                FULL if unsafe { self.keys[i].assume_init_ref() } == &key => { // occupied with same key
                    let old = unsafe { ptr::read(self.values[i].as_ptr()) };
                    self.values[i].write(value);
                    return Some(old);
                }
                TOMBSTONE => { // can place at first tombstone we encounter
                    if first_deleted.is_none() {
                        first_deleted = Some(i);
                    }
                    i = (i + 1) % self.size;
                }
                _ => i = (i + 1) % self.size,
            }
        }
    }

    pub fn delete(&mut self, key: &K) -> Option<V> {
        let mut i = self.index(&key);

        loop {
            match self.ctrl[i] {
                EMPTY => return None,
                FULL if unsafe { self.keys[i].assume_init_ref() } == key => {
                    self.ctrl[i] = TOMBSTONE;
                    self.tombstones += 1;
                    self.elements -= 1;

                    unsafe { std::ptr::drop_in_place(self.keys[i].as_mut_ptr()) };
                    return Some(unsafe { ptr::read(self.values[i].as_ptr()) });
                }
                _ => i = (i + 1) % self.size,
            }
        }
    }

    fn resize(&mut self) {
        let mut new_map = VectorHash::<K, V>::with_capacity(self.size * 3);

        let old_keys = std::mem::take(&mut self.keys);
        let old_values = std::mem::take(&mut self.values);
        let old_ctrl = std::mem::take(&mut self.ctrl);

        for i in 0..self.size {
            if old_ctrl[i] == FULL {
                unsafe {
                    let k = ptr::read(old_keys[i].as_ptr());
                    let v = ptr::read(old_values[i].as_ptr());
                    new_map.put(k, v);
                }
            }
        }

        *self = new_map;
    }

    fn index(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        hash as usize % self.size
    }
}

impl<K: Hash + Eq, V> Drop for VectorHash<K, V> {
    fn drop(&mut self) {
        let mut old_keys = std::mem::take(&mut self.keys);
        let mut old_values = std::mem::take(&mut self.values);
        let old_ctrl = std::mem::take(&mut self.ctrl);

        for i in 0..old_ctrl.len() {
            if old_ctrl[i] == FULL {
                unsafe {
                    ptr::drop_in_place(old_keys[i].as_mut_ptr());
                    ptr::drop_in_place(old_values[i].as_mut_ptr());
                }
            }
        }
    }
}
