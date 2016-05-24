use std::sync::atomic;
use std::sync::atomic::{AtomicUsize,Ordering};
use std::hash::{Hasher,SipHasher,Hash};

#[derive(Debug)]
pub struct Entry {
    key: AtomicUsize,
    value: AtomicUsize,
}

impl Entry {
    fn new(key: usize, val: usize) -> Self {
        Entry { 
            key: AtomicUsize::new(key),
            value: AtomicUsize::new(val),
        }
    }
}

pub struct IntMap<H: Hasher> {
    storage: Vec<Entry>,
    size: usize,
    hasher: H,
}

impl<H: Hasher> IntMap<H> {
    pub fn new(size: usize, hasher: H) -> IntMap<H> {
        let mut v = Vec::with_capacity(size);
        for i in 0..size {
            v.push(Entry::new(0, 0));
        }
        IntMap { 
            storage: v,
            size: size,
            hasher: hasher,
        }
    }

    pub fn insert(&mut self, key: usize, val: usize) {
        let idx = {
            let mut hasher = SipHasher::new();
            let size = self.size;
            key.hash(&mut hasher);
            (hasher.finish() & ((size as u64) - 1)) as usize
        };
        {
            let from_idx_to_end = &mut self.storage[idx..];
            for e in from_idx_to_end {
                let prev_key = e.key.compare_and_swap(0, key, Ordering::Relaxed);
                if prev_key == 0 || prev_key == key {
                    e.value.store(val, Ordering::Relaxed);
                    return;
                }
            }
        }
        let from_start_to_idx = &mut self.storage[0..idx];
        for e in from_start_to_idx {
            let prev_key = e.key.compare_and_swap(0, key, Ordering::Relaxed);
            if prev_key == 0 || prev_key == key {
                e.value.store(val, Ordering::Relaxed);
                return;
            }
        }
    }

    pub fn get(&mut self, key: usize) -> Option<usize> {
        let idx = {
            let mut hasher = SipHasher::new();
            let size = self.size;
            key.hash(&mut hasher);
            (hasher.finish() & ((size as u64) - 1)) as usize
        };
        let from_idx_to_end = &self.storage[idx..];
        for e in from_idx_to_end {
            let probed_key = e.key.load(Ordering::Relaxed);
            if probed_key == key {
                return Some(e.value.load(Ordering::Relaxed));
            }
            if probed_key == 0 {
                return None;
            }
        }
        let from_start_to_idx = &self.storage[0..idx];
        for e in from_start_to_idx {
            let probed_key = e.key.load(Ordering::Relaxed);
            if probed_key == key {
                return Some(e.value.load(Ordering::Relaxed));
            }
            if probed_key == 0 {
                return None;
            }
        }
        return None;
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        use super::*;
        use std::hash::{Hasher,SipHasher};

        let mut map: IntMap<SipHasher> = IntMap::new(1024, SipHasher::new());
        map.insert(1, 1);
        map.insert(2, 3);
        println!("KEY: {:?} VAL: {:?}", 1, map.get(1));
        assert!(map.get(1) == Some(1));
        assert!(map.get(2) == Some(3));
        map.insert(1, 2);
        assert!(map.get(1) == Some(2));
    }
}
