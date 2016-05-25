#![feature(test)]
use std::sync::atomic::{AtomicUsize,Ordering};
use std::hash::{Hasher,BuildHasher,Hash};
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::ops::{Deref,DerefMut};

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

#[derive(Clone)]
pub struct LocklessIntMap<H: BuildHasher> {
    map: Arc<UnsafeCell<IntMap<H>>>
}

unsafe impl<H: BuildHasher> Sync for LocklessIntMap<H> {}
unsafe impl<H: BuildHasher> Send for LocklessIntMap<H> {}
unsafe impl<H: BuildHasher> Sync for IntMap<H> {}
unsafe impl<H: BuildHasher> Send for IntMap<H> {}

impl<H: BuildHasher> LocklessIntMap<H> {
    pub fn new(size: usize, hasher: H) -> Self {
        let map = IntMap::new(size, hasher);
        LocklessIntMap {
            map: Arc::new(UnsafeCell::new(map))
        }
    }
}

impl<H: BuildHasher> Deref for LocklessIntMap<H> {
    type Target = IntMap<H>;

    fn deref(&self) -> &IntMap<H> {
        unsafe { & *(*self.map).get() }
    }
}

impl<H: BuildHasher> DerefMut for LocklessIntMap<H> {
    fn deref_mut(&mut self) -> &mut IntMap<H> {
        unsafe { &mut *(*self.map).get() }
    }
}

pub struct IntMap<H: BuildHasher> {
    storage: Vec<Entry>,
    size: usize,
    hasher: H,
}


impl<H: BuildHasher> IntMap<H> {
    fn new(size: usize, hasher: H) -> IntMap<H> {
        let mut v = Vec::with_capacity(size);
        for _ in 0..size {
            v.push(Entry::new(0, 0));
        }
        IntMap { 
            storage: v,
            size: size,
            hasher: hasher,
        }
    }

    fn hash_key(&self, key: usize) -> usize {
        let size = self.size;
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        (hasher.finish() & ((size as u64) - 1)) as usize
    }

    pub fn insert(&mut self, key: usize, val: usize) {
        let idx = self.hash_key(key);
        {
            let from_idx_to_end = &mut self.storage[idx..];
            for e in from_idx_to_end {
                let prev_key = e.key.load(Ordering::Relaxed);
                if prev_key != key {
                    if prev_key != 0 {
                        continue;
                    }
                    let prev_key = e.key.compare_and_swap(0, key, Ordering::Relaxed);
                    if prev_key != 0 && prev_key != key {
                        continue;
                    }
                }
                e.value.store(val, Ordering::Relaxed);
                return;
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

    pub fn get(&self, key: usize) -> Option<usize> {
        let idx = self.hash_key(key);
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
    extern crate test;
    use self::test::Bencher;

    #[test]
    fn it_works() {
        use super::*;
        use std::collections::hash_map::RandomState;

        let mut map = LocklessIntMap::new(1024, RandomState::new());
        map.insert(1, 1);
        map.insert(2, 3);
        println!("KEY: {:?} VAL: {:?}", 1, map.get(1));
        assert!(map.get(1) == Some(1));
        assert!(map.get(2) == Some(3));
        map.insert(1, 2);
        map.insert(1, 2);
        map.insert(1, 2);
        assert!(map.get(1) == Some(2));
    }

    #[bench]
    fn insert_1000(b: &mut Bencher) {
        use super::*;
        use std::collections::hash_map::RandomState;

        let mut map = LocklessIntMap::new(1024, RandomState::new());

        b.iter(|| {
            for i in 0..1000 {
                map.insert(i, i);
            }
        });
        for i in 0..1000 {
            assert!(map.get(i) == Some(i));
        }
    }

    #[bench]
    fn insert_1000_std(b: &mut Bencher) {
        use std::collections::hash_map::RandomState;
        use std::collections::hash_map::HashMap;

        let mut map = HashMap::with_hasher(RandomState::new());

        b.iter(|| {
            for i in 0..1000 {
                map.insert(i, i);
            }
        })
    }

    #[bench]
    fn multi_thread_insert(b: &mut Bencher) {
        use super::*;
        use std::collections::hash_map::RandomState;
        use std::thread;

        let mut map = LocklessIntMap::new(1024, RandomState::new());

        b.iter(|| {
            let mut m = map.clone();
            let t1 = thread::spawn(move || {
                for i in 0..1000 {
                    m.insert(i, i);
                }
            });
            let mut m = map.clone();
            let t2 = thread::spawn(move || {
                for i in (0..1000).rev() {
                    m.insert(i, i);
                }
            });
            t1.join().unwrap();
            t2.join().unwrap();
        });
        for i in 0..1000 {
            assert!(map.get(i) == Some(i));
        }
    }

    #[bench]
    fn multi_thread_insert_std(b: &mut Bencher) {
        use std::collections::hash_map::HashMap;
        use std::thread;
        use std::sync::Arc;
        use std::sync::RwLock;

        let map = Arc::new(RwLock::new(HashMap::new()));

        b.iter(|| {
            let m = map.clone();
            let t1 = thread::spawn(move || {
                for i in 0..500 {
                    let mut m = (*m).write().unwrap();
                    m.insert(i, i);
                }
            });
            let m = map.clone();
            let t2 = thread::spawn(move || {
                for i in (0..1000).rev() {
                    let mut m = (*m).write().unwrap();
                    m.insert(i, i);
                }
            });
            t1.join().unwrap();
            t2.join().unwrap();
        });
    }
}
