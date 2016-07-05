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

impl Clone for Entry {
    fn clone(&self) -> Self {
        Entry::new(self.key(), self.value())
    }
}

impl Entry {
    fn new(key: usize, val: usize) -> Self {
        Entry { 
            key: AtomicUsize::new(key),
            value: AtomicUsize::new(val),
        }
    }

    pub fn key(&self) -> usize {
        self.key.load(Ordering::Relaxed)
    }


    pub fn value(&self) -> usize {
        self.value.load(Ordering::Relaxed)
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
    population: AtomicUsize,
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
            population: AtomicUsize::new(0),
            hasher: hasher,
        }
    }

    pub fn entries(&self) -> Vec<Entry> {
        let mut vec = Vec::with_capacity(self.size);
        for e in self.storage.iter() {
            let entry = e.clone();
            vec.push(entry);
        }
        vec
    }

    fn hash_key(&self, key: usize) -> usize {
        let size = self.size;
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        (hasher.finish() & ((size as u64) - 1)) as usize
    }

    fn insert_at(entry: &mut Entry, key: usize, val: usize) -> bool {
        let prev_key = entry.key.load(Ordering::Relaxed);
        if prev_key != key {
            if prev_key != 0 {
                return false;
            }
            let prev_key = entry.key.compare_and_swap(0, key, Ordering::Relaxed);
            if prev_key != 0 && prev_key != key {
                return false;
            }
        }
        entry.value.store(val, Ordering::Relaxed);
        return true;
    }

    /// Insert value at key. Returns true if successful.
    pub fn insert(&mut self, key: usize, val: usize) -> bool {
        if self.population.load(Ordering::Relaxed) == self.size {
            return false;
        }
        let idx = self.hash_key(key);
        {
            let from_idx_to_end = &mut self.storage[idx..];
            for e in from_idx_to_end {
                let inserted = Self::insert_at(e, key, val);
                if inserted {
                    self.population.fetch_add(1, Ordering::Acquire);
                    return true;
                }
            }
        }
        {
            let from_start_to_idx = &mut self.storage[0..idx];
            for e in from_start_to_idx {
                let inserted = Self::insert_at(e, key, val);
                if inserted {
                    self.population.fetch_add(1, Ordering::Acquire);
                    return true;
                }
            }
        }
        false
    }

    fn delete_at(entry: &mut Entry, key: usize) -> bool {
        let prev_key = entry.key.load(Ordering::Relaxed);
        if prev_key != key {
            return false;
        }
        let val = entry.value.load(Ordering::Relaxed);
        if entry.value.compare_and_swap(val, 0, Ordering::Relaxed) == val {
            true
        } else {
            false
        }
    }

    pub fn delete(&mut self, key: usize) -> bool {
        let idx = self.hash_key(key);
        {
            let from_idx_to_end = &mut self.storage[idx..];
            for e in from_idx_to_end {
                let found = Self::delete_at(e, key);
                if found {
                    self.population.fetch_sub(1, Ordering::Acquire);
                    return true;
                }
            }
        }
        {
            let from_start_to_idx = &mut self.storage[0..idx];
            for e in from_start_to_idx {
                let found = Self::delete_at(e, key);
                if found {
                    self.population.fetch_sub(1, Ordering::Acquire);
                    return true;
                }
            }
        }
        false
    }

    fn get_at(entry: &Entry, key: usize) -> (bool, Option<usize>) {
        let probed_key = entry.key.load(Ordering::Relaxed);
        if probed_key == key {
            let val = entry.value.load(Ordering::Relaxed);
            if val == 0 {
                return (true, None);
            }
            return (true, Some(entry.value.load(Ordering::Relaxed)));
        }
        if probed_key == 0 {
            return (true, None);
        }
        (false, None)
    }

    pub fn get(&self, key: usize) -> Option<usize> {
        let idx = self.hash_key(key);
        let from_idx_to_end = &self.storage[idx..];
        for e in from_idx_to_end {
            let (found, val) = Self::get_at(e, key);
            if found {
                return val;
            }
        }
        let from_start_to_idx = &self.storage[0..idx];
        for e in from_start_to_idx {
            let (found, val) = Self::get_at(e, key);
            if found {
                return val;
            }
        }
        return None;
    }


    pub fn population(&self) -> usize {
        self.population.load(Ordering::Relaxed)
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

#[cfg(test)]
mod tests {
    extern crate test;
    use self::test::Bencher;

    #[test]
    fn test_clone() {
        use super::*;
        use std::collections::hash_map::RandomState;

        let mut map = LocklessIntMap::new(1024, RandomState::new());
        map.insert(1, 1);
        map.insert(2, 3);
        println!("KEY: {:?} VAL: {:?}", 1, map.get(1));
        assert!(map.get(1) == Some(1));
        assert!(map.get(2) == Some(3));

        let mut map2 = map.clone();
        println!("KEY: {:?} VAL: {:?}", 1, map2.get(1));
        assert!(map2.get(1) == Some(1));
        assert!(map2.get(2) == Some(3));
    }

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
            for i in 1..1000 {
                map.insert(i, i);
            }
        });
        for i in 1..1000 {
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
                for i in 1..1000 {
                    m.insert(i, i);
                }
            });
            let mut m = map.clone();
            let t2 = thread::spawn(move || {
                for i in (1..1000).rev() {
                    m.insert(i, i);
                }
            });
            t1.join().unwrap();
            t2.join().unwrap();
        });
        for i in 1..1000 {
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
                for i in 1..500 {
                    let mut m = (*m).write().unwrap();
                    m.insert(i, i);
                }
            });
            let m = map.clone();
            let t2 = thread::spawn(move || {
                for i in (1..1000).rev() {
                    let mut m = (*m).write().unwrap();
                    m.insert(i, i);
                }
            });
            t1.join().unwrap();
            t2.join().unwrap();
        });
    }

    #[test]
    fn over_pop() {
        use super::*;
        use std::collections::hash_map::RandomState;

        let mut map = LocklessIntMap::new(8, RandomState::new());
        for i in 1..10 {
            let res = map.insert(i, i);
            if i >= 9 {
                assert!(res == false);
            }
        }
        for i in 1..9 {
            assert!(Some(i) == map.get(i));
        }
    }


    #[test]
    fn test_delete() {
        use super::*;
        use std::collections::hash_map::RandomState;

        let mut map = LocklessIntMap::new(8, RandomState::new());
        for i in 1..9 {
            let res = map.insert(i, i);
        }
        map.delete(5);
        for i in 1..9 {
            if i == 5 {
                assert!(None == map.get(i));
            } else {
                assert!(Some(i) == map.get(i));
            }
        }
        assert!(map.population() == 7);
        map.insert(5, 8);
        assert!(map.get(5) == Some(8));
        assert!(map.population() == 8);
    }
}
