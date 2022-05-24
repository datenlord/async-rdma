use core::hash::Hash;
use std::collections::{hash_map::Entry, HashMap};

/// `HashMap` extension
pub(crate) trait HashMapExtension<K: Eq + Hash + Copy, V> {
    /// Insert the value if the key does not exist in map
    fn insert_if_not_exists(&mut self, key: K, value: V) -> Option<V>;

    /// Insert value into map until success.
    ///
    /// If the key exists in the map, use `key_gen()` to create a new key and return the new key.
    fn insert_until_success<F: FnOnce() -> K + Copy>(&mut self, value: V, key_gen: F) -> K;
}

impl<K: Eq + Hash + Copy, V> HashMapExtension<K, V> for HashMap<K, V> {
    fn insert_if_not_exists(&mut self, key: K, value: V) -> Option<V> {
        match self.entry(key) {
            Entry::Occupied(_) => Some(value),
            Entry::Vacant(vac) => {
                let _ = vac.insert(value);
                None
            }
        }
    }

    fn insert_until_success<F: FnOnce() -> K + Copy>(&mut self, value: V, key_gen: F) -> K {
        let mut key_stub = key_gen();
        let mut val_stub = value;
        loop {
            match self.insert_if_not_exists(key_stub, val_stub) {
                Some(old_value) => {
                    val_stub = old_value;
                    key_stub = key_gen();
                }
                None => return key_stub,
            }
        }
    }
}

#[test]
fn insert_if_not_exists_test() {
    let mut contributors = HashMap::from([
        ("pwang7", 1_i32),
        ("rogercloud", 2_i32),
        ("gipsyh", 3_i32),
        ("Nugine", 4_i32),
        ("GTwhy", 5_i32),
    ]);
    assert!(contributors
        .insert_if_not_exists("gitter-badger", 6_i32)
        .is_none());
    assert_eq!(
        contributors.insert_if_not_exists("pwang7", 7_i32),
        Some(7_i32)
    );
}

/// Just a key generator simulator
#[allow(dead_code)] // acturally not dead
fn key_gen() -> i32 {
    /// Increase from 0
    static mut NUM: i32 = 0_i32;
    // SAFETY: unsoundness
    // BUG: unsound internal api
    unsafe {
        NUM = NUM.wrapping_add(1_i32);
        NUM
    }
}

#[test]
fn insert_until_success_test() {
    let mut contributors = HashMap::from([
        (1_i32, "pwang7"),
        (2_i32, "rogercloud"),
        (3_i32, "gipsyh"),
        (4_i32, "Nugine"),
    ]);
    assert_eq!(contributors.insert_until_success("GTwhy", key_gen), 5_i32);
    assert_eq!(
        contributors.insert_until_success("gitter-badger", key_gen),
        6_i32
    );
}
