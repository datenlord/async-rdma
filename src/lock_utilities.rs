use parking_lot::lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock as RawRwLockTrait};
use parking_lot::{Mutex, MutexGuard, RawRwLock, RwLockReadGuard, RwLockWriteGuard};
use std::fmt::Debug;
use std::mem;
use std::ops::{Deref, DerefMut};

use crate::memory_region::local::LocalMrInner;

/// Used to transfer rwlock read guard to another type
pub struct MappedRwLockReadGuard<'a, T> {
    /// `RawRwLock` reference
    raw: &'a RawRwLock,
    /// data with lock
    data: T,
}

impl<T: Debug> Debug for MappedRwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedRwLockReadGuard")
            .field("data", &&self.data)
            .finish()
    }
}

impl<T> Drop for MappedRwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        // SAFETY: raw lock was locked before this guard
        unsafe { self.raw.unlock_shared() }
    }
}

impl<T> Deref for MappedRwLockReadGuard<'_, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.data
    }
}

impl<'a, T> MappedRwLockReadGuard<'a, T> {
    /// Create a new `MappedRwLockReadGuard` from another `MappedRwLockReadGuard`
    pub(crate) fn map<F, U>(s: Self, f: F) -> MappedRwLockReadGuard<'a, U>
    where
        F: FnOnce(T) -> U,
    {
        let raw = s.raw;
        // SAFETY: copy from the same type
        let data = unsafe { mem::transmute_copy(&s.data) };
        #[allow(clippy::mem_forget)] // forget parent guard to avoid double unlock
        mem::forget(s);
        let data = f(data);
        MappedRwLockReadGuard { raw, data }
    }

    /// Create a new `RwLockReadGuard`
    pub(crate) fn new<U>(s: RwLockReadGuard<'a, U>, data: T) -> Self {
        // SAFETY: only one mapped lock guard hold raw lock reference and will unlock
        // raw lock in the right way when the guard dropped.
        let raw = unsafe { RwLockReadGuard::rwlock(&s).raw() };
        #[allow(clippy::mem_forget)] // forget parent guard to avoid double unlock
        mem::forget(s);
        MappedRwLockReadGuard { raw, data }
    }
}

/// Used to transfer rwlock write guard to another type
pub struct MappedRwLockWriteGuard<'a, T> {
    /// `RawRwLock` reference
    raw: &'a RawRwLock,
    /// data with lock
    data: T,
}

impl<T: Debug> Debug for MappedRwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedRwLockWriteGuard")
            .field("data", &&self.data)
            .finish()
    }
}

impl<T> Drop for MappedRwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        // SAFETY: raw lock was locked before this guard
        unsafe { self.raw.unlock_exclusive() }
    }
}

impl<T> Deref for MappedRwLockWriteGuard<'_, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.data
    }
}

impl<T> DerefMut for MappedRwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.data
    }
}

impl<'a, T> MappedRwLockWriteGuard<'a, T> {
    /// Create a new `MappedRwLockWriteGuard` from another `MappedRwLockWriteGuard`
    pub(crate) fn map<F, U>(s: Self, f: F) -> MappedRwLockWriteGuard<'a, U>
    where
        F: FnOnce(T) -> U,
    {
        let raw = s.raw;
        // SAFETY: copy from the same type
        let data = unsafe { mem::transmute_copy(&s.data) };
        #[allow(clippy::mem_forget)] // forget parent guard to avoid double unlock
        mem::forget(s);
        let data = f(data);
        MappedRwLockWriteGuard { raw, data }
    }

    /// Create a new `MappedRwLockWriteGuard`
    pub(crate) fn new<U>(s: RwLockWriteGuard<'a, U>, data: T) -> Self {
        // SAFETY: only one mapped lock guard hold raw lock reference and will unlock
        // raw lock in the right way when the guard dropped.
        let raw = unsafe { RwLockWriteGuard::rwlock(&s).raw() };
        #[allow(clippy::mem_forget)] // forget parent guard to avoid double unlock
        mem::forget(s);
        MappedRwLockWriteGuard { raw, data }
    }
}

/// Insert `ArcRwLockGuard` to a map and remove it when the corresponding
/// `LocalMrInner`'s RDMA operation is done to ensure that mr will not be misused during ops.
#[derive(Debug)]
pub(crate) enum ArcRwLockGuard {
    /// An RAII rwlock guard returned by the `rwlock.read_arc()`
    RwLockReadGuard(ArcRwLockReadGuard<RawRwLock, LocalMrInner>),
    /// An RAII rwlock guard returned by the `rwlock.write_arc()`
    RwLockWriteGuard(ArcRwLockWriteGuard<RawRwLock, LocalMrInner>),
}

/// Provides functional expression methods for `Mutex`.
pub(crate) trait MappedMutex<T> {
    /// Use `func` to read the value in the mutex
    fn map_read<F, R>(&self, func: F) -> R
    where
        F: FnOnce(&MutexGuard<'_, T>) -> R;

    /// Use `func` to write the value in the mutex
    fn map_write<F, R>(&self, func: F) -> R
    where
        F: FnOnce(&mut MutexGuard<'_, T>) -> R;
}

impl<T> MappedMutex<T> for Mutex<T> {
    fn map_read<F, R>(&self, func: F) -> R
    where
        F: FnOnce(&MutexGuard<'_, T>) -> R,
    {
        let guard = self.lock();
        func(&guard)
    }

    fn map_write<F, R>(&self, func: F) -> R
    where
        F: FnOnce(&mut MutexGuard<'_, T>) -> R,
    {
        let mut guard = self.lock();
        func(&mut guard)
    }
}

#[test]
fn mapped_read_guard_test() {
    let lock = parking_lot::RwLock::new(1_i32);
    let guard = lock.read();
    assert_eq!(*guard, 1_i32);
    let mapped_1 = MappedRwLockReadGuard::new(guard, 3_i32);
    assert!(lock.is_locked());
    assert_eq!(*mapped_1, 3_i32);
    let mapped_2 = MappedRwLockReadGuard::map(mapped_1, |num| num + 1_i32);
    assert!(lock.is_locked());
    assert_eq!(*mapped_2, 4_i32);
    drop(mapped_2);
    assert!(!lock.is_locked());
}

#[test]
fn mapped_write_guard_test() {
    let lock = parking_lot::RwLock::new(1_i32);
    let mut guard = lock.write();
    *guard += 1_i32;
    assert_eq!(*guard, 2_i32);
    let mut mapped_1 = MappedRwLockWriteGuard::new(guard, 3_i32);
    *mapped_1 += 1_i32;
    assert_eq!(*mapped_1, 4_i32);
    assert!(lock.is_locked_exclusive());
    let mapped_2 = MappedRwLockWriteGuard::map(mapped_1, |num| num + 1_i32);
    assert!(lock.is_locked_exclusive());
    assert_eq!(*mapped_2, 5_i32);
    drop(mapped_2);
    assert!(!lock.is_locked_exclusive());
}
