//! Locking idioms `std` lacks.

use std::sync::{Mutex, MutexGuard, PoisonError};

/// `m` locked, poisoned or not: for a lock guarding state a panic under it
/// leaves whole.
pub fn lock<T: ?Sized>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}
