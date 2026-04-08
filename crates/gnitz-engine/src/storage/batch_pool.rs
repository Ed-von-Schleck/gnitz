//! Thread-local pool of cleared `OwnedBatch` objects.
//!
//! Keyed by `num_payload_cols` (`col_data.len()`) so recycled batches match
//! the caller's schema shape.  Capacity mismatch is fine — a batch with
//! capacity 1024 works when the caller needs 16, since the existing
//! capacity averts malloc.
//!
//! One worker = one OS thread = no contention.

use std::cell::RefCell;
use std::collections::HashMap;

use super::memtable::OwnedBatch;

const MAX_PER_BUCKET: usize = 32;
const MAX_TOTAL: usize = 256;

struct BatchPool {
    buckets: HashMap<usize, Vec<OwnedBatch>>,
    total: usize,
}

impl BatchPool {
    fn new() -> Self {
        BatchPool {
            buckets: HashMap::new(),
            total: 0,
        }
    }
}

thread_local! {
    static POOL: RefCell<BatchPool> = RefCell::new(BatchPool::new());
}

/// Try to acquire a cleared batch with the given number of payload columns.
/// Returns `None` on pool miss (cold start or bucket empty).
pub(crate) fn acquire(num_payload_cols: usize) -> Option<OwnedBatch> {
    POOL.with(|pool| {
        let mut p = pool.borrow_mut();
        if let Some(bucket) = p.buckets.get_mut(&num_payload_cols) {
            if let Some(batch) = bucket.pop() {
                p.total -= 1;
                return Some(batch);
            }
        }
        None
    })
}

/// Return a batch to the pool for reuse.  Clears all buffers (retaining
/// capacity) and resets schema to `None`.
pub(crate) fn recycle(mut batch: OwnedBatch) {
    batch.clear();
    batch.schema = None;
    let npc = batch.col_data.len();
    POOL.with(|pool| {
        let mut p = pool.borrow_mut();
        if p.total >= MAX_TOTAL {
            return; // drop instead of pooling
        }
        let bucket = p.buckets.entry(npc).or_insert_with(Vec::new);
        if bucket.len() >= MAX_PER_BUCKET {
            return; // drop
        }
        bucket.push(batch);
        p.total += 1;
    });
}
