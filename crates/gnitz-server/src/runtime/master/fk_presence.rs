//! Master-side FK presence caches: for each referenced table whose FK target is
//! its lone PK column, a bounded set of the keys known to be committed, so an FK
//! insert probes only the references it cannot prove present.
//!
//! An entry is the key's whole image, never a fingerprint: a collision would
//! admit a reference to a row that does not exist. It holds only while the key
//! is committed — a write to the referenced table evicts what it retracts before
//! its table lock is released, and a write that references the table holds that
//! lock.

use std::cell::Ref;

use rustc_hash::FxHashSet;

use super::*;

/// The largest committed write whose inserted keys are recorded.
const FILL_MAX_ROWS: usize = 64;

pub(crate) struct FkPresence {
    keys: FxHashSet<u128>,
    /// Keys held before the set is dropped whole.
    cap: usize,
}

impl FkPresence {
    fn new(cap: usize) -> Self {
        FkPresence { keys: FxHashSet::default(), cap }
    }

    pub(super) fn holds(&self, key: u128) -> bool {
        self.keys.contains(&key)
    }

    /// Record `key` as committed. Past the cap the set is dropped whole, and
    /// every key is proven again by its next probe.
    fn record(&mut self, key: u128) {
        if self.keys.len() >= self.cap && !self.keys.contains(&key) {
            self.keys.clear();
        }
        self.keys.insert(key);
    }

    /// Apply a durable `batch` of the table: per key the batch's last row
    /// decides, as it does in the store. A retraction always evicts; only a
    /// small write's inserts are recorded, so a bulk load of the table costs
    /// nothing here.
    fn apply(&mut self, batch: &Batch) {
        let records = batch.len() <= FILL_MAX_ROWS;
        if !records && batch.all_weights_positive() {
            return;
        }
        for row in 0..batch.len() {
            let key = gnitz_wire::widen_pk_be(batch.get_pk_bytes(row));
            match batch.get_weight(row) {
                0 => {}
                w if w > 0 => {
                    if records {
                        self.record(key);
                    }
                }
                _ => {
                    self.keys.remove(&key);
                }
            }
        }
    }
}

impl MasterDispatcher {
    /// `parent_tid`'s cache, when it has one.
    pub(super) fn fk_presence_of(&self, parent_tid: u64) -> Option<Ref<'_, FkPresence>> {
        Ref::filter_map(self.fk_presence.borrow(), |caches| caches.get(&parent_tid)).ok()
    }

    /// Record that a probe found `key` committed in `parent_tid`.
    pub(super) fn fk_presence_found(&self, parent_tid: u64, key: u128) {
        self.fk_presence
            .borrow_mut()
            .entry(parent_tid)
            .or_insert_with(|| FkPresence::new(self.fk_presence_cap))
            .record(key);
    }

    /// Apply a durable `batch` of `tid` to its cache, when it has one.
    pub(super) fn fk_presence_ingest_batch(&self, tid: u64, batch: &Batch) {
        if let Some(cache) = self.fk_presence.borrow_mut().get_mut(&tid) {
            cache.apply(batch);
        }
    }

    /// Drop `tid`'s cache.
    pub(super) fn fk_presence_invalidate_table(&self, tid: u64) {
        self.fk_presence.borrow_mut().remove(&tid);
    }
}

#[cfg(test)]
#[path = "tests/fk_presence.rs"]
mod tests;
