//! Write validation: the PK / FK / unique-index rules a user write is checked
//! against before any SAL byte of it is written (`preflight`), and the DDL-time
//! pre-flight of `CREATE UNIQUE INDEX` (`unique_preflight`).
//!
//! [`Checks`] holds the caches that let a write probe less: the unique-index
//! filters, which take a write's spans at its verdict, and the FK presence
//! caches, which take its rows at its commit. Every probe goes to the workers
//! through the master's dispatcher, which each check is handed.

mod preflight;
mod unique_preflight;

use std::cell::RefCell;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::catalog::CatalogEngine;
use crate::runtime::master::MasterDispatcher;
use crate::runtime::reactor::Train;
use crate::runtime::sal::Read;
use gnitz_store::read::Probe;
use gnitz_wire::{PkColList, WireFault};
use gnitz_zset::algebra::extend_spans;
use gnitz_zset::repr::MemBatch;
use gnitz_zset::schema::probe_key;

pub(super) use unique_preflight::validate_unique_index_create;

/// The validation caches of the master's write path.
pub(super) struct Checks {
    /// The filter of each unique index that has one, by (table_id, column
    /// list). Every entry holds all of its index's spans.
    unique_filters: RefCell<FxHashMap<(u64, PkColList), UniqueFilter>>,
    /// Spans one unique filter holds before it caps; a filter that churned past
    /// it is rebuilt from the live index once, under half of it. A test lowers it.
    unique_filter_cap: usize,
    /// For each referenced table whose FK target is its lone PK column and an
    /// FK probe found a key of, by table id: the keys known to be committed, so
    /// an FK insert probes only the references it cannot prove present.
    ///
    /// An entry is the key's whole image, never a fingerprint: a collision would
    /// admit a reference to a row that does not exist. It holds only while the
    /// key is committed; see [`Checks::committed`].
    fk_presence: RefCell<FxHashMap<u64, FxHashSet<u128>>>,
    /// Keys one presence cache holds before it is dropped whole.
    /// `GNITZ_FK_PRESENCE_KEYS` lowers it, so tests reach it on small tables.
    fk_presence_cap: usize,
    /// Bytes of rows one write's cascade may delete, at each row's fixed width:
    /// what a client's own delete of them could carry in one frame.
    /// `GNITZ_CASCADE_BYTES` lowers it, so tests reach it on small tables.
    cascade_bytes: usize,
}

/// The largest committed write whose inserted keys an FK presence cache records.
const FK_PRESENCE_FILL_MAX_ROWS: usize = 64;

impl Checks {
    /// Empty caches, under the knobs the environment sets.
    pub(super) fn new() -> Self {
        Checks {
            unique_filters: RefCell::default(),
            unique_filter_cap: 7 << 20,
            fk_presence: RefCell::default(),
            fk_presence_cap: gnitz_foundation::env::env_num("GNITZ_FK_PRESENCE_KEYS", 1usize << 20),
            cascade_bytes: gnitz_foundation::env::env_num_in(
                "GNITZ_CASCADE_BYTES",
                gnitz_wire::MAX_FRAME_PAYLOAD,
                1..=gnitz_wire::MAX_FRAME_PAYLOAD,
            ),
        }
    }

    /// Bring `tid`'s FK presence cache up to a `batch` of it, laid out in the
    /// SAL. Called once the batch is committed and before its writer is
    /// answered: the writer's table lock is what keeps the cache true of
    /// committed rows.
    ///
    /// Per key the batch's last row decides, as it does in the store. A
    /// retraction always evicts; only a small write's inserts are recorded, so
    /// a bulk load of the table costs nothing here.
    pub(super) fn committed(&self, tid: u64, batch: MemBatch<'_>) {
        let mut caches = self.fk_presence.borrow_mut();
        let Some(keys) = caches.get_mut(&tid) else {
            return;
        };
        let records = batch.len() <= FK_PRESENCE_FILL_MAX_ROWS;
        if !records && batch.all_weights_positive() {
            return;
        }
        for row in 0..batch.len() {
            let key = gnitz_wire::widen_pk_be(batch.get_pk_bytes(row));
            match batch.get_weight(row) {
                0 => {}
                w if w > 0 => {
                    if records {
                        record_present(keys, self.fk_presence_cap, key);
                    }
                }
                _ => {
                    keys.remove(&key);
                }
            }
        }
    }

    /// Drop every cache of relation `id`.
    pub(super) fn forget(&self, id: u64) {
        self.unique_filters.borrow_mut().retain(|&(t, _), _| t != id);
        self.fk_presence.borrow_mut().remove(&id);
    }

    /// Drop the filter for one index, leaving the table's other filters.
    pub(super) fn unique_filter_remove(&self, owner_id: u64, cols: PkColList) {
        self.unique_filters.borrow_mut().remove(&(owner_id, cols));
    }
}

/// Record `key` as committed in an FK presence cache of at most `cap` keys.
/// Past the cap the set is dropped whole, and every key is proven again by its
/// next probe.
fn record_present(keys: &mut FxHashSet<u128>, cap: usize, key: u128) {
    if keys.len() >= cap && !keys.contains(&key) {
        keys.clear();
    }
    keys.insert(key);
}

/// A unique index's filter: the [`probe_key`] fingerprints of the spans it
/// holds, so a write probes only the spans it cannot prove absent.
enum UniqueFilter {
    /// The fingerprint of every span the index holds.
    Live(FxHashSet<u64>),
    /// An admitted write capped it: it proves nothing until the next write to
    /// its index rebuilds it from the spans the index holds.
    Churned,
    /// It capped while it was built: it proves nothing for the boot.
    Capped,
}

impl Default for UniqueFilter {
    /// The filter of an index that holds no span.
    fn default() -> Self {
        UniqueFilter::Live(FxHashSet::default())
    }
}

impl UniqueFilter {
    /// Records every fingerprint of `fps` up to the first that would grow the
    /// set past `cap`, which leaves the filter [`Self::Capped`]; false once the
    /// filter proves nothing.
    fn insert_all(&mut self, fps: impl IntoIterator<Item = u64>, cap: usize) -> bool {
        let UniqueFilter::Live(values) = self else {
            return false;
        };
        for fp in fps {
            // Growing past the cap would copy the whole table for a set this call drops.
            if values.len() >= cap && !values.contains(&fp) {
                *self = UniqueFilter::Capped;
                return false;
            }
            values.insert(fp);
        }
        true
    }

    /// A fingerprint probe: a collision, and every span once the filter has
    /// capped, answers true.
    fn may_contain(&self, fp: u64) -> bool {
        match self {
            UniqueFilter::Live(values) => values.contains(&fp),
            UniqueFilter::Churned | UniqueFilter::Capped => true,
        }
    }
}

/// An integrity-violation refusal: a write, or an index build, that a PK,
/// unique-index or FK constraint forbids.
fn integrity_violation(text: String) -> WireFault {
    WireFault {
        status: gnitz_wire::WireStatus::IntegrityViolation,
        text,
    }
}

#[cfg(test)]
#[path = "tests/validate.rs"]
mod tests;
