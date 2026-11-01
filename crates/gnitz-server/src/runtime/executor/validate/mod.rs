//! Write validation: the PK / FK / unique-index rules a user write is checked
//! against before any SAL byte of it is written (`preflight`), the DDL-time
//! pre-flight of `CREATE UNIQUE INDEX` (`unique_preflight`), and the caches
//! derived from committed rows that let both probe less: the unique-index
//! filters (`unique_filter`) and the FK presence caches (`fk_presence`).
//!
//! [`Checks`] holds those caches. Every probe goes to the workers through the
//! master's dispatcher, which each check is handed.

mod fk_presence;
mod preflight;
mod unique_filter;
mod unique_preflight;

use std::cell::RefCell;

use rustc_hash::FxHashMap;

use crate::catalog::CatalogEngine;
use crate::runtime::master::MasterDispatcher;
use crate::runtime::reactor::TrainLease;
use crate::runtime::sal::Read;
use gnitz_store::read::Probe;
use gnitz_wire::{PkColList, WireFault};
use gnitz_zset::algebra::spans;
use gnitz_zset::repr::MemBatch;

use self::fk_presence::FkPresence;

pub(super) use preflight::{FamilyRows, PushedBlock, TxnFamily};
pub(super) use unique_filter::UniqueFilter;
pub(super) use unique_preflight::validate_unique_index_create;

/// The validation caches of the master's write path.
pub(super) struct Checks {
    /// The filter of each unique index that has one, by (table_id, column
    /// list). Every entry holds all of its index's spans.
    unique_filters: RefCell<FxHashMap<(u64, PkColList), UniqueFilter>>,
    /// The presence cache of each table an FK probe found a key of, by table id.
    fk_presence: RefCell<FxHashMap<u64, FkPresence>>,
    /// Keys one presence cache holds before it is dropped whole.
    /// `GNITZ_FK_PRESENCE_KEYS` lowers it, so tests reach it on small tables.
    fk_presence_cap: usize,
    /// Bytes of rows one write's cascade may delete, at each row's fixed width:
    /// what a client's own delete of them could carry in one frame.
    /// `GNITZ_CASCADE_BYTES` lowers it, so tests reach it on small tables.
    cascade_bytes: usize,
}

impl Checks {
    /// Empty caches, under the knobs the environment sets.
    pub(super) fn new() -> Self {
        Checks {
            unique_filters: RefCell::default(),
            fk_presence: RefCell::default(),
            fk_presence_cap: gnitz_foundation::env::env_num("GNITZ_FK_PRESENCE_KEYS", 1usize << 20),
            cascade_bytes: gnitz_foundation::env::env_num("GNITZ_CASCADE_BYTES", gnitz_wire::MAX_FRAME_PAYLOAD)
                .min(gnitz_wire::MAX_FRAME_PAYLOAD),
        }
    }

    /// Bring every cache derived from `tid`'s committed rows up to a `batch` of
    /// it. Called once the batch is committed and before its writer is
    /// answered: the writer's table lock is what keeps a cache true of
    /// committed rows.
    pub(super) fn committed(&self, cat: &CatalogEngine, tid: u64, batch: MemBatch<'_>) {
        if let Some(cache) = self.fk_presence.borrow_mut().get_mut(&tid) {
            cache.apply(&batch);
        }
        let Some(relation) = cat.registry.relation(tid) else {
            return;
        };
        let mut filters = self.unique_filters.borrow_mut();
        for ic in relation.unique_indexes_to_check() {
            if let Some(filter) = filters.get_mut(&(tid, ic.cols())) {
                // A capped filter takes nothing, so its spans are not extracted.
                if !filter.is_capped() {
                    let spec = ic.key_spec();
                    filter.insert_all(spans(&batch, spec).chunks_exact(spec.key_size()));
                }
            }
        }
    }

    /// Drop every cache of relation `id`.
    pub(super) fn forget(&self, id: u64) {
        self.unique_filters.borrow_mut().retain(|&(t, _), _| t != id);
        self.fk_presence.borrow_mut().remove(&id);
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
