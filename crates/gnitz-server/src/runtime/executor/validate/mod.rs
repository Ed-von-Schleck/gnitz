//! Write validation: the PK / FK / unique-index rules a user write is checked
//! against before any SAL byte of it is written (`preflight`), which read the
//! write folded per table and key (`bundle`) and refuse a PK or unique-index
//! collision in the words of `violation`; the DDL-time pre-flight of `CREATE
//! UNIQUE INDEX` (`unique_preflight`); and the caches that let a write probe
//! less: the unique-index filters (`unique_filter`), which take a write's
//! spans at its verdict, and the FK presence caches (`fk_presence`), which
//! take its rows at its commit.
//!
//! [`Checks`] holds those caches. Every probe goes to the workers through the
//! master's dispatcher, which each check is handed.

mod bundle;
mod fk_presence;
mod preflight;
mod unique_filter;
mod unique_preflight;
mod violation;

use std::cell::RefCell;

use rustc_hash::FxHashMap;

use crate::catalog::CatalogEngine;
use crate::runtime::master::MasterDispatcher;
use crate::runtime::reactor::Train;
use crate::runtime::sal::Read;
use gnitz_store::read::Probe;
use gnitz_wire::{PkColList, WireFault};
use gnitz_zset::algebra::extend_spans;
use gnitz_zset::repr::MemBatch;

use self::fk_presence::FkPresence;

use unique_filter::{SpanClaim, UNIQUE_FILTER_CAP, UniqueFilter};
pub(super) use unique_preflight::validate_unique_index_create;

/// The validation caches of the master's write path.
pub(super) struct Checks {
    /// The filter of each unique index that has one, by (table_id, column
    /// list). Every entry holds all of its index's spans.
    unique_filters: RefCell<FxHashMap<(u64, PkColList), UniqueFilter>>,
    /// Spans one unique filter holds before it caps; a test lowers it.
    unique_filter_cap: usize,
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
            unique_filter_cap: UNIQUE_FILTER_CAP,
            fk_presence: RefCell::default(),
            fk_presence_cap: gnitz_foundation::env::env_num("GNITZ_FK_PRESENCE_KEYS", 1usize << 20),
            cascade_bytes: gnitz_foundation::env::env_num_in(
                "GNITZ_CASCADE_BYTES",
                gnitz_wire::MAX_FRAME_PAYLOAD,
                1..=gnitz_wire::MAX_FRAME_PAYLOAD,
            ),
        }
    }

    /// Bring `tid`'s FK presence cache up to a `batch` of it. Called once the
    /// batch is committed and before its writer is answered: the writer's
    /// table lock is what keeps the cache true of committed rows.
    pub(super) fn committed(&self, tid: u64, batch: MemBatch<'_>) {
        if let Some(cache) = self.fk_presence.borrow_mut().get_mut(&tid) {
            cache.apply(&batch);
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
