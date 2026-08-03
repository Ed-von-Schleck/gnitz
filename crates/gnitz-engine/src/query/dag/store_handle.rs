//! `StoreHandle` — the storage adapter for a registered relation, and the
//! single inbound target catalog reaches for. `Partitioned` owns its boxed
//! `PartitionedTable`; `Borrowed` is a non-owning pointer to a system table
//! owned by `CatalogEngine`. There is no custom `Drop`: the `Partitioned`
//! box is freed by the default drop glue when its registry entry is removed.

use crate::storage::{Batch, ChildAddr, PartitionProbe, PartitionedTable, ReadCursor, Routing, StorageError, Table};
use std::cell::UnsafeCell;

/// Storage handle of a registered relation. `Partitioned` owns its boxed
/// `PartitionedTable` (freed by default drop glue when its registry entry is
/// removed); `Borrowed` is a non-owning `*mut Table` to a system table owned
/// by `CatalogEngine`.
pub enum StoreHandle {
    /// Owned PartitionedTable — used by base tables and views. Wrapped in
    /// `UnsafeCell` so the interior-mutable accessors can hand out `&mut`
    /// through a shared `&self` without violating Stacked Borrows (a raw
    /// pointer derived from a shared reference may not be used to mutate).
    Partitioned(UnsafeCell<Box<PartitionedTable>>),
    /// Non-owning pointer to a Table owned elsewhere (system tables).
    Borrowed(*mut Table),
}

// SAFETY: Borrowed wraps a raw pointer that is only accessed on the
// thread that owns the DagEngine. The DagEngine itself is never shared
// across threads.
unsafe impl Send for StoreHandle {}

impl StoreHandle {
    // ------------------------------------------------------------------
    // Interior-mutable accessors
    //
    // DagEngine/CatalogEngine are single-threaded (!Sync) and the
    // HashMap<id, TableEntry> stores owning Boxes whose heap allocations
    // have stable addresses, so the mutation is race-free. But the
    // registry HashMap is read via immutable get(), which would normally
    // prevent handing out &mut to the owned Table / PartitionedTable.
    // These accessors encapsulate the raw-pointer re-borrow that
    // reconciles the lookup API with the mutation need, so call sites
    // stop reimplementing it inline.
    //
    // SAFETY contract for every method below: no aliasing &mut into the
    // same storage may be live across the call.
    // ------------------------------------------------------------------

    /// Get `&mut PartitionedTable` if this handle is Partitioned.
    // Interior mutability through UnsafeCell: the `&mut` is handed out under the
    // SAFETY contract documented above (no live aliasing &mut), not derived from
    // `&self` by reborrow — so clippy's mut_from_ref does not apply.
    #[allow(clippy::mut_from_ref)]
    pub fn as_partitioned_mut(&self) -> Option<&mut PartitionedTable> {
        match self {
            StoreHandle::Partitioned(cell) => Some(unsafe { &mut **cell.get() }),
            _ => None,
        }
    }

    /// How this store routes its rows, or `None` for a borrowed system table —
    /// one unpartitioned `Table` with no children and so no routing at all.
    pub fn routing(&self) -> Option<Routing> {
        match self {
            StoreHandle::Partitioned(cell) => Some(unsafe { (**cell.get()).routing() }),
            StoreHandle::Borrowed(_) => None,
        }
    }

    /// True iff this is an unhashed (single rank-homed child) partitioned store.
    /// Borrowed system tables are never unhashed stores.
    pub fn is_unhashed(&self) -> bool {
        self.routing().is_some_and(Routing::is_unhashed)
    }

    /// This store's children across the whole cluster at `num_workers`, empty for
    /// a borrowed system table. See [`Routing::cluster_children`].
    pub fn cluster_children(&self, num_workers: u32) -> impl Iterator<Item = ChildAddr<'static>> {
        self.routing()
            .into_iter()
            .flat_map(move |r| r.cluster_children(num_workers))
    }

    /// The owned `PartitionedTable`, or `None` for a borrowed system table —
    /// one unpartitioned `Table` with no partitions to route among. Shared, not
    /// `&mut`: keyed reads only read through it, and callers keep this borrow
    /// live while opening further cursors off the same `UnsafeCell`, which a
    /// `&mut` would forbid.
    pub fn as_partitioned(&self) -> Option<&PartitionedTable> {
        match self {
            StoreHandle::Borrowed(_) => None,
            StoreHandle::Partitioned(cell) => Some(unsafe { &**cell.get() }),
        }
    }

    /// Dispatched `has_pk` that works for every variant. Takes a **native**
    /// `u128`; routes via `opk_key` internally.
    #[cfg(test)] // sole caller is the test-only inline FK check (validate_fk_inline)
    pub fn has_pk(&self, key: u128) -> bool {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { (**ptr).has_pk(key) },
            StoreHandle::Partitioned(cell) => unsafe { (**cell.get()).has_pk(key) },
        }
    }

    /// Dispatched non-compacting `open_cursor` across all variants.
    /// Infallible, non-mutating — the recommended default. See
    /// `Table::open_cursor`.
    pub fn open_cursor(&self) -> ReadCursor {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { (**ptr).open_cursor() },
            StoreHandle::Partitioned(cell) => unsafe { (**cell.get()).open_cursor() },
        }
    }

    /// A cursor over only the partition that could hold `key` (full OPK bytes),
    /// or `None` when this process holds none. A borrowed system table is one
    /// unpartitioned `Table` with nothing to route among, so it opens whole.
    /// See [`PartitionedTable::open_cursor_for_key`].
    pub fn open_cursor_for_key(&self, key: &[u8]) -> Option<ReadCursor> {
        match self {
            StoreHandle::Borrowed(ptr) => Some(unsafe { (**ptr).open_cursor() }),
            StoreHandle::Partitioned(cell) => unsafe { (**cell.get()).open_cursor_for_key(key) },
        }
    }

    /// The [`StoreProbe`] for a multi-key read over this handle.
    pub(crate) fn open_probe(&self) -> StoreProbe {
        match self.as_partitioned() {
            Some(store) => StoreProbe::Routed(PartitionProbe::new(store)),
            None => StoreProbe::Whole(Box::new(self.open_cursor())),
        }
    }

    /// Materialize every positive-weight row. Borrowed delegates to
    /// `Table::full_scan` (preserving its `Rc` snapshot cache exactly);
    /// Partitioned materializes through the merged cursor.
    pub fn full_scan(&self) -> std::rc::Rc<Batch> {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { &mut **ptr }.full_scan(),
            StoreHandle::Partitioned(cell) => unsafe { (**cell.get()).open_cursor() }.materialize(),
        }
    }

    /// Dispatched durable ingest of a borrowed `Batch` — the single-copy path
    /// for callers that keep reading the batch (see
    /// `Table::ingest_borrowed_batch`).
    pub fn ingest_borrowed_batch(&self, batch: &Batch) -> Result<(), StorageError> {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { (**ptr).ingest_borrowed_batch(batch) },
            StoreHandle::Partitioned(cell) => unsafe { (**cell.get()).ingest_borrowed_batch(batch) },
        }
    }

    /// Dispatched flush across all variants.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { &mut **ptr }.flush(),
            StoreHandle::Partitioned(cell) => cell.get_mut().flush(),
        }
    }

    /// Current LSN of the store (Table: current_lsn field; Partitioned: max across shards).
    pub fn current_lsn(&self) -> u64 {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { &**ptr }.current_lsn(),
            StoreHandle::Partitioned(cell) => unsafe { (**cell.get()).current_lsn() },
        }
    }

    /// Recovery watermark of the store: the LSN below which committed SAL
    /// zones may be skipped on replay. Borrowed → `current_lsn`; Partitioned →
    /// the **min** across partitions (see `PartitionedTable::min_flushed_lsn`),
    /// so a partial family flush never causes the dedupe filter to over-skip a
    /// lagging partition's unflushed rows.
    pub fn recovery_lsn(&self) -> u64 {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { &**ptr }.current_lsn(),
            StoreHandle::Partitioned(cell) => unsafe { (**cell.get()).min_flushed_lsn() },
        }
    }
}

/// The read side of a multi-key lookup over a [`StoreHandle`]: a cursor per
/// partition a key routes into (opened on first touch), or one whole cursor over
/// a borrowed system table, which has no partitions to route among. Built by
/// [`StoreHandle::open_probe`].
///
/// The whole cursor is boxed — a `ReadCursor` is ~560 bytes against the probe's
/// `Vec` (clippy's `large_enum_variant`) — one allocation per request.
pub(crate) enum StoreProbe {
    Routed(PartitionProbe),
    Whole(Box<ReadCursor>),
}

impl StoreProbe {
    /// The cursor for `key`'s partition, or `None` when this process holds none
    /// — which a broadcast key list makes the common case.
    ///
    /// `store` is the handle's own `PartitionedTable`, passed per call because a
    /// probe outlives its construction and cannot hold that borrow across the
    /// `&mut CatalogEngine` uses between chunks. A `Routed` probe is only ever
    /// built over `Some`.
    fn cursor_for<'s>(&'s mut self, store: Option<&PartitionedTable>, key: &[u8]) -> Option<&'s mut ReadCursor> {
        match self {
            StoreProbe::Routed(p) => p.probe(store?, key),
            StoreProbe::Whole(c) => Some(c.as_mut()),
        }
    }

    /// The cursor positioned on `key`'s live row (full OPK bytes), or `None`
    /// when no such row is reachable — the key is absent, or unreachable here.
    /// `advance_to` is backward-capable, so any key order is correct; ascending
    /// keys additionally keep each routed cursor's probes monotone.
    ///
    /// Resolves the key to at most one row, so it fits a reader whose consumer is
    /// keyed by PK. A reader that must see every row a key names wants
    /// [`Self::copy_live_pk_group_into`] instead.
    pub(crate) fn advance_to_exact_live<'s>(
        &'s mut self,
        store: Option<&PartitionedTable>,
        key: &[u8],
    ) -> Option<&'s mut ReadCursor> {
        let cursor = self.cursor_for(store, key)?;
        cursor.advance_to_exact_live(key).then_some(cursor)
    }

    /// Group-walking sibling of [`Self::advance_to_exact_live`]: append every live
    /// row of `key`'s PK group to `out`, per the
    /// [`ReadCursor::copy_live_pk_group_into`] contract. Appends nothing when the
    /// key is absent or this process holds no partition for it.
    pub(crate) fn copy_live_pk_group_into(&mut self, store: Option<&PartitionedTable>, key: &[u8], out: &mut Batch) {
        if let Some(cursor) = self.cursor_for(store, key) {
            cursor.copy_live_pk_group_into(key, out);
        }
    }
}
