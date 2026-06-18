//! `StoreHandle` — the storage adapter for a registered relation, and the
//! single inbound target catalog reaches for. The Owned variant drops the
//! underlying storage on Drop; Borrowed/Partitioned hold non-owning handles.

use std::cell::UnsafeCell;
use crate::storage::{Batch, CursorHandle, FlushOutcome, FlushWork, Table, PartitionedTable, StorageError};

/// Storage handle of a registered relation. The owned variant drops the
/// underlying storage on Drop; the Borrowed variant holds a non-owning
/// pointer for system tables owned by CatalogEngine directly.
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
    /// Get a raw pointer to the Table (Borrowed), or null for Partitioned.
    /// SAFETY: the owner keeps the Table alive across any synchronous call
    /// using the pointer. The caller must ensure no aliasing &mut references
    /// exist.
    pub fn table_ptr(&self) -> *mut Table {
        match self {
            StoreHandle::Borrowed(ptr) => *ptr,
            StoreHandle::Partitioned(_) => std::ptr::null_mut(),
        }
    }

    /// Get a raw pointer to the PartitionedTable, or null if not Partitioned.
    pub fn ptable_ptr(&self) -> *mut PartitionedTable {
        match self {
            StoreHandle::Partitioned(cell) => unsafe { &mut **cell.get() as *mut PartitionedTable },
            _ => std::ptr::null_mut(),
        }
    }

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

    /// Dispatched `has_pk` that works for every variant. Takes a **native**
    /// `u128`; routes via `opk_key` internally. Never feed it `get_pk`
    /// (OPK-widened) — use [`has_pk_bytes`] for verbatim OPK bytes.
    #[cfg(test)] // sole caller is the test-only inline FK check (validate_fk_inline)
    pub fn has_pk(&self, key: u128) -> bool {
        let tptr = self.table_ptr();
        unsafe {
            if !tptr.is_null() { (*tptr).has_pk(key) } else { (*self.ptable_ptr()).has_pk(key) }
        }
    }

    /// Dispatched verbatim-OPK-bytes `has_pk` across all variants. Correct for
    /// every PK width; takes the bytes `Batch::get_pk_bytes` produces, with no
    /// native round-trip (and thus no double-encode for signed/compound PKs).
    pub fn has_pk_bytes(&self, key: &[u8]) -> bool {
        let tptr = self.table_ptr();
        unsafe {
            if !tptr.is_null() {
                (*tptr).has_pk_bytes(key)
            } else {
                (*self.ptable_ptr()).has_pk_bytes(key)
            }
        }
    }

    /// Dispatched non-compacting `open_cursor` across all variants.
    /// Infallible, non-mutating — the recommended default. See
    /// `Table::open_cursor`.
    pub fn open_cursor(&self) -> CursorHandle {
        let tptr = self.table_ptr();
        unsafe {
            if !tptr.is_null() {
                (*tptr).open_cursor()
            } else {
                (*self.ptable_ptr()).open_cursor()
            }
        }
    }

    /// Dispatched compacting cursor across all variants. Maintenance-only —
    /// see `Table::create_cursor_compacting` for the validator hazard this
    /// name surfaces. The lint guards external callers, not this dispatch.
    #[allow(clippy::disallowed_methods)]
    pub fn create_cursor_compacting(&self) -> Result<CursorHandle, StorageError> {
        let tptr = self.table_ptr();
        unsafe {
            if !tptr.is_null() {
                (*tptr).create_cursor_compacting()
            } else {
                (*self.ptable_ptr()).create_cursor_compacting()
            }
        }
    }

    /// Dispatched durable ingest of an already-owned `Batch`. Skips the
    /// regions memcpy round-trip; moves the batch directly into the
    /// storage layer (zero-copy for single-partition stores; one
    /// MemBatch-borrowed scatter for multi-partition).
    pub fn ingest_owned_batch(&self, batch: Batch) -> Result<(), StorageError> {
        let tptr = self.table_ptr();
        unsafe {
            if !tptr.is_null() {
                (*tptr).ingest_owned_batch(batch)
            } else {
                (*self.ptable_ptr()).ingest_owned_batch(batch)
            }
        }
    }

    /// Dispatched flush across all variants.
    pub fn flush(&mut self) -> Result<bool, StorageError> {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { &mut **ptr }.flush(),
            StoreHandle::Partitioned(cell) => cell.get_mut().flush(),
        }
    }

    /// Dispatched Phase 1 across all variants. Returns one
    /// (partition_idx, FlushWork) per partition that produced deferred
    /// work; for Borrowed `partition_idx` is always 0.
    pub fn flush_prepare(&mut self) -> Result<Vec<(usize, FlushWork)>, StorageError> {
        let table: &mut Table = match self {
            StoreHandle::Borrowed(ptr) => unsafe { &mut **ptr },
            StoreHandle::Partitioned(cell) => return cell.get_mut().flush_prepare(),
        };
        match table.flush_prepare()? {
            FlushOutcome::Empty | FlushOutcome::DoneInline => Ok(Vec::new()),
            FlushOutcome::Pending(w) => Ok(vec![(0, *w)]),
        }
    }

    /// Dispatched Phase 3 across all variants. Returns one dirfd per
    /// committed partition.
    pub fn flush_commit_batch(
        &mut self,
        works: Vec<(usize, FlushWork)>,
    ) -> Result<Vec<libc::c_int>, StorageError> {
        let t: &mut Table = match self {
            StoreHandle::Borrowed(ptr) => unsafe { &mut **ptr },
            StoreHandle::Partitioned(cell) => return cell.get_mut().flush_commit_batch(works),
        };
        let mut out = Vec::with_capacity(works.len());
        for (_, w) in works {
            if let Some(fd) = t.flush_commit(w)? { out.push(fd); }
        }
        Ok(out)
    }

    /// Current LSN of the store (Table: current_lsn field; Partitioned: max across shards).
    pub fn current_lsn(&self) -> u64 {
        match self {
            StoreHandle::Borrowed(ptr) => unsafe { &**ptr }.current_lsn,
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
            StoreHandle::Borrowed(ptr) => unsafe { &**ptr }.current_lsn,
            StoreHandle::Partitioned(cell) => unsafe { (**cell.get()).min_flushed_lsn() },
        }
    }
}
