//! `Run` — one sorted row block, whatever backs it.
//!
//! Every tier of the LSM stores a sorted, consolidated block of rows:
//! `(PK, payload)`-ordered, or, for a capacity-bounded view's skeleton shard, one
//! (PK, summed weight) row per key with no payload. Only the backing differs — an
//! in-heap [`Batch`] for the memtable and RAM tiers, an mmap'd [`MappedShard`] on
//! disk.
//!
//! Both variants own their backing via `Rc`, so a `ReadCursor` or [`StoredRow`]
//! holding a `Run` carries no borrow of the table that produced it.

use std::ops::Range;
use std::rc::Rc;

use crate::runs::MappedShard;
use gnitz_wire::{Heap, RowSource};
use gnitz_zset::repr::Batch;
use gnitz_zset::repr::ColPtr;
use gnitz_zset::repr::{ColumnarSource, DecodedColumns};

#[derive(Clone)]
pub enum Run {
    /// Rc-owned in-heap batch: a memtable or RAM-tier run.
    Mem(Rc<Batch>),
    /// Rc-owned mmap'd shard. The Rc keeps the mapping alive.
    Shard(Rc<MappedShard>),
}

impl Run {
    /// Row `row`'s signed Z-set weight.
    #[inline(always)]
    pub fn get_weight(&self, row: usize) -> i64 {
        ColumnarSource::get_weight(self, row)
    }

    /// The first row whose OPK bytes are `>= key`, galloping from `hint` (this
    /// run's live position) where there is one; `key` is exactly `pk_stride`
    /// bytes.
    pub(crate) fn seek(&self, key: &[u8], hint: Option<usize>) -> usize {
        match self {
            Run::Mem(b) => b.seek(key, hint),
            Run::Shard(s) => s.seek(key, hint),
        }
    }
}

/// One located row, pinned to the run that holds it. Keeps its backing alive via
/// the `Run`'s `Rc`, so the row stays readable with no borrow on the table.
pub struct StoredRow {
    pub(crate) run: Run,
    pub(crate) row: usize,
}

impl StoredRow {
    /// Row `row` of `run`.
    #[inline]
    pub fn new(run: Run, row: usize) -> Self {
        StoredRow { run, row }
    }

    /// The row this points at, as the `(source, index)` pair every row reader
    /// and every comparator takes. An `impl Trait` return, so which run kind
    /// backs it stays this crate's business.
    #[inline]
    pub fn source(&self) -> (&impl RowSource, usize) {
        (&self.run, self.row)
    }
}

/// `#[inline(always)]` on every forwarder, as [`RowSource`] asks of its
/// implementors.
impl RowSource for Run {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        match self {
            Run::Mem(b) => b.get_pk_bytes(row),
            Run::Shard(s) => s.get_pk_bytes(row),
        }
    }
    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        match self {
            Run::Mem(b) => b.get_null_word(row),
            Run::Shard(s) => s.get_null_word(row),
        }
    }
    #[inline(always)]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        match self {
            Run::Mem(b) => b.get_col_ptr(row, payload_col, col_size),
            Run::Shard(s) => s.get_col_ptr(row, payload_col, col_size),
        }
    }
    #[inline(always)]
    fn heap(&self) -> Heap<'_> {
        match self {
            Run::Mem(b) => RowSource::heap(&**b),
            Run::Shard(s) => s.heap(),
        }
    }
    #[inline(always)]
    fn row_count(&self) -> usize {
        match self {
            Run::Mem(b) => b.len(),
            Run::Shard(s) => s.row_count(),
        }
    }
}

impl ColumnarSource for Run {
    #[inline(always)]
    fn get_weight(&self, row: usize) -> i64 {
        match self {
            Run::Mem(b) => b.get_weight(row),
            Run::Shard(s) => s.get_weight(row),
        }
    }
    #[inline(always)]
    fn is_skeleton(&self) -> bool {
        match self {
            Run::Mem(_) => false,
            Run::Shard(s) => s.is_skeleton(),
        }
    }

    fn in_place(&self, r: usize) -> Option<ColPtr> {
        match self {
            Run::Mem(b) => b.as_mem_batch().in_place(r),
            Run::Shard(s) => s.in_place(r),
        }
    }

    fn region(&self, r: usize, window: &Range<usize>, decoded: &mut DecodedColumns) -> ColPtr {
        match self {
            Run::Mem(b) => ColumnarSource::region(&b.as_mem_batch(), r, window, decoded),
            Run::Shard(s) => s.region(r, window, decoded),
        }
    }
}
