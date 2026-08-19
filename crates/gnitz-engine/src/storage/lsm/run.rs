//! `Run` — one sorted row block, whatever backs it.
//!
//! Every tier of the LSM stores the same thing: a `(PK, payload)`-ordered,
//! consolidated block of rows. Only the backing differs — an in-heap [`Batch`]
//! for the memtable and RAM tiers, an mmap'd [`MappedShard`] on disk. `Run` is
//! that one concept, so the merge engine, the point-lookup probe, and the
//! located-row appender all read through a single type instead of one per tier.
//!
//! Both variants own their backing via `Rc`, so a `Run` (and anything holding
//! one) is a self-contained owning value with no borrow lifetime — what lets a
//! `ReadCursor` cross DAG/VM boundaries and a [`StoredRow`] outlive the table
//! lookup that found it.

use std::rc::Rc;

use super::batch::Batch;
use super::columnar::ColumnarSource;
use super::merge::{ColPtr, UnifiedSource};
use super::shard_reader::MappedShard;
use crate::schema::key::{pk_bytes_eq, pk_in_range};
use crate::schema::SchemaDescriptor;
use gnitz_expr::RowSource;

pub(crate) enum Run {
    /// Rc-owned in-heap batch: a memtable or RAM-tier run.
    Mem(Rc<Batch>),
    /// Rc-owned mmap'd shard. The Rc keeps the mapping alive.
    Shard(Rc<MappedShard>),
}

impl Run {
    #[inline]
    pub(crate) fn count(&self) -> usize {
        match self {
            Run::Mem(b) => b.count,
            Run::Shard(s) => s.count,
        }
    }

    /// First row whose OPK bytes are `>= key`. A raw `memcmp` binary search over
    /// the order-preserving PK region — correct at every PK width with no schema
    /// dependency. `key` must be exactly `pk_stride` OPK bytes.
    pub(crate) fn find_lower_bound_bytes(&self, key: &[u8]) -> usize {
        match self {
            Run::Mem(b) => b.find_lower_bound_bytes(key),
            Run::Shard(s) => s.find_lower_bound_bytes(key),
        }
    }

    /// Galloping forward lower bound seeded at `hint` (this run's live position).
    pub(crate) fn advance_to(&self, key: &[u8], hint: usize) -> usize {
        match self {
            Run::Mem(b) => b.advance_to(key, hint),
            Run::Shard(s) => s.advance_to(key, hint),
        }
    }

    /// Build a `UnifiedSource` view backed by either a `MemBatch`'s flat data
    /// buffer (always Raw regions) or a `MappedShard`'s mmap (Raw or Constant
    /// regions, indexed by payload position).
    ///
    /// Infallible: `MappedShard::open` validates all encoding constraints and
    /// region sizes at open time, so no arm here can fail.
    pub(crate) fn to_unified(&self, schema: &SchemaDescriptor, cols: &mut Vec<ColPtr>) -> UnifiedSource {
        match self {
            Run::Mem(b) => super::merge::mem_batch_to_unified(&b.as_mem_batch(), schema, cols),
            Run::Shard(s) => s.to_unified(schema, cols),
        }
    }

    /// Bulk-copy `[start, start + row_count)` into an owned batch — one memcpy
    /// per column. The arms differ only in the layout they may claim, which is a
    /// property of the backing: `Mem` inherits the source's tag, a shard is
    /// ghost-free by construction and certifies `Consolidated`.
    pub(crate) fn slice_to_owned_batch(&self, start: usize, row_count: usize, schema: &SchemaDescriptor) -> Batch {
        match self {
            Run::Mem(b) => b.slice_to_owned_batch(start, row_count, schema),
            Run::Shard(s) => s.slice_to_owned_batch(start, row_count, schema),
        }
    }

    /// Row indices whose PK equals `key`. Runs are PK-sorted, so exact matches
    /// form one contiguous range; iteration is lazy and stops at the first
    /// non-matching row.
    pub(crate) fn pk_match_rows<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item = usize> + 'a {
        let count = self.count();
        let start = if count > 0 && pk_in_range(self.get_pk_bytes(0), self.get_pk_bytes(count - 1), key) {
            self.find_lower_bound_bytes(key)
        } else {
            count // out of range: the scan below runs zero times
        };
        pk_match_rows_from(self, count, start, key)
    }
}

impl Clone for Run {
    fn clone(&self) -> Self {
        match self {
            Run::Mem(b) => Run::Mem(Rc::clone(b)),
            Run::Shard(s) => Run::Shard(Rc::clone(s)),
        }
    }
}

/// Row indices of `src` in `[start, count)` whose PK equals `key`, stopping at
/// the first mismatch. The one exact-match scan: callers that already hold a
/// start index (the shard index's gated binary search) pass it directly, and
/// [`Run::pk_match_rows`] derives its own.
pub(crate) fn pk_match_rows_from<'a, S: RowSource>(
    src: &'a S,
    count: usize,
    start: usize,
    key: &'a [u8],
) -> impl Iterator<Item = usize> + 'a {
    (start..count).take_while(move |&i| pk_bytes_eq(src.get_pk_bytes(i), key))
}

/// One located row, pinned to the run that holds it. Keeps its backing alive via
/// the `Run`'s `Rc`, so the row stays readable with no borrow on the table.
pub(crate) struct StoredRow {
    pub(crate) run: Run,
    pub(crate) row: usize,
}

impl StoredRow {
    #[inline]
    pub(crate) fn weight(&self) -> i64 {
        self.run.get_weight(self.row)
    }
}

/// `#[inline(always)]` on every forwarder, for the reason spelled out on
/// [`gnitz_expr::BatchView`]: at `opt-level=0` the plain hint is a no-op, and the
/// merge engine calls through here once per payload column of every row it emits.
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
    fn blob(&self) -> &[u8] {
        match self {
            Run::Mem(b) => &b.blob,
            Run::Shard(s) => s.blob_slice(),
        }
    }
    #[inline(always)]
    fn row_count(&self) -> usize {
        Run::count(self)
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
}
