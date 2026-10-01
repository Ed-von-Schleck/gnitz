//! `Run` — one sorted row block, whatever backs it.
//!
//! Every tier of the LSM stores the same thing: a `(PK, payload)`-ordered,
//! consolidated block of rows. Only the backing differs — an in-heap [`Batch`]
//! for the memtable and RAM tiers, an mmap'd [`MappedShard`] on disk.
//!
//! Both variants own their backing via `Rc`, so a `ReadCursor` or [`StoredRow`]
//! holding a `Run` carries no borrow of the table that produced it.

use std::ops::Range;
use std::rc::Rc;

use crate::repr::batch::Batch;
use crate::repr::merge::ColumnarSource;
use crate::repr::merge::{ColPtr, UnifiedSource};
use crate::repr::scatter::DecodedColumns;
use crate::repr::shard_reader::MappedShard;
use crate::schema::payload_order::{with_payload_cmp, PayloadOrder};
use crate::schema::SchemaDescriptor;
use gnitz_expr::RowSource;

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

    /// First row whose OPK bytes are `>= key`; `key` is exactly `pk_stride`
    /// bytes. Correct at every PK width with no schema dependency.
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

    /// Bulk-copy `[start, start + row_count)` into an owned batch under `schema`.
    pub(crate) fn slice_to_owned_batch(&self, start: usize, row_count: usize, schema: &SchemaDescriptor) -> Batch {
        match self {
            Run::Mem(b) => {
                let mut slice = Batch::from_ranges(b, &[(start, start + row_count)], 0);
                slice.set_schema(schema);
                slice
            }
            Run::Shard(s) => {
                debug_assert!(*s.schema() == *schema, "a shard slices under the schema it is bound to");
                s.slice_to_owned_batch(start, row_count)
            }
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

    #[inline]
    pub(crate) fn weight(&self) -> i64 {
        self.run.get_weight(self.row)
    }

    /// The row this points at, as the `(source, index)` pair every row reader
    /// and every comparator takes. An `impl Trait` return, so which run kind
    /// backs it stays this crate's business.
    #[inline]
    pub fn source(&self) -> (&impl gnitz_expr::RowSource, usize) {
        (&self.run, self.row)
    }
}

/// Index of the first candidate in `pool` whose payload group nets strictly
/// positive.
pub fn first_live_payload_group(schema: &SchemaDescriptor, pool: &[StoredRow]) -> Option<usize> {
    with_payload_cmp!(schema, first_live_payload_group_with, schema, pool)
}

fn first_live_payload_group_with<P: PayloadOrder>(
    schema: &SchemaDescriptor,
    pool: &[StoredRow],
    payload: P,
) -> Option<usize> {
    (0..pool.len()).find(|&i| {
        let (run, row) = (&pool[i].run, pool[i].row);
        let others: i64 = pool
            .iter()
            .enumerate()
            .filter(|&(j, c)| j != i && payload.compare(schema, run, row, &c.run, c.row).is_eq())
            .map(|(_, c)| c.weight())
            .sum();
        pool[i].weight() + others > 0
    })
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
    fn blob(&self) -> &[u8] {
        match self {
            Run::Mem(b) => b.blob(),
            Run::Shard(s) => s.blob(),
        }
    }
    #[inline(always)]
    fn row_count(&self) -> usize {
        match self {
            Run::Mem(b) => b.count,
            Run::Shard(s) => s.row_count(),
        }
    }
}

impl ColumnarSource for Run {
    fn to_unified(
        &self,
        schema: &SchemaDescriptor,
        cols: &mut Vec<ColPtr>,
        window: Range<usize>,
        decoded: &mut DecodedColumns,
    ) -> UnifiedSource<'_> {
        match self {
            Run::Mem(b) => crate::repr::merge::mem_batch_to_unified(&b.as_mem_batch(), schema, cols),
            Run::Shard(s) => s.to_unified(schema, cols, window, decoded),
        }
    }

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
