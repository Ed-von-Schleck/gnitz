//! The test helpers only this crate uses.
//!
//! Unlike [`super::shared`], this file is compiled once, inside `gnitz-zset`,
//! so it names crate-internals through `crate::` and needs nothing published on
//! its behalf.

use std::cmp::Ordering;
use std::path::Path;
use std::rc::Rc;

use crate::repr::{
    from_runs, from_runs_at, from_runs_in_band, Batch, MappedShard, MemBatch, ReadCursor, Run, ShardWriteOpts,
};
use crate::schema::key::{key_range_between_cuts, KeyCut};
use crate::schema::payload_order::compare_full_rows;
use crate::schema::{ColumnLocator, SchemaColumn, SchemaDescriptor};
use gnitz_wire::{PkBuf, TypeCode};

use proptest::strategy::Strategy;

use super::shared::{arb_type_code, pk_payload_schema, read_german_string, u64_pk_schema, zset_of, zset_sum};

/// A cursor over `batches` and `shards`, one run each.
pub(crate) fn create_read_cursor(
    batches: &[Rc<Batch>],
    shards: &[Rc<MappedShard>],
    schema: SchemaDescriptor,
) -> ReadCursor {
    from_runs(
        batches
            .iter()
            .map(|b| Run::Mem(Rc::clone(b)))
            .chain(shards.iter().cloned().map(Run::Shard)),
        schema,
        batches.len() + shards.len(),
    )
}

/// `rows` dealt round-robin into `runs` runs, each consolidated alone, under one
/// cursor: an element can cancel across runs.
pub(crate) fn dealt_cursor(rows: &Batch, runs: usize) -> ReadCursor {
    let dealt: Vec<Rc<Batch>> = (0..runs)
        .map(|k| {
            let rows_k: Vec<(usize, usize)> = (k..rows.count).step_by(runs).map(|r| (r, r + 1)).collect();
            Rc::new(Batch::from_ranges(rows, &rows_k, 0).into_consolidated())
        })
        .collect();
    create_read_cursor(&dealt, &[], *rows.schema())
}

/// The reindex of `source` onto its columns `key`, each at its own type,
/// keeping `keep`: the map a join's trace integrates behind.
pub(crate) fn rekey_plan(source: &SchemaDescriptor, key: &[u32], keep: &[u32]) -> crate::algebra::MapPlan {
    let rekey = gnitz_wire::MapKind::Reindex {
        keep: keep.to_vec(),
        key: key.iter().map(|&c| (c, source.columns[c as usize].type_code)).collect(),
        role: gnitz_wire::ReindexRole::Auxiliary,
        nulls: gnitz_wire::NullKeys::Drop,
    };
    crate::algebra::MapPlan::from_wire(source, &rekey).expect("fixture reindex is well-formed")
}

/// A [`ReadCursor`] over one in-memory batch: the integral an operator reads
/// back as `z⁻¹(I(X))`, the shape every delta-against-trace unit test wants.
pub fn trace_cursor(batch: Batch) -> ReadCursor {
    let schema = *batch.schema();
    create_read_cursor(&[Rc::new(batch)], &[], schema)
}

/// An operator's trace in a test: one consolidated run per ingest, read through
/// one cursor — what a store's memtable holds between folds.
pub(crate) struct TestTrace {
    schema: SchemaDescriptor,
    runs: Vec<Rc<Batch>>,
}

impl TestTrace {
    pub(crate) fn new(schema: SchemaDescriptor) -> Self {
        TestTrace { schema, runs: Vec::new() }
    }

    /// Add `batch` as one more run.
    pub(crate) fn ingest(&mut self, mut batch: Batch) {
        batch.set_schema(&self.schema);
        let run = batch.into_consolidated();
        if !run.is_empty() {
            self.runs.push(Rc::new(run));
        }
    }

    /// A cursor over every run.
    pub(crate) fn cursor(&self) -> ReadCursor {
        create_read_cursor(&self.runs, &[], self.schema)
    }

    /// A cursor over the rows whose PK begins with a key in `[first, last]` and
    /// over no other: an operator that reads outside the keys it opened its
    /// history at finds nothing there.
    pub(crate) fn cursor_within(&self, first: &[u8], last: &[u8]) -> ReadCursor {
        let stride = self.schema.pk_stride();
        let (start, end) = key_range_between_cuts(KeyCut::min_of(first), KeyCut::above(last), stride)
            .expect("`first <= last`, so the band holds a key");
        let runs = self.runs.iter().cloned().map(Run::Mem);
        let end = end.as_ref().map(PkBuf::pk_bytes);
        from_runs_in_band(runs, self.schema, self.runs.len(), start.pk_bytes(), end)
    }

    /// A cursor positioned on the first live row `>= first`.
    pub(crate) fn cursor_from(&self, first: &[u8]) -> ReadCursor {
        let runs = self.runs.iter().cloned().map(Run::Mem);
        from_runs_at(runs, self.schema, self.runs.len(), first)
    }
}

/// This process's resident set once the allocator has handed its free memory
/// back, so a later delta counts what is still referenced.
pub fn settled_rss() -> u64 {
    // SAFETY: `malloc_trim` only releases memory the allocator holds free.
    unsafe { libc::malloc_trim(0) };
    gnitz_foundation::perf::rss_bytes()
}

/// One column of one row as its native little-endian cell, a string as its
/// content; `None` for NULL.
pub(crate) fn cell(mb: &MemBatch, loc: ColumnLocator, row: usize) -> Option<Vec<u8>> {
    let mut scratch = [0u8; 16];
    (!loc.is_null(mb, row)).then(|| match loc.type_code().is_german_string() {
        true => gnitz_wire::german_string_content(loc.bytes(mb, row), mb.blob).to_vec(),
        false => loc.native_le_bytes(mb, row, &mut scratch).to_vec(),
    })
}

/// A native little-endian cell of up to 16 bytes as the zero-extended value
/// `BatchBuilder::put_int` writes back at the cell's own width.
pub(crate) fn le_cell(cell: &[u8]) -> u128 {
    let mut v = [0u8; 16];
    v[..cell.len()].copy_from_slice(cell);
    u128::from_le_bytes(v)
}

/// A random schema: `1..=MAX_PK_COLUMNS` PK columns of PK-eligible types and
/// `0..=6` payload columns drawn from `payload`, nullable or not, all
/// interleaved at random positions, with the PK list in random order.
pub(crate) fn random_schema(
    rng: &mut crate::test_support::Rng,
    payload: &[TypeCode],
    nullable: bool,
) -> SchemaDescriptor {
    let pk_types: Vec<TypeCode> = TypeCode::ALL.iter().copied().filter(|t| t.is_pk_eligible()).collect();
    let n_pk = 1 + rng.gen_range(crate::schema::MAX_PK_COLUMNS as u64) as usize;
    let n_payload = rng.gen_range(7) as usize;
    let mut cols: Vec<SchemaColumn> = (0..n_pk)
        .map(|_| SchemaColumn::new(rng.pick(&pk_types), false))
        .collect();
    for _ in 0..n_payload {
        let null = nullable && rng.gen_range(2) == 1;
        cols.push(SchemaColumn::new(rng.pick(payload), null));
    }
    // `pos[i]` is column i's position; the PK list is the PK columns' positions.
    let mut pos: Vec<u32> = (0..cols.len() as u32).collect();
    rng.shuffle(&mut pos);
    let mut placed = cols.clone();
    for (c, &p) in cols.iter().zip(&pos) {
        placed[p as usize] = *c;
    }
    SchemaDescriptor::new(&placed, &pos[..n_pk])
}

// ---------------------------------------------------------------------------
// The Z-set fold oracle
// ---------------------------------------------------------------------------

/// `got` is the Z-set sum of `inputs`: one row per element, strictly ascending
/// in (PK, payload) order.
pub(crate) fn assert_folds(inputs: &[Batch], got: &Batch, what: &str) {
    let schema = got.schema();
    let want = zset_sum(inputs, schema);
    assert_eq!(zset_of(got, schema), want, "{what}: the Z-set sum");
    assert_eq!(got.count, want.len(), "{what}: one row per element");
    let mb = got.as_mem_batch();
    for r in 1..got.count {
        assert_eq!(
            compare_full_rows(schema, &mb, r - 1, &mb, r),
            Ordering::Less,
            "{what}: rows {} and {r} out of order",
            r - 1
        );
    }
}

/// Payload column 0's string on every row, in order.
pub(crate) fn read_strings(batch: &Batch) -> Vec<Vec<u8>> {
    (0..batch.len()).map(|row| read_german_string(batch, 0, row)).collect()
}

/// U64 pk + a single BLOB payload column.
pub(crate) fn make_schema_pk_u64_payload_blob() -> SchemaDescriptor {
    u64_pk_schema(SchemaColumn::new(TypeCode::Blob, false))
}

/// The canonical wide-PK test schema: a 3×U64 compound primary key
/// (`pk_stride = 24`, wide) with a single I64 payload column.
pub(crate) fn wide_pk_3xu64_schema() -> SchemaDescriptor {
    pk_payload_schema(&[TypeCode::U64; 3])
}

/// I64 pk + I64 payload schema — the signed-PK exercise of the order-preserving
/// key (negatives sort before positives only because the encoder sign-flips).
pub(crate) fn make_schema_i64pk_i64() -> SchemaDescriptor {
    pk_payload_schema(&[TypeCode::I64])
}

/// Run `f` once as warmup, then `iters` times, returning the total elapsed.
pub(crate) fn bench_time(iters: usize, mut f: impl FnMut()) -> std::time::Duration {
    f();
    let start = std::time::Instant::now();
    for _ in 0..iters {
        f();
    }
    start.elapsed()
}

/// [`bench_time`] for a body that consumes per-iteration state the clock must
/// not see — a batch to fold, a fresh table to write into. `setup` runs outside
/// the timed region of every iteration, the warmup included.
pub(crate) fn bench_time_each<S>(
    iters: usize,
    mut setup: impl FnMut() -> S,
    mut body: impl FnMut(S),
) -> std::time::Duration {
    let mut total = std::time::Duration::ZERO;
    for i in 0..=iters {
        let state = setup();
        let start = std::time::Instant::now();
        body(state);
        if i > 0 {
            total += start.elapsed();
        }
    }
    total
}

/// `batch` written to `path` under `opts` and mapped back, as a store maps the
/// shards it writes.
pub(crate) fn map_shard(path: &Path, batch: &Batch, opts: ShardWriteOpts) -> Rc<MappedShard> {
    let path = path.to_str().unwrap();
    batch.write_as_shard(path, opts).unwrap();
    Rc::new(MappedShard::open(path, batch.schema()).unwrap())
}

/// The PK-eligible type codes, derived from [`gnitz_wire::is_pk_eligible`] so the
/// set can never drift from the predicate the schema layer enforces (fixed-width
/// integer scalars: U8..U64, I8..I64, U128, I128, UUID — STRING / BLOB / float
/// are rejected by `SchemaDescriptor::new`).
pub(crate) fn arb_pk_type() -> impl Strategy<Value = TypeCode> {
    arb_type_code().prop_filter("type code must be PK-eligible", |&tc| tc.is_pk_eligible())
}
