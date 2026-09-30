//! The test helpers only this crate uses.
//!
//! Unlike [`super::shared`], this file is compiled once, inside `gnitz-store`,
//! so it names crate-internals through `crate::` and needs nothing published on
//! its behalf — `SchemaDescriptor::pk_columns` is `pub(crate)` and reachable
//! here, where the shared file would have had to publish it.

use std::cmp::Ordering;

use proptest::prelude::*;

use crate::schema::key::compare_pk_bytes;
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, BatchBuilder, Layout, ReadCursor, RecoverySource, StoreBudgets, Table};
use gnitz_wire::TypeCode;

use super::shared::{arb_type_code, pk_payload_schema};

/// The canonical wide-PK test schema: a 3×U64 compound primary key
/// (`pk_stride = 24`, wide) with a single I64 payload column.
pub fn wide_pk_3xu64_schema() -> SchemaDescriptor {
    pk_payload_schema(&[TypeCode::U64; 3])
}

/// U64 pk + two I64 payload columns — the flush/merge fixtures' shape, where a
/// second payload column is what makes a partially-written row detectable.
pub fn pk_u64_two_i64_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    )
}

/// A consolidated batch over [`wide_pk_3xu64_schema`] from native
/// `(c0, c1, c2, weight, payload)` rows, which must be OPK-sorted.
pub fn make_wide_batch(schema: &SchemaDescriptor, rows: &[(u64, u64, u64, i64, i64)]) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(c0, c1, c2, w, val) in rows {
        b.begin_row_opk(&[c0 as u128, c1 as u128, c2 as u128], w);
        b.put_int(val as u128);
        b.end_row();
    }
    let mut b = b.finish();
    for r in 1..b.count {
        assert_ne!(
            compare_pk_bytes(b.get_pk_bytes(r - 1), b.get_pk_bytes(r)),
            Ordering::Greater,
            "make_wide_batch row {r}: non-OPK-sorted PK (encoder regression?)",
        );
    }
    b.certify_layout(Layout::Consolidated);
    b
}

/// Build a batch from raw OPK key bytes, one row per `(pk, weight, payload)`,
/// with a single non-null I64 payload at slot 0.
///
/// The PK is stored verbatim. Every consumer below the encode boundary treats it
/// as an opaque ordered byte string, so this one builder serves every stride —
/// which is what keeps a new PK width from growing another near-identical
/// builder. [`opk_pk`] produces the bytes from native column values.
pub fn make_batch_opk(schema: &SchemaDescriptor, rows: &[(impl AsRef<[u8]>, i64, i64)]) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for (pk, w, val) in rows {
        let pk = pk.as_ref();
        assert_eq!(pk.len(), schema.pk_stride(), "PK bytes must be exactly one stride wide");
        b.begin_row_bytes(pk, *w);
        b.put_int(*val as u128);
        b.end_row();
    }
    b.finish()
}

/// A [`ReadCursor`] over one in-memory batch: the integral an operator reads
/// back as `z⁻¹(I(X))`, the shape every delta-against-trace unit test wants.
pub fn trace_cursor(batch: Batch, schema: SchemaDescriptor) -> ReadCursor {
    crate::storage::create_read_cursor(&[std::rc::Rc::new(batch)], &[], schema)
}

/// Decode a single signed I64 PK column from its OPK (big-endian, sign-flipped)
/// bytes back to the native value — the inverse of `BatchBuilder::begin_row` for an I64 PK.
pub fn opk_pk_i64(opk_bytes: &[u8]) -> i64 {
    gnitz_wire::decode_opk_i64(&opk_bytes[..8], gnitz_wire::FixedInt::I64)
}

/// Payload column 0 of row `row`, an 8-byte integer.
pub fn payload0_i64<S: gnitz_expr::RowSource>(src: &S, row: usize) -> i64 {
    gnitz_expr::payload_u64(src, row, 0) as i64
}

/// [`payload0_i64`] of a located store row.
pub fn stored_payload0_i64(fr: &crate::storage::StoredRow) -> i64 {
    payload0_i64(&fr.run, fr.row)
}

/// I64 pk + I64 payload schema — the signed-PK exercise of the order-preserving
/// key (negatives sort before positives only because the encoder sign-flips).
pub fn make_schema_i64pk_i64() -> SchemaDescriptor {
    pk_payload_schema(&[TypeCode::I64])
}

// ---------------------------------------------------------------------------
// Shared proptest strategies
// ---------------------------------------------------------------------------

/// The PK-eligible type codes, derived from [`gnitz_wire::is_pk_eligible`] so the
/// set can never drift from the predicate the schema layer enforces (fixed-width
/// integer scalars: U8..U64, I8..I64, U128, I128, UUID — STRING / BLOB / float
/// are rejected by `SchemaDescriptor::new`).
pub fn arb_pk_type() -> impl Strategy<Value = TypeCode> {
    arb_type_code().prop_filter("type code must be PK-eligible", |&tc| tc.is_pk_eligible())
}

// ---------------------------------------------------------------------------
// Micro-benchmark timing
// ---------------------------------------------------------------------------

/// Run `f` once as warmup, then `iters` times, returning the total elapsed.
pub fn bench_time(iters: usize, mut f: impl FnMut()) -> std::time::Duration {
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
pub fn bench_time_each<S>(iters: usize, mut setup: impl FnMut() -> S, mut body: impl FnMut(S)) -> std::time::Duration {
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

/// A rederived table under `dir` at the default budgets — nothing a test puts
/// here spills, since that needs the whole 32 MiB RAM tier. For a test that just
/// needs somewhere to put rows.
pub(crate) fn scratch_table(dir: &str, schema: SchemaDescriptor) -> Table {
    Table::new(
        dir,
        schema,
        RecoverySource::Rederive { resume_at: None },
        StoreBudgets::default(),
    )
    .unwrap()
}

/// Flip the low bit of `path`'s last byte with a `pwrite`, which a live mapping
/// of the file sees; a truncating rewrite would fault that mapping instead.
pub(crate) fn flip_last_byte_in_place(path: &std::path::Path) {
    use std::os::unix::fs::FileExt;
    let file = std::fs::OpenOptions::new().read(true).write(true).open(path).unwrap();
    let last = file.metadata().unwrap().len() - 1;
    let mut byte = [0u8; 1];
    file.read_exact_at(&mut byte, last).unwrap();
    file.write_all_at(&[byte[0] ^ 0x01], last).unwrap();
}

/// This process's resident set once the allocator has handed its free memory
/// back, so a later delta counts what is still referenced.
pub fn settled_rss() -> u64 {
    // SAFETY: `malloc_trim` only releases memory the allocator holds free.
    unsafe { libc::malloc_trim(0) };
    gnitz_foundation::perf::rss_bytes()
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
pub(crate) fn random_schema(rng: &mut crate::test_rng::Rng, payload: &[TypeCode], nullable: bool) -> SchemaDescriptor {
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
