//! The test helpers only this crate uses.
//!
//! Unlike [`super::shared`], this file is compiled once, inside `gnitz-store`,
//! so it names crate-internals through `crate::` and needs nothing published on
//! its behalf — `encode_leading_opk` and `SchemaDescriptor::pk_columns` are both
//! `pub(crate)` and reachable here, where the shared file would have had to
//! publish them.

use std::cmp::Ordering;

use proptest::prelude::*;

use crate::schema::key::compare_pk_bytes;
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, BatchBuilder, Layout, ReadCursor};
use gnitz_wire::type_code;

use super::shared::{arb_type_code, pk_payload_schema, u64_pk_schema};

/// The canonical wide-PK test schema: a 3×U64 compound primary key
/// (`pk_stride = 24`, wide) with a single I64 payload column.
pub fn wide_pk_3xu64_schema() -> SchemaDescriptor {
    pk_payload_schema(&[type_code::U64; 3])
}

/// U64 pk + two I64 payload columns — the flush/merge fixtures' shape, where a
/// second payload column is what makes a partially-written row detectable.
pub fn pk_u64_two_i64_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Build a consolidated wide-PK batch from native `(c0, c1, c2, weight, payload)`
/// tuples. The PK is OPK-encoded via [`Batch::extend_pk_opk`] (big-endian per
/// column — the at-rest region layout), so the bytes are byte-identical to an
/// ingested row and to what `worker_for_pk_bytes` routes.
///
/// `rows` must be OPK-sorted (non-decreasing PK). The explicit assert below runs
/// in every build (unlike `certify_layout`'s debug-only verify) and gives a
/// targeted message: a dropped big-endian flip in the encoder (which sorts e.g.
/// 256 before 1) trips it here rather than silently scattering scrambled bytes
/// past a test's self-referential checks. Equal PKs with differing payloads are
/// allowed (multiset deltas), so only a strictly *decreasing* PK is rejected;
/// `certify_layout(Consolidated)` then debug-verifies the full (PK, payload) order.
pub fn make_wide_batch(schema: &SchemaDescriptor, rows: &[(u64, u64, u64, i64, i64)]) -> Batch {
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    for &(c0, c1, c2, w, val) in rows {
        b.extend_pk_opk(schema, &[c0 as u128, c1 as u128, c2 as u128]);
        b.extend_weight(&w.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.commit_row(0);
    }
    for r in 1..b.count {
        assert_ne!(
            compare_pk_bytes(b.get_pk_bytes(r - 1), b.get_pk_bytes(r)),
            Ordering::Greater,
            "make_wide_batch row {r}: non-OPK-sorted PK (encoder regression?)",
        );
    }
    b.certify_layout(Layout::Consolidated, schema);
    b
}

/// Build a batch from raw OPK key bytes, one row per `(pk, weight, payload)`,
/// with a single non-null I64 payload at slot 0.
///
/// The PK is stored verbatim. Every consumer below the encode boundary treats it
/// as an opaque ordered byte string, so this one builder serves every stride —
/// which is what keeps a new PK width from growing another near-identical
/// builder. [`opk_pk`] produces the bytes from native column values.
pub fn make_batch_opk(schema: &SchemaDescriptor, rows: &[(&[u8], i64, i64)]) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for &(pk, w, val) in rows {
        assert_eq!(pk.len(), schema.pk_stride(), "PK bytes must be exactly one stride wide");
        b.begin_row_bytes(pk, w);
        b.put_int(val as u128);
        b.end_row();
    }
    b.finish()
}

/// A [`ReadCursor`] over one in-memory batch: the integral an operator reads
/// back as `z⁻¹(I(X))`, the shape every delta-against-trace unit test wants.
pub fn trace_cursor(batch: Batch, schema: SchemaDescriptor) -> ReadCursor {
    ReadCursor::over_batches(&[std::rc::Rc::new(batch)], schema)
}

/// [`make_batch_opk`] for the single row wide-PK storage/dag tests ingest one
/// at a time.
pub fn wide_row(schema: &SchemaDescriptor, pk: &[u8], w: i64, val: i64) -> Batch {
    make_batch_opk(schema, &[(pk, w, val)])
}

/// Read row `i`'s PK out of a raw `stride`-wide OPK region as its native
/// unsigned value — the read-back twin of `Batch::extend_pk` for tests that
/// inspect a scatter/merge destination buffer directly.
pub fn read_pk_opk(region: &[u8], i: usize, stride: usize) -> u128 {
    gnitz_wire::widen_pk_be(&region[i * stride..(i + 1) * stride])
}

/// Decode a single signed I64 PK column from its OPK (big-endian, sign-flipped)
/// bytes back to the native value — the inverse of `extend_pk_opk` for an I64 PK.
pub fn opk_pk_i64(opk_bytes: &[u8]) -> i64 {
    let mut le = [0u8; 8];
    gnitz_wire::decode_pk_column(&opk_bytes[..8], type_code::I64, &mut le);
    i64::from_le_bytes(le)
}

/// Read a German-string payload cell (16-byte struct at payload `col`, `row`)
/// back to its content bytes — the test-side readback inverse of
/// `gnitz_wire::encode_german_string`, via the production decoder. `unwrap`s:
/// a test that writes a cell through the encoder and cannot read it back has
/// found a bug, not a corrupt input.
pub fn read_german_string(batch: &Batch, col: usize, row: usize) -> Vec<u8> {
    let off = row * 16;
    let gs: &[u8; 16] = batch.col_data(col)[off..off + 16].try_into().unwrap();
    gnitz_wire::try_decode_german_string(gs, &batch.blob).unwrap()
}

/// I64 pk + I64 payload schema — the signed-PK exercise of the order-preserving
/// key (negatives sort before positives only because the encoder sign-flips).
pub fn make_schema_i64pk_i64() -> SchemaDescriptor {
    pk_payload_schema(&[type_code::I64])
}

/// Build a sorted, consolidated batch with an I64 PK and a single I64 payload
/// from native `(pk, weight, payload)` tuples. The PK is OPK-encoded via
/// [`Batch::extend_pk_opk`] (sign-flipped big-endian), so the bytes match an
/// ingested row; callers must pass OPK-sorted rows.
pub fn make_batch_i64pk(schema: &SchemaDescriptor, rows: &[(i64, i64, i64)]) -> Batch {
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    for &(pk, w, val) in rows {
        b.extend_pk_opk(schema, &[(pk as u64) as u128]);
        b.extend_weight(&w.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.commit_row(0);
    }
    b.certify_layout(Layout::Consolidated, schema);
    b
}

/// U64 pk + a single STRING payload column.
pub fn make_schema_pk_u64_payload_string() -> SchemaDescriptor {
    u64_pk_schema(SchemaColumn::new(type_code::STRING, 0))
}

/// U64 pk + a single BLOB payload column.
pub fn make_schema_pk_u64_payload_blob() -> SchemaDescriptor {
    u64_pk_schema(SchemaColumn::new(type_code::BLOB, 0))
}

/// Build a sorted, consolidated batch for a `(U64 pk, STRING|BLOB payload)` schema
/// from `(pk, weight, bytes)` rows, already in (PK, payload) order. Values over 12
/// bytes land in the blob heap, so this is the builder for blob-propagation tests.
pub fn make_batch_bytes(schema: &SchemaDescriptor, rows: &[(u64, i64, &[u8])]) -> Batch {
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    for &(pk, w, val) in rows {
        let cell = gnitz_wire::encode_german_string(val, &mut b.blob);
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_col(0, &cell);
        b.commit_row(0);
    }
    b.certify_layout(Layout::Consolidated, schema);
    b
}

// ---------------------------------------------------------------------------
// Shared proptest strategies
// ---------------------------------------------------------------------------

/// The PK-eligible type codes, derived from [`gnitz_wire::is_pk_eligible`] so the
/// set can never drift from the predicate the schema layer enforces (fixed-width
/// integer scalars: U8..U64, I8..I64, U128, I128, UUID — STRING / BLOB / float
/// are rejected by `SchemaDescriptor::new`).
pub fn arb_pk_type() -> impl Strategy<Value = u8> {
    arb_type_code().prop_filter("type code must be PK-eligible", |&tc| gnitz_wire::is_pk_eligible(tc))
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

/// Every `.rs` file under `dir`, recursively. `enter` decides whether a
/// sub-directory is descended into; `dir` itself is always read, and an
/// unreadable one is a panic — a source-text guard that silently walked nothing
/// would pass for the wrong reason.
pub fn rs_files_under(
    dir: &std::path::Path,
    enter: impl Fn(&std::path::Path) -> bool + Copy,
) -> Vec<std::path::PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        panic!("a source walk must be able to read {}", dir.display());
    };
    let mut out = Vec::new();
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            if enter(&p) {
                out.extend(rs_files_under(&p, enter));
            }
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
    out
}
