//! The test helpers only this crate uses.
//!
//! Unlike [`super::shared`], this file is compiled once, inside `gnitz-engine`,
//! so it names crate-internals through `crate::` and needs nothing published on
//! its behalf. Every helper here that reaches one — `Batch::extend_pk_opk`,
//! `schema::key::encode_leading_opk`, `SchemaDescriptor::pk_columns` — is why
//! that matters: in the shared file each of those had to become public API.

use std::cmp::Ordering;

use proptest::prelude::*;

use crate::schema::key::{compare_pk_bytes, encode_leading_opk};
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, Layout};
use gnitz_wire::type_code;

use super::shared::{arb_type_code, col_def, pk_i64_schema, u64_pk_schema};
use crate::catalog::ColumnDef;

/// The canonical wide-PK test schema: a 3×U64 compound primary key
/// (`pk_stride = 24`, wide) with a single I64 payload column.
pub fn wide_pk_3xu64_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1, 2],
    )
}

/// An all-PK schema: one column per type code in `types`, every column a PK
/// column (`pk_indices = 0..n`) and no payload — the generic PK-shape builder
/// for the OPK encode/compare/route tests.
pub fn pk_only_schema(types: &[u8]) -> SchemaDescriptor {
    let cols: Vec<SchemaColumn> = types.iter().map(|&tc| SchemaColumn::new(tc, 0)).collect();
    let pk: Vec<u32> = (0..types.len() as u32).collect();
    SchemaDescriptor::new(&cols, &pk)
}

/// OPK-encode native PK column values (one per PK column, in `pk_columns()`
/// order) into the canonical order-preserving key — the exact bytes the ingest
/// path produces, and byte-identical to [`Batch::extend_pk_opk`]. Signed columns
/// are passed as `v as u128` (the low `size()` little-endian bytes are the
/// two's-complement image the encoder sign-flips).
pub fn opk_pk(schema: &SchemaDescriptor, vals: &[u128]) -> Vec<u8> {
    // The production leading-span encoder over every PK column: source and
    // target type are equal here, so its promote step is the identity arm and
    // the span is the whole PK. Encoding through it rather than re-packing the
    // native image by hand keeps the oracle from being a second spelling of the
    // OPK encoding.
    let cols = schema.pk_columns().map(|(_, col)| (col.type_code, *col));
    encode_leading_opk(cols, vals).pk_bytes().to_vec()
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
    let mut b = Batch::with_capacity(*schema, rows.len().max(1));
    for &(c0, c1, c2, w, val) in rows {
        b.extend_pk_opk(schema, &[c0 as u128, c1 as u128, c2 as u128]);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
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

/// Build a one-row `Batch` from OPK-encoded `pk` bytes (see [`opk_pk`]), DBSP
/// weight `w`, and a single non-null I64 payload `val` at payload slot 0 — the
/// row shape wide-PK storage/dag tests ingest one at a time.
pub fn wide_row(schema: &SchemaDescriptor, pk: &[u8], w: i64, val: i64) -> Batch {
    let mut b = Batch::with_capacity(*schema, 1);
    b.extend_pk_bytes(pk);
    b.extend_weight(&w.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &val.to_le_bytes());
    b.count += 1;
    b
}

/// Read row `i`'s PK out of a raw `stride`-wide OPK region as its native
/// unsigned value — the read-back twin of `Batch::extend_pk` for tests that
/// inspect a scatter/merge destination buffer directly.
pub fn read_pk_opk(region: &[u8], i: usize, stride: usize) -> u128 {
    gnitz_wire::widen_pk_be(&region[i * stride..(i + 1) * stride], stride)
}

/// [`make_batch_raw`] over [`make_schema_u128_i64`]-shaped schemas — native
/// u128 PKs, rows left `Raw` in the order given.
pub fn make_batch_u128_raw(schema: &SchemaDescriptor, rows: &[(u128, i64, i64)]) -> Batch {
    let mut b = Batch::with_capacity(*schema, rows.len().max(1));
    for &(pk, w, val) in rows {
        b.extend_pk(pk);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b
}

/// U128 pk + a single I64 payload column — the 16-byte-PK sibling of
/// [`make_schema_u64_i64`].
pub fn make_schema_u128_i64() -> SchemaDescriptor {
    pk_i64_schema(type_code::U128)
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
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Build a sorted, consolidated batch with an I64 PK and a single I64 payload
/// from native `(pk, weight, payload)` tuples. The PK is OPK-encoded via
/// [`Batch::extend_pk_opk`] (sign-flipped big-endian), so the bytes match an
/// ingested row; callers must pass OPK-sorted rows.
pub fn make_batch_i64pk(schema: &SchemaDescriptor, rows: &[(i64, i64, i64)]) -> Batch {
    let mut b = Batch::with_capacity(*schema, rows.len().max(1));
    for &(pk, w, val) in rows {
        b.extend_pk_opk(schema, &[(pk as u64) as u128]);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated, schema);
    b
}

/// U64 pk + a single BLOB payload column.
pub fn make_schema_pk_u64_payload_blob() -> SchemaDescriptor {
    u64_pk_schema(type_code::BLOB)
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

/// A plain non-nullable UUID column.
pub fn uuid_def(name: &str) -> ColumnDef {
    col_def(name, type_code::UUID)
}

/// A nullable column of the given type.
pub fn nullable_def(name: &str, type_code: u8) -> ColumnDef {
    ColumnDef {
        is_nullable: true,
        ..col_def(name, type_code)
    }
}

/// A column of `type_code` carrying an FK onto `(parent_tid, parent_col)`.
pub fn fk_def(name: &str, type_code: u8, parent_tid: i64, parent_col: u32) -> ColumnDef {
    ColumnDef {
        fk_table_id: parent_tid,
        fk_col_idx: parent_col,
        ..col_def(name, type_code)
    }
}
