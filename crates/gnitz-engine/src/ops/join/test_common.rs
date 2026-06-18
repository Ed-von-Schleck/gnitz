//! Shared test builders for the join split (delta_trace / range / delta_delta /
//! rowwrite). `pub(super)` so each sub-file's `mod tests` reaches them via
//! `super::super::test_common::*`.

use crate::foundation::codec::read_i64_le;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, ConsolidatedBatch};

pub(super) fn make_schema_u64_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

pub(super) fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> ConsolidatedBatch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));

    for &(pk, w, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
}

pub(super) fn get_payload_i64(b: &Batch, row: usize) -> i64 {
    read_i64_le(b.col_data(0), row * 8)
}

pub(super) fn make_schema_compound() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    )
}

/// Pack a compound `(U64, U64)` PK into its 16-byte region: col0 then col1,
/// each little-endian — the layout storage stores and orders by.
pub(super) fn compound_pk_bytes(c0: u64, c1: u64) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0..8].copy_from_slice(&c0.to_le_bytes());
    b[8..16].copy_from_slice(&c1.to_le_bytes());
    b
}

pub(super) fn make_compound_batch(schema: &SchemaDescriptor, rows: &[(u64, u64, i64, i64)]) -> ConsolidatedBatch {
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(c0, c1, w, val) in rows {
        b.extend_pk_bytes(&compound_pk_bytes(c0, c1));
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
}

pub(super) fn make_schema_signed() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

pub(super) fn make_signed_batch(schema: &SchemaDescriptor, rows: &[(i64, i64, i64)]) -> ConsolidatedBatch {
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(pk, w, val) in rows {
        // Signed PK at rest is OPK (big-endian, sign-bit flipped) so the
        // join's compare_pk_bytes merge orders it correctly; the raw
        // `extend_pk` would store non-order-preserving native bytes.
        b.extend_pk_opk(schema, &[(pk as u64) as u128]);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
}

/// Decode a single signed-I64 PK at `row` from its OPK bytes to native.
pub(super) fn signed_pk_i64(b: &Batch, row: usize) -> i64 {
    let mut le = [0u8; 8];
    gnitz_wire::decode_pk_column(b.get_pk_bytes(row), type_code::I64, &mut le);
    i64::from_le_bytes(le)
}

// -----------------------------------------------------------------------
// Anti-join DD tests
// -----------------------------------------------------------------------

pub(super) fn wide_pk_bytes(schema: &SchemaDescriptor, c0: u64, c1: u64, c2: u64) -> Vec<u8> {
    let mut tmp = Batch::with_schema(*schema, 1);
    tmp.extend_pk_opk(schema, &[c0 as u128, c1 as u128, c2 as u128]);
    tmp.get_pk_bytes(0).to_vec()
}

// -----------------------------------------------------------------------
// Wide-PK inner-join multiset-delta tests
// -----------------------------------------------------------------------
