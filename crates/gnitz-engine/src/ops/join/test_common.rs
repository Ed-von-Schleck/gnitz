//! Shared test builders for the join split (delta_trace / range / rowwrite).
//! `pub(super)` so each sub-file's `mod tests` reaches them via
//! `super::super::test_common::*`.

use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, Layout};

pub(super) fn make_schema_u64_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

pub(super) fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));

    for &(pk, w, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated, schema);
    b
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

pub(super) fn make_schema_signed() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

pub(super) fn make_signed_batch(schema: &SchemaDescriptor, rows: &[(i64, i64, i64)]) -> Batch {
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
    b.certify_layout(Layout::Consolidated, schema);
    b
}

// -----------------------------------------------------------------------
// Wide-PK helpers
// -----------------------------------------------------------------------

pub(super) fn wide_pk_bytes(schema: &SchemaDescriptor, c0: u64, c1: u64, c2: u64) -> Vec<u8> {
    let mut tmp = Batch::with_schema(*schema, 1);
    tmp.extend_pk_opk(schema, &[c0 as u128, c1 as u128, c2 as u128]);
    tmp.get_pk_bytes(0).to_vec()
}

// -----------------------------------------------------------------------
// Wide-PK inner-join multiset-delta tests
// -----------------------------------------------------------------------
