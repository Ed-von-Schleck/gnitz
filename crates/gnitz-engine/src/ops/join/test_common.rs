//! Shared test builders for the join split (delta_trace / range / rowwrite).
//! `pub(super)` so each sub-file's `mod tests` reaches them via
//! `super::super::test_common::*`.

use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::Batch;

pub(super) use crate::test_support::{
    make_batch, make_batch_i64pk as make_signed_batch, make_schema_i64pk_i64 as make_schema_signed, make_schema_u64_i64,
};

/// The compiler-built join output schema (`reg_meta`): left columns followed by
/// the right side's payload columns, keyed by the left PK — what `emit.rs`'s
/// `merge_schemas_for_join` bakes and `exec.rs` passes to the join ops. Test
/// schemas are PK-leading, so the plain concatenation matches.
pub(super) fn join_out_schema(left: &SchemaDescriptor, right: &SchemaDescriptor) -> SchemaDescriptor {
    let mut cols: Vec<SchemaColumn> = (0..left.num_columns()).map(|ci| left.columns[ci]).collect();
    for (_, _, col) in right.payload_columns() {
        cols.push(*col);
    }
    SchemaDescriptor::new(&cols, left.pk_indices())
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
