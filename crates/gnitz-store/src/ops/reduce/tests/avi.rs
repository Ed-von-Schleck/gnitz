//! AVI tests: the MAX complement over the shared value image, and a bake mixing
//! scalar and wide ordinals.

use super::super::plan::ReducePlan;
use super::*;
use crate::schema::{ColumnLocator, TypeCode};
use crate::storage::MemBatch;
use crate::test_support::scratch_table;
use gnitz_wire::{AggDescriptor, AggFunc, ScalarKind};

/// One-row batch holding `le` (the value's native little-endian bytes) in a
/// nullable payload column of type `tc`, plus the locator addressing it. The
/// value image is read through the same accessor the AVI's write side uses.
fn payload_row(tc: TypeCode, le: &[u8]) -> (Batch, ColumnLocator) {
    let schema = SchemaDescriptor::new(
        &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc as u8, 0)],
        &[0],
    );
    let mut b = Batch::with_capacity(&schema, 1);
    b.extend_pk(1u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &le[..schema.columns[1].size() as usize]);
    b.count += 1;
    (b, schema.locate(1))
}

/// The AVI's own convention on top of the shared value image: a MAX ordinal
/// stores the bitwise complement, so the index's ascending walk yields that
/// ordinal's extreme first. Checked against the MIN image of the same row.
#[test]
fn for_max_inverts_the_value_image() {
    let (b, loc) = payload_row(TypeCode::I64, &(-7i64).to_le_bytes());
    let mb: MemBatch = b.as_mem_batch();
    let kind = ScalarKind::Int(gnitz_wire::FixedInt::I64);
    assert_eq!(
        scalar_image(&loc, kind, true, &mb, 0),
        !scalar_image(&loc, kind, false, &mb, 0)
    );
}

/// Each aggregate is classified on its own, so `MIN(i64), MAX(u128)` is one bake
/// with a scalar ordinal and a wide one sharing the wide value slot — and the
/// scalar ordinal reads back out of a key eight bytes longer than usual.
#[test]
fn a_mixed_scalar_and_wide_bake_reads_both_ordinals_back() {
    // `[pk:U64, g:I32, a:I64, b:U128]`, GROUP BY g, with MIN(a) and MAX(b).
    let src = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U128, 0),
        ],
        &[0],
    );
    let descs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 3, agg_op: AggFunc::Max },
        AggDescriptor::COUNT_STAR,
    ];
    let plan = ReducePlan::from_wire(&src, &[1], &descs, false, false).unwrap();
    let bake = plan.avi.as_ref().expect("a MIN/MAX reduce bakes an AVI");

    // One group. Every `b` is below 2^64, so the wide images share their leading
    // eight bytes — the collapse the sixteen-byte slot is there to avoid.
    let rows: [(u64, i32, i64, u128); 3] = [(1, 7, 5, 9), (2, 7, -3, 1 << 40), (3, 7, 11, 12)];
    let mut delta = Batch::with_capacity(&src, rows.len());
    for (pk, g, a, b) in rows {
        delta.extend_pk(pk as u128);
        delta.extend_weight(&1i64.to_le_bytes());
        delta.extend_null_bmp(&0u64.to_le_bytes());
        delta.extend_col(src.try_payload_idx(1).unwrap(), &g.to_le_bytes());
        delta.extend_col(src.try_payload_idx(2).unwrap(), &a.to_le_bytes());
        delta.extend_col(src.try_payload_idx(3).unwrap(), &b.to_le_bytes());
        delta.count += 1;
    }

    let tmp = tempfile::tempdir().unwrap();
    let mut table = scratch_table(tmp.path().to_str().unwrap(), bake.schema);
    table.ingest_owned_batch(avi_batch(&delta, bake)).unwrap();

    let mut accs = plan.shape.acc_template.clone();
    bake.seed_extremes(&mut table.open_cursor(), &delta.as_mem_batch(), 0, &mut accs);
    assert_eq!(accs[0].value_bits() as i64, -3, "MIN(a) out of the widened slot");
    let Some(super::super::agg::AggValue::Wide(_, bytes)) = accs[1].value() else {
        panic!("MAX(b) is a wide extreme");
    };
    assert_eq!(u128::from_le_bytes(bytes.try_into().unwrap()), 1u128 << 40);
}
