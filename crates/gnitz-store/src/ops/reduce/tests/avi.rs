//! AVI tests: the MAX complement over the shared value image, and bakes mixing
//! scalar, 16-byte and byte-string ordinals.

use super::super::agg::AggValue;
use super::super::plan::ReducePlan;
use super::*;
use crate::ops::order_image::scalar_image;
use crate::schema::{ColumnLocator, TypeCode};
use crate::storage::{BatchBuilder, MemBatch};
use crate::test_support::{le_cell, scratch_table};
use gnitz_wire::{AggDescriptor, AggFunc, ScalarKind};

/// One-row batch holding `le` (the value's native little-endian bytes) in a
/// nullable payload column of type `tc`, plus the locator addressing it. The
/// value image is read through the same accessor the AVI's write side uses.
fn payload_row(tc: TypeCode, le: &[u8]) -> (Batch, ColumnLocator) {
    let schema = SchemaDescriptor::new(
        &[SchemaColumn::new(TypeCode::U64, false), SchemaColumn::new(tc, false)],
        &[0],
    );
    let mut b = BatchBuilder::new(schema);
    b.begin_row(1, 1);
    b.put_int(le_cell(&le[..schema.columns[1].size() as usize]));
    b.end_row();
    (b.finish(), schema.locate(1))
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
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::U128, false),
        ],
        &[0],
    );
    let descs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 3, agg_op: AggFunc::Max },
        AggDescriptor::COUNT_STAR,
    ];
    let plan = ReducePlan::from_wire(&src, &[1], &descs, false).unwrap();

    // One group. Every `b` is below 2^64, so the wide images share their leading
    // eight bytes — the collapse the sixteen-byte slot is there to avoid.
    let rows: [(u64, i32, i64, u128); 3] = [(1, 7, 5, 9), (2, 7, -3, 1 << 40), (3, 7, 11, 12)];
    let mut delta = BatchBuilder::new(src);
    for (pk, g, a, b) in rows {
        delta.begin_row(pk as u128, 1i64);
        delta.put_int(g as u128);
        delta.put_int(a as u128);
        delta.put_int(b);
        delta.end_row();
    }
    let delta = delta.finish();

    let accs = index_and_seed(&plan, &delta);
    assert_eq!(accs[0].value_bits() as i64, -3, "MIN(a) out of the widened slot");
    assert_eq!(wide_value(&accs[1]), (1u128 << 40).to_le_bytes(), "MAX(b)");
}

/// The accumulators of `delta` row 0's group, seeded out of an index over `delta`.
fn index_and_seed(plan: &ReducePlan, delta: &Batch) -> Vec<Accumulator> {
    let bake = plan.avi.as_ref().expect("a MIN/MAX reduce bakes an AVI");
    let tmp = tempfile::tempdir().unwrap();
    let mut table = scratch_table(tmp.path().to_str().unwrap(), bake.schema);
    table.ingest_owned_batch(avi_batch(delta, bake)).unwrap();
    let mut accs = plan.shape.acc_template.clone();
    bake.seed_extremes(&mut table.open_cursor(), &delta.as_mem_batch(), 0, &mut accs);
    accs
}

fn wide_value(acc: &Accumulator) -> Vec<u8> {
    let Some(AggValue::Wide(_, bytes)) = acc.value() else {
        panic!("a wide extreme with a value");
    };
    bytes.to_vec()
}

/// `[pk:U64, g:I32, v]` rows of one group, `v` written by `put`.
fn one_group_delta(src: &SchemaDescriptor, n: u64, mut put: impl FnMut(&mut BatchBuilder, u64)) -> Batch {
    let mut delta = BatchBuilder::new(*src);
    for pk in 0..n {
        delta.begin_row(pk as u128, 1);
        delta.put_int(7);
        put(&mut delta, pk);
        delta.end_row();
    }
    delta.finish()
}

/// A 16-byte-only index has no payload column and reads its extreme out of the key.
#[test]
fn a_sixteen_byte_only_bake_keeps_its_image_in_the_key() {
    let src = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::U128, false),
        ],
        &[0],
    );
    let descs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
        AggDescriptor::COUNT_STAR,
    ];
    let plan = ReducePlan::from_wire(&src, &[1], &descs, false).unwrap();
    let schema = &plan.avi.as_ref().unwrap().schema;
    assert!(
        (0..schema.num_columns()).all(|c| schema.payload_slot(c).is_none()),
        "no payload column"
    );

    let values = [3u128 << 100, (1 << 64) | 5, 1 << 64, u128::MAX];
    let delta = one_group_delta(&src, values.len() as u64, |b, pk| {
        b.put_int(values[pk as usize]);
    });
    let accs = index_and_seed(&plan, &delta);
    assert_eq!(wide_value(&accs[0]), (1u128 << 64).to_le_bytes(), "MIN(v)");
}

#[test]
fn a_sixteen_byte_and_string_bake_reads_both_ordinals_back() {
    let src = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::String, false),
        ],
        &[0],
    );
    let descs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Min },
        AggDescriptor { col_idx: 3, agg_op: AggFunc::Max },
        AggDescriptor::COUNT_STAR,
    ];
    let plan = ReducePlan::from_wire(&src, &[1], &descs, false).unwrap();

    const SLOT_WIDE: &str = "sixteen-byte-pre";
    assert_eq!(SLOT_WIDE.len(), 16);
    let rows = [(9u128 << 70, 'b'), (4 << 70, 'z'), (6 << 70, 'm')];
    let delta = one_group_delta(&src, rows.len() as u64, |b, pk| {
        let (a, last) = rows[pk as usize];
        b.put_int(a);
        b.put_string(&format!("{SLOT_WIDE}{last}"));
    });
    let accs = index_and_seed(&plan, &delta);
    assert_eq!(wide_value(&accs[0]), (4u128 << 70).to_le_bytes(), "MIN(a)");
    assert_eq!(wide_value(&accs[1]), format!("{SLOT_WIDE}z").as_bytes(), "MAX(s)");
}
