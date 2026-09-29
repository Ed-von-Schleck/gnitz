use super::*;
use crate::schema::key::NarrowPkOpk;
use crate::schema::ColumnTable;
use crate::schema::{SchemaColumn, TypeCode};
use crate::storage::{BatchBuilder, Layout};
use gnitz_wire::{read_i64_le, AggDescriptor, AggFunc};

// Source: pk(U64), grp(I64), val(I64, nullable). val is payload slot 1.
fn src_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    )
}

/// (pk, weight, grp, Option<val>) → a consolidated source batch.
fn build(rows: &[(u64, i64, i64, Option<i64>)]) -> Batch {
    let s = src_schema();
    let mut b = BatchBuilder::new(s);
    for &(pk, w, grp, val) in rows {
        b.begin_row(pk as u128, w);
        b.put_int(grp as u128);
        b.put_opt_int(val.map(|v| v as u128));
        b.end_row();
    }
    let mut b = b.finish();
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

fn direct(group_cols: Vec<u32>, aggs: Vec<AggDescriptor>) -> AggReadSpec {
    AggReadSpec { group_cols, aggs }
}

fn agg(agg_op: AggFunc, col_idx: u32) -> AggDescriptor {
    AggDescriptor { agg_op, col_idx }
}

/// Collect the partial output by group value (grp is the natural output PK),
/// returning per group `(weight, [Option<i64> per agg col])`.
#[allow(clippy::type_complexity)]
fn by_group(out: &Batch, n_aggs: usize) -> std::collections::HashMap<i64, (i64, Vec<Option<i64>>)> {
    let mb = out.as_mem_batch();
    let mut map = std::collections::HashMap::new();
    for row in 0..out.count {
        // `read_i64_le` takes a byte offset; every column here is an 8-byte I64.
        let grp = crate::test_support::opk_pk_i64(mb.get_pk_bytes(row));
        let nw = mb.get_null_word(row);
        let vals: Vec<Option<i64>> = (0..n_aggs)
            .map(|k| {
                // Agg col k is payload slot k: the key region spells the group col.
                if gnitz_wire::null_word_get(nw, k) {
                    None
                } else {
                    Some(read_i64_le(out.col_data(k), row * 8))
                }
            })
            .collect();
        map.insert(grp, (mb.get_weight(row), vals));
    }
    map
}

#[test]
fn fold_grouped_multi_agg_with_nulls() {
    // group 10: 100, 200, NULL  → COUNT*=3, COUNT(val)=2, SUM=300, MIN=100, MAX=200
    // group 20: 50 at weight 2   → COUNT*=2, COUNT(val)=2, SUM=100, MIN=50,  MAX=50
    // group 30: NULL             → COUNT*=1, COUNT(val)=0, SUM=0, MIN/MAX = NULL
    let batch = build(&[
        (1, 1, 10, Some(100)),
        (2, 1, 10, Some(200)),
        (3, 1, 10, None),
        (4, 2, 20, Some(50)),
        (5, 1, 30, None),
    ]);
    let spec = direct(
        vec![1],
        vec![
            agg(AggFunc::Count, 0),
            agg(AggFunc::CountNonNull, 2),
            agg(AggFunc::Sum, 2),
            agg(AggFunc::Min, 2),
            agg(AggFunc::Max, 2),
        ],
    );
    let src = src_schema();
    let mut fold = AdhocFold::new(&src, &spec, 1000).unwrap();
    fold.fold_ranges(&batch, &[(0, batch.count)]).unwrap();
    let g = by_group(&fold.finish(), 5);
    assert_eq!(g.len(), 3);
    assert_eq!(g[&10], (1, vec![Some(3), Some(2), Some(300), Some(100), Some(200)]));
    assert_eq!(g[&20], (1, vec![Some(2), Some(2), Some(100), Some(50), Some(50)]));
    assert_eq!(g[&30], (1, vec![Some(1), Some(0), Some(0), None, None]));
}

/// The partial layout is a view's reduce output over the same group set: a
/// single non-null column and the whole PK are the natural key, a nullable
/// column folds into `_group_pk` and rides as payload.
#[test]
fn fold_partial_layout_is_the_views() {
    let count = vec![agg(AggFunc::Count, 0)];
    let layout = |group: Vec<u32>| {
        let s = *AdhocFold::new(&src_schema(), &direct(group, count.clone()), 1000)
            .unwrap()
            .output_schema();
        let cols: Vec<(TypeCode, bool)> = s.columns[..s.num_columns()]
            .iter()
            .map(|c| (c.type_code, c.nullable))
            .collect();
        (cols, s.pk_cols().to_vec())
    };
    let i64c = (TypeCode::I64, false);
    assert_eq!(layout(vec![1]), (vec![i64c, i64c], vec![0]), "single non-null column");
    assert_eq!(
        layout(vec![0]),
        (vec![(TypeCode::U64, false), i64c], vec![0]),
        "the whole PK"
    );
    assert_eq!(
        layout(vec![2]),
        (vec![(TypeCode::U128, false), (TypeCode::I64, true), i64c], vec![0]),
        "a nullable column folds"
    );
}

/// A whole-PK fold over a compound PK stamps each group's own PK.
#[test]
fn fold_over_the_whole_compound_pk_keys_by_it() {
    let src = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0, 1],
    );
    let spec = direct(vec![0, 1], vec![agg(AggFunc::Count, 0), agg(AggFunc::Sum, 2)]);
    let mut fold = AdhocFold::new(&src, &spec, 1000).unwrap();
    let pk = |a: u64, b: i32| {
        let mut k = [0u8; 12];
        gnitz_wire::encode_pk_column(&a.to_le_bytes(), TypeCode::U64, &mut k[..8]);
        gnitz_wire::encode_pk_column(&b.to_le_bytes(), TypeCode::I32, &mut k[8..]);
        k
    };
    let mut b = BatchBuilder::new(src);
    for (k, v) in [(pk(1, -1), 10i64), (pk(1, 2), 20)] {
        b.begin_row_bytes(&k, 1);
        b.put_int(v as u128);
        b.end_row();
    }
    let mut b = b.finish();
    b.set_layout_unchecked(Layout::Consolidated);
    fold.fold_ranges(&b, &[(0, b.count)]).unwrap();
    let out = fold.finish();
    assert_eq!(out.schema().pk_stride(), 12);
    let got: Vec<(Vec<u8>, i64, i64)> = (0..out.count)
        .map(|r| {
            (
                out.get_pk_bytes(r).to_vec(),
                read_i64_le(out.col_data(0), r * 8),
                read_i64_le(out.col_data(1), r * 8),
            )
        })
        .collect();
    assert_eq!(got, vec![(pk(1, -1).to_vec(), 1, 10), (pk(1, 2).to_vec(), 1, 20)]);
}

#[test]
fn fold_accumulates_across_chunks() {
    let spec = direct(vec![1], vec![agg(AggFunc::Count, 0), agg(AggFunc::Sum, 2)]);
    let src = src_schema();
    let mut fold = AdhocFold::new(&src, &spec, 1000).unwrap();
    let (c1, c2) = (
        build(&[(1, 1, 7, Some(10)), (2, 1, 7, Some(20))]),
        build(&[(3, 1, 7, Some(5)), (4, 1, 8, Some(99))]),
    );
    fold.fold_ranges(&c1, &[(0, c1.count)]).unwrap();
    fold.fold_ranges(&c2, &[(0, c2.count)]).unwrap();
    let g = by_group(&fold.finish(), 2);
    assert_eq!(g[&7], (1, vec![Some(3), Some(35)]));
    assert_eq!(g[&8], (1, vec![Some(1), Some(99)]));
}

/// The range bounds are the filter's survivor ranges: rows outside every
/// folded range contribute nothing, and a group discovered only in a skipped
/// range never appears.
#[test]
fn fold_ranges_folds_only_the_given_ranges() {
    let spec = direct(vec![1], vec![agg(AggFunc::Count, 0), agg(AggFunc::Sum, 2)]);
    let src = src_schema();
    let batch = build(&[
        (1, 1, 7, Some(10)),
        (2, 1, 9, Some(999)), // skipped
        (3, 1, 7, Some(20)),
        (4, 1, 7, Some(30)),
    ]);
    let mut fold = AdhocFold::new(&src, &spec, 1000).unwrap();
    // Two survivor ranges: [0,1) and [2,4) — row 1 (group 9) is filtered out.
    fold.fold_ranges(&batch, &[(0, 1), (2, 4)]).unwrap();
    let g = by_group(&fold.finish(), 2);
    assert_eq!(g.len(), 1, "group 9 lived only in the skipped range");
    assert_eq!(g[&7], (1, vec![Some(3), Some(60)]));
}

#[test]
fn fold_global_single_group() {
    let spec = direct(vec![], vec![agg(AggFunc::Count, 0), agg(AggFunc::Max, 2)]);
    let src = src_schema();
    let mut fold = AdhocFold::new(&src, &spec, 1000).unwrap();
    let b = build(&[(1, 1, 0, Some(3)), (2, 1, 0, Some(9)), (3, 1, 0, Some(1))]);
    fold.fold_ranges(&b, &[(0, b.count)]).unwrap();
    let out = fold.finish();
    assert_eq!(out.count, 1);
    assert_eq!(read_i64_le(out.col_data(0), 0), 3); // COUNT*
    assert_eq!(read_i64_le(out.col_data(1), 0), 9); // MAX
    assert_eq!(out.as_mem_batch().get_weight(0), 1);
}

/// A global fold owns its one group from the start, so a worker that folded no
/// rows still emits the ground row: COUNT a concrete, null-clear 0, MAX NULL.
#[test]
fn fold_global_over_no_rows_emits_the_ground_row() {
    let spec = direct(vec![], vec![agg(AggFunc::Count, 0), agg(AggFunc::Max, 2)]);
    let src = src_schema();
    let fold = AdhocFold::new(&src, &spec, 1000).unwrap();
    let out = fold.finish();
    assert_eq!(out.count, 1);
    let mb = out.as_mem_batch();
    assert_eq!(mb.get_weight(0), 1);
    assert_eq!(
        mb.get_pk_bytes(0),
        NarrowPkOpk::new(gnitz_wire::global_group_key(), 16).bytes()
    );
    assert_eq!(read_i64_le(out.col_data(0), 0), 0); // COUNT*
    assert!(
        !gnitz_wire::null_word_get(mb.get_null_word(0), 0),
        "COUNT(*) is null-clear"
    );
    assert!(
        gnitz_wire::null_word_get(mb.get_null_word(0), 1),
        "MAX over nothing is NULL"
    );
}

#[test]
fn fold_grouped_over_no_rows_emits_nothing() {
    let spec = direct(vec![1], vec![agg(AggFunc::Count, 0)]);
    let src = src_schema();
    let fold = AdhocFold::new(&src, &spec, 1000).unwrap();
    assert_eq!(fold.finish().count, 0);
}

#[test]
fn fold_group_cap_aborts() {
    let spec = direct(vec![1], vec![agg(AggFunc::Count, 0)]);
    let src = src_schema();
    // Cap of 2 distinct groups; a third distinct group trips it.
    let mut fold = AdhocFold::new(&src, &spec, 2).unwrap();
    let b = build(&[(1, 1, 1, Some(0)), (2, 1, 2, Some(0)), (3, 1, 3, Some(0))]);
    let err = fold.fold_ranges(&b, &[(0, b.count)]).unwrap_err();
    assert!(err.to_string().contains("CREATE VIEW"), "{err}");
}

/// pk(U64), s(STRING), w(U128) — the wide-column source both tests below read.
fn wide_src_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::String, false),
            SchemaColumn::new(TypeCode::U128, false),
        ],
        &[0],
    )
}

/// The spec is a trust boundary: SUM over a STRING or a U128 is refused.
#[test]
fn fold_rejects_a_sum_with_no_encoding() {
    let src = wide_src_schema();
    for col in [1u32, 2] {
        let spec = direct(vec![], vec![agg(AggFunc::Sum, col)]);
        let Err(err) = AdhocFold::new(&src, &spec, 1000) else {
            panic!("SUM over column {col} must be rejected");
        };
        assert!(err.to_string().contains("Sum is not defined over"), "{err}");
    }
}

/// MIN selects a row of any type and COUNT reads no value at all, so both take
/// the same wide columns SUM is refused over — MIN's partial column being the
/// source type, which the reply schema has to declare.
#[test]
fn fold_accepts_a_row_selecting_aggregate_over_a_wide_column() {
    let src = wide_src_schema();
    for col in [1u32, 2] {
        let spec = direct(vec![], vec![agg(AggFunc::Min, col)]);
        assert!(AdhocFold::new(&src, &spec, 1000).is_ok(), "MIN over column {col}");
        let spec = direct(vec![], vec![agg(AggFunc::Count, col)]);
        assert!(AdhocFold::new(&src, &spec, 1000).is_ok(), "COUNT over column {col}");
    }
}

#[test]
fn fold_rejects_out_of_range_column() {
    // group column 9: no such column
    let spec = direct(vec![9], vec![agg(AggFunc::Count, 0)]);
    let src = src_schema();
    assert!(AdhocFold::new(&src, &spec, 1000).is_err());
}
