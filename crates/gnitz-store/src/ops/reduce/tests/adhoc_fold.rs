use super::*;
use crate::schema::{type_code, SchemaColumn};
use crate::storage::Layout;
use gnitz_wire::{read_i64_le, AggFunc, AggReadItem, AGG_COUNT, AGG_COUNT_NON_NULL, AGG_MAX, AGG_MIN, AGG_SUM};

// Source: pk(U64), grp(I64), val(I64, nullable). val is payload slot 1.
fn src_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

// Reply (SyntheticFold): _agg_pk(U128), grp(I64), then one I64 per agg spec.
fn reply_schema(n_aggs: usize) -> SchemaDescriptor {
    let mut cols = vec![
        SchemaColumn::new(type_code::U128, 0),
        SchemaColumn::new(type_code::I64, 0),
    ];
    for _ in 0..n_aggs {
        cols.push(SchemaColumn::new(type_code::I64, 1));
    }
    SchemaDescriptor::new(&cols, &[0])
}

/// Global (group-less) reply: `_agg_pk(U128)` then one I64 per agg spec.
fn reply_global(n_aggs: usize) -> SchemaDescriptor {
    let mut cols = vec![SchemaColumn::new(type_code::U128, 0)];
    for _ in 0..n_aggs {
        cols.push(SchemaColumn::new(type_code::I64, 1));
    }
    SchemaDescriptor::new(&cols, &[0])
}

/// (pk, weight, grp, Option<val>) → a consolidated source batch.
fn build(rows: &[(u64, i64, i64, Option<i64>)]) -> Batch {
    let s = src_schema();
    let mut b = Batch::with_capacity(s, rows.len().max(1));
    for &(pk, w, grp, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        let null_word = if val.is_none() { 1u64 << 1 } else { 0 };
        b.extend_null_bmp(&null_word.to_le_bytes());
        b.extend_col(0, &grp.to_le_bytes()); // grp (payload 0)
        b.extend_col(1, &val.unwrap_or(0).to_le_bytes()); // val (payload 1)
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

fn agg(op: u64, col: u16) -> AggReadItem {
    AggReadItem {
        op: AggFunc::from_wire(op).unwrap(),
        src_col: col,
    }
}

/// Collect the partial output by group value (grp is payload col 0), returning
/// per group `(weight, [Option<i64> per agg col])`.
#[allow(clippy::type_complexity)]
fn by_group(out: &Batch, n_aggs: usize) -> std::collections::HashMap<i64, (i64, Vec<Option<i64>>)> {
    let mb = out.as_mem_batch();
    let mut map = std::collections::HashMap::new();
    for row in 0..out.count {
        // `read_i64_le` takes a byte offset; every column here is an 8-byte I64.
        let grp = read_i64_le(out.col_data(0), row * 8);
        let nw = mb.get_null_word(row);
        let vals: Vec<Option<i64>> = (0..n_aggs)
            .map(|k| {
                // Agg col k is payload slot 1 + k (slot 0 is the group col).
                if gnitz_wire::null_word_get(nw, 1 + k) {
                    None
                } else {
                    Some(read_i64_le(out.col_data(1 + k), row * 8))
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
    // group 30: NULL             → COUNT*=1, COUNT(val)=0, SUM/MIN/MAX = NULL
    let batch = build(&[
        (1, 1, 10, Some(100)),
        (2, 1, 10, Some(200)),
        (3, 1, 10, None),
        (4, 2, 20, Some(50)),
        (5, 1, 30, None),
    ]);
    let spec = AggReadSpec {
        group_cols: vec![1],
        aggs: vec![
            agg(AGG_COUNT, 0),
            agg(AGG_COUNT_NON_NULL, 2),
            agg(AGG_SUM, 2),
            agg(AGG_MIN, 2),
            agg(AGG_MAX, 2),
        ],
    };
    let (src, reply) = (src_schema(), reply_schema(5));
    let mut fold = AdhocFold::new(&src, &reply, &spec, 1000).unwrap();
    fold.fold_ranges(&batch, &[(0, batch.count)]).unwrap();
    let g = by_group(&fold.finish(), 5);
    assert_eq!(g.len(), 3);
    assert_eq!(g[&10], (1, vec![Some(3), Some(2), Some(300), Some(100), Some(200)]));
    assert_eq!(g[&20], (1, vec![Some(2), Some(2), Some(100), Some(50), Some(50)]));
    assert_eq!(g[&30], (1, vec![Some(1), Some(0), None, None, None]));
}

#[test]
fn fold_accumulates_across_chunks() {
    let spec = AggReadSpec {
        group_cols: vec![1],
        aggs: vec![agg(AGG_COUNT, 0), agg(AGG_SUM, 2)],
    };
    let (src, reply) = (src_schema(), reply_schema(2));
    let mut fold = AdhocFold::new(&src, &reply, &spec, 1000).unwrap();
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
    let spec = AggReadSpec {
        group_cols: vec![1],
        aggs: vec![agg(AGG_COUNT, 0), agg(AGG_SUM, 2)],
    };
    let (src, reply) = (src_schema(), reply_schema(2));
    let batch = build(&[
        (1, 1, 7, Some(10)),
        (2, 1, 9, Some(999)), // skipped
        (3, 1, 7, Some(20)),
        (4, 1, 7, Some(30)),
    ]);
    let mut fold = AdhocFold::new(&src, &reply, &spec, 1000).unwrap();
    // Two survivor ranges: [0,1) and [2,4) — row 1 (group 9) is filtered out.
    fold.fold_ranges(&batch, &[(0, 1), (2, 4)]).unwrap();
    let g = by_group(&fold.finish(), 2);
    assert_eq!(g.len(), 1, "group 9 lived only in the skipped range");
    assert_eq!(g[&7], (1, vec![Some(3), Some(60)]));
}

#[test]
fn fold_global_single_group() {
    let spec = AggReadSpec {
        group_cols: vec![],
        aggs: vec![agg(AGG_COUNT, 0), agg(AGG_MAX, 2)],
    };
    let src = src_schema();
    // Global reply: _agg_pk(U128) + 2 agg cols (no group col).
    let reply = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let mut fold = AdhocFold::new(&src, &reply, &spec, 1000).unwrap();
    let b = build(&[(1, 1, 0, Some(3)), (2, 1, 0, Some(9)), (3, 1, 0, Some(1))]);
    fold.fold_ranges(&b, &[(0, b.count)]).unwrap();
    let out = fold.finish();
    assert_eq!(out.count, 1);
    assert_eq!(read_i64_le(out.col_data(0), 0), 3); // COUNT*
    assert_eq!(read_i64_le(out.col_data(1), 0), 9); // MAX
    assert_eq!(out.as_mem_batch().get_weight(0), 1);
}

#[test]
fn fold_group_cap_aborts() {
    let spec = AggReadSpec {
        group_cols: vec![1],
        aggs: vec![agg(AGG_COUNT, 0)],
    };
    let (src, reply) = (src_schema(), reply_schema(1));
    // Cap of 2 distinct groups; a third distinct group trips it.
    let mut fold = AdhocFold::new(&src, &reply, &spec, 2).unwrap();
    let b = build(&[(1, 1, 1, Some(0)), (2, 1, 2, Some(0)), (3, 1, 3, Some(0))]);
    let err = fold.fold_ranges(&b, &[(0, b.count)]).unwrap_err();
    assert!(err.contains("CREATE VIEW"), "{err}");
}

/// A structurally valid reply schema that is not the derived SyntheticFold
/// layout (here: missing the agg column) is a malformed frame — rejected,
/// never fed to `ReducePlan::new` (whose `cbase` arithmetic would panic).
#[test]
fn fold_rejects_mismatched_reply_schema() {
    let spec = AggReadSpec {
        group_cols: vec![1],
        aggs: vec![agg(AGG_COUNT, 0)],
    };
    let src = src_schema();
    let too_few = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    assert!(AdhocFold::new(&src, &too_few, &spec, 1000).is_err());
    let wrong_type = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::F64, 0), // COUNT partial is I64
        ],
        &[0],
    );
    assert!(AdhocFold::new(&src, &wrong_type, &spec, 1000).is_err());
}

/// The spec is a trust boundary and the accumulator is not defensive: SUM over a
/// STRING and MIN over a U128 have no scalar register image, and a U128 MIN would
/// slip past the reply-schema check too (`agg_output_type(Min, U128)` is I64).
#[test]
fn fold_rejects_aggregates_with_no_encoding() {
    // pk(U64), s(STRING), w(U128).
    let src = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 0),
            SchemaColumn::new(type_code::U128, 0),
        ],
        &[0],
    );
    let reply = reply_schema(1);
    for (op, col) in [(AGG_SUM, 1u16), (AGG_MIN, 2)] {
        let spec = AggReadSpec {
            group_cols: vec![],
            aggs: vec![agg(op, col)],
        };
        let Err(err) = AdhocFold::new(&src, &reply, &spec, 1000) else {
            panic!("{op:#x} over column {col} must be rejected");
        };
        assert!(err.contains("no scalar register image"), "{err}");
    }
    // COUNT reads no value, so the same columns are countable.
    for col in [1u16, 2] {
        let spec = AggReadSpec {
            group_cols: vec![],
            aggs: vec![agg(AGG_COUNT, col)],
        };
        assert!(AdhocFold::new(&src, &reply_global(1), &spec, 1000).is_ok());
    }
}

#[test]
fn fold_rejects_out_of_range_column() {
    let spec = AggReadSpec {
        group_cols: vec![9], // no such column
        aggs: vec![agg(AGG_COUNT, 0)],
    };
    let (src, reply) = (src_schema(), reply_schema(1));
    assert!(AdhocFold::new(&src, &reply, &spec, 1000).is_err());
}
