use super::*;
use crate::hir::bind_and_lower_fold;
use crate::test_support::{col, ncol, parse_query, table};
use gnitz_core::{BatchAppender, PkColumn, RelDescriptor};
use gnitz_expr::{payload_str, payload_u64};
use gnitz_wire::{global_group_key, FixedInt, TypeCode};

/// A long body, spilled to the arena rather than inlined in its German cell.
const LONG_M: &str = "a string past the inline prefix: m";
const LONG_Z: &str = "a string past the inline prefix: z";

/// One payload cell, written into a partial reply or read back out of a result.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Cell<'a> {
    Int(i128),
    F64(f64),
    Str(&'a str),
    Null,
}
use self::Cell::{Int, Null, Str, F64};

/// `(pk U64 | g I64 | s STRING | sm I16 | x I64 | f F64 | u U64)`, every payload nullable.
fn t() -> Arc<RelDescriptor> {
    table(
        1,
        vec![
            col("pk", TypeCode::U64),
            ncol("g", TypeCode::I64),
            ncol("s", TypeCode::String),
            ncol("sm", TypeCode::I16),
            ncol("x", TypeCode::I64),
            ncol("f", TypeCode::F64),
            ncol("u", TypeCode::U64),
        ],
        vec![0],
    )
}

/// The finisher the ad-hoc read path plans for `sql` over `rel`, bound as `t`.
fn plan(sql: &str, rel: &Arc<RelDescriptor>) -> FoldFinish {
    let sqlparser::ast::SetExpr::Select(select) = *parse_query(sql).body else {
        panic!("{sql}: not a SELECT");
    };
    let (pieces, _) = bind_and_lower_fold(&select, rel, "t", &[]).unwrap().expect("a fold");
    FoldFinish::new(
        pieces.partial_schema,
        pieces.agg.aggs.iter().map(|d| d.agg_op),
        &pieces.having,
        pieces.finalize,
    )
    .unwrap()
}

/// A concatenated partial reply to `f`: one weight-1 row per `(key, cells)`, the key as
/// its columns' natives, one cell per payload slot.
fn reply(f: &FoldFinish, rows: &[(&[u128], &[Cell])]) -> ZSetBatch {
    let schema = f.partial_schema.as_ref();
    let mut b = ZSetBatch::new(schema);
    let mut app = BatchAppender::new(&mut b, schema);
    for &(key, cells) in rows {
        app.add_row_natives(key, 1);
        for &cell in cells {
            match cell {
                Int(v) => app.int_val(v),
                F64(v) => app.f64_val(v),
                Str(s) => app.str_val(s),
                Null => app.null(),
            };
        }
    }
    b
}

/// Each row of `b` (in `schema`) as its payload cells and weight.
fn rows<'a>(schema: &Schema, b: &'a ZSetBatch) -> Vec<(Vec<Cell<'a>>, i64)> {
    let locs = schema.payload_locators();
    let cell = |r: usize, pi: usize, loc: &ColumnLocator| {
        let tc = loc.type_code();
        if loc.is_null(b, r) {
            Null
        } else if tc.is_german_string() {
            Str(payload_str(b, r, pi))
        } else if tc == TypeCode::F64 {
            F64(f64::from_bits(payload_u64(b, r, pi)))
        } else {
            let fi = FixedInt::from_type_code(tc).expect("an integer column");
            let v = loc.decode_i64(b, r, fi);
            Int(if fi.is_signed() { v as i128 } else { v as u64 as i128 })
        }
    };
    (0..b.len())
        .map(|r| {
            (
                locs.iter().enumerate().map(|(pi, loc)| cell(r, pi, loc)).collect(),
                b.weights[r],
            )
        })
        .collect()
}

/// Replies concatenate in worker order: keys unsorted, a group split across two workers.
#[test]
fn the_output_row_carries_the_engine_group_key() {
    let mut f = plan("SELECT g, COUNT(*) FROM t GROUP BY g", &t());
    let got = f.finish(reply(
        &f,
        &[
            (&[0x2222], &[Int(20), Int(1)]),
            (&[0x1111], &[Int(10), Int(2)]),
            (&[0x1111], &[Int(10), Int(3)]),
        ],
    ));

    assert_eq!(got.pks, PkColumn::from_natives(f.out_schema(), [0x2222, 0x1111]));
    assert_eq!(
        rows(f.out_schema(), &got),
        [(vec![Int(20), Int(1)], 1), (vec![Int(10), Int(5)], 1)]
    );
}

/// Every worker emits a global fold's row, most of them the empty-input ground row.
#[test]
fn partials_merge_per_aggregate_with_null_as_identity() {
    let mut f = plan("SELECT COUNT(*), SUM(x), SUM(f), MIN(sm), MAX(s) FROM t", &t());
    let v0 = global_group_key();
    // COUNT(*), then each SUM with its non-null companion, then MIN and MAX.
    let ground: &[Cell] = &[Int(0), Int(0), Int(0), F64(0.0), Int(0), Null, Null];

    let got = f.finish(reply(
        &f,
        &[
            (&[v0], ground),
            (
                &[v0],
                &[Int(3), Int(i64::MAX as i128), Int(3), F64(1.5), Int(3), Int(-5), Null],
            ),
            (&[v0], ground),
            (&[v0], &[Int(2), Int(1), Int(2), F64(2.25), Int(2), Int(3), Str(LONG_Z)]),
            (&[v0], &[Int(1), Int(0), Int(1), F64(0.0), Int(1), Int(-7), Str(LONG_M)]),
        ],
    ));
    let (wrapped, signed_min, past_prefix) = (Int(i64::MIN as i128), Int(-7), Str(LONG_Z));
    assert_eq!(
        rows(f.out_schema(), &got),
        [(vec![Int(6), wrapped, F64(3.75), signed_min, past_prefix], 1)]
    );

    let got = f.finish(reply(&f, &[(&[v0], ground), (&[v0], ground)]));
    assert_eq!(rows(f.out_schema(), &got), [(vec![Int(0), Null, Null, Null, Null], 1)]);
}

/// Group 0x2 passes HAVING only once its two partials merge.
#[test]
fn having_filters_combined_groups_before_the_finalize_map() {
    let mut f = plan(
        "SELECT COUNT(*) AS c, s, COUNT(*) + 1 AS c1 FROM t GROUP BY s HAVING COUNT(*) > 1",
        &t(),
    );
    let got = f.finish(reply(
        &f,
        &[
            (&[0x1], &[Null, Int(2)]),
            (&[0x3], &[Str("x"), Int(1)]),
            (&[0x2], &[Str(LONG_M), Int(1)]),
            (&[0x2], &[Str(LONG_M), Int(1)]),
        ],
    ));

    got.validate(f.out_schema()).unwrap();
    assert_eq!(got.pks, PkColumn::from_natives(f.out_schema(), [0x1, 0x2]));
    assert_eq!(
        rows(f.out_schema(), &got),
        [(vec![Int(2), Null, Int(3)], 1), (vec![Int(2), Str(LONG_M), Int(3)], 1)]
    );
}

#[test]
fn avg_divides_an_unsigned_sum_unsigned_and_nulls_on_a_zero_count() {
    let mut f = plan("SELECT AVG(u) FROM t", &t());
    let got = f.finish(reply(
        &f,
        &[(&[1], &[Int(u64::MAX as i128), Int(1)]), (&[2], &[Int(0), Int(0)])],
    ));

    assert_eq!(
        rows(f.out_schema(), &got),
        [(vec![F64(u64::MAX as f64)], 1), (vec![Null], 1)]
    );
}

/// Grouped by the whole PK, the partial is keyed by it and holds no group column.
#[test]
fn a_whole_pk_natural_partial_merges_only_its_aggregates() {
    let c = table(
        2,
        vec![
            col("a", TypeCode::U64),
            col("b", TypeCode::I32),
            ncol("x", TypeCode::I64),
        ],
        vec![0, 1],
    );
    let mut f = plan("SELECT a, b, COUNT(*), SUM(x), MIN(x) FROM t GROUP BY a, b", &c);
    let neg = -1i32 as u128;
    // COUNT(*), SUM(x) with its non-null companion, MIN(x).
    let got = f.finish(reply(
        &f,
        &[
            (&[7, neg], &[Int(1), Int(10), Int(1), Int(10)]),
            (&[7, 2], &[Int(2), Int(5), Int(2), Int(1)]),
            (&[7, neg], &[Int(3), Int(20), Int(3), Int(-4)]),
        ],
    ));

    assert_eq!(
        rows(f.out_schema(), &got),
        [
            (vec![Int(7), Int(-1), Int(4), Int(30), Int(-4)], 1),
            (vec![Int(7), Int(2), Int(2), Int(5), Int(1)], 1)
        ]
    );
}
