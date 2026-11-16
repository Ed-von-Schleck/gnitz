use super::*;
use crate::hir::{AdhocShape, bind_adhoc_read};
use crate::test_support::Cell::{self, F64, Int, Null, Str};
use crate::test_support::{catalog, col, keyed_batch, ncol, parse_query, rows_of, table};
use gnitz_core::RelDescriptor;
use gnitz_wire::{TypeCode, global_group_key};

/// A long body, spilled to the arena rather than inlined in its German cell.
const LONG_M: &str = "a string past the inline prefix: m";
const LONG_Z: &str = "a string past the inline prefix: z";

/// `(pk U64 | g I64 | s STRING | sm I16 | x I64 | f F64 | u U64)`, every payload nullable.
pub(super) fn t() -> Arc<RelDescriptor> {
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

/// `(a U64, b U64, c I32 | x I64)`: a 20-byte key, past the narrow image, groups by its bytes.
pub(super) fn wide() -> Arc<RelDescriptor> {
    table(
        3,
        vec![
            col("a", TypeCode::U64),
            col("b", TypeCode::U64),
            col("c", TypeCode::I32),
            ncol("x", TypeCode::I64),
        ],
        vec![0, 1, 2],
    )
}

/// The finisher the ad-hoc read path plans for `sql` over `rel`, bound as `t`.
pub(super) fn plan(sql: &str, rel: &Arc<RelDescriptor>) -> FoldFinish {
    let query = parse_query(sql);
    let cat = catalog(vec![("t", Arc::clone(rel))]);
    let read = bind_adhoc_read(&cat, &query, &[]).unwrap();
    let crate::hir::AdhocRead::Relation {
        shape: AdhocShape::Fold { pieces, .. }, ..
    } = read
    else {
        panic!("{sql}: not a fold");
    };
    FoldFinish::new(pieces.partial_schema, &pieces.agg.aggs, &pieces.having, pieces.finalize).unwrap()
}

type Rows<'a> = &'a [(&'a [u128], &'a [Cell<'a>])];

/// Worker replies concatenate in worker order, keys unsorted: a group's first row absorbs
/// its later ones — a linear partial adds, wrapping as the engine accumulator does, an
/// extreme takes the winner with NULL as identity — then HAVING filters the combined
/// groups and finalize maps them. Each row is `(key, cells)` at weight 1.
#[test]
fn partials_finish_to_one_row_per_group() {
    let (t, pk2, wide, nat) = (
        t(),
        table(
            2,
            vec![
                col("a", TypeCode::U64),
                col("b", TypeCode::I32),
                ncol("x", TypeCode::I64),
            ],
            vec![0, 1],
        ),
        wide(),
        table(
            4,
            vec![
                col("pk", TypeCode::U64),
                col("n", TypeCode::I64),
                ncol("x", TypeCode::I64),
            ],
            vec![0],
        ),
    );
    let v0: &[u128] = &[global_group_key()];
    let global = "SELECT COUNT(*), SUM(x), SUM(f), MIN(sm), MAX(s), AVG(u) FROM t";
    // COUNT(*), then SUM(x) and SUM(f) each with its non-null count, SUM(f)'s
    // +inf and -inf counts, MIN, MAX, then AVG(u)'s sum and count.
    let ground: &[Cell] = &[
        Int(0),
        Int(0),
        Int(0),
        F64(0.0),
        Int(0),
        Int(0),
        Int(0),
        Null,
        Null,
        Int(0),
        Int(0),
    ];
    // SUM(f) and AVG(f) share their sum, count and infinity counts.
    let float = "SELECT SUM(f), AVG(f) FROM t";
    let neg = -1i32 as u128;
    let neg3 = -3i64 as u64 as u128;
    let cases: [(&str, &Arc<RelDescriptor>, Rows, Rows); 13] = [
        // A lone non-NULL group column is the partial's key, decoded back out by finalize.
        (
            "SELECT n, COUNT(*), SUM(x) FROM t GROUP BY n",
            &nat,
            &[
                (&[neg3], &[Int(1), Int(4), Int(1)]),
                (&[5], &[Int(2), Int(7), Int(2)]),
                (&[neg3], &[Int(3), Int(-1), Int(3)]),
            ],
            &[(&[neg3], &[Int(-3), Int(4), Int(3)]), (&[5], &[Int(5), Int(2), Int(7)])],
        ),
        // An inline string against spilled ones.
        (
            "SELECT MAX(s) FROM t",
            &t,
            &[
                (v0, &[Str(LONG_M)]),
                (v0, &[Str("zz")]),
                (v0, &[Null]),
                (v0, &[Str("b")]),
            ],
            &[(v0, &[Str("zz")])],
        ),
        (
            "SELECT g, COUNT(*) FROM t GROUP BY g",
            &t,
            &[
                (&[0x2222], &[Int(20), Int(1)]),
                (&[0x1111], &[Int(10), Int(2)]),
                (&[0x1111], &[Int(10), Int(3)]),
            ],
            &[(&[0x2222], &[Int(20), Int(1)]), (&[0x1111], &[Int(10), Int(5)])],
        ),
        // Every worker emits a global fold's row, most of them the empty-input ground row.
        // AVG divides an unsigned sum unsigned.
        (
            global,
            &t,
            &[
                (v0, ground),
                (
                    v0,
                    &[
                        Int(3),
                        Int(i64::MAX as i128),
                        Int(3),
                        F64(1.5),
                        Int(3),
                        Int(0),
                        Int(0),
                        Int(-5),
                        Null,
                        Int(u64::MAX as i128),
                        Int(1),
                    ],
                ),
                (v0, ground),
                (
                    v0,
                    &[
                        Int(2),
                        Int(1),
                        Int(2),
                        F64(2.25),
                        Int(2),
                        Int(0),
                        Int(0),
                        Int(3),
                        Str(LONG_Z),
                        Int(0),
                        Int(0),
                    ],
                ),
                (
                    v0,
                    &[
                        Int(1),
                        Int(0),
                        Int(1),
                        F64(0.0),
                        Int(1),
                        Int(0),
                        Int(0),
                        Int(-7),
                        Str(LONG_M),
                        Int(0),
                        Int(0),
                    ],
                ),
            ],
            &[(
                v0,
                &[
                    Int(6),
                    Int(i64::MIN as i128),
                    F64(3.75),
                    Int(-7),
                    Str(LONG_Z),
                    F64(u64::MAX as f64),
                ],
            )],
        ),
        // AVG of a zero count is NULL.
        (
            global,
            &t,
            &[(v0, ground), (v0, ground)],
            &[(v0, &[Int(0), Null, Null, Null, Null, Null])],
        ),
        // The finite sum is overridden once a worker's partial counted an
        // infinity, and the counts of every worker's partials add.
        (
            float,
            &t,
            &[
                (v0, &[F64(1.5), Int(1), Int(0), Int(0)]),
                (v0, &[F64(0.0), Int(1), Int(0), Int(1)]),
            ],
            &[(v0, &[F64(f64::NEG_INFINITY), F64(f64::NEG_INFINITY)])],
        ),
        (
            float,
            &t,
            &[
                (v0, &[F64(1.5), Int(1), Int(0), Int(0)]),
                (v0, &[F64(2.5), Int(3), Int(1), Int(0)]),
            ],
            &[(v0, &[F64(f64::INFINITY), F64(f64::INFINITY)])],
        ),
        // Group 0x2 passes HAVING only once its two partials merge.
        (
            "SELECT COUNT(*) AS c, s, COUNT(*) + 1 AS c1 FROM t GROUP BY s HAVING COUNT(*) > 1",
            &t,
            &[
                (&[0x1], &[Null, Int(2)]),
                (&[0x3], &[Str("x"), Int(1)]),
                (&[0x2], &[Str(LONG_M), Int(1)]),
                (&[0x2], &[Str(LONG_M), Int(1)]),
            ],
            &[
                (&[0x1], &[Int(2), Null, Int(3)]),
                (&[0x2], &[Int(2), Str(LONG_M), Int(3)]),
            ],
        ),
        // No aggregate: the partials only dedup.
        (
            "SELECT DISTINCT g, s FROM t",
            &t,
            &[
                (&[0x5], &[Int(1), Str("x")]),
                (&[0x6], &[Int(2), Str(LONG_M)]),
                (&[0x5], &[Int(1), Str("x")]),
            ],
            &[(&[0x5], &[Int(1), Str("x")]), (&[0x6], &[Int(2), Str(LONG_M)])],
        ),
        ("SELECT g, COUNT(*) FROM t GROUP BY g", &t, &[], &[]),
        // Grouped by the whole PK, the partial is keyed by it and holds no group column.
        (
            "SELECT a, b, COUNT(*), SUM(x), MIN(x) FROM t GROUP BY a, b",
            &pk2,
            &[
                (&[7, neg], &[Int(1), Int(10), Int(1), Int(10)]),
                (&[7, 2], &[Int(2), Int(5), Int(2), Int(1)]),
                (&[7, neg], &[Int(3), Int(20), Int(3), Int(-4)]),
            ],
            &[
                (&[7, neg], &[Int(7), Int(-1), Int(4), Int(30), Int(-4)]),
                (&[7, 2], &[Int(7), Int(2), Int(2), Int(5), Int(1)]),
            ],
        ),
        (
            "SELECT a, b, c, COUNT(*), MAX(x) FROM t GROUP BY a, b, c",
            &wide,
            &[
                (&[1, 2, 3], &[Int(1), Null]),
                (&[1, 2, 4], &[Int(2), Int(9)]),
                (&[1, 2, 3], &[Int(3), Int(-4)]),
                (&[1, 2, 3], &[Int(1), Int(-6)]),
            ],
            &[
                (&[1, 2, 3], &[Int(1), Int(2), Int(3), Int(5), Int(-4)]),
                (&[1, 2, 4], &[Int(1), Int(2), Int(4), Int(2), Int(9)]),
            ],
        ),
        // Every row its own group: the reply comes back as it went in.
        (
            "SELECT a, b, c, COUNT(*), MAX(x) FROM t GROUP BY a, b, c",
            &wide,
            &[(&[1, 2, 3], &[Int(1), Null]), (&[1, 2, 4], &[Int(2), Int(9)])],
            &[
                (&[1, 2, 3], &[Int(1), Int(2), Int(3), Int(1), Null]),
                (&[1, 2, 4], &[Int(1), Int(2), Int(4), Int(2), Int(9)]),
            ],
        ),
    ];
    let batch =
        |schema: &Schema, rows: Rows| keyed_batch(schema, &rows.iter().map(|&(k, c)| (k, 1, c)).collect::<Vec<_>>());
    for (sql, rel, partials, want) in cases {
        let mut f = plan(sql, rel);
        let got = f.finish(batch(&f.partial_schema, partials));
        let want = batch(f.out_schema(), want);
        got.validate(f.out_schema()).unwrap();
        assert_eq!(got.pks, want.pks, "{sql}");
        assert_eq!(rows_of(f.out_schema(), &got), rows_of(f.out_schema(), &want), "{sql}");
    }
}

/// A float extreme takes the total order's winner: -0.0 below 0.0, NaN above every number.
#[test]
fn float_extremes_follow_the_total_order() {
    let mut f = plan("SELECT MIN(f), MAX(f) FROM t", &t());
    let v0: &[u128] = &[global_group_key()];
    let rows: [(&[u128], i64, &[Cell]); 4] = [
        (v0, 1, &[F64(0.0), F64(-0.0)]),
        (v0, 1, &[F64(-0.0), F64(0.0)]),
        (v0, 1, &[F64(f64::NAN), F64(f64::NAN)]),
        (v0, 1, &[Null, Null]),
    ];
    let got = f.finish(keyed_batch(f.partial_schema(), &rows));
    let bits: Vec<_> = rows_of(f.out_schema(), &got)
        .into_iter()
        .map(|(cells, w)| {
            let bits = cells.iter().map(|c| match c {
                F64(v) => v.to_bits(),
                other => panic!("not a float: {other:?}"),
            });
            (bits.collect::<Vec<_>>(), w)
        })
        .collect();
    assert_eq!(bits, [(vec![(-0.0f64).to_bits(), f64::NAN.to_bits()], 1)]);
}
