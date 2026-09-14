use super::*;
use crate::agg::AggFunc;
use crate::agg::{agg_col_def, finalize_agg_bexpr};
use crate::ir::{BExpr, BinOp};
use crate::test_support::col_def;
use gnitz_core::{PkColumn, TypeCode};
use gnitz_wire::AggDescriptor;

/// A long body, spilled to the arena rather than inlined in its German cell.
const LONG_M: &str = "a string past the inline prefix: m";
const LONG_Z: &str = "a string past the inline prefix: z";

/// One partial cell: `Int` truncated to its column's stride, `Null` a zeroed cell
/// under a set bit.
#[derive(Clone, Copy)]
enum Cell<'a> {
    Int(i64),
    F64(f64),
    Str(&'a str),
    Null,
}
use self::Cell::{Int, Null, Str, F64};

/// `(pk U64 | g I64 | s STRING | sm I16 | x I64 | f F64)`, every payload nullable.
fn source_schema() -> Schema {
    Schema {
        columns: vec![
            col_def("pk", TypeCode::U64, false),
            col_def("g", TypeCode::I64, true),
            col_def("s", TypeCode::String, true),
            col_def("sm", TypeCode::I16, true),
            col_def("x", TypeCode::I64, true),
            col_def("f", TypeCode::F64, true),
        ],
        pk_cols: vec![0],
    }
}

fn spec(agg_op: WireAggFunc, col: usize, _: TypeCode) -> AggDescriptor {
    AggDescriptor { agg_op, col_idx: col as u32 }
}

/// The SyntheticFold partial layout over `source`: the hidden key, the group
/// columns, then each spec's raw column.
fn partial_over(source: &Schema, group: &[usize], specs: &[AggDescriptor]) -> Schema {
    let mut cols = vec![group_pk_def()];
    cols.extend(group.iter().map(|&g| source.columns[g].clone()));
    cols.extend(
        specs
            .iter()
            .map(|d| agg_col_def(d.agg_op, Some(&source.columns[d.col_idx as usize]), group.is_empty())),
    );
    Schema::from_parts(cols, vec![0]).expect("the SyntheticFold layout is a valid client schema")
}

fn partial_schema(group: &[usize], specs: &[AggDescriptor]) -> Schema {
    partial_over(&source_schema(), group, specs)
}

/// Every partial column passed through under a name of its own: an identity
/// finalize.
fn passthrough_all(partial: &Schema) -> Vec<(BoundExpr, ColumnDef)> {
    (1..partial.columns.len())
        .map(|ci| {
            let c = &partial.columns[ci];
            (
                BExpr::ColRef(ci),
                ColumnDef::new(format!("c{ci}"), c.type_code, c.is_nullable),
            )
        })
        .collect()
}

fn finish_of(
    partial: &Schema,
    specs: &[AggDescriptor],
    having: &[BoundExpr],
    finalize: Vec<(BoundExpr, ColumnDef)>,
) -> FoldFinish {
    FoldFinish::new(partial.clone(), specs.iter().map(|d| d.agg_op), having, finalize).unwrap()
}

/// A concatenated partial reply over `schema`: one weight-1 row per
/// `(key, cells)`, one cell per payload slot.
fn batch(schema: &Schema, rows: &[(u128, &[Cell])]) -> ZSetBatch {
    let mut b = ZSetBatch::new(schema);
    for &(key, cells) in rows {
        b.pks.push_u128(schema, key);
        b.weights.push(1);
        let mut nulls = 0u64;
        for (pi, cell) in cells.iter().enumerate() {
            let stride = b.payload[pi].stride();
            match *cell {
                Int(v) => b.payload[pi].bytes.extend_from_slice(&v.to_le_bytes()[..stride]),
                F64(v) => b.payload[pi].bytes.extend_from_slice(&v.to_le_bytes()),
                Str(s) => {
                    let c = gnitz_wire::encode_german_string(s.as_bytes(), &mut b.blob);
                    b.payload[pi].bytes.extend_from_slice(&c);
                }
                Null => {
                    gnitz_wire::null_word_set(&mut nulls, pi, true);
                    b.payload[pi].push_zero();
                }
            }
        }
        b.nulls.push(nulls);
    }
    b
}

/// Payload slot `pi` of an 8-byte integer column.
fn ints(b: &ZSetBatch, pi: usize) -> Vec<i64> {
    b.payload[pi]
        .bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|c| i64::from_le_bytes(*c))
        .collect()
}

fn strs(b: &ZSetBatch, pi: usize) -> Vec<String> {
    b.payload[pi]
        .bytes
        .as_chunks::<16>()
        .0
        .iter()
        .map(|c| String::from_utf8(gnitz_wire::german_string_content(c, &b.blob).to_vec()).unwrap())
        .collect()
}

fn null_bits(b: &ZSetBatch, pi: usize) -> Vec<bool> {
    b.nulls.iter().map(|&w| gnitz_wire::null_word_get(w, pi)).collect()
}

fn gt(ci: usize, v: i64) -> BoundExpr {
    BExpr::bin(BExpr::ColRef(ci), BinOp::Gt, BExpr::LitInt(v))
}

/// The partial mirrors what `fetch_bound` concatenates: replies in worker
/// order, so a group's rows split across two workers and the keys arrive
/// descending — the emission ordinal and the group key then disagree on both
/// order and value, and only the key is a function of the data.
#[test]
fn the_output_row_carries_the_engine_group_key() {
    let specs = [spec(WireAggFunc::Count, 0, TypeCode::I64)];
    let partial = partial_schema(&[1], &specs);
    let f = finish_of(&partial, &specs, &[], passthrough_all(&partial));
    let reply = batch(
        &partial,
        &[
            (0x2222, &[Int(20), Int(1)]),
            (0x1111, &[Int(10), Int(2)]),
            (0x1111, &[Int(10), Int(3)]),
        ],
    );

    let got = f.apply(f.combine(reply));

    assert_eq!(got.pks, PkColumn::from_natives(&partial, [0x2222, 0x1111]));
    assert_eq!(ints(&got, 0), [20, 10]);
    assert_eq!(ints(&got, 1), [1, 5]);
}

/// Every worker emits a global fold's row, most of them the ground row; merged
/// into a computed row it changes nothing, and grounds alone stay the ground.
#[test]
fn ground_rows_are_the_merge_identity() {
    let specs = [
        spec(WireAggFunc::Count, 0, TypeCode::I64),
        spec(WireAggFunc::Sum, 4, TypeCode::I64),
        spec(WireAggFunc::Min, 3, TypeCode::I16),
        spec(WireAggFunc::Max, 2, TypeCode::String),
    ];
    let partial = partial_schema(&[], &specs);
    let f = finish_of(&partial, &specs, &[], passthrough_all(&partial));
    let v0 = gnitz_wire::global_group_key();
    let ground: &[Cell] = &[Int(0), Null, Null, Null];

    let got = f.combine(batch(
        &partial,
        &[
            (v0, ground),
            (v0, &[Int(3), Int(40), Int(-2), Str(LONG_M)]),
            (v0, ground),
        ],
    ));
    assert_eq!(got.len(), 1);
    assert_eq!(got.nulls, [0]);
    assert_eq!(ints(&got, 0), [3]);
    assert_eq!(ints(&got, 1), [40]);
    assert_eq!(got.payload[2].bytes, (-2i16).to_le_bytes());
    assert_eq!(strs(&got, 3), [LONG_M]);

    let got = f.combine(batch(&partial, &[(v0, ground), (v0, ground)]));
    assert_eq!(got.len(), 1);
    assert_eq!(ints(&got, 0), [0]);
    assert_eq!(got.nulls, [0b1110]);
}

/// A winner replaces the held value by its own type's order — signed at a
/// sub-8-byte width, by content past a German cell's prefix — and a NULL neither
/// wins nor blocks a later winner.
#[test]
fn extremes_merge_by_value_including_strings() {
    let specs = [
        spec(WireAggFunc::Min, 3, TypeCode::I16),
        spec(WireAggFunc::Max, 2, TypeCode::String),
    ];
    let partial = partial_schema(&[], &specs);
    let f = finish_of(&partial, &specs, &[], passthrough_all(&partial));
    let v0 = gnitz_wire::global_group_key();

    let got = f.combine(batch(
        &partial,
        &[
            (v0, &[Int(-5), Null]),
            (v0, &[Int(3), Str(LONG_Z)]),
            (v0, &[Int(-7), Str(LONG_M)]),
        ],
    ));

    assert_eq!(got.len(), 1);
    assert_eq!(got.nulls, [0]);
    assert_eq!(got.payload[0].bytes, (-7i16).to_le_bytes());
    assert_eq!(strs(&got, 1), [LONG_Z]);
}

#[test]
fn sums_wrap_and_floats_add() {
    let specs = [
        spec(WireAggFunc::Sum, 4, TypeCode::I64),
        spec(WireAggFunc::Sum, 5, TypeCode::F64),
    ];
    let partial = partial_schema(&[], &specs);
    let f = finish_of(&partial, &specs, &[], passthrough_all(&partial));
    let v0 = gnitz_wire::global_group_key();

    let got = f.combine(batch(
        &partial,
        &[
            (v0, &[Int(i64::MAX), F64(1.5)]),
            (v0, &[Int(1), F64(2.25)]),
            (v0, &[Null, Null]),
        ],
    ));

    assert_eq!(got.nulls, [0]);
    assert_eq!(ints(&got, 0), [i64::MIN]);
    assert_eq!(got.payload[1].bytes, 3.75f64.to_le_bytes());
}

/// Rows sharing a `_group_pk` but not their group values stay apart, a NULL
/// group value included — which is also not the zero its cell holds.
#[test]
fn a_digest_collision_keeps_groups_apart() {
    let specs = [spec(WireAggFunc::Count, 0, TypeCode::I64)];
    let partial = partial_schema(&[1], &specs);
    let f = finish_of(&partial, &specs, &[], passthrough_all(&partial));

    let got = f.combine(batch(
        &partial,
        &[
            (0x42, &[Int(10), Int(1)]),
            (0x42, &[Int(20), Int(2)]),
            (0x42, &[Null, Int(5)]),
            (0x42, &[Int(10), Int(3)]),
            (0x42, &[Int(0), Int(7)]),
            (0x42, &[Null, Int(11)]),
        ],
    ));

    assert_eq!(got.pks, PkColumn::from_natives(&partial, [0x42; 4]));
    assert_eq!(null_bits(&got, 0), [false, false, true, false]);
    assert_eq!(ints(&got, 0), [10, 20, 0, 0]);
    assert_eq!(ints(&got, 1), [4, 2, 16, 7]);
}

#[test]
fn a_reply_sharing_no_group_is_returned_whole() {
    let specs = [spec(WireAggFunc::Count, 0, TypeCode::I64)];
    let partial = partial_schema(&[1], &specs);
    let f = finish_of(&partial, &specs, &[], passthrough_all(&partial));
    let reply = batch(
        &partial,
        &[
            (0x1, &[Int(1), Int(1)]),
            (0x2, &[Int(2), Int(2)]),
            (0x3, &[Null, Int(3)]),
        ],
    );

    assert_eq!(f.combine(reply.clone()), reply);
}

#[test]
fn having_compacts_before_an_identity_finalize() {
    let specs = [spec(WireAggFunc::Count, 0, TypeCode::I64)];
    let partial = partial_schema(&[1], &specs);
    let f = finish_of(&partial, &specs, &[gt(2, 1)], passthrough_all(&partial));
    assert!(f.identity);

    let got = f.apply(batch(
        &partial,
        &[
            (0x1, &[Int(1), Int(1)]),
            (0x2, &[Int(2), Int(3)]),
            (0x3, &[Int(3), Int(1)]),
            (0x4, &[Int(4), Int(5)]),
        ],
    ));

    assert_eq!(got.pks, PkColumn::from_natives(&partial, [0x2, 0x4]));
    assert_eq!(ints(&got, 0), [2, 4]);
    assert_eq!(ints(&got, 1), [3, 5]);
}

/// `SELECT COUNT(*) AS c, s, COUNT(*) + 1 AS c1 … GROUP BY s HAVING COUNT(*) > 1`:
/// the map moves the STRING group column behind a COUNT, carrying its NULL bit
/// to the new slot, and computes over the surviving groups only.
#[test]
fn a_projecting_finalize_runs_the_map() {
    let specs = [spec(WireAggFunc::Count, 0, TypeCode::I64)];
    let partial = partial_schema(&[2], &specs);
    let plus_one = BExpr::bin(BExpr::ColRef(2), BinOp::Add, BExpr::LitInt(1));
    let f = finish_of(
        &partial,
        &specs,
        &[gt(2, 1)],
        vec![
            (BExpr::ColRef(2), ColumnDef::new("c", TypeCode::I64, false)),
            (BExpr::ColRef(1), ColumnDef::new("s", TypeCode::String, true)),
            (plus_one, ColumnDef::new("c1", TypeCode::I64, true)),
        ],
    );
    assert!(!f.identity);

    let got = f.apply(batch(
        &partial,
        &[
            (0x1, &[Null, Int(2)]),
            (0x2, &[Str(LONG_M), Int(4)]),
            (0x3, &[Str("x"), Int(1)]),
        ],
    ));

    assert!(got.validate(&f.out_schema).is_ok());
    assert_eq!(got.pks, PkColumn::from_natives(&f.out_schema, [0x1, 0x2]));
    assert_eq!(got.weights, [1, 1]);
    assert_eq!(got.nulls, [0b010, 0]);
    assert_eq!(ints(&got, 0), [2, 4]);
    assert_eq!(strs(&got, 1)[1], LONG_M);
    assert_eq!(ints(&got, 2), [3, 5]);
}

/// A global `AVG(u)` over a `BIGINT UNSIGNED` column, built the way the planner
/// builds one: `agg_ops` splits it into `[Sum, CountNonNull]`, and the
/// finalize item is the shared composite over the two partial columns — the
/// *only* place the division happens on this path. Drives one worker partial
/// `(sum bits, count)` through the whole finish and reads the AVG cell back.
fn finish_avg(sum_bits: i64, cnt: i64) -> Option<f64> {
    let src = Schema {
        columns: vec![col_def("pk", TypeCode::U64, false), col_def("u", TypeCode::U64, true)],
        pk_cols: vec![0],
    };
    let specs = [
        spec(WireAggFunc::Sum, 1, TypeCode::U64),
        spec(WireAggFunc::CountNonNull, 1, TypeCode::I64),
    ];
    let partial = partial_over(&src, &[], &specs);
    // No group columns: the SUM lands at partial column 1, its companion at 2.
    let f = finish_of(
        &partial,
        &specs,
        &[],
        vec![(
            finalize_agg_bexpr(BExpr::ColRef(1), Some(BExpr::ColRef(2)), AggFunc::Avg),
            col_def("a", TypeCode::F64, true),
        )],
    );
    let reply = batch(
        &partial,
        &[(gnitz_wire::global_group_key(), &[Int(sum_bits), Int(cnt)])],
    );

    let got = f.apply(f.combine(reply));
    assert_eq!(got.len(), 1);
    (!gnitz_wire::null_word_get(got.nulls[0], 0))
        .then(|| f64::from_bits(u64::from_le_bytes(got.payload[0].bytes[..8].try_into().unwrap())))
}

/// AVG divides its SUM accumulator at the accumulator's declared type. A SUM
/// over a `BIGINT UNSIGNED` source is typed U64 on the partial schema, so past
/// 2^63 its i64 bit pattern only reads as the true sum unsigned — and it is that
/// declared type, not a switch in the finisher, that makes the divide unsigned.
#[test]
fn avg_divides_an_unsigned_sum_unsigned() {
    // One cell of 2^64 - 1: the accumulator holds -1, which is that sum only
    // when read unsigned. (2^64 - 1 has no exact f64 image; it rounds to 2^64.)
    assert_eq!(finish_avg(-1, 1), Some(1.8446744073709552e19));
}

/// A zero CountNonNull companion is AVG's NULL — an empty or all-NULL group.
/// The composite renders it by dividing by zero, so nothing has to special-case
/// it.
#[test]
fn avg_nulls_on_a_zero_count_companion() {
    assert_eq!(finish_avg(0, 0), None);
}
