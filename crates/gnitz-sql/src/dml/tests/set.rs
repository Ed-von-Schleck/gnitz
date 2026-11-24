use super::*;
use crate::test_support::Cell::{self, F64, Int, Null, Str};
use crate::test_support::{LONG, assert_rejects, batch_of, col, ncol, parse_stmt, rows_of, schema, typed_schema};
use sqlparser::ast::Statement;

/// The SET list `set` of `clause`, bound against `schema` under the alias `t`.
fn bound(schema: &Schema, set: &str, clause: SetClause) -> Result<Vec<SetCol>, GnitzSqlError> {
    let Statement::Update(u) = parse_stmt(&format!("UPDATE t SET {set}")) else {
        unreachable!("an UPDATE parses as one")
    };
    bind_set_list(&u.assignments, schema, "t", clause)
}

/// What `UPDATE t SET <set>` writes for the rows `held` of `schema`, each read at
/// weight 3.
fn updated<'a>(schema: &Schema, set: &str, held: &[impl AsRef<[Cell<'a>]>]) -> Result<ZSetBatch, GnitzSqlError> {
    let mut set = bound(schema, set, SetClause::Update)?;
    let mut rows = batch_of(schema, held);
    rows.fill_weights(3);
    apply_set(&mut set, rows, None)
}

const I: ColType = ColType::of(TypeCode::I64);
const F: ColType = ColType::of(TypeCode::F64);
const S: ColType = ColType::of(TypeCode::String);
const D2: ColType = ColType::decimal(2);
const D4: ColType = ColType::decimal(4);
const DATE: ColType = ColType::of(TypeCode::Date);
const TS: ColType = ColType::of(TypeCode::Timestamp);

/// Over `t (pk, c1, c2, …)` with `pk = 1`: every assigned column takes its value,
/// read off the row as it was; every other column, null bits included, is carried;
/// and the row is written at weight +1 whatever weight it was read at.
#[test]
fn a_set_list_rewrites_the_row_it_read() {
    /// `(payload types, SET list, row read, row written)`.
    type Case<'a> = (&'a [ColType], &'a str, &'a [Cell<'a>], &'a [Cell<'a>]);
    let cases: &[Case] = &[
        // A literal, through INSERT's cell encoder.
        (&[I, I], "c1 = 99", &[Null, Null], &[Int(99), Null]),
        (&[I, I], "c1 = NULL", &[Int(5), Int(6)], &[Null, Int(6)]),
        (&[F], "c1 = 1.5", &[Null], &[F64(1.5)]),
        // A column of the target's own type is copied, NULL and spill included.
        (&[I, I], "c1 = c2", &[Int(5), Null], &[Null, Null]),
        (&[I, I], "c1 = c2, c2 = c1", &[Int(1), Int(2)], &[Int(2), Int(1)]),
        (&[F, F], "c1 = c2", &[F64(0.0), F64(2.5)], &[F64(2.5), F64(2.5)]),
        (&[S, S], "c1 = c2", &[Str("old"), Str(LONG)], &[Str(LONG), Str(LONG)]),
        // A computed value; NULL in is NULL out, not the filler zero.
        (&[I, I], "c1 = c2 + 1", &[Int(0), Int(5)], &[Int(6), Int(5)]),
        (&[I, I], "c1 = c2 + 1", &[Int(0), Null], &[Null, Null]),
        (&[I], "c1 = pk + 1", &[Int(0)], &[Int(2)]),
        (&[I], "c1 = pk", &[Int(0)], &[Int(1)]),
        (
            &[S, S],
            "c1 = UPPER(c2)",
            &[Str("x"), Str("hello")],
            &[Str("HELLO"), Str("hello")],
        ),
        (&[S, S], "c1 = UPPER(c2)", &[Str("x"), Null], &[Null, Null]),
        // A source of another type converts as a CAST to the target does.
        (&[I, D2], "c1 = c2", &[Int(0), Int(150)], &[Int(2), Int(150)]),
        (&[I, D2], "c1 = c2", &[Int(0), Int(149)], &[Int(1), Int(149)]),
        (&[D2, I], "c1 = c2", &[Int(150), Int(5)], &[Int(500), Int(5)]),
        (&[D2, D4], "c1 = c2", &[Int(150), Int(12345)], &[Int(123), Int(12345)]),
        (
            &[DATE, TS],
            "c2 = c1",
            &[Int(2), Int(0)],
            &[Int(2), Int(172_800_000_000)],
        ),
        (&[DATE, TS], "c1 = c2", &[Int(1), Int(-1)], &[Int(-1), Int(-1)]),
    ];
    for (tys, set, before, after) in cases {
        let schema = typed_schema(tys);
        let out = updated(&schema, set, &[before]).unwrap_or_else(|e| panic!("{set}: {e:?}"));
        assert_eq!(rows_of(&out), [(after.to_vec(), 1)], "{set} over {before:?}");
    }
}

/// A 16-byte column has no register, so it is assignable only as a copy.
#[test]
fn a_uuid_column_is_copied() {
    let out = updated(&typed_schema(&[TypeCode::UUID; 2]), "c1 = c2", &[[Int(0), Int(-8)]]).unwrap();
    assert_eq!(out.view().col_data(0), (u128::MAX - 7).to_le_bytes());
}

/// A computed value outside its column's range is refused per row, not truncated
/// to the low bits; at either end of the range it is written at the column's own
/// width and sign.
#[test]
fn a_computed_value_is_range_checked_at_the_column_width() {
    use TypeCode::*;
    for dst in [U8, I8, U16, I16, U32, I32, U64, I64] {
        let (lo, hi) = FixedInt::from_type_code(dst).unwrap().range();
        // Each end of the range and its neighbour past it, where a 64-bit source holds one.
        let held = |v: i128| (i64::MIN as i128..=u64::MAX as i128).contains(&v);
        for v in [lo - 1, lo, hi, hi + 1].into_iter().filter(|&v| held(v)) {
            let src = if v > i64::MAX as i128 { U64 } else { I64 };
            let out = updated(&typed_schema(&[src, dst]), "c2 = c1 + 0", &[[Int(v), Null]]);
            match (lo..=hi).contains(&v) {
                true => assert_eq!(rows_of(&out.unwrap()), [(vec![Int(v), Int(v)], 1)], "{dst:?} {v}"),
                false => assert_rejects(&format!("{dst:?} {v}"), out, "out of range"),
            }
        }
    }
}

/// Every kind of value lands in its own row past the first morsel — a literal, a
/// copy and a computed value, a NULL among them — and a computed value out of range
/// is refused wherever it lies.
#[test]
fn a_set_list_is_written_across_morsels() {
    let schema = typed_schema(&[TypeCode::I64, TypeCode::U8, TypeCode::I64, TypeCode::String]);
    let set = format!("c2 = c1 + 0, c3 = c1, c4 = '{LONG}'");
    let src = |i: i128| if i % 97 == 5 { Null } else { Int(i % 256) };
    let mut held: Vec<[Cell; 4]> = (0..700).map(|i| [src(i), Null, Null, Str("x")]).collect();
    let want: Vec<_> = (0..700).map(|i| (vec![src(i), src(i), src(i), Str(LONG)], 1)).collect();
    assert_eq!(rows_of(&updated(&schema, &set, &held).unwrap()), want);
    held[650][0] = Int(300);
    assert_rejects("row 650", updated(&schema, &set, &held), "out of range: 300");
}

/// A NULL into a NOT NULL column is refused when any rewritten row holds one,
/// whichever way the value is produced.
#[test]
fn a_null_into_a_not_null_column_is_refused() {
    let schema = schema(
        vec![
            col("pk", TypeCode::U64),
            ncol("a", TypeCode::I64),
            col("nn", TypeCode::I64),
        ],
        &[0],
    );
    for set in ["nn = NULL", "nn = a", "nn = a + 1"] {
        assert_rejects(
            set,
            updated(&schema, set, &[&[Null, Int(1)]]),
            "column 'nn' violates NOT NULL",
        );
    }
}

/// A SET list no row could take is refused as it binds, under either clause: a
/// target that is not one plain name of a non-key column assigned once, and a
/// value its column cannot hold. Only DO UPDATE reads `EXCLUDED`, and only as a
/// whole value.
#[test]
fn a_set_list_no_row_could_take_is_refused() {
    use TypeCode::*;
    let schema = schema(
        vec![
            col("id", I64),
            col("v", I64),
            ncol("u8c", U8),
            ncol("n", I64),
            ncol("f", F64),
            ncol("s", String),
            ncol("u", UUID),
        ],
        &[0],
    );
    let either = [
        ("v = 1, v = 2", "multiple assignments to column 'v'"),
        ("t.v = 1", "simple identifier"),
        ("(v, s) = (1, 'x')", "simple identifier"),
        ("nope = 1", "'nope' not found"),
        ("v = x.v", "table alias 'x' not found"),
        ("u8c = 300", "out of range"),
        ("n = f", "floating-point"),
        ("f = f + 1.0", "floating-point"),
        ("f = n", "cannot assign a value of type BIGINT"),
        ("u = n", "cannot assign a value of type BIGINT"),
        ("n = UPPER(s)", "cannot assign a value of type TEXT"),
        ("s = n + 1", "cannot assign a value of type BIGINT"),
        ("n = f > 1.5", "cannot assign a value of type BOOLEAN"),
    ];
    let update = [("v = EXCLUDED.v", "table alias 'EXCLUDED' not found")];
    let do_update = [
        ("v = v + EXCLUDED.v", "compound expressions"),
        ("v = COALESCE(EXCLUDED.v, 0)", "compound expressions"),
        ("v = EXCLUDED.nope", "EXCLUDED: column 'nope' not found"),
        ("s = EXCLUDED.v", "cannot assign a value of type BIGINT"),
    ];
    for (clause, own) in [(SetClause::Update, &update[..]), (SetClause::DoUpdate, &do_update[..])] {
        for (set, needle) in either.iter().chain(own) {
            assert_rejects(set, bound(&schema, set, clause), needle);
        }
        let key = format!("cannot assign to primary key column in {}", clause.name());
        assert_rejects("id = 9", bound(&schema, "id = 9", clause), &key);
    }
}
