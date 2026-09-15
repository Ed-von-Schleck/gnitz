use super::*;
use crate::bind::bind_single_table;
use crate::codec::pk_codec::extract_pk_value;
use crate::ir::NumLit;
use crate::test_support::{
    bind_conjunct, bind_where, col_def, compound_schema_u64_u64, eq_expr, idx_metas, idx_metas_flagged, in_list_expr,
    neg_num_expr, num_expr, parse_expr_sql, pk_schema, two_col, uuid_schema_payload, uuid_schema_pk,
};
use gnitz_core::PkBuf;
use sqlparser::ast::Expr;

/// Bind against `schema` as relation `t` — the alias every qualified reference
/// in this file writes.
fn bind1(e: &Expr, schema: &gnitz_core::Schema) -> Result<crate::ir::BoundExpr, crate::error::GnitzSqlError> {
    bind_single_table(e, schema, "t")
}

/// `(id U64 pk, a tc, b tc)` — indexable cols a=1, b=2, `b` nullable per arg.
fn schema3(tc: TypeCode, b_nullable: bool) -> Schema {
    Schema {
        columns: vec![
            col_def("id", TypeCode::U64, false),
            col_def("a", tc, false),
            col_def("b", tc, b_nullable),
        ],
        pk_cols: vec![0],
    }
}

/// The classified terms of `conjuncts`.
fn terms_of(conjuncts: &[BoundExpr], sch: &Schema) -> Vec<Term> {
    conjuncts
        .iter()
        .enumerate()
        .filter_map(|(i, c)| term(c, sch).map(|(col, pin)| Term { conjunct: i, col, pin }))
        .collect()
}

/// The first index candidate for `conjuncts`: its declared column list, its
/// descriptor, and how many conjuncts stay residual.
fn first_index(
    conjuncts: &[BoundExpr],
    sch: &Schema,
    metas: &[IndexMeta],
) -> Option<(Vec<u32>, RangeDescriptor, usize)> {
    candidates(conjuncts, sch, metas)
        .into_iter()
        .find_map(|c| match c.bound {
            ReadBound::IndexRange { bound, .. } => Some((
                bound.idx_cols.as_slice().to_vec(),
                bound.desc,
                conjuncts.len() - c.consumed.len(),
            )),
            _ => None,
        })
}

/// The best index candidate for `where_sql`: the winning index's declared column
/// list and its descriptor. `lists` carries each index's `is_unique` flag.
fn picked_flagged(where_sql: &str, sch: &Schema, lists: &[(&[u32], bool)]) -> Option<(Vec<u32>, RangeDescriptor)> {
    let bound = bind_where(where_sql, sch);
    first_index(&bound, sch, &idx_metas_flagged(lists)).map(|(cols, desc, _)| (cols, desc))
}

/// [`picked_flagged`] over non-unique indexes.
fn picked(where_sql: &str, sch: &Schema, lists: &[&[u32]]) -> Option<(Vec<u32>, RangeDescriptor)> {
    let flagged: Vec<(&[u32], bool)> = lists.iter().map(|&cols| (cols, false)).collect();
    picked_flagged(where_sql, sch, &flagged)
}

/// The bound [`bound_column_list`] derives for the key list `cols`, plus its
/// residual — the one recognizer alone, without the candidate ordering around it.
fn bound_list_of<'e>(
    conjuncts: &'e [BoundExpr],
    cols: &[u32],
    sch: &Schema,
) -> Option<(RangeDescriptor, Vec<&'e BoundExpr>)> {
    let (desc, consumed) = bound_column_list(cols, &terms_of(conjuncts, sch), sch)?;
    Some((desc, residual(conjuncts, &consumed)))
}

/// The first PK candidate (`PkSet` or `PkRange`) for `conjuncts`, with how many
/// conjuncts stay residual.
fn pk_bound(conjuncts: &[BoundExpr], schema: &Schema) -> Option<(ReadBound, usize)> {
    candidates(conjuncts, schema, &[])
        .into_iter()
        .find(|c| matches!(c.bound, ReadBound::PkSet(_) | ReadBound::PkRange(_)))
        .map(|c| (c.bound, conjuncts.len() - c.consumed.len()))
}

/// The one key a WHERE names as a one-key `PkSet`, with its residual count.
fn pk_point_of(conjuncts: &[BoundExpr], schema: &Schema) -> Option<(Vec<u8>, usize)> {
    match pk_bound(conjuncts, schema)? {
        (ReadBound::PkSet(keys), residual) if keys.len() == 1 => Some((keys.as_bytes().to_vec(), residual)),
        _ => None,
    }
}

/// The `(column, key)` an equality conjunct pins.
fn eq_of(expr: &BoundExpr, schema: &Schema) -> Option<(usize, u128)> {
    match term(expr, schema)? {
        (col, Pin::Eq(key)) => Some((col, key)),
        _ => None,
    }
}

// ------------------------------------------------------------------
// Equality terms — UUID + signed/wide seek keys
// ------------------------------------------------------------------

#[test]
fn test_uuid_index_seek_string_literal() {
    let schema = uuid_schema_payload();
    let expr = bind_conjunct("uid = '550e8400-e29b-41d4-a716-446655440000'", &schema);
    assert_eq!(
        eq_of(&expr, &schema),
        Some((1, 0x550e8400_e29b_41d4_a716_446655440000_u128))
    );
}

/// The seek key `col = literal` packs, per column type: a negative signed
/// literal packs at the column's own width, an unsigned column takes none, and
/// a magnitude past `i64` (which binds as `LitWide`) still packs exactly.
#[test]
fn an_equality_packs_its_key_at_the_columns_width() {
    for (tc, sql, want) in [
        (TypeCode::I64, "val = -1", Some(((-1i64) as u64) as u128)),
        (TypeCode::I32, "val = -1", Some(((-1i32) as u32) as u128)),
        (TypeCode::I16, "val = -1", Some(((-1i16) as u16) as u128)),
        (TypeCode::I8, "val = -5", Some(((-5i8) as u8) as u128)),
        (TypeCode::I64, "val = 42", Some(42u128)),
        // An unsigned column holds no negative value.
        (TypeCode::U64, "val = -1", None),
        // u64::MAX overflows i64 → binds to `LitWide`; the recognizer parses it
        // byte-exactly for a U64 column (the servable wide index seek).
        (TypeCode::U64, "val = 18446744073709551615", Some(u64::MAX as u128)),
    ] {
        let schema = two_col(tc);
        let expr = bind_conjunct(sql, &schema);
        assert_eq!(eq_of(&expr, &schema), want.map(|key| (1, key)), "{tc:?}: {sql}");
    }
}

/// A single-quoted UUID binds as a servable seek literal; a double-quoted token
/// is an `Identifier` in `GenericDialect`, so binding it as a column reference
/// fails (no such column) — it is never a seek literal. The double-quote
/// guarantee moved from the AST recognizer to the parser + binder.
#[test]
fn double_quoted_uuid_binds_as_column_ref_not_seek() {
    let schema = uuid_schema_payload();
    let sq = bind1(&parse_expr_sql("uid = '550e8400-e29b-41d4-a716-446655440000'"), &schema).unwrap();
    assert!(eq_of(&sq, &schema).is_some(), "single-quoted UUID is a seek key");

    let err = bind1(
        &parse_expr_sql("uid = \"550e8400-e29b-41d4-a716-446655440000\""),
        &schema,
    )
    .expect_err("double-quoted token must not bind as a literal");
    assert!(matches!(err, crate::GnitzSqlError::Bind(_)), "got {err:?}");
}

/// A qualified reference resolves to the same column (the single-relation
/// leniency at bind), and the literal may sit on EITHER side of the `=` — the
/// mirrored arm of `bound_col_vs_literal` no other equality test reaches.
#[test]
fn a_flipped_qualified_equality_is_the_same_seek_key() {
    let schema = pk_schema(TypeCode::U64); // (id U64 pk, v I64)
    assert_eq!(eq_of(&bind_conjunct("5 = t.v", &schema), &schema), Some((1, 5)));
}

// ------------------------------------------------------------------
// The exact key a fully-pinned PK names
// ------------------------------------------------------------------

/// Which WHEREs over a compound PK name a single key, and which name only a
/// key *group* — a group must never restrict a caller's buffered rows to one
/// PK. The named key packs in pk-list order however the conjuncts were
/// spelled, and whatever does not bind the PK rides the residual.
#[test]
fn a_compound_pk_point_names_a_key_only_when_every_column_binds() {
    let schema = compound_schema_u64_u64();
    // OPK: each unsigned column big-endian, at its pk-list offset.
    let mut key_1_2 = [0u8; 16];
    key_1_2[..8].copy_from_slice(&1u64.to_be_bytes());
    key_1_2[8..16].copy_from_slice(&2u64.to_be_bytes());

    for (sql, want_residual) in [
        ("a = 1 AND b = 2", 0),
        // Order swapped; the tuple must still pack in pk-list order.
        ("b = 2 AND a = 1", 0),
        ("a = 1 AND b = 2 AND v = 9", 1),
        // A range-spelled point extends the equality prefix.
        ("a >= 1 AND a <= 1 AND b = 2", 0),
    ] {
        let expr = bind_where(sql, &schema);
        let (pk, residual) = pk_point_of(&expr, &schema).unwrap_or_else(|| panic!("{sql}: must bind"));
        assert_eq!(pk, &key_1_2[..], "{sql}");
        assert_eq!(residual, want_residual, "{sql}: residual");
    }

    for (sql, want_residual) in [
        // A bare prefix: the worker still walks the `a = 1` group.
        ("a = 1", 0),
        // `a` binds, `v` is a payload conjunct → residual; the PK stays incomplete.
        ("a = 1 AND v = 9", 1),
    ] {
        let expr = bind_where(sql, &schema);
        let (bound, residual) = pk_bound(&expr, &schema).unwrap_or_else(|| panic!("{sql}: still bounds"));
        assert_eq!(bound, ReadBound::PkRange(RangeDescriptor::point(&[], 1)), "{sql}");
        assert_eq!(residual, want_residual, "{sql}: residual");
    }

    // Contradictory pins fold to the inverted interval on `a`, which names no key
    // and returns nothing.
    let expr = bind_where("a = 1 AND a = 2", &schema);
    let (bound, residual) = pk_bound(&expr, &schema).expect("still bounds");
    assert_eq!(
        bound,
        ReadBound::PkRange(RangeDescriptor::new(&[], Cut::Before(2), Cut::After(1)))
    );
    assert_eq!(residual, 0);
}

/// A single-column PK point names one OPK key however it is spelled, consuming
/// every conjunct; an open range names no key.
#[test]
fn a_single_pk_point_is_the_same_key_however_it_is_spelled() {
    let schema = pk_schema(TypeCode::U64);
    for sql in ["id = 5", "id >= 5 AND id <= 5"] {
        let expr = bind_where(sql, &schema);
        let (pk, residual) = pk_point_of(&expr, &schema).unwrap_or_else(|| panic!("{sql}: must bind"));
        assert_eq!(pk, &5u64.to_be_bytes()[..], "{sql}");
        assert_eq!(residual, 0, "{sql}: every conjunct is consumed by the walk");
    }
    let with_payload = bind_where("id = 1 AND v = 9", &schema);
    let (pk, residual) = pk_point_of(&with_payload, &schema).expect("PK binds");
    assert_eq!(pk, &1u64.to_be_bytes()[..]);
    assert_eq!(residual, 1, "the non-PK conjunct rides the residual");
    assert!(pk_point_of(&bind_where("id > 5", &schema), &schema).is_none());

    let qualified = bind_where("t.id = 1", &schema);
    let (pk, residual) = pk_point_of(&qualified, &schema).expect("a qualified PK binds");
    assert_eq!(pk, &1u64.to_be_bytes()[..]);
    assert_eq!(residual, 0);
}

/// Redundant ends on a U128 PK fold into one exact interval, so no conjunct is
/// left for the expression VM, which has no 16-byte slot.
#[test]
fn redundant_wide_pk_ends_consume_every_conjunct() {
    let schema = pk_schema(TypeCode::U128);
    let expr = bind_where("id > 5 AND id > 7", &schema);
    let (bound, residual) = pk_bound(&expr, &schema).expect("bounds");
    let ReadBound::PkRange(desc) = bound else {
        panic!("expected a PK range, got {bound:?}");
    };
    assert_eq!((desc.start, desc.end), (Cut::After(7), Cut::After(u128::MAX)));
    assert_eq!(residual, 0);
}

// ------------------------------------------------------------------
// What a column list matches at all
// ------------------------------------------------------------------

/// `WHERE val = 1` on a float column bounds nothing: a type that has no packed
/// key is no term, so no index can serve it.
#[test]
fn a_float_column_is_never_an_index_key() {
    let schema = two_col(TypeCode::F64);
    let expr = bind_where("val = 1", &schema);
    assert!(candidates(&expr, &schema, &idx_metas(&[&[1]])).is_empty());
}

/// The whole AND-tree is walked. `a = 1 AND b = 2 AND c = 3` binds as
/// `((a = 1 AND b = 2) AND c = 3)`, so finding the leftmost-deepest conjunct
/// and the rightmost one — each against an index that names only its column —
/// proves every leaf is reached.
#[test]
fn every_leaf_of_the_and_tree_is_reached() {
    let schema = Schema {
        columns: vec![
            col_def("pk", TypeCode::U64, false),
            col_def("a", TypeCode::U64, true),
            col_def("b", TypeCode::U64, true),
            col_def("c", TypeCode::U64, true),
        ],
        pk_cols: vec![0],
    };
    let expr = bind_where("a = 1 AND b = 2 AND c = 3", &schema);
    for (col, key) in [(1u32, 1u128), (3, 3)] {
        let cands = candidates(&expr, &schema, &idx_metas(&[&[col]]));
        assert_eq!(cands.len(), 1, "INDEX({col})");
        let (_, desc, residual) = first_index(&expr, &schema, &idx_metas(&[&[col]])).unwrap();
        assert_eq!(desc, RangeDescriptor::point(&[], key), "INDEX({col})");
        assert_eq!(residual, 2, "INDEX({col})");
    }
}

/// `(id U64 pk, x U64, a U64, b U64, c U64)` — every payload col non-nullable,
/// so no candidate is dropped by the nullable trailing-column guard.
fn seek_rank_schema() -> Schema {
    Schema {
        columns: vec![
            col_def("id", TypeCode::U64, false),
            col_def("x", TypeCode::U64, false),
            col_def("a", TypeCode::U64, false),
            col_def("b", TypeCode::U64, false),
            col_def("c", TypeCode::U64, false),
        ],
        pk_cols: vec![0],
    }
}

/// A point covering every column of UNIQUE(x) selects at most one row, so it
/// outranks the longer — but non-unique, and disjoint — prefix of INDEX(a, b).
#[test]
fn a_covered_unique_index_outranks_a_longer_disjoint_prefix() {
    let (cols, _) = picked_flagged(
        "x = 1 AND a = 2 AND b = 3",
        &seek_rank_schema(),
        &[(&[1], true), (&[2, 3], false)],
    )
    .expect("some index must bound");
    assert_eq!(cols, vec![1], "UNIQUE(x) admits one row; INDEX(a, b) admits many");
}

/// With no candidate fully covered the leading term is false for both, so the
/// rest of the rank decides: equal pinned depth, then the tighter index.
/// (`seek_rank_schema`'s payload columns are all non-nullable, so the
/// uncovered trailing column of each index does not reject it outright.)
#[test]
fn a_partial_cover_of_a_unique_index_does_not_jump_the_queue() {
    let (cols, _) = picked_flagged("a = 1", &seek_rank_schema(), &[(&[2, 3], true), (&[2, 3, 4], false)])
        .expect("some index must bound");
    assert_eq!(cols, vec![2, 3], "a prefix point on UNIQUE(a, b) is not itself unique");
}

// ------------------------------------------------------------------
// The PK key set
// ------------------------------------------------------------------

/// The gather keys of `sql`, decoded to native values.
fn pk_in_keys_of(sql: &str, schema: &Schema) -> Option<Vec<u128>> {
    set_keys_of(&bind_where(sql, schema), schema)
}

/// The first `PkSet` candidate's keys, decoded to native values.
fn set_keys_of(conjuncts: &[BoundExpr], schema: &Schema) -> Option<Vec<u128>> {
    candidates(conjuncts, schema, &[])
        .into_iter()
        .find_map(|c| match c.bound {
            ReadBound::PkSet(keys) => Some(natives(schema, &keys)),
            _ => None,
        })
}

/// A gather's OPK keys decoded back to native values, in key order.
fn natives(schema: &Schema, keys: &PkKeys) -> Vec<u128> {
    keys.iter()
        .map(|k| u128::from_le_bytes(gnitz_core::native_le_key(schema, k)[..16].try_into().unwrap()))
        .collect()
}

/// A list gather needs every PK column: a list on a PK prefix names no key, a
/// companion point completes it, and a one-key list is the point it folds to.
#[test]
fn a_list_gather_needs_every_pk_column() {
    let compound = compound_schema_u64_u64();
    assert!(pk_in_keys_of("a IN (1, 2)", &compound).is_none());
    // Native images pack `a` in the low 8 bytes and `b` above them.
    let key = |a: u128, b: u128| a | (b << 64);
    assert_eq!(
        pk_in_keys_of("a IN (7, 8) AND b = 3", &compound),
        Some(vec![key(7, 3), key(8, 3)])
    );
    // A repeat is no second key.
    assert_eq!(
        pk_in_keys_of("a IN (1, 1, 2) AND b = 3", &compound),
        Some(vec![key(1, 3), key(2, 3)])
    );
    assert_eq!(pk_in_keys_of("id IN (7)", &pk_schema(TypeCode::U64)), Some(vec![7]));
}

/// Two lists cross into a product exponential in the SQL text, so one past a
/// request's worth of keys gathers nothing.
#[test]
fn a_list_product_past_one_request_is_no_set() {
    let schema = compound_schema_u64_u64();
    let list = |col: usize| BoundExpr::InList {
        inner: Box::new(BoundExpr::ColRef(col)),
        items: (0..300).map(BoundExpr::LitInt).collect(),
    };
    assert!(300 * 300 > PkKeys::max_per_request(schema.pk_stride()));
    assert!(set_keys_of(&[list(0), list(1)], &schema).is_none());
}

#[test]
fn a_uuid_string_list_gathers() {
    assert_eq!(
        pk_in_keys_of(
            "id IN ('550e8400-e29b-41d4-a716-446655440000', '6ba7b810-9dad-11d1-80b4-00c04fd430c8')",
            &uuid_schema_pk()
        ),
        Some(vec![
            0x550e8400_e29b_41d4_a716_446655440000,
            0x6ba7b810_9dad_11d1_80b4_00c04fd430c8
        ])
    );
}

#[test]
fn an_invalid_uuid_in_a_list_gathers_nothing() {
    assert!(
        pk_in_keys_of(
            "id IN ('550e8400-e29b-41d4-a716-446655440000', 'not-a-uuid')",
            &uuid_schema_pk()
        )
        .is_none(),
        "invalid UUID in list should fall back to slow scan"
    );
}

#[test]
fn a_negative_i32_list_gathers_in_key_order() {
    assert_eq!(
        pk_in_keys_of("id IN (-1, -2)", &pk_schema(TypeCode::I32)),
        // In key order: the gather sorts its keys, and -2 sorts first.
        Some(vec![(-2i32 as u32) as u128, (-1i32 as u32) as u128])
    );
}

/// A list inside an AND-tree still gathers, its companion conjunct left residual.
#[test]
fn pk_in_inside_an_and_tree_gathers_with_a_residual() {
    let schema = pk_schema(TypeCode::U64);
    let expr = bind_where("v > 5 AND id IN (7, 9)", &schema);
    let (bound, residual) = pk_bound(&expr, &schema).expect("the IN conjunct bounds the gather");
    let ReadBound::PkSet(keys) = bound else {
        panic!("expected a gather, got {bound:?}");
    };
    assert_eq!(natives(&schema, &keys), vec![7, 9]);
    assert_eq!(residual, 1, "`v > 5` stays residual");
    assert_eq!(pk_in_keys_of("t.id IN (1, 2)", &schema), Some(vec![1, 2]));
}

// ------------------------------------------------------------------
// A one-element IN list is the equality it spells. `bind::structural` pins the
// fold itself.
// ------------------------------------------------------------------

/// On `PRIMARY KEY (a, b)`, `a IN (7) AND b = 3` names the whole key.
#[test]
fn one_key_in_list_names_the_pk_key() {
    let schema = compound_schema_u64_u64();
    let expr = bind_where("a IN (7) AND b = 3", &schema);
    let (pk, residual) = pk_point_of(&expr, &schema).expect("one-key IN names a key");
    assert_eq!(pk, gnitz_core::opk_key_cols(&schema, [7, 3]).pk_bytes());
    assert_eq!(residual, 0);
}

/// On a U128 column the index bound must consume the conjunct outright — nothing
/// may reach the expression VM, which has no 16-byte slot for the OR-chain a
/// list lowers to.
#[test]
fn one_key_in_list_takes_an_index_bound() {
    let schema = two_col(TypeCode::U128);
    let expr = bind_where("val IN (7)", &schema);
    let (_, desc, residual) =
        first_index(&expr, &schema, &idx_metas(&[&[1]])).expect("one-key IN must take an index bound");
    assert_eq!(desc, RangeDescriptor::point(&[], 7));
    assert_eq!(residual, 0, "the bound consumes the conjunct");
}

// ------------------------------------------------------------------
// Routing parity: extract_pk_value (INSERT, AST) / the equality term / the PK key
// set (bound) must agree byte-for-byte on the packed u128.
// ------------------------------------------------------------------

fn check_pk_parity(pk_tc: TypeCode, literal: Expr, expected: u128) {
    let schema = pk_schema(pk_tc);

    // 1. extract_pk_value (INSERT row) — pk_codec, AST-based.
    let row = vec![literal.clone(), num_expr("0")];
    let got_insert: PkBuf =
        extract_pk_value(&row, &schema).unwrap_or_else(|e| panic!("extract_pk_value({pk_tc:?}): {e}"));
    assert_eq!(
        got_insert,
        gnitz_core::opk_key_cols(&schema, [expected]),
        "extract_pk_value"
    );

    // 2. The equality term (bound WHERE pk = literal).
    let eq = bind1(&eq_expr("id", literal.clone()), &schema).expect("bind eq");
    assert_eq!(eq_of(&eq, &schema), Some((0, expected)), "equality term");

    // 3. The key set — the repeat keeps it an `InList` (a one-item list folds to
    // the `Eq` leg 2 already covers); the dedup collapses it to one key.
    let in_e = bind1(&in_list_expr("id", vec![literal.clone(), literal]), &schema).expect("bind in");
    assert_eq!(set_keys_of(&[in_e], &schema), Some(vec![expected]), "key set");
}

#[test]
fn every_pk_type_routes_to_one_packed_key() {
    for (tc, literal, expected) in [
        (TypeCode::I8, neg_num_expr("1"), (-1i8 as u8) as u128),
        (TypeCode::I16, neg_num_expr("1"), (-1i16 as u16) as u128),
        (TypeCode::I32, neg_num_expr("1"), (-1i32 as u32) as u128),
        (TypeCode::I64, neg_num_expr("1"), ((-1i64) as u64) as u128),
        // The prepend-`-` parse rule: the `i64::MIN` magnitude overflows i64 →
        // `LitWide`, and every path parses it byte-exactly.
        (
            TypeCode::I64,
            neg_num_expr("9223372036854775808"),
            (i64::MIN as u64) as u128,
        ),
        (TypeCode::U16, num_expr("65535"), 65535u128),
        (TypeCode::U32, num_expr("4294967295"), 4294967295u128),
        // u64::MAX binds to `LitWide` for the WHERE seeks; INSERT parses it directly.
        (TypeCode::U64, num_expr("18446744073709551615"), u64::MAX as u128),
        // `-0` names the value `0`: the sign carries no information for an
        // integer, so an unsigned column takes it on every path. `-1` does not
        // (`extract_pk_value_u128_rejects_negative`).
        (TypeCode::U128, neg_num_expr("0"), 0),
        (TypeCode::UUID, neg_num_expr("0"), 0),
    ] {
        check_pk_parity(tc, literal, expected);
    }
}

// ------------------------------------------------------------------
// Range terms
// ------------------------------------------------------------------

#[test]
fn range_cut_saturates() {
    use Cut::{After, Before};
    let ck = |tc, s: &str, neg, mk: fn(u128) -> Cut| {
        range_cut(
            BoundLit::Num(NumLit {
                mag: s.parse().expect("a digit run"),
                neg,
            }),
            tc,
            mk,
        )
    };
    assert_eq!(ck(TypeCode::I32, "5", false, Before), Some(Before(5)));
    assert_eq!(ck(TypeCode::I32, "5", false, After), Some(After(5)));
    assert_eq!(
        ck(TypeCode::I32, "5", true, Before),
        Some(Before((-5i32 as u32) as u128))
    );
    let (min, max) = ((i32::MIN as u32) as u128, i32::MAX as u128);
    assert_eq!(ck(TypeCode::I32, "3000000000", false, Before), Some(After(max)));
    assert_eq!(ck(TypeCode::I32, "3000000000", false, After), Some(After(max)));
    assert_eq!(ck(TypeCode::I32, "3000000000", true, Before), Some(Before(min)));
    assert_eq!(ck(TypeCode::I32, "3000000000", true, After), Some(Before(min)));
    // A magnitude past `i128` saturates on its sign too.
    assert_eq!(
        ck(TypeCode::I32, "340282366920938463463374607431768211455", true, After),
        Some(Before(min))
    );
    assert_eq!(ck(TypeCode::U8, "300", false, Before), Some(After(255)));
    assert_eq!(ck(TypeCode::String, "5", false, Before), None);
    // U128 takes the full unsigned range, and a negative saturates to its floor.
    assert_eq!(
        ck(TypeCode::U128, "340282366920938463463374607431768211455", false, Before),
        Some(Before(u128::MAX))
    );
    assert_eq!(ck(TypeCode::U128, "5", true, Before), Some(Before(0)));
}

#[test]
fn range_term_orientations() {
    use Cut::{After, Before};
    let schema = two_col(TypeCode::I64);
    let ck = |sql: &str| match term(&bind_conjunct(sql, &schema), &schema) {
        Some((col, Pin::Start(c))) => Some((col, "start", c)),
        Some((col, Pin::End(c))) => Some((col, "end", c)),
        _ => None,
    };
    assert_eq!(ck("val > 5"), Some((1, "start", After(5))));
    assert_eq!(ck("5 < val"), Some((1, "start", After(5))));
    assert_eq!(ck("val <= 5"), Some((1, "end", After(5))));
    assert_eq!(ck("5 >= val"), Some((1, "end", After(5))));
    assert_eq!(ck("val >= 5"), Some((1, "start", Before(5))));
    assert_eq!(ck("val < 5"), Some((1, "end", Before(5))));
    // Equality is not a range end.
    assert!(ck("val = 5").is_none());
}

// ── bound_column_list: the one recognizer, over one key list ────────────────

#[test]
fn a_composite_eq_prefix_pins_and_then_bounds() {
    use Cut::Before;
    let schema = schema3(TypeCode::U64, false);
    let expr = bind_where("a = 7 AND b < 50", &schema);
    let (desc, residual) = bound_list_of(&expr, &[1, 2], &schema).expect("the eq prefix + range bounds");
    assert_eq!(desc.eq_vals(), &[7u128]);
    assert_eq!(desc.start, Before(0));
    assert_eq!(desc.end, Before(50));
    assert!(residual.is_empty(), "both conjuncts consumed");
}

#[test]
fn between_desugars_to_two_ends_and_not_between_to_none() {
    use Cut::{After, Before};
    let schema = two_col(TypeCode::I64);
    // BETWEEN desugars at bind to `val >= 10 AND val <= 20` → two consumed ends.
    let expr = bind_where("val BETWEEN 10 AND 20", &schema);
    let (desc, residual) = bound_list_of(&expr, &[1], &schema).expect("BETWEEN bounds");
    assert_eq!((desc.start, desc.end), (Before(10), After(20)));
    assert!(residual.is_empty());

    // NOT BETWEEN binds to `Not(val >= 10 AND val <= 20)` — one leaf conjunct,
    // no range end → nothing bounds.
    let expr = bind_where("val NOT BETWEEN 10 AND 20", &schema);
    assert!(bound_list_of(&expr, &[1], &schema).is_none());
}

/// Every pin on a column intersects into one interval, and all of them are
/// consumed: the intersection is exact, so nothing is left to re-impose.
#[test]
fn redundant_pins_fold_to_their_intersection() {
    let schema = two_col(TypeCode::U64);
    for sql in ["val > 5 AND val > 10", "val > 10 AND val > 5"] {
        let expr = bind_where(sql, &schema);
        let (desc, residual) = bound_list_of(&expr, &[1], &schema).unwrap_or_else(|| panic!("{sql}: bounds"));
        assert_eq!(desc.start, Cut::After(10), "{sql}");
        assert!(residual.is_empty(), "{sql}");
    }
    let expr = bind_where("val = 5 AND val > 3", &schema);
    let (desc, residual) = bound_list_of(&expr, &[1], &schema).expect("bounds");
    assert_eq!(desc, RangeDescriptor::point(&[], 5));
    assert!(residual.is_empty());
}

/// Signed values fold in signed order, not in the order of their packed images.
#[test]
fn a_signed_column_folds_in_signed_order() {
    let schema = two_col(TypeCode::I32);
    let expr = bind_where("val > -5 AND val > 3", &schema);
    let (desc, _) = bound_list_of(&expr, &[1], &schema).expect("bounds");
    assert_eq!(desc.start, Cut::After(3));
    let expr = bind_where("val < -5 AND val < 3", &schema);
    let (desc, _) = bound_list_of(&expr, &[1], &schema).expect("bounds");
    assert_eq!(desc.end, Cut::Before((-5i32 as u32) as u128));
}

#[test]
fn an_out_of_range_end_saturates_to_the_type_edge() {
    use Cut::{After, Before};
    let schema = two_col(TypeCode::I32);
    let (min, max) = ((i32::MIN as u32) as u128, i32::MAX as u128);

    let expr = bind_where("val > 3000000000", &schema);
    let (desc, _) = bound_list_of(&expr, &[1], &schema).expect("a saturated end still bounds");
    assert_eq!((desc.start, desc.end), (After(max), After(max)));

    let expr = bind_where("val < 3000000000", &schema);
    let (desc, _) = bound_list_of(&expr, &[1], &schema).expect("a saturated end still bounds");
    assert_eq!((desc.start, desc.end), (Before(min), After(max)));
}

/// A string range end on a DATE column is a key through the same temporal rule
/// its equality takes.
#[test]
fn a_date_string_range_end_bounds_its_index() {
    let schema = Schema {
        columns: vec![col_def("id", TypeCode::U64, false), col_def("d", TypeCode::Date, false)],
        pk_cols: vec![0],
    };
    let (cols, desc) = picked("d >= '2024-01-01'", &schema, &[&[1]]).expect("the string end bounds INDEX(d)");
    assert_eq!(cols, vec![1]);
    assert_eq!(desc.start, Cut::Before(19723));
}

#[test]
fn a_conjunct_the_key_list_does_not_name_stays_residual() {
    use Cut::After;
    let schema = schema3(TypeCode::U64, false);
    // The list names `b` only, so `a = 7` is not consumed.
    let expr = bind_where("b > 10 AND a = 7", &schema);
    let (desc, residual) = bound_list_of(&expr, &[2], &schema).expect("`b > 10` bounds INDEX(b)");
    assert!(desc.eq_vals().is_empty());
    assert_eq!(desc.start, After(10));
    assert_eq!(residual.len(), 1, "`a = 7` is a residual conjunct");
}

// ── candidates: the whole-WHERE arbitration ─────────────────────────────────
//
// These pin the *chosen* bound, not just the recognizer above: which candidate
// wins across shapes and across separate indexes.

/// A pure equality on a 1-column index lowers to a degenerate point range.
#[test]
fn equality_lowers_to_a_degenerate_point_range() {
    let (idx_cols, desc) =
        picked("a = 5", &schema3(TypeCode::U64, false), &[&[1]]).expect("a = 5 on an index over `a` must bound");
    assert_eq!(idx_cols, vec![1]);
    assert_eq!(desc, RangeDescriptor::point(&[], 5));
}

/// A two-column equality pins the leading column and points at the last.
#[test]
fn compound_equality_pins_the_leading_column() {
    let b = picked("a = 5 AND b = 7", &schema3(TypeCode::U64, false), &[&[1, 2]]);
    let (idx_cols, desc) = b.expect("a compound equality must bound the compound index");
    assert_eq!(idx_cols, vec![1, 2]);
    assert_eq!(desc, RangeDescriptor::point(&[5], 7));
}

/// An equality prefix plus a range on ONE index takes the range.
#[test]
fn equality_prefix_plus_range_takes_the_range_candidate() {
    let b = picked("a = 5 AND b > 10", &schema3(TypeCode::U64, false), &[&[1, 2]]);
    let (_, desc) = b.expect("an eq-prefix + range must bound");
    assert_eq!(desc.eq_vals(), &[5u128], "`a` is the pinned prefix");
    assert_eq!(desc.start, Cut::After(10), "`b > 10` is an exclusive lower cut");
    assert_ne!(desc.end, Cut::After(10), "the upper side stays open, not a point");
}

/// Across SEPARATE indexes, most-pinned wins.
#[test]
fn point_on_one_index_beats_half_open_range_on_another() {
    let b = picked("a = 5 AND b > 10", &schema3(TypeCode::U64, false), &[&[1], &[2]]);
    let (idx_cols, desc) = b.expect("the point candidate must bound");
    assert_eq!(idx_cols, vec![1], "INDEX(a)'s point beats INDEX(b)'s range");
    assert_eq!(desc, RangeDescriptor::point(&[], 5));
}

/// A PK column is an ordinary index column: an equality on it bounds nothing
/// against an index that does not name it, and points one that does.
#[test]
fn a_pk_equality_bounds_the_index_that_names_it() {
    let sch = schema3(TypeCode::U64, false);
    assert!(picked("id = 5", &sch, &[&[1]]).is_none());
    let (idx_cols, desc) = picked("id = 5", &sch, &[&[0]]).expect("INDEX(id) is bounded by `id = 5`");
    assert_eq!(idx_cols, vec![0]);
    assert_eq!(desc, RangeDescriptor::point(&[], 5));
}

/// An index over a PK column of a COMPOUND PK is the shape no PK candidate can
/// serve: nothing pins the leading PK column, so only the index bounds.
#[test]
fn an_index_over_a_compound_pk_column_bounds_like_any_other() {
    let sch = Schema {
        columns: vec![
            col_def("a", TypeCode::U64, false),
            col_def("b", TypeCode::U64, false),
            col_def("v", TypeCode::U64, false),
        ],
        pk_cols: vec![0, 1],
    };
    for sql in ["b = 1 AND v = 2", "b > 5"] {
        assert!(
            pk_bound(&bind_where(sql, &sch), &sch).is_none(),
            "{sql}: nothing pins the leading PK column"
        );
    }
    let (idx_cols, desc) = picked("b = 1 AND v = 2", &sch, &[&[1, 2]]).expect("INDEX(b, v) is fully pinned");
    assert_eq!(idx_cols, vec![1, 2]);
    assert_eq!(desc, RangeDescriptor::point(&[1], 2));

    let (_, desc) = picked("b > 5", &sch, &[&[1, 2]]).expect("a range on a PK column bounds the index");
    assert_eq!(desc.start, Cut::After(5));
}

/// An uncovered NULLABLE trailing index column must NOT bound.
#[test]
fn uncovered_nullable_trailing_column_never_bounds() {
    assert!(picked("a = 5", &schema3(TypeCode::U64, true), &[&[1, 2]]).is_none());
    assert!(picked("a = 5", &schema3(TypeCode::U64, false), &[&[1, 2]]).is_some());
}

/// A column no index covers bounds nothing, and neither does a WHERE with no
/// `col OP literal` conjunct at all.
#[test]
fn unindexed_column_bounds_nothing() {
    assert!(picked("a = 5", &schema3(TypeCode::U64, false), &[&[2]]).is_none());
    assert!(picked("a + b > 3", &schema3(TypeCode::U64, false), &[&[1]]).is_none());
}

/// A BETWEEN is a two-sided range over one column (desugared at bind).
#[test]
fn between_bounds_both_sides() {
    let b = picked("a BETWEEN 5 AND 9", &schema3(TypeCode::U64, false), &[&[1]]);
    let (_, desc) = b.expect("BETWEEN must bound");
    assert_eq!(desc.eq_vals(), &[] as &[u128]);
    assert_eq!((desc.start, desc.end), (Cut::Before(5), Cut::After(9)));
}

/// A UUID index column: `Cut::type_edges(UUID)` is `None`, so an equality on
/// one must be ranked without ever consulting the column's edges. Such a
/// predicate has no index-free plan — the VM has no 128-bit register — so the
/// bound must consume the conjunct outright.
#[test]
fn a_uuid_index_equality_bounds_a_point() {
    let schema = uuid_schema_payload();
    let expr = bind_where("uid = '550e8400-e29b-41d4-a716-446655440000'", &schema);
    let metas = idx_metas(&[&[1]]);
    assert_eq!(candidates(&expr, &schema, &metas).len(), 1);
    let (_, desc, residual) = first_index(&expr, &schema, &metas).unwrap();
    assert_eq!(
        desc,
        RangeDescriptor::point(&[], 0x550e8400_e29b_41d4_a716_446655440000_u128)
    );
    assert_eq!(residual, 0);
}

// ── the rank: a point outranks an interval at equal depth ───────────────────

/// A full point on a UNIQUE index beats a two-sided interval on another index,
/// on a signed column and on an unsigned one (`a = 0`, whose cuts coincide with
/// `type_edges(U64)`).
#[test]
fn full_unique_point_beats_an_interval() {
    for (tc, pt) in [(TypeCode::U64, "5"), (TypeCode::I64, "5")] {
        let sch = schema3(tc, false);
        let where_sql = format!("a = {pt} AND b BETWEEN 1 AND 9");
        let (cols, desc) = picked_flagged(&where_sql, &sch, &[(&[1], true), (&[2], false)]).expect("must bound");
        assert_eq!(cols, vec![1], "{tc:?}: the unique point on `a` must win");
        assert_eq!((desc.start, desc.end), (Cut::Before(5), Cut::After(5)));
    }
}

/// The same at both type edges of both signednesses.
#[test]
fn full_unique_point_wins_at_the_type_edges() {
    for (tc, lit) in [
        (TypeCode::U64, "0"),
        (TypeCode::U64, "18446744073709551615"),
        (TypeCode::I64, "-9223372036854775808"),
        (TypeCode::I64, "9223372036854775807"),
    ] {
        let sch = schema3(tc, false);
        let where_sql = format!("a = {lit} AND b BETWEEN 1 AND 9");
        let (cols, _) = picked_flagged(&where_sql, &sch, &[(&[1], true), (&[2], false)]).expect("must bound");
        assert_eq!(cols, vec![1], "{tc:?} `a = {lit}` must win");
    }
}

/// Uniqueness is not what decides it: at equal pinned depth a point outranks a
/// two-sided interval outright, because it pins its column where the interval
/// only narrows one.
#[test]
fn a_point_outranks_an_interval_at_equal_depth() {
    let sch = schema3(TypeCode::I64, false);
    let (cols, _) = picked("a = 5 AND b BETWEEN 1 AND 9", &sch, &[&[1], &[2]]).expect("some index must bound");
    assert_eq!(cols, vec![1], "the point on `a` admits one group, the interval nine");
}

/// A point on a PREFIX of a UNIQUE index is not a unique point — it leaves the
/// index's trailing column free — but it still pins one column, which outranks
/// the interval's none.
#[test]
fn a_partial_prefix_of_a_unique_index_is_not_a_unique_point() {
    // `(id U64 pk, a I64, b I64, c I64)` — `b` non-nullable, or the partial
    // candidate is rejected outright by the nullable trailing-column guard.
    let sch = Schema {
        columns: vec![
            col_def("id", TypeCode::U64, false),
            col_def("a", TypeCode::I64, false),
            col_def("b", TypeCode::I64, false),
            col_def("c", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    };
    let (cols, desc) =
        picked_flagged("a = 5 AND c BETWEEN 1 AND 9", &sch, &[(&[1, 2], true), (&[3], false)]).expect("bounds");
    assert_eq!(cols, vec![1, 2]);
    assert!(
        !desc.pins_all(2),
        "a prefix point on UNIQUE(a, b) selects more than one row"
    );
}

/// The uniqueness lookup is not equality-only: a degenerate point the range
/// ends fold to (`a >= 5 AND a <= 5`) is scored unique too.
#[test]
fn a_range_spelled_point_is_scored_unique() {
    let sch = schema3(TypeCode::I64, false);
    let (cols, desc) = picked_flagged(
        "a >= 5 AND a <= 5 AND b BETWEEN 1 AND 9",
        &sch,
        &[(&[1], true), (&[2], false)],
    )
    .expect("must bound");
    assert_eq!(cols, vec![1]);
    assert_eq!((desc.start, desc.end), (Cut::Before(5), Cut::After(5)));
}

/// The two spellings of one point rank identically. `0` is
/// `type_edges(U64).0`, so scoring a point by comparing its cuts against the
/// type edges would have ranked `a = 0` below the interval it must beat, and
/// split the two spellings apart.
#[test]
fn a_point_ranks_the_same_however_it_is_spelled() {
    let sch = schema3(TypeCode::U64, false);
    for sql in ["a = 0 AND b BETWEEN 1 AND 9", "a >= 0 AND a <= 0 AND b BETWEEN 1 AND 9"] {
        let (cols, desc) = picked(sql, &sch, &[&[1], &[2]]).unwrap_or_else(|| panic!("{sql}: must bound"));
        assert_eq!(cols, vec![1], "{sql}");
        assert_eq!(desc, RangeDescriptor::point(&[], 0), "{sql}");
    }
}

// ── where the PK range sits among the index candidates ──────────────────────

fn kinds(conjuncts: &[BoundExpr], sch: &Schema, lists: &[(&[u32], bool)]) -> Vec<&'static str> {
    candidates(conjuncts, sch, &idx_metas_flagged(lists))
        .iter()
        .map(|c| match c.bound {
            ReadBound::None => "None",
            ReadBound::PkRange(_) => "PkRange",
            ReadBound::IndexRange { .. } => "IndexRange",
            ReadBound::PkSet(_) => "PkSet",
        })
        .collect()
}

/// An unpinned PK range yields only to a full unique point; a pinned one keeps
/// its place ahead of every index.
#[test]
fn an_unpinned_pk_range_yields_only_to_a_full_unique_point() {
    let sch = schema3(TypeCode::U64, false);
    let expr = bind_where("id > 0 AND a = 42", &sch);
    assert_eq!(kinds(&expr, &sch, &[(&[1], true)]), ["IndexRange", "PkRange"]);
    assert_eq!(kinds(&expr, &sch, &[(&[1], false)]), ["PkRange", "IndexRange"]);
    let expr = bind_where("id = 7 AND a = 42", &sch);
    assert_eq!(kinds(&expr, &sch, &[(&[1], true)]), ["PkSet", "IndexRange"]);
}

/// A PK range whose cuts sit on its column's type edges bounds nothing, so it
/// falls behind every index — but stays a candidate.
#[test]
fn a_pk_range_bounding_nothing_falls_behind_every_index() {
    let sch = two_col(TypeCode::U64);
    let expr = bind_where("pk >= 0 AND val = 7", &sch);
    assert_eq!(kinds(&expr, &sch, &[(&[1], false)]), ["IndexRange", "PkRange"]);
}
