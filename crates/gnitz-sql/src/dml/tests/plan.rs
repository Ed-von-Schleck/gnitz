use super::*;
use crate::expr_lower::compile_wire_conjuncts;
use crate::test_support::{bind_where, col_def, idx_metas_flagged, pk_schema, two_col};
use gnitz_core::TypeCode;

/// The plan for `where_expr` against `lists` (the table's indexes).
fn plan_of(conjuncts: &[BoundExpr], schema: &Schema, lists: &[(&[u32], bool)]) -> AccessPlan {
    bound_and_predicate(schema, conjuncts, &idx_metas_flagged(lists)).expect("the WHERE must plan")
}

/// The predicate the conjuncts of `residual` compile to; empty for none.
fn predicate_of(residual: Option<&str>, schema: &Schema) -> Vec<u8> {
    let conjuncts = residual.map(|s| bind_where(s, schema)).unwrap_or_default();
    compile_wire_conjuncts(&conjuncts, &schema.columns).expect("the residual compiles")
}

/// How many keys `plan`'s bound pins; `None` when it pins none.
fn pinned_keys(plan: &AccessPlan) -> Option<usize> {
    match &plan.bound {
        ReadBound::PkSet(keys) => Some(keys.len()),
        _ => None,
    }
}

/// The bound's kind over `schema`, for a shape assertion that does not care about
/// the range's contents: a range names the store it walks.
fn shape(bound: &ReadBound, schema: &Schema) -> &'static str {
    match bound {
        ReadBound::None => "None",
        ReadBound::Range(r) if r.walks_pk(&schema.pk_cols) => "PkRange",
        ReadBound::Range(_) => "IndexRange",
        ReadBound::PkSet(_) => "PkSet",
    }
}

/// Every WHERE shape, one ladder: which bound it takes, which of it stays
/// residual, and how many keys the bound pins. The schema is `(id U64 pk, v
/// I64)` with an index on `v`, so every rung is reachable from one table.
#[test]
fn the_ladder_maps_each_where_shape_to_its_bound() {
    let schema = pk_schema(TypeCode::U64);
    let idx: &[(&[u32], bool)] = &[(&[1], false)];
    for (sql, want_shape, want_residual, want_keys) in [
        // No WHERE: nothing to walk, nothing to re-impose.
        (None, "None", None, 0),
        // A `pk IN (…)` gather, and the point a one-key list folds to at bind.
        (Some("id IN (7, 9)"), "PkSet", None, 2),
        (Some("id IN (7)"), "PkSet", None, 1),
        (Some("id = 7"), "PkSet", None, 1),
        // `NOT IN` binds to `Not(…)`, which no PK recognizer matches.
        (Some("id NOT IN (7, 9)"), "None", Some("id NOT IN (7, 9)"), 0),
        // A companion conjunct rides the residual of a key-pinning bound: the
        // key restriction supplies the consumed PK conjunct, the residual the rest.
        (Some("id IN (7, 9) AND v > 5"), "PkSet", Some("v > 5"), 2),
        (Some("id = 7 AND v > 5"), "PkSet", Some("v > 5"), 1),
        // No PK conjunct: the index rung, which applies its conjunct exactly, then
        // the unbounded scan. An arithmetic WHERE has no `col OP literal` conjunct.
        (Some("v = 7"), "IndexRange", None, 0),
        (Some("id + v = 7"), "None", Some("id + v = 7"), 0),
        // A non-integral literal names no key, so the bound is the empty range;
        // a top-level OR pins nothing and stays a predicate over the whole table.
        (Some("id = 3.5"), "PkRange", None, 0),
        (Some("v = 5 OR id = 1"), "None", Some("v = 5 OR id = 1"), 0),
    ] {
        let bound_where = sql.map(|s| bind_where(s, &schema)).unwrap_or_default();
        let plan = plan_of(&bound_where, &schema, idx);
        let label = sql.unwrap_or("<no WHERE>");
        assert_eq!(shape(&plan.bound, &schema), want_shape, "{label}");
        assert_eq!(pinned_keys(&plan).unwrap_or(0), want_keys, "{label}: pinned keys");
        assert_eq!(
            plan.predicate,
            predicate_of(want_residual, &schema),
            "{label}: the residual is what ships as a predicate"
        );
    }
}

/// A lone `pk IN (…)` list is keys the statement spelled out, so however long it is
/// it plans one gather.
#[test]
fn a_long_in_list_plans_one_pk_set() {
    let schema = pk_schema(TypeCode::U64);
    let n = 70_000;
    // Built directly: the same list as SQL text is megabytes for the parser.
    let where_expr = [BoundExpr::InList {
        inner: Box::new(BoundExpr::ColRef(0)),
        items: (0..n as i64).map(BoundExpr::LitInt).collect(),
    }];
    let plan = plan_of(&where_expr, &schema, &[]);
    assert_eq!(shape(&plan.bound, &schema), "PkSet");
    assert_eq!(pinned_keys(&plan), Some(n));
    assert!(plan.predicate.is_empty());
}

/// When a PK bound yields to a unique index point, and when it keeps the PK
/// walk instead. Asserted on the resulting bound rather than on a predicate,
/// so it states the plan the rule exists to produce.
#[test]
fn a_pk_bound_yields_only_to_a_unique_index_point() {
    // `(id U64 pk, v I64)`, with `v` indexed — unique or not per case.
    let schema = pk_schema(TypeCode::U64);
    let uniq: &[(&[u32], bool)] = &[(&[1], true)];
    let non_uniq: &[(&[u32], bool)] = &[(&[1], false)];
    for (sql, idx, want, why) in [
        // Nothing pinned on the PK and a unique point available: the point
        // admits one row where the open PK range admits the table.
        ("id > 0 AND v = 42", uniq, "IndexRange", "unpinned PK range yields"),
        // A degenerate range IS a point, so it yields on the same rule — the
        // shape of the conjuncts that produced it does not matter.
        (
            "id > 0 AND v >= 5 AND v <= 5",
            uniq,
            "IndexRange",
            "a degenerate range is a point",
        ),
        // A non-unique index point admits many rows, so it does not beat the
        // PK walk.
        ("id > 0 AND v = 42", non_uniq, "PkRange", "a non-unique point does not"),
        // No index point to build at all.
        ("id > 5 AND v > 1", uniq, "PkRange", "no point available"),
        // A pinned PK point already admits one row.
        ("id = 7 AND v = 42", uniq, "PkSet", "a PK point is never given up"),
    ] {
        let where_expr = bind_where(sql, &schema);
        let plan = plan_of(&where_expr, &schema, idx);
        assert_eq!(shape(&plan.bound, &schema), want, "{sql}: {why}");
    }

    // A descriptor pinning any PK column keeps the PK walk, unique point on
    // offer or not — the client cannot see the distribution prefix, so the ladder
    // bets on the `CLUSTER BY` unicast rather than measuring it.
    let compound = Schema {
        columns: vec![
            col_def("tenant", TypeCode::U64, false),
            col_def("id", TypeCode::U64, false),
            col_def("email", TypeCode::U64, false),
        ],
        pk_cols: vec![0, 1],
    };
    let email_uniq: &[(&[u32], bool)] = &[(&[2], true)];
    for (sql, why) in [
        ("tenant = 7 AND id > 0 AND email = 42", "an equality prefix is pinned"),
        // The bare prefix lowers to a point over `tenant` with nothing pinned
        // before it — still one worker's rows, not one row.
        ("tenant = 7 AND email = 42", "a bare prefix lowers to a pinned point"),
    ] {
        let where_expr = bind_where(sql, &compound);
        let plan = plan_of(&where_expr, &compound, email_uniq);
        assert_eq!(shape(&plan.bound, &compound), "PkRange", "{sql}: {why}");
    }
}

/// An index walk is exact, so it ships only the conjuncts it does not consume — a
/// wide literal or a wide-int column included.
#[test]
fn an_index_walk_ships_only_its_residual() {
    // `(id U64 pk, v U64, w U64)` — `w` keeps a companion conjunct off the PK,
    // which would otherwise take the PK-range rung ahead of any index.
    let narrow = Schema {
        columns: vec![
            col_def("id", TypeCode::U64, false),
            col_def("v", TypeCode::U64, false),
            col_def("w", TypeCode::U64, false),
        ],
        pk_cols: vec![0],
    };
    let wide = two_col(TypeCode::U128); // `val` is U128
    let idx: &[(&[u32], bool)] = &[(&[1], false)];
    for (schema, sql, want_residual) in [
        (&narrow, "v = 5", None),
        (&narrow, "v = 5 AND w = 9", Some("w = 9")),
        (&narrow, "v = 18446744073709551615", None),
        (&wide, "val = 7", None),
        (&wide, "val > 7", None),
    ] {
        let where_expr = bind_where(sql, schema);
        let plan = plan_of(&where_expr, schema, idx);
        assert_eq!(shape(&plan.bound, schema), "IndexRange", "{sql}");
        assert_eq!(plan.predicate, predicate_of(want_residual, schema), "{sql}: predicate");
    }
}

/// A candidate whose residual the expression VM refuses falls through to the next:
/// the PK walk keeps `val = 7` on a U128 column residual, where the index walk
/// consumes it and keeps only the PK conjunct.
#[test]
fn an_uncompilable_pk_residual_falls_through_to_the_index() {
    let schema = two_col(TypeCode::U128);
    for (sql, pk_conjunct) in [
        ("pk > 5 AND val = 7", "pk > 5"),
        ("pk IN (1, 2) AND val = 7", "pk IN (1, 2)"),
    ] {
        let where_expr = bind_where(sql, &schema);
        let plan = plan_of(&where_expr, &schema, &[(&[1], false)]);
        assert_eq!(shape(&plan.bound, &schema), "IndexRange", "{sql}");
        assert_eq!(
            plan.predicate,
            predicate_of(Some(pk_conjunct), &schema),
            "{sql}: the PK conjunct stays residual"
        );
    }
}

/// When no candidate's residual compiles, the error is the best candidate's: the
/// conjunct that candidate could not consume, not one it did.
#[test]
fn an_unsupported_residual_reports_the_best_candidates_blocker() {
    let schema = Schema {
        columns: vec![
            col_def("id", TypeCode::U128, false),
            col_def("name", TypeCode::String, false),
        ],
        pk_cols: vec![0],
    };
    let where_expr = bind_where("id = 7 AND name + 1 > 0", &schema);
    let Err(err) = bound_and_predicate(&schema, &where_expr, &[]) else {
        panic!("`name + 1` cannot compile");
    };
    let only_name = bind_where("name + 1 > 0", &schema);
    let Err(want) = compile_wire_conjuncts(only_name.iter(), &schema.columns) else {
        panic!("`name + 1` cannot compile on its own either");
    };
    assert_eq!(err.to_string(), want.to_string());
}

/// `rows_reply` over `sql` against `schema`, read as relation `t`.
fn rows_reply_of(sql: &str, schema: &Arc<Schema>) -> RowsReply {
    let q = crate::test_support::parse_query(sql);
    let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
        panic!("`{sql}` is not a plain SELECT");
    };
    rows_reply(&sel.projection, q.order_by.as_ref(), schema, "t").unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
}

/// A projection reproducing the relation replies in its layout with no program, ORDER BY
/// keys on source columns; any other projection carries a program.
#[test]
fn only_a_reproducing_projection_ships_no_map() {
    let u = TypeCode::U64;
    let schema = |cols: [&str; 3], pk: u32| {
        Arc::new(Schema {
            columns: cols.iter().map(|n| col_def(n, u, false)).collect(),
            pk_cols: vec![pk],
        })
    };
    let t = schema(["id", "v", "w"], 0);
    for (sql, keys) in [
        ("SELECT * FROM t", vec![]),
        ("SELECT id, v, w FROM t", vec![]),
        ("SELECT * FROM t ORDER BY w", vec![2u16]),
    ] {
        let RowsReply { schema: reply, program, order } = rows_reply_of(sql, &t);
        assert!(Arc::ptr_eq(&reply, &t), "`{sql}`: the source schema itself");
        assert!(program.is_none(), "`{sql}`");
        assert_eq!(order.iter().map(|k| k.col).collect::<Vec<_>>(), keys, "`{sql}`");
    }
    let mid = schema(["v", "id", "w"], 1);
    let RowsReply { schema: reply, program, order } = rows_reply_of("SELECT * FROM t ORDER BY id", &mid);
    assert!(Arc::ptr_eq(&reply, &mid) && program.is_none());
    assert_eq!(order.iter().map(|k| k.col).collect::<Vec<_>>(), [1u16]);
    for sql in [
        "SELECT v, id, w FROM t",
        "SELECT * FROM t ORDER BY v + 1",
        "SELECT id AS x, v, w FROM t",
        // The hidden key appended for `w` sits where the omitted column would.
        "SELECT id, v FROM t ORDER BY w",
    ] {
        let RowsReply { schema: reply, program, .. } = rows_reply_of(sql, &t);
        assert!(program.is_some(), "`{sql}`");
        assert!(!Arc::ptr_eq(&reply, &t), "`{sql}`");
    }
}
