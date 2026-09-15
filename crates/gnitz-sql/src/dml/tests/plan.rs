use super::*;
use crate::test_support::{bind_where, col_def, idx_metas_flagged, pk_schema, two_col};
use gnitz_core::TypeCode;

/// The plan for `where_expr` against `lists` (the table's indexes).
fn plan_of<'e>(
    conjuncts: &'e [BoundExpr],
    schema: &Schema,
    lists: &[(&[u32], bool)],
    budget: ReadBudget,
) -> AccessPlan<'e> {
    bound_and_predicate(schema, conjuncts, budget, &idx_metas_flagged(lists)).expect("the WHERE must plan")
}

/// How many keys `plan` restricts the transaction's buffered rows to; `None`
/// when it restricts nothing.
fn pinned_keys(plan: &AccessPlan<'_>, schema: &Schema) -> Option<usize> {
    match plan.buffered_scope(schema).0 {
        BufferedKeys::Keys(keys) => Some(keys.len()),
        BufferedKeys::Point(_) => Some(1),
        BufferedKeys::All => None,
    }
}

/// The bound's discriminant name, for a shape assertion that does not care
/// about the descriptor's contents.
fn shape(bound: &ReadBound) -> &'static str {
    match bound {
        ReadBound::None => "None",
        ReadBound::PkRange(_) => "PkRange",
        ReadBound::IndexRange { .. } => "IndexRange",
        ReadBound::PkSet(_) => "PkSet",
    }
}

/// Every WHERE shape, one ladder: which bound it takes, how much of it stays
/// residual, and how many keys the bound pins. The schema is `(id U64 pk, v
/// I64)` with an index on `v`, so every rung is reachable from one table.
#[test]
fn the_ladder_maps_each_where_shape_to_its_bound() {
    let schema = pk_schema(TypeCode::U64);
    let idx: &[(&[u32], bool)] = &[(&[1], false)];
    for (sql, want_shape, want_residual, want_keys) in [
        // No WHERE: nothing to walk, nothing to re-impose.
        (None, "None", 0, 0),
        // A `pk IN (…)` gather, and the point a one-key list folds to at bind.
        (Some("id IN (7, 9)"), "PkSet", 0, 2),
        (Some("id IN (7)"), "PkRange", 0, 1),
        (Some("id = 7"), "PkRange", 0, 1),
        // `NOT IN` binds to `Not(…)`, which no PK recognizer matches.
        (Some("id NOT IN (7, 9)"), "None", 1, 0),
        // A companion conjunct rides the residual of a key-pinning bound: the
        // key restriction supplies the consumed PK conjunct, the residual the rest.
        (Some("id IN (7, 9) AND v > 5"), "PkSet", 1, 2),
        (Some("id = 7 AND v > 5"), "PkRange", 1, 1),
        // No PK conjunct: the index rung, then the unbounded scan. An
        // arithmetic WHERE has no `col OP literal` conjunct at all.
        (Some("v = 7"), "IndexRange", 1, 0),
        (Some("id + v = 7"), "None", 1, 0),
        // A non-integral literal pins no key, and a top-level OR pins nothing
        // at all: both stay a predicate over the whole table.
        (Some("id = 3.5"), "None", 1, 0),
        (Some("v = 5 OR id = 1"), "None", 1, 0),
    ] {
        let bound_where = sql.map(|s| bind_where(s, &schema)).unwrap_or_default();
        let plan = plan_of(&bound_where, &schema, idx, ReadBudget::OneRequest);
        let label = sql.unwrap_or("<no WHERE>");
        assert_eq!(shape(&plan.access.bound), want_shape, "{label}");
        assert_eq!(plan.residual.len(), want_residual, "{label}: residual");
        assert_eq!(
            pinned_keys(&plan, &schema).unwrap_or(0),
            want_keys,
            "{label}: pinned keys"
        );
        assert_eq!(
            plan.access.predicate.is_empty(),
            want_residual == 0,
            "{label}: the residual is what ships as a predicate"
        );
    }
}

/// The budget is the whole difference between the verbs: a gather past the
/// wire's per-request key cap declines to the rest of the ladder (an ordinary
/// predicate scan) unless the caller may chunk.
#[test]
fn an_over_cap_pk_in_list_needs_a_chunking_budget() {
    let schema = pk_schema(TypeCode::U64);
    let n = gnitz_wire::MAX_PK_SET_KEYS + 1;
    // Built directly: the same list as SQL text is megabytes for the parser.
    let where_expr = [BoundExpr::InList {
        inner: Box::new(BoundExpr::ColRef(0)),
        items: (0..n as i64).map(BoundExpr::LitInt).collect(),
    }];
    let declined = plan_of(&where_expr, &schema, &[], ReadBudget::OneRequest);
    assert_eq!(
        shape(&declined.access.bound),
        "None",
        "{n} keys past the one-request cap"
    );
    assert!(pinned_keys(&declined, &schema).is_none());
    assert!(
        !declined.access.predicate.is_empty(),
        "the list ships as a predicate instead"
    );

    let gathered = plan_of(&where_expr, &schema, &[], ReadBudget::MayChunk);
    assert_eq!(shape(&gathered.access.bound), "PkSet");
    assert_eq!(pinned_keys(&gathered, &schema), Some(n));
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
        ("id = 7 AND v = 42", uniq, "PkRange", "a PK point is never given up"),
    ] {
        let where_expr = bind_where(sql, &schema);
        let plan = plan_of(&where_expr, &schema, idx, ReadBudget::OneRequest);
        assert_eq!(shape(&plan.access.bound), want, "{sql}: {why}");
    }

    // A descriptor pinning any PK column keeps the PK walk, unique point on
    // offer or not — the client cannot see `dist_prefix_len`, so the ladder
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
        let plan = plan_of(&where_expr, &compound, email_uniq, ReadBudget::OneRequest);
        assert_eq!(shape(&plan.access.bound), "PkRange", "{sql}: {why}");
    }
}

/// The index walk is marked exact — and its conjunct stripped from the
/// predicate — exactly when the predicate could not carry that conjunct: a
/// literal past the VM's `i64` constant on a narrow column, or a wide-int
/// column outright. An ordinary narrow bound keeps the whole WHERE, leaving
/// the worker free to trade the walk for a full cursor.
#[test]
fn an_index_walk_is_exact_only_when_the_predicate_cannot_carry_its_conjunct() {
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
    for (schema, sql, want_required, want_residual) in [
        (&narrow, "v = 5", false, 1),
        (&narrow, "v = 5 AND w = 9", false, 2),
        (&narrow, "v = 18446744073709551615", true, 0),
        (&wide, "val = 7", true, 0),
    ] {
        let where_expr = bind_where(sql, schema);
        let plan = plan_of(&where_expr, schema, idx, ReadBudget::OneRequest);
        let ReadBound::IndexRange { walk, .. } = plan.access.bound else {
            panic!("{sql}: expected an index bound, got {}", shape(&plan.access.bound));
        };
        assert_eq!(walk == IndexWalk::Required, want_required, "{sql}: walk");
        assert_eq!(plan.residual.len(), want_residual, "{sql}: residual");
    }
}

/// A PK bound pinning no PK column yields to a point covering every column of
/// a UNIQUE index — one row, where the unpinned range admits the table.
#[test]
fn an_unpinned_pk_range_yields_to_a_full_unique_point() {
    let schema = two_col(TypeCode::U64);
    let where_expr = bind_where("pk > 0 AND val = 42", &schema);
    let plan = plan_of(&where_expr, &schema, &[(&[1], true)], ReadBudget::OneRequest);
    assert_eq!(shape(&plan.access.bound), "IndexRange");
    assert!(
        pinned_keys(&plan, &schema).is_none(),
        "an index bound never pins a PK key"
    );
}

/// `rows_sink` over `sql` against `schema`, read as relation `t`.
fn rows_sink_of(sql: &str, schema: &Arc<Schema>) -> (Arc<Schema>, ReadSink, Vec<gnitz_wire::OrderKey>) {
    let q = crate::test_support::parse_query(sql);
    let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
        panic!("`{sql}` is not a plain SELECT");
    };
    rows_sink(&sel.projection, q.order_by.as_ref(), schema, "t", 0).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
}

/// A projection reproducing the relation replies in its layout with no map, ORDER BY keys
/// on source columns; any other projection ships a map.
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
        let (reply, sink, order) = rows_sink_of(sql, &t);
        assert!(Arc::ptr_eq(&reply, &t), "`{sql}`: the source schema itself");
        assert!(sink.map.is_none(), "`{sql}`");
        assert_eq!(order.iter().map(|k| k.col).collect::<Vec<_>>(), keys, "`{sql}`");
        let SinkKind::Rows { order: shipped, .. } = &sink.kind else {
            panic!("`{sql}`: a rows sink");
        };
        assert_eq!(shipped, &order, "`{sql}`: the worker orders by the same keys");
    }
    let mid = schema(["v", "id", "w"], 1);
    let (reply, sink, order) = rows_sink_of("SELECT * FROM t ORDER BY id", &mid);
    assert!(Arc::ptr_eq(&reply, &mid) && sink.map.is_none());
    assert_eq!(order.iter().map(|k| k.col).collect::<Vec<_>>(), [1u16]);
    for sql in [
        "SELECT v, id, w FROM t",
        "SELECT * FROM t ORDER BY v + 1",
        "SELECT id AS x, v, w FROM t",
        // The hidden key appended for `w` sits where the omitted column would.
        "SELECT id, v FROM t ORDER BY w",
    ] {
        let (reply, sink, _) = rows_sink_of(sql, &t);
        assert!(sink.map.is_some(), "`{sql}`");
        assert!(!Arc::ptr_eq(&reply, &t), "`{sql}`");
    }
}
