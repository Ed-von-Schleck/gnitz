use super::*;
use crate::expr_lower::compile_wire_conjuncts;
use crate::test_support::{bind_where, col, ix, pk_schema, schema, two_col};
use gnitz_expr::SchemaFacts;
use gnitz_wire::TypeCode;

/// The first candidate whose residual compiles serves the WHERE and ships exactly
/// the conjuncts it leaves; with no candidate the whole WHERE ships under no bound.
/// `Some(k)` names the candidate, by rank, whose bound the plan must carry.
#[test]
fn the_first_servable_candidate_bounds_the_read_and_ships_its_residual() {
    let by_pk = pk_schema(TypeCode::U64); // (id pk, v), v indexed
    let narrow = schema(["id", "v", "w"].map(|n| col(n, TypeCode::U64)).to_vec(), &[0]);
    // No register holds `val`, so a residual naming it does not compile.
    let wide = two_col(TypeCode::U128);
    for (s, sql, served_by, residual) in [
        (&by_pk, "", None, ""),
        (&by_pk, "id IN (7, 9) AND v > 5", Some(0), "v > 5"),
        // No conjunct any bound consumes.
        (&by_pk, "id + v = 7", None, "id + v = 7"),
        (&narrow, "v = 5 AND w = 9", Some(0), "w = 9"),
        // The PK walk would leave `val = 7` residual, so the index walk serves.
        (&wide, "pk > 5 AND val = 7", Some(1), "pk > 5"),
    ] {
        let conjuncts = |sql: &str| match sql {
            "" => Vec::new(),
            sql => bind_where(sql, s),
        };
        let (indexes, bound_conjuncts) = ([ix(&[1])], conjuncts(sql));
        let (bound, predicate) = bound_and_predicate(s, &bound_conjuncts, &indexes).expect(sql);
        let want = match served_by {
            Some(k) => candidates(&bound_conjuncts, s, &indexes).swap_remove(k).bound,
            None => ReadBound::None,
        };
        assert_eq!(bound, want, "{sql}");
        assert_eq!(
            predicate,
            compile_wire_conjuncts(&conjuncts(residual), &s.columns).unwrap(),
            "{sql}: the residual is what ships as a predicate"
        );
    }
}

/// When no candidate's residual compiles, the error is the best candidate's: the
/// conjunct that candidate could not consume, not one a later candidate left.
#[test]
fn an_unsupported_residual_reports_the_best_candidates_blocker() {
    let schema = schema(
        vec![
            col("id", TypeCode::U64),
            col("big", TypeCode::U128),
            col("name", TypeCode::String),
        ],
        &[0],
    );
    let indexes = [ix(&[1])];
    let conjuncts = bind_where("id > 5 AND big = 7 AND name + 1 > 0", &schema);
    let blockers: Vec<String> = candidates(&conjuncts, &schema, &indexes)
        .iter()
        .map(|c| {
            compile_wire_conjuncts(residual(&conjuncts, &c.consumed).iter().copied(), &schema.columns)
                .expect_err("every residual keeps `name + 1`")
                .to_string()
        })
        .collect();
    assert!(blockers.len() == 2 && blockers[0] != blockers[1], "{blockers:?}");
    let err = bound_and_predicate(&schema, &conjuncts, &indexes).expect_err("no residual compiles");
    assert_eq!(err.to_string(), blockers[0]);
}

/// `rows_reply` over `sql` against `desc`, read as relation `t`.
fn rows_reply_of(sql: &str, desc: &Arc<RelDescriptor>) -> RowsReply {
    let q = crate::test_support::parse_query(sql);
    let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
        panic!("`{sql}` is not a plain SELECT");
    };
    let cat = crate::test_support::catalog(vec![("t", Arc::clone(desc))]);
    let keys = crate::tail::parse_order_by(q.order_by.as_ref()).unwrap();
    let exprs = crate::tail::order_exprs(&keys);
    let read =
        crate::hir::bind_adhoc_read(&cat, &q, sel, "SELECT", &exprs).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
    let crate::hir::AdhocRead::Relation {
        shape: crate::hir::AdhocShape::Rows(rows),
        ..
    } = read
    else {
        panic!("`{sql}` is not a rows read");
    };
    rows_reply(rows, &keys, desc).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
}

/// A projection whose payload is the relation's own, each column copied in
/// place, replies in the relation's regions with no program — wherever the key
/// stands in the SELECT list and whatever the columns are called — its ORDER BY
/// keys numbered by the reply for the client and by the relation for the worker.
/// Any other projection carries a program, whose output the worker numbers key
/// first.
#[test]
fn a_projection_keeping_the_relations_payload_ships_no_map() {
    let rel3 = |cols: [&str; 3], pk: u32| {
        crate::test_support::table(1, cols.iter().map(|n| col(n, TypeCode::U64)).collect(), vec![pk])
    };
    let (t, mid) = (rel3(["id", "v", "w"], 0), rel3(["v", "id", "w"], 1));
    let cols = |keys: &[gnitz_wire::OrderKey]| keys.iter().map(|k| k.col).collect::<Vec<_>>();
    let names = |s: &gnitz_core::Schema| s.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
    // `(relation, statement, reply columns, the key's column, ORDER BY in the
    // reply, ORDER BY in the relation)`.
    for (desc, sql, reply_cols, key, in_reply, in_relation) in [
        (&t, "SELECT * FROM t ORDER BY w", ["id", "v", "w"], 0, 2u16, 2u16),
        (&mid, "SELECT * FROM t ORDER BY id", ["v", "id", "w"], 1, 1, 1),
        (&t, "SELECT v, id, w FROM t ORDER BY w", ["v", "id", "w"], 1, 2, 2),
        (&t, "SELECT v, w, id AS k FROM t ORDER BY k", ["v", "w", "k"], 2, 2, 0),
        (
            &mid,
            "SELECT id, v AS a, w FROM t ORDER BY a",
            ["id", "a", "w"],
            0,
            1,
            0,
        ),
        // The key no item names rides hidden in front.
        (&t, "SELECT v, w FROM t ORDER BY v", ["id", "v", "w"], 0, 1, 1),
    ] {
        let RowsReply {
            schema: reply,
            program,
            order,
            sink_order,
            ..
        } = rows_reply_of(sql, desc);
        assert!(program.is_none(), "`{sql}`");
        assert!(reply.same_region_types(desc.schema.as_ref()), "`{sql}`");
        assert_eq!(
            (names(&reply), &reply.pk_cols[..]),
            (reply_cols.map(String::from).to_vec(), &[key][..]),
            "`{sql}`"
        );
        assert_eq!(
            (cols(&order), cols(&sink_order)),
            (vec![in_reply], vec![in_relation]),
            "`{sql}`"
        );
    }
    // `(statement, visible reply columns, the key's column, ORDER BY in the
    // reply, ORDER BY in the program's output)`.
    for (sql, visible, key, in_reply, in_output) in [
        // A column dropped, two exchanged, a second copy of the key.
        ("SELECT v, id FROM t ORDER BY v", vec!["v", "id"], 1, 0u16, 1u16),
        ("SELECT w, v, id FROM t ORDER BY id", vec!["w", "v", "id"], 2, 2, 0),
        (
            "SELECT id, id AS again, v, w FROM t ORDER BY again",
            vec!["id", "again", "v", "w"],
            0,
            1,
            1,
        ),
        // A hidden key the ORDER BY computes, behind the SELECT list.
        ("SELECT * FROM t ORDER BY v + 1", vec!["id", "v", "w"], 0, 3, 3),
    ] {
        let RowsReply {
            schema: reply,
            program,
            order,
            sink_order,
            ..
        } = rows_reply_of(sql, &t);
        assert!(program.is_some(), "`{sql}`");
        assert_eq!(reply.pk_cols, [key], "`{sql}`");
        let shown: Vec<&str> = reply.visible_columns().map(|(_, c)| c.name.as_str()).collect();
        assert_eq!(shown, visible, "`{sql}`");
        assert_eq!(
            (cols(&order), cols(&sink_order)),
            (vec![in_reply], vec![in_output]),
            "`{sql}`"
        );
    }
}
