use super::*;
use crate::expr_lower::compile_wire_conjuncts;
use crate::test_support::{bind_where, col, ix, pk_schema, schema, two_col};
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
        (&by_pk, "v = 7", Some(0), ""),
        // No conjunct any bound consumes.
        (&by_pk, "id NOT IN (7, 9)", None, "id NOT IN (7, 9)"),
        (&by_pk, "id + v = 7", None, "id + v = 7"),
        (&by_pk, "v = 5 OR id = 1", None, "v = 5 OR id = 1"),
        (&narrow, "v = 5 AND w = 9", Some(0), "w = 9"),
        (&narrow, "v = 18446744073709551615", Some(0), ""),
        (&wide, "val = 7", Some(0), ""),
        (&wide, "val > 7", Some(0), ""),
        // The PK walk would leave `val = 7` residual, so the index walk serves.
        (&wide, "pk > 5 AND val = 7", Some(1), "pk > 5"),
        (&wide, "pk IN (1, 2) AND val = 7", Some(1), "pk IN (1, 2)"),
    ] {
        let conjuncts = |sql: &str| match sql {
            "" => Vec::new(),
            sql => bind_where(sql, s),
        };
        let indexes = [ix(&[1])];
        let (bound, predicate) = bound_and_predicate(s, &conjuncts(sql), &indexes).expect(sql);
        let want = match served_by {
            Some(k) => candidates(&conjuncts(sql), s, &indexes).swap_remove(k).bound,
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
/// conjunct that candidate could not consume, not one it did.
#[test]
fn an_unsupported_residual_reports_the_best_candidates_blocker() {
    let schema = schema(vec![col("id", TypeCode::U128), col("name", TypeCode::String)], &[0]);
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

/// `rows_reply` over `sql` against `desc`, read as relation `t`.
fn rows_reply_of(sql: &str, desc: &Arc<RelDescriptor>) -> RowsReply {
    let q = crate::test_support::parse_query(sql);
    let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
        panic!("`{sql}` is not a plain SELECT");
    };
    rows_reply(&sel.projection, q.order_by.as_ref(), desc, "t").unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
}

/// A projection reproducing the relation replies in its layout with no program, ORDER BY
/// keys on source columns; any other projection carries a program.
#[test]
fn only_a_reproducing_projection_ships_no_map() {
    let u = TypeCode::U64;
    let schema =
        |cols: [&str; 3], pk: u32| crate::test_support::table(1, cols.iter().map(|n| col(n, u)).collect(), vec![pk]);
    let t = schema(["id", "v", "w"], 0);
    for (sql, keys) in [
        ("SELECT * FROM t", vec![]),
        ("SELECT id, v, w FROM t", vec![]),
        ("SELECT * FROM t ORDER BY w", vec![2u16]),
    ] {
        let RowsReply { schema: reply, program, order } = rows_reply_of(sql, &t);
        assert!(Arc::ptr_eq(&reply, &t.schema), "`{sql}`: the source schema itself");
        assert!(program.is_none(), "`{sql}`");
        assert_eq!(order.iter().map(|k| k.col).collect::<Vec<_>>(), keys, "`{sql}`");
    }
    let mid = schema(["v", "id", "w"], 1);
    let RowsReply { schema: reply, program, order } = rows_reply_of("SELECT * FROM t ORDER BY id", &mid);
    assert!(Arc::ptr_eq(&reply, &mid.schema) && program.is_none());
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
        assert!(!Arc::ptr_eq(&reply, &t.schema), "`{sql}`");
    }
}
