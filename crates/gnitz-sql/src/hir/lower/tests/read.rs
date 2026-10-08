use super::*;
use crate::test_support::col;
use gnitz_wire::TypeCode;

/// A table of U64 columns `cols` keyed on `pk`.
fn u64_table(cols: &[&str], pk: &[u32]) -> Arc<RelDescriptor> {
    crate::test_support::table(1, cols.iter().map(|n| col(n, TypeCode::U64)).collect(), pk.to_vec())
}

/// The rows reply of `sql` against `desc`, read as relation `t`.
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
    rows.reply(&keys).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
}

/// A projection whose payload is the relation's own, each column copied in
/// place, replies in the relation's regions with no program — wherever the key
/// stands in the SELECT list and whatever the columns are called — its ORDER BY
/// keys numbered by the reply for the client and by the relation for the worker.
/// Any other projection carries a program, whose output the worker numbers key
/// first.
#[test]
fn a_projection_keeping_the_relations_payload_ships_no_map() {
    let (t, mid) = (u64_table(&["id", "v", "w"], &[0]), u64_table(&["v", "id", "w"], &[1]));
    let cols = |keys: &[gnitz_wire::OrderKey]| keys.iter().map(|k| k.col).collect::<Vec<_>>();
    let names = |s: &gnitz_core::Schema| s.columns().iter().map(|c| c.name.clone()).collect::<Vec<_>>();
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
        // The key a hidden ORDER BY item copies is that hidden key.
        (&t, "SELECT v, w FROM t ORDER BY id", ["id", "v", "w"], 0, 0, 0),
    ] {
        let RowsReply {
            schema: reply,
            program,
            order,
            sink_order,
            ..
        } = rows_reply_of(sql, desc);
        assert!(program.is_none(), "`{sql}`");
        assert!(
            reply.layout().same_region_types(desc.schema.as_ref().layout()),
            "`{sql}`"
        );
        assert_eq!(
            (names(&reply), reply.pk_cols()),
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
        assert_eq!(reply.pk_cols(), [key], "`{sql}`");
        let shown: Vec<&str> = reply.visible_columns().map(|(_, c)| c.name.as_str()).collect();
        assert_eq!(shown, visible, "`{sql}`");
        assert_eq!(
            (cols(&order), cols(&sink_order)),
            (vec![in_reply], vec![in_output]),
            "`{sql}`"
        );
    }
}

/// A key column an ORDER BY names and no item does rides hidden in its own key slot, under
/// the relation's definition of it; a second visible copy of a key is a payload column.
#[test]
fn an_ordering_key_column_no_item_names_is_the_hidden_key() {
    let (t, ab) = (u64_table(&["id", "v", "w"], &[0]), u64_table(&["a", "b", "v"], &[0, 1]));
    // `(relation, statement, mapped, pk_ordered, ORDER BY in the reply, in the sink's input)`.
    for (desc, sql, mapped, pk_ordered, in_reply, in_sink) in [
        (&t, "SELECT v, w FROM t ORDER BY id", false, true, 0u16, 0u16),
        (&ab, "SELECT v FROM t ORDER BY b", false, false, 1, 1),
        (&ab, "SELECT v FROM t ORDER BY a, b", false, true, 0, 0),
        (
            &t,
            "SELECT id, id AS again, v FROM t ORDER BY again DESC",
            true,
            false,
            1,
            1,
        ),
    ] {
        let reply = rows_reply_of(sql, desc);
        assert_eq!(reply.program.is_some(), mapped, "`{sql}`");
        assert_eq!(reply.pk_ordered, pk_ordered, "`{sql}`");
        assert_eq!(
            (reply.order[0].col, reply.sink_order[0].col),
            (in_reply, in_sink),
            "`{sql}`"
        );
        // The key region is the relation's, a column no item names hidden.
        for (&at, &pk) in reply.schema.pk_cols().iter().zip(desc.schema.pk_cols()) {
            let (got, want) = (
                &reply.schema.columns()[at as usize],
                &desc.schema.columns()[pk as usize],
            );
            let named = sql.starts_with("SELECT id");
            assert_eq!(
                *got,
                if named { want.clone() } else { want.clone().hidden() },
                "`{sql}`"
            );
        }
    }
}
