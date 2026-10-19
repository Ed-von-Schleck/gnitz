use super::*;
use crate::test_support::col;
use gnitz_expr::SchemaDescriptor;
use gnitz_wire::TypeCode;

/// A table of U64 columns `cols` keyed on `pk`.
fn u64_table(cols: &[&str], pk: &[u32]) -> Arc<RelDescriptor> {
    crate::test_support::table(1, cols.iter().map(|n| col(n, TypeCode::U64)).collect(), pk.to_vec())
}

/// The rows reply of `sql` against `desc`, read as relation `t`, for a worker walking
/// in PK order when `walks_in_pk_order`.
fn rows_reply_of(sql: &str, desc: &Arc<RelDescriptor>, walks_in_pk_order: bool) -> RowsReply {
    let q = crate::test_support::parse_query(sql);
    let sel = crate::validate::as_plain_select(&q.body, sql).unwrap();
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
    rows.reply(&keys, walks_in_pk_order)
        .unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
}

/// Column `c`'s region: its place in the key region, then in the payload.
fn region(s: &SchemaDescriptor, c: u16) -> usize {
    let c = c as usize;
    s.payload_slot(c).map_or_else(
        || s.pk_cols().iter().position(|&p| p as usize == c).unwrap(),
        |p| s.pk_cols().len() + p,
    )
}

/// The reply lists the key columns no item names, hidden under the relation's
/// definition, then the items — a key column at the first item that copies it, a
/// second copy a payload column. A projection whose payload is the relation's own,
/// each column copied in place, ships no program, wherever the key stands and
/// whatever the columns are called. The worker sorts its input — the program's
/// output, or the relation — on the region the client sorts the reply on, in the
/// same direction, and a worker walking in PK order ranks nothing for keys ascending
/// a leading run of the PK.
#[test]
fn a_rows_reply_numbers_the_select_list_over_the_relations_regions() {
    let t = u64_table(&["id", "v", "w"], &[0]);
    let mid = u64_table(&["v", "id", "w"], &[1]);
    let ab = u64_table(&["a", "b", "v"], &[0, 1]);
    /// The relation, the statement, whether it is mapped, the reply columns with hidden
    /// ones bracketed, the key, the ORDER BY columns, and whether a walk in PK order
    /// yields them.
    type Case<'a> = (
        &'a Arc<RelDescriptor>,
        &'static str,
        bool,
        &'static str,
        &'static [u32],
        &'static str,
        bool,
    );
    #[rustfmt::skip]
    let cases: &[Case] = &[
        (&t,   "SELECT * FROM t ORDER BY w",                 false, "id v w",    &[0], "w", false),
        (&mid, "SELECT * FROM t ORDER BY id",                false, "v id w",    &[1], "id", true),
        (&t,   "SELECT v, id, w FROM t ORDER BY w",          false, "v id w",    &[1], "w", false),
        (&t,   "SELECT v, w, id AS k FROM t ORDER BY k",     false, "v w k",     &[2], "k", true),
        (&mid, "SELECT id, v AS a, w FROM t ORDER BY a",     false, "id a w",    &[0], "a", false),
        // The key no item names rides hidden in front; a hidden ORDER BY item copying
        // it is that hidden key.
        (&t,   "SELECT v, w FROM t ORDER BY v",              false, "[id] v w",  &[0], "v", false),
        (&t,   "SELECT v, w FROM t ORDER BY id",             false, "[id] v w",  &[0], "[id]", true),
        (&ab,  "SELECT v FROM t ORDER BY b",                 false, "[a] [b] v", &[0, 1], "[b]", false),
        (&ab,  "SELECT v FROM t ORDER BY a, b",              false, "[a] [b] v", &[0, 1], "[a] [b]", true),
        (&ab,  "SELECT v FROM t ORDER BY b, a",              false, "[a] [b] v", &[0, 1], "[b] [a]", false),
        // A column dropped, two exchanged, a second copy of the key.
        (&t,   "SELECT v, id FROM t ORDER BY v",             true,  "v id",      &[1], "v", false),
        (&t,   "SELECT w, v, id FROM t ORDER BY id",         true,  "w v id",    &[2], "id", true),
        (&t,   "SELECT id, id AS again, v, w FROM t ORDER BY again", true, "id again v w", &[0], "again", true),
        (&t,   "SELECT id, id AS again, v FROM t ORDER BY again DESC", true, "id again v", &[0], "again", false),
        // A hidden key the ORDER BY computes, behind the SELECT list.
        (&t,   "SELECT * FROM t ORDER BY v + 1",             true,  "id v w [_order0]", &[0], "[_order0]", false),
    ];
    for &(desc, sql, mapped, cols, key, order_cols, pk_ordered) in cases {
        let r = rows_reply_of(sql, desc, false);
        let name = |c: usize| {
            let c = &r.schema.columns()[c];
            match c.is_hidden {
                true => format!("[{}]", c.name),
                false => c.name.clone(),
            }
        };
        let shown: Vec<String> = (0..r.schema.columns().len()).map(name).collect();
        let ordered: Vec<String> = r.order.iter().map(|k| name(k.col as usize)).collect();
        assert_eq!(
            (
                shown.join(" "),
                r.schema.pk_cols(),
                ordered.join(" "),
                r.program.is_some()
            ),
            (cols.to_string(), key, order_cols.to_string(), mapped),
            "`{sql}`"
        );
        let (reply, relation) = (r.schema.layout(), desc.schema.layout());
        assert_eq!(r.sink_order.len(), r.order.len(), "`{sql}`");
        for (o, s) in r.order.iter().zip(&r.sink_order) {
            let sink_region = match mapped {
                true => s.col as usize,
                false => region(relation, s.col),
            };
            assert_eq!(region(reply, o.col), sink_region, "`{sql}`");
            assert_eq!(OrderKey { col: 0, ..*o }, OrderKey { col: 0, ..*s }, "`{sql}`");
        }
        // With no program every relation column is copied at its own region.
        if !mapped {
            assert_eq!(r.copied.len(), relation.num_columns(), "`{sql}`");
            for &(c, to) in &r.copied {
                assert_eq!(region(relation, c as u16), region(reply, to as u16), "`{sql}`");
            }
        }
        let walked = rows_reply_of(sql, desc, true);
        assert_eq!(walked.sink_order.is_empty(), pk_ordered, "`{sql}`");
    }
}
