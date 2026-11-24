use super::*;
use crate::test_support::col;
use gnitz_wire::{RelClass, TypeCode};

/// A table of U64 columns `cols` keyed on `pk`.
fn u64_table(cols: &[&str], pk: &[u32]) -> Arc<RelDescriptor> {
    crate::test_support::table(1, cols.iter().map(|n| col(n, TypeCode::U64)).collect(), pk.to_vec())
}

/// The rows reply of `sql` against `desc`, read as relation `t`.
fn rows_reply_of(sql: &str, desc: &Arc<RelDescriptor>) -> RowsReply {
    let q = crate::test_support::parse_query(sql);
    let cat = crate::test_support::catalog(vec![("t", Arc::clone(desc))]);
    let read = crate::hir::bind_adhoc_read(&cat, &q).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
    let AdhocRead::Relation { shape: AdhocShape::Rows(rows), .. } = read else {
        panic!("`{sql}` is not a rows read");
    };
    rows
}

/// The reply lists the key columns no item names, hidden under the relation's
/// definition, then the items — a key column at the first item that copies it, a
/// second copy a payload column. A projection whose payload is the relation's own,
/// each column copied in place, ships no program, wherever the key stands and
/// whatever the columns are called; a view's hidden columns ride along, where a
/// table's are copied out from beside them. Keys ascending a leading run of the PK
/// are ones a walk in PK order already yields.
#[test]
fn a_rows_reply_numbers_the_select_list_over_the_relations_regions() {
    let t = u64_table(&["id", "v", "w"], &[0]);
    let mid = u64_table(&["v", "id", "w"], &[1]);
    let ab = u64_table(&["a", "b", "v"], &[0, 1]);
    let hid = |class| {
        let u = |n| col(n, TypeCode::U64);
        crate::test_support::rel(
            1,
            class,
            vec![u("id"), u("v"), u("_h").hidden(), u("w")],
            vec![0],
            vec![],
        )
    };
    let (view, hidden) = (hid(RelClass::View), hid(RelClass::Table));
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
        (&t,   "SELECT id, v, w FROM t ORDER BY v",          false, "id v w",    &[0], "v", false),
        (&t,   "SELECT id AS k, v, w FROM t ORDER BY k",     false, "k v w",     &[0], "k", true),
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
        // A computed copy of the key is no key: the key rides hidden.
        (&t,   "SELECT id + 0 AS k, v FROM t ORDER BY v",    true,  "[id] k v",  &[0], "v", false),
        // A view's hidden column rides along; a table's is copied out from beside it.
        (&view,   "SELECT * FROM t ORDER BY w",              false, "id v [_h] w", &[0], "w", false),
        (&hidden, "SELECT * FROM t ORDER BY w",              true,  "id v w",    &[0], "w", false),
        // A hidden key the ORDER BY computes, behind the SELECT list.
        (&t,   "SELECT * FROM t ORDER BY v + 1",             true,  "id v w [_order0]", &[0], "[_order0]", false),
        // A float literal is the same literal only bit for bit: `-0.0` is not `0.0`.
        (&t,   "SELECT v * 0.0 AS a FROM t ORDER BY v * -0.0", true, "[id] a [_order0]", &[0], "[_order0]", false),
    ];
    for &(desc, sql, mapped, cols, key, order_cols, pk_ordered) in cases {
        let r = rows_reply_of(sql, desc);
        let name = |c: usize| {
            let c = &r.schema.columns()[c];
            match c.is_hidden {
                true => format!("[{}]", c.name),
                false => c.name.to_string(),
            }
        };
        let shown: Vec<String> = (0..r.schema.columns().len()).map(name).collect();
        let ordered: Vec<String> = r.order.iter().map(|k| name(k.col as usize)).collect();
        assert_eq!(
            (
                shown.join(" "),
                r.schema.pk_cols(),
                ordered.join(" "),
                r.program.is_some(),
                r.pk_ordered
            ),
            (cols.to_string(), key, order_cols.to_string(), mapped, pk_ordered),
            "`{sql}`"
        );
    }
}
