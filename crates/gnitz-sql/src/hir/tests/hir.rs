use super::*;
use crate::hir::bind::tests::bound;

/// A differing set-op pair promotes on the join-key ladder, and only to a target
/// a column copy can widen into.
#[test]
fn set_op_common_type_admits_what_a_column_copy_widens() {
    use TypeCode as T;
    let (t, dec) = (ColType::of, ColType::decimal);
    for (l, r, want) in [
        // Identical types are their own common type, even where no join key could be.
        (t(T::F64), t(T::F64), Some(t(T::F64))),
        (dec(2), dec(2), Some(dec(2))),
        (t(T::U32), t(T::I32), Some(t(T::I64))),
        (t(T::I32), t(T::I64), Some(t(T::I64))),
        // The I128 collapse and every 16-byte / non-integer differing pair are
        // rejected: a column copy widens only into a ≤8-byte integer slot.
        (t(T::U64), t(T::I64), None),
        (t(T::U128), t(T::I64), None),
        (t(T::F64), t(T::F32), None),
        (t(T::String), t(T::I64), None),
        // A column copy moves bytes: it cannot turn days into microseconds, nor
        // either into a plain integer the client would read as a count.
        (t(T::Date), t(T::Timestamp), None),
        (t(T::Date), t(T::I32), None),
        (t(T::I64), t(T::Timestamp), None),
        // A DECIMAL unions only with the same DECIMAL: the stored integers of two
        // scales, or of a scale and an integer column, never mean the same value.
        (dec(2), dec(3), None),
        (dec(0), t(T::I64), None),
    ] {
        assert_eq!(set_op_common_type(l, r), want, "{l} {r}");
    }
}

/// `unique_key` reads a relation's `pk_repeats` flag and carries the key, under
/// each node's own ids, through a filter, a projection passing it through, an
/// alias, a reduce's group, a DISTINCT, a top-N, a window, an EXCEPT or INTERSECT
/// over a set operand, and a join matching each row of the keyed side at most
/// once. A UNION ALL has none. Each row names the key's output columns, over
/// `t`, `u` and the stream `st` of the bind tests' catalog.
#[test]
fn unique_key_is_the_flag_carried_through_set_preserving_nodes() {
    #[rustfmt::skip]
    let cases = [
        ("SELECT id, a FROM t WHERE b > 1", Some("id")),
        ("SELECT a FROM t", None),
        ("SELECT id AS k, a FROM t", Some("k")),
        ("SELECT id, a FROM st", None),
        ("SELECT a, COUNT(*) AS n FROM t GROUP BY a", Some("a")),
        ("SELECT DISTINCT a, b FROM t", Some("a b")),
        ("SELECT DISTINCT id, b FROM t", Some("id")),
        ("SELECT id FROM t UNION ALL SELECT uid FROM u", None),
        ("SELECT id, a FROM t UNION SELECT uid, a FROM u", Some("id a")),
        // An ALL operation keeps a set operand's key; a DISTINCT EXCEPT over a bag
        // clamps it, so every column is the key.
        ("SELECT id, a FROM t EXCEPT ALL SELECT id, a FROM st", Some("id")),
        ("SELECT id, a FROM st INTERSECT ALL SELECT uid, a FROM u", Some("id")),
        ("SELECT id, a FROM st EXCEPT SELECT id, a FROM t", Some("id a")),
        ("SELECT id, a FROM st EXCEPT ALL SELECT id, a FROM t", None),
        // The right key is equated: each left row matches at most one right row.
        ("SELECT t.id, u.uid FROM t JOIN u ON t.a = u.uid", Some("id")),
        ("SELECT t.id, u.uid FROM t LEFT JOIN u ON t.a = u.uid", Some("id")),
        // The left key is equated: the mirror.
        ("SELECT t.id, u.uid FROM t JOIN u ON t.id = u.a", Some("uid")),
        ("SELECT t.id, u.uid FROM t RIGHT JOIN u ON t.id = u.a", Some("uid")),
        // A preserved side matched many times, and a key equated on neither side.
        ("SELECT t.id, u.uid FROM t RIGHT JOIN u ON t.a = u.uid", None),
        ("SELECT t.id, u.uid FROM t LEFT JOIN u ON t.id = u.a", None),
        ("SELECT t.id, u.uid FROM t FULL JOIN u ON t.id = u.uid", None),
        ("SELECT t.id, u.uid FROM t JOIN u ON t.a = u.a", None),
        // A bag on the keyed side is no key, however it is equated.
        ("SELECT st.id FROM st JOIN u ON st.a = u.uid", None),
        ("SELECT t.id FROM t JOIN st ON t.a = st.id", None),
        // One output row per left row, whatever the right side matches.
        ("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.a = t.a)", Some("id")),
        ("SELECT id, EXISTS (SELECT 1 FROM u WHERE u.a = t.a) AS e FROM t", Some("id")),
        ("SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.a = t.a)", Some("id")),
        ("SELECT id FROM t ORDER BY a LIMIT 2", Some("id")),
        ("SELECT id FROM st ORDER BY a LIMIT 2", None),
        ("SELECT id FROM t QUALIFY ROW_NUMBER() OVER (ORDER BY a) <= 2", Some("id")),
        ("SELECT id, SUM(a) OVER (ORDER BY a) AS r FROM t", Some("id")),
        ("SELECT id, SUM(a) OVER (PARTITION BY a) AS r FROM t", Some("id")),
        ("WITH c AS (SELECT id, a FROM t) SELECT x.id, y.id AS y FROM c x JOIN c y ON x.a = y.id", Some("id")),
    ];
    for (sql, want) in cases {
        let rel = bound(sql);
        let cols = rel.cols();
        let key = rel.unique_key().map(|k| {
            let names: Vec<&str> = k.iter().map(|&id| &*hircol_of(&cols, id).def.name).collect();
            names.join(" ")
        });
        assert_eq!(key.as_deref(), want, "{sql}");
    }
}
