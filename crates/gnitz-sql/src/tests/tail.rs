use super::*;
use crate::test_support::{assert_rejects, col, parse_expr_sql, parse_query, rejected};
use gnitz_wire::TypeCode;

/// The ORDER BY keys of `q`.
fn keys_of(q: &sqlparser::ast::Query) -> Vec<OrderKey<'_>> {
    parse_order_by(q.order_by.as_ref()).unwrap()
}

/// A hidden leading key, then `a`, `b`, `c`.
fn hidden_led() -> Vec<ColumnDef> {
    vec![
        col("_group_pk", TypeCode::U128).hidden(),
        col("a", TypeCode::I64),
        col("b", TypeCode::I64),
        col("c", TypeCode::I64),
    ]
}

/// A position counts the visible columns only, in its range as in its target.
#[test]
fn a_position_names_a_visible_column() {
    let cols = hidden_led();
    let q = parse_query("SELECT * FROM t ORDER BY 1, 3");
    assert_eq!(key_slots(&keys_of(&q), &cols, []).unwrap(), vec![1, 3]);
    for pos in [0, 4] {
        let q = parse_query(&format!("SELECT * FROM t ORDER BY {pos}"));
        assert_eq!(
            rejected(key_slots(&keys_of(&q), &cols, [])),
            format!("ORDER BY position {pos} is out of range (1..=3)")
        );
    }
}

/// Expression keys take their placements in order, positions their visible
/// column; NULLs sort last ascending and first descending unless placed.
#[test]
fn each_key_sorts_on_its_slot_in_its_direction() {
    let cols = hidden_led();
    let q = parse_query("SELECT * FROM t ORDER BY a + 1, 2, b + 1 DESC, c ASC NULLS FIRST, 1 DESC NULLS LAST");
    let keys = keys_of(&q);
    assert_eq!(order_exprs(&keys).len(), 3);
    let k = |col, desc, nulls_first| gnitz_wire::OrderKey { col, desc, nulls_first };
    assert_eq!(
        wire_keys(&keys, &cols, [7, 5, 6]).unwrap(),
        [
            k(7, false, false),
            k(2, false, false),
            k(5, true, true),
            k(6, false, true),
            k(1, true, false)
        ]
    );
    assert!(matches!(key_slots(&keys, &cols, [7]), Err(GnitzSqlError::Internal(_))));
}

/// The key list is capped at what the wire carries.
#[test]
fn an_order_by_past_the_key_cap_is_rejected() {
    let order_by = |n: usize| {
        let keys: Vec<String> = (0..n).map(|i| format!("a + {i}")).collect();
        parse_query(&format!("SELECT * FROM t ORDER BY {}", keys.join(", ")))
    };
    let cap = gnitz_wire::MAX_ORDER_KEYS;
    assert_eq!(parse_order_by(order_by(cap).order_by.as_ref()).unwrap().len(), cap);
    assert_eq!(
        rejected(parse_order_by(order_by(cap + 1).order_by.as_ref())),
        format!("ORDER BY has more than {cap} keys")
    );
}

/// Each clause form's count and skip; an absent one is no limit and no skip.
#[test]
fn limit_and_offset_read_every_clause_form() {
    for (sql, limit, offset) in [
        ("SELECT * FROM t", None, 0),
        ("SELECT * FROM t LIMIT 3", Some(3), 0),
        ("SELECT * FROM t OFFSET 2", None, 2),
        ("SELECT * FROM t LIMIT 3 OFFSET 2", Some(3), 2),
        // MySQL `LIMIT off, lim`.
        ("SELECT * FROM t LIMIT 2, 3", Some(3), 2),
    ] {
        let q = parse_query(sql);
        assert_eq!(
            (extract_limit(&q).unwrap(), extract_offset(&q).unwrap()),
            (limit, offset),
            "{sql}"
        );
    }
}

/// LIMIT/OFFSET read the same decoder, so `(10)` and `+10` are counts; a
/// negative or fractional one names itself in the message.
#[test]
fn expr_usize_literal_reads_the_constant_decoder() {
    for src in ["10", "+10", "(10)"] {
        assert_eq!(expr_usize_literal(&parse_expr_sql(src), "LIMIT").unwrap(), 10, "{src}");
    }
    for (src, want) in [
        ("-1", "'-1'"),
        ("1.5", "'1.5'"),
        ("'x'", "not an expression"),
        ("NULL", "not an expression"),
        ("a", "not an expression"),
        ("1 + 1", "not an expression"),
    ] {
        assert_rejects(src, expr_usize_literal(&parse_expr_sql(src), "LIMIT"), want);
    }
}

/// ORDER BY / GROUP BY positions stay narrower than a constant on purpose:
/// `(1)` and `+1` are expressions over the output, not positions into it.
#[test]
fn clause_position_is_narrower_than_a_constant() {
    assert_eq!(clause_position(&parse_expr_sql("1"), "ORDER BY").unwrap(), Some(1));
    for src in ["(1)", "+1", "a"] {
        assert_eq!(
            clause_position(&parse_expr_sql(src), "ORDER BY").unwrap(),
            None,
            "{src}"
        );
    }
}
