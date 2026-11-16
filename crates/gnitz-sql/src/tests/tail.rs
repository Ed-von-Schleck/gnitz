use super::*;
use crate::test_support::{assert_rejects, parse_expr_sql, parse_query, rejected};

/// The ORDER BY keys of `q`.
fn keys_of(q: &sqlparser::ast::Query) -> Vec<OrderKey<'_>> {
    parse_order_by(q.order_by.as_ref()).unwrap()
}

/// NULLs sort last ascending and first descending unless placed.
#[test]
fn each_key_sorts_in_its_direction() {
    let q = parse_query("SELECT * FROM t ORDER BY a + 1, 2, b + 1 DESC, c ASC NULLS FIRST, 1 DESC NULLS LAST");
    let keys = keys_of(&q);
    let dirs: Vec<_> = keys.iter().map(|k| (k.desc, k.nulls_first)).collect();
    assert_eq!(
        dirs,
        [
            (false, false),
            (false, false),
            (true, true),
            (false, true),
            (true, false)
        ]
    );
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

/// Each clause form's count and skip, `None` where absent.
#[test]
fn limit_and_offset_read_every_clause_form() {
    for (sql, limit, offset) in [
        ("SELECT * FROM t", None, None),
        ("SELECT * FROM t LIMIT 3", Some(3), None),
        ("SELECT * FROM t OFFSET 2", None, Some(2)),
        ("SELECT * FROM t OFFSET 0", None, Some(0)),
        ("SELECT * FROM t LIMIT 3 OFFSET 2", Some(3), Some(2)),
        // MySQL `LIMIT off, lim`.
        ("SELECT * FROM t LIMIT 2, 3", Some(3), Some(2)),
    ] {
        let q = parse_query(sql);
        assert_eq!(extract_limit_offset(&q).unwrap(), (limit, offset), "{sql}");
    }
    for (sql, want) in [
        ("SELECT * FROM t LIMIT 1 OFFSET 'x'", "OFFSET"),
        ("SELECT * FROM t LIMIT 1+1", "LIMIT"),
    ] {
        assert_rejects(sql, extract_limit_offset(&parse_query(sql)), want);
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
