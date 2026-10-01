use super::*;
use crate::test_support::{col, parse_query, rejected};
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

#[test]
fn expression_keys_consume_placements_in_order() {
    let cols = hidden_led();
    let q = parse_query("SELECT * FROM t ORDER BY a + 1, 2, b + 1 DESC");
    let keys = keys_of(&q);
    assert_eq!(order_exprs(&keys).len(), 2);
    assert_eq!(key_slots(&keys, &cols, [7, 5]).unwrap(), vec![7, 2, 5]);
    let wire = wire_keys(&keys, &cols, [7, 5]).unwrap();
    assert_eq!(
        wire.iter().map(|k| (k.col, k.desc)).collect::<Vec<_>>(),
        [(7, false), (2, false), (5, true)]
    );
    assert!(matches!(key_slots(&keys, &cols, [7]), Err(GnitzSqlError::Internal(_))));
}

#[test]
fn null_placement_defaults_follow_the_direction() {
    let q = parse_query("SELECT * FROM t ORDER BY a, b DESC, c ASC NULLS FIRST, a DESC NULLS LAST");
    let dirs: Vec<(bool, bool)> = keys_of(&q).iter().map(OrderKey::dir).collect();
    assert_eq!(dirs, [(false, false), (true, true), (false, true), (true, false)]);
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

/// `ORDER BY ALL` parses as the identifier `ALL` under the dialect the planner
/// uses, so the typed form is built here.
#[test]
fn order_by_all_is_rejected() {
    let all = OrderBy {
        kind: OrderByKind::All(OrderByOptions::default()),
        interpolate: None,
    };
    assert_eq!(rejected(resolve_order_by(&all)), "ORDER BY: ALL is not supported");
}
