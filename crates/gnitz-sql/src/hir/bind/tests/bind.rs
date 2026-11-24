use super::*;
use crate::test_support::{TestCatalog, assert_rejects, col, ncol, parse_query, rejected, rel, shadowed_cols, table};
use gnitz_wire::{RelClass, TypeCode};
use std::rc::Rc;

/// `t(id BIGINT PK, a BIGINT, b BIGINT NULL, f DOUBLE NULL)`,
/// `u(uid BIGINT PK, a BIGINT)` and the stream `st(id BIGINT PK, a BIGINT)` —
/// one catalog a linear, a join and a bag body bind against.
fn catalog() -> TestCatalog {
    let i = TypeCode::I64;
    crate::test_support::catalog(vec![
        (
            "t",
            table(
                1,
                vec![col("id", i), col("a", i), ncol("b", i), ncol("f", TypeCode::F64)],
                vec![0],
            ),
        ),
        ("u", table(2, vec![col("uid", i), col("a", i)], vec![0])),
        (
            "st",
            rel(
                3,
                RelClass::Stream,
                vec![col("id", i), col("a", i)],
                vec![0],
                Vec::new(),
            ),
        ),
    ])
}

/// Bind one view body as `CREATE VIEW` binds it.
pub(in crate::hir) fn bound(sql: &str) -> Rc<RelExpr> {
    let (cat, ids) = (catalog(), ColIdGen::new());
    let mut cx = BindCx::new(
        &cat,
        &ids,
        Surface::View(ViewBody {
            stmt: "CREATE VIEW",
            replacing: None,
            bounded: false,
        }),
    );
    bind_query(&mut cx, &parse_query(sql)).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}

/// Each output column's `(name, type, nullable)`, as the view schema publishes
/// it. An outer join widens its null-supplying side in scope, and a USING keeps
/// the preserved side's copy. A derived table's `AS d(col…)` names its columns
/// through a join and a GROUP BY. A computed group key reads as computed, and an
/// aggregate over a computed argument takes its default name and the argument's
/// type, nullable only where the pre-map column holding it can be NULL.
#[test]
fn a_body_declares_its_output_columns() {
    use TypeCode::{F64, I64};
    for (sql, want) in [
        (
            "SELECT t.id, u.a FROM t RIGHT JOIN u ON t.a = u.a",
            &[("id", I64, true), ("a", I64, false)][..],
        ),
        (
            "SELECT * FROM t RIGHT JOIN u USING (a)",
            &[
                ("id", I64, true),
                ("b", I64, true),
                ("f", F64, true),
                ("uid", I64, false),
                ("a", I64, false),
            ],
        ),
        ("SELECT x FROM (SELECT a FROM t) AS d(x)", &[("x", I64, false)]),
        (
            "SELECT x, COUNT(*) AS n FROM (SELECT a FROM t) AS d(x) GROUP BY x",
            &[("x", I64, false), ("n", I64, false)],
        ),
        (
            "SELECT x, uid FROM u LEFT JOIN (SELECT a FROM t) AS d(x) ON x = u.a",
            &[("x", I64, true), ("uid", I64, false)],
        ),
        (
            "SELECT a + 1, SUM(a * 2) FROM t GROUP BY a + 1",
            &[("_expr0", I64, false), ("_sum1", I64, false)],
        ),
        (
            "SELECT a, SUM(f * 2) FROM t GROUP BY a",
            &[("a", I64, false), ("_sum1", F64, true)],
        ),
    ] {
        let rel = bound(sql);
        let cols = rel.cols();
        let got: Vec<_> = cols
            .iter()
            .map(|c| (&*c.def.name, c.def.ty.tc, c.def.is_nullable))
            .collect();
        assert_eq!(got, want, "{sql}");
    }
}

/// A grouped body's reduce: the set it reduces over, and its physical columns.
/// A DISTINCT aggregate is the plain aggregate over `Distinct(group cols, arg)`,
/// and every DISTINCT aggregate of one body rides that one set, HAVING's
/// included. `MIN`/`MAX(DISTINCT x)` is `MIN`/`MAX(x)`, so it needs no set, and
/// neither does an argument that keeps the source's PK. Aggregates computing one
/// op over one argument share one column. The set names the projection under the
/// `Distinct`, `<computed>` for a materialized column.
#[test]
fn a_reduce_reads_one_distinct_set_and_shares_its_columns() {
    use gnitz_wire::AggFunc::{Count, CountNonNull, Max, Min, Sum};
    for (sql, want_set, want_ops) in [
        (
            "SELECT a, COUNT(DISTINCT b) FROM t GROUP BY a",
            Some("a b"),
            &[CountNonNull][..],
        ),
        ("SELECT COUNT(DISTINCT b) FROM t", Some("b"), &[CountNonNull]),
        (
            "SELECT COUNT(DISTINCT a + 1) FROM t GROUP BY b",
            Some("b <computed>"),
            &[Count],
        ),
        (
            "SELECT a, COUNT(DISTINCT b), SUM(DISTINCT b), MAX(DISTINCT b) FROM t GROUP BY a \
             HAVING COUNT(DISTINCT b) > 1",
            Some("a b"),
            &[CountNonNull, Sum, Max],
        ),
        ("SELECT MIN(DISTINCT b), MAX(DISTINCT a) FROM t", None, &[Min, Max]),
        // Keeps company the all-or-nothing DISTINCT rule would otherwise refuse.
        (
            "SELECT a, MAX(DISTINCT b), COUNT(*) FROM t GROUP BY a",
            None,
            &[Max, Count],
        ),
        // A float argument is no key here, because nothing hashes it.
        ("SELECT MIN(DISTINCT f) FROM t", None, &[Min]),
        ("SELECT a, COUNT(DISTINCT id) FROM t GROUP BY a", None, &[Count]),
        // Over a nullable `b`, SUM's companion, AVG's two columns and COUNT(b) are
        // one `Sum(b)` and one `CountNonNull(b)`; an inert DISTINCT is one `Max(b)`.
        (
            "SELECT SUM(b), AVG(b), COUNT(b) FROM t GROUP BY a",
            None,
            &[Sum, CountNonNull],
        ),
        ("SELECT a, MAX(DISTINCT b), MAX(b) FROM t GROUP BY a", None, &[Max]),
    ] {
        let rel = bound(sql);
        let RelExpr::Project { input, .. } = rel.as_ref() else {
            panic!("{sql}: no projection")
        };
        let input = match input.as_ref() {
            RelExpr::Filter { input, .. } => input,
            _ => input,
        };
        let RelExpr::Reduce { input, aggs, .. } = input.as_ref() else {
            panic!("{sql}: no reduce")
        };
        let set = match input.as_ref() {
            RelExpr::Distinct { input } => {
                let RelExpr::Project { items, .. } = input.as_ref() else {
                    panic!("{sql}: no distinct projection")
                };
                let names: Vec<&str> = items
                    .iter()
                    .map(|e| {
                        if e.out.def.is_hidden {
                            "<computed>"
                        } else {
                            &*e.out.def.name
                        }
                    })
                    .collect();
                Some(names.join(" "))
            }
            _ => None,
        };
        assert_eq!(set.as_deref(), want_set, "{sql}");
        assert_eq!(aggs.iter().map(|c| c.op).collect::<Vec<_>>(), want_ops, "{sql}");
    }
}

/// A position names a visible column, counting the visible ones only, and an
/// unqualified name the visible column of that name; a qualified one is left to
/// the caller's scope, which checks the qualifier.
#[test]
fn an_output_key_is_a_position_or_an_unqualified_name() {
    let ids = ColIdGen::new();
    let cols: Vec<HirCol> = shadowed_cols()
        .into_iter()
        .map(|d| HirCol::new(ids.next(), d))
        .collect();
    let key = |src: &str| {
        let q = parse_query(&format!("SELECT * FROM t ORDER BY {src}"));
        let keys = parse_order_by(q.order_by.as_ref()).unwrap();
        let at = output_key(&keys[0].target, cols.iter())?.ok();
        Ok(at.map(|id| cols.iter().position(|c| c.id == id).unwrap()))
    };
    for (src, want) in [
        ("ID", Some(1)),
        ("(id)", Some(1)),
        ("t.id", None),
        ("id + 1", None),
        ("_k", None),
        ("1", Some(1)),
        ("3", Some(4)),
    ] {
        assert_eq!(key(src).unwrap(), want, "{src}");
    }
    for pos in [0, 4] {
        assert_eq!(
            rejected(key(&pos.to_string())),
            format!("ORDER BY position {pos} is out of range (1..=3)")
        );
    }
}

#[test]
fn positional_aliases_rename_the_visible_columns_in_order() {
    let rename = |aliases: &[&str]| {
        let aliases: Vec<Ident> = aliases.iter().map(|a| Ident::new(*a)).collect();
        let mut defs = vec![
            col("_k", TypeCode::U64).hidden(),
            col("a", TypeCode::U64),
            col("b", TypeCode::U64),
        ];
        apply_positional_aliases(aliases.iter(), &mut defs, "CTE 'd'")
            .map(|()| defs.into_iter().map(|c| c.name.to_string()).collect::<Vec<_>>())
    };
    assert_eq!(rename(&["x", "y"]).unwrap(), ["_k", "x", "y"]);
    assert_eq!(rename(&[]).unwrap(), ["_k", "a", "b"]);
    assert_eq!(
        rejected(rename(&["x"])),
        "CTE 'd' defines 1 column aliases but body returns 2 columns"
    );
    assert_rejects("x, X", rename(&["x", "X"]), "duplicate column name 'X'");
}
