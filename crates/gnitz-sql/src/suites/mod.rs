//! The planner as a pure function of `(statement, catalog)`.
//!
//! Every suite here hands a hand-built catalog to the real planning entry
//! points — no server, no socket, no `GnitzClient`. Compiling a `CREATE VIEW` to
//! a `Circuit` and a `SELECT` to a `ReadSpec` reaches nothing, so a rejection, an
//! output schema or a circuit shape is asserted in-process.

mod plan_create_table;
mod plan_read;
mod plan_view_circuit;
mod plan_view_rejections;
mod plan_view_schema;
mod plan_view_window;

use std::cell::RefCell;

use crate::bind::Catalog;
use crate::dml::ReadPlan;
use crate::error::GnitzSqlError;
use crate::hir::{plan_alter_view, plan_create_view, PlannedChain};
use crate::test_support::*;
use gnitz_core::PlannedView;
use gnitz_wire::{Circuit, OpNode, RelClass, TypeCode};
use sqlparser::ast::Statement;

/// Assert `r` is a rejection whose message contains `want` — the guard's
/// identity, not its wording. `what` labels the failure (the SQL, usually).
fn assert_rejects<T>(what: &str, r: Result<T, GnitzSqlError>, want: &str) {
    let msg = match r {
        Err(GnitzSqlError::Rejected(m)) => m,
        Err(e) => panic!("`{what}`: expected a rejection, got {e:?}"),
        Ok(_) => panic!("`{what}` was accepted"),
    };
    assert!(
        msg.contains(want),
        "for `{what}`\n  expected substring: {want:?}\n  got: {msg:?}"
    );
}

/// The fixture every pure file plans against. Every `BIGINT` column is `I64`.
///
/// | name | shape |
/// |---|---|
/// | `t` | `(id PK, g, v)`, indexed on `v` |
/// | `u` | `(id PK, g, v)` |
/// | `a` | `(id PK, k, v)` |
/// | `b` | `(id PK, k, w)` |
/// | `n` | `(id PK, k NULL, v NULL)` — nullable join keys and values |
/// | `ty` | `(id PK, s TEXT, f DOUBLE, big UINT128, uid UUID, i32c INT, u8c U8, i16c SMALLINT)` |
/// | `c` | `(a U64, b U64, v)` with `PRIMARY KEY (a, b)` |
/// | `r` | `(id PK, v)` |
/// | `tv` | a view with `t`'s columns |
/// | `bv` | a capacity-bounded view with `t`'s columns |
/// | `fv` | a view with a delta feed, with `t`'s columns |
fn base() -> Catalog<'static> {
    let i = TypeCode::I64;
    let tgv = || vec![col("id", i), col("g", i), col("v", i)];
    catalog(vec![
        ("t", rel(16, RelClass::Table, tgv(), vec![0], vec![ix(&[2])])),
        ("u", table(17, tgv(), vec![0])),
        ("a", table(18, vec![col("id", i), col("k", i), col("v", i)], vec![0])),
        ("b", table(19, vec![col("id", i), col("k", i), col("w", i)], vec![0])),
        ("n", table(20, vec![col("id", i), ncol("k", i), ncol("v", i)], vec![0])),
        (
            "ty",
            table(
                21,
                vec![
                    col("id", i),
                    col("s", TypeCode::String),
                    col("f", TypeCode::F64),
                    col("big", TypeCode::U128),
                    col("uid", TypeCode::UUID),
                    col("i32c", TypeCode::I32),
                    col("u8c", TypeCode::U8),
                    col("i16c", TypeCode::I16),
                ],
                vec![0],
            ),
        ),
        (
            "c",
            table(
                22,
                vec![col("a", TypeCode::U64), col("b", TypeCode::U64), col("v", i)],
                vec![0, 1],
            ),
        ),
        (
            "r",
            rel(23, RelClass::Table, vec![col("id", i), col("v", i)], vec![0], vec![]),
        ),
        ("tv", rel(24, RelClass::View, tgv(), vec![0], vec![])),
        ("bv", rel(25, RelClass::BoundedView, tgv(), vec![0], vec![])),
        ("fv", rel(26, RelClass::FedView, tgv(), vec![0], vec![])),
    ])
}

/// Plan a `CREATE VIEW` / `ALTER VIEW … AS` against `cat`. None of the statements
/// here carry `IF NOT EXISTS`, the one clause that plans nothing.
fn plan(cat: &Catalog<'_>, sql: &str) -> Result<PlannedChain, GnitzSqlError> {
    match parse_stmt(sql) {
        Statement::CreateView(cv) => {
            plan_create_view(&cv, cat).map(|p| p.unwrap_or_else(|| panic!("`{sql}` planned nothing")))
        }
        Statement::AlterView { name, columns, query, with_options } => {
            plan_alter_view(&name, &columns, &query, &with_options, cat)
        }
        _ => panic!("`{sql}` is not a view statement"),
    }
}

/// Plan a read (`SELECT` / `EXPLAIN`) against `cat`.
fn read(cat: &Catalog<'_>, sql: &str) -> Result<ReadPlan, GnitzSqlError> {
    crate::dml::plan_read(&parse_stmt(sql), cat)
}

/// The user-named view of a planned chain.
fn final_view(chain: &PlannedChain) -> &PlannedView {
    &chain.bundle.view
}

/// Every view of a planned chain, its segments first.
fn all_views(chain: &PlannedChain) -> impl Iterator<Item = &PlannedView> {
    chain.bundle.segments.iter().chain([&chain.bundle.view])
}

/// How many views a planned chain commits, the user-named view included.
fn view_count(chain: &PlannedChain) -> usize {
    chain.bundle.segments.len() + 1
}

/// `CREATE VIEW v AS <body>` planned against `cat`, unwrapped.
fn view(cat: &Catalog<'_>, body: &str) -> PlannedChain {
    plan(cat, &format!("CREATE VIEW v AS {body}")).unwrap_or_else(|e| panic!("`{body}`: {e:?}"))
}

/// The final view's output columns as `(name, hidden, nullable)`.
fn output_shape(chain: &PlannedChain) -> Vec<(String, bool, bool)> {
    final_view(chain)
        .schema
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.is_hidden, c.is_nullable))
        .collect()
}

/// How many of `circuit`'s nodes satisfy `pred`.
fn count(circuit: &Circuit, pred: impl Fn(&OpNode) -> bool) -> usize {
    circuit.nodes().iter().filter(|n| pred(&n.op)).count()
}

/// Plan against a catalog that resolves each name out of `known`, recording the
/// names the planner asked for, in order.
fn resolving<T>(
    known: &Catalog<'_>,
    plan: impl FnOnce(&Catalog<'_>) -> Result<T, GnitzSqlError>,
) -> (Result<T, GnitzSqlError>, Vec<String>) {
    let asked = RefCell::new(Vec::new());
    let ask = |n: &str| {
        asked.borrow_mut().push(n.to_string());
        known.probe(n)
    };
    let r = plan(&Catalog::new(SN, &ask));
    (r, asked.into_inner())
}
