#![allow(dead_code)]

//! The planner as a pure function of `(statement, CatalogSnapshot)`.
//!
//! Every test built on this module hands a hand-built catalog to the real
//! planning entry points — no server, no socket, no `GnitzClient`. Compiling a
//! `CREATE VIEW` to a `Circuit` and a `SELECT` to a `ReadSpec` reaches nothing,
//! so a rejection, an output schema or a circuit shape is asserted in-process.
//! These files are not gated on the `integration` feature, so a bare
//! `cargo test` runs them.

use std::sync::Arc;

use gnitz_core::{
    CatalogSnapshot, Circuit, ColumnDef, IndexMeta, OpNode, PkColList, PlannedView, RelClass, RelDescriptor, Schema,
    TypeCode,
};
use gnitz_sql::sqlparser::ast::Statement;
use gnitz_sql::sqlparser::dialect::GenericDialect;
use gnitz_sql::sqlparser::parser::Parser;
use gnitz_sql::{plan_read, plan_view, GnitzSqlError, PlannedChain, ReadPlan, ViewPlan};

pub const SN: &str = "s";

pub fn parse(sql: &str) -> Statement {
    Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"))
        .into_iter()
        .next()
        .expect("one statement")
}

/// `Err` unwrapped without demanding `Debug` of the success value.
pub fn err_of<T>(r: Result<T, GnitzSqlError>) -> GnitzSqlError {
    match r {
        Err(e) => e,
        Ok(_) => panic!("expected a rejection"),
    }
}

/// The error's variant name and message.
pub fn variant_of(e: &GnitzSqlError) -> (&'static str, String) {
    match e {
        GnitzSqlError::Parse(m) => ("Parse", m.to_string()),
        GnitzSqlError::Bind(m) => ("Bind", m.clone()),
        GnitzSqlError::Plan(m) => ("Plan", m.clone()),
        GnitzSqlError::Exec(m) => ("Exec", m.to_string()),
        GnitzSqlError::Unsupported(m) => ("Unsupported", m.clone()),
        GnitzSqlError::CatalogMiss(m) => ("CatalogMiss", m.clone()),
        other => ("other", format!("{other:?}")),
    }
}

/// Assert `r` is the rejection `want_variant` whose message contains
/// `want_msg` — the guard's identity, not its wording. `what` labels the
/// failure (the SQL, usually).
pub fn assert_rejects<T>(what: &str, r: Result<T, GnitzSqlError>, want_variant: &str, want_msg: &str) {
    let e = match r {
        Err(e) => e,
        Ok(_) => panic!("`{what}` was accepted"),
    };
    let (variant, msg) = variant_of(&e);
    assert_eq!(variant, want_variant, "variant mismatch for `{what}`: {e:?}");
    assert!(
        msg.contains(want_msg),
        "for `{what}`\n  expected substring: {want_msg:?}\n  got: {msg:?}"
    );
}

/// A NOT NULL column.
pub fn col(name: &str, tc: TypeCode) -> ColumnDef {
    ColumnDef::new(name, tc, false)
}

/// A nullable column.
pub fn ncol(name: &str, tc: TypeCode) -> ColumnDef {
    ColumnDef::new(name, tc, true)
}

/// A relation descriptor. `indexes` lists secondary indexes by column index.
pub fn rel(
    tid: u64,
    class: RelClass,
    replicated: bool,
    columns: Vec<ColumnDef>,
    pk_cols: Vec<usize>,
    indexes: &[&[u32]],
) -> Arc<RelDescriptor> {
    Arc::new(RelDescriptor {
        tid,
        class,
        replicated,
        delta: false,
        schema: Arc::new(Schema { columns, pk_cols }),
        indexes: Arc::new(
            indexes
                .iter()
                .map(|cols| IndexMeta {
                    cols: PkColList::from_slice(cols),
                    is_unique: false,
                })
                .collect(),
        ),
    })
}

/// A plain, partitioned, unindexed base table.
pub fn table(tid: u64, columns: Vec<ColumnDef>, pk_cols: Vec<usize>) -> Arc<RelDescriptor> {
    rel(tid, RelClass::Table, false, columns, pk_cols, &[])
}

/// A catalog holding `rels` under [`SN`].
pub fn catalog(rels: Vec<(&str, Arc<RelDescriptor>)>) -> CatalogSnapshot {
    let mut cat = CatalogSnapshot::default();
    for (name, desc) in rels {
        cat.insert(SN, name, Some(desc));
    }
    cat
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
/// | `ty` | `(id PK, s TEXT, f DOUBLE, big DECIMAL(38,0), uid UUID, i32c INT, u8c U8, i16c SMALLINT)` |
/// | `c` | `(a U64, b U64, v)` with `PRIMARY KEY (a, b)` |
/// | `r` | `(id PK, v)`, replicated |
/// | `tv` | a view with `t`'s columns |
/// | `bv` | a capacity-bounded view with `t`'s columns |
pub fn base() -> CatalogSnapshot {
    let i = TypeCode::I64;
    let tgv = || vec![col("id", i), col("g", i), col("v", i)];
    catalog(vec![
        ("t", rel(16, RelClass::Table, false, tgv(), vec![0], &[&[2]])),
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
            rel(23, RelClass::Table, true, vec![col("id", i), col("v", i)], vec![0], &[]),
        ),
        ("tv", rel(24, RelClass::View, false, tgv(), vec![0], &[])),
        ("bv", rel(25, RelClass::BoundedView, false, tgv(), vec![0], &[])),
    ])
}

/// Plan a `CREATE VIEW` / `ALTER VIEW … AS` against `cat`. None of the statements
/// here carry `IF NOT EXISTS`, the one clause that plans a skip.
pub fn plan(cat: &CatalogSnapshot, sql: &str) -> Result<PlannedChain, GnitzSqlError> {
    plan_view(&parse(sql), cat, SN).map(|p| match p {
        ViewPlan::Create { chain, .. } => chain,
        ViewPlan::Skip { .. } => panic!("`{sql}` planned a skip"),
    })
}

/// Plan a read (`SELECT` / `EXPLAIN`) against `cat`.
pub fn read(cat: &CatalogSnapshot, sql: &str) -> Result<ReadPlan, GnitzSqlError> {
    plan_read(&parse(sql), cat, SN)
}

/// The user-named view of a planned chain — always its last element.
pub fn final_view(chain: &PlannedChain) -> &PlannedView {
    chain.views.last().expect("a chain has a final view")
}

/// `CREATE VIEW v AS <body>` planned against `cat`, unwrapped.
pub fn view(cat: &CatalogSnapshot, body: &str) -> PlannedChain {
    plan(cat, &format!("CREATE VIEW v AS {body}")).unwrap_or_else(|e| panic!("`{body}`: {e:?}"))
}

/// Register a planned view under `name` so a later body can read it, as the
/// server would after `CREATE VIEW`.
pub fn register(cat: &mut CatalogSnapshot, name: &str, tid: u64, chain: &PlannedChain) {
    let fv = final_view(chain);
    let class = if fv.capacity_bytes.is_some() {
        RelClass::BoundedView
    } else {
        RelClass::View
    };
    let pk_cols = fv.pk_cols.iter().map(|&c| c as usize).collect();
    cat.insert(
        SN,
        name,
        Some(rel(tid, class, false, fv.output_columns.clone(), pk_cols, &[])),
    );
}

/// The final view's output columns as `(name, hidden, nullable)`.
pub fn output_shape(chain: &PlannedChain) -> Vec<(String, bool, bool)> {
    final_view(chain)
        .output_columns
        .iter()
        .map(|c| (c.name.clone(), c.is_hidden, c.is_nullable))
        .collect()
}

/// How many of `circuit`'s nodes satisfy `pred`.
pub fn count(circuit: &Circuit, pred: impl Fn(&OpNode) -> bool) -> usize {
    circuit.nodes.values().filter(|op| pred(op)).count()
}

/// Plan `sql` the way `dispatch` does: run the pass, resolve each name it
/// reports missing out of `known`, and re-run. Returns the plan and the names
/// resolved, in the order the planner asked for them.
pub fn resolving<T>(
    known: &CatalogSnapshot,
    mut plan: impl FnMut(&CatalogSnapshot) -> Result<T, GnitzSqlError>,
) -> (Result<T, GnitzSqlError>, Vec<String>) {
    let mut cat = CatalogSnapshot::default();
    let mut asked = Vec::new();
    loop {
        match plan(&cat) {
            Err(GnitzSqlError::CatalogMiss(name)) => {
                cat.insert(SN, &name, known.get(SN, &name).flatten());
                asked.push(name);
            }
            other => return (other, asked),
        }
    }
}
