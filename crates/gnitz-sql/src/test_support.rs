//! Shared `#[cfg(test)]` helpers: column, schema, catalog and batch builders,
//! and the parse/bind entry points the unit tests and the suites drive.

use crate::bind::Catalog;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::{BatchAppender, PlannedView, RelDescriptor, Schema, ZSetBatch};
use gnitz_expr::{ColumnLocator, SchemaFacts};
use gnitz_wire::{payload_str, payload_u64};
use gnitz_wire::{ColType, ColumnDef, FixedInt, PkColList, RelClass, RelIndex, TypeCode};
use sqlparser::ast::Expr;
use std::sync::Arc;

/// The schema every test catalog resolves under.
pub(crate) const SN: &str = "s";

/// A NOT NULL column.
pub(crate) fn col(name: &str, tc: TypeCode) -> ColumnDef {
    ColumnDef::new(name, tc, false)
}

/// A nullable column.
pub(crate) fn ncol(name: &str, tc: TypeCode) -> ColumnDef {
    ColumnDef::new(name, tc, true)
}

/// A schema, held to the rules every decoded schema is.
pub(crate) fn schema(columns: Vec<ColumnDef>, pk_cols: &[u32]) -> Schema {
    Schema::from_parts(columns, pk_cols.to_vec()).expect("a valid test schema")
}

/// A non-unique secondary index over `cols`.
pub(crate) fn ix(cols: &[u32]) -> RelIndex {
    RelIndex {
        cols: PkColList::from_slice(cols),
        is_unique: false,
    }
}

/// A unique secondary index over `cols`.
pub(crate) fn uq(cols: &[u32]) -> RelIndex {
    RelIndex { is_unique: true, ..ix(cols) }
}

/// A relation descriptor.
pub(crate) fn rel(
    tid: u64,
    class: RelClass,
    columns: Vec<ColumnDef>,
    pk_cols: Vec<u32>,
    indexes: Vec<RelIndex>,
) -> Arc<RelDescriptor> {
    Arc::new(RelDescriptor {
        tid,
        class,
        pk_repeats: class == RelClass::Stream,
        serial: false,
        schema: Arc::new(schema(columns, &pk_cols)),
        indexes,
        token: 0,
    })
}

/// A plain, partitioned, unindexed base table.
pub(crate) fn table(tid: u64, columns: Vec<ColumnDef>, pk_cols: Vec<u32>) -> Arc<RelDescriptor> {
    rel(tid, RelClass::Table, columns, pk_cols, Vec::new())
}

/// A catalog holding `rels` under [`SN`]; any other name is absent.
pub(crate) fn catalog(rels: Vec<(&str, Arc<RelDescriptor>)>) -> Catalog<'static> {
    let cat = Catalog::complete(SN);
    for (name, desc) in rels {
        cat.insert(name, Some(desc));
    }
    cat
}

/// Register `view` under `name` as a `class` relation, as the server would after
/// `CREATE VIEW`.
pub(crate) fn register(cat: &Catalog<'_>, name: &str, tid: u64, class: RelClass, view: &PlannedView) {
    cat.insert(
        name,
        Some(Arc::new(RelDescriptor {
            tid,
            class,
            pk_repeats: view.pk_repeats,
            serial: false,
            schema: Arc::clone(&view.schema),
            indexes: Vec::new(),
            token: 0,
        })),
    );
}

/// `(id pk_tc PK, v I64)`.
pub(crate) fn pk_schema(pk_tc: TypeCode) -> Schema {
    schema(vec![col("id", pk_tc), col("v", TypeCode::I64)], &[0])
}

/// `(pk U64 PK, val val_tc nullable)`.
pub(crate) fn two_col(val_tc: TypeCode) -> Schema {
    schema(vec![col("pk", TypeCode::U64), ncol("val", val_tc)], &[0])
}

/// `(pk U64 PK, c1, c2, …)`, the payload columns of types `tys`, all nullable.
pub(crate) fn typed_schema<T: Into<ColType> + Copy>(tys: &[T]) -> Schema {
    let mut columns = vec![col("pk", TypeCode::U64)];
    columns.extend(
        tys.iter()
            .enumerate()
            .map(|(i, &ty)| ColumnDef::typed(format!("c{}", i + 1), ty.into(), true)),
    );
    schema(columns, &[0])
}

/// `(a U64, b U64) PRIMARY KEY (a, b)` plus a nullable I64 payload.
pub(crate) fn compound_schema_u64_u64() -> Schema {
    schema(
        vec![
            col("a", TypeCode::U64),
            col("b", TypeCode::U64),
            ncol("v", TypeCode::I64),
        ],
        &[0, 1],
    )
}

/// One payload cell, written into a batch or read back out of a result.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Cell<'a> {
    Int(i128),
    F64(f64),
    Str(&'a str),
    Null,
}

impl Cell<'_> {
    /// Append this cell as the row's next payload column.
    pub(crate) fn push(self, app: &mut BatchAppender<'_>) {
        match self {
            Cell::Int(v) => app.int_val(v),
            Cell::F64(v) => app.f64_val(v),
            Cell::Str(s) => app.str_val(s),
            Cell::Null => app.null(),
        };
    }
}

/// A batch of `schema`, single-column PK: one weight-1 row per entry of `rows`,
/// its PK the row's 1-based position, its payload the entry's cells.
pub(crate) fn batch_of(schema: &Schema, rows: &[&[Cell]]) -> ZSetBatch {
    let mut b = ZSetBatch::new(schema);
    let mut app = BatchAppender::new(&mut b);
    for (r, cells) in rows.iter().enumerate() {
        app.add_row(r as u128 + 1, 1);
        for cell in *cells {
            cell.push(&mut app);
        }
    }
    b
}

/// Each row of `b` (in `schema`) as its payload cells and weight — what
/// [`batch_of`] wrote, read back.
pub(crate) fn rows_of<'a>(schema: &Schema, b: &'a ZSetBatch) -> Vec<(Vec<Cell<'a>>, i64)> {
    let locs = schema.payload_locators();
    let cell = |r: usize, pi: usize, loc: &ColumnLocator| {
        let tc = loc.type_code();
        if loc.is_null(b, r) {
            Cell::Null
        } else if tc.is_german_string() {
            Cell::Str(payload_str(b, r, pi).unwrap())
        } else if tc == TypeCode::F64 {
            Cell::F64(f64::from_bits(payload_u64(b, r, pi)))
        } else {
            let fi = FixedInt::from_type_code(tc).expect("an integer column");
            let v = loc.decode_i64(b, r, fi);
            Cell::Int(if fi.is_signed() { v as i128 } else { v as u64 as i128 })
        }
    };
    (0..b.len())
        .map(|r| {
            (
                locs.iter().enumerate().map(|(pi, loc)| cell(r, pi, loc)).collect(),
                b.weights[r],
            )
        })
        .collect()
}

/// Parse + bind an expression against `schema` as relation `t`, which is what
/// every qualified reference in these tests writes.
pub(crate) fn bind_sql(sql: &str, schema: &Schema) -> Result<BoundExpr, GnitzSqlError> {
    crate::bind::bind_single_table(&parse_expr_sql(sql), schema, "t")
}

/// The message of the `Rejected` error `r` must be.
pub(crate) fn rejected<T>(r: Result<T, GnitzSqlError>) -> String {
    match r {
        Err(GnitzSqlError::Rejected(m)) => m,
        Err(e) => panic!("expected Rejected, got {e:?}"),
        Ok(_) => panic!("expected Rejected, got Ok"),
    }
}

/// Assert `r` is a rejection whose message contains `want` — the guard's
/// identity, not its wording. `what` labels the failure (the SQL, usually).
pub(crate) fn assert_rejects<T>(what: &str, r: Result<T, GnitzSqlError>, want: &str) {
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

/// [`bind_sql`] of a WHERE predicate, as its bound conjuncts.
pub(crate) fn bind_where(sql: &str, schema: &Schema) -> Vec<BoundExpr> {
    bind_sql(sql, schema)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .conjuncts()
}

/// [`bind_where`] for a predicate that is one conjunct — the input of the
/// single-conjunct recognizers.
pub(crate) fn bind_conjunct(sql: &str, schema: &Schema) -> BoundExpr {
    let mut conjuncts = bind_where(sql, schema);
    assert_eq!(conjuncts.len(), 1, "{sql}: one conjunct");
    conjuncts.pop().expect("one conjunct")
}

/// Parse a bare SQL expression (e.g. a WHERE predicate) via `GenericDialect`.
pub(crate) fn parse_expr_sql(src: &str) -> Expr {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    Parser::new(&GenericDialect {})
        .try_with_sql(src)
        .unwrap()
        .parse_expr()
        .unwrap()
}

/// The first statement of `sql`, parsed via `GenericDialect` — the same dialect
/// the planner parses with, so a test sees the AST shape the planner receives.
pub(crate) fn parse_stmt(sql: &str) -> sqlparser::ast::Statement {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    Parser::parse_sql(&GenericDialect {}, sql)
        .expect("parses")
        .into_iter()
        .next()
        .expect("one statement")
}

/// [`parse_stmt`] narrowed to the `Query` it must be.
pub(crate) fn parse_query(sql: &str) -> sqlparser::ast::Query {
    match parse_stmt(sql) {
        sqlparser::ast::Statement::Query(q) => *q,
        other => panic!("not a query: {other}"),
    }
}
