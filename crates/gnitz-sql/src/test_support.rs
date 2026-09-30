//! Shared `#[cfg(test)]` test helpers: column/schema builders and `sqlparser`
//! expression literals reused across the codec, exec, and dml unit tests. A
//! single source of truth so the canonical schemas (PK widths, UUID columns,
//! compound PKs) and the literal-expression shapes can't drift between modules.

use crate::bind::Catalog;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::{PkColumn, PlannedView, RelDescriptor, Schema, ZSetBatch};
use gnitz_wire::{ColumnDef, PkBuf, PkColList, RelClass, RelIndex, TypeCode};
use sqlparser::ast::{BinaryOperator, Expr, Ident, Value};
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

/// A relation descriptor over `indexes`.
fn descriptor(
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
        schema: Arc::new(Schema { columns, pk_cols }),
        indexes,
    })
}

/// A relation descriptor. `indexes` lists secondary indexes as
/// `(column indices, is_unique)`.
pub(crate) fn rel_with(
    tid: u64,
    class: RelClass,
    columns: Vec<ColumnDef>,
    pk_cols: Vec<u32>,
    indexes: &[(&[u32], bool)],
) -> Arc<RelDescriptor> {
    descriptor(tid, class, columns, pk_cols, idx_metas_flagged(indexes))
}

/// [`rel_with`], every index non-unique — what a read plan cares about.
pub(crate) fn rel(
    tid: u64,
    class: RelClass,
    columns: Vec<ColumnDef>,
    pk_cols: Vec<u32>,
    indexes: &[&[u32]],
) -> Arc<RelDescriptor> {
    descriptor(tid, class, columns, pk_cols, idx_metas(indexes))
}

/// A plain, partitioned, unindexed base table.
pub(crate) fn table(tid: u64, columns: Vec<ColumnDef>, pk_cols: Vec<u32>) -> Arc<RelDescriptor> {
    rel(tid, RelClass::Table, columns, pk_cols, &[])
}

/// A resolver that knows no relation.
fn absent(_: &str) -> Result<Option<Arc<RelDescriptor>>, GnitzSqlError> {
    Ok(None)
}

/// A catalog holding `rels` under [`SN`]; any other name is absent.
pub(crate) fn catalog(rels: Vec<(&str, Arc<RelDescriptor>)>) -> Catalog<'static> {
    let cat = Catalog::new(SN, &absent);
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
        })),
    );
}

/// `(pk pk_tc, v I64)` — single-column PK of a chosen type plus one payload.
pub(crate) fn pk_schema(pk_tc: TypeCode) -> Schema {
    Schema {
        columns: vec![col("id", pk_tc), col("v", TypeCode::I64)],
        pk_cols: vec![0],
    }
}

/// `(pk U64, val val_tc nullable)` — single-PK schema with one nullable payload.
pub(crate) fn two_col(val_tc: TypeCode) -> Schema {
    Schema {
        columns: vec![col("pk", TypeCode::U64), ncol("val", val_tc)],
        pk_cols: vec![0],
    }
}

/// A single-row batch for [`two_col`]: pk = 1, the given payload bytes/null bits.
pub(crate) fn batch_2col(val_bytes: Vec<u8>, val_tc: TypeCode, null_bits: u64) -> ZSetBatch {
    let schema = two_col(val_tc);
    let mut b = ZSetBatch::new(&schema);
    b.pks.push_u128(&schema, 1u128);
    b.weights.push(1);
    b.nulls.push(null_bits);
    b.payload[0].bytes.extend(val_bytes);
    b
}

/// A single UUID PK column.
pub(crate) fn uuid_schema_pk() -> Schema {
    Schema {
        columns: vec![col("id", TypeCode::UUID)],
        pk_cols: vec![0],
    }
}

/// `(pk U64, uid UUID nullable)` — a UUID in a non-PK (payload) slot.
pub(crate) fn uuid_schema_payload() -> Schema {
    Schema {
        columns: vec![col("pk", TypeCode::U64), ncol("uid", TypeCode::UUID)],
        pk_cols: vec![0],
    }
}

/// `(a U64, b U64) PRIMARY KEY (a, b)` plus a nullable I64 payload — the
/// canonical compound-PK test schema (`pk_stride = 16`).
pub(crate) fn compound_schema_u64_u64() -> Schema {
    Schema {
        columns: vec![
            col("a", TypeCode::U64),
            col("b", TypeCode::U64),
            ncol("v", TypeCode::I64),
        ],
        pk_cols: vec![0, 1],
    }
}

/// A numeric literal bound as the binder binds it, e.g. `lit("1.5")`.
pub(crate) fn lit(n: &str) -> BoundExpr {
    crate::ast_util::bind_literal(&Value::Number(n.into(), false)).expect("a numeric literal")
}

/// A `col = rhs` equality expression (AST), for building recognizer/parity inputs.
pub(crate) fn eq_expr(col: &str, rhs: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new(col))),
        op: BinaryOperator::Eq,
        right: Box::new(rhs),
    }
}

/// A `col IN (items…)` expression (AST).
pub(crate) fn in_list_expr(col: &str, items: Vec<Expr>) -> Expr {
    Expr::InList {
        expr: Box::new(Expr::Identifier(Ident::new(col))),
        list: items,
        negated: false,
    }
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

/// [`bind_sql`] of a WHERE predicate, as its bound conjuncts — the production
/// shape, and the input of the `access` recognizers.
pub(crate) fn bind_where(sql: &str, schema: &Schema) -> Vec<BoundExpr> {
    bind_sql(sql, schema).expect("bind WHERE").conjuncts()
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

/// Non-unique `RelIndex` list from raw column-index lists.
pub(crate) fn idx_metas(col_lists: &[&[u32]]) -> Vec<RelIndex> {
    let flagged: Vec<(&[u32], bool)> = col_lists.iter().map(|cols| (*cols, false)).collect();
    idx_metas_flagged(&flagged)
}

/// `RelIndex` list from raw column-index lists, each with its `is_unique` flag.
pub(crate) fn idx_metas_flagged(col_lists: &[(&[u32], bool)]) -> Vec<RelIndex> {
    col_lists
        .iter()
        .map(|(cols, is_unique)| RelIndex {
            cols: PkColList::from_slice(cols),
            is_unique: *is_unique,
        })
        .collect()
}

/// The primary key of a VALUES row of written expressions, through the INSERT
/// path's [`PkPlan`](crate::dml::PkPlan) with the identity slot map.
pub(crate) fn extract_pk_value(row: &[Expr], schema: &Schema) -> Result<PkBuf, GnitzSqlError> {
    let slot_of: Vec<Option<usize>> = (0..row.len()).map(Some).collect();
    let cells = row
        .iter()
        .map(crate::bind::structural::bind_constant)
        .collect::<Result<Vec<_>, _>>()?;
    let mut pks = PkColumn::empty_for_schema(schema);
    crate::dml::PkPlan::written(&slot_of, schema)?.push(schema, 0, &cells, &mut pks)?;
    Ok(PkBuf::from_bytes(pks.get_bytes(0)))
}
