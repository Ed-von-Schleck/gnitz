//! Shared `#[cfg(test)]` test helpers: column/schema builders and `sqlparser`
//! expression literals reused across the codec, exec, and dml unit tests. A
//! single source of truth so the canonical schemas (PK widths, UUID columns,
//! compound PKs) and the literal-expression shapes can't drift between modules.

use crate::ir::BoundExpr;
use gnitz_core::{ColumnDef, Schema, TypeCode, ZSetBatch};
use sqlparser::ast::{BinaryOperator, Expr, Ident, UnaryOperator, Value};

pub(crate) fn col_def(name: &str, tc: TypeCode, nullable: bool) -> ColumnDef {
    ColumnDef::new(name, tc, nullable)
}

/// `(pk pk_tc, v I64)` — single-column PK of a chosen type plus one payload.
pub(crate) fn pk_schema(pk_tc: TypeCode) -> Schema {
    Schema {
        columns: vec![col_def("id", pk_tc, false), col_def("v", TypeCode::I64, false)],
        pk_cols: vec![0],
    }
}

/// `(pk U64, val val_tc nullable)` — single-PK schema with one nullable payload.
pub(crate) fn two_col(val_tc: TypeCode) -> Schema {
    Schema {
        columns: vec![col_def("pk", TypeCode::U64, false), col_def("val", val_tc, true)],
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
    b.columns[1].extend(val_bytes);
    b
}

/// A single UUID PK column.
pub(crate) fn uuid_schema_pk() -> Schema {
    Schema {
        columns: vec![col_def("id", TypeCode::UUID, false)],
        pk_cols: vec![0],
    }
}

/// `(pk U64, uid UUID nullable)` — a UUID in a non-PK (payload) slot.
pub(crate) fn uuid_schema_payload() -> Schema {
    Schema {
        columns: vec![
            col_def("pk", TypeCode::U64, false),
            col_def("uid", TypeCode::UUID, true),
        ],
        pk_cols: vec![0],
    }
}

/// `(a U64, b U64) PRIMARY KEY (a, b)` plus a nullable I64 payload — the
/// canonical compound-PK test schema (`pk_stride = 16`).
pub(crate) fn compound_schema_u64_u64() -> Schema {
    Schema {
        columns: vec![
            col_def("a", TypeCode::U64, false),
            col_def("b", TypeCode::U64, false),
            col_def("v", TypeCode::I64, true),
        ],
        pk_cols: vec![0, 1],
    }
}

/// An unsigned decimal literal, e.g. `42`.
pub(crate) fn num_expr(n: &str) -> Expr {
    Expr::value(Value::Number(n.into(), false))
}

/// A negated decimal literal, e.g. `-1` (a `UnaryOp(Minus)` over a number).
pub(crate) fn neg_num_expr(n: &str) -> Expr {
    Expr::UnaryOp {
        op: UnaryOperator::Minus,
        expr: Box::new(num_expr(n)),
    }
}

/// A single-quoted string literal (a valid UUID seek key).
pub(crate) fn uuid_str_expr(s: &str) -> Expr {
    Expr::value(Value::SingleQuotedString(s.into()))
}

/// A double-quoted token — an identifier in `GenericDialect`, never a literal.
pub(crate) fn dquote_expr(s: &str) -> Expr {
    Expr::value(Value::DoubleQuotedString(s.into()))
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

/// Parse + bind a WHERE predicate against `schema` into its bound conjuncts —
/// the production shape, and the input of the `access` recognizers. The relation
/// is `t`, which is what every qualified reference in these tests writes.
pub(crate) fn bind_where(sql: &str, schema: &Schema) -> Vec<BoundExpr> {
    crate::dml::plan::bind_where(schema, "t", Some(&parse_expr_sql(sql))).expect("bind WHERE")
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

/// Non-unique `IndexMeta` list from raw column-index lists.
pub(crate) fn idx_metas(col_lists: &[&[u32]]) -> Vec<gnitz_core::IndexMeta> {
    let flagged: Vec<(&[u32], bool)> = col_lists.iter().map(|cols| (*cols, false)).collect();
    idx_metas_flagged(&flagged)
}

/// `IndexMeta` list from raw column-index lists, each with its `is_unique` flag.
pub(crate) fn idx_metas_flagged(col_lists: &[(&[u32], bool)]) -> Vec<gnitz_core::IndexMeta> {
    col_lists
        .iter()
        .map(|(cols, is_unique)| gnitz_core::IndexMeta {
            cols: gnitz_core::PkColList::from_slice(cols),
            is_unique: *is_unique,
        })
        .collect()
}
