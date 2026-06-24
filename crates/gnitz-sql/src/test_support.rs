//! Shared `#[cfg(test)]` test helpers: column/schema builders and `sqlparser`
//! expression literals reused across the codec, exec, and dml unit tests. A
//! single source of truth so the canonical schemas (PK widths, UUID columns,
//! compound PKs) and the literal-expression shapes can't drift between modules.

use gnitz_core::{ColData, ColumnDef, Schema, TypeCode, ZSetBatch};
use sqlparser::ast::{Expr, UnaryOperator, Value, ValueWithSpan};
use sqlparser::tokenizer::Span;

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
    b.pks.push_u128(1u128);
    b.weights.push(1);
    b.nulls.push(null_bits);
    if let ColData::Fixed(ref mut buf) = b.columns[1] {
        buf.extend(val_bytes);
    }
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
    Expr::Value(ValueWithSpan {
        value: Value::Number(n.into(), false),
        span: Span::empty(),
    })
}

/// A negated decimal literal, e.g. `-1` (a `UnaryOp(Minus)` over a number).
pub(crate) fn neg_num_expr(n: &str) -> Expr {
    Expr::UnaryOp {
        op: UnaryOperator::Minus,
        expr: Box::new(Expr::Value(ValueWithSpan {
            value: Value::Number(n.into(), false),
            span: Span::empty(),
        })),
    }
}

/// A single-quoted string literal (a valid UUID seek key).
pub(crate) fn uuid_str_expr(s: &str) -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::SingleQuotedString(s.into()),
        span: Span::empty(),
    })
}

/// A double-quoted token — an identifier in `GenericDialect`, never a literal.
pub(crate) fn dquote_expr(s: &str) -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::DoubleQuotedString(s.into()),
        span: Span::empty(),
    })
}
