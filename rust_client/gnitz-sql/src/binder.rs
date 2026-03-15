use std::collections::HashMap;
use sqlparser::ast::{Expr, BinaryOperator, UnaryOperator, Value};
use gnitz_protocol::Schema;
use gnitz_core::GnitzClient;
use crate::error::GnitzSqlError;
use crate::logical_plan::{BoundExpr, BinOp, UnaryOp};

pub struct Binder<'a> {
    pub client:      &'a GnitzClient,
    pub schema_name: &'a str,
    cache:           HashMap<String, (u64, Schema)>,
}

impl<'a> Binder<'a> {
    pub fn new(client: &'a GnitzClient, schema_name: &'a str) -> Self {
        Binder { client, schema_name, cache: HashMap::new() }
    }

    pub fn resolve(&mut self, name: &str) -> Result<(u64, Schema), GnitzSqlError> {
        if let Some(entry) = self.cache.get(name) {
            return Ok(entry.clone());
        }
        let result = self.client.resolve_table_or_view_id(self.schema_name, name)
            .map_err(GnitzSqlError::Exec)?;
        self.cache.insert(name.to_string(), result.clone());
        Ok(result)
    }

    pub fn bind_expr(&self, expr: &Expr, schema: &Schema) -> Result<BoundExpr, GnitzSqlError> {
        match expr {
            Expr::Identifier(ident) => {
                let col_name = &ident.value;
                let idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                    .ok_or_else(|| GnitzSqlError::Bind(
                        format!("column '{}' not found", col_name)
                    ))?;
                Ok(BoundExpr::ColRef(idx))
            }
            Expr::Value(vws) => {
                match &vws.value {
                    Value::Number(n, _) => {
                        // Try integer first, then float
                        if let Ok(i) = n.parse::<i64>() {
                            Ok(BoundExpr::LitInt(i))
                        } else if let Ok(f) = n.parse::<f64>() {
                            Ok(BoundExpr::LitFloat(f))
                        } else {
                            Err(GnitzSqlError::Bind(format!("invalid number literal: {}", n)))
                        }
                    }
                    _ => Err(GnitzSqlError::Unsupported(
                        format!("value type not supported in expressions: {:?}", vws.value)
                    )),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.bind_expr(left, schema)?;
                let r = self.bind_expr(right, schema)?;
                let bop = match op {
                    BinaryOperator::Plus      => BinOp::Add,
                    BinaryOperator::Minus     => BinOp::Sub,
                    BinaryOperator::Multiply  => BinOp::Mul,
                    BinaryOperator::Divide    => BinOp::Div,
                    BinaryOperator::Modulo    => BinOp::Mod,
                    BinaryOperator::Eq        => BinOp::Eq,
                    BinaryOperator::NotEq     => BinOp::Ne,
                    BinaryOperator::Gt        => BinOp::Gt,
                    BinaryOperator::GtEq      => BinOp::Ge,
                    BinaryOperator::Lt        => BinOp::Lt,
                    BinaryOperator::LtEq      => BinOp::Le,
                    BinaryOperator::And       => BinOp::And,
                    BinaryOperator::Or        => BinOp::Or,
                    op => return Err(GnitzSqlError::Unsupported(
                        format!("binary operator {:?} not supported in Phase 1", op)
                    )),
                };
                Ok(BoundExpr::BinOp(Box::new(l), bop, Box::new(r)))
            }
            Expr::UnaryOp { op, expr } => {
                let inner = self.bind_expr(expr, schema)?;
                let uop = match op {
                    UnaryOperator::Minus => UnaryOp::Neg,
                    UnaryOperator::Not   => UnaryOp::Not,
                    op => return Err(GnitzSqlError::Unsupported(
                        format!("unary operator {:?} not supported in Phase 1", op)
                    )),
                };
                Ok(BoundExpr::UnaryOp(uop, Box::new(inner)))
            }
            Expr::IsNull(inner) => {
                match inner.as_ref() {
                    Expr::Identifier(ident) => {
                        let idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                            .ok_or_else(|| GnitzSqlError::Bind(
                                format!("column '{}' not found", ident.value)
                            ))?;
                        Ok(BoundExpr::IsNull(idx))
                    }
                    _ => Err(GnitzSqlError::Unsupported("IS NULL on non-column expression".to_string())),
                }
            }
            Expr::IsNotNull(inner) => {
                match inner.as_ref() {
                    Expr::Identifier(ident) => {
                        let idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                            .ok_or_else(|| GnitzSqlError::Bind(
                                format!("column '{}' not found", ident.value)
                            ))?;
                        Ok(BoundExpr::IsNotNull(idx))
                    }
                    _ => Err(GnitzSqlError::Unsupported("IS NOT NULL on non-column expression".to_string())),
                }
            }
            _ => Err(GnitzSqlError::Unsupported(
                format!("expression type not supported in Phase 1: {:?}", expr)
            )),
        }
    }
}
