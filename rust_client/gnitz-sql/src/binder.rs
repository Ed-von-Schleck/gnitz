use std::collections::HashMap;
use sqlparser::ast::{
    Expr, BinaryOperator, UnaryOperator, Value,
    FunctionArguments, FunctionArg, FunctionArgExpr,
};
use gnitz_protocol::Schema;
use gnitz_core::GnitzClient;
use crate::error::GnitzSqlError;
use crate::logical_plan::{BoundExpr, BinOp, UnaryOp, AggFunc};

/// Resolves a column in a multi-table context (for joins).
/// `tables` maps alias/name → (table_id, Schema, column_offset).
pub fn resolve_qualified_column(
    table_alias: &str,
    col_name: &str,
    tables: &HashMap<String, (u64, Schema, usize)>,
) -> Result<usize, GnitzSqlError> {
    let (_, schema, offset) = tables.get(table_alias)
        .ok_or_else(|| GnitzSqlError::Bind(
            format!("table alias '{}' not found", table_alias)
        ))?;
    let idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
        .ok_or_else(|| GnitzSqlError::Bind(
            format!("column '{}' not found in table '{}'", col_name, table_alias)
        ))?;
    Ok(offset + idx)
}

/// Resolves an unqualified column in a multi-table context.
/// Tries each table in order; errors on ambiguity.
pub fn resolve_unqualified_column(
    col_name: &str,
    tables: &HashMap<String, (u64, Schema, usize)>,
) -> Result<usize, GnitzSqlError> {
    let mut found: Option<usize> = None;
    for (_alias, (_, schema, offset)) in tables {
        if let Some(idx) = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name)) {
            if found.is_some() {
                return Err(GnitzSqlError::Bind(
                    format!("ambiguous column '{}' — qualify with table alias", col_name)
                ));
            }
            found = Some(offset + idx);
        }
    }
    found.ok_or_else(|| GnitzSqlError::Bind(
        format!("column '{}' not found in any table", col_name)
    ))
}

pub struct Binder<'a> {
    client:      &'a GnitzClient,
    schema_name: &'a str,
    cache:       HashMap<String, (u64, Schema)>,
    index_cache: HashMap<(u64, usize), Option<(u64, bool)>>,
}

impl<'a> Binder<'a> {
    pub fn new(client: &'a GnitzClient, schema_name: &'a str) -> Self {
        Binder { client, schema_name, cache: HashMap::new(), index_cache: HashMap::new() }
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

    /// Cache a CTE or alias name as resolving to the given (table_id, schema).
    pub fn cache_alias(&mut self, name: String, resolved: (u64, Schema)) {
        self.cache.insert(name, resolved);
    }

    pub fn find_index(
        &mut self, table_id: u64, col_idx: usize,
    ) -> Result<Option<(u64, bool)>, GnitzSqlError> {
        if let Some(&cached) = self.index_cache.get(&(table_id, col_idx)) {
            return Ok(cached);
        }
        let result = self.client.find_index_for_column(table_id, col_idx)
            .map_err(GnitzSqlError::Exec)?;
        self.index_cache.insert((table_id, col_idx), result);
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
                    Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                        Ok(BoundExpr::LitStr(s.clone()))
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
                        format!("binary operator {:?} not supported", op)
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
                        format!("unary operator {:?} not supported", op)
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
            Expr::CompoundIdentifier(parts) => {
                // Qualified column ref like t1.col — for single-table context,
                // just use the column name (ignore table qualifier)
                if parts.len() == 2 {
                    let col_name = &parts[1].value;
                    let idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                        .ok_or_else(|| GnitzSqlError::Bind(
                            format!("column '{}' not found", col_name)
                        ))?;
                    Ok(BoundExpr::ColRef(idx))
                } else {
                    Err(GnitzSqlError::Unsupported(
                        format!("compound identifier with {} parts not supported", parts.len())
                    ))
                }
            }
            Expr::Nested(inner) => self.bind_expr(inner, schema),
            Expr::Function(func) => {
                let name = func.name.to_string().to_lowercase();
                match name.as_str() {
                    "count" => {
                        if let FunctionArguments::List(list) = &func.args {
                            if list.args.len() == 1 {
                                match &list.args[0] {
                                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                                        return Ok(BoundExpr::AggCall { func: AggFunc::Count, arg: None });
                                    }
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                                        let bound = self.bind_expr(inner, schema)?;
                                        return Ok(BoundExpr::AggCall {
                                            func: AggFunc::CountNonNull,
                                            arg: Some(Box::new(bound)),
                                        });
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Err(GnitzSqlError::Unsupported("COUNT: unsupported argument form".to_string()))
                    }
                    "sum" | "min" | "max" | "avg" => {
                        let agg_func = match name.as_str() {
                            "sum" => AggFunc::Sum,
                            "min" => AggFunc::Min,
                            "max" => AggFunc::Max,
                            "avg" => AggFunc::Avg,
                            _ => unreachable!(),
                        };
                        if let FunctionArguments::List(list) = &func.args {
                            if list.args.len() == 1 {
                                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = &list.args[0] {
                                    let bound = self.bind_expr(inner, schema)?;
                                    return Ok(BoundExpr::AggCall {
                                        func: agg_func,
                                        arg: Some(Box::new(bound)),
                                    });
                                }
                            }
                        }
                        Err(GnitzSqlError::Unsupported(
                            format!("{}: requires exactly one column argument", name)
                        ))
                    }
                    _ => Err(GnitzSqlError::Unsupported(
                        format!("function '{}' not supported", name)
                    )),
                }
            }
            _ => Err(GnitzSqlError::Unsupported(
                format!("expression type not supported: {:?}", expr)
            )),
        }
    }
}
