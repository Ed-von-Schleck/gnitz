//! SQL→logical-plan binding. Uses a `CatalogResolver` instead of a
//! `GnitzClient` so the server-side compile path doesn't need IPC.

use std::collections::HashMap;
use std::rc::Rc;

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    UnaryOperator, Value,
};

use gnitz_protocol::Schema;

use crate::error::GnitzSqlParseError;
use crate::plan_types::{AggFunc, BinOp, BoundExpr, UnaryOp};
use crate::resolver::CatalogResolver;

/// Resolves a column in a multi-table context (joins).
pub fn resolve_qualified_column(
    table_alias: &str,
    col_name:    &str,
    tables:      &HashMap<String, (u64, Schema, usize)>,
) -> Result<usize, GnitzSqlParseError> {
    let (_, schema, offset) = tables.get(table_alias)
        .ok_or_else(|| GnitzSqlParseError::Bind(
            format!("table alias '{}' not found", table_alias),
        ))?;
    let idx = schema.columns.iter()
        .position(|c| c.name.eq_ignore_ascii_case(col_name))
        .ok_or_else(|| GnitzSqlParseError::Bind(
            format!("column '{}' not found in table '{}'", col_name, table_alias),
        ))?;
    Ok(offset + idx)
}

/// Resolves an unqualified column across all tables; errors on ambiguity.
pub fn resolve_unqualified_column(
    col_name: &str,
    tables:   &HashMap<String, (u64, Schema, usize)>,
) -> Result<usize, GnitzSqlParseError> {
    let mut found: Option<usize> = None;
    for (_, (_, schema, offset)) in tables {
        if let Some(idx) = schema.columns.iter()
            .position(|c| c.name.eq_ignore_ascii_case(col_name))
        {
            if found.is_some() {
                return Err(GnitzSqlParseError::Bind(
                    format!("ambiguous column '{}' — qualify with table alias", col_name),
                ));
            }
            found = Some(offset + idx);
        }
    }
    found.ok_or_else(|| GnitzSqlParseError::Bind(
        format!("column '{}' not found in any table", col_name),
    ))
}

pub struct Binder<'a, R: CatalogResolver + ?Sized> {
    resolver:    &'a R,
    schema_name: &'a str,
    cache:       HashMap<String, (u64, Rc<Schema>)>,
    index_cache: HashMap<(u64, usize), Option<(u64, bool)>>,
}

impl<'a, R: CatalogResolver + ?Sized> Binder<'a, R> {
    pub fn new(resolver: &'a R, schema_name: &'a str) -> Self {
        Binder {
            resolver, schema_name,
            cache: HashMap::new(), index_cache: HashMap::new(),
        }
    }

    pub fn resolve(&mut self, name: &str) -> Result<(u64, Rc<Schema>), GnitzSqlParseError> {
        if let Some(entry) = self.cache.get(name) {
            return Ok((entry.0, Rc::clone(&entry.1)));
        }
        let (tid, schema) = self.resolver
            .resolve_table_or_view(self.schema_name, name)
            .ok_or_else(|| GnitzSqlParseError::Bind(
                format!("table or view '{}' not found in schema '{}'", name, self.schema_name),
            ))?;
        let rc = Rc::new(schema);
        self.cache.insert(name.to_string(), (tid, Rc::clone(&rc)));
        Ok((tid, rc))
    }

    pub fn cache_alias(&mut self, name: String, resolved: (u64, Rc<Schema>)) {
        self.cache.insert(name, resolved);
    }

    pub fn find_index(
        &mut self, table_id: u64, col_idx: usize,
    ) -> Result<Option<(u64, bool)>, GnitzSqlParseError> {
        if let Some(&cached) = self.index_cache.get(&(table_id, col_idx)) {
            return Ok(cached);
        }
        let result = self.resolver.find_index_for_column(table_id, col_idx);
        self.index_cache.insert((table_id, col_idx), result);
        Ok(result)
    }

    pub fn bind_expr(&self, expr: &Expr, schema: &Schema) -> Result<BoundExpr, GnitzSqlParseError> {
        match expr {
            Expr::Identifier(ident) => {
                let col_name = &ident.value;
                let idx = schema.columns.iter()
                    .position(|c| c.name.eq_ignore_ascii_case(col_name))
                    .ok_or_else(|| GnitzSqlParseError::Bind(
                        format!("column '{}' not found", col_name),
                    ))?;
                Ok(BoundExpr::ColRef(idx))
            }
            Expr::Value(vws) => match &vws.value {
                Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(BoundExpr::LitInt(i))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(BoundExpr::LitFloat(f))
                    } else {
                        Err(GnitzSqlParseError::Bind(format!("invalid number literal: {}", n)))
                    }
                }
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                    Ok(BoundExpr::LitStr(s.clone()))
                }
                _ => Err(GnitzSqlParseError::Unsupported(
                    format!("value type not supported in expressions: {:?}", vws.value),
                )),
            },
            Expr::BinaryOp { left, op, right } => {
                let l = self.bind_expr(left, schema)?;
                let r = self.bind_expr(right, schema)?;
                let bop = match op {
                    BinaryOperator::Plus     => BinOp::Add,
                    BinaryOperator::Minus    => BinOp::Sub,
                    BinaryOperator::Multiply => BinOp::Mul,
                    BinaryOperator::Divide   => BinOp::Div,
                    BinaryOperator::Modulo   => BinOp::Mod,
                    BinaryOperator::Eq       => BinOp::Eq,
                    BinaryOperator::NotEq    => BinOp::Ne,
                    BinaryOperator::Gt       => BinOp::Gt,
                    BinaryOperator::GtEq     => BinOp::Ge,
                    BinaryOperator::Lt       => BinOp::Lt,
                    BinaryOperator::LtEq     => BinOp::Le,
                    BinaryOperator::And      => BinOp::And,
                    BinaryOperator::Or       => BinOp::Or,
                    op => return Err(GnitzSqlParseError::Unsupported(
                        format!("binary operator {:?} not supported", op),
                    )),
                };
                Ok(BoundExpr::BinOp(Box::new(l), bop, Box::new(r)))
            }
            Expr::UnaryOp { op, expr } => {
                let inner = self.bind_expr(expr, schema)?;
                let uop = match op {
                    UnaryOperator::Minus => UnaryOp::Neg,
                    UnaryOperator::Not   => UnaryOp::Not,
                    op => return Err(GnitzSqlParseError::Unsupported(
                        format!("unary operator {:?} not supported", op),
                    )),
                };
                Ok(BoundExpr::UnaryOp(uop, Box::new(inner)))
            }
            Expr::IsNull(inner) => match inner.as_ref() {
                Expr::Identifier(ident) => {
                    let idx = schema.columns.iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                        .ok_or_else(|| GnitzSqlParseError::Bind(
                            format!("column '{}' not found", ident.value),
                        ))?;
                    Ok(BoundExpr::IsNull(idx))
                }
                _ => Err(GnitzSqlParseError::Unsupported("IS NULL on non-column expression".into())),
            },
            Expr::IsNotNull(inner) => match inner.as_ref() {
                Expr::Identifier(ident) => {
                    let idx = schema.columns.iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                        .ok_or_else(|| GnitzSqlParseError::Bind(
                            format!("column '{}' not found", ident.value),
                        ))?;
                    Ok(BoundExpr::IsNotNull(idx))
                }
                _ => Err(GnitzSqlParseError::Unsupported("IS NOT NULL on non-column expression".into())),
            },
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 2 {
                    let col_name = &parts[1].value;
                    let idx = schema.columns.iter()
                        .position(|c| c.name.eq_ignore_ascii_case(col_name))
                        .ok_or_else(|| GnitzSqlParseError::Bind(
                            format!("column '{}' not found", col_name),
                        ))?;
                    Ok(BoundExpr::ColRef(idx))
                } else {
                    Err(GnitzSqlParseError::Unsupported(
                        format!("compound identifier with {} parts not supported", parts.len()),
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
                                            arg:  Some(Box::new(bound)),
                                        });
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Err(GnitzSqlParseError::Unsupported("COUNT: unsupported argument form".into()))
                    }
                    "sum" | "min" | "max" | "avg" => {
                        let agg_func = match name.as_str() {
                            "sum" => AggFunc::Sum, "min" => AggFunc::Min,
                            "max" => AggFunc::Max, "avg" => AggFunc::Avg,
                            _ => unreachable!(),
                        };
                        if let FunctionArguments::List(list) = &func.args {
                            if list.args.len() == 1 {
                                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = &list.args[0] {
                                    let bound = self.bind_expr(inner, schema)?;
                                    return Ok(BoundExpr::AggCall {
                                        func: agg_func, arg: Some(Box::new(bound)),
                                    });
                                }
                            }
                        }
                        Err(GnitzSqlParseError::Unsupported(
                            format!("{}: requires exactly one column argument", name),
                        ))
                    }
                    _ => Err(GnitzSqlParseError::Unsupported(
                        format!("function '{}' not supported", name),
                    )),
                }
            }
            _ => Err(GnitzSqlParseError::Unsupported(
                format!("expression type not supported: {:?}", expr),
            )),
        }
    }
}
