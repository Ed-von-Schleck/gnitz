use crate::error::GnitzSqlError;
use crate::ir::{AggFunc, BinOp, BoundExpr, UnaryOp};
use gnitz_core::GnitzClient;
use gnitz_core::{ColumnDef, Schema, TypeCode};
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, UnaryOperator, Value,
};
use std::collections::HashMap;
use std::rc::Rc;

/// Find the column named `col_name` in `columns`, case-insensitively.
///
/// - `Ok(Some(idx))` — exactly one column matches.
/// - `Ok(None)` — no column matches (callers attach their own "not found" text).
/// - `Err(Bind)` — more than one column matches. A `SELECT *` join view carries
///   same-named columns from both sides; an unqualified reference to such a name
///   is ambiguous (standard SQL) and must be rejected, not silently bound to the
///   first match.
///
/// Single home for the name→index lookup that was previously
/// `columns.iter().position(...)` (first-match) at every call site.
pub(crate) fn find_unique_column(columns: &[ColumnDef], col_name: &str) -> Result<Option<usize>, GnitzSqlError> {
    let mut found: Option<usize> = None;
    for (i, c) in columns.iter().enumerate() {
        if c.name.eq_ignore_ascii_case(col_name) {
            if found.is_some() {
                return Err(GnitzSqlError::Bind(format!(
                    "column reference '{col_name}' is ambiguous"
                )));
            }
            found = Some(i);
        }
    }
    Ok(found)
}

/// Resolves a column in a multi-table context (for joins).
/// `tables` maps alias/name → (table_id, Schema, column_offset).
pub(crate) fn resolve_qualified_column(
    table_alias: &str,
    col_name: &str,
    tables: &HashMap<String, (u64, Rc<Schema>, usize)>,
) -> Result<usize, GnitzSqlError> {
    let (_, schema, offset) = tables
        .get(table_alias)
        .ok_or_else(|| GnitzSqlError::Bind(format!("table alias '{table_alias}' not found")))?;
    let idx = find_unique_column(&schema.columns, col_name)?
        .ok_or_else(|| GnitzSqlError::Bind(format!("column '{col_name}' not found in table '{table_alias}'")))?;
    Ok(offset + idx)
}

/// Resolves an unqualified column in a multi-table context.
/// Tries each table in order; errors on ambiguity.
pub(crate) fn resolve_unqualified_column(
    col_name: &str,
    tables: &HashMap<String, (u64, Rc<Schema>, usize)>,
) -> Result<usize, GnitzSqlError> {
    let mut found: Option<usize> = None;
    for (_, schema, offset) in tables.values() {
        // The `?` rejects a name duplicated *within* one source relation (joining
        // a dup-named `SELECT *` view); the `found.is_some()` check below rejects a
        // name that appears across two source relations.
        if let Some(idx) = find_unique_column(&schema.columns, col_name)? {
            if found.is_some() {
                return Err(GnitzSqlError::Bind(format!(
                    "ambiguous column '{col_name}' — qualify with table alias"
                )));
            }
            found = Some(offset + idx);
        }
    }
    found.ok_or_else(|| GnitzSqlError::Bind(format!("column '{col_name}' not found in any table")))
}

pub(crate) struct Binder<'a> {
    schema_name: &'a str,
    cache: HashMap<String, (u64, Rc<Schema>)>,
}

impl<'a> Binder<'a> {
    pub(crate) fn new(schema_name: &'a str) -> Self {
        Binder {
            schema_name,
            cache: HashMap::new(),
        }
    }

    pub(crate) fn resolve(&mut self, client: &mut GnitzClient, name: &str) -> Result<(u64, Rc<Schema>), GnitzSqlError> {
        if let Some(entry) = self.cache.get(name) {
            return Ok((entry.0, Rc::clone(&entry.1)));
        }
        let (tid, schema) = client
            .resolve_table_or_view_id(self.schema_name, name)
            .map_err(GnitzSqlError::Exec)?;
        let rc = Rc::new(schema);
        self.cache.insert(name.to_string(), (tid, Rc::clone(&rc)));
        Ok((tid, rc))
    }

    /// Resolve a write/index target that must be a base table. INSERT, UPDATE,
    /// DELETE and CREATE INDEX may only act on base tables: a view is a read-only
    /// derived relation whose store is maintained solely by its circuit, so
    /// writing to it or indexing it corrupts that state. `resolve` (used by
    /// SELECT and view definitions) still accepts views; this is the
    /// writable-target variant.
    pub(crate) fn resolve_base_table(
        &mut self,
        client: &mut GnitzClient,
        name: &str,
    ) -> Result<(u64, Rc<Schema>), GnitzSqlError> {
        // `resolve_table_id` consults TABLE_TAB only, so a view name misses it.
        match client.resolve_table_id(self.schema_name, name) {
            Ok((tid, schema)) => {
                let rc = Rc::new(schema);
                self.cache.insert(name.to_string(), (tid, Rc::clone(&rc)));
                Ok((tid, rc))
            }
            // Miss: re-probe including views to tell "is a view" (reject as
            // read-only) from "does not exist" (propagate the original error).
            Err(not_found) => match client.resolve_table_or_view_id(self.schema_name, name) {
                Ok(_) => Err(GnitzSqlError::Unsupported(format!(
                    "'{name}' is a view; INSERT, UPDATE, DELETE and CREATE INDEX \
                     require a base table"
                ))),
                Err(_) => Err(GnitzSqlError::Exec(not_found)),
            },
        }
    }

    /// Cache a CTE or alias name as resolving to the given (table_id, schema).
    pub(crate) fn cache_alias(&mut self, name: String, resolved: (u64, Rc<Schema>)) {
        self.cache.insert(name, resolved);
    }

    /// Bind an expression against a single-relation schema (WHERE, projections,
    /// set-op branches, DML). The structural recursion lives in `bind_structural`;
    /// the `SingleTable` leaf supplies the three schema-aware decisions (column
    /// lookup, aggregate calls, `IS [NOT] NULL`). `&self` is retained for API
    /// symmetry and future binding state, though the recursion no longer threads it.
    pub(crate) fn bind_expr(&self, expr: &Expr, schema: &Schema) -> Result<BoundExpr, GnitzSqlError> {
        bind_structural(expr, &SingleTable { schema })
    }
}

// ---------------------------------------------------------------------------
// Shared structural expression binding
// ---------------------------------------------------------------------------
//
// One `Expr → BoundExpr` recursion, parametrized by a three-method leaf. The
// only things that differ across GnitzDB's binding contexts (WHERE/projection,
// HAVING, JOIN residual) are the three schema-aware leaves — column reference,
// function call, and `IS [NOT] NULL`. Everything structural (literals, the full
// operator map, unary ops, the BETWEEN desugar, Nested unwrap) lives here once,
// so a new operator or fold lands on every context at the same time and cannot
// silently drift between hand-copied walks.

/// The three schema-aware leaves of `bind_structural`.
pub(crate) trait LeafBinder {
    /// An `Identifier` / `CompoundIdentifier` column reference.
    fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError>;
    /// A function call (aggregates, or a context-specific rejection).
    fn bind_function(&self, f: &Function) -> Result<BoundExpr, GnitzSqlError>;
    /// `inner IS [NOT] NULL` (`want_null` picks IS NULL vs IS NOT NULL).
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BoundExpr, GnitzSqlError>;
}

/// The one structural recursion. Needs no schema — every schema-aware decision
/// is a leaf method. Generic over `L` (static dispatch) so a leaf's
/// `bind_function` can recurse via `bind_structural(arg, self)`.
pub(crate) fn bind_structural<L: LeafBinder>(expr: &Expr, leaf: &L) -> Result<BoundExpr, GnitzSqlError> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => leaf.bind_column(expr),
        Expr::Function(f) => leaf.bind_function(f),
        Expr::IsNull(i) => leaf.bind_null_test(i, true),
        Expr::IsNotNull(i) => leaf.bind_null_test(i, false),
        Expr::Nested(i) => bind_structural(i, leaf),
        Expr::Value(vws) => bind_literal(&vws.value),
        Expr::BinaryOp { left, op, right } => {
            let l = bind_structural(left, leaf)?;
            let r = bind_structural(right, leaf)?;
            Ok(BoundExpr::BinOp(Box::new(l), map_binop(op)?, Box::new(r)))
        }
        Expr::UnaryOp { op, expr } => {
            let inner = bind_structural(expr, leaf)?;
            let uop = match op {
                UnaryOperator::Minus => UnaryOp::Neg,
                UnaryOperator::Not => UnaryOp::Not,
                o => {
                    return Err(GnitzSqlError::Unsupported(format!(
                        "unary operator {o:?} not supported"
                    )))
                }
            };
            Ok(BoundExpr::UnaryOp(uop, Box::new(inner)))
        }
        // `e BETWEEN lo AND hi` ≡ `e >= lo AND e <= hi`; NOT BETWEEN negates it.
        // NULL semantics are SQL-correct under either form (a NULL operand makes
        // the AND NULL, and NOT(NULL) is NULL → the row is excluded).
        Expr::Between {
            expr: e,
            negated,
            low,
            high,
        } => {
            let ge = BoundExpr::BinOp(
                Box::new(bind_structural(e, leaf)?),
                BinOp::Ge,
                Box::new(bind_structural(low, leaf)?),
            );
            let le = BoundExpr::BinOp(
                Box::new(bind_structural(e, leaf)?),
                BinOp::Le,
                Box::new(bind_structural(high, leaf)?),
            );
            let between = BoundExpr::BinOp(Box::new(ge), BinOp::And, Box::new(le));
            Ok(if *negated {
                BoundExpr::UnaryOp(UnaryOp::Not, Box::new(between))
            } else {
                between
            })
        }
        _ => Err(GnitzSqlError::Unsupported(format!(
            "expression type not supported: {expr:?}"
        ))),
    }
}

/// sqlparser binary op → `BinOp` (the single, complete map).
pub(crate) fn map_binop(op: &BinaryOperator) -> Result<BinOp, GnitzSqlError> {
    Ok(match op {
        BinaryOperator::Plus => BinOp::Add,
        BinaryOperator::Minus => BinOp::Sub,
        BinaryOperator::Multiply => BinOp::Mul,
        BinaryOperator::Divide => BinOp::Div,
        BinaryOperator::Modulo => BinOp::Mod,
        BinaryOperator::Eq => BinOp::Eq,
        BinaryOperator::NotEq => BinOp::Ne,
        BinaryOperator::Gt => BinOp::Gt,
        BinaryOperator::GtEq => BinOp::Ge,
        BinaryOperator::Lt => BinOp::Lt,
        BinaryOperator::LtEq => BinOp::Le,
        BinaryOperator::And => BinOp::And,
        BinaryOperator::Or => BinOp::Or,
        o => {
            return Err(GnitzSqlError::Unsupported(format!(
                "binary operator {o:?} not supported"
            )))
        }
    })
}

/// Number/string literal → `BoundExpr`.
pub(crate) fn bind_literal(v: &Value) -> Result<BoundExpr, GnitzSqlError> {
    match v {
        // Try integer first, then float.
        Value::Number(n, _) => n
            .parse::<i64>()
            .map(BoundExpr::LitInt)
            .or_else(|_| n.parse::<f64>().map(BoundExpr::LitFloat))
            .map_err(|_| GnitzSqlError::Bind(format!("invalid number literal: {n}"))),
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(BoundExpr::LitStr(s.clone())),
        _ => Err(GnitzSqlError::Unsupported(format!(
            "value type not supported in expressions: {v:?}"
        ))),
    }
}

/// Shared NOT-NULL fold: a non-nullable column makes `IS [NOT] NULL` a constant
/// (never null, never — for IS NULL — true), keeping the null-tracking opcode
/// (which forces `eval_batch`'s slow path — `is_strictly_non_nullable` returns
/// false on any is_null) out of the program.
pub(crate) fn fold_null_test(schema: &Schema, idx: usize, want_null: bool) -> BoundExpr {
    if !schema.columns[idx].is_nullable {
        BoundExpr::LitInt(i64::from(!want_null))
    } else if want_null {
        BoundExpr::IsNull(idx)
    } else {
        BoundExpr::IsNotNull(idx)
    }
}

/// Leaf for a single-relation schema (WHERE, projections, set-ops, DML).
pub(crate) struct SingleTable<'a> {
    pub schema: &'a Schema,
}

impl SingleTable<'_> {
    /// The column index of an `Identifier` / two-part `CompoundIdentifier`. The
    /// qualifier on a compound ref is informational in a single-table context
    /// (it carries no disambiguating information — a duplicated name is ambiguous
    /// even when qualified).
    fn idx(&self, e: &Expr) -> Result<usize, GnitzSqlError> {
        let name = match e {
            Expr::Identifier(id) => &id.value,
            Expr::CompoundIdentifier(p) if p.len() == 2 => &p[1].value,
            _ => return Err(GnitzSqlError::Unsupported("expected a column reference".into())),
        };
        find_unique_column(&self.schema.columns, name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("column '{name}' not found")))
    }
}

impl LeafBinder for SingleTable<'_> {
    fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError> {
        Ok(BoundExpr::ColRef(self.idx(e)?))
    }
    fn bind_function(&self, func: &Function) -> Result<BoundExpr, GnitzSqlError> {
        let name = func.name.to_string().to_lowercase();
        match name.as_str() {
            "count" => {
                if let FunctionArguments::List(list) = &func.args {
                    if list.args.len() == 1 {
                        match &list.args[0] {
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                                return Ok(BoundExpr::AggCall {
                                    func: AggFunc::Count,
                                    arg: None,
                                });
                            }
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                                let bound = bind_structural(inner, self)?;
                                return Ok(BoundExpr::AggCall {
                                    func: AggFunc::CountNonNull,
                                    arg: Some(Box::new(bound)),
                                });
                            }
                            _ => {}
                        }
                    }
                }
                Err(GnitzSqlError::Unsupported(
                    "COUNT: unsupported argument form".to_string(),
                ))
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
                            let bound = bind_structural(inner, self)?;
                            // MIN/MAX have no correct accumulator path for wide
                            // (U128/UUID) types — the i64 slot cannot hold them —
                            // Blob has no ordering, and the String comparator in
                            // `decode_signed` reads the prefix as LE signed i64,
                            // which orders by neither bytes nor signedness. Reject
                            // here so the operator only sees types it can compare
                            // correctly.
                            if matches!(agg_func, AggFunc::Min | AggFunc::Max) {
                                let arg_ty = bound.infer_type(self.schema);
                                if matches!(
                                    arg_ty,
                                    TypeCode::U128
                                        | TypeCode::UUID
                                        | TypeCode::Blob
                                        | TypeCode::String
                                        | TypeCode::I128
                                ) {
                                    return Err(GnitzSqlError::Unsupported(format!(
                                        "{}: not supported on {:?} columns",
                                        name.to_uppercase(),
                                        arg_ty
                                    )));
                                }
                            }
                            return Ok(BoundExpr::AggCall {
                                func: agg_func,
                                arg: Some(Box::new(bound)),
                            });
                        }
                    }
                }
                Err(GnitzSqlError::Unsupported(format!(
                    "{name}: requires exactly one column argument"
                )))
            }
            _ => Err(GnitzSqlError::Unsupported(format!("function '{name}' not supported"))),
        }
    }
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BoundExpr, GnitzSqlError> {
        Ok(fold_null_test(self.schema, self.idx(inner)?, want_null))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gnitz_core::{ColumnDef, Schema, TypeCode};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn col(name: &str, tc: TypeCode) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            type_code: tc,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
        }
    }

    fn schema_with_val(val_tc: TypeCode) -> Schema {
        Schema {
            columns: vec![col("pk", TypeCode::U64), col("c", val_tc)],
            pk_cols: vec![0],
        }
    }

    fn parse(src: &str) -> Expr {
        let dialect = GenericDialect {};
        Parser::new(&dialect).try_with_sql(src).unwrap().parse_expr().unwrap()
    }

    fn assert_unsupported(r: Result<BoundExpr, GnitzSqlError>, want_substr: &str) {
        match r.unwrap_err() {
            GnitzSqlError::Unsupported(msg) => {
                assert!(
                    msg.contains(want_substr),
                    "got Unsupported({msg:?}), expected to contain {want_substr:?}"
                );
            }
            e => panic!("expected Unsupported, got {e:?}"),
        }
    }

    #[test]
    fn test_find_unique_column_unique() {
        let cols = vec![col("a", TypeCode::U64), col("b", TypeCode::I64)];
        assert_eq!(find_unique_column(&cols, "a").unwrap(), Some(0));
        assert_eq!(find_unique_column(&cols, "B").unwrap(), Some(1)); // case-insensitive
    }

    #[test]
    fn test_find_unique_column_absent() {
        let cols = vec![col("a", TypeCode::U64)];
        assert_eq!(find_unique_column(&cols, "missing").unwrap(), None);
    }

    #[test]
    fn test_find_unique_column_duplicate_is_ambiguous() {
        // Two case-insensitively equal names (as a `SELECT *` join view produces).
        let cols = vec![col("Id", TypeCode::U64), col("ID", TypeCode::U64)];
        match find_unique_column(&cols, "id") {
            Err(GnitzSqlError::Bind(s)) => assert!(s.contains("ambiguous"), "got: {s}"),
            other => panic!("expected Bind(ambiguous), got {other:?}"),
        }
    }

    #[test]
    fn test_bind_between_desugars_to_comparison_tree() {
        let binder = Binder::new("s");
        let schema = schema_with_val(TypeCode::I64); // (pk U64, c I64)
                                                     // `c BETWEEN 1 AND 9` ≡ `c >= 1 AND c <= 9` — a residual BETWEEN now binds
                                                     // (regression guard for the new Expr::Between arm) instead of Unsupported.
        match binder.bind_expr(&parse("c BETWEEN 1 AND 9"), &schema).unwrap() {
            BoundExpr::BinOp(l, BinOp::And, r) => {
                assert!(matches!(*l, BoundExpr::BinOp(_, BinOp::Ge, _)));
                assert!(matches!(*r, BoundExpr::BinOp(_, BinOp::Le, _)));
            }
            other => panic!("expected And(Ge, Le), got {other:?}"),
        }
        // `c NOT BETWEEN 1 AND 9` ≡ NOT(c >= 1 AND c <= 9).
        match binder.bind_expr(&parse("c NOT BETWEEN 1 AND 9"), &schema).unwrap() {
            BoundExpr::UnaryOp(UnaryOp::Not, inner) => {
                assert!(matches!(*inner, BoundExpr::BinOp(_, BinOp::And, _)))
            }
            other => panic!("expected Not(And(..)), got {other:?}"),
        }
    }

    #[test]
    fn test_binder_rejects_min_max_unsupported_types() {
        let binder = Binder::new("s");
        for &tc in &[
            TypeCode::U128,
            TypeCode::UUID,
            TypeCode::Blob,
            TypeCode::String,
            TypeCode::I128,
        ] {
            let schema = schema_with_val(tc);
            for fname in &["MIN", "MAX"] {
                let expr = parse(&format!("{fname}(c)"));
                let r = binder.bind_expr(&expr, &schema);
                assert_unsupported(r, fname);
            }
        }
    }

    #[test]
    fn test_binder_accepts_min_max_orderable_types() {
        let binder = Binder::new("s");
        // Types the operator can compare correctly:
        // narrow unsigned + zero-extend, signed, U64 (with the unsigned fix),
        // and floats.
        let accepted = [
            TypeCode::U8,
            TypeCode::U16,
            TypeCode::U32,
            TypeCode::U64,
            TypeCode::I8,
            TypeCode::I16,
            TypeCode::I32,
            TypeCode::I64,
            TypeCode::F32,
            TypeCode::F64,
        ];
        for &tc in &accepted {
            let schema = schema_with_val(tc);
            for fname in &["MIN", "MAX"] {
                let expr = parse(&format!("{fname}(c)"));
                let r = binder.bind_expr(&expr, &schema);
                assert!(r.is_ok(), "expected {}({:?}) to bind, got {:?}", fname, tc, r.err());
            }
        }
    }

    /// `t.x IS [NOT] NULL` on a *qualified* (CompoundIdentifier) column now binds
    /// like the unqualified form in every `bind_expr` context — the unified core
    /// reaches the shared null-test leaf, which the old bare-`Identifier`-only arm
    /// rejected with "IS NULL on non-column expression".
    #[test]
    fn test_compound_identifier_null_test_binds() {
        let binder = Binder::new("s");
        // Non-nullable column: folds to the constant (0 for IS NULL, 1 for IS NOT NULL).
        let nn = schema_with_val(TypeCode::I64); // (pk U64 NOT NULL, c I64 NOT NULL)
        assert!(matches!(
            binder.bind_expr(&parse("t.c IS NULL"), &nn).unwrap(),
            BoundExpr::LitInt(0)
        ));
        assert!(matches!(
            binder.bind_expr(&parse("t.c IS NOT NULL"), &nn).unwrap(),
            BoundExpr::LitInt(1)
        ));
        // Nullable column: binds to the IsNull / IsNotNull opcode at the column index.
        let nullable = Schema {
            columns: vec![
                col("pk", TypeCode::U64),
                ColumnDef {
                    name: "c".into(),
                    type_code: TypeCode::I64,
                    is_nullable: true,
                    fk_table_id: 0,
                    fk_col_idx: 0,
                },
            ],
            pk_cols: vec![0],
        };
        assert!(matches!(
            binder.bind_expr(&parse("t.c IS NULL"), &nullable).unwrap(),
            BoundExpr::IsNull(1)
        ));
        assert!(matches!(
            binder.bind_expr(&parse("t.c IS NOT NULL"), &nullable).unwrap(),
            BoundExpr::IsNotNull(1)
        ));
    }
}
