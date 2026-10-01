use std::convert::Infallible;

use super::resolve::require_column;
use crate::ast_util::{
    bind_literal, classify_agg_call, col_ref_parts, peel_nested, single_fn_name, CallSurface, PlainCall,
};
use crate::codec::literal::{invalid_literal, parse_temporal};
use crate::error::{reject_if, GnitzSqlError};
use crate::ir::{BExpr, BinOp, BoundExpr, FloatUnaryOp, NumFunc, StrArg, StrFunc, TrimMode};
use crate::types::sql_col_type;
use gnitz_core::Schema;
use gnitz_expr::{CalendarOp, LikePattern};
use gnitz_wire::{ColType, ColumnDef};
use sqlparser::ast::{
    BinaryOperator, CaseWhen, CeilFloorKind, DataType, DateTimeField, Expr, Function, TrimWhereField, TypedString,
    UnaryOperator, Value, ValueWithSpan,
};

/// Bind an expression against a single-relation schema (WHERE, projections,
/// set-op branches, DML). The structural recursion lives in `bind_structural`;
/// the `SingleTable` leaf supplies the schema-aware decisions (column lookup,
/// aggregate calls, nullability). Context is fully determined by
/// `(expr, schema, alias)` — no binder state is involved.
pub(crate) fn bind_single_table(expr: &Expr, schema: &Schema, alias: &str) -> Result<BoundExpr, GnitzSqlError> {
    bind_structural(expr, &SingleTable { schema, alias })
}

/// A WHERE / ON clause as its bound conjunct list — the one home of that shape,
/// which every access recognizer and predicate compiler reads. Bound whole, then
/// split at every top-level `AND`, so a desugar that produces one (BETWEEN)
/// contributes its conjuncts like the written ones.
pub(crate) fn bind_conjuncts<R: Clone, L: LeafBinder<R>>(
    expr: &Expr,
    leaf: &L,
) -> Result<Vec<BExpr<R>>, GnitzSqlError> {
    Ok(bind_structural(expr, leaf)?.conjuncts())
}

// ---------------------------------------------------------------------------
// Shared structural expression binding
// ---------------------------------------------------------------------------
//
// One `Expr → BoundExpr` recursion, parametrized by a leaf. The only things that
// differ across GnitzDB's binding contexts (WHERE/projection, HAVING, JOIN
// residual) are the schema-aware positions — column reference, function call,
// a column's nullability, and a whole expression the context names. Everything
// structural (literals, the full operator map, unary ops, the BETWEEN desugar,
// Nested unwrap) lives here once, so a new operator or fold lands on every
// context at the same time and cannot silently drift between hand-copied walks.

/// The schema-aware leaves of `bind_structural`, generic over the leaf reference
/// type `R` (the `ColRef` payload).
pub(crate) trait LeafBinder<R> {
    /// A whole expression this leaf's context names, claimed before the recursion
    /// descends into it — a GROUP BY key over a grouped relation. `None` recurses
    /// as usual, so declining cannot mask the recursion's own error.
    fn bind_node(&self, _e: &Expr) -> Option<BExpr<R>> {
        None
    }
    /// An `Identifier` / `CompoundIdentifier` column reference.
    fn bind_column(&self, e: &Expr) -> Result<BExpr<R>, GnitzSqlError>;
    /// A function call. Only a grouped context admits an aggregate, so the
    /// default rejects one.
    fn bind_function(&self, f: &Function) -> Result<BExpr<R>, GnitzSqlError> {
        Err(aggregate_not_allowed(f))
    }
    /// Whether the value a leaf reference names can be NULL, and its declared
    /// type — the two facts a leaf owns that [`nullness`] folds a test by.
    fn is_nullable(&self, r: &R) -> bool;
    fn type_of(&self, r: &R) -> ColType;
    /// A windowed call (`f(…) OVER (…)`). Only a view body's windowed SELECT
    /// list and QUALIFY admit one; every other context keeps this rejection.
    fn bind_window(&self, _f: &Function) -> Result<BExpr<R>, GnitzSqlError> {
        Err(GnitzSqlError::Rejected(
            "window functions (OVER) are only supported in the SELECT list and QUALIFY of a CREATE VIEW \
             body, and cannot be nested in another window function's operands"
                .into(),
        ))
    }
    /// A subquery node — `[NOT] EXISTS`, `[NOT] IN (SELECT …)`, a scalar
    /// `(SELECT …)`, or an `ANY`/`ALL`/`SOME` comparison. The HIR view leaf
    /// overrides this to record/bind the subquery for decorrelation; every other
    /// context (DML, HAVING, ad-hoc reads) keeps the per-kind placement rejection.
    fn bind_subquery(&self, e: &Expr) -> Result<BExpr<R>, GnitzSqlError> {
        Err(unsupported_subquery(e))
    }
}

/// The rejection of a call in a context that admits no aggregate — the
/// [`LeafBinder::bind_function`] default. Classified first, so an unknown or
/// malformed call is named as such and the aggregate message goes only to a
/// name that really is one.
pub(crate) fn aggregate_not_allowed(f: &Function) -> GnitzSqlError {
    match classify_agg_call(f) {
        Err(e) => e,
        Ok(_) => GnitzSqlError::Rejected("aggregate functions are not allowed here".to_string()),
    }
}

/// The per-kind "no subquery here" rejection — the [`LeafBinder::bind_subquery`]
/// default, and what a leaf that overrides the method for *some* shapes falls back
/// to for the rest.
pub(crate) fn unsupported_subquery(e: &Expr) -> GnitzSqlError {
    GnitzSqlError::Rejected(match e {
        Expr::Subquery(_) => "scalar subqueries are not supported".into(),
        Expr::AnyOp { .. } | Expr::AllOp { .. } => "ANY/SOME/ALL subquery comparisons are not supported".into(),
        _ => "[NOT] EXISTS/IN (SELECT …) is only supported in a single-table CREATE VIEW \
              (in the WHERE clause or the SELECT list)"
            .into(),
    })
}

/// `CAST(e AS dt)`; a string literal cast to DATE/TIMESTAMP folds to its value.
fn bind_cast<R>(e: BExpr<R>, dt: &DataType) -> Result<BExpr<R>, GnitzSqlError> {
    let to = sql_col_type(dt)?;
    Ok(match e {
        BExpr::LitStr(s) if to.tc.is_temporal() => BExpr::LitTemporal {
            tc: to.tc,
            v: parse_temporal(to.tc, &s).ok_or_else(|| GnitzSqlError::Rejected(invalid_literal(to, &s)))?,
        },
        e => BExpr::Cast { expr: Box::new(e), to },
    })
}

/// The one structural recursion. Needs no schema — every schema-aware decision
/// is a leaf method. Generic over `L` (static dispatch) so a leaf's
/// `bind_function` can recurse via `bind_structural(arg, self)`.
pub(crate) fn bind_structural<R: Clone, L: LeafBinder<R>>(expr: &Expr, leaf: &L) -> Result<BExpr<R>, GnitzSqlError> {
    if let Some(claimed) = leaf.bind_node(expr) {
        return Ok(claimed);
    }
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => leaf.bind_column(expr),
        // The context-independent names (the CASE desugars and the numeric scalar
        // functions) bind here, above the leaf, so no leaf impl carries them.
        // Every other name falls through to the leaf — aggregates, or a context
        // rejection.
        Expr::Function(f) if f.over.is_some() => leaf.bind_window(f),
        Expr::Function(f) => match scalar_call(f) {
            Some((name, call)) => bind_scalar_call(name, call, f, leaf),
            None => match single_fn_name(f) {
                Some(n) if VOLATILE_FNS.iter().any(|v| n.eq_ignore_ascii_case(v)) => {
                    Err(GnitzSqlError::Rejected(format!(
                        "{}: a non-deterministic function is not supported",
                        n.to_ascii_uppercase()
                    )))
                }
                _ => leaf.bind_function(f),
            },
        },
        // CEIL/FLOOR are keyword-dispatched by sqlparser into their own AST nodes
        // and never arrive as `Expr::Function` (CEILING, which is not, does).
        Expr::Ceil { expr: e, field } => bind_ceil_floor(FloatUnaryOp::Ceil, "CEIL", e, field, leaf),
        Expr::Floor { expr: e, field } => bind_ceil_floor(FloatUnaryOp::Floor, "FLOOR", e, field, leaf),
        Expr::Cast {
            // A failed cast is already NULL, which is what TRY_CAST/SAFE_CAST mean.
            kind: _,
            expr: e,
            data_type,
            array,
            format,
        } => {
            reject_if(*array, "CAST", "an ARRAY target type")?;
            reject_if(format.is_some(), "CAST", "FORMAT")?;
            bind_cast(bind_structural(e, leaf)?, data_type)
        }
        Expr::Extract { field, syntax: _, expr: e } => Ok(BExpr::Calendar {
            op: calendar_field(&field.to_string())
                .ok_or_else(|| GnitzSqlError::Rejected(format!("EXTRACT: field {field} is not supported")))?,
            arg: Box::new(bind_structural(e, leaf)?),
        }),
        // SUBSTR and SUBSTRING, the `FROM/FOR` form and the comma form, all
        // arrive as this one keyword-dispatched node; `special`/`shorthand` only
        // record which spelling was written. An absent FROM starts at 1.
        Expr::Substring {
            expr: e,
            substring_from,
            substring_for,
            special: _,
            shorthand: _,
        } => {
            let mut args = vec![
                bind_structural(e, leaf)?,
                match substring_from {
                    Some(f) => bind_structural(f, leaf)?,
                    None => BExpr::LitInt(1),
                },
            ];
            if let Some(l) = substring_for {
                args.push(bind_structural(l, leaf)?);
            }
            Ok(BExpr::StrCall { f: StrFunc::Substr, args })
        }
        Expr::Trim {
            trim_where,
            trim_what,
            expr: e,
            trim_characters,
        } => {
            reject_if(trim_characters.is_some(), "TRIM", "the (… , <characters>) form")?;
            Ok(BExpr::TrimCall {
                s: Box::new(bind_structural(e, leaf)?),
                mode: match trim_where {
                    Some(TrimWhereField::Leading) => TrimMode::Leading,
                    Some(TrimWhereField::Trailing) => TrimMode::Trailing,
                    Some(TrimWhereField::Both) | None => TrimMode::Both,
                },
                set: trim_set(trim_what.as_deref())?,
            })
        }
        // `POSITION(needle IN hay)` is `STRPOS(hay, needle)` with its arguments
        // the other way round.
        Expr::Position { expr: needle, r#in: hay } => Ok(BExpr::StrCall {
            f: StrFunc::Pos,
            args: vec![bind_structural(hay, leaf)?, bind_structural(needle, leaf)?],
        }),
        // `a IS [NOT] DISTINCT FROM b` is the null-safe `<>` (`=`): with a NULL
        // on either side the null flags are compared instead of the values —
        // `CASE WHEN a IS NULL OR b IS NULL THEN (a IS NULL) <> (b IS NULL) ELSE a <> b END`
        // — so every branch is definite and the two polarities are one shape.
        Expr::IsDistinctFrom(a, b) | Expr::IsNotDistinctFrom(a, b) => {
            let op = if matches!(expr, Expr::IsDistinctFrom(..)) {
                BinOp::Ne
            } else {
                BinOp::Eq
            };
            let (a, b) = (bind_structural(a, leaf)?, bind_structural(b, leaf)?);
            // Two never-NULL operands make the null branch dead, so the whole
            // CASE is the plain comparison.
            if let (Nullness::Never, Nullness::Never) = (nullness(&a, leaf), nullness(&b, leaf)) {
                return Ok(BExpr::bin(a, op, b));
            }
            let (a_null, b_null) = (null_test(a.clone(), true, leaf), null_test(b.clone(), true, leaf));
            let cmp = BExpr::bin(a, op, b);
            Ok(BExpr::Case {
                branches: vec![(
                    BExpr::bin(a_null.clone(), BinOp::Or, b_null.clone()),
                    BExpr::bin(a_null, op, b_null),
                )],
                else_: Some(Box::new(cmp)),
            })
        }
        // LIKE and ILIKE differ only in `ci`.
        Expr::Like {
            negated,
            any,
            expr: subject,
            pattern,
            escape_char,
        }
        | Expr::ILike {
            negated,
            any,
            expr: subject,
            pattern,
            escape_char,
        } => {
            reject_if(*any, "LIKE", "ANY")?;
            let escape = like_escape(escape_char.as_ref())?;
            let node = BExpr::Like {
                s: Box::new(bind_structural(subject, leaf)?),
                pattern: like_pattern(pattern, escape)?,
                ci: matches!(expr, Expr::ILike { .. }),
            };
            Ok(maybe_negate(node, *negated))
        }
        Expr::IsNull(i) => Ok(null_test(bind_structural(i, leaf)?, true, leaf)),
        Expr::IsNotNull(i) => Ok(null_test(bind_structural(i, leaf)?, false, leaf)),
        Expr::Nested(i) => bind_structural(i, leaf),
        Expr::Value(vws) => bind_literal(&vws.value),
        // `DATE '…'` (and the ODBC `{d '…'}`) is `CAST('…' AS DATE)`.
        Expr::TypedString(TypedString { data_type, value, uses_odbc_syntax: _ }) => match &value.value {
            Value::SingleQuotedString(s) => bind_cast(BExpr::LitStr(s.clone()), data_type),
            v => Err(GnitzSqlError::Rejected(format!(
                "{data_type} literal must be a single-quoted string, got {v}"
            ))),
        },
        // Searched CASE binds each `(condition, result)`; the simple-operand form
        // desugars each WHEN value `w` to `operand = w`. A missing ELSE is the
        // implicit ELSE NULL (`else_ = None`, lowered to `load_null`).
        //
        // The operand binds once and is cloned per branch. Re-binding it is not
        // the same thing: each bind of a subquery records a second subquery,
        // which decorrelation joins a second time.
        Expr::Case {
            operand,
            conditions,
            else_result,
            case_token: _,
            end_token: _,
        } => {
            let operand = operand.as_deref().map(|op| bind_structural(op, leaf)).transpose()?;
            let mut branches = Vec::with_capacity(conditions.len());
            for CaseWhen { condition, result } in conditions {
                let cond = match &operand {
                    Some(op) => BExpr::bin(op.clone(), BinOp::Eq, bind_structural(condition, leaf)?),
                    None => bind_structural(condition, leaf)?,
                };
                branches.push((cond, bind_structural(result, leaf)?));
            }
            let else_ = else_result
                .as_ref()
                .map(|e| bind_structural(e, leaf))
                .transpose()?
                .map(Box::new);
            Ok(BExpr::Case { branches, else_ })
        }
        Expr::BinaryOp { left, op, right } => {
            let l = bind_structural(left, leaf)?;
            let r = bind_structural(right, leaf)?;
            Ok(BExpr::bin(l, map_binop(op)?, r))
        }
        Expr::UnaryOp { op, expr } => {
            let inner = bind_structural(expr, leaf)?;
            // A negated literal leaves here as the literal `-1`. Unary `+` folds
            // over a numeric literal or NULL and nothing else, as in PostgreSQL:
            // a total identity would accept `+'abc'` and `+(a > 1)`.
            match (op, inner) {
                (UnaryOperator::Minus, inner) => Ok(inner.negate_literal().unwrap_or_else(|inner| BExpr::Func {
                    f: NumFunc::Unary(FloatUnaryOp::Neg),
                    arg: Box::new(inner),
                })),
                (UnaryOperator::Not, inner) => Ok(BExpr::Not(Box::new(inner))),
                (
                    UnaryOperator::Plus,
                    inner @ (BExpr::LitInt(_) | BExpr::LitFloat { .. } | BExpr::LitWide(_) | BExpr::LitNull),
                ) => Ok(inner),
                (o, _) => Err(GnitzSqlError::Rejected(format!("unary operator {o} not supported"))),
            }
        }
        // `e BETWEEN lo AND hi` ≡ `e >= lo AND e <= hi`; NOT BETWEEN negates it.
        // NULL semantics are SQL-correct under either form (a NULL operand makes
        // the AND NULL, and NOT(NULL) is NULL → the row is excluded). The subject
        // binds once and is cloned, for the simple-CASE operand's reason above.
        Expr::Between { expr: e, negated, low, high } => {
            let subject = bind_structural(e, leaf)?;
            let ge = BExpr::bin(subject.clone(), BinOp::Ge, bind_structural(low, leaf)?);
            let le = BExpr::bin(subject, BinOp::Le, bind_structural(high, leaf)?);
            Ok(maybe_negate(BExpr::bin(ge, BinOp::And, le), *negated))
        }
        // `e IN (l)` IS `e = l` — the same structural desugar as BETWEEN above. Two or
        // more items keep the faithful `InList` node for lowering to shape. `NOT IN`
        // wraps whichever node the arity picked.
        Expr::InList { expr: e, list, negated } => {
            let node = match list.as_slice() {
                [] => return Err(GnitzSqlError::Rejected("IN with an empty list".into())),
                [only] => BExpr::bin(bind_structural(e, leaf)?, BinOp::Eq, bind_structural(only, leaf)?),
                _ => BExpr::InList {
                    inner: Box::new(bind_structural(e, leaf)?),
                    items: list
                        .iter()
                        .map(|it| bind_structural(it, leaf))
                        .collect::<Result<Vec<_>, _>>()?,
                },
            };
            Ok(maybe_negate(node, *negated))
        }
        // Subquery placement is the leaf's decision: the HIR view leaf records the
        // node for decorrelation; every other leaf keeps the default per-kind
        // placement rejection (HAVING, DML, a direct SELECT, …).
        Expr::Exists { .. } | Expr::InSubquery { .. } | Expr::Subquery(_) | Expr::AnyOp { .. } | Expr::AllOp { .. } => {
            leaf.bind_subquery(expr)
        }
        // Rendered as SQL, never `Debug`: the parser's `Debug` is a wall of
        // spans, and the reader wants the form they wrote.
        _ => Err(GnitzSqlError::Rejected(format!("expression not supported: {expr}"))),
    }
}

/// The bytes a TRIM strips: an ASCII string literal, defaulting to a single
/// space. The set is compile-time data, so a NULL one is rejected here, where
/// PostgreSQL's `btrim(s, NULL)` is NULL per row.
fn trim_set(trim_what: Option<&Expr>) -> Result<String, GnitzSqlError> {
    let Some(e) = trim_what else {
        return Ok(" ".to_string());
    };
    let bad = || GnitzSqlError::Rejected("TRIM: the characters to trim must be an ASCII string literal".into());
    literal_expr_string(e).filter(|s| s.is_ascii()).ok_or_else(bad)
}

/// The `NOT` wrapper the negatable predicates (BETWEEN, IN, LIKE, a subquery's
/// `NOT EXISTS` / `NOT IN`) share.
pub(crate) fn maybe_negate<R>(node: BExpr<R>, negated: bool) -> BExpr<R> {
    if negated {
        BExpr::Not(Box::new(node))
    } else {
        node
    }
}

/// The string a compile-time-data position (TRIM's byte set, LIKE's pattern)
/// spells, read through [`bind_constant`] so `TRIM(s, ('ab'))` reads like
/// `TRIM(s, 'ab')`. `None` for anything else.
fn literal_expr_string(e: &Expr) -> Option<String> {
    match bind_constant(e) {
        Ok(BExpr::LitStr(s)) => Some(s),
        _ => None,
    }
}

/// The one decoder for a constant position: a literal, parenthesized and signed
/// as written, bound by the expression binder, so a VALUES cell and a WHERE
/// operand cannot disagree on one.
pub(crate) fn bind_constant(e: &Expr) -> Result<BExpr<Infallible>, GnitzSqlError> {
    let not_constant = || GnitzSqlError::Rejected(format!("expected a constant, got the expression: {e}"));
    // Checked on the AST first: the binder folds `COALESCE(2, x)` and `NULL IS
    // NULL` to literals without binding the rest.
    if !literal_shaped(e) {
        return Err(not_constant());
    }
    let lit = bind_structural(e, &NoColumns)?;
    lit.is_literal().then_some(lit).ok_or_else(not_constant)
}

/// Parentheses and `+`/`-` around a `Value`, a typed string, or a cast of a `Value`.
fn literal_shaped(e: &Expr) -> bool {
    match peel_nested(e) {
        Expr::UnaryOp {
            op: UnaryOperator::Minus | UnaryOperator::Plus,
            expr,
        } => literal_shaped(expr),
        Expr::Value(_) | Expr::TypedString(_) => true,
        Expr::Cast { expr, .. } => matches!(peel_nested(expr), Expr::Value(_)),
        _ => false,
    }
}

/// A constant position names no column.
struct NoColumns;

impl LeafBinder<Infallible> for NoColumns {
    fn bind_column(&self, _: &Expr) -> Result<BExpr<Infallible>, GnitzSqlError> {
        unreachable!("`literal_shaped` admits no identifier")
    }
    fn is_nullable(&self, r: &Infallible) -> bool {
        match *r {}
    }
    fn type_of(&self, r: &Infallible) -> ColType {
        match *r {}
    }
}

/// A string literal only, so `s LIKE NULL` is rejected where PostgreSQL
/// evaluates it to NULL.
fn like_pattern(pattern: &Expr, escape: Option<char>) -> Result<LikePattern, GnitzSqlError> {
    let bad = || GnitzSqlError::Rejected("LIKE pattern must be a string literal".into());
    let s = literal_expr_string(pattern).ok_or_else(bad)?;
    LikePattern::encode(&s, escape)
        .ok_or_else(|| GnitzSqlError::Rejected("LIKE pattern must not end with escape character".to_string()))
}

/// `\` by default, as in PostgreSQL and MySQL; `None` for `ESCAPE ''`.
fn like_escape(escape_char: Option<&ValueWithSpan>) -> Result<Option<char>, GnitzSqlError> {
    let Some(v) = escape_char else { return Ok(Some('\\')) };
    let bad = || GnitzSqlError::Rejected("LIKE: ESCAPE must be a single character or ''".into());
    // A `ValueWithSpan`, not an `Expr`: the parser puts the escape in its own
    // slot, so there is no sign or parenthesis for `bind_constant` to peel.
    let Ok(BExpr::LitStr(s)) = bind_literal::<Infallible>(&v.value) else {
        return Err(bad());
    };
    let mut chars = s.chars();
    match (chars.next(), chars.next()) {
        (None, _) => Ok(None),
        (Some(c), None) => Ok(Some(c)),
        _ => Err(bad()),
    }
}

/// What a structurally-bound function name binds to.
#[derive(Clone, Copy)]
enum Call {
    Coalesce,
    Nullif,
    /// A unary numeric transform over its single argument.
    Unary(FloatUnaryOp),
    Round,
    /// A binary operator spelled as a call: `MOD(a, b)`, `POWER(a, b)`.
    Binary(BinOp),
    /// GREATEST (`true`) / LEAST (`false`).
    MinMax(bool),
    /// A string function, sized by [`StrFunc::signature`].
    Str(StrFunc),
    /// `IF(c, a, b)`: a one-branch CASE.
    If,
    /// `IFNULL` / `NVL`: two-argument COALESCE.
    Ifnull,
    /// `LTRIM`/`RTRIM`: one argument, or two with a literal trim set.
    Trim1(TrimMode),
    Concat,
    /// `DATE_TRUNC('unit', x)` and `DATE_PART('field', x)`: a literal unit
    /// name, then the temporal operand.
    DateTrunc,
    DatePart,
}

impl Call {
    /// The `(min, max)` argument count this call takes, `max` unbounded when
    /// `None` — the one arity declaration per call. `Ifnull` is a two-argument
    /// `COALESCE`, and this is the only thing that distinguishes them.
    fn arity(self) -> (usize, Option<usize>) {
        match self {
            Call::Coalesce | Call::MinMax(_) | Call::Concat => (1, None),
            Call::Ifnull | Call::Nullif | Call::Binary(_) | Call::DateTrunc | Call::DatePart => (2, Some(2)),
            Call::If => (3, Some(3)),
            Call::Unary(_) => (1, Some(1)),
            Call::Round | Call::Trim1(_) => (1, Some(2)),
            Call::Str(f) => {
                let sig = f.signature();
                // A trailing slot a call need not write may be omitted.
                (sig.iter().filter(|a| a.required()).count(), Some(sig.len()))
            }
        }
    }
}

/// The function names `bind_structural` binds above the leaf, keyed by their SQL
/// spelling (which also names the call in its arity errors). Matched
/// case-insensitively. The one name→call map, like `AGG_NAMES` is for the
/// aggregates: a name added here reaches every binding context at once.
const SCALAR_CALLS: &[(&str, Call)] = &[
    ("COALESCE", Call::Coalesce),
    ("NULLIF", Call::Nullif),
    ("ABS", Call::Unary(FloatUnaryOp::Abs)),
    ("CEILING", Call::Unary(FloatUnaryOp::Ceil)),
    ("TRUNC", Call::Unary(FloatUnaryOp::Trunc)),
    ("SQRT", Call::Unary(FloatUnaryOp::Sqrt)),
    ("LN", Call::Unary(FloatUnaryOp::Ln)),
    ("LOG", Call::Unary(FloatUnaryOp::Log10)),
    ("EXP", Call::Unary(FloatUnaryOp::Exp)),
    ("SIGN", Call::Unary(FloatUnaryOp::Sign)),
    ("POWER", Call::Binary(BinOp::Pow)),
    ("POW", Call::Binary(BinOp::Pow)),
    ("ROUND", Call::Round),
    ("MOD", Call::Binary(BinOp::Mod)),
    ("GREATEST", Call::MinMax(true)),
    ("LEAST", Call::MinMax(false)),
    ("UPPER", Call::Str(StrFunc::Upper)),
    ("LOWER", Call::Str(StrFunc::Lower)),
    ("LENGTH", Call::Str(StrFunc::LenChars)),
    ("CHAR_LENGTH", Call::Str(StrFunc::LenChars)),
    ("CHARACTER_LENGTH", Call::Str(StrFunc::LenChars)),
    ("OCTET_LENGTH", Call::Str(StrFunc::LenBytes)),
    ("REVERSE", Call::Str(StrFunc::Reverse)),
    ("LEFT", Call::Str(StrFunc::Left)),
    ("RIGHT", Call::Str(StrFunc::Right)),
    ("STRPOS", Call::Str(StrFunc::Pos)),
    ("REPLACE", Call::Str(StrFunc::Replace)),
    ("LPAD", Call::Str(StrFunc::Lpad)),
    ("RPAD", Call::Str(StrFunc::Rpad)),
    ("SPLIT_PART", Call::Str(StrFunc::SplitPart)),
    ("SUBSTRING", Call::Str(StrFunc::Substr)),
    ("IF", Call::If),
    ("IFNULL", Call::Ifnull),
    ("NVL", Call::Ifnull),
    ("LTRIM", Call::Trim1(TrimMode::Leading)),
    ("RTRIM", Call::Trim1(TrimMode::Trailing)),
    ("CONCAT", Call::Concat),
    ("DATE_TRUNC", Call::DateTrunc),
    ("DATE_PART", Call::DatePart),
];

/// The clock and random functions, beside [`SCALAR_CALLS`] so the two name
/// tables live together. Listed to say *why* they are refused, which the
/// unknown-name error they would otherwise take cannot: nothing re-evaluates a
/// clock or a coin flip off an input delta, so they are not pending support.
const VOLATILE_FNS: &[&str] = &[
    "NOW",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "RANDOM",
    "RAND",
];

/// The calendar field a unit name selects, as `EXTRACT` (which is how
/// sqlparser spells its field) and `DATE_PART` write it. Case and a plural `s`
/// are ignored. `DATE_TRUNC`'s units are this same vocabulary read through
/// [`CalendarOp::trunc_of`], so they are not spelled a second time.
fn calendar_field(name: &str) -> Option<CalendarOp> {
    use CalendarOp as C;
    let n = name.trim().to_ascii_lowercase();
    match n.strip_suffix('s').filter(|s| !s.is_empty()).unwrap_or(&n) {
        "year" => Some(C::Year),
        "quarter" => Some(C::Quarter),
        "month" => Some(C::Month),
        "week" | "isoweek" => Some(C::Week),
        "day" => Some(C::Day),
        "dow" | "dayofweek" => Some(C::Dow),
        "isodow" => Some(C::Isodow),
        "doy" | "dayofyear" => Some(C::Doy),
        "hour" => Some(C::Hour),
        "minute" => Some(C::Minute),
        "second" => Some(C::Second),
        "epoch" => Some(C::Epoch),
        _ => None,
    }
}

fn scalar_call(f: &Function) -> Option<(&'static str, Call)> {
    let n = single_fn_name(f)?;
    SCALAR_CALLS
        .iter()
        .find(|(name, _)| n.eq_ignore_ascii_case(name))
        .copied()
}

/// Bind one of the [`SCALAR_CALLS`].
fn bind_scalar_call<R: Clone, L: LeafBinder<R>>(
    name: &str,
    call: Call,
    f: &Function,
    leaf: &L,
) -> Result<BExpr<R>, GnitzSqlError> {
    let args = PlainCall::check(f, CallSurface::Scalar(name))?.args(name, call.arity())?;
    match call {
        // IFNULL/NVL is COALESCE at arity two; only `Call::arity` separates them.
        Call::Coalesce | Call::Ifnull => bind_coalesce(&args, leaf),
        // `NULLIF(a, b)` ≡ `CASE WHEN a = b THEN NULL ELSE a END`.
        Call::Nullif => {
            let a = bind_structural(args[0], leaf)?;
            let b = bind_structural(args[1], leaf)?;
            Ok(BExpr::Case {
                branches: vec![(BExpr::bin(a.clone(), BinOp::Eq, b), BExpr::LitNull)],
                else_: Some(Box::new(a)),
            })
        }
        Call::Unary(op) => Ok(BExpr::Func {
            f: NumFunc::Unary(op),
            arg: Box::new(bind_structural(args[0], leaf)?),
        }),
        // The scale rides the IR node rather than desugaring here: which form
        // `ROUND` takes depends on the argument's type, and the binder is
        // schema-free. Read first, so `ROUND(x, 2.5)` names its own defect.
        Call::Round => {
            let f = match args.get(1) {
                Some(n) => NumFunc::Round(round_scale(n)?),
                None => NumFunc::Unary(FloatUnaryOp::Round),
            };
            Ok(BExpr::Func {
                f,
                arg: Box::new(bind_structural(args[0], leaf)?),
            })
        }
        Call::Binary(op) => {
            let a = bind_structural(args[0], leaf)?;
            let b = bind_structural(args[1], leaf)?;
            Ok(BExpr::bin(a, op, b))
        }
        // NULL skipping is the MAX2/MIN2 opcode's, so no argument needs a
        // null-test rewrite and a computed argument is as good as a column.
        Call::MinMax(is_max) => Ok(BExpr::MinMaxN { is_max, args: bind_all(&args, leaf)? }),
        Call::Str(sf) => bind_str_call(sf, &args, leaf),
        Call::If => {
            let c = bind_structural(args[0], leaf)?;
            let a = bind_structural(args[1], leaf)?;
            let b = bind_structural(args[2], leaf)?;
            Ok(BExpr::Case {
                branches: vec![(c, a)],
                else_: Some(Box::new(b)),
            })
        }
        // The subject binds before the set is decoded, so `LTRIM(bad, 'x')`
        // names the operand rather than the set.
        Call::Trim1(mode) => Ok(BExpr::TrimCall {
            s: Box::new(bind_structural(args[0], leaf)?),
            mode,
            set: trim_set(args.get(1).copied())?,
        }),
        Call::Concat => Ok(BExpr::ConcatN { args: bind_all(&args, leaf)? }),
        Call::DateTrunc | Call::DatePart => {
            let unit = literal_expr_string(args[0])
                .ok_or_else(|| GnitzSqlError::Rejected(format!("{name}: the unit must be a string literal")))?;
            // DATE_TRUNC takes the truncating half of the same vocabulary, so
            // the fields with no truncation (DOW, EPOCH, …) fall out as
            // unsupported units here rather than needing a second table.
            let op = calendar_field(&unit).and_then(|op| match call {
                Call::DateTrunc => op.trunc_of(),
                _ => Some(op),
            });
            Ok(BExpr::Calendar {
                op: op.ok_or_else(|| GnitzSqlError::Rejected(format!("{name}: unit {unit:?} is not supported")))?,
                arg: Box::new(bind_structural(args[1], leaf)?),
            })
        }
    }
}

/// A string function call, its omitted trailing slots supplied as the literal
/// defaults [`StrFunc::signature`] declares, so the node carries every defaulted
/// slot (an omitted `IntOpt` stays absent).
fn bind_str_call<R: Clone, L: LeafBinder<R>>(f: StrFunc, args: &[&Expr], leaf: &L) -> Result<BExpr<R>, GnitzSqlError> {
    let sig = f.signature();
    let mut bound = bind_all(args, leaf)?;
    bound.extend(
        sig[args.len()..]
            .iter()
            .filter_map(|a| StrArg::default(*a))
            .map(|d| BExpr::LitStr(d.to_string())),
    );
    Ok(BExpr::StrCall { f, args: bound })
}

/// Every argument of a call, bound in written order.
fn bind_all<R: Clone, L: LeafBinder<R>>(args: &[&Expr], leaf: &L) -> Result<Vec<BExpr<R>>, GnitzSqlError> {
    args.iter().map(|a| bind_structural(a, leaf)).collect()
}

/// The `n` of `ROUND(x, n)`: an integer literal, optionally signed, in
/// `-15..=15`. f64 carries ~15–17 significant decimal digits, so a wider scale
/// has no digits left to round at.
fn round_scale(e: &Expr) -> Result<i8, GnitzSqlError> {
    let bad = || GnitzSqlError::Rejected("ROUND: scale must be an integer literal in -15..=15".to_string());
    let BExpr::LitInt(n) = bind_constant(e).map_err(|_| bad())? else {
        return Err(bad());
    };
    i8::try_from(n).ok().filter(|n| (-15..=15).contains(n)).ok_or_else(bad)
}

/// `CEIL(x)` / `FLOOR(x)`. Only the bare-call parse binds: `CEIL(x TO DAY)` and
/// `CEIL(x, 2)` carry a field this plan's opcode has no place for, so they are
/// rejected rather than having the field dropped.
fn bind_ceil_floor<R: Clone, L: LeafBinder<R>>(
    f: FloatUnaryOp,
    name: &str,
    e: &Expr,
    field: &CeilFloorKind,
    leaf: &L,
) -> Result<BExpr<R>, GnitzSqlError> {
    if !matches!(field, CeilFloorKind::DateTimeField(DateTimeField::NoDateTime)) {
        return Err(GnitzSqlError::Rejected(format!(
            "{name}: only the plain {name}(x) form is supported"
        )));
    }
    Ok(BExpr::Func {
        f: NumFunc::Unary(f),
        arg: Box::new(bind_structural(e, leaf)?),
    })
}

/// sqlparser binary op → `BinOp` (the single, complete map).
fn map_binop(op: &BinaryOperator) -> Result<BinOp, GnitzSqlError> {
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
        BinaryOperator::StringConcat => BinOp::Concat,
        o => return Err(GnitzSqlError::Rejected(format!("binary operator {o} not supported"))),
    })
}

/// `COALESCE(args…)` desugars right-to-left into CASE: the first non-NULL operand
/// is the result. An operand whose nullness is settled at bind time — a literal,
/// a NOT NULL column — ends or skips the chain there, so the rest is never bound.
fn bind_coalesce<R: Clone, L: LeafBinder<R>>(args: &[&Expr], leaf: &L) -> Result<BExpr<R>, GnitzSqlError> {
    let (a, rest) = args.split_first().expect("arity checked");
    let value = bind_structural(a, leaf)?;
    if rest.is_empty() {
        return Ok(value); // last operand: its value is the result
    }
    match nullness(&value, leaf) {
        Nullness::Never => Ok(value),
        Nullness::Always => bind_coalesce(rest, leaf),
        Nullness::Unknown => Ok(BExpr::coalesce(value, bind_coalesce(rest, leaf)?)),
    }
}

/// What is settled about a bound value's nullness at bind time.
enum Nullness {
    /// Provably never NULL.
    Never,
    /// Provably always NULL — the `LitNull` literal, the one value the prover
    /// reports as nullable *because* it is always NULL.
    Always,
    /// Only the row can tell.
    Unknown,
}

/// A bound value's [`Nullness`], read through [`BExpr::never_null_with`] over the
/// leaf's own verdict per reference. The verdict alone, for a caller that only
/// needs to know which shape to emit and would otherwise clone the value to ask.
fn nullness<R, L: LeafBinder<R>>(value: &BExpr<R>, leaf: &L) -> Nullness {
    match value {
        BExpr::LitNull => Nullness::Always,
        v if v.never_null_with(&|r| leaf.is_nullable(r), &|r| leaf.type_of(r)) => Nullness::Never,
        _ => Nullness::Unknown,
    }
}

/// `value IS [NOT] NULL`, settled at bind time wherever the value's nullness is
/// provable, so a never-null operand costs no test at runtime and a filter over
/// one can be elided whole. Anything else is a `NullTest` over the value.
fn null_test<R, L: LeafBinder<R>>(value: BExpr<R>, want_null: bool, leaf: &L) -> BExpr<R> {
    match nullness(&value, leaf) {
        Nullness::Never => BExpr::LitInt(i64::from(!want_null)),
        Nullness::Always => BExpr::LitInt(i64::from(want_null)),
        Nullness::Unknown => BExpr::NullTest { inner: Box::new(value), want_null },
    }
}

/// The position of an `Identifier` / two-part `CompoundIdentifier` within one
/// relation's columns. A written qualifier must name `alias`, the relation's
/// effective alias, so `SELECT b.val FROM a` is a rejection rather than a read of
/// `a.val`; it disambiguates nothing beyond that. An empty `alias` means no
/// relation is in scope: there is no alias a qualifier could name, so such a
/// reference is reported whole.
///
/// The one home for the single-relation resolve contract — the "expected a column
/// reference" / "column not found" pair and `find_unique_column`'s ambiguity
/// error.
pub(crate) fn single_relation_col_idx<'a>(
    cols: impl IntoIterator<Item = &'a ColumnDef>,
    alias: &str,
    e: &Expr,
) -> Result<usize, GnitzSqlError> {
    let (qual, name) = col_ref_parts(e).ok_or_else(|| GnitzSqlError::Rejected("expected a column reference".into()))?;
    reject_foreign_qualifier(qual, name, alias)?;
    require_column(cols, name)
}

/// The qualifier half of [`single_relation_col_idx`], for a resolver that carries
/// its own name lookup.
pub(crate) fn reject_foreign_qualifier(qual: Option<&str>, name: &str, alias: &str) -> Result<(), GnitzSqlError> {
    let Some(q) = qual else { return Ok(()) };
    if alias.is_empty() {
        return Err(GnitzSqlError::Rejected(format!(
            "column '{q}.{name}' not found (no relation is in scope)"
        )));
    }
    if !q.eq_ignore_ascii_case(alias) {
        return Err(GnitzSqlError::Rejected(format!(
            "table alias '{q}' not found (the relation in scope is '{alias}')"
        )));
    }
    Ok(())
}

/// Leaf for one relation's columns under one alias.
pub(crate) struct SingleTable<'a> {
    pub schema: &'a Schema,
    /// The relation's effective alias — what a written qualifier must name.
    pub alias: &'a str,
}

impl LeafBinder<usize> for SingleTable<'_> {
    fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError> {
        Ok(BoundExpr::ColRef(single_relation_col_idx(
            &self.schema.columns,
            self.alias,
            e,
        )?))
    }
    fn is_nullable(&self, r: &usize) -> bool {
        self.schema.columns[*r].is_nullable
    }
    fn type_of(&self, r: &usize) -> ColType {
        self.schema.columns[*r].ty
    }
}

#[cfg(test)]
#[path = "tests/structural.rs"]
mod tests;
