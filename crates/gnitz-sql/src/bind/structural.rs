use std::convert::Infallible;

use super::resolve::find_unique_column;
use crate::ast_util::{
    bind_constant, bind_literal, classify_agg_call, col_ref_parts, function_positional_args, single_fn_name,
    temporal_constant, Constant,
};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BinOp, BoundExpr, FloatUnaryOp, NumFunc, StrFunc, TrimMode, UnaryOp};
use crate::types::{is_cast_target, sql_type_to_typecode};
use gnitz_core::{ColumnDef, Schema};
use gnitz_expr::CalendarOp;
use sqlparser::ast::{
    BinaryOperator, CaseWhen, CeilFloorKind, DateTimeField, Expr, Function, TrimWhereField, UnaryOperator,
    ValueWithSpan,
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
    fn split<R>(e: BExpr<R>, out: &mut Vec<BExpr<R>>) {
        match e {
            BExpr::BinOp(l, BinOp::And, r) => {
                split(*l, out);
                split(*r, out);
            }
            e => out.push(e),
        }
    }
    let mut out = Vec::new();
    split(bind_structural(expr, leaf)?, &mut out);
    Ok(out)
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
    /// default rejects one — after `classify_agg_call`, so an unknown or
    /// malformed call is named as such and reaching past it means the name
    /// really is an aggregate.
    fn bind_function(&self, f: &Function) -> Result<BExpr<R>, GnitzSqlError> {
        classify_agg_call(f)?;
        Err(GnitzSqlError::Unsupported(
            "aggregate function not allowed in expression context".to_string(),
        ))
    }
    /// Whether the value a leaf reference names can be NULL — the one
    /// nullability fact a leaf owns, which [`nullness`] folds a test by.
    fn is_nullable(&self, r: &R) -> bool;
    /// A windowed call (`f(…) OVER (…)`). Only a view body's windowed SELECT
    /// list and QUALIFY admit one; every other context keeps this rejection.
    fn bind_window(&self, _f: &Function) -> Result<BExpr<R>, GnitzSqlError> {
        Err(GnitzSqlError::Unsupported(
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

/// The per-kind "no subquery here" rejection — the [`LeafBinder::bind_subquery`]
/// default, and what a leaf that overrides the method for *some* shapes falls back
/// to for the rest.
pub(crate) fn unsupported_subquery(e: &Expr) -> GnitzSqlError {
    GnitzSqlError::Unsupported(match e {
        Expr::Subquery(_) => "scalar subqueries are not supported".into(),
        Expr::AnyOp { .. } | Expr::AllOp { .. } => "ANY/SOME/ALL subquery comparisons are not supported".into(),
        _ => "[NOT] EXISTS/IN (SELECT …) is only supported in a single-table CREATE VIEW \
              (in the WHERE clause or the SELECT list)"
            .into(),
    })
}

/// The one structural recursion. Needs no schema — every schema-aware decision
/// is a leaf method. Generic over `L` (static dispatch) so a leaf's
/// `bind_function` can recurse via `bind_structural(arg, self)`.
pub(crate) fn bind_structural<R: Clone, L: LeafBinder<R>>(expr: &Expr, leaf: &L) -> Result<BExpr<R>, GnitzSqlError> {
    if let Some(claimed) = leaf.bind_node(expr) {
        return Ok(claimed);
    }
    // The VM parses no calendar text, so a temporal literal is folded here and
    // a non-literal string cast to DATE/TIMESTAMP is refused at lowering.
    if let Some((to, v)) = temporal_constant(expr)? {
        return Ok(BExpr::temporal_lit(to, v));
    }
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => leaf.bind_column(expr),
        // The context-independent names (the CASE desugars and the numeric scalar
        // functions) bind here, above the leaf, so no leaf impl carries them.
        // Every other name falls through to the leaf — aggregates, or a context
        // rejection.
        // Above the name dispatch, so `ABS(x) OVER (…)` is a window call the leaf
        // refuses by name, not a scalar call refused for carrying a qualifier.
        Expr::Function(f) if f.over.is_some() => leaf.bind_window(f),
        Expr::Function(f) => match scalar_call(f) {
            Some((name, call)) => bind_scalar_call(name, call, f, leaf),
            None => match single_fn_name(f) {
                Some(n) if VOLATILE_FNS.iter().any(|v| n.eq_ignore_ascii_case(v)) => {
                    Err(GnitzSqlError::Unsupported(format!(
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
            // Every kind is the same operation here: a failed cast is a NULL, which
            // is exactly what TRY_CAST/SAFE_CAST are documented to mean, so the
            // explicit spellings are accepted rather than rejected. Destructuring
            // the rest exhaustively is what keeps a qualifier from being dropped.
            kind: _,
            expr: e,
            data_type,
            array,
            format,
        } => {
            if *array {
                return Err(GnitzSqlError::Unsupported(
                    "CAST to an ARRAY type is not supported".into(),
                ));
            }
            if format.is_some() {
                return Err(GnitzSqlError::Unsupported("CAST … FORMAT is not supported".into()));
            }
            let to = sql_type_to_typecode(data_type)?;
            // The 16-byte wide integer types have no register at all.
            if !is_cast_target(to) {
                return Err(GnitzSqlError::Unsupported(format!("CAST to {to:?} is not supported")));
            }
            Ok(BExpr::Cast {
                expr: Box::new(bind_structural(e, leaf)?),
                to,
            })
        }
        Expr::Extract { field, syntax: _, expr: e } => Ok(BExpr::Calendar {
            op: calendar_field(&field.to_string())
                .ok_or_else(|| GnitzSqlError::Unsupported(format!("EXTRACT: field {field} is not supported")))?,
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
        } => Ok(BExpr::Substr {
            s: Box::new(bind_structural(e, leaf)?),
            start: Box::new(match substring_from {
                Some(f) => bind_structural(f, leaf)?,
                None => BExpr::LitInt(1),
            }),
            len: substring_for
                .as_deref()
                .map(|l| bind_structural(l, leaf))
                .transpose()?
                .map(Box::new),
        }),
        Expr::Trim {
            trim_where,
            trim_what,
            expr: e,
            trim_characters,
        } => {
            if trim_characters.is_some() {
                return Err(GnitzSqlError::Unsupported(
                    "TRIM(… , <characters>) is not supported".into(),
                ));
            }
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
                return Ok(BExpr::BinOp(Box::new(a), op, Box::new(b)));
            }
            let (a_null, b_null) = (null_test(a.clone(), true, leaf), null_test(b.clone(), true, leaf));
            let cmp = BExpr::BinOp(Box::new(a), op, Box::new(b));
            Ok(BExpr::Case {
                branches: vec![(
                    BExpr::BinOp(Box::new(a_null.clone()), BinOp::Or, Box::new(b_null.clone())),
                    BExpr::BinOp(Box::new(a_null), op, Box::new(b_null)),
                )],
                else_: Some(Box::new(cmp)),
            })
        }
        // LIKE and ILIKE differ only in case folding, so one arm binds both and
        // takes `ci` from the node it matched. Only the subject is an operand:
        // the pattern and the escape are compile-time data (`like_pattern` /
        // `like_escape`), which is also what lets the trailing-escape rule be
        // decided here rather than per row.
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
            if *any {
                return Err(GnitzSqlError::Unsupported("LIKE ANY is not supported".into()));
            }
            let escape = like_escape(escape_char.as_ref())?;
            let node = BExpr::Like {
                s: Box::new(bind_structural(subject, leaf)?),
                pattern: like_pattern(pattern, escape)?,
                escape,
                ci: matches!(expr, Expr::ILike { .. }),
            };
            Ok(maybe_negate(node, *negated))
        }
        Expr::IsNull(i) => Ok(null_test(bind_structural(i, leaf)?, true, leaf)),
        Expr::IsNotNull(i) => Ok(null_test(bind_structural(i, leaf)?, false, leaf)),
        Expr::Nested(i) => bind_structural(i, leaf),
        Expr::Value(vws) => bind_literal(&vws.value),
        // Searched CASE binds each `(condition, result)`; the simple-operand form
        // desugars each WHEN value `w` to `operand = w`. A missing ELSE is the
        // implicit ELSE NULL (`else_ = None`, lowered to `load_null`).
        //
        // The operand binds once and is cloned per branch. Re-binding it is not
        // the same thing: each bind mints a fresh node identity, and a subquery
        // under two identities trips the decorrelation rewrite's one-mark rule.
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
                    Some(op) => BExpr::BinOp(
                        Box::new(op.clone()),
                        BinOp::Eq,
                        Box::new(bind_structural(condition, leaf)?),
                    ),
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
            Ok(BExpr::BinOp(Box::new(l), map_binop(op)?, Box::new(r)))
        }
        Expr::UnaryOp { op, expr } => {
            let inner = bind_structural(expr, leaf)?;
            // A negated literal leaves here as the literal `-1`, the one shape
            // every consumer reads a constant by. Unary `+` folds over a numeric
            // literal and nothing else, as in PostgreSQL: a total identity would
            // newly accept `+'abc'` and `+(a > 1)`.
            match (op, inner) {
                (UnaryOperator::Minus, BExpr::LitInt(v)) => Ok(BExpr::LitInt(-v)),
                (UnaryOperator::Minus, BExpr::LitFloat(v)) => Ok(BExpr::LitFloat(-v)),
                (UnaryOperator::Minus, inner) => Ok(BExpr::UnaryOp(UnaryOp::Neg, Box::new(inner))),
                (UnaryOperator::Not, inner) => Ok(BExpr::UnaryOp(UnaryOp::Not, Box::new(inner))),
                (UnaryOperator::Plus, inner @ (BExpr::LitInt(_) | BExpr::LitFloat(_) | BExpr::LitWide(_))) => Ok(inner),
                (o, _) => Err(GnitzSqlError::Unsupported(format!(
                    "unary operator {o:?} not supported"
                ))),
            }
        }
        // `e BETWEEN lo AND hi` ≡ `e >= lo AND e <= hi`; NOT BETWEEN negates it.
        // NULL semantics are SQL-correct under either form (a NULL operand makes
        // the AND NULL, and NOT(NULL) is NULL → the row is excluded). The subject
        // binds once and is cloned, for the simple-CASE operand's reason above.
        Expr::Between { expr: e, negated, low, high } => {
            let subject = bind_structural(e, leaf)?;
            let ge = BExpr::BinOp(
                Box::new(subject.clone()),
                BinOp::Ge,
                Box::new(bind_structural(low, leaf)?),
            );
            let le = BExpr::BinOp(Box::new(subject), BinOp::Le, Box::new(bind_structural(high, leaf)?));
            Ok(maybe_negate(
                BExpr::BinOp(Box::new(ge), BinOp::And, Box::new(le)),
                *negated,
            ))
        }
        // `e IN (l)` IS `e = l` — the same structural desugar as BETWEEN above, and
        // what makes the equality visible to the `access` recognizers, which all gate
        // on `BinOp(_, Eq, _)`. Two or more items keep the faithful `InList` node for
        // lowering to shape. `NOT IN` wraps whichever node the arity picked. Item
        // binding is eager, so a NULL/string/non-literal item errors in its written
        // position.
        Expr::InList { expr: e, list, negated } => {
            let node = match list.as_slice() {
                [] => return Err(GnitzSqlError::Unsupported("IN with an empty list".into())),
                [only] => BExpr::BinOp(
                    Box::new(bind_structural(e, leaf)?),
                    BinOp::Eq,
                    Box::new(bind_structural(only, leaf)?),
                ),
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
        _ => Err(GnitzSqlError::Unsupported(format!("expression not supported: {expr}"))),
    }
}

/// The bytes a TRIM strips: an ASCII string literal, defaulting to a single
/// space.
///
/// A non-literal, non-ASCII or NULL set is rejected here rather than deferred,
/// because the set is compile-time data the engine bakes into a membership
/// table, not a per-row operand. Restricting it to ASCII is what makes the strip
/// byte-wise and still character-safe: an ASCII byte never occurs inside a
/// UTF-8 multibyte sequence. PostgreSQL instead treats `btrim(s, NULL)` as
/// runtime NULL propagation.
fn trim_set(trim_what: Option<&Expr>) -> Result<String, GnitzSqlError> {
    let Some(e) = trim_what else {
        return Ok(" ".to_string());
    };
    let bad = || GnitzSqlError::Unsupported("TRIM: the characters to trim must be an ASCII string literal".into());
    literal_expr_string(e).filter(|s| s.is_ascii()).ok_or_else(bad)
}

/// The `NOT` wrapper the negatable predicates (BETWEEN, IN, LIKE) share.
fn maybe_negate<R>(node: BExpr<R>, negated: bool) -> BExpr<R> {
    if negated {
        BExpr::UnaryOp(UnaryOp::Not, Box::new(node))
    } else {
        node
    }
}

/// The string a compile-time-data position (TRIM's byte set, LIKE's pattern)
/// spells, read through [`bind_constant`] so `TRIM(s, ('ab'))` reads like
/// `TRIM(s, 'ab')` and accepts the spellings every other literal position does.
/// `None` for anything else — a NULL or numeric literal is a non-`LitStr`.
fn literal_expr_string(e: &Expr) -> Option<String> {
    match bind_constant(e) {
        Ok(Constant { lit: BExpr::LitStr(s), .. }) => Some(s),
        _ => None,
    }
}

/// The pattern of a LIKE: a string literal, and not one ending in a live escape
/// character.
///
/// A non-literal pattern is rejected for `trim_set`'s reason — it is
/// compile-time data the engine tokenizes once per program, not a per-row
/// operand. `s LIKE NULL` falls out as a non-`LitStr` and is rejected, where
/// PostgreSQL evaluates it to NULL — the deviation `btrim(s, NULL)` already
/// carries.
fn like_pattern(pattern: &Expr, escape: Option<u8>) -> Result<String, GnitzSqlError> {
    let bad = || GnitzSqlError::Unsupported("LIKE pattern must be a string literal".into());
    let s = literal_expr_string(pattern).ok_or_else(bad)?;
    // The engine's own tokenizer answers, so the binder's rule and the matcher's
    // cannot drift.
    if gnitz_expr::like_pattern_ends_with_live_escape(s.as_bytes(), escape) {
        return Err(GnitzSqlError::Plan(
            "LIKE pattern must not end with escape character".to_string(),
        ));
    }
    Ok(s)
}

/// The escape character of a LIKE: `\` by default (PostgreSQL's and MySQL's
/// choice; the standard specifies none), a single ASCII non-NUL character from
/// `ESCAPE 'c'`, or `None` — escaping disabled — from `ESCAPE ''`.
///
/// Two deviations from PostgreSQL, which takes any single character of the
/// database encoding: the escape is one byte on the wire, so `ESCAPE 'é'` is an
/// error, and byte 0 encodes "escaping disabled", so `ESCAPE '<NUL>'` is one too.
fn like_escape(escape_char: Option<&ValueWithSpan>) -> Result<Option<u8>, GnitzSqlError> {
    let Some(v) = escape_char else { return Ok(Some(b'\\')) };
    let bad = || GnitzSqlError::Unsupported("LIKE: ESCAPE must be a single non-NUL ASCII character or ''".into());
    // A `ValueWithSpan`, not an `Expr`: the parser puts the escape in its own
    // slot, so there is no sign or parenthesis for `bind_constant` to peel.
    let Ok(BExpr::LitStr(s)) = bind_literal::<Infallible>(&v.value) else {
        return Err(bad());
    };
    match s.as_bytes() {
        [] => Ok(None),
        &[b] if b != 0 => Ok(Some(b)),
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
                // A trailing slot with a default may be omitted.
                (sig.iter().filter(|a| a.default().is_none()).count(), Some(sig.len()))
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

/// The SQL spelling of a string function, read back out of [`SCALAR_CALLS`] —
/// its first entry, for the names with aliases — so an error names the
/// function as the parser accepts it. The `AGG_NAMES` / `agg_func_name` shape.
pub(crate) fn str_func_name(f: StrFunc) -> &'static str {
    SCALAR_CALLS
        .iter()
        .find_map(|&(n, c)| matches!(c, Call::Str(g) if g == f).then_some(n))
        .expect("every StrFunc has a spelling in SCALAR_CALLS")
}

fn scalar_call(f: &Function) -> Option<(&'static str, Call)> {
    let n = single_fn_name(f)?;
    SCALAR_CALLS
        .iter()
        .find(|(name, _)| n.eq_ignore_ascii_case(name))
        .copied()
}

/// The one arity-error shape for every structurally-bound call, matching what
/// `classify_agg_call` already reports for the aggregates: `min..=max`
/// arguments, unbounded above when `max` is `None`.
fn wrong_arity(name: &str, min: usize, max: Option<usize>) -> GnitzSqlError {
    const WORDS: [&str; 4] = ["zero", "one", "two", "three"];
    // Spelled out up to three, then digits — total over every arity, so a wider
    // signature cannot panic here.
    let word = |n: usize| WORDS.get(n).map_or_else(|| n.to_string(), |w| (*w).to_string());
    let plural = |n: usize| if n == 1 { "argument" } else { "arguments" };
    let want = match max {
        Some(max) if max == min => format!("exactly {} {}", word(min), plural(min)),
        Some(max) => format!("{} or {} arguments", word(min), word(max)),
        None => format!("at least {} {}", word(min), plural(min)),
    };
    GnitzSqlError::Unsupported(format!("{name}: requires {want}"))
}

/// Bind one of the [`SCALAR_CALLS`]. Arguments come through
/// `function_positional_args`, so every call inherits the shared qualifier
/// rejection (FILTER/DISTINCT/WITHIN GROUP/…) that COALESCE already applied; the arity
/// is checked once, from [`Call::arity`], so each arm below indexes a validated
/// slice.
fn bind_scalar_call<R: Clone, L: LeafBinder<R>>(
    name: &str,
    call: Call,
    f: &Function,
    leaf: &L,
) -> Result<BExpr<R>, GnitzSqlError> {
    let args = function_positional_args(f, name)?;
    let (min, max) = call.arity();
    if args.len() < min || max.is_some_and(|max| args.len() > max) {
        return Err(wrong_arity(name, min, max));
    }
    match call {
        // IFNULL/NVL is COALESCE at arity two; only `Call::arity` separates them.
        Call::Coalesce | Call::Ifnull => bind_coalesce(&args, leaf),
        // `NULLIF(a, b)` ≡ `CASE WHEN a = b THEN NULL ELSE a END`.
        Call::Nullif => {
            let a = bind_structural(args[0], leaf)?;
            let b = bind_structural(args[1], leaf)?;
            Ok(BExpr::Case {
                branches: vec![(
                    BExpr::BinOp(Box::new(a.clone()), BinOp::Eq, Box::new(b)),
                    BExpr::LitNull,
                )],
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
            Ok(BExpr::BinOp(Box::new(a), op, Box::new(b)))
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
                .ok_or_else(|| GnitzSqlError::Unsupported(format!("{name}: the unit must be a string literal")))?;
            // DATE_TRUNC takes the truncating half of the same vocabulary, so
            // the fields with no truncation (DOW, EPOCH, …) fall out as
            // unsupported units here rather than needing a second table.
            let op = calendar_field(&unit).and_then(|op| match call {
                Call::DateTrunc => op.trunc_of(),
                _ => Some(op),
            });
            Ok(BExpr::Calendar {
                op: op.ok_or_else(|| GnitzSqlError::Unsupported(format!("{name}: unit {unit:?} is not supported")))?,
                arg: Box::new(bind_structural(args[1], leaf)?),
            })
        }
    }
}

/// A string function call, its omitted trailing slots supplied as the literal
/// defaults [`StrFunc::signature`] declares, so the node always carries the
/// full signature.
fn bind_str_call<R: Clone, L: LeafBinder<R>>(f: StrFunc, args: &[&Expr], leaf: &L) -> Result<BExpr<R>, GnitzSqlError> {
    let sig = f.signature();
    let mut bound = bind_all(args, leaf)?;
    bound.extend(
        sig[args.len()..]
            .iter()
            .map(|a| BExpr::LitStr(a.default().expect("only a defaulted slot may be omitted").to_string())),
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
    // Every failure here reports the one `Plan` error, which names the range as
    // well as the shape; `LitInt` is what rejects a fractional scale.
    let bad = || GnitzSqlError::Plan("ROUND: scale must be an integer literal in -15..=15".to_string());
    let c = bind_constant(e).map_err(|_| bad())?;
    let BExpr::LitInt(mag) = c.lit else { return Err(bad()) };
    let n = if c.negated { -mag } else { mag };
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
        return Err(GnitzSqlError::Unsupported(format!(
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
        o => {
            return Err(GnitzSqlError::Unsupported(format!(
                "binary operator {o:?} not supported"
            )))
        }
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
        Nullness::Unknown => Ok(BExpr::Case {
            branches: vec![(null_test(value.clone(), /* want_null = */ false, leaf), value)],
            else_: Some(Box::new(bind_coalesce(rest, leaf)?)),
        }),
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
        v if v.never_null_with(&|r| leaf.is_nullable(r)) => Nullness::Never,
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
/// `a.val`; it disambiguates nothing beyond that.
///
/// The one home for the single-relation resolve contract — the "expected a column
/// reference" / "column not found" pair and `find_unique_column`'s ambiguity
/// error.
pub(crate) fn single_relation_col_idx<'a>(
    cols: impl IntoIterator<Item = &'a ColumnDef>,
    alias: &str,
    e: &Expr,
) -> Result<usize, GnitzSqlError> {
    let (qual, name) =
        col_ref_parts(e).ok_or_else(|| GnitzSqlError::Unsupported("expected a column reference".into()))?;
    if let Some(q) = qual {
        if !q.eq_ignore_ascii_case(alias) {
            return Err(GnitzSqlError::Bind(format!(
                "table alias '{q}' not found (the relation in scope is '{alias}')"
            )));
        }
    }
    find_unique_column(cols, name)?.ok_or_else(|| GnitzSqlError::Bind(format!("column '{name}' not found")))
}

/// Leaf for a single-relation schema (WHERE, projections, set-ops, DML).
pub(crate) struct SingleTable<'a> {
    pub schema: &'a Schema,
    /// The relation's effective alias — what a written qualifier must name.
    pub alias: &'a str,
}

impl SingleTable<'_> {
    fn idx(&self, e: &Expr) -> Result<usize, GnitzSqlError> {
        single_relation_col_idx(&self.schema.columns, self.alias, e)
    }
}

impl LeafBinder<usize> for SingleTable<'_> {
    fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError> {
        Ok(BoundExpr::ColRef(self.idx(e)?))
    }
    fn is_nullable(&self, r: &usize) -> bool {
        self.schema.columns[*r].is_nullable
    }
}

#[cfg(test)]
#[path = "tests/structural.rs"]
mod tests;
