use super::resolve::find_unique_column;
use crate::ast_util::{classify_agg_call, function_positional_args, single_fn_name, single_relation_col_name};
use crate::codec::pk_codec::{extract_sql_literal, SqlLiteral};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BinOp, BoundExpr, NumFunc, StrFunc, TrimMode, UnaryOp};
use crate::types::{is_cast_target, sql_type_to_typecode};
use gnitz_core::{ColumnDef, Schema};
use sqlparser::ast::{
    BinaryOperator, CaseWhen, CeilFloorKind, DateTimeField, Expr, Function, TrimWhereField, UnaryOperator, Value,
    ValueWithSpan,
};

/// Bind an expression against a single-relation schema (WHERE, projections,
/// set-op branches, DML). The structural recursion lives in `bind_structural`;
/// the `SingleTable` leaf supplies the three schema-aware decisions (column
/// lookup, aggregate calls, `IS [NOT] NULL`). Context is fully determined by
/// `(expr, schema)` — no binder state is involved.
pub(crate) fn bind_single_table(expr: &Expr, schema: &Schema) -> Result<BoundExpr, GnitzSqlError> {
    bind_structural(expr, &SingleTable { schema })
}

// ---------------------------------------------------------------------------
// Shared structural expression binding
// ---------------------------------------------------------------------------
//
// One `Expr → BoundExpr` recursion, parametrized by a leaf. The only things that
// differ across GnitzDB's binding contexts (WHERE/projection, HAVING, JOIN
// residual) are the schema-aware positions — column reference, function call,
// `IS [NOT] NULL`, and a whole expression the context names. Everything
// structural (literals, the full operator map, unary ops, the BETWEEN desugar,
// Nested unwrap) lives here once, so a new operator or fold lands on every
// context at the same time and cannot silently drift between hand-copied walks.

/// The schema-aware leaves of `bind_structural`, generic over the leaf reference
/// type `R` (the `ColRef`/`IsNull`/`IsNotNull` payload). `R` defaults to `usize`,
/// which is what the two ad-hoc read-path leaves resolve to unannotated; the four
/// view-path leaves in `hir::bind` instantiate the column-identity `HirRef`.
pub(crate) trait LeafBinder<R = usize> {
    /// A whole expression this leaf's context names, claimed before the recursion
    /// descends into it — a GROUP BY key over a grouped relation. `None` recurses
    /// as usual, so declining cannot mask the recursion's own error.
    fn bind_node(&self, _e: &Expr) -> Option<BExpr<R>> {
        None
    }
    /// An `Identifier` / `CompoundIdentifier` column reference.
    fn bind_column(&self, e: &Expr) -> Result<BExpr<R>, GnitzSqlError>;
    /// A function call (aggregates, or a context-specific rejection).
    fn bind_function(&self, f: &Function) -> Result<BExpr<R>, GnitzSqlError>;
    /// `inner IS [NOT] NULL` (`want_null` picks IS NULL vs IS NOT NULL).
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BExpr<R>, GnitzSqlError>;
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
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => leaf.bind_column(expr),
        // The context-independent names (the CASE desugars and the numeric scalar
        // functions) bind here, above the leaf, so all three leaf impls stay
        // untouched. Every other name falls through to the leaf — aggregates, or a
        // context rejection.
        Expr::Function(f) => match scalar_call(f) {
            Some((name, call)) => bind_scalar_call(name, call, f, leaf),
            None => leaf.bind_function(f),
        },
        // CEIL/FLOOR are keyword-dispatched by sqlparser into their own AST nodes
        // and never arrive as `Expr::Function` (CEILING, which is not, does).
        Expr::Ceil { expr: e, field } => bind_ceil_floor(NumFunc::Ceil, "CEIL", e, field, leaf),
        Expr::Floor { expr: e, field } => bind_ceil_floor(NumFunc::Floor, "FLOOR", e, field, leaf),
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
        // Peel here, not in each leaf: every other operand reaches the leaves
        // through the `Nested` arm below, so `(c) IS NULL` would otherwise bind
        // only under the leaves that happened to peel for themselves.
        Expr::IsNull(i) => leaf.bind_null_test(crate::ast_util::peel_nested(i), true),
        Expr::IsNotNull(i) => leaf.bind_null_test(crate::ast_util::peel_nested(i), false),
        Expr::Nested(i) => bind_structural(i, leaf),
        Expr::Value(vws) => bind_literal(&vws.value),
        // Searched CASE binds each `(condition, result)`; the simple-operand form
        // desugars each WHEN value `w` to `operand = w` (operand re-bound per
        // branch, like the BETWEEN desugar). A missing ELSE is the implicit
        // ELSE NULL (`else_ = None`, lowered to `load_null`).
        Expr::Case {
            operand,
            conditions,
            else_result,
            case_token: _,
            end_token: _,
        } => {
            let mut branches = Vec::with_capacity(conditions.len());
            for CaseWhen { condition, result } in conditions {
                let cond = match operand {
                    Some(op) => BExpr::BinOp(
                        Box::new(bind_structural(op, leaf)?),
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
            let uop = match op {
                UnaryOperator::Minus => UnaryOp::Neg,
                UnaryOperator::Not => UnaryOp::Not,
                o => {
                    return Err(GnitzSqlError::Unsupported(format!(
                        "unary operator {o:?} not supported"
                    )))
                }
            };
            Ok(BExpr::UnaryOp(uop, Box::new(inner)))
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
            let ge = BExpr::BinOp(
                Box::new(bind_structural(e, leaf)?),
                BinOp::Ge,
                Box::new(bind_structural(low, leaf)?),
            );
            let le = BExpr::BinOp(
                Box::new(bind_structural(e, leaf)?),
                BinOp::Le,
                Box::new(bind_structural(high, leaf)?),
            );
            Ok(maybe_negate(
                BExpr::BinOp(Box::new(ge), BinOp::And, Box::new(le)),
                *negated,
            ))
        }
        // `e IN (l)` IS `e = l` — the same structural desugar as BETWEEN above, and
        // what makes the equality visible to the `access` recognizers, which all gate
        // on `BinOp(_, Eq, _)`. Two or more items keep the faithful `InList` node, so
        // lowering can pick a ≤8-byte-integer operand with all-integer-literal items
        // to one `INT_IN_SET` and everything else to the `e = l1 OR … OR e = ln`
        // chain. `NOT IN` wraps whichever node the arity picked. Item binding is
        // eager, so a NULL/string/non-literal item errors in its written position.
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
        _ => Err(GnitzSqlError::Unsupported(format!(
            "expression type not supported: {expr:?}"
        ))),
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

/// A string literal's value, read through the one literal decoder so every
/// compile-time-data position (TRIM's byte set, LIKE's pattern and escape)
/// accepts exactly the spellings every other string-literal position does.
/// `None` for anything else — a NULL or numeric literal falls out as a
/// non-`LitStr`.
fn literal_string(v: &Value) -> Option<String> {
    match bind_literal::<()>(v) {
        Ok(BExpr::LitStr(s)) => Some(s),
        _ => None,
    }
}

/// [`literal_string`] of an expression in a compile-time-data position.
/// Parentheses are peeled first, so `TRIM(s, ('ab'))` reads like `TRIM(s, 'ab')`
/// — the operand positions get that from `bind_structural`'s `Nested` arm.
fn literal_expr_string(e: &Expr) -> Option<String> {
    match crate::ast_util::peel_nested(e) {
        Expr::Value(v) => literal_string(&v.value),
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
        return Err(GnitzSqlError::Bind(
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
    match literal_string(&v.value).ok_or_else(bad)?.as_bytes() {
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
    Unary(NumFunc),
    Round,
    Mod,
    /// GREATEST (`true`) / LEAST (`false`).
    MinMax(bool),
    /// A unary string transform or measure over its single argument.
    Str(StrFunc),
    /// `LTRIM`/`RTRIM`: one argument, or two with a literal trim set.
    Trim1(TrimMode),
    Concat,
}

/// The function names `bind_structural` binds above the leaf, keyed by their SQL
/// spelling (which also names the call in its arity errors). Matched
/// case-insensitively. The one name→call map, like `AGG_NAMES` is for the
/// aggregates: a name added here reaches every binding context at once.
const SCALAR_CALLS: &[(&str, Call)] = &[
    ("COALESCE", Call::Coalesce),
    ("NULLIF", Call::Nullif),
    ("ABS", Call::Unary(NumFunc::Abs)),
    ("CEILING", Call::Unary(NumFunc::Ceil)),
    ("TRUNC", Call::Unary(NumFunc::Trunc)),
    ("ROUND", Call::Round),
    ("MOD", Call::Mod),
    ("GREATEST", Call::MinMax(true)),
    ("LEAST", Call::MinMax(false)),
    ("UPPER", Call::Str(StrFunc::Upper)),
    ("LOWER", Call::Str(StrFunc::Lower)),
    ("LENGTH", Call::Str(StrFunc::LenChars)),
    ("CHAR_LENGTH", Call::Str(StrFunc::LenChars)),
    ("CHARACTER_LENGTH", Call::Str(StrFunc::LenChars)),
    ("OCTET_LENGTH", Call::Str(StrFunc::LenBytes)),
    ("LTRIM", Call::Trim1(TrimMode::Leading)),
    ("RTRIM", Call::Trim1(TrimMode::Trailing)),
    ("CONCAT", Call::Concat),
];

fn scalar_call(f: &Function) -> Option<(&'static str, Call)> {
    let n = single_fn_name(f)?;
    SCALAR_CALLS
        .iter()
        .find(|(name, _)| n.eq_ignore_ascii_case(name))
        .copied()
}

/// The one arity-error shape for every structurally-bound call, matching what
/// `classify_agg_call` already reports for the aggregates.
fn wrong_arity(name: &str, want: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!("{name}: requires {want}"))
}

/// Bind one of the [`SCALAR_CALLS`]. Arguments come through
/// `function_positional_args`, so every call inherits the shared qualifier
/// rejection (FILTER/OVER/DISTINCT/…) that COALESCE already applied.
fn bind_scalar_call<R: Clone, L: LeafBinder<R>>(
    name: &str,
    call: Call,
    f: &Function,
    leaf: &L,
) -> Result<BExpr<R>, GnitzSqlError> {
    let args = function_positional_args(f, name)?;
    match call {
        Call::Coalesce => bind_coalesce(&args, leaf),
        Call::Nullif => bind_nullif(&args, leaf),
        Call::Unary(nf) => Ok(BExpr::Func {
            f: nf,
            arg: Box::new(bind_one(name, &args, leaf)?),
        }),
        Call::Round => bind_round(&args, leaf),
        // `MOD` is the `%` operator: the `IntMod` opcode is total, substituting a
        // safe divisor and masking the row NULL when the divisor is zero.
        Call::Mod => {
            let [a, b] = args.as_slice() else {
                return Err(wrong_arity(name, "exactly two arguments"));
            };
            Ok(BExpr::BinOp(
                Box::new(bind_structural(a, leaf)?),
                BinOp::Mod,
                Box::new(bind_structural(b, leaf)?),
            ))
        }
        // NULL skipping is the MAX2/MIN2 opcode's, so no argument needs a
        // null-test rewrite and a computed argument is as good as a column.
        Call::MinMax(is_max) => Ok(BExpr::MinMaxN {
            is_max,
            args: bind_all(name, &args, leaf)?,
        }),
        Call::Str(sf) => Ok(BExpr::StrCall {
            f: sf,
            arg: Box::new(bind_one(name, &args, leaf)?),
        }),
        Call::Trim1(mode) => {
            let (s, set) = match args.as_slice() {
                [s] => (*s, None),
                [s, set] => (*s, Some(*set)),
                _ => return Err(wrong_arity(name, "one or two arguments")),
            };
            Ok(BExpr::TrimCall {
                s: Box::new(bind_structural(s, leaf)?),
                mode,
                set: trim_set(set)?,
            })
        }
        Call::Concat => Ok(BExpr::ConcatN {
            args: bind_all(name, &args, leaf)?,
        }),
    }
}

/// The single argument of a one-argument function, bound.
fn bind_one<R: Clone, L: LeafBinder<R>>(name: &str, args: &[&Expr], leaf: &L) -> Result<BExpr<R>, GnitzSqlError> {
    let [arg] = args else {
        return Err(wrong_arity(name, "exactly one argument"));
    };
    bind_structural(arg, leaf)
}

/// Every argument of a variadic function, bound; at least one is required.
fn bind_all<R: Clone, L: LeafBinder<R>>(name: &str, args: &[&Expr], leaf: &L) -> Result<Vec<BExpr<R>>, GnitzSqlError> {
    if args.is_empty() {
        return Err(wrong_arity(name, "at least one argument"));
    }
    args.iter().map(|a| bind_structural(a, leaf)).collect()
}

/// `ROUND(x)` / `ROUND(x, n)`. The scale rides the IR node rather than being
/// desugared here: which form `ROUND(x, n)` takes depends on the argument's
/// type, and the binder is schema-free.
fn bind_round<R: Clone, L: LeafBinder<R>>(args: &[&Expr], leaf: &L) -> Result<BExpr<R>, GnitzSqlError> {
    let (arg, n) = match args {
        [x] => (*x, 0),
        [x, n] => (*x, round_scale(n)?),
        _ => return Err(wrong_arity("ROUND", "one or two arguments")),
    };
    Ok(BExpr::Func {
        f: NumFunc::Round(n),
        arg: Box::new(bind_structural(arg, leaf)?),
    })
}

/// The `n` of `ROUND(x, n)`: an integer literal, optionally signed, in
/// `-15..=15`. f64 carries ~15–17 significant decimal digits, so a wider scale
/// has no digits left to round at.
fn round_scale(e: &Expr) -> Result<i8, GnitzSqlError> {
    let bad = || GnitzSqlError::Bind("ROUND: scale must be an integer literal in -15..=15".to_string());
    let Some(SqlLiteral::Number(mag, neg)) = extract_sql_literal(e) else {
        return Err(bad());
    };
    // Parsing as an integer is what rejects a fractional scale.
    let mag: i64 = mag.parse().map_err(|_| bad())?;
    let n = if neg { -mag } else { mag };
    i8::try_from(n).ok().filter(|n| (-15..=15).contains(n)).ok_or_else(bad)
}

/// `CEIL(x)` / `FLOOR(x)`. Only the bare-call parse binds: `CEIL(x TO DAY)` and
/// `CEIL(x, 2)` carry a field this plan's opcode has no place for, so they are
/// rejected rather than having the field dropped.
fn bind_ceil_floor<R: Clone, L: LeafBinder<R>>(
    f: NumFunc,
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
        f,
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

/// SQL literal → `BExpr`. Leaf-free (no `ColRef` produced), so it is generic
/// over `R` without a `Clone` bound.
fn bind_literal<R>(v: &Value) -> Result<BExpr<R>, GnitzSqlError> {
    match v {
        // NULL is an ordinary value here: it lowers to `LOAD_NULL`, types as
        // `unify_blend_type`'s neutral element, and every consumer of the bound IR
        // already has a `LitNull` arm. Only `bind_null_test`, which must resolve
        // a column, still rejects it.
        Value::Null => Ok(BExpr::LitNull),
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(BExpr::LitInt(i))
            } else if n.contains(['.', 'e', 'E']) {
                n.parse::<f64>()
                    .map(BExpr::LitFloat)
                    .map_err(|_| GnitzSqlError::Bind(format!("invalid number literal: {n}")))
            } else {
                // Non-fractional literal that overflows i64 (U128/UUID/I128 range,
                // or the `i64::MIN` magnitude under an outer `Neg`). Bind it
                // *faithfully* as a `LitWide` carrying the raw magnitude string —
                // schemaless binding cannot choose `i128`-vs-`u128` parsing, so the
                // access-path recognizer (which holds the column `TypeCode`) parses
                // it byte-exactly into a seek bound. Representing it as f64 would run
                // an integer-column comparison through a lossy 52-bit mantissa (e.g.
                // u64::MAX matches the wrong rows); the un-servable case rejects
                // honestly at the compile boundary (`OpcodeBackend::lower`), not here.
                Ok(BExpr::LitWide(n.clone()))
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(BExpr::LitStr(s.clone())),
        _ => Err(GnitzSqlError::Unsupported(format!(
            "value type not supported in expressions: {v:?}"
        ))),
    }
}

/// `COALESCE(args…)` desugars right-to-left into CASE: the first non-NULL operand
/// is the result. Total over arity via explicit base cases; a literal operand is
/// folded at the AST level so it never reaches `bind_null_test` (which resolves a
/// *column* and would reject a literal with "expected a column reference").
fn bind_coalesce<R: Clone, L: LeafBinder<R>>(args: &[&Expr], leaf: &L) -> Result<BExpr<R>, GnitzSqlError> {
    let [a, rest @ ..] = args else {
        // COALESCE() ≡ NULL (parser forbids; stay total) — also the tail after an
        // all-NULL-literals argument list.
        return Ok(BExpr::LitNull);
    };
    if let Expr::Value(v) = a {
        // A NULL literal contributes nothing; any other literal is non-null → the result.
        return match &v.value {
            Value::Null => bind_coalesce(rest, leaf),
            _ => bind_literal(&v.value),
        };
    }
    if rest.is_empty() {
        return bind_structural(a, leaf); // last operand: its value is the result
    }
    let cond = leaf.bind_null_test(a, /* want_null = */ false)?; // IsNotNull(col) | LitInt(1)
    if matches!(cond, BExpr::LitInt(1)) {
        return bind_structural(a, leaf); // provably non-null (NOT NULL column) → always taken
    }
    Ok(BExpr::Case {
        branches: vec![(cond, bind_structural(a, leaf)?)],
        else_: Some(Box::new(bind_coalesce(rest, leaf)?)),
    })
}

/// `NULLIF(a, b)` ≡ `CASE WHEN a = b THEN NULL ELSE a END`. Both operands bind
/// through `bind_structural` (not `bind_null_test`), so NULLIF has no
/// literal-operand pitfall.
fn bind_nullif<R: Clone, L: LeafBinder<R>>(args: &[&Expr], leaf: &L) -> Result<BExpr<R>, GnitzSqlError> {
    let [a, b] = args else {
        return Err(wrong_arity("NULLIF", "exactly two arguments"));
    };
    let a_bound = bind_structural(a, leaf)?;
    let cond = BExpr::BinOp(
        Box::new(a_bound.clone()),
        BinOp::Eq,
        Box::new(bind_structural(b, leaf)?),
    );
    Ok(BExpr::Case {
        branches: vec![(cond, BExpr::LitNull)],
        else_: Some(Box::new(a_bound)),
    })
}

/// Shared NOT-NULL fold: a never-null value makes `IS [NOT] NULL` a constant
/// (never null, never — for IS NULL — true), so the answer is settled at bind
/// time and the opcode never reaches the program. `nullable` is the caller's
/// authoritative nullability fact for the value referenced by `r` — a schema
/// column's `is_nullable`, or a HAVING aggregate's structural nullability.
/// Generic over the leaf reference `R`: the `usize` runtime leaves reuse it via
/// inference (`BExpr<usize>` = `BoundExpr`), and the HIR view leaf calls it with
/// a `HirRef` so both derive the fold from the one function.
pub(crate) fn fold_null_test<R>(nullable: bool, r: R, want_null: bool) -> BExpr<R> {
    if !nullable {
        BExpr::LitInt(i64::from(!want_null))
    } else if want_null {
        BExpr::IsNull(r)
    } else {
        BExpr::IsNotNull(r)
    }
}

/// The position of an `Identifier` / two-part `CompoundIdentifier` within one
/// relation's columns. The qualifier on a compound ref is informational in a
/// single-relation context (it carries no disambiguating information — a
/// duplicated name is ambiguous even when qualified).
///
/// The one home for the single-relation resolve contract — the "expected a column
/// reference" / "column not found" pair and the ambiguity error `find_unique_column`
/// raises — shared by the runtime `SingleTable` leaf (columns by slice) and the HIR
/// `HirSingleTable` leaf (columns behind `HirCol`), which resolve to a `usize` and a
/// `ColId` respectively off the same index.
pub(crate) fn single_relation_col_idx<'a>(
    cols: impl IntoIterator<Item = &'a ColumnDef>,
    e: &Expr,
) -> Result<usize, GnitzSqlError> {
    let name =
        single_relation_col_name(e).ok_or_else(|| GnitzSqlError::Unsupported("expected a column reference".into()))?;
    find_unique_column(cols, name)?.ok_or_else(|| GnitzSqlError::Bind(format!("column '{name}' not found")))
}

/// Leaf for a single-relation schema (WHERE, projections, set-ops, DML).
pub(crate) struct SingleTable<'a> {
    pub schema: &'a Schema,
}

impl SingleTable<'_> {
    fn idx(&self, e: &Expr) -> Result<usize, GnitzSqlError> {
        single_relation_col_idx(&self.schema.columns, e)
    }
}

impl LeafBinder for SingleTable<'_> {
    fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError> {
        Ok(BoundExpr::ColRef(self.idx(e)?))
    }
    fn bind_function(&self, func: &Function) -> Result<BoundExpr, GnitzSqlError> {
        // Shape dispatch (COUNT(*) vs COUNT(x), arity) is leaf-independent — one
        // home in `classify_agg_call`; this leaf only binds the argument. Its
        // type is checked where the aggregate is typed (`agg_typing`).
        let (agg_func, arg) = classify_agg_call(func)?;
        let bound = arg.map(|e| bind_structural(e, self)).transpose()?;
        Ok(BoundExpr::AggCall {
            func: agg_func,
            arg: bound.map(Box::new),
        })
    }
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BoundExpr, GnitzSqlError> {
        let idx = self.idx(inner)?;
        Ok(fold_null_test(self.schema.columns[idx].is_nullable, idx, want_null))
    }
}

#[cfg(test)]
#[path = "tests/structural.rs"]
mod tests;
