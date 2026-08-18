use super::resolve::find_unique_column;
use crate::agg::reject_min_max_unorderable;
use crate::ast_util::{classify_agg_call, function_positional_args, single_fn_name, single_relation_col_name};
use crate::codec::pk_codec::{extract_sql_literal, SqlLiteral};
use crate::error::GnitzSqlError;
use crate::ir::{AggFunc, BExpr, BinOp, BoundExpr, NumFunc, StrFunc, TrimMode, UnaryOp};
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
// One `Expr → BoundExpr` recursion, parametrized by a three-method leaf. The
// only things that differ across GnitzDB's binding contexts (WHERE/projection,
// HAVING, JOIN residual) are the three schema-aware leaves — column reference,
// function call, and `IS [NOT] NULL`. Everything structural (literals, the full
// operator map, unary ops, the BETWEEN desugar, Nested unwrap) lives here once,
// so a new operator or fold lands on every context at the same time and cannot
// silently drift between hand-copied walks.

/// The schema-aware leaves of `bind_structural`, generic over the leaf reference
/// type `R` (the `ColRef`/`IsNull`/`IsNotNull` payload). `R` defaults to `usize`,
/// which is what the two ad-hoc read-path leaves resolve to unannotated; the four
/// view-path leaves in `hir::bind` instantiate the column-identity `HirRef`.
pub(crate) trait LeafBinder<R = usize> {
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
        // home in `classify_agg_call`; this leaf only binds the argument and
        // guards MIN/MAX orderability, once the argument's type is in hand.
        let (agg_func, arg) = classify_agg_call(func)?;
        let bound = arg.map(|e| bind_structural(e, self)).transpose()?;
        if matches!(agg_func, AggFunc::Min | AggFunc::Max) {
            if let Some(b) = &bound {
                reject_min_max_unorderable(agg_func, b.infer_type(&self.schema.columns))?;
            }
        }
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
mod tests {
    use super::*;
    use crate::test_support::parse_expr_sql;
    use gnitz_core::{ColumnDef, Schema, TypeCode};

    fn col(name: &str, tc: TypeCode) -> ColumnDef {
        ColumnDef::new(name, tc, false)
    }

    fn schema_with_val(val_tc: TypeCode) -> Schema {
        Schema {
            columns: vec![col("pk", TypeCode::U64), col("c", val_tc)],
            pk_cols: vec![0],
        }
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

    fn num(n: &str) -> Result<BoundExpr, GnitzSqlError> {
        bind_literal(&Value::Number(n.into(), false))
    }

    #[test]
    fn bind_literal_wide_int_to_litwide() {
        // u64::MAX overflows i64 and is non-fractional → bound faithfully as a
        // `LitWide` carrying the raw magnitude string (not coerced to f64, not an
        // error). The recognizer parses it byte-exactly; the un-servable case
        // rejects at the compile boundary.
        match num("18446744073709551615") {
            Ok(BoundExpr::LitWide(s)) => assert_eq!(s, "18446744073709551615"),
            other => panic!("expected LitWide, got {other:?}"),
        }
    }

    #[test]
    fn bind_literal_accepts_fractional_and_exponent_floats() {
        assert!(matches!(num("1.5"), Ok(BoundExpr::LitFloat(_))));
        assert!(matches!(num("1e3"), Ok(BoundExpr::LitFloat(_))));
    }

    #[test]
    fn bind_literal_accepts_in_range_integer() {
        assert!(matches!(num("42"), Ok(BoundExpr::LitInt(42))));
    }

    /// A null test on a PK column never survives binding: a PK column is always
    /// non-nullable, so [`fold_null_test`] const-folds it to a literal. That is
    /// what keeps `IS [NOT] NULL` safe on a PK for the compiled evaluator, whose
    /// `IsNull`/`IsNotNull` opcodes are payload-only and would reject a PK operand.
    #[test]
    fn null_test_on_pk_column_folds_to_a_literal() {
        let schema = schema_with_val(TypeCode::I64); // pk is NOT NULL
        assert!(matches!(
            bind_single_table(&parse_expr_sql("pk IS NULL"), &schema).unwrap(),
            BoundExpr::LitInt(0)
        ));
        assert!(matches!(
            bind_single_table(&parse_expr_sql("pk IS NOT NULL"), &schema).unwrap(),
            BoundExpr::LitInt(1)
        ));
    }

    #[test]
    fn test_bind_between_desugars_to_comparison_tree() {
        let schema = schema_with_val(TypeCode::I64); // (pk U64, c I64)
                                                     // `c BETWEEN 1 AND 9` ≡ `c >= 1 AND c <= 9` — a residual BETWEEN now binds
                                                     // (regression guard for the new Expr::Between arm) instead of Unsupported.
        match bind_single_table(&parse_expr_sql("c BETWEEN 1 AND 9"), &schema).unwrap() {
            BoundExpr::BinOp(l, BinOp::And, r) => {
                assert!(matches!(*l, BoundExpr::BinOp(_, BinOp::Ge, _)));
                assert!(matches!(*r, BoundExpr::BinOp(_, BinOp::Le, _)));
            }
            other => panic!("expected And(Ge, Le), got {other:?}"),
        }
        // `c NOT BETWEEN 1 AND 9` ≡ NOT(c >= 1 AND c <= 9).
        match bind_single_table(&parse_expr_sql("c NOT BETWEEN 1 AND 9"), &schema).unwrap() {
            BoundExpr::UnaryOp(UnaryOp::Not, inner) => {
                assert!(matches!(*inner, BoundExpr::BinOp(_, BinOp::And, _)))
            }
            other => panic!("expected Not(And(..)), got {other:?}"),
        }
    }

    #[test]
    fn test_binder_rejects_min_max_unsupported_types() {
        for &tc in &[
            TypeCode::U128,
            TypeCode::UUID,
            TypeCode::Blob,
            TypeCode::String,
            TypeCode::I128,
        ] {
            let schema = schema_with_val(tc);
            for fname in &["MIN", "MAX"] {
                let expr = parse_expr_sql(&format!("{fname}(c)"));
                let r = bind_single_table(&expr, &schema);
                assert_unsupported(r, fname);
            }
        }
    }

    #[test]
    fn test_binder_accepts_min_max_orderable_types() {
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
                let expr = parse_expr_sql(&format!("{fname}(c)"));
                let r = bind_single_table(&expr, &schema);
                assert!(r.is_ok(), "expected {}({:?}) to bind, got {:?}", fname, tc, r.err());
            }
        }
    }

    /// `t.x IS [NOT] NULL` on a *qualified* (CompoundIdentifier) column binds
    /// like the unqualified form on every `bind_structural` surface — the unified
    /// core reaches the shared null-test leaf, which a bare-`Identifier`-only arm
    /// would reject with "IS NULL on non-column expression".
    #[test]
    fn test_compound_identifier_null_test_binds() {
        // Non-nullable column: folds to the constant (0 for IS NULL, 1 for IS NOT NULL).
        let nn = schema_with_val(TypeCode::I64); // (pk U64 NOT NULL, c I64 NOT NULL)
        assert!(matches!(
            bind_single_table(&parse_expr_sql("t.c IS NULL"), &nn).unwrap(),
            BoundExpr::LitInt(0)
        ));
        assert!(matches!(
            bind_single_table(&parse_expr_sql("t.c IS NOT NULL"), &nn).unwrap(),
            BoundExpr::LitInt(1)
        ));
        // Nullable column: binds to the IsNull / IsNotNull opcode at the column index.
        let nullable = Schema {
            columns: vec![col("pk", TypeCode::U64), ColumnDef::new("c", TypeCode::I64, true)],
            pk_cols: vec![0],
        };
        assert!(matches!(
            bind_single_table(&parse_expr_sql("t.c IS NULL"), &nullable).unwrap(),
            BoundExpr::IsNull(1)
        ));
        assert!(matches!(
            bind_single_table(&parse_expr_sql("t.c IS NOT NULL"), &nullable).unwrap(),
            BoundExpr::IsNotNull(1)
        ));
    }

    /// Every aggregate qualifier the binder does not implement must be rejected,
    /// not silently dropped to the plain aggregate. Exercised through
    /// `bind_single_table` so the guard's wiring into `bind_function` is covered,
    /// not just the helper in isolation.
    #[test]
    fn test_binder_rejects_aggregate_qualifiers() {
        let schema = schema_with_val(TypeCode::I64); // (pk U64, c I64)
        for (src, want) in [
            ("COUNT(DISTINCT c)", "DISTINCT"),
            ("SUM(DISTINCT c)", "DISTINCT"),
            ("MIN(DISTINCT c)", "DISTINCT"), // no-op distinct, rejected for a uniform surface
            ("SUM(c) FILTER (WHERE c > 0)", "FILTER"),
            ("SUM(c) OVER (PARTITION BY pk)", "OVER"),
        ] {
            assert_unsupported(bind_single_table(&parse_expr_sql(src), &schema), want);
        }
    }

    /// `c IN (…)` with two or more items binds faithfully to an `InList` node
    /// (un-desugared); a one-item list folds to the `Eq` it is; `NOT IN` wraps the
    /// result in `Not`. The tested operand is bound once as `inner`; every list
    /// item is bound in order into `items`. Lowering — not binding — chooses the
    /// INT_IN_SET fast path vs the OR-chain fallback.
    #[test]
    fn test_bind_in_list_folds_one_item_else_binds_faithful() {
        let schema = schema_with_val(TypeCode::I64); // (pk U64, c I64)
        match bind_single_table(&parse_expr_sql("c IN (1, 2)"), &schema).unwrap() {
            BoundExpr::InList { inner, items } => {
                assert!(matches!(*inner, BoundExpr::ColRef(1)));
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0], BoundExpr::LitInt(1)));
                assert!(matches!(items[1], BoundExpr::LitInt(2)));
            }
            other => panic!("expected InList, got {other:?}"),
        }
        // A single element folds to the equality it spells — the shape the `access`
        // recognizers match on, identical to what `c = 7` binds to.
        match bind_single_table(&parse_expr_sql("c IN (7)"), &schema).unwrap() {
            BoundExpr::BinOp(inner, BinOp::Eq, item) => {
                assert!(matches!(*inner, BoundExpr::ColRef(1)));
                assert!(matches!(*item, BoundExpr::LitInt(7)));
            }
            other => panic!("expected Eq, got {other:?}"),
        }
        // NOT IN wraps whichever node the arity picked.
        for (src, folded) in [("c NOT IN (7)", true), ("c NOT IN (1, 2)", false)] {
            match bind_single_table(&parse_expr_sql(src), &schema).unwrap() {
                BoundExpr::UnaryOp(UnaryOp::Not, inner) => {
                    assert_eq!(matches!(*inner, BoundExpr::BinOp(_, BinOp::Eq, _)), folded, "{src}")
                }
                other => panic!("{src}: expected Not(_), got {other:?}"),
            }
        }
        // Negative-literal items bind as `UnaryOp(Neg, LitInt)` (sqlparser lexes
        // the minus separately) — the INT_IN_SET lowering unwraps them.
        match bind_single_table(&parse_expr_sql("c IN (-1, -2)"), &schema).unwrap() {
            BoundExpr::InList { items, .. } => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0], BoundExpr::UnaryOp(UnaryOp::Neg, _)));
            }
            other => panic!("expected InList, got {other:?}"),
        }
    }

    /// String and float list elements bind through the same literal leaves plain
    /// `=` uses; an empty list is rejected (constructed directly — the parser
    /// won't produce one).
    #[test]
    fn test_bind_in_list_string_float_and_empty() {
        let s = schema_with_val(TypeCode::String);
        assert!(bind_single_table(&parse_expr_sql("c IN ('a', 'b')"), &s).is_ok());
        let f = schema_with_val(TypeCode::F64);
        assert!(bind_single_table(&parse_expr_sql("c IN (1.5, 2.5)"), &f).is_ok());
        let empty = Expr::InList {
            expr: Box::new(parse_expr_sql("c")),
            list: vec![],
            negated: false,
        };
        assert_unsupported(bind_single_table(&empty, &f), "empty list");
    }

    /// Subquery expressions get the targeted per-kind message from the default
    /// `bind_subquery` leaf, not the generic catch-all. (The HIR view leaf
    /// overrides `bind_subquery` to record the node for decorrelation instead.)
    #[test]
    fn test_bind_rejects_subquery_expressions_with_targeted_messages() {
        let schema = schema_with_val(TypeCode::I64);
        for src in [
            "EXISTS (SELECT c FROM t)",
            "NOT EXISTS (SELECT c FROM t)",
            "c IN (SELECT c FROM t)",
            "c NOT IN (SELECT c FROM t)",
            "c = 1 OR EXISTS (SELECT c FROM t)",
        ] {
            assert_unsupported(
                bind_single_table(&parse_expr_sql(src), &schema),
                "only supported in a single-table CREATE VIEW",
            );
        }
        assert_unsupported(
            bind_single_table(&parse_expr_sql("c = ANY (SELECT c FROM t)"), &schema),
            "ANY/SOME/ALL",
        );
        assert_unsupported(
            bind_single_table(&parse_expr_sql("c > ALL (SELECT c FROM t)"), &schema),
            "ANY/SOME/ALL",
        );
        assert_unsupported(
            bind_single_table(&parse_expr_sql("(SELECT c FROM t) = 1"), &schema),
            "scalar subqueries",
        );
    }

    fn nullable_schema(val_tc: TypeCode) -> Schema {
        Schema {
            columns: vec![col("pk", TypeCode::U64), ColumnDef::new("c", val_tc, true)],
            pk_cols: vec![0],
        }
    }

    /// Searched `CASE WHEN … THEN … [ELSE …] END` binds each branch; a missing
    /// ELSE is `else_ = None` (implicit NULL).
    #[test]
    fn test_bind_searched_case() {
        let s = nullable_schema(TypeCode::I64);
        match bind_single_table(
            &parse_expr_sql("CASE WHEN c > 0 THEN 1 WHEN c < 0 THEN 2 ELSE 3 END"),
            &s,
        )
        .unwrap()
        {
            BoundExpr::Case { branches, else_ } => {
                assert_eq!(branches.len(), 2);
                assert!(matches!(branches[0].0, BoundExpr::BinOp(_, BinOp::Gt, _)));
                assert!(matches!(branches[0].1, BoundExpr::LitInt(1)));
                assert!(matches!(branches[1].0, BoundExpr::BinOp(_, BinOp::Lt, _)));
                assert!(matches!(else_.as_deref(), Some(BoundExpr::LitInt(3))));
            }
            other => panic!("expected Case, got {other:?}"),
        }
        // Missing ELSE → else_ = None.
        match bind_single_table(&parse_expr_sql("CASE WHEN c > 0 THEN 1 END"), &s).unwrap() {
            BoundExpr::Case { else_, .. } => assert!(else_.is_none(), "missing ELSE → None"),
            other => panic!("expected Case, got {other:?}"),
        }
    }

    /// Simple-operand `CASE c WHEN v THEN … END` desugars each WHEN to `c = v`.
    #[test]
    fn test_bind_simple_case_desugars_to_operand_eq() {
        let s = nullable_schema(TypeCode::I64);
        match bind_single_table(&parse_expr_sql("CASE c WHEN 1 THEN 10 WHEN 2 THEN 20 END"), &s).unwrap() {
            BoundExpr::Case { branches, else_ } => {
                assert_eq!(branches.len(), 2);
                assert!(matches!(branches[0].0, BoundExpr::BinOp(_, BinOp::Eq, _)));
                assert!(matches!(branches[1].0, BoundExpr::BinOp(_, BinOp::Eq, _)));
                assert!(else_.is_none());
            }
            other => panic!("expected Case, got {other:?}"),
        }
    }

    /// COALESCE base cases and shape.
    #[test]
    fn test_bind_coalesce_base_cases() {
        let s = nullable_schema(TypeCode::I64);
        // COALESCE(c) → c.
        assert!(matches!(
            bind_single_table(&parse_expr_sql("COALESCE(c)"), &s).unwrap(),
            BoundExpr::ColRef(1)
        ));
        // COALESCE(NULL, c) → c (a NULL literal contributes nothing).
        assert!(matches!(
            bind_single_table(&parse_expr_sql("COALESCE(NULL, c)"), &s).unwrap(),
            BoundExpr::ColRef(1)
        ));
        // COALESCE(c, 0) → Case{[(IsNotNull(c), c)], else: 0}.
        match bind_single_table(&parse_expr_sql("COALESCE(c, 0)"), &s).unwrap() {
            BoundExpr::Case { branches, else_ } => {
                assert_eq!(branches.len(), 1);
                assert!(matches!(branches[0].0, BoundExpr::IsNotNull(1)));
                assert!(matches!(branches[0].1, BoundExpr::ColRef(1)));
                assert!(matches!(else_.as_deref(), Some(BoundExpr::LitInt(0))));
            }
            other => panic!("expected Case, got {other:?}"),
        }
        // A NOT NULL column folds COALESCE(c, 0) to c directly (provably non-null).
        let nn = schema_with_val(TypeCode::I64);
        assert!(matches!(
            bind_single_table(&parse_expr_sql("COALESCE(c, 0)"), &nn).unwrap(),
            BoundExpr::ColRef(1)
        ));
    }

    /// A 3-arg COALESCE nests right-to-left.
    #[test]
    fn test_bind_coalesce_nested_three_arg() {
        let s = Schema {
            columns: vec![
                col("pk", TypeCode::U64),
                ColumnDef::new("a", TypeCode::I64, true),
                ColumnDef::new("b", TypeCode::I64, true),
            ],
            pk_cols: vec![0],
        };
        match bind_single_table(&parse_expr_sql("COALESCE(a, b, 0)"), &s).unwrap() {
            BoundExpr::Case { branches, else_ } => {
                assert!(matches!(branches[0].0, BoundExpr::IsNotNull(1)));
                match else_.as_deref() {
                    Some(BoundExpr::Case {
                        branches: inner,
                        else_: inner_else,
                    }) => {
                        assert!(matches!(inner[0].0, BoundExpr::IsNotNull(2)));
                        assert!(matches!(inner_else.as_deref(), Some(BoundExpr::LitInt(0))));
                    }
                    other => panic!("expected nested Case, got {other:?}"),
                }
            }
            other => panic!("expected Case, got {other:?}"),
        }
    }

    /// A computed COALESCE operand (`c + 1`) reaches `bind_null_test`, which
    /// resolves a *column*, so it errors "expected a column reference" rather than
    /// mis-binding.
    #[test]
    fn test_bind_coalesce_computed_operand_rejected() {
        let s = nullable_schema(TypeCode::I64);
        assert_unsupported(
            bind_single_table(&parse_expr_sql("COALESCE(c + 1, 0)"), &s),
            "expected a column reference",
        );
    }

    /// `NULLIF(a, b)` → `CASE WHEN a = b THEN NULL ELSE a END`; wrong arity errors.
    #[test]
    fn test_bind_nullif() {
        let s = nullable_schema(TypeCode::I64);
        match bind_single_table(&parse_expr_sql("NULLIF(c, 0)"), &s).unwrap() {
            BoundExpr::Case { branches, else_ } => {
                assert_eq!(branches.len(), 1);
                assert!(matches!(branches[0].0, BoundExpr::BinOp(_, BinOp::Eq, _)));
                assert!(matches!(branches[0].1, BoundExpr::LitNull));
                assert!(matches!(else_.as_deref(), Some(BoundExpr::ColRef(1))));
            }
            other => panic!("expected Case, got {other:?}"),
        }
        // Both operands bind through bind_structural, so a computed operand is fine.
        assert!(bind_single_table(&parse_expr_sql("NULLIF(c + 1, 0)"), &s).is_ok());
        // Wrong arity rejected.
        assert_unsupported(bind_single_table(&parse_expr_sql("NULLIF(c)"), &s), "exactly two");
    }

    /// The accepted qualifier (`ALL`, the SQL default — `COUNT(ALL c)` ≡
    /// `COUNT(c)`) and the plain forms must keep binding.
    #[test]
    fn test_binder_accepts_all_and_plain_aggregates() {
        let schema = schema_with_val(TypeCode::I64);
        for src in [
            "COUNT(*)",
            "COUNT(c)",
            "COUNT(ALL c)",
            "SUM(c)",
            "MIN(c)",
            "MAX(c)",
            "AVG(c)",
        ] {
            assert!(
                bind_single_table(&parse_expr_sql(src), &schema).is_ok(),
                "expected {src} to bind"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Numeric scalar functions and numeric CAST
    // -----------------------------------------------------------------------

    fn bind_num(src: &str) -> Result<BoundExpr, GnitzSqlError> {
        bind_single_table(&parse_expr_sql(src), &schema_with_val(TypeCode::I64))
    }

    fn assert_bind_err(r: Result<BoundExpr, GnitzSqlError>, want_substr: &str) {
        match r.unwrap_err() {
            GnitzSqlError::Bind(msg) => assert!(
                msg.contains(want_substr),
                "got Bind({msg:?}), expected to contain {want_substr:?}"
            ),
            e => panic!("expected Bind, got {e:?}"),
        }
    }

    /// Every unary numeric name binds to its `NumFunc`. `CEIL`/`FLOOR` arrive as
    /// their own AST nodes and `CEILING` as a plain call — all three must land on
    /// the same two IR nodes.
    #[test]
    fn unary_numeric_functions_bind_to_their_numfunc() {
        for (src, want) in [
            ("ABS(c)", NumFunc::Abs),
            ("abs(c)", NumFunc::Abs),
            ("CEIL(c)", NumFunc::Ceil),
            ("CEILING(c)", NumFunc::Ceil),
            ("FLOOR(c)", NumFunc::Floor),
            ("TRUNC(c)", NumFunc::Trunc),
            ("ROUND(c)", NumFunc::Round(0)),
        ] {
            match bind_num(src).unwrap() {
                BoundExpr::Func { f, arg } => {
                    assert_eq!(f, want, "{src}");
                    assert!(matches!(*arg, BoundExpr::ColRef(1)), "{src}");
                }
                other => panic!("{src}: expected Func, got {other:?}"),
            }
        }
        assert_unsupported(bind_num("ABS(c, 1)"), "exactly one argument");
        assert_unsupported(bind_num("TRUNC(c, 2)"), "exactly one argument");
    }

    /// `CEIL(x TO DAY)` and `CEIL(x, 2)` parse into the same node with a
    /// non-empty field; dropping it would silently compute plain `CEIL(x)`.
    #[test]
    fn ceil_floor_reject_the_field_carrying_forms() {
        assert_unsupported(bind_num("CEIL(c TO DAY)"), "CEIL");
        assert_unsupported(bind_num("FLOOR(c TO DAY)"), "FLOOR");
        assert_unsupported(bind_num("CEIL(c, 2)"), "CEIL");
    }

    #[test]
    fn round_scale_must_be_a_small_integer_literal() {
        for (src, want) in [("ROUND(c, 2)", 2i8), ("ROUND(c, -2)", -2), ("ROUND(c, +15)", 15)] {
            match bind_num(src).unwrap() {
                BoundExpr::Func {
                    f: NumFunc::Round(n), ..
                } => assert_eq!(n, want, "{src}"),
                other => panic!("{src}: expected Round, got {other:?}"),
            }
        }
        for src in ["ROUND(c, 16)", "ROUND(c, -16)", "ROUND(c, 2.5)", "ROUND(c, c)"] {
            assert_bind_err(bind_num(src), "scale must be an integer literal");
        }
        assert_unsupported(bind_num("ROUND(c, 1, 2)"), "one or two arguments");
    }

    /// MOD is a pure desugar onto `%`, so it inherits `IntMod`'s total semantics
    /// (zero divisor NULLs the row) with no opcode of its own.
    #[test]
    fn mod_desugars_to_the_modulo_binop() {
        match bind_num("MOD(c, 2)").unwrap() {
            BoundExpr::BinOp(l, BinOp::Mod, r) => {
                assert!(matches!(*l, BoundExpr::ColRef(1)));
                assert!(matches!(*r, BoundExpr::LitInt(2)));
            }
            other => panic!("expected BinOp(Mod), got {other:?}"),
        }
        assert_unsupported(bind_num("MOD(c)"), "exactly two arguments");
    }

    /// GREATEST/LEAST keep every argument as written — no literal or column
    /// restriction, and no null-test rewrite: NULL skipping is the opcode's.
    #[test]
    fn greatest_least_bind_n_ary_with_computed_args() {
        match bind_num("GREATEST(c, c + 1, -1, NULL)").unwrap() {
            BoundExpr::MinMaxN { is_max, args } => {
                assert!(is_max);
                assert_eq!(args.len(), 4);
                assert!(matches!(args[1], BoundExpr::BinOp(_, BinOp::Add, _)));
                assert!(matches!(args[2], BoundExpr::UnaryOp(UnaryOp::Neg, _)));
                assert!(matches!(args[3], BoundExpr::LitNull));
            }
            other => panic!("expected MinMaxN, got {other:?}"),
        }
        assert!(matches!(
            bind_num("LEAST(c)").unwrap(),
            BoundExpr::MinMaxN { is_max: false, .. }
        ));
    }

    /// The `NULL` literal is an ordinary value, not a GREATEST/LEAST special
    /// case: it binds wherever a literal does. `IS [NOT] NULL` still needs a
    /// column, so it keeps rejecting one.
    #[test]
    fn null_literal_binds_wherever_a_literal_does() {
        for src in [
            "NULL",
            "CASE WHEN c > 0 THEN 1 ELSE NULL END",
            "CASE WHEN c > 0 THEN NULL ELSE 1 END",
            "GREATEST(c, NULL)",
            "COALESCE(NULL, c)",
            "CAST(NULL AS BIGINT)",
            "ABS(NULL)",
        ] {
            assert!(bind_num(src).is_ok(), "expected {src} to bind");
        }
        assert!(matches!(bind_num("NULL").unwrap(), BoundExpr::LitNull));
        // COALESCE folds a leading NULL away rather than making it the result.
        assert!(matches!(bind_num("COALESCE(NULL, c)").unwrap(), BoundExpr::ColRef(1)));
        assert!(bind_num("NULL IS NULL").is_err());
    }

    /// All four cast kinds mean the same thing here — a failed cast is a NULL,
    /// which is what TRY_CAST/SAFE_CAST are documented to do.
    #[test]
    fn every_cast_kind_binds_to_one_node() {
        for src in [
            "CAST(c AS BIGINT)",
            "c::BIGINT",
            "TRY_CAST(c AS BIGINT)",
            "SAFE_CAST(c AS BIGINT)",
        ] {
            match bind_num(src).unwrap() {
                BoundExpr::Cast { expr, to } => {
                    assert_eq!(to, TypeCode::I64, "{src}");
                    assert!(matches!(*expr, BoundExpr::ColRef(1)), "{src}");
                }
                other => panic!("{src}: expected Cast, got {other:?}"),
            }
        }
    }

    #[test]
    fn cast_accepts_every_numeric_target_and_rejects_the_rest() {
        for (src, want) in [
            ("CAST(c AS TINYINT)", TypeCode::I8),
            ("CAST(c AS SMALLINT)", TypeCode::I16),
            ("CAST(c AS INT)", TypeCode::I32),
            ("CAST(c AS TINYINT UNSIGNED)", TypeCode::U8),
            ("CAST(c AS INT UNSIGNED)", TypeCode::U32),
            ("CAST(c AS BIGINT UNSIGNED)", TypeCode::U64),
            ("CAST(c AS FLOAT)", TypeCode::F32),
            ("CAST(c AS DOUBLE)", TypeCode::F64),
            ("CAST(c AS REAL)", TypeCode::F64),
        ] {
            match bind_num(src).unwrap() {
                BoundExpr::Cast { to, .. } => assert_eq!(to, want, "{src}"),
                other => panic!("{src}: expected Cast, got {other:?}"),
            }
        }
        // STRING is a cast target too — the VM has a string register class.
        for src in ["CAST(c AS TEXT)", "CAST(c AS VARCHAR(10))", "CAST(c AS CHAR(4))"] {
            match bind_num(src).unwrap() {
                BoundExpr::Cast { to, .. } => assert_eq!(to, TypeCode::String, "{src}"),
                other => panic!("{src}: expected Cast, got {other:?}"),
            }
        }
        // The 16-byte integer-ish targets have no register at all.
        for src in ["CAST(c AS UUID)", "CAST(c AS DECIMAL(38,0))"] {
            assert_unsupported(bind_num(src), "is not supported");
        }
        // BOOLEAN has no gnitz type at all, so it rejects one level earlier.
        assert!(bind_num("CAST(c AS BOOLEAN)").is_err());
        assert_unsupported(bind_num("CAST(c AS INT ARRAY)"), "ARRAY");
    }

    /// The scalar wrapper is consumed above the leaf, so its aggregate argument
    /// still resolves through the leaf on the recursion.
    #[test]
    fn scalar_functions_wrap_an_aggregate_argument() {
        let s = schema_with_val(TypeCode::I64);
        for src in ["ABS(SUM(c))", "CAST(SUM(c) AS INT)", "GREATEST(SUM(c), COUNT(*))"] {
            assert!(
                bind_single_table(&parse_expr_sql(src), &s).is_ok(),
                "expected {src} to bind"
            );
        }
        match bind_single_table(&parse_expr_sql("ABS(SUM(c))"), &s).unwrap() {
            BoundExpr::Func { f: NumFunc::Abs, arg } => {
                assert!(matches!(*arg, BoundExpr::AggCall { func: AggFunc::Sum, .. }))
            }
            other => panic!("expected Func(Abs, AggCall), got {other:?}"),
        }
    }

    /// The shared qualifier inventory applies to the new names too — a dropped
    /// `FILTER`/`OVER`/`DISTINCT` would compute the plain call.
    #[test]
    fn scalar_functions_reject_call_qualifiers() {
        assert_unsupported(bind_num("ABS(DISTINCT c)"), "DISTINCT");
        assert_unsupported(bind_num("ABS(c) OVER ()"), "OVER");
        assert_unsupported(bind_num("GREATEST(c) FILTER (WHERE c > 0)"), "FILTER");
    }

    /// Bind against a schema whose `c` is a STRING, for the string surface.
    fn bind_str(src: &str) -> Result<BoundExpr, GnitzSqlError> {
        bind_single_table(&parse_expr_sql(src), &schema_with_val(TypeCode::String))
    }

    /// The `(pattern, escape, ci)` a LIKE bound to, unwrapping a `NOT` if one is
    /// there.
    fn like_parts(src: &str) -> (String, Option<u8>, bool, bool) {
        let (e, negated) = match bind_str(src).unwrap() {
            BoundExpr::UnaryOp(UnaryOp::Not, inner) => (*inner, true),
            other => (other, false),
        };
        match e {
            BoundExpr::Like {
                pattern, escape, ci, ..
            } => (pattern, escape, ci, negated),
            other => panic!("expected Like, got {other:?}"),
        }
    }

    #[test]
    fn like_binds_its_pattern_escape_and_case_folding() {
        assert_eq!(like_parts("c LIKE 'a%'"), ("a%".to_string(), Some(b'\\'), false, false));
        assert_eq!(like_parts("c ILIKE 'a%'"), ("a%".to_string(), Some(b'\\'), true, false));
        assert_eq!(
            like_parts("c NOT LIKE 'a%'"),
            ("a%".to_string(), Some(b'\\'), false, true)
        );
        assert_eq!(
            like_parts("c NOT ILIKE 'a%'"),
            ("a%".to_string(), Some(b'\\'), true, true)
        );
        // `ESCAPE ''` disables escaping; any other single ASCII byte overrides.
        assert_eq!(
            like_parts(r"c LIKE 'a\%' ESCAPE ''"),
            (r"a\%".to_string(), None, false, false)
        );
        assert_eq!(
            like_parts("c LIKE 'a!%' ESCAPE '!'"),
            ("a!%".to_string(), Some(b'!'), false, false)
        );
        // Parentheses around the literal are peeled, as they are in an operand
        // position.
        assert_eq!(like_parts("c LIKE ('a%')").0, "a%");
    }

    #[test]
    fn like_rejects_what_it_cannot_bake_in() {
        assert_unsupported(bind_str("c LIKE c"), "LIKE pattern must be a string literal");
        assert_unsupported(bind_str("c LIKE NULL"), "LIKE pattern must be a string literal");
        assert_unsupported(bind_str("c LIKE 1"), "LIKE pattern must be a string literal");
        assert_unsupported(bind_str("c LIKE ANY ('a%')"), "LIKE ANY is not supported");
        // Two characters, non-ASCII, and NUL are all rejected escapes.
        for esc in ["'ab'", "'é'", "'\0'", "1"] {
            assert_unsupported(bind_str(&format!("c LIKE 'a' ESCAPE {esc}")), "ESCAPE must be a single");
        }
    }

    /// A pattern ending in a *live* escape is rejected; one whose trailing
    /// escape is itself escaped is legal, and only the tokenizer walk tells
    /// them apart.
    #[test]
    fn like_rejects_a_pattern_ending_in_a_live_escape() {
        match bind_str(r"c LIKE 'ab\'").unwrap_err() {
            GnitzSqlError::Bind(msg) => assert_eq!(msg, "LIKE pattern must not end with escape character"),
            e => panic!("expected Bind, got {e:?}"),
        }
        assert_eq!(like_parts(r"c LIKE 'ab\\'").0, r"ab\\");
        // With escaping disabled the byte is ordinary.
        assert_eq!(like_parts(r"c LIKE 'ab\' ESCAPE ''").0, r"ab\");
    }

    /// Every string function name reaches the same IR node, whichever of its
    /// spellings is written. `LENGTH` and its two SQL-standard aliases must land
    /// on the *character* measure and `OCTET_LENGTH` on the byte one — swapping
    /// them is invisible until a multibyte value shows up.
    #[test]
    fn string_function_names_bind_to_their_measure_and_transform() {
        for (src, want) in [
            ("UPPER(c)", StrFunc::Upper),
            ("lower(c)", StrFunc::Lower),
            ("LENGTH(c)", StrFunc::LenChars),
            ("CHAR_LENGTH(c)", StrFunc::LenChars),
            ("character_length(c)", StrFunc::LenChars),
            ("OCTET_LENGTH(c)", StrFunc::LenBytes),
        ] {
            match bind_str(src).unwrap() {
                BExpr::StrCall { f, .. } => assert_eq!(f, want, "{src}"),
                other => panic!("{src}: expected StrCall, got {other:?}"),
            }
        }
        for src in ["UPPER()", "UPPER(c, c)", "LENGTH()"] {
            assert_unsupported(bind_str(src), "exactly one argument");
        }
    }

    /// The full TRIM syntax matrix collapses to `(mode, set)`. The keyword form
    /// and the `LTRIM`/`RTRIM` calls must agree, since they lower identically.
    #[test]
    fn trim_syntax_matrix_collapses_to_a_mode_and_a_byte_set() {
        for (src, mode, set) in [
            ("TRIM(c)", TrimMode::Both, " "),
            ("TRIM(BOTH c)", TrimMode::Both, " "),
            ("TRIM(LEADING c)", TrimMode::Leading, " "),
            ("TRIM(TRAILING c)", TrimMode::Trailing, " "),
            ("TRIM(LEADING 'xy' FROM c)", TrimMode::Leading, "xy"),
            ("TRIM(TRAILING 'xy' FROM c)", TrimMode::Trailing, "xy"),
            ("TRIM('xy' FROM c)", TrimMode::Both, "xy"),
            ("LTRIM(c)", TrimMode::Leading, " "),
            ("RTRIM(c)", TrimMode::Trailing, " "),
            ("LTRIM(c, 'xy')", TrimMode::Leading, "xy"),
            ("RTRIM(c, 'xy')", TrimMode::Trailing, "xy"),
        ] {
            match bind_str(src).unwrap() {
                BExpr::TrimCall { mode: m, set: st, .. } => assert_eq!((m, st.as_str()), (mode, set), "{src}"),
                other => panic!("{src}: expected TrimCall, got {other:?}"),
            }
        }
    }

    /// The trim set is compile-time data the engine bakes into a membership
    /// table, so it must be a literal — and ASCII, which is what keeps a
    /// byte-wise strip from splitting a UTF-8 sequence.
    #[test]
    fn trim_set_must_be_an_ascii_literal() {
        for src in ["TRIM(c FROM c)", "LTRIM(c, c)", "TRIM('ä' FROM c)", "TRIM(NULL FROM c)"] {
            assert_unsupported(bind_str(src), "ASCII string literal");
        }
        // Parentheses around the literal are peeled, as they are in an operand
        // position.
        match bind_str("LTRIM(c, ('ab'))").unwrap() {
            BoundExpr::TrimCall { set, .. } => assert_eq!(set, "ab"),
            other => panic!("expected TrimCall, got {other:?}"),
        }
    }

    /// `SUBSTR` and `SUBSTRING`, the `FROM/FOR` form and the comma form, all
    /// arrive as one AST node; an absent FROM starts the window at 1.
    #[test]
    fn substring_spellings_bind_to_one_node() {
        for src in [
            "SUBSTRING(c FROM 2 FOR 3)",
            "SUBSTRING(c, 2, 3)",
            "SUBSTR(c, 2, 3)",
            "SUBSTR(c FROM 2 FOR 3)",
        ] {
            match bind_str(src).unwrap() {
                BExpr::Substr { start, len, .. } => {
                    assert!(matches!(*start, BExpr::LitInt(2)), "{src}");
                    assert!(matches!(len.as_deref(), Some(BExpr::LitInt(3))), "{src}");
                }
                other => panic!("{src}: expected Substr, got {other:?}"),
            }
        }
        match bind_str("SUBSTRING(c)").unwrap() {
            BExpr::Substr { start, len, .. } => {
                assert!(matches!(*start, BExpr::LitInt(1)), "an absent FROM starts at 1");
                assert!(len.is_none());
            }
            other => panic!("expected Substr, got {other:?}"),
        }
    }

    #[test]
    fn concat_binds_any_arity_and_the_operator_maps_to_its_own_binop() {
        match bind_str("CONCAT(c, 'x', 42)").unwrap() {
            BExpr::ConcatN { args } => assert_eq!(args.len(), 3),
            other => panic!("expected ConcatN, got {other:?}"),
        }
        assert!(matches!(bind_str("CONCAT(c)").unwrap(), BExpr::ConcatN { .. }));
        assert_unsupported(bind_str("CONCAT()"), "at least one argument");
        assert!(matches!(
            bind_str("c || 'x'").unwrap(),
            BExpr::BinOp(_, BinOp::Concat, _)
        ));
    }

    /// The walkers see through the two keyword-dispatched nodes. `EXCLUDED` is
    /// the observable: a reference the walk cannot reach is one the `EXCLUDED`
    /// guard and the aggregate collectors would silently miss.
    #[test]
    fn expr_operands_reaches_inside_substring_and_trim() {
        use crate::ast_util::expr_operands;
        for src in [
            "SUBSTRING(EXCLUDED.c FROM 1)",
            "SUBSTRING(c FROM EXCLUDED.n)",
            "SUBSTRING(c FROM 1 FOR EXCLUDED.n)",
            "TRIM(EXCLUDED.c)",
            "TRIM('x' FROM EXCLUDED.c)",
        ] {
            let e = parse_expr_sql(src);
            let found = expr_operands(&e).iter().any(|o| format!("{o}").contains("EXCLUDED"));
            assert!(found, "{src}: the walker must reach the EXCLUDED reference");
        }
    }
}
