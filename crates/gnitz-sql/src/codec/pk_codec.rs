//! Literal parsing + PK/seek key packing — the partition-routing source of
//! truth.
//!
//! `parse_pk_literal_packed` is the single helper through which every INSERT and
//! SEEK PK literal flows (`extract_pk_value_mapped`, plus `try_col_eq_literal` /
//! `try_extract_pk_in` in the WHERE planner), so the master cannot route an
//! INSERT and a DELETE for the same key to different workers.
//!
//! Two literal surfaces feed those packers, both defined here: the AST
//! ([`SqlLiteral`]) that INSERT VALUES parses, and the bound IR ([`BoundLit`])
//! that the WHERE recognizers and the UPDATE SET classifier read.

use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BoundExpr, UnaryOp};
use gnitz_core::{FixedInt, PkTuple, Schema, TypeCode};
use sqlparser::ast::{Expr, UnaryOperator, Value};

pub(crate) fn parse_uuid_str(s: &str) -> Result<u128, GnitzSqlError> {
    gnitz_wire::parse_uuid(s).ok_or_else(|| GnitzSqlError::Bind(format!("invalid UUID literal: {s:?}")))
}

/// Parse a numeric SQL literal as `i128`, applying `negated` (the literal sat
/// under `Expr::UnaryOp(Minus, _)`). The magnitude parses as `u128` so a type
/// minimum's own digit string is accepted — up to `2^127`, whose `as i128` is
/// `i128::MIN` and whose `wrapping_neg` is itself; parse-as-`i128`-then-negate
/// would reject it. `i128` covers every ≤8-byte type with room to spare, so an
/// out-of-type-range literal is *representable* — callers classify it against
/// `FixedInt::range` and decline or saturate, never wrap.
pub(crate) fn parse_literal_i128(n_str: &str, negated: bool) -> Option<i128> {
    let m = n_str.parse::<u128>().ok()?;
    if negated {
        (m <= i128::MAX as u128 + 1).then(|| (m as i128).wrapping_neg())
    } else {
        (m <= i128::MAX as u128).then_some(m as i128)
    }
}

/// Parse a numeric SQL literal into its packed-u128 PK form: the low
/// `wire_stride` bytes carry the column's native LE encoding
/// (`FixedInt::pack` — e.g. `-1_i8` → `0xFF`, not `0xFFFF_FFFF_FFFF_FFFF`),
/// the rest stay zero. An out-of-type-range literal declines (`None`) instead
/// of wrapping: without the `FixedInt::range` check, `x = 3000000000` on an
/// I32 column would cast to `-1294967296` and seek the wrong value (silent
/// wrong rows — the cff7c58-class trap).
///
/// This helper is the single source of truth for INSERT/SEEK PK routing —
/// `extract_pk_value_mapped`, `try_col_eq_literal`, and `try_extract_pk_in` all
/// dispatch through it so the master cannot send INSERT and DELETE for the
/// same key to different workers.
pub(crate) fn parse_pk_literal_packed(tc: TypeCode, n_str: &str, negated: bool) -> Option<u128> {
    match tc {
        // U128/UUID take the full unsigned range — beyond i128, so they parse as
        // u128 directly rather than through the i128 value path.
        TypeCode::U128 | TypeCode::UUID => {
            if negated {
                return None;
            }
            n_str.parse::<u128>().ok()
        }
        _ => pack_pk_value(tc, parse_literal_i128(n_str, negated)?),
    }
}

/// Pack an already-parsed literal value into its packed-u128 PK form — the
/// value-level half of [`parse_pk_literal_packed`], for callers holding a native
/// integer (a `LitInt`) that would otherwise stringify only to re-parse. Same
/// contract: an out-of-type-range value declines (`None`) instead of wrapping.
/// (A U128/UUID value above `i128::MAX` cannot arrive here — such literals only
/// exist as digit strings and take the u128 parse above.)
pub(crate) fn pack_pk_value(tc: TypeCode, v: i128) -> Option<u128> {
    match tc {
        TypeCode::U128 | TypeCode::UUID => (v >= 0).then_some(v as u128),
        // I128 (the internal join-key promotion type) is two's complement at 16
        // bytes, exactly `as u128`.
        TypeCode::I128 => Some(v as u128),
        _ => {
            let fi = FixedInt::from_type_code(tc)?;
            let (min, max) = fi.range();
            (min <= v && v <= max).then(|| fi.pack(v))
        }
    }
}

// ---------------------------------------------------------------------------
// The bound-literal seam
// ---------------------------------------------------------------------------

/// A bound numeric literal, sign applied. `LitInt` carries its own sign; a
/// `LitWide` magnitude rides under an outer `Neg` when negative.
#[derive(Clone, Copy)]
pub(crate) enum NumLit<'e> {
    /// A native literal (any i64, sign applied — `-(i64::MIN)` fits i128).
    Small(i128),
    /// A wide magnitude digit string + sign. Kept as the raw string because the
    /// `i128`-vs-`u128` parse is the consumer's call: it holds the column
    /// `TypeCode`, and a `LitWide` in the `(i128::MAX, u128::MAX]` band
    /// (`U128`/`UUID`) needs the u128 parse a signed value could not represent.
    Wide(&'e str, bool),
}

impl NumLit<'_> {
    /// The literal as a signed value, for a consumer that classifies against a
    /// ≤8-byte type's range. `None` for a magnitude past `i128` — only a
    /// `U128`/`UUID` literal, which takes [`pack_num`]'s unsigned path instead.
    pub(crate) fn to_i128(self) -> Option<i128> {
        match self {
            NumLit::Small(v) => Some(v),
            NumLit::Wide(s, negated) => parse_literal_i128(s, negated),
        }
    }
}

/// The one seam from a bound numeric literal to a [`NumLit`].
pub(crate) fn bound_num_literal(e: &BoundExpr) -> Option<NumLit<'_>> {
    match e {
        BExpr::LitInt(v) => Some(NumLit::Small(*v as i128)),
        BExpr::LitWide(s) => Some(NumLit::Wide(s, false)),
        BExpr::UnaryOp(UnaryOp::Neg, inner) => match inner.as_ref() {
            BExpr::LitWide(s) => Some(NumLit::Wide(s, true)),
            _ => None,
        },
        _ => None,
    }
}

/// Pack a numeric literal as a seek/range key for column type `tc`, byte-exactly
/// (an out-of-type-range literal declines, never wraps).
pub(crate) fn pack_num(tc: TypeCode, lit: NumLit<'_>) -> Option<u128> {
    match lit {
        NumLit::Small(v) => pack_pk_value(tc, v),
        NumLit::Wide(s, negated) => parse_pk_literal_packed(tc, s, negated),
    }
}

/// A bound literal accepted for a seek/range key: a numeric value + sign, or a
/// string (a single-quoted UUID). The bound-IR analogue of [`SqlLiteral`].
pub(crate) enum BoundLit<'e> {
    Num(NumLit<'e>),
    Str(&'e str),
}

pub(crate) fn bound_literal(e: &BoundExpr) -> Option<BoundLit<'_>> {
    if let Some(n) = bound_num_literal(e) {
        return Some(BoundLit::Num(n));
    }
    if let BExpr::LitStr(s) = e {
        return Some(BoundLit::Str(s));
    }
    None
}

/// A bound literal as the packed key of a column of type `tc`: a single-quoted
/// string is a key only for a UUID column, numerics pack through the same
/// `pk_codec` path INSERT takes. The one rule behind `col = literal` and
/// `col IN (literal, …)`, so those two spellings cannot route differently.
pub(crate) fn bound_key_literal(lit: BoundLit<'_>, tc: TypeCode) -> Option<u128> {
    match lit {
        BoundLit::Str(s) if tc == TypeCode::UUID => parse_uuid_str(s).ok(),
        BoundLit::Str(_) => None,
        BoundLit::Num(n) => pack_num(tc, n),
    }
}

/// A SQL literal extracted from an `Expr` for PK/seek routing. `Number`'s
/// second field is `negated` (the literal sat under `UnaryOp(Minus, _)`);
/// `Str` carries the unescaped single-quoted contents.
pub(crate) enum SqlLiteral<'a> {
    Number(&'a str, bool),
    Str(&'a str),
}

/// The `Expr::Value` / `UnaryOp(Minus, Number)` unwrap for the INSERT VALUES
/// parse site ([`parse_one_pk_literal`]); the WHERE recognizers unwrap bound
/// literals instead (`access`'s bound-literal seam).
///
/// Matches `SingleQuotedString` only — NOT `DoubleQuotedString`: in
/// `GenericDialect` a double-quoted token is an identifier, so treating
/// `col = "x"` as a UUID seek literal would silently change which queries
/// take the index fast path.
pub(crate) fn extract_sql_literal(expr: &Expr) -> Option<SqlLiteral<'_>> {
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::Number(n, _) => Some(SqlLiteral::Number(n, false)),
            Value::SingleQuotedString(s) => Some(SqlLiteral::Str(s)),
            _ => None,
        },
        // `+5` and `-5`: sqlparser lexes the sign as a separate unary operator,
        // so a signed literal is never a bare `Value::Number`.
        Expr::UnaryOp {
            op: op @ (UnaryOperator::Minus | UnaryOperator::Plus),
            expr,
        } => match expr.as_ref() {
            Expr::Value(vws) => match &vws.value {
                Value::Number(n, _) => Some(SqlLiteral::Number(n, matches!(op, UnaryOperator::Minus))),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

/// Parse one PK column literal at `pk_expr` into its packed u128 form.
/// Routes through `parse_pk_literal_packed` (numerics) or `parse_uuid_str`
/// (UUID); the returned u128's low `wire_stride` bytes carry the column's
/// native LE bytes.
fn parse_one_pk_literal(pk_expr: &Expr, tc: TypeCode, col_name: &str) -> Result<u128, GnitzSqlError> {
    match extract_sql_literal(pk_expr) {
        Some(SqlLiteral::Number(n, negated)) => parse_pk_literal_packed(tc, n, negated).ok_or_else(|| {
            if negated
                && matches!(
                    tc,
                    TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64 | TypeCode::U128 | TypeCode::UUID
                )
            {
                GnitzSqlError::Bind(format!(
                    "PK column '{col_name}' of type {tc:?} does not accept negative literals"
                ))
            } else {
                let s_disp = if negated { format!("-{n}") } else { n.to_string() };
                GnitzSqlError::Bind(format!("PK column '{col_name}' value is not a valid {tc:?}: {s_disp}"))
            }
        }),
        // UUID accepts a single-quoted UUID string; non-UUID PKs are numeric only.
        Some(SqlLiteral::Str(s)) if tc == TypeCode::UUID => parse_uuid_str(s),
        _ => Err(GnitzSqlError::Bind(format!(
            "PK column '{col_name}' value must be a numeric literal"
        ))),
    }
}

/// Extract the primary key from a VALUES row as a `PkTuple`, walking the PK
/// columns in pk-list order, dispatching each through `parse_one_pk_literal`,
/// and copying the column's native LE bytes into the tuple buffer. Test-only
/// convenience over [`extract_pk_value_mapped`] for rows with the identity
/// slot map; the INSERT path builds a hidden/SERIAL-aware map.
#[cfg(test)]
pub(crate) fn extract_pk_value(row: &[Expr], schema: &Schema) -> Result<PkTuple, GnitzSqlError> {
    let slot_of: Vec<Option<usize>> = (0..row.len()).map(Some).collect();
    extract_pk_value_mapped(row, &slot_of, schema)
}

/// Extract the primary key from a VALUES row as a `PkTuple`. `slot_of` maps each
/// **physical** column index to its VALUES slot, with `None` for columns that
/// carry no user value (SERIAL, or a hidden/dropped column). A PK column is
/// never SERIAL-in-payload nor hidden, so every `pk_indices()` entry maps to
/// `Some` slot for a well-formed INSERT.
pub(crate) fn extract_pk_value_mapped(
    row: &[Expr],
    slot_of: &[Option<usize>],
    schema: &Schema,
) -> Result<PkTuple, GnitzSqlError> {
    // `PK_LIST_MAX_COLS < MAX_PK_COLUMNS`, so every validated PK fits without a
    // per-row allocation; `PkTuple::from_columns` owns the byte layout.
    let mut natives = [0u128; gnitz_wire::MAX_PK_COLUMNS];
    for (slot, &pi) in natives.iter_mut().zip(schema.pk_indices()) {
        let pk_expr = slot_of
            .get(pi)
            .copied()
            .flatten()
            .and_then(|s| row.get(s))
            .ok_or_else(|| {
                GnitzSqlError::Bind(format!(
                    "PK column '{}' missing from INSERT row",
                    schema.columns[pi].name
                ))
            })?;
        *slot = parse_one_pk_literal(pk_expr, schema.columns[pi].type_code, &schema.columns[pi].name)?;
    }
    Ok(PkTuple::from_columns(schema, natives))
}

pub(crate) fn is_null_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Value(vws) if matches!(vws.value, Value::Null))
}

#[cfg(test)]
#[path = "tests/pk_codec.rs"]
mod tests;
