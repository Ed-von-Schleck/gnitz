//! Predicate placement at construction: [`RelExpr::join`] and [`RelExpr::filter`]
//! place each conjunct as low as it is exact — in an input, in the join's keys, or
//! in a `Filter` above the join.

use super::{cross_comparison, side, EqPair, HirCol, HirExpr, HirRange, JoinClass, JoinShape, JoinType, RelExpr, Side};
use crate::error::GnitzSqlError;
use crate::ir::BinOp;
use crate::rules::{reject_arity, reject_float_keys};
use gnitz_wire::{ColumnDef, JoinKeyRule, TypeCode};
use std::rc::Rc;

/// Where a conjunct was written: in the join's own ON, or in a filter over it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Origin {
    On,
    Filter,
}

impl RelExpr {
    /// A join over `on`, each conjunct placed; rejects a key the join cannot build.
    pub(crate) fn join(
        left: Rc<RelExpr>,
        right: Rc<RelExpr>,
        kind: JoinType,
        on: Vec<HirExpr>,
    ) -> Result<Rc<RelExpr>, GnitzSqlError> {
        let on = on.into_iter().flat_map(HirExpr::conjuncts).collect();
        let placed = place(&left, &right, kind, JoinClass::default(), on, Origin::On)?;
        reject_arity(
            "join key list",
            placed.class.eq.len() + usize::from(placed.class.range.is_some()),
            gnitz_wire::PK_LIST_MAX_COLS,
        )?;
        reject_join_shape(kind, placed.class.shape())?;
        reject_outer_with_residual(kind, &placed.above)?;
        placed.build(&left, &right, kind)
    }

    /// `input` filtered by `preds`, each conjunct placed as low as it is exact.
    pub(crate) fn filter(input: Rc<RelExpr>, preds: Vec<HirExpr>) -> Result<Rc<RelExpr>, GnitzSqlError> {
        if preds.is_empty() {
            return Ok(input);
        }
        let preds = preds.into_iter().flat_map(HirExpr::conjuncts).collect();
        match input.as_ref() {
            RelExpr::Filter { input: below, preds: first } => {
                RelExpr::filter(Rc::clone(below), first.iter().cloned().chain(preds).collect())
            }
            RelExpr::Join { left, right, kind, on } => {
                place(left, right, *kind, on.clone(), preds, Origin::Filter)?.build(left, right, *kind)
            }
            _ => Ok(Rc::new(RelExpr::Filter { input, preds })),
        }
    }
}

/// A join's conjuncts once placed: its keys, what each input evaluates, and what
/// stays above it.
struct Placed {
    class: JoinClass,
    below: [Vec<HirExpr>; 2],
    above: Vec<HirExpr>,
}

impl Placed {
    fn build(self, left: &Rc<RelExpr>, right: &Rc<RelExpr>, kind: JoinType) -> Result<Rc<RelExpr>, GnitzSqlError> {
        let [l, r] = self.below;
        let join = Rc::new(RelExpr::Join {
            left: RelExpr::filter(Rc::clone(left), l)?,
            right: RelExpr::filter(Rc::clone(right), r)?,
            kind,
            on: self.class,
        });
        Ok(match self.above.is_empty() {
            true => join,
            false => Rc::new(RelExpr::Filter { input: join, preds: self.above }),
        })
    }
}

/// Whether a conjunct naming only one input's columns stays exact inside it.
fn admits(kind: JoinType, origin: Origin, is_left: bool) -> bool {
    match origin {
        // Exact unless this input's unmatched rows are emitted or counted.
        Origin::On => !kind.has_nu(is_left),
        // Exact unless the join null-fills this input's columns.
        Origin::Filter => !kind.preserves(!is_left),
    }
}

/// Place `conjs` over the join of `left` and `right`, keying into `class`. Fails
/// only on an ON comparison whose column pair cannot be a key.
fn place(
    left: &Rc<RelExpr>,
    right: &Rc<RelExpr>,
    kind: JoinType,
    mut class: JoinClass,
    conjs: Vec<HirExpr>,
    origin: Origin,
) -> Result<Placed, GnitzSqlError> {
    let (lcols, rcols) = (left.cols(), right.cols());
    let (mut below, mut above) = ([Vec::new(), Vec::new()], Vec::new());
    for conj in conjs {
        let input = match side(&conj, &lcols, &rcols) {
            Side::Left => admits(kind, origin, true).then_some(0),
            Side::Right => admits(kind, origin, false).then_some(1),
            Side::Neither => (0..2).find(|&i| admits(kind, origin, i == 0)),
            Side::Both => None,
        };
        if let Some(i) = input {
            below[i].push(conj);
            continue;
        }
        // `ON p WHERE q ≡ ON (p AND q)` for an INNER join only.
        if origin == Origin::Filter && kind != JoinType::Inner {
            above.push(conj);
            continue;
        }
        above.extend(class.key(conj, &lcols, &rcols, origin)?);
    }
    Ok(Placed { class, below, above })
}

impl JoinClass {
    /// Key on `conj`, or hand it back to stay a filter. An ON comparison whose
    /// column pair cannot be a key is an error; a filter one stays a filter.
    fn key(
        &mut self,
        conj: HirExpr,
        left: &[HirCol],
        right: &[HirCol],
        origin: Origin,
    ) -> Result<Option<HirExpr>, GnitzSqlError> {
        let Some((l, r, op)) = cross_comparison(&conj, left, right) else {
            return Ok(Some(conj));
        };
        let range = op.as_range_rel();
        // A pair already keyed on, in either orientation, adds nothing.
        if op == BinOp::Eq && self.eq.iter().any(|p| (p.left, p.right) == (l.id, r.id)) {
            return Ok(None);
        }
        let slot = if range.is_some() {
            self.range.is_none()
        } else {
            op == BinOp::Eq
        };
        // Past the arity cap an ON comparison still claims its slot, so the cap's
        // error reports the count written; a filter comparison stays a filter.
        let fits = origin == Origin::On
            || self.eq.len() + 1 + usize::from(self.range.is_some()) <= gnitz_wire::PK_LIST_MAX_COLS;
        if !(slot && fits) {
            return Ok(Some(conj));
        }
        let tc = match range {
            None => validate_join_key_pair(&l.def, &r.def),
            Some(_) => validate_range_join_key_pair(&l.def, &r.def),
        };
        match (tc, range) {
            (Ok(tc), None) => self.eq.push(EqPair { left: l.id, right: r.id, tc }),
            (Ok(tc), Some(op)) => self.range = Some(HirRange { left: l.id, right: r.id, op, tc }),
            (Err(e), _) if origin == Origin::On => return Err(e),
            (Err(_), _) => return Ok(Some(conj)),
        }
        Ok(None)
    }
}

/// `residual`, what a join's ON left unkeyed, applies only as a filter over an
/// INNER product: `JoinClass` has nowhere to hold one, so `place()` drops it into
/// `Placed.above`, where nothing tells it from a WHERE.
fn reject_outer_with_residual(kind: JoinType, residual: &[HirExpr]) -> Result<(), GnitzSqlError> {
    if residual.is_empty() {
        return Ok(());
    }
    match kind {
        JoinType::Inner => Ok(()),
        JoinType::Left | JoinType::Right | JoinType::Full => Err(GnitzSqlError::Rejected(
            "LEFT/RIGHT/FULL JOIN with a residual ON predicate (a non-equi/non-range \
             conjunct, or a second range conjunct) is not supported; the residual \
             would have to participate in the outer null-fill. Use INNER JOIN, or \
             move the predicate to a WHERE over a wrapping view."
                .into(),
        )),
        JoinType::Semi | JoinType::Anti | JoinType::Mark(_) => Err(GnitzSqlError::Rejected(
            "EXISTS/IN correlation contains a conjunct the semi-join cannot consume \
             (a non-equality/non-range comparison, a second range conjunct, or an OR \
             group spanning both relations); it would have to participate in the \
             match-existence decision. Filter inside the subquery or a wrapping view."
                .into(),
        )),
    }
}

/// Which (shape, kind) combinations a join step exists in. Both refusals are
/// ordinary SQL with a buildable lowering; neither emitter was written.
fn reject_join_shape(kind: JoinType, shape: JoinShape) -> Result<(), GnitzSqlError> {
    match (shape, kind) {
        (JoinShape::Cross, k) if k != JoinType::Inner => Err(GnitzSqlError::Rejected(
            "a LEFT/RIGHT/FULL JOIN or an EXISTS/IN correlation needs at least one equijoin \
             or range predicate between its two sides; only an INNER step (CROSS JOIN, a \
             comma-separated FROM, or JOIN … ON with no cross-table comparison) may be keyless."
                .into(),
        )),
        (JoinShape::PureRange, k) if k.preserves_right() => Err(GnitzSqlError::Rejected(
            "pure-range RIGHT/FULL JOIN (a sole inequality range conjunct with no \
             equality prefix) is not supported; its mirror null-fill has no inner-join \
             witness on the preserved side. Use INNER/LEFT JOIN, or add an equality \
             conjunct to make it a band join."
                .into(),
        )),
        _ => Ok(()),
    }
}

/// Validate one equijoin key pair and return the key type both sides pack at.
fn validate_join_key_pair(left: &ColumnDef, right: &ColumnDef) -> Result<TypeCode, GnitzSqlError> {
    let t = left
        .ty
        .tc
        .join_key_common_type(right.ty.tc)
        .map_err(|rule| match rule {
            JoinKeyRule::Float => {
                reject_float_keys([left, right], "JOIN ON").expect_err("a Float pair holds a float column")
            }
            JoinKeyRule::UnitMismatch => GnitzSqlError::Rejected(format!(
                "JOIN ON: join key columns '{}' ({}) and '{}' ({}) differ in unit (days vs microseconds)",
                left.name, left.ty, right.name, right.ty
            )),
            JoinKeyRule::StringWithNative => GnitzSqlError::Rejected(format!(
                "JOIN ON: cannot equijoin string/blob column '{}' ({}) with non-string \
             column '{}' ({}); a string content hash never matches a native key",
                left.name, left.ty, right.name, right.ty
            )),
            JoinKeyRule::BoolWithNumber => GnitzSqlError::Rejected(format!(
                "JOIN ON: cannot equijoin BOOLEAN column with a non-BOOLEAN one: '{}' ({}) and '{}' ({}); \
             CAST one side",
                left.name, left.ty, right.name, right.ty
            )),
            JoinKeyRule::NoSigned256 => GnitzSqlError::Rejected(format!(
                "JOIN ON: join key columns '{}' ({}) and '{}' ({}) cannot co-partition; \
             a cross-sign pair whose unsigned side is 128-bit (e.g. UINT128/UUID \
             joined with a signed integer) needs a signed-256 type that does not exist",
                left.name, left.ty, right.name, right.ty
            )),
        })?;
    if !left.ty.decimal_domains_match(right.ty) {
        return Err(GnitzSqlError::Rejected(format!(
            "JOIN ON: join key columns '{}' ({}) and '{}' ({}) differ; a DECIMAL joins only a \
             DECIMAL of the same scale",
            left.name, left.ty, right.name, right.ty
        )));
    }
    Ok(t)
}

/// Validate the range conjunct's key pair and return its common reindex output
/// type `T`. A range bound must be order-preserving: STRING/BLOB reindex to a
/// 16-byte content hash that is equality-correct but NOT order-preserving, so
/// they are rejected here (they remain legal in the equality prefix).
fn validate_range_join_key_pair(left: &ColumnDef, right: &ColumnDef) -> Result<TypeCode, GnitzSqlError> {
    for col in [left, right] {
        if col.ty.tc.is_german_string() {
            return Err(GnitzSqlError::Rejected(format!(
                "range join key column '{}' ({:?}): a string/blob content hash is not \
                 order-preserving and cannot bound a range conjunct",
                col.name, col.ty.tc
            )));
        }
    }
    validate_join_key_pair(left, right)
}

#[cfg(test)]
#[path = "tests/place.rs"]
mod tests;
