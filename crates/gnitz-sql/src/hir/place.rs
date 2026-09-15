//! Predicate placement at construction: [`RelExpr::join`] and [`RelExpr::filter`]
//! place each conjunct as low as it is exact — in an input, in the join's keys, or
//! in a `Filter` above the join.

use super::guards::{
    reject_join_key_arity, reject_keyless_non_inner, reject_outer_with_residual, reject_pure_range,
    validate_join_key_pair, validate_range_join_key_pair,
};
use super::{cross_comparison, side, EqPair, HirCol, HirExpr, HirRange, JoinClass, JoinType, RelExpr, Side};
use crate::error::GnitzSqlError;
use crate::ir::BinOp;
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
        reject_join_key_arity(placed.class.eq.len(), placed.class.range.is_some())?;
        reject_keyless_non_inner(kind, placed.class.shape())?;
        reject_pure_range(kind, &placed.class)?;
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
            || reject_join_key_arity(
                self.eq.len() + usize::from(range.is_none()),
                self.range.is_some() || range.is_some(),
            )
            .is_ok();
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

#[cfg(test)]
#[path = "tests/place.rs"]
mod tests;
