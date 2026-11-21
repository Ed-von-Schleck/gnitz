//! Joins in the subqueries a body reads, where the body's projection is built.
//! Bind reads each subquery through its column ([`SubqueryRef::id`]); its join
//! produces that column.

use super::super::{ColId, ExistsKind, HirExpr, JoinType, ProjEntry, RelExpr, split_filter};
use super::subquery::{SubqueryKind, SubqueryRef};
use crate::error::GnitzSqlError;
use crate::ir::BExpr;
use std::rc::Rc;

/// `Project(items)` over `source`, with every subquery in `subs` that `source`'s
/// filter or `items` read joined in below it.
pub(super) fn decorrelate(
    source: Rc<RelExpr>,
    items: Vec<ProjEntry>,
    subs: &[SubqueryRef],
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    if subs.is_empty() {
        return Ok(RelExpr::project(source, items));
    }
    let (preds, input) = split_filter(&source);
    let mut joins = Joins {
        pending: subs.iter().collect(),
        cur: Rc::clone(input),
    };
    let mut kept = Vec::new();
    for pred in preds {
        // A bare WHERE conjunct reading an EXISTS/IN is a Semi/Anti join, and
        // leaves the filter.
        let (bare, negated) = peel_not(pred);
        if let Some(s) = bare
            .as_col()
            .and_then(|id| joins.find(id))
            .filter(|s| matches!(s.kind, SubqueryKind::Exists { .. }))
        {
            joins.join(
                s,
                JoinType::Exists(if negated { ExistsKind::Anti } else { ExistsKind::Semi }),
            )?;
            continue;
        }
        kept.push(pred.clone());
    }
    // Every other subquery read: EXISTS/IN as a Mark join, a scalar as INNER when
    // uncorrelated — its reduce always publishes one row — else LEFT. A kept
    // conjunct reading one stays a filter, which placement keys when it can.
    let mut counts = Vec::new();
    for s in joins.reads(kept.iter().chain(items.iter().map(|it| &it.expr))) {
        let kind = match s.kind {
            SubqueryKind::Exists { .. } => JoinType::Exists(ExistsKind::Mark(s.id)),
            SubqueryKind::Scalar { .. } if s.correlation.is_empty() => JoinType::Inner,
            SubqueryKind::Scalar { count, .. } => {
                if count {
                    counts.push(s.id);
                }
                JoinType::Left
            }
        };
        joins.join(s, kind)?;
    }
    let kept = kept.into_iter().map(|e| zero_absent_counts(e, &counts)).collect();
    let items = items
        .into_iter()
        .map(|it| ProjEntry {
            expr: zero_absent_counts(it.expr, &counts),
            out: it.out,
        })
        .collect();
    Ok(RelExpr::project(RelExpr::filter(joins.cur, kept)?, items))
}

/// The relation the subqueries are joined onto, and those not joined yet.
struct Joins<'s> {
    pending: Vec<&'s SubqueryRef>,
    cur: Rc<RelExpr>,
}

impl<'s> Joins<'s> {
    fn find(&self, id: ColId) -> Option<&'s SubqueryRef> {
        self.pending.iter().copied().find(|s| s.id == id)
    }

    /// Join `s` in as `kind` on its correlation.
    fn join(&mut self, s: &SubqueryRef, kind: JoinType) -> Result<(), GnitzSqlError> {
        reject_nullable_in(kind, s)?;
        self.cur = RelExpr::join(Rc::clone(&self.cur), Rc::clone(&s.rel), kind, s.correlation.clone())?;
        self.pending.retain(|p| p.id != s.id);
        Ok(())
    }

    /// The pending subqueries `exprs` read, in the order they read them.
    fn reads<'e>(&self, exprs: impl Iterator<Item = &'e HirExpr>) -> Vec<&'s SubqueryRef> {
        let mut read: Vec<&'s SubqueryRef> = Vec::new();
        for e in exprs {
            e.for_each_ref(&mut |id| {
                if let Some(s) = self.find(*id).filter(|s| !read.iter().any(|r| r.id == s.id)) {
                    read.push(s);
                }
            });
        }
        read
    }
}

/// `e` under every `NOT` wrapping it, and whether their count is odd.
fn peel_not(mut e: &HirExpr) -> (&HirExpr, bool) {
    let mut negated = false;
    while let BExpr::Not(inner) = e {
        e = inner;
        negated = !negated;
    }
    (e, negated)
}

/// `e` reading each of `counts` — a COUNT joined as LEFT, which null-fills an
/// outer row its reduce has no group for — as 0 where it is NULL.
fn zero_absent_counts(e: HirExpr, counts: &[ColId]) -> HirExpr {
    if counts.is_empty() {
        return e;
    }
    e.rebuild(&mut |id| {
        let col = BExpr::ColRef(*id);
        match counts.contains(id) {
            true => BExpr::coalesce(col, BExpr::LitInt(0)),
            false => col,
        }
    })
}

/// An IN over a nullable operand is three-valued; only a top-level conjunct — a
/// semi-join, where WHERE reads NULL as false — computes it exactly.
fn reject_nullable_in(kind: JoinType, s: &SubqueryRef) -> Result<(), GnitzSqlError> {
    if !matches!(s.kind, SubqueryKind::Exists { nullable: true }) {
        return Ok(());
    }
    match kind {
        JoinType::Exists(ExistsKind::Anti) => Err(GnitzSqlError::rejected(
            "NOT IN (SELECT …) requires the outer operand and the subquery column to be \
             NOT NULL (SQL's NULL semantics diverge from the anti-join otherwise); \
             use NOT EXISTS with an explicit equality instead",
        )),
        JoinType::Exists(ExistsKind::Mark(_)) => Err(GnitzSqlError::rejected(
            "IN (SELECT …) in a mark position (under OR/NOT, in CASE, or projected) requires \
             the outer operand and the subquery column to be NOT NULL — SQL's 3VL diverges \
             from the two-valued mark; use a top-level AND `x IN (SELECT …)` conjunct, or \
             NOT EXISTS with an explicit equality",
        )),
        _ => Ok(()),
    }
}
