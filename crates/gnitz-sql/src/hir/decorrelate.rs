//! Joins in the subqueries a body reads, where the body's projection is built.
//! Bind reads each subquery through its column ([`SubqueryRef::id`]); its join
//! produces that column.

use super::guards::reject_nullable_in;
use super::{as_col, split_filter, ColId, HirExpr, JoinType, ProjEntry, RelExpr, SubqueryKind, SubqueryRef};
use crate::error::GnitzSqlError;
use crate::ir::BExpr;
use std::rc::Rc;

/// `Project(items)` over `source`, with every subquery in `subs` that `source`'s
/// filter or `items` read joined in below it.
pub(crate) fn decorrelate(
    source: Rc<RelExpr>,
    items: Vec<ProjEntry>,
    subs: &[SubqueryRef],
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    if subs.is_empty() {
        return Ok(RelExpr::project(source, items));
    }
    let (preds, input) = split_filter(&source);
    let mut joins = Joins {
        subs,
        cur: Rc::clone(input),
        joined: Vec::new(),
    };
    let mut kept = Vec::new();
    for pred in preds {
        if !joins.consume(pred)? {
            kept.push(pred.clone());
        }
    }
    let mut counts = Vec::new();
    for s in joins.unjoined_reads(kept.iter().chain(items.iter().map(|it| &it.expr))) {
        if joins.join_read(s)? {
            counts.push(s.id);
        }
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

/// The relation the subqueries are joined onto, and which of them are.
struct Joins<'s> {
    subs: &'s [SubqueryRef],
    cur: Rc<RelExpr>,
    joined: Vec<ColId>,
}

impl<'s> Joins<'s> {
    fn unjoined(&self, id: ColId) -> Option<&'s SubqueryRef> {
        self.subs.iter().find(|s| s.id == id && !self.joined.contains(&id))
    }

    /// Join `s` in as `kind` on its correlation.
    fn join(&mut self, s: &SubqueryRef, kind: JoinType) -> Result<(), GnitzSqlError> {
        reject_nullable_in(kind, s)?;
        self.cur = RelExpr::join(Rc::clone(&self.cur), Rc::clone(&s.rel), kind, s.correlation.clone())?;
        self.joined.push(s.id);
        Ok(())
    }

    /// Join in the EXISTS/IN a bare WHERE conjunct reads as a Semi/Anti join;
    /// whether it did.
    fn consume(&mut self, pred: &HirExpr) -> Result<bool, GnitzSqlError> {
        let (bare, negated) = peel_not(pred);
        if let Some(s) = as_col(bare)
            .and_then(|id| self.unjoined(id))
            .filter(|s| matches!(s.kind, SubqueryKind::Exists { .. }))
        {
            let kind = if negated { JoinType::Anti } else { JoinType::Semi };
            self.join(s, kind)?;
            return Ok(true);
        }
        // An uncorrelated scalar joins where its conjunct stands; the conjunct
        // stays a filter, which placement keys when it can.
        for s in self.unjoined_reads(std::iter::once(pred)) {
            if matches!(s.kind, SubqueryKind::Scalar { .. }) && s.correlation.is_empty() {
                self.join(s, JoinType::Inner)?;
            }
        }
        Ok(false)
    }

    /// The unjoined subqueries `exprs` read, in the order they read them.
    fn unjoined_reads<'e>(&self, exprs: impl Iterator<Item = &'e HirExpr>) -> Vec<&'s SubqueryRef> {
        let mut read: Vec<&'s SubqueryRef> = Vec::new();
        for e in exprs {
            e.for_each_ref(&mut |id| {
                if let Some(s) = self.unjoined(*id).filter(|s| !read.iter().any(|r| r.id == s.id)) {
                    read.push(s);
                }
            });
        }
        read
    }

    /// Join in a subquery a WHERE conjunct did not consume: EXISTS/IN as a Mark
    /// join, a scalar as INNER when uncorrelated — its reduce always publishes one
    /// row — else LEFT. Whether it is a COUNT joined as LEFT.
    fn join_read(&mut self, s: &SubqueryRef) -> Result<bool, GnitzSqlError> {
        match s.kind {
            SubqueryKind::Exists { .. } => {
                self.join(s, JoinType::Mark(s.id))?;
                Ok(false)
            }
            SubqueryKind::Scalar { count, .. } => {
                let kind = match s.correlation.is_empty() {
                    true => JoinType::Inner,
                    false => JoinType::Left,
                };
                self.join(s, kind)?;
                Ok(count && kind == JoinType::Left)
            }
        }
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
    let Ok(out) = e.try_rebuild::<ColId, std::convert::Infallible>(&mut |id| {
        let col = BExpr::ColRef(*id);
        Ok(match counts.contains(id) {
            true => BExpr::coalesce(col, BExpr::LitInt(0)),
            false => col,
        })
    });
    out
}
