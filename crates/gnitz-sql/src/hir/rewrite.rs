//! Predicate-classification rewrite (pass 3). Partitions every `Join.on` into a
//! `JoinClass` (equality pairs + an optional range conjunct + the residual) and
//! folds the WHERE `Filter` — INNER into the residual, OUTER left as a
//! post-null-fill filter. This is `PredicateCollector` re-hosted on `HirExpr`:
//! left-vs-right is a `ColId` membership test (bind mints distinct ids per
//! reference site, so even a self-join's two sides are disjoint), replacing the
//! AST version's combined-offset arithmetic.

use super::{col_by_id, ColId, EqPair, HirCol, HirExpr, HirRange, HirRef, JoinClass, RelExpr};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BinOp};
use crate::plan::view::join::JoinType;
use crate::plan::view::predicates::{converse_rel, validate_join_key_pair, validate_range_join_key_pair};
use gnitz_core::{ColumnDef, RangeRel};
use std::collections::HashMap;
use std::rc::Rc;

/// Classify every join's predicates and place the WHERE, recursing through the
/// whole tree so a Join buried under a `Reduce`/`Distinct`/`SetOp` (GROUP BY over
/// a join, set-op side that is a join, …) is classified too — else its
/// `classified` stays `None` and lowering panics. A join-free (linear) tree is
/// rebuilt unchanged.
///
/// Memoized by `Rc::as_ptr`, so a subtree referenced from two places stays **one**
/// shared node through the rewrite. That identity is what the lowering's
/// `Rc::as_ptr`-keyed cut memo reads to emit a shared subtree as a single hidden
/// segment; a naive rebuild would split it into two.
pub(crate) fn classify(rel: Rc<RelExpr>) -> Result<Rc<RelExpr>, GnitzSqlError> {
    classify_rel(rel, &mut RewriteMemo::new())
}

/// The per-pass `Rc` identity memo: original node pointer → rewritten node.
type RewriteMemo = HashMap<*const RelExpr, Rc<RelExpr>>;

/// Rebuild the spine: classify every buried `Join`, and fold each WHERE `Filter`
/// directly above a Join into its residual (INNER) or keep it as a post-null-fill
/// filter (OUTER) — matching `classify_join_where`. Every other node delegates its
/// reassembly to the generic `RelExpr::map_children`.
fn classify_rel(rel: Rc<RelExpr>, memo: &mut RewriteMemo) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let key = Rc::as_ptr(&rel);
    if let Some(done) = memo.get(&key) {
        return Ok(Rc::clone(done));
    }
    let out = match rel.as_ref() {
        RelExpr::Filter { input, preds } if matches!(input.as_ref(), RelExpr::Join { .. }) => {
            let mut join = classify_join(input, memo)?;
            if matches!(
                join.as_ref(),
                RelExpr::Join {
                    kind: JoinType::Inner,
                    ..
                }
            ) {
                // INNER: `ON p WHERE q ≡ ON (p AND q)` — fold WHERE into the residual.
                // `classify_join` returns a fresh Join, so mutate in place.
                let RelExpr::Join { classified, .. } =
                    Rc::get_mut(&mut join).expect("classify_join returns a fresh Join")
                else {
                    unreachable!()
                };
                classified
                    .as_mut()
                    .expect("classified before fold")
                    .residual
                    .extend(preds.iter().cloned());
                join
            } else {
                // OUTER: the WHERE is a 3VL filter over the post-null-fill output.
                RelExpr::filter(join, preds.clone())
            }
        }
        RelExpr::Join { .. } => classify_join(&rel, memo)?,
        _ => RelExpr::map_children(&rel, &mut |child| classify_rel(Rc::clone(child), memo))?,
    };
    memo.insert(key, Rc::clone(&out));
    Ok(out)
}

/// Classify one `Join` node (fill `classified` from its ON, recursing children).
fn classify_join(rel: &Rc<RelExpr>, memo: &mut RewriteMemo) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let RelExpr::Join {
        left, right, kind, on, ..
    } = rel.as_ref()
    else {
        unreachable!("classify_join receives a Join");
    };
    let new_left = classify_rel(Rc::clone(left), memo)?;
    let new_right = classify_rel(Rc::clone(right), memo)?;
    let class = classify_on(on, &left.cols(), &right.cols())?;
    // Outer + residual is unsupported: an outer preserved-side row's null-fill
    // decides match existence independently of the residual (matching emit_join).
    if *kind != JoinType::Inner && !class.residual.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "LEFT/RIGHT/FULL JOIN with a residual ON predicate (a non-equi/non-range \
             conjunct, or a second range conjunct) is not supported; the residual \
             would have to participate in the outer null-fill. Use INNER JOIN, or \
             move the predicate to a WHERE over a wrapping view."
                .into(),
        ));
    }
    Ok(Rc::new(RelExpr::Join {
        left: new_left,
        right: new_right,
        kind: *kind,
        on: Vec::new(), // consumed into `classified`
        classified: Some(class),
    }))
}

/// Partition the flattened `on` conjuncts into a `JoinClass`. `left_cols` /
/// `right_cols` define the left-vs-right membership.
fn classify_on(on: &[HirExpr], left_cols: &[HirCol], right_cols: &[HirCol]) -> Result<JoinClass, GnitzSqlError> {
    let mut eq: Vec<EqPair> = Vec::new();
    let mut range: Option<HirRange> = None;
    let mut residual: Vec<HirExpr> = Vec::new();

    for conj in on {
        match conj {
            BExpr::BinOp(l, op, r) => {
                let cross = match (as_col(l), as_col(r)) {
                    (Some(l), Some(r)) => cross_table(l, r, left_cols, right_cols),
                    _ => None,
                };
                match (op, cross) {
                    (BinOp::Eq, Some((lc, rc, _swapped))) => {
                        // Drop an exact-duplicate / sides-swapped-duplicate pair.
                        if eq.iter().any(|p| p.left == lc && p.right == rc) {
                            continue;
                        }
                        let tc = validate_join_key_pair(def_of(left_cols, lc), def_of(right_cols, rc))?;
                        eq.push(EqPair {
                            left: lc,
                            right: rc,
                            tc,
                        });
                    }
                    (_, Some((lc, rc, swapped))) if binop_to_range_rel(*op).is_some() && range.is_none() => {
                        let rel = binop_to_range_rel(*op).expect("range rel present");
                        let op = if swapped { converse_rel(rel) } else { rel };
                        let tc = validate_range_join_key_pair(def_of(left_cols, lc), def_of(right_cols, rc))?;
                        range = Some(HirRange {
                            left: lc,
                            right: rc,
                            op,
                            tc,
                        });
                    }
                    _ => residual.push(conj.clone()),
                }
            }
            _ => residual.push(conj.clone()),
        }
    }

    if eq.is_empty() && range.is_none() {
        return Err(GnitzSqlError::Bind(
            "JOIN ON must have at least one equijoin or range predicate".into(),
        ));
    }
    let slots = eq.len() + range.is_some() as usize;
    if slots > gnitz_core::PK_LIST_MAX_COLS {
        return Err(GnitzSqlError::Unsupported(if range.is_none() {
            format!(
                "JOIN ON: at most {} equijoin key columns are supported (got {})",
                gnitz_core::PK_LIST_MAX_COLS,
                eq.len()
            )
        } else {
            format!(
                "range JOIN ON: at most {} join key columns (equality prefix + \
                 range) are supported (got {})",
                gnitz_core::PK_LIST_MAX_COLS,
                slots
            )
        }));
    }
    Ok(JoinClass { eq, range, residual })
}

/// The `ColId` of a bare `ColRef` leaf, else `None` (a literal/expression operand).
fn as_col(e: &HirExpr) -> Option<ColId> {
    match e {
        BExpr::ColRef(HirRef::Col(id)) => Some(*id),
        _ => None,
    }
}

/// Canonicalize a cross-table pair to `(left ColId, right ColId, swapped)` — the
/// HIR analogue of `cross_table_pair`, using `ColId` membership instead of the
/// combined-offset comparison. `swapped` = the right-table column was the
/// syntactically-left operand (`b.y OP a.x`), which drives `converse_rel`.
fn cross_table(l: ColId, r: ColId, left_cols: &[HirCol], right_cols: &[HirCol]) -> Option<(ColId, ColId, bool)> {
    let in_left = |id: ColId| col_by_id(left_cols, id).is_some();
    let in_right = |id: ColId| col_by_id(right_cols, id).is_some();
    if in_left(l) && in_right(r) {
        Some((l, r, false))
    } else if in_left(r) && in_right(l) {
        Some((r, l, true))
    } else {
        None
    }
}

/// The def of a `ColId` within one side's cols.
fn def_of(cols: &[HirCol], id: ColId) -> &ColumnDef {
    &col_by_id(cols, id).expect("classified ColId in its side").def
}

/// `ir::BinOp` → the ordering `RangeRel` (the four range variants). AST-free —
/// the `sql_binop_to_range_rel` in predicates.rs is `sqlparser::BinaryOperator`-typed.
fn binop_to_range_rel(op: BinOp) -> Option<RangeRel> {
    match op {
        BinOp::Lt => Some(RangeRel::Lt),
        BinOp::Le => Some(RangeRel::Le),
        BinOp::Gt => Some(RangeRel::Gt),
        BinOp::Ge => Some(RangeRel::Ge),
        _ => None,
    }
}
