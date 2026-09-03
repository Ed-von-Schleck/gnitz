//! Predicate-classification rewrite (pass 3). Partitions a join's conjuncts into
//! a `JoinClass` (equality pairs + an optional range conjunct + the residual). An
//! INNER join also classifies the WHERE directly above it — `ON p WHERE q ≡
//! ON (p AND q)` — which is what keys a comma-join step; an OUTER join leaves it
//! above as a post-null-fill filter. Left-vs-right is a `ColId` membership test
//! (bind mints distinct ids per reference site, so even a self-join's two sides
//! are disjoint).

use super::{
    as_col, col_by_id, hircol_of, ColId, ColIdGen, EqPair, HirCol, HirExpr, HirRange, HirRef, JoinClass, JoinOn,
    ProjEntry, RelExpr, SubqueryKind, SubqueryRef,
};
use crate::error::GnitzSqlError;
use crate::hir::guards::{converse_rel, validate_join_key_pair, validate_range_join_key_pair};
use crate::hir::JoinType;
use crate::ir::{AggFunc, BExpr, BinOp, UnaryOp};
use gnitz_core::{ColumnDef, RangeRel, TypeCode};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Classify every join's predicates and place the WHERE, recursing through the
/// whole tree so a Join buried under a `Reduce`/`Distinct`/`SetOp` (GROUP BY over
/// a join, set-op side that is a join, …) is classified too — else its `on` stays
/// `JoinOn::Raw` and lowering rejects it. A join-free (linear) tree is rebuilt
/// unchanged.
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

/// Rebuild the spine: classify every buried `Join`, folding each WHERE `Filter`
/// directly above a Join into that join's own classification (INNER) or keeping
/// it as a post-null-fill filter (OUTER). Every other node delegates its
/// reassembly to the generic `RelExpr::map_children`.
fn classify_rel(rel: Rc<RelExpr>, memo: &mut RewriteMemo) -> Result<Rc<RelExpr>, GnitzSqlError> {
    if matches!(rel.as_ref(), RelExpr::Join { .. }) {
        return classify_join(&rel, &[], memo);
    }
    let key = Rc::as_ptr(&rel);
    if let Some(done) = memo.get(&key) {
        return Ok(Rc::clone(done));
    }
    let out = match rel.as_ref() {
        RelExpr::Filter { input, preds } if matches!(input.as_ref(), RelExpr::Join { .. }) => {
            let inner = matches!(
                input.as_ref(),
                RelExpr::Join {
                    kind: JoinType::Inner,
                    ..
                }
            );
            // INNER: `ON p WHERE q ≡ ON (p AND q)`, so the WHERE joins the ON in one
            // classification and may key the join. OUTER: it is a 3VL filter over the
            // post-null-fill output, so it stays above and contributes no key.
            let extra: &[HirExpr] = if inner { preds } else { &[] };
            let join = classify_join(input, extra, memo)?;
            if inner {
                join
            } else {
                RelExpr::filter(join, preds.clone())
            }
        }
        _ => RelExpr::map_children(&rel, &mut |child| classify_rel(Rc::clone(child), memo))?,
    };
    memo.insert(key, Rc::clone(&out));
    Ok(out)
}

/// Classify one `Join` node (`JoinOn::Raw` → `JoinOn::Class`, recursing children).
/// `extra` carries the WHERE conjuncts an INNER join absorbs.
fn classify_join(rel: &Rc<RelExpr>, extra: &[HirExpr], memo: &mut RewriteMemo) -> Result<Rc<RelExpr>, GnitzSqlError> {
    // Shared through the memo only when the result is the node's own: classified
    // against `extra`, it belongs to the parent that pushed those conjuncts down.
    let key = extra.is_empty().then_some(Rc::as_ptr(rel));
    if let Some(done) = key.and_then(|k| memo.get(&k)) {
        return Ok(Rc::clone(done));
    }
    let RelExpr::Join {
        left,
        right,
        kind,
        on,
        mark,
        ..
    } = rel.as_ref()
    else {
        unreachable!("classify_join receives a Join");
    };
    let left_cols = left.cols();
    let right_cols = right.cols();

    // A left-deep spine puts every step's WHERE above the TOP join, so a conjunct
    // naming only left-input columns keys a step further down. Hand those there —
    // but never below an outer join, which would run them before its null-fill.
    let inner_left = matches!(
        left.as_ref(),
        RelExpr::Join {
            kind: JoinType::Inner,
            ..
        }
    );
    let (here, deeper): (Vec<HirExpr>, Vec<HirExpr>) = extra
        .iter()
        .cloned()
        .partition(|c| !inner_left || !refs_within(c, &left_cols));

    let new_left = if deeper.is_empty() {
        classify_rel(Rc::clone(left), memo)?
    } else {
        classify_join(left, &deeper, memo)?
    };
    let new_right = classify_rel(Rc::clone(right), memo)?;
    let JoinOn::Raw(raw) = on else {
        unreachable!("classify visits each join once")
    };
    let class = classify_on(raw, &here, &left_cols, &right_cols)?;
    crate::hir::guards::reject_keyless_non_inner(*kind, class.shape())?;
    crate::hir::guards::reject_outer_with_residual(*kind, class.residual.is_empty())?;
    let out = Rc::new(RelExpr::Join {
        left: new_left,
        right: new_right,
        kind: *kind,
        on: JoinOn::Class(class),
        mark: mark.clone(),
    });
    if let Some(k) = key {
        memo.insert(k, Rc::clone(&out));
    }
    Ok(out)
}

/// Whether `conj` names at least one column and every column it names lives in
/// `cols` — the test for "this conjunct is about the left input alone", and so a
/// candidate for a step further down the spine. A non-column reference (a
/// subquery leaf) keeps it here, where its operands are known to resolve.
fn refs_within(conj: &HirExpr, cols: &[HirCol]) -> bool {
    let mut seen = 0usize;
    let mut within = true;
    conj.for_each_ref(&mut |r: &HirRef| {
        seen += 1;
        within &= matches!(r, HirRef::Col(id) if col_by_id(cols, *id).is_some());
    });
    seen > 0 && within
}

/// Partition the join's conjuncts into a `JoinClass`. `left_cols` / `right_cols`
/// define the left-vs-right membership. `on` is what the join step wrote, `extra`
/// what an INNER join's WHERE contributes.
fn classify_on(
    on: &[HirExpr],
    extra: &[HirExpr],
    left_cols: &[HirCol],
    right_cols: &[HirCol],
) -> Result<JoinClass, GnitzSqlError> {
    let mut class = JoinClass {
        eq: Vec::new(),
        range: None,
        residual: Vec::new(),
    };
    absorb_conjuncts(&mut class, on, left_cols, right_cols, KeyDemand::Required)?;
    absorb_conjuncts(&mut class, extra, left_cols, right_cols, KeyDemand::Optional)?;
    crate::hir::guards::reject_join_key_arity(class.eq.len(), class.range.is_some())?;
    Ok(class)
}

/// What a cross-table comparison this join cannot key means.
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyDemand {
    /// It was written as the join's own predicate: raise the error that names why
    /// it cannot be a key (a float key column, an unpromotable type pair, one
    /// range conjunct too many).
    Required,
    /// It was written as a WHERE predicate: keep it as a residual filter. The
    /// promotion is a better plan for the same rows, never a requirement, so a
    /// pair that will not key must not turn a working query into an error.
    Optional,
}

/// One conjunct's cross-table shape, or `None` for anything that is not a
/// comparison between a left column and a right one — which is every conjunct
/// this join can only apply as a filter. `swapped` records that it was written
/// `right OP left`, so a range operator can be turned to face the same way.
fn cross_comparison(
    conj: &HirExpr,
    left_cols: &[HirCol],
    right_cols: &[HirCol],
) -> Option<(ColId, ColId, bool, BinOp)> {
    let BExpr::BinOp(l, op, r) = conj else {
        return None;
    };
    let (lc, rc, swapped) = cross_table(as_col(l)?, as_col(r)?, left_cols, right_cols)?;
    Some((lc, rc, swapped, *op))
}

/// Fold `conjuncts` into `class`: each cross-table equality becomes a key column,
/// the first cross-table range comparison becomes the range slot, and everything
/// else becomes a residual predicate.
fn absorb_conjuncts(
    class: &mut JoinClass,
    conjuncts: &[HirExpr],
    left_cols: &[HirCol],
    right_cols: &[HirCol],
    demand: KeyDemand,
) -> Result<(), GnitzSqlError> {
    for conj in conjuncts {
        let Some((lc, rc, swapped, op)) = cross_comparison(conj, left_cols, right_cols) else {
            class.residual.push(conj.clone());
            continue;
        };
        // A pair this join has already keyed on, in either orientation, adds
        // nothing — the duplicate is dropped rather than widening the key.
        if op == BinOp::Eq && class.eq.iter().any(|p| p.left == lc && p.right == rc) {
            continue;
        }
        let (left_def, right_def) = (&hircol_of(left_cols, lc).def, &hircol_of(right_cols, rc).def);
        // Past the cap a `Required` conjunct still claims a slot, so the closing
        // `reject_join_key_arity` reports the count the user wrote; an `Optional` one
        // stays a residual filter, which computes the same rows.
        let range_after = class.range.is_some() || op != BinOp::Eq;
        let fits = |eq: usize| {
            demand == KeyDemand::Required
                || crate::hir::guards::reject_join_key_arity(class.eq.len() + eq, range_after).is_ok()
        };
        // `binop_to_range_rel` never answers for `Eq`, so the two key arms cannot
        // both match and neither needs to exclude the other.
        let failed = match (op, binop_to_range_rel(op)) {
            (BinOp::Eq, _) if fits(1) => match validate_join_key_pair(left_def, right_def) {
                Ok(tc) => {
                    class.eq.push(EqPair {
                        left: lc,
                        right: rc,
                        tc,
                    });
                    continue;
                }
                Err(e) => e,
            },
            (_, Some(rel)) if class.range.is_none() && fits(0) => {
                match validate_range_join_key_pair(left_def, right_def) {
                    Ok(tc) => {
                        class.range = Some(HirRange {
                            left: lc,
                            right: rc,
                            op: if swapped { converse_rel(rel) } else { rel },
                            tc,
                        });
                        continue;
                    }
                    Err(e) => e,
                }
            }
            // No slot for it, or an operator no key can carry: a filter.
            _ => {
                class.residual.push(conj.clone());
                continue;
            }
        };
        // The pair will not key. Written as the join's own predicate that is the
        // error; written in the WHERE it is still a perfectly good filter.
        if demand == KeyDemand::Required {
            return Err(failed);
        }
        class.residual.push(conj.clone());
    }
    Ok(())
}

/// Canonicalize a cross-table pair to `(left ColId, right ColId, swapped)`,
/// deciding each side by `ColId` membership in the two column lists rather than
/// by a combined-offset comparison. `swapped` = the right-table column was the
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

/// `ir::BinOp` → the ordering `RangeRel` (the four range variants).
fn binop_to_range_rel(op: BinOp) -> Option<RangeRel> {
    match op {
        BinOp::Lt => Some(RangeRel::Lt),
        BinOp::Le => Some(RangeRel::Le),
        BinOp::Gt => Some(RangeRel::Gt),
        BinOp::Ge => Some(RangeRel::Ge),
        _ => None,
    }
}

// ── Decorrelation (pass 2 — before classify) ─────────────────────────────────────
//
// Rewrites every `HirRef::Subquery` leaf into `Join`/`Reduce` structure: an EXISTS
// as a bare top-level WHERE conjunct becomes a Semi/Anti join (the conjunct
// consumed); an EXISTS anywhere else becomes a Mark join with the leaf substituted
// by its `0/1` mark column; a correlated scalar becomes a LEFT join with the leaf
// substituted by the aggregate's finalize composite; an uncorrelated scalar in a
// top-level WHERE comparison becomes an INNER join whose ON is that comparison
// (the aggregate value is the join key), the conjunct consumed. Rebuild-only
// (never mutates), so the rewrite is safe over shared subtrees. No
// `HirRef::Subquery` survives — a leftover is the physicalization's hard error.

/// Decorrelate every subquery leaf in `rel`, minting mark columns from `ids`.
pub(crate) fn decorrelate(rel: Rc<RelExpr>, ids: &ColIdGen) -> Result<Rc<RelExpr>, GnitzSqlError> {
    // Subqueries live only in a single-table linear body's `Project(Filter?(Get))`
    // (bind rejects them elsewhere); transform that node, else recurse.
    if let RelExpr::Project { input, items } = rel.as_ref() {
        if items.iter().any(|i| expr_has_subquery(&i.expr)) || filter_has_subquery(input) {
            return decorrelate_body(items, input, ids);
        }
    }
    RelExpr::map_children(&rel, &mut |c| decorrelate(Rc::clone(c), ids))
}

/// Whether an expression carries a `HirRef::Subquery` leaf.
fn expr_has_subquery(e: &HirExpr) -> bool {
    let mut found = false;
    e.for_each_ref(&mut |r| found |= matches!(r, HirRef::Subquery(_)));
    found
}

/// Whether an optional WHERE `Filter` above a Get carries a subquery leaf.
fn filter_has_subquery(input: &Rc<RelExpr>) -> bool {
    matches!(input.as_ref(), RelExpr::Filter { preds, .. } if preds.iter().any(expr_has_subquery))
}

/// Transform a subquery-carrying `Project(items, Filter?(Get))` body.
fn decorrelate_body(items: &[ProjEntry], input: &Rc<RelExpr>, ids: &ColIdGen) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let (fpreds, get): (Vec<HirExpr>, Rc<RelExpr>) = match input.as_ref() {
        RelExpr::Filter { input, preds } => (preds.clone(), Rc::clone(input)),
        _ => (Vec::new(), Rc::clone(input)),
    };

    let mut cur = get;
    let mut kept_preds: Vec<HirExpr> = Vec::new();
    let mut subst: HashMap<*const RelExpr, HirExpr> = HashMap::new();

    // Pass 1 — consumed WHERE conjuncts (bare EXISTS → Semi/Anti; uncorrelated
    // scalar comparison → INNER join-as-filter). Everything else is kept. A
    // consumed conjunct is dropped from `kept_preds` wholesale, so its subquery
    // `rel` never reaches pass 2's scan — no cross-pass dedup is needed.
    for pred in fpreds {
        if let Some((subref, not_wrap)) = as_bare_exists(&pred) {
            let negated = exists_negated(subref) ^ not_wrap;
            let kind = if negated { JoinType::Anti } else { JoinType::Semi };
            // A NOT IN over a nullable operand diverges from the anti-join (SQL
            // 3VL). The bind-time guard only sees the node's own `negated`; a
            // wrapping `NOT` flips it here, so re-check at the effective polarity.
            if kind == JoinType::Anti && subref.in_pair.is_some_and(|p| p.nullable) {
                return Err(GnitzSqlError::Unsupported(
                    "NOT IN (SELECT …) requires the outer operand and the subquery column to be \
                     NOT NULL (SQL's NULL semantics diverge from the anti-join otherwise); \
                     use NOT EXISTS with an explicit equality instead"
                        .into(),
                ));
            }
            cur = RelExpr::join(cur, Rc::clone(&subref.rel), kind, corr_on(subref), None);
            continue;
        }
        if let Some((outer_col, op, subref)) = as_uncorrelated_scalar_cmp(&pred) {
            cur = build_uncorrelated_inner(cur, outer_col, op, subref, ids)?;
            continue;
        }
        kept_preds.push(pred);
    }

    // Pass 2 — build Mark / LEFT joins for every remaining subref (kept preds +
    // projection), deduped by `rel` pointer (a range quantifier references its
    // extremum leaf twice, both clones sharing the `rel` Rc).
    let mut collected: Vec<SubqueryRef> = Vec::new();
    let mut collect_seen: HashSet<*const RelExpr> = HashSet::new();
    for e in kept_preds.iter().chain(items.iter().map(|i| &i.expr)) {
        e.for_each_ref(&mut |r| {
            if let HirRef::Subquery(s) = r {
                if collect_seen.insert(Rc::as_ptr(&s.rel)) {
                    collected.push((**s).clone());
                }
            }
        });
    }
    // A mark join consumes its `0/1` column via two per-branch substitutions, so it
    // cannot expose that column for a second mark to read — at most one EXISTS/IN
    // may sit in a mark position (under OR/NOT, in CASE, or projected). A top-level
    // AND conjunct instead becomes a semi/anti join, which composes freely.
    if collected
        .iter()
        .filter(|s| matches!(s.kind, SubqueryKind::Exists { .. }))
        .count()
        > 1
    {
        return Err(GnitzSqlError::Unsupported(
            "at most one EXISTS/IN subquery in a mark position (under OR/NOT, in CASE, or projected) \
             is supported; compose via stacked views or top-level AND conjuncts"
                .into(),
        ));
    }
    for s in &collected {
        cur = build_joined_subref(cur, s, ids, &mut subst)?;
    }

    // Pass 3 — substitute leaves.
    let new_preds = kept_preds
        .iter()
        .map(|p| substitute(p, &subst))
        .collect::<Result<Vec<_>, _>>()?;
    let new_items = items
        .iter()
        .map(|it| -> Result<ProjEntry, GnitzSqlError> {
            Ok(ProjEntry {
                expr: substitute(&it.expr, &subst)?,
                out: it.out.clone(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let source = if new_preds.is_empty() {
        cur
    } else {
        RelExpr::filter(cur, new_preds)
    };
    Ok(RelExpr::project(source, new_items))
}

/// Build a Mark join (EXISTS elsewhere) or a correlated-scalar LEFT join, and
/// record the leaf substitution.
fn build_joined_subref(
    cur: Rc<RelExpr>,
    s: &SubqueryRef,
    ids: &ColIdGen,
    subst: &mut HashMap<*const RelExpr, HirExpr>,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let p = Rc::as_ptr(&s.rel);
    match s.kind {
        SubqueryKind::Exists { negated } => {
            // A nullable-operand IN in a mark position diverges from the two-valued
            // mark (SQL 3VL), regardless of polarity.
            if s.in_pair.is_some_and(|p| p.nullable) {
                return Err(GnitzSqlError::Unsupported(
                    "IN (SELECT …) in a mark position (under OR/NOT, in CASE, or projected) requires \
                     the outer operand and the subquery column to be NOT NULL — SQL's 3VL diverges \
                     from the two-valued mark; use a top-level AND `x IN (SELECT …)` conjunct, or \
                     NOT EXISTS with an explicit equality"
                        .into(),
                ));
            }
            let mark_id = ids.next();
            let mark = HirCol::new(mark_id, ColumnDef::new("_mark", TypeCode::I64, false).hidden());
            let joined = RelExpr::join(cur, Rc::clone(&s.rel), JoinType::Mark, corr_on(s), Some(mark));
            // The mark column is 1 iff the subquery matched; the node's truth is
            // `matched` (EXISTS) or `!matched` (NOT EXISTS).
            let val = if negated {
                BExpr::BinOp(
                    Box::new(BExpr::ColRef(HirRef::Col(mark_id))),
                    BinOp::Eq,
                    Box::new(BExpr::LitInt(0)),
                )
            } else {
                BExpr::ColRef(HirRef::Col(mark_id))
            };
            subst.insert(p, val);
            Ok(joined)
        }
        SubqueryKind::Scalar => {
            if s.correlation.is_empty() {
                return Err(GnitzSqlError::Unsupported(
                    "an uncorrelated scalar aggregate subquery is only supported as a top-level WHERE \
                     comparison conjunct (`outer_col OP (SELECT AGG …)`)"
                        .into(),
                ));
            }
            let joined = RelExpr::join(cur, Rc::clone(&s.rel), JoinType::Left, s.correlation.clone(), None);
            subst.insert(p, scalar_value(s, true)?);
            Ok(joined)
        }
    }
}

/// Build the INNER join-as-filter of an uncorrelated scalar comparison: the ON is
/// `outer_col OP agg_col`, so the classify pass turns the comparison itself into
/// the join key. The aggregate value is a join key, so AVG / a float aggregate is
/// rejected (there is no order-preserving reindex for a float key).
fn build_uncorrelated_inner(
    cur: Rc<RelExpr>,
    outer_col: ColId,
    op: BinOp,
    subref: &SubqueryRef,
    ids: &ColIdGen,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let agg = subref.scalar_agg()?;
    if matches!(agg.func, AggFunc::Avg) || agg.out.def.type_code.is_float() {
        return Err(GnitzSqlError::Unsupported(
            "an uncorrelated scalar aggregate compared to an outer column must be integer-typed \
             (AVG and a float SUM/MIN/MAX are rejected — the aggregate value is a join key)"
                .into(),
        ));
    }
    // The aggregate value is the join key. A companion-bearing aggregate (a nullable
    // SUM) is NULL for an empty / fully-retracted group only once finalized over its
    // COUNT_NON_NULL companion (`sum / (cnt != 0)`) — the raw reduce column reads a
    // spurious `0`, which as a join key would then spuriously match `outer = 0`. So
    // key on the *finalized* value, materialized as one column by a `Project` the
    // lowering cuts to a hidden segment (the `G_global` finalize runs through the
    // general grouped-view finalize). A companion-free
    // aggregate needs no finalize — its raw column already is the value (plain SUM,
    // MIN/MAX re-derive NULL on exhaustion, COUNT is never NULL) — so it keys directly
    // with no extra segment.
    let (right, key_id) = match &agg.companion {
        None => (Rc::clone(&subref.rel), agg.out.id),
        Some(_) => {
            // Hidden like every other synthetic slot: the finalized value exists
            // to be a join key, and no name reaches it.
            let key = HirCol::new(ids.next(), ColumnDef::new("_agg", agg.out.def.type_code, true).hidden());
            let key_id = key.id;
            let proj = RelExpr::project(
                Rc::clone(&subref.rel),
                vec![ProjEntry {
                    expr: scalar_value(subref, false)?,
                    out: key,
                }],
            );
            (proj, key_id)
        }
    };
    let on = vec![BExpr::BinOp(
        Box::new(BExpr::ColRef(HirRef::Col(outer_col))),
        op,
        Box::new(BExpr::ColRef(HirRef::Col(key_id))),
    )];
    Ok(RelExpr::join(cur, right, JoinType::Inner, on, None))
}

/// The scalar subquery's substituted value: the aggregate's finalize composite.
/// `null_filled` says the decorrelation was a LEFT join, which null-fills an
/// unmatched outer row — so a COUNT (never NULL by definition: an empty group
/// counts `0`) is re-floored to `0`. The uncorrelated INNER join-as-filter passes
/// `false`: it drops unmatched rows instead, so there is nothing to repair.
fn scalar_value(s: &SubqueryRef, null_filled: bool) -> Result<HirExpr, GnitzSqlError> {
    let agg = s.scalar_agg()?;
    let v = agg.finalize();
    if null_filled && s.never_null() {
        // COUNT is Direct (`v == ColRef(agg.out)`); after the LEFT join it is
        // nullable, so `CASE WHEN it IS NOT NULL THEN it ELSE 0`.
        Ok(BExpr::Case {
            branches: vec![(
                BExpr::NullTest {
                    inner: Box::new(BExpr::ColRef(HirRef::Col(agg.out.id))),
                    want_null: false,
                },
                v,
            )],
            else_: Some(Box::new(BExpr::LitInt(0))),
        })
    } else {
        Ok(v)
    }
}

/// A bare EXISTS/IN WHERE conjunct (optionally `NOT`-wrapped), returning its
/// `SubqueryRef` and whether a `NOT` wrapped it (XORed into the `negated` flag).
fn as_bare_exists(pred: &HirExpr) -> Option<(&SubqueryRef, bool)> {
    match pred {
        BExpr::ColRef(HirRef::Subquery(s)) if matches!(s.kind, SubqueryKind::Exists { .. }) => Some((s, false)),
        BExpr::UnaryOp(UnaryOp::Not, inner) => match inner.as_ref() {
            BExpr::ColRef(HirRef::Subquery(s)) if matches!(s.kind, SubqueryKind::Exists { .. }) => Some((s, true)),
            _ => None,
        },
        _ => None,
    }
}

fn exists_negated(s: &SubqueryRef) -> bool {
    matches!(s.kind, SubqueryKind::Exists { negated: true })
}

/// An uncorrelated scalar comparison `outer_col OP (SELECT agg)` (either operand
/// order), oriented so the outer column is the left operand.
fn as_uncorrelated_scalar_cmp(pred: &HirExpr) -> Option<(ColId, BinOp, &SubqueryRef)> {
    let BExpr::BinOp(l, op, r) = pred else {
        return None;
    };
    let op = *op;
    if !matches!(op, BinOp::Eq | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge) {
        return None;
    }
    if let (Some(oc), Some(s)) = (as_col(l), uncorr_scalar(r)) {
        return Some((oc, op, s));
    }
    if let (Some(s), Some(oc)) = (uncorr_scalar(l), as_col(r)) {
        return Some((oc, op.converse(), s));
    }
    None
}

fn uncorr_scalar(e: &HirExpr) -> Option<&SubqueryRef> {
    match e {
        BExpr::ColRef(HirRef::Subquery(s)) if matches!(s.kind, SubqueryKind::Scalar) && s.correlation.is_empty() => {
            Some(s)
        }
        _ => None,
    }
}

/// The decorrelated join's ON conjuncts: the correlation conjuncts plus, for IN,
/// the `(outer, inner)` equality.
fn corr_on(s: &SubqueryRef) -> Vec<HirExpr> {
    let mut on = s.correlation.clone();
    if let Some(p) = s.in_pair {
        on.push(BExpr::BinOp(
            Box::new(BExpr::ColRef(HirRef::Col(p.outer))),
            BinOp::Eq,
            Box::new(BExpr::ColRef(HirRef::Col(p.inner))),
        ));
    }
    on
}

/// Replace every `HirRef::Subquery` leaf with its substituted value; a `Col` leaf
/// passes through.
fn substitute(e: &HirExpr, subst: &HashMap<*const RelExpr, HirExpr>) -> Result<HirExpr, GnitzSqlError> {
    e.try_rebuild(&|r| match r {
        HirRef::Subquery(s) => subst
            .get(&Rc::as_ptr(&s.rel))
            .cloned()
            .ok_or_else(|| GnitzSqlError::Internal("subquery leaf without a substitution".into())),
        HirRef::Col(_) => Ok(BExpr::ColRef(r.clone())),
    })
}
