//! The one join shell: every `Join` step, decorrelated kinds included, over the
//! circuit primitives in [`super::joincore`], its output key read off [`OutKey`].

use super::super::{ColId, HirExpr, JoinClass, JoinShape, JoinType, OutKey, ProjEntry, RelExpr};
use super::joincore::{
    emit_range_null_fill_tail, equi_prologue, join_pk_coldefs, pair_pk_coldefs, range_prologue, rekey_on_source_pk,
    src_pk_coldefs, unmatched,
};
use super::{emit_filter, emit_join_inputs, project_front, Demand, EmitPieces, JoinSide, ViewChain};
use crate::error::GnitzSqlError;
use crate::hir::physical::Frame;
use crate::ir::BExpr;
use crate::validate::reject_pk_list_arity;

use gnitz_core::{Circuit, ColumnDef, NodeId};
use gnitz_wire::JoinKind;
use std::borrow::Cow;

/// One branch an emitter hands the shell: its node over the join frame, and a mark
/// branch's `0/1` constant.
pub(super) type Branch = (NodeId, Option<i64>);

/// Lower a `Project(Filter?(Join))` tree's join to circuit pieces, whose output
/// leads with the join step's [`OutKey`] slots, then the projected payload in item
/// order.
pub(super) fn lower_join_view(
    chain: &mut ViewChain,
    items: &[ProjEntry],
    where_preds: &[HirExpr],
    join: &RelExpr,
) -> Result<EmitPieces, GnitzSqlError> {
    let RelExpr::Join { left, right, kind, on: class } = join else {
        unreachable!("lower_join_view receives a Join");
    };
    let kind = *kind;
    let down = Demand { items, where_preds };

    let mut cb = Circuit::default();
    let (inputs, sides) = emit_join_inputs(chain, &mut cb, down, [left, right], kind, class)?;

    let out_key = class.out_key(kind);
    let out_pk = match out_key {
        OutKey::JoinKey => join_pk_coldefs(&class.key_tcs()),
        OutKey::PairPk => {
            let surface = match class.shape() {
                JoinShape::Cross => "CROSS JOIN output PK",
                _ => "range JOIN output PK",
            };
            reject_pk_list_arity(surface, sides[0].pa() + sides[1].pa())?;
            pair_pk_coldefs(&sides[0].frame.schema, &sides[1].frame.schema)
        }
        OutKey::OuterPk { .. } => src_pk_coldefs(&sides[0].frame.schema),
    };

    let unique = [class.side_unique(left, true), class.side_unique(right, false)];
    let branches = match class.shape() {
        JoinShape::Equi => emit_equi(&mut cb, inputs, class, kind, &sides, unique)?,
        JoinShape::Band | JoinShape::PureRange => emit_range(&mut cb, inputs, class, kind, &sides, unique)?,
        JoinShape::Cross if kind == JoinType::Inner => emit_cross(&mut cb, inputs, &sides)?,
        JoinShape::Cross => unreachable!("reject_join_shape refuses a keyless non-INNER join"),
    };

    // The frame every branch filters and projects against.
    let frame = join_frame(out_pk, &sides, kind)?;
    let mut acc: Option<(NodeId, Frame)> = None;
    for (node, mark) in branches {
        let (preds, items): (Cow<'_, [HirExpr]>, Cow<'_, [ProjEntry]>) = match (kind, mark) {
            (JoinType::Mark(id), Some(val)) => (
                where_preds.iter().map(|e| subst_mark_lit(e, id, val)).collect(),
                items
                    .iter()
                    .map(|it| ProjEntry {
                        expr: subst_mark_lit(&it.expr, id, val),
                        out: it.out.clone(),
                    })
                    .collect(),
            ),
            _ => (Cow::Borrowed(where_preds), Cow::Borrowed(items)),
        };
        let filtered = emit_filter(&mut cb, node, preds.iter(), &frame)?;
        let (node, out) = project_front(&mut cb, filtered, &items, &frame)?;
        acc = Some(match acc {
            None => (node, out),
            Some((prior, out)) => (cb.union(node, prior), out),
        });
    }
    let (node, out) = acc.expect("a join step emits at least one branch");
    let node = match out_key.exchanged() {
        true => cb.shard(node, &(0..out.npk() as u32).collect::<Vec<_>>()),
        false => node,
    };
    // A join key and a pair PK each identify a matched pair, not a row.
    let pk_repeats = match out_key {
        OutKey::JoinKey | OutKey::PairPk => true,
        OutKey::OuterPk { .. } => sides[0].pk_repeats(),
    };
    Ok(EmitPieces { circuit: cb, top: node, out, pk_repeats })
}

/// Substitute `ColRef(mark_id)` with `LitInt(val)` throughout an expression —
/// the mark column's per-branch constant; every other leaf passes through.
fn subst_mark_lit(e: &HirExpr, mark_id: ColId, val: i64) -> HirExpr {
    let Ok(out) = e.try_rebuild::<ColId, std::convert::Infallible>(&mut |id| {
        Ok(match *id == mark_id {
            true => BExpr::LitInt(val),
            false => BExpr::ColRef(*id),
        })
    });
    out
}

/// One half of the outer input split by match existence.
#[derive(Clone, Copy)]
enum Split {
    Matched(NodeId),
    Unmatched(NodeId),
}

/// A decorrelated `kind`'s branches, from one half of `all`; the other half is
/// `all − known`.
fn exists_branches(cb: &mut Circuit, kind: JoinType, split: Split, all: NodeId) -> Vec<Branch> {
    let (Split::Matched(known) | Split::Unmatched(known)) = split;
    let complement = |cb: &mut Circuit| cb.difference(all, known);
    match (kind, split) {
        (JoinType::Semi, Split::Matched(half)) | (JoinType::Anti, Split::Unmatched(half)) => vec![(half, None)],
        (JoinType::Semi | JoinType::Anti, _) => vec![(complement(cb), None)],
        (JoinType::Mark(_), Split::Matched(matched)) => vec![(matched, Some(1)), (complement(cb), Some(0))],
        (JoinType::Mark(_), Split::Unmatched(unmatched)) => vec![(complement(cb), Some(1)), (unmatched, Some(0))],
        _ => unreachable!("exists_branches receives a decorrelated kind"),
    }
}

// ── Equi emission ───────────────────────────────────────────────────────────────

/// `inner ∪ ν_A ∪ ν_B`, a union over each preserved side's null-fill; a
/// decorrelated kind's branches split A by whether it matched B's key set.
fn emit_equi(
    cb: &mut Circuit,
    inputs: [NodeId; 2],
    class: &JoinClass,
    kind: JoinType,
    sides: &[JoinSide; 2],
    unique: [bool; 2],
) -> Result<Vec<Branch>, GnitzSqlError> {
    let (all, inner) = equi_prologue(cb, class, sides, inputs, kind, unique[1])?;
    if kind.is_decorrelated() {
        return Ok(exists_branches(cb, kind, Split::Matched(inner), all[0]));
    }
    let k = class.eq.len();
    // Each `[_join_pk, A, B]`, the other side's half all-NULL.
    let nus: Vec<NodeId> = [true, false]
        .into_iter()
        .filter(|&preserved_is_left| kind.preserves(preserved_is_left))
        .map(|preserved_is_left| {
            let (p, o) = (usize::from(!preserved_is_left), usize::from(preserved_is_left));
            let p0 = k + if preserved_is_left { 0 } else { sides[0].n() };
            let proj: Vec<u32> = (p0..p0 + sides[p].n()).map(|c| c as u32).collect();
            let pi = cb.map(inner, &proj); // π_P(inner) = [_join_pk, P]
            let nu_p = unmatched(cb, all[p], pi, unique[o]);
            // A side that keeps nothing contributes no NULL region at all.
            let o_tcs = sides[o].kept_type_codes();
            match o_tcs.is_empty() {
                true => nu_p,
                false => cb.null_extend(nu_p, &o_tcs, !preserved_is_left),
            }
        })
        .collect();
    let node = nus.into_iter().fold(inner, |merged, nu| cb.union(nu, merged));
    Ok(vec![(node, None)])
}

// ── Range / band emission ───────────────────────────────────────────────────────

fn emit_range(
    cb: &mut Circuit,
    inputs: [NodeId; 2],
    class: &JoinClass,
    kind: JoinType,
    sides: &[JoinSide; 2],
    unique: [bool; 2],
) -> Result<Vec<Branch>, GnitzSqlError> {
    let pro = range_prologue(cb, class, sides, inputs, kind)?;
    if kind.is_decorrelated() {
        return Ok(match class.shape() {
            // The one-row threshold `m = MAX/MIN(b.range)` decides existence over
            // A's owned slice, already on the worker owning its output key.
            JoinShape::PureRange => {
                let (owned, matched) = pro.threshold(cb);
                exists_branches(cb, kind, Split::Matched(matched), owned)
            }
            // ν over A, keyed on A's source PK behind the output exchange.
            _ => {
                let merged = pro.merged(cb);
                let (all, nu) = pro.nu(cb, merged, true, unique[1])?;
                exists_branches(cb, kind, Split::Unmatched(nu), all)
            }
        });
    }
    let merged = pro.merged(cb);
    // Every branch below lands on `[pair-PK, kept-A, kept-B]`.
    let rekey = pro.pair_keyed(cb, merged);
    let node = match (kind, class.shape()) {
        (JoinType::Inner, _) => rekey,
        (JoinType::Left, JoinShape::PureRange) => {
            // Pure-range threshold subtraction: `A − matched` against `m = MAX/MIN(b.range)`.
            let (owned, matched) = pro.threshold(cb);
            let nu_a = cb.difference(owned, matched);
            let branch = emit_range_null_fill_tail(cb, sides, nu_a, true);
            cb.union(branch, rekey)
        }
        (JoinType::Right | JoinType::Full, JoinShape::PureRange) => {
            unreachable!("reject_join_shape refuses a pure-range RIGHT/FULL join")
        }
        _ => {
            // Band ν_A (preserves_left) and/or ν_B (preserves_right).
            let mut acc = rekey;
            for preserved_is_left in [true, false] {
                if !kind.preserves(preserved_is_left) {
                    continue;
                }
                let (_, nu) = pro.nu(cb, merged, preserved_is_left, unique[usize::from(preserved_is_left)])?;
                let branch = emit_range_null_fill_tail(cb, sides, nu, preserved_is_left);
                acc = cb.union(branch, acc);
            }
            acc
        }
    };
    Ok(vec![(node, None)])
}

// ── Cross emission ──────────────────────────────────────────────────────────────

/// The keyless join `A × B`, INNER only. Each side keys its trace on its own
/// source PK, which takes no part in the match — it only partitions the trace the
/// broadcast delta is paired against.
fn emit_cross(
    cb: &mut Circuit,
    [input_a, input_b]: [NodeId; 2],
    sides: &[JoinSide; 2],
) -> Result<Vec<Branch>, GnitzSqlError> {
    let reindex_a = rekey_on_source_pk(cb, input_a, &sides[0], true)?;
    let reindex_b = rekey_on_source_pk(cb, input_b, &sides[1], true)?;
    let int_a = cb.worker_filter(reindex_a);
    let int_b = cb.worker_filter(reindex_b);
    let trace_a = cb.integrate_trace(int_a);
    let trace_b = cb.integrate_trace(int_b);
    // A keyless term keys on `[left PK…, right PK…]`, which is the pair-PK itself,
    // so both terms already share one schema.
    let inner = cb.join_terms([reindex_a, reindex_b], [trace_a, trace_b], JoinKind::Cross);
    Ok(vec![(inner, None)]) // [pair-PK, A, B]
}

// ── shared helpers ──────────────────────────────────────────────────────────────

/// The frame a join step's filters and projection read: `pk_cols`, then both
/// sides' kept payload with the null-providing side widened per `kind`, through
/// the same [`JoinType::widen_sides`] the logical join output uses.
fn join_frame(pk_cols: Vec<ColumnDef>, sides: &[JoinSide; 2], kind: JoinType) -> Result<Frame, GnitzSqlError> {
    let kept = |s: &JoinSide| s.kept_defs().cloned().collect::<Vec<_>>();
    let (mut l, mut r) = (kept(&sides[0]), kept(&sides[1]));
    kind.widen_sides(l.iter_mut(), r.iter_mut());
    Frame::keyed(
        pk_cols,
        sides[0].ids().chain(sides[1].ids()).zip(l.into_iter().chain(r)),
    )
}
