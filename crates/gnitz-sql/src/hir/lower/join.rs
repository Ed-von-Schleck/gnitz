//! The one join shell: every `Join` step, decorrelated kinds included, over the
//! circuit primitives in [`super::joincore`], its output key read off [`OutKey`].

use super::super::{ColId, HirExpr, JoinClass, JoinShape, JoinType, OutKey, ProjEntry, RelExpr};
use super::joincore::{
    emit_range_null_fill_tail, equi_prologue, join_pk_coldefs, pair_pk_coldefs, range_prologue, rekey_on_source_pk,
    src_pk_coldefs,
};
use super::spine::{self, SourceOrigin, Spine, Top};
use super::{emit_filter, materialize, project_front, Demand, EmitPieces, ViewChain};
use crate::error::GnitzSqlError;
use crate::hir::physical::Frame;
use crate::ir::BExpr;
use crate::rules::reject_pk_list_arity;

use gnitz_wire::JoinKind;
use gnitz_wire::{Circuit, ColumnDef, NodeId, TypeCode};
use std::borrow::Cow;
use std::collections::HashSet;
use std::rc::Rc;

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
        let filtered = emit_filter(&mut cb, node, &preds, &frame)?;
        let (node, out) = project_front(&mut cb, filtered, &items, &frame)?;
        acc = Some(match acc {
            None => (node, out),
            Some((prior, out)) => (cb.union(node, prior), out),
        });
    }
    let (node, out) = acc.expect("a join step emits at least one branch");
    let node = match out_key.exchanged() {
        true => cb.shard(node, &(0..out.schema.pk_cols.len() as u32).collect::<Vec<_>>()),
        false => node,
    };
    // A join key and a pair PK each identify a matched pair, not a row.
    let pk_repeats = match out_key {
        OutKey::JoinKey | OutKey::PairPk => true,
        OutKey::OuterPk { .. } => sides[0].pk_repeats(),
    };
    Ok(EmitPieces { circuit: cb, top: node, out, pk_repeats })
}

/// A join's two inputs opened through the spine and emitted into `cb`, each
/// carrying what `down` reads and its own keys, and kept under [`join_keep`].
fn emit_join_inputs(
    chain: &mut ViewChain,
    cb: &mut Circuit,
    down: Demand<'_>,
    [left, right]: [&Rc<RelExpr>; 2],
    kind: JoinType,
    class: &JoinClass,
) -> Result<([NodeId; 2], [JoinSide; 2]), GnitzSqlError> {
    let live = |is_left: bool| {
        let mut live = HashSet::new();
        down.refs(&mut live);
        live.extend(class.key_cols(is_left));
        live
    };
    let (live_l, live_r) = (live(true), live(false));
    let mut l = spine::open(chain, left, &live_l)?;
    let mut r = spine::open(chain, right, &live_r)?;
    // A join reads two distinct sources, one delta per epoch: a right side over the
    // left's source is re-read as a second relation.
    if l.tid() == r.tid() {
        r = Spine::segment(materialize(chain, right, &live_r)?);
    }
    // A key the spine computes (`ON x.s = u.k` over `SELECT a + 1 AS s`) is no
    // column of the scanned relation, so nothing over that relation can state
    // where its delta scatters. Cut to a segment, the computed column is one.
    let mut origin_l = l.origin();
    if !class.key_cols(true).all(|id| origin_l.resolves(id)) {
        l = Spine::segment(materialize(chain, left, &live_l)?);
        origin_l = l.origin();
    }
    let mut origin_r = r.origin();
    if !class.key_cols(false).all(|id| origin_r.resolves(id)) {
        r = Spine::segment(materialize(chain, right, &live_r)?);
        origin_r = r.origin();
    }
    let (a, left_frame) = l.emit(cb, Top::Slots)?;
    let (b, right_frame) = r.emit(cb, Top::Slots)?;
    let frames = [left_frame, right_frame];
    let [kept_l, kept_r] = join_keep(down, class, kind, &frames)?;
    let [frame_l, frame_r] = frames;
    let side = |frame: Frame, origin, (keep, pk_arity), is_left| -> Result<JoinSide, GnitzSqlError> {
        Ok(JoinSide {
            key: frame.slots(class.key_cols(is_left))?,
            keep,
            pk_arity,
            frame,
            origin,
        })
    };
    Ok((
        [a, b],
        [
            side(frame_l, origin_l, kept_l, true)?,
            side(frame_r, origin_r, kept_r, false)?,
        ],
    ))
}

/// One side of a join as [`join_keep`] left it.
pub(super) struct JoinSide {
    pub(super) frame: Frame,
    /// The reindex payload, in emission order.
    pub(super) keep: Vec<u32>,
    /// The join key columns, [`JoinClass::key_cols`] order, as `frame` slots.
    pub(super) key: Vec<usize>,
    pk_arity: usize,
    origin: SourceOrigin,
}

impl JoinSide {
    /// `key`, this side's reindex key in its own emitted layout, restated over
    /// the relation the master scatters.
    pub(super) fn scatter_key(
        &self,
        key: &[gnitz_wire::ReindexSlot],
    ) -> Result<gnitz_wire::ReindexRole, GnitzSqlError> {
        self.origin.scatter_role(&self.frame, key).ok_or_else(|| {
            GnitzSqlError::Internal("a join key column is not a column of the relation it scatters".into())
        })
    }

    /// The kept payload width.
    pub(super) fn n(&self) -> usize {
        self.keep.len()
    }

    /// The pinned source PK's arity — `0` when this shape packs no PK out of the
    /// payload.
    pub(super) fn pa(&self) -> usize {
        self.pk_arity
    }

    /// Whether two of this side's source rows may share their leading key.
    pub(super) fn pk_repeats(&self) -> bool {
        self.origin.src.pk_repeats()
    }

    /// The kept payload's `ColId`s, in keep order.
    pub(super) fn ids(&self) -> impl Iterator<Item = Option<ColId>> + '_ {
        self.keep.iter().map(|&i| self.frame.layout[i as usize])
    }

    /// The kept payload columns' defs, in keep order.
    pub(super) fn kept_defs(&self) -> impl Iterator<Item = &ColumnDef> + '_ {
        self.keep.iter().map(|&i| &self.frame.schema.columns[i as usize])
    }

    /// The type codes of the kept payload columns — what `null_extend` needs to
    /// synthesize this side's NULL region.
    pub(super) fn kept_type_codes(&self) -> Vec<TypeCode> {
        self.kept_defs().map(|c| c.ty.tc).collect()
    }
}

/// Each side's reindex payload under the keep rule — which source columns survive
/// into the join's traces, and so onto disk — and its pinned source PK's arity.
/// A keep list is the pinned PK, then every other kept column in source order: it
/// need not ascend, and the PK at the front is what makes every pair-PK slot list
/// a range. A wildcard projection is already expanded into `ProjEntry` column
/// refs by bind, so Rule 1 covers `SELECT *`.
fn join_keep(
    down: Demand<'_>,
    class: &JoinClass,
    kind: JoinType,
    frames: &[Frame; 2],
) -> Result<[(Vec<u32>, usize); 2], GnitzSqlError> {
    let mut keep = frames.each_ref().map(|f| vec![false; f.layout.len()]);
    // Rules 1 + 2: the projection and the WHERE over the join. The mark column is
    // in neither layout: the shell substitutes it per branch by its `0/1` constant.
    let mut referenced: HashSet<ColId> = HashSet::new();
    down.refs(&mut referenced);
    if let JoinType::Mark(mark) = kind {
        referenced.remove(&mark);
    }
    for id in referenced {
        let side = usize::from(!frames[0].layout.contains(&Some(id)));
        keep[side][frames[side].slot(id)?] = true;
    }
    let pins = class.out_key(kind).pins();
    let banded = matches!(class.shape(), JoinShape::Band | JoinShape::PureRange);
    for (side, is_left) in [(0, true), (1, false)] {
        // Rule 3: a band or pure-range side with a ν keeps its key columns. A band
        // ν is keyed by the source PK, which a bag-valued side repeats; a pure
        // range re-keys its owned A slice onto the range column.
        if banded && kind.has_nu(is_left) {
            for pos in frames[side].slots(class.key_cols(is_left))? {
                keep[side][pos] = true;
            }
        }
        // Rule 4: a side whose source PK the output key packs out of the payload
        // keeps it, at the front.
        if pins[side] {
            for &c in &frames[side].schema.pk_cols {
                keep[side][c as usize] = true;
            }
        }
    }
    // Rule 5, the fallback: a side with a ν needs an identity to subtract on. A
    // side without a ν keeps nothing — column 0 would split one trace element per
    // key into one per row.
    for (side, is_left) in [(0, true), (1, false)] {
        if kind.has_nu(is_left) && !keep[side].contains(&true) {
            keep[side][0] = true;
        }
    }
    // A join reading nothing from either side still emits rows.
    if !keep[0].contains(&true) && !keep[1].contains(&true) {
        keep[0][0] = true;
    }
    Ok([0, 1].map(|side| {
        let pinned: &[u32] = if pins[side] { &frames[side].schema.pk_cols } else { &[] };
        let rest = (0..keep[side].len() as u32).filter(|&i| keep[side][i as usize] && !pinned.contains(&i));
        (pinned.iter().copied().chain(rest).collect(), pinned.len())
    }))
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
    let pro = equi_prologue(cb, class, sides, inputs, kind, unique[1])?;
    let (all, inner) = (pro.all, pro.inner);
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
            // A side unique on the key matches each P row at most once, so
            // π_P(inner) = [_join_pk, P] is P's matched rows at their own weight.
            let matched = match unique[o] {
                true => cb.map(inner, &proj),
                false => pro.matched(cb, p),
            };
            let nu_p = cb.difference(all[p], matched);
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

#[cfg(test)]
#[path = "tests/join.rs"]
mod tests;
