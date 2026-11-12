//! The one join shell: every `Join` step, decorrelated kinds included, its output
//! key chosen by [`out_key`]; the output-key PK columns it mints; the re-keys onto
//! join keys and source PKs; and the equi, range/band and cross emitters over them.

use super::super::{ColId, ExistsKind, HirExpr, JoinClass, JoinPlan, JoinType, ProjEntry, PureRangeKind, RelExpr};
use super::spine::{self, Spine, Top};
use super::{materialize, Demand, EmitPieces, ViewChain};
use crate::error::GnitzSqlError;
use crate::hir::physical::Frame;
use crate::ir::BExpr;
use crate::rules::reject_arity;

use gnitz_core::Schema;
use gnitz_expr::LogicalProgram;
use gnitz_wire::{AggDescriptor, AggFunc as WireAggFunc, GroupAggs, JoinKind};
use gnitz_wire::{Circuit, ColumnDef, NodeId, NullKeys, RangeRel, ReindexSlot, TypeCode};
use std::borrow::Cow;
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;

/// One branch an emitter hands the shell: its node over the join frame, and a mark
/// branch's BOOLEAN constant.
type Branch = (NodeId, Option<bool>);

/// The key a join step's output rows carry.
#[derive(Clone, Copy)]
enum OutKey {
    /// The equi join key: each match is made once, on the worker its operands meet on.
    JoinKey,
    /// Both sides' source PKs, computed after the join, away from the worker owning it.
    PairPk,
    /// The outer side's source PK; `owned` when every row was kept on that PK's
    /// owner before the join.
    OuterPk { owned: bool },
}

impl OutKey {
    /// Which sides pin their `pk_cols` to the front of the keep list.
    fn pins(self) -> [bool; 2] {
        match self {
            OutKey::JoinKey => [false, false],
            OutKey::PairPk => [true, true],
            OutKey::OuterPk { .. } => [true, false],
        }
    }

    /// Whether the output needs an exchange onto its own PK.
    fn exchanged(self) -> bool {
        matches!(self, OutKey::PairPk | OutKey::OuterPk { owned: false })
    }
}

/// The key a step of `kind` planned as `plan` emits its rows under.
fn out_key(plan: JoinPlan, kind: JoinType) -> OutKey {
    match plan {
        JoinPlan::Equi => OutKey::JoinKey,
        JoinPlan::Band(_) if kind.is_decorrelated() => OutKey::OuterPk { owned: false },
        JoinPlan::PureRange(_, PureRangeKind::Exists(_)) => OutKey::OuterPk { owned: true },
        JoinPlan::Band(_) | JoinPlan::PureRange(..) | JoinPlan::Cross => OutKey::PairPk,
    }
}

/// Lower the join of `left` and `right` with `above` over each of its branches to
/// circuit pieces, whose output leads with the join step's [`OutKey`] slots, then
/// the projected payload in item order.
pub(super) fn lower_join_view(
    chain: &mut ViewChain,
    above: Demand<'_>,
    [left, right]: [&Rc<RelExpr>; 2],
    kind: JoinType,
    class: &JoinClass,
    plan: JoinPlan,
) -> Result<EmitPieces, GnitzSqlError> {
    let mut refs = HashSet::new();
    above.refs(&mut refs);

    let mut cb = Circuit::default();
    let out_key = out_key(plan, kind);
    let sides = emit_join_inputs(chain, &mut cb, &refs, [left, right], kind, class, plan)?;

    let out_pk = match out_key {
        OutKey::JoinKey => join_pk_coldefs(class),
        OutKey::PairPk => {
            let surface = match plan {
                JoinPlan::Cross => "CROSS JOIN output PK",
                _ => "range JOIN output PK",
            };
            reject_arity(
                surface,
                sides[0].pk_arity + sides[1].pk_arity,
                gnitz_wire::PK_LIST_MAX_COLS,
            )?;
            pair_pk_coldefs(&sides[0].frame.schema, &sides[1].frame.schema)
        }
        OutKey::OuterPk { .. } => src_pk_coldefs(&sides[0].frame.schema),
    };

    if matches!(plan, JoinPlan::Band(_) | JoinPlan::PureRange(..)) {
        // No frame covers the range terms `[key region, A, B]`: the join frame's
        // key may be narrower than theirs.
        reject_arity(
            "range JOIN terms",
            class.arity() + sides[0].n() + sides[1].n(),
            gnitz_wire::MAX_COLUMNS,
        )?;
    }
    let branches = match plan {
        JoinPlan::Equi => emit_equi(&mut cb, class, kind, &sides),
        JoinPlan::Band(rel) => emit_band(&mut cb, class, rel, kind, &sides),
        JoinPlan::PureRange(rel, kind) => emit_pure_range(&mut cb, rel, kind, &sides),
        JoinPlan::Cross => emit_cross(&mut cb, &sides),
    };

    // The frame every branch filters and projects against.
    let frame = join_frame(out_pk, &sides, kind)?;
    let mut acc: Option<(NodeId, Frame)> = None;
    for (node, mark) in branches {
        let (preds, items): (Cow<'_, [HirExpr]>, Cow<'_, [ProjEntry]>) = match (kind, mark) {
            (JoinType::Mark(id), Some(val)) => (
                above.where_preds.iter().map(|e| subst_mark_lit(e, id, val)).collect(),
                above
                    .items
                    .iter()
                    .map(|it| ProjEntry {
                        expr: subst_mark_lit(&it.expr, id, val),
                        out: it.out.clone(),
                    })
                    .collect(),
            ),
            _ => (Cow::Borrowed(above.where_preds), Cow::Borrowed(above.items)),
        };
        let (node, out) = Demand { items: &items, where_preds: &preds }.emit(&mut cb, node, &frame)?;
        acc = Some(match acc {
            None => (node, out),
            Some((prior, out)) => (cb.union(node, prior), out),
        });
    }
    let (node, out) = acc.expect("a join step emits at least one branch");
    if out_key.exchanged() {
        cb.shard(node);
    }
    // A join key and a pair PK each identify a matched pair, not a row.
    let pk_repeats = match out_key {
        OutKey::JoinKey | OutKey::PairPk => true,
        OutKey::OuterPk { .. } => sides[0].pk_repeats,
    };
    Ok(EmitPieces { circuit: cb, out, pk_repeats })
}

/// A join's two inputs opened through the spine and emitted into `cb`, each
/// carrying the `refs` read above it and its own keys, and kept under [`join_keep`].
fn emit_join_inputs(
    chain: &mut ViewChain,
    cb: &mut Circuit,
    refs: &HashSet<ColId>,
    rels: [&Rc<RelExpr>; 2],
    kind: JoinType,
    class: &JoinClass,
    plan: JoinPlan,
) -> Result<[JoinSide; 2], GnitzSqlError> {
    let live = [0, 1].map(|i| -> HashSet<ColId> { refs.iter().copied().chain(class.key_cols(i)).collect() });
    let mut spines = [
        spine::open(chain, rels[0], &live[0])?,
        spine::open(chain, rels[1], &live[1])?,
    ];
    // A key the spine computes (`ON x.s = u.k` over `SELECT a + 1 AS s`) is no
    // column of the scanned relation, so nothing over that relation can state
    // where its delta scatters. Cut to a segment, the computed column is one.
    for i in 0..2 {
        if spines[i].source_cols(class.key_cols(i)).is_none() {
            spines[i] = Spine::segment(materialize(chain, rels[i], &live[i])?);
        }
    }
    // A join reads two distinct sources, one delta per epoch: a right side over the
    // left's source is re-read as a second relation. A side cut above reads a fresh
    // segment, so it collides with nothing.
    if spines[0].tid() == spines[1].tid() {
        spines[1] = Spine::segment(materialize(chain, rels[1], &live[1])?);
    }
    // Each outer row is decided on the worker owning its PK, the output key.
    let pk_placed = [matches!(out_key(plan, kind), OutKey::OuterPk { owned: true }), false];
    for (i, spine) in spines.iter_mut().enumerate() {
        let key = match pk_placed[i] {
            true => spine.pk_key(),
            // The equality slots the sides meet on; with none, the delta is broadcast.
            false => {
                let cols = spine
                    .source_cols(class.key_cols(i).take(class.eq.len()))
                    .ok_or_else(|| {
                        GnitzSqlError::Internal("a join key column is not a column of the relation it scatters".into())
                    })?;
                cols.into_iter()
                    .zip(class.key_types())
                    .map(|(c, ty)| (c, ty.tc))
                    .collect()
            }
        };
        spine.scatter(key);
    }
    let pk_repeats = spines.each_ref().map(Spine::pk_repeats);
    // A backfill feeds the sources in scan order, and the first is joined against
    // nothing: where one side alone outputs its unmatched rows, it goes second.
    let [l, r] = spines;
    let [(a, frame_l), (b, frame_r)] = match kind.emits_unmatched(0) && !kind.emits_unmatched(1) {
        true => {
            let b = r.emit(cb, Top::Slots)?;
            [l.emit(cb, Top::Slots)?, b]
        }
        false => [l.emit(cb, Top::Slots)?, r.emit(cb, Top::Slots)?],
    };
    let frames = [frame_l, frame_r];
    let [kept_l, kept_r] = join_keep(refs, class, kind, plan, &frames)?;
    let [frame_l, frame_r] = frames;
    let side = |i: usize, input, frame: Frame, (keep, pk_arity)| -> Result<JoinSide, GnitzSqlError> {
        let slots = frame.slots(class.key_cols(i))?;
        Ok(JoinSide {
            input,
            key: slots
                .into_iter()
                .zip(class.key_types())
                .map(|(c, t)| (c as u32, t.tc))
                .collect(),
            keep,
            pk_arity,
            unique: class.side_unique(rels[i], i),
            pk_repeats: pk_repeats[i],
            pk_placed: pk_placed[i],
            frame,
        })
    };
    Ok([side(0, a, frame_l, kept_l)?, side(1, b, frame_r, kept_r)?])
}

/// One side of a join as [`join_keep`] left it.
struct JoinSide {
    /// The node this side's input is emitted at.
    input: NodeId,
    frame: Frame,
    /// The reindex payload, in emission order.
    keep: Vec<u32>,
    /// The join key, [`JoinClass::key_cols`] order: each column as a `frame` slot,
    /// typed at its pair's common type.
    key: Vec<ReindexSlot>,
    /// The pinned source PK's arity — `0` when this shape packs no PK out of the
    /// payload.
    pk_arity: usize,
    /// Rows unique on this join's equality columns, so a row of the other side
    /// matches at most one.
    unique: bool,
    /// Whether two of this side's source rows may share their leading key.
    pk_repeats: bool,
    /// This side's delta is scattered by its source PK, so each row is on its
    /// PK's owner already.
    pk_placed: bool,
}

impl JoinSide {
    /// The kept payload width.
    fn n(&self) -> usize {
        self.keep.len()
    }

    /// The kept payload's `ColId`s, in keep order.
    fn ids(&self) -> impl Iterator<Item = Option<ColId>> + '_ {
        self.keep.iter().map(|&i| self.frame.layout[i as usize])
    }

    /// The kept payload columns' defs, in keep order.
    fn kept_defs(&self) -> impl Iterator<Item = &ColumnDef> + '_ {
        self.keep.iter().map(|&i| &self.frame.schema.columns()[i as usize])
    }

    /// The type codes of the kept payload columns — what `null_extend` needs to
    /// synthesize this side's NULL region.
    fn kept_type_codes(&self) -> Vec<TypeCode> {
        self.kept_defs().map(|c| c.ty.tc).collect()
    }
}

/// Each side's reindex payload under the keep rule — which source columns survive
/// into the join's traces, and so onto disk — and its pinned source PK's arity.
/// A keep list is the pinned PK, then every other kept column in source order: it
/// need not ascend, and the PK at the front is what makes every pair-PK slot list
/// a range.
fn join_keep(
    refs: &HashSet<ColId>,
    class: &JoinClass,
    kind: JoinType,
    plan: JoinPlan,
    frames: &[Frame; 2],
) -> Result<[(Vec<u32>, usize); 2], GnitzSqlError> {
    let mut keep = frames.each_ref().map(|f| vec![false; f.layout.len()]);
    // Rules 1 + 2: the projection and the WHERE over the join. The mark column is
    // in neither layout: the shell substitutes it per branch by its BOOLEAN constant.
    for &id in refs.iter().filter(|&&id| kind != JoinType::Mark(id)) {
        let side = usize::from(!frames[0].layout.contains(&Some(id)));
        keep[side][frames[side].slot(id)?] = true;
    }
    let pins = out_key(plan, kind).pins();
    let banded = matches!(plan, JoinPlan::Band(_) | JoinPlan::PureRange(..));
    for side in 0..2 {
        // Rule 3: a band or pure-range side with a ν keeps its key columns. A band
        // ν is keyed by the source PK, which a bag-valued side repeats; a pure
        // range re-keys its owned A slice onto the range column.
        if banded && kind.has_nu(side) {
            for pos in frames[side].slots(class.key_cols(side))? {
                keep[side][pos] = true;
            }
        }
        // Rule 4: a side whose source PK the output key packs out of the payload
        // keeps it, at the front.
        if pins[side] {
            for &c in frames[side].schema.pk_cols() {
                keep[side][c as usize] = true;
            }
        }
        // Rule 5, the fallback: a side with a ν needs an identity to subtract on. A
        // side without a ν keeps nothing — column 0 would split one trace element per
        // key into one per row.
        if kind.has_nu(side) && !keep[side].contains(&true) {
            keep[side][0] = true;
        }
    }
    // A join reading nothing from either side still emits rows.
    if !keep[0].contains(&true) && !keep[1].contains(&true) {
        keep[0][0] = true;
    }
    Ok([0, 1].map(|side| {
        let pinned: &[u32] = if pins[side] { frames[side].schema.pk_cols() } else { &[] };
        let rest = (0..keep[side].len() as u32).filter(|&i| keep[side][i as usize] && !pinned.contains(&i));
        (pinned.iter().copied().chain(rest).collect(), pinned.len())
    }))
}

/// Substitute `ColRef(mark_id)` with `LitBool(val)` throughout an expression —
/// the mark column's per-branch constant; every other leaf passes through.
fn subst_mark_lit(e: &HirExpr, mark_id: ColId, val: bool) -> HirExpr {
    e.rebuild(&mut |id| match *id == mark_id {
        true => BExpr::LitBool(val),
        false => BExpr::ColRef(*id),
    })
}

/// One half of the outer input split by match existence.
#[derive(Clone, Copy)]
enum Split {
    Matched(NodeId),
    Unmatched(NodeId),
}

/// `kind`'s branches, from one half of `all`; the other half is `all − known`.
fn exists_branches(cb: &mut Circuit, kind: ExistsKind, split: Split, all: NodeId) -> Vec<Branch> {
    let (Split::Matched(known) | Split::Unmatched(known)) = split;
    let complement = |cb: &mut Circuit| cb.difference(all, known);
    match (kind, split) {
        (ExistsKind::Semi, Split::Matched(half)) | (ExistsKind::Anti, Split::Unmatched(half)) => vec![(half, None)],
        (ExistsKind::Semi | ExistsKind::Anti, _) => vec![(complement(cb), None)],
        (ExistsKind::Mark(_), Split::Matched(matched)) => vec![(matched, Some(true)), (complement(cb), Some(false))],
        (ExistsKind::Mark(_), Split::Unmatched(unmatched)) => {
            vec![(complement(cb), Some(true)), (unmatched, Some(false))]
        }
    }
}

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

// ── Output-key PK columns ───────────────────────────────────────────────────────

/// The hidden `_join_pk` output PK columns of an equi join, one per key slot at
/// its pair's common type.
fn join_pk_coldefs(class: &JoinClass) -> Vec<ColumnDef> {
    let k = class.arity();
    class
        .key_types()
        .enumerate()
        .map(|(i, ty)| {
            let name: Arc<str> = if k == 1 {
                "_join_pk".into()
            } else {
                format!("_join_pk_{i}").into()
            };
            ColumnDef::new(name, ty, false).hidden()
        })
        .collect()
}

/// The output PK columns a re-key onto source PKs mints: one per listed
/// `(schema, pk column)`, at the column's own key slot type, non-nullable, and
/// hidden. The two producers below differ only in `name`.
fn rekey_pk_coldefs<'a>(
    cols: impl IntoIterator<Item = (&'a Schema, u32)>,
    name: impl Fn(usize, &ColumnDef) -> Arc<str>,
) -> Vec<ColumnDef> {
    cols.into_iter()
        .enumerate()
        .map(|(slot, (schema, c))| {
            let src = &schema.columns()[c as usize];
            let ty = src.ty.with_code(src.ty.tc.reindex_output_type());
            ColumnDef::new(name(slot, src), ty, false).hidden()
        })
        .collect()
}

/// The leading output PK columns of a range/band or cross join: A's then B's
/// source PK, `_pair_pk_{slot}`-numbered across both sides.
fn pair_pk_coldefs(left_schema: &Schema, right_schema: &Schema) -> Vec<ColumnDef> {
    rekey_pk_coldefs(
        left_schema
            .pk_cols()
            .iter()
            .map(|&c| (left_schema, c))
            .chain(right_schema.pk_cols().iter().map(|&c| (right_schema, c))),
        |slot, _| format!("_pair_pk_{slot}").into(),
    )
}

/// The output PK columns of a range-correlated EXISTS/IN view: the outer source
/// PK under its own names (it also rides the payload verbatim).
fn src_pk_coldefs(schema: &Schema) -> Vec<ColumnDef> {
    rekey_pk_coldefs(schema.pk_cols().iter().map(|&c| (schema, c)), |_, src| src.name.clone())
}

// ── Re-keys ─────────────────────────────────────────────────────────────────────

/// `(all, reindex)`: `side`'s input re-keyed onto its join key, `reindex` without
/// its NULL-keyed rows and `all` with them where `emits_unmatched`.
fn keyed_side(cb: &mut Circuit, side: &JoinSide, emits_unmatched: bool) -> (NodeId, NodeId) {
    let key = &side.key;
    let reindex = cb.map_reindex(side.input, key, &side.keep, NullKeys::Drop);
    // With no nullable key column the two re-keys keep the same rows.
    let nullable = key
        .iter()
        .any(|&(c, _)| side.frame.schema.columns()[c as usize].is_nullable);
    let all = match emits_unmatched && nullable {
        true => cb.map_reindex(side.input, key, &side.keep, NullKeys::Keep),
        false => reindex,
    };
    (all, reindex)
}

/// Re-key `node`, laid out `[lead…, each of `sides`' kept payload in order]`, onto
/// those sides' pinned source PKs — each at the front of its kept payload —
/// keeping the payloads and dropping the lead region.
fn rekey_pinned(cb: &mut Circuit, node: NodeId, lead: usize, sides: &[JoinSide]) -> NodeId {
    let (mut key, mut at) = (Vec::new(), lead);
    for s in sides {
        let pinned = &s.keep[..s.pk_arity];
        key.extend(pinned.iter().enumerate().map(|(j, &c)| {
            let tc = s.frame.schema.columns()[c as usize].ty.tc;
            ((at + j) as u32, tc.reindex_output_type())
        }));
        at += s.n();
    }
    let keep: Vec<u32> = (lead as u32..at as u32).collect();
    cb.map_reindex(node, &key, &keep, NullKeys::Keep)
}

/// Re-key `side`'s input onto its source's PK.
fn rekey_on_source_pk(cb: &mut Circuit, side: &JoinSide) -> NodeId {
    cb.map_reindex(
        side.input,
        &spine::pk_slots(&side.frame.schema),
        &side.keep,
        NullKeys::Keep,
    )
}

/// A range/band outer join's null-fill branch: `ν_P` of the preserved side
/// `sides[p]`, the other side's half all-NULL, keyed on the pair-PK like the inner terms.
fn emit_range_null_fill_tail(cb: &mut Circuit, sides: &[JoinSide; 2], nf_keyed: NodeId, p: usize) -> NodeId {
    // [P.pk × P.pk_arity, A, B], one of the two payload halves all-NULL.
    let nullfill = cb.null_extend(nf_keyed, &sides[1 - p].kept_type_codes(), p == 1);
    rekey_pinned(cb, nullfill, sides[p].pk_arity, sides)
}

// ── Equi emission ───────────────────────────────────────────────────────────────

/// `inner ∪ ν_A ∪ ν_B`, a union over each preserved side's null-fill; a
/// decorrelated kind's branches split A by whether it matched B's key set.
fn emit_equi(cb: &mut Circuit, class: &JoinClass, kind: JoinType, sides: &[JoinSide; 2]) -> Vec<Branch> {
    let k = class.arity();
    let keyed = |cb: &mut Circuit, i: usize| keyed_side(cb, &sides[i], kind.emits_unmatched(i));
    let (all_b, reindex_b) = keyed(cb, 1);
    let (all_a, reindex_a) = keyed(cb, 0);
    let all = [all_a, all_b];
    // A decorrelated join asks only whether a key exists: B as a set weighs every
    // match 1, so the joined A rows are the matched set at their own weight.
    let b_delta = match kind.is_decorrelated() && !sides[1].unique {
        true => cb.distinct(reindex_b),
        false => reindex_b,
    };
    // The re-key each side's delta joins through, and whose integral the other
    // side's delta probes; the symmetric 2-term join over `[_join_pk, kept-A, kept-B]`.
    let reindex = [reindex_a, b_delta];
    let inner = cb.join(reindex, reindex, JoinKind::Equi);
    if let Some(e) = kind.exists() {
        return exists_branches(cb, e, Split::Matched(inner), all[0]);
    }
    // Each `[_join_pk, A, B]`, the other side's half all-NULL.
    let nus: Vec<NodeId> = (0..2)
        .filter(|&p| kind.preserves(p))
        .map(|p| {
            let o = 1 - p;
            let p0 = k + if p == 0 { 0 } else { sides[0].n() };
            let proj: Vec<u32> = (p0..p0 + sides[p].n()).map(|c| c as u32).collect();
            // P's keyed rows that some O row matches, each at its own weight
            // `w_P · [S > 0]`, laid out like `all[p]`. A side unique on the key
            // matches each P row at most once, so π_P(inner) = [_join_pk, P] is
            // that; otherwise P's reindex is joined against O's key set.
            let matched = match sides[o].unique {
                true => {
                    let slots = sides[p].kept_defs().map(|c| (c.ty.tc, c.is_nullable)).collect();
                    cb.map_expr(inner, LogicalProgram::copy_cols(&proj).to_map(slots))
                }
                false => {
                    let keys = cb.map_expr(reindex[o], LogicalProgram::copy_cols(&[]).to_map(Vec::new()));
                    let mut deltas = reindex;
                    deltas[o] = cb.distinct(keys);
                    cb.join(deltas, deltas, JoinKind::Equi)
                }
            };
            let nu_p = cb.difference(all[p], matched);
            // A side that keeps nothing contributes no NULL region at all.
            let o_tcs = sides[o].kept_type_codes();
            match o_tcs.is_empty() {
                true => nu_p,
                false => cb.null_extend(nu_p, &o_tcs, p == 1),
            }
        })
        .collect();
    let node = nus.into_iter().fold(inner, |merged, nu| cb.union(nu, merged));
    vec![(node, None)]
}

// ── Range / band emission ───────────────────────────────────────────────────────

/// A band join over both sides re-keyed on `[eq slots…, range slot]`, NULL keys
/// dropped: the pair terms and each preserved side's null-fill; a decorrelated
/// kind's branches split A by its ν.
fn emit_band(cb: &mut Circuit, class: &JoinClass, op: RangeRel, kind: JoinType, sides: &[JoinSide; 2]) -> Vec<Branch> {
    let k = class.arity();
    let reindex = [range_keyed(cb, &sides[0]), range_keyed(cb, &sides[1])];
    // The two range terms over `[key region, A, B]`, each carrying the ON clause's
    // left-to-right range relation verbatim.
    let merged = cb.join(reindex, reindex, JoinKind::Range { rel: op });
    // `(P_all, ν_P)`, P = `sides[p]`: P's input and its rows no pair matches, both
    // keyed on P's source PK.
    let nu = |cb: &mut Circuit, p: usize| -> (NodeId, NodeId) {
        let base = k + if p == 0 { 0 } else { sides[0].n() };
        let pi = rekey_pinned(cb, merged, base, std::slice::from_ref(&sides[p]));
        let all = rekey_on_source_pk(cb, &sides[p]);
        // `π` is P's matched multiplicity, which only a side unique on the
        // equality key bounds by `all`; otherwise the difference is clamped at 0.
        let nu = match sides[1 - p].unique {
            true => cb.difference(all, pi),
            false => cb.positive_diff(all, pi),
        };
        (all, nu)
    };
    if let Some(e) = kind.exists() {
        // ν over A, keyed on A's source PK behind the output exchange.
        let (all, nu_a) = nu(cb, 0);
        return exists_branches(cb, e, Split::Unmatched(nu_a), all);
    }
    // The pairs re-keyed onto `_pair_pk`, dropping the key region: every branch
    // lands on `[pair-PK, kept-A, kept-B]`.
    let mut acc = rekey_pinned(cb, merged, k, sides);
    for p in (0..2).filter(|&p| kind.preserves(p)) {
        let (_, nu_p) = nu(cb, p);
        let branch = emit_range_null_fill_tail(cb, sides, nu_p, p);
        acc = cb.union(branch, acc);
    }
    vec![(acc, None)]
}

/// A pure-range join, keyed on the range slot alone. Its pair terms integrate
/// each worker's own slice of them: a whole integral against the broadcast delta would
/// emit each pair once per worker. A's ν is `A_owned − matched` against the
/// one-row [`threshold`].
fn emit_pure_range(cb: &mut Circuit, op: RangeRel, kind: PureRangeKind, sides: &[JoinSide; 2]) -> Vec<Branch> {
    let rel = JoinKind::Range { rel: op };
    match kind {
        // The threshold decides existence over A's owned slice, already on the
        // worker owning its output key.
        PureRangeKind::Exists(e) => {
            let (owned, int_a) = own_a(cb, &sides[0]);
            let reindex_b = range_keyed(cb, &sides[1]);
            let matched = threshold(cb, &sides[0], reindex_b, int_a, op);
            exists_branches(cb, e, Split::Matched(matched), owned)
        }
        // Each branch below lands on `[pair-PK, kept-A, kept-B]`; a LEFT join's
        // pair terms integrate A's owned slice, which its ν subtracts from.
        PureRangeKind::Inner | PureRangeKind::Left => {
            let reindex_a = range_keyed(cb, &sides[0]);
            let owned = (kind == PureRangeKind::Left).then(|| own_a(cb, &sides[0]));
            let int_a = match owned {
                Some((_, int_a)) => int_a,
                None => cb.shard(reindex_a),
            };
            let reindex_b = range_keyed(cb, &sides[1]);
            let int_b = cb.shard(reindex_b);
            let merged = cb.join([reindex_a, reindex_b], [int_a, int_b], rel);
            let pairs = rekey_pinned(cb, merged, 1, sides);
            let Some((owned, int_a)) = owned else {
                return vec![(pairs, None)];
            };
            let matched = threshold(cb, &sides[0], reindex_b, int_a, op);
            let nu_a = cb.difference(owned, matched);
            let branch = emit_range_null_fill_tail(cb, sides, nu_a, 0);
            vec![(cb.union(branch, pairs), None)]
        }
    }
}

/// `side`'s input re-keyed onto a range join's key, NULL keys dropped.
fn range_keyed(cb: &mut Circuit, side: &JoinSide) -> NodeId {
    keyed_side(cb, side, false).1
}

/// The rows of A's range-keyed slice `int_a` matching the one-row threshold
/// `m = MAX/MIN(b.range)` under `op`, keyed on `[a.pk…, A]` like A's owned slice.
fn threshold(cb: &mut Circuit, side_a: &JoinSide, reindex_b: NodeId, int_a: NodeId, op: RangeRel) -> NodeId {
    let agg_func = match op.bounds_below() {
        true => WireAggFunc::Min,
        false => WireAggFunc::Max,
    };
    // B's range keys alone: rows sharing one consolidate before the MIN/MAX reads it.
    let keys = cb.map_expr(reindex_b, LogicalProgram::copy_cols(&[]).to_map(Vec::new()));
    // Local over the broadcast B, so every worker holds the same extremum. No
    // ground row: a `m = NULL` seed would break `A − 0 = A` over an empty B.
    // The COUNT is the cardinality gate every reduce carries; the reindex
    // below keeps no payload, so it goes no further.
    let agg = GroupAggs {
        group_cols: vec![],
        aggs: vec![
            AggDescriptor { agg_op: agg_func, col_idx: 0 },
            AggDescriptor::COUNT_STAR,
        ],
    };
    // [_group_pk:U128, m:Tc, count]
    let red = cb.reduce_local(keys, agg);
    // `m` is a MIN/MAX of B's non-NULL range keys, so never NULL, and is typed as
    // the range pair's common type, which A's range key also carries.
    let reindex_m = cb.map_reindex(red, &[(1, side_a.key[0].1)], &[], NullKeys::Keep);
    // `m` carries no payload, so both terms are `[_join_pk, A]`.
    let deltas = [int_a, reindex_m];
    let matched = cb.join(deltas, deltas, JoinKind::Range { rel: op });
    rekey_pinned(cb, matched, 1, std::slice::from_ref(side_a))
}

/// `(owned, int_a)` for a pure range: A's rows re-keyed onto their source PK and
/// kept on its owner, and that slice re-keyed onto the range column, NULL range
/// keys dropped. Unless A's delta is placed by its PK, it is broadcast, and an
/// exchange keeps each worker its own.
fn own_a(cb: &mut Circuit, side: &JoinSide) -> (NodeId, NodeId) {
    let owned = rekey_on_source_pk(cb, side);
    // [a.pk, kept A]
    let owned = match side.pk_placed {
        true => owned,
        false => cb.shard(owned),
    };
    let &[(range_slot, range_tc)] = side.key.as_slice() else {
        unreachable!("a pure range keys on its range column alone")
    };
    let at = side.keep.iter().position(|&c| c == range_slot);
    let at = side.pk_arity + at.expect("the keep rule keeps a pure range's range column");
    let keep: Vec<u32> = (side.pk_arity as u32..(side.pk_arity + side.n()) as u32).collect();
    // [range key, kept A]
    let int_a = cb.map_reindex(owned, &[(at as u32, range_tc)], &keep, NullKeys::Drop);
    (owned, int_a)
}

// ── Cross emission ──────────────────────────────────────────────────────────────

/// The keyless join `A × B`, INNER only. Each side is keyed on its own source PK,
/// which takes no part in the match — it only partitions the integral the
/// broadcast delta is paired against.
fn emit_cross(cb: &mut Circuit, sides: &[JoinSide; 2]) -> Vec<Branch> {
    let reindex_a = rekey_on_source_pk(cb, &sides[0]);
    let reindex_b = rekey_on_source_pk(cb, &sides[1]);
    let int_a = cb.shard(reindex_a);
    let int_b = cb.shard(reindex_b);
    // A keyless term keys on `[left PK…, right PK…]`, which is the pair-PK itself,
    // so both terms already share one schema.
    let inner = cb.join([reindex_a, reindex_b], [int_a, int_b], JoinKind::Cross);
    vec![(inner, None)] // [pair-PK, A, B]
}

#[cfg(test)]
#[path = "tests/join.rs"]
mod tests;
