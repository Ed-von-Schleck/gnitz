//! The one join shell: every `Join` step, decorrelated kinds included, its output
//! key chosen by [`out_key`]; the output-key PK columns it mints; the re-keys onto
//! join keys and source PKs; and the equi, range/band and cross emitters over them.

use super::super::{ColSet, ExistsKind, JoinClass, JoinPlan, JoinType, RelExpr};
use super::spine::{self, Head, Peeled, Spine};
use super::{EmitPieces, ViewChain};
use crate::error::GnitzSqlError;
use crate::hir::physical::{Frame, Relabel};
use crate::ir::BExpr;
use crate::rules::reject_arity;

use gnitz_core::Schema;
use gnitz_expr::LogicalProgram;
use gnitz_wire::{AggDescriptor, AggFunc as WireAggFunc, GroupAggs, JoinKind, ReduceKind};
use gnitz_wire::{Circuit, ColumnDef, NodeId, NullKeys, RangeRel, ReindexSlot, TypeCode};
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

/// The key a step of `kind` planned as `plan` emits its rows under.
fn out_key(plan: JoinPlan, kind: JoinType) -> OutKey {
    match plan {
        JoinPlan::Equi => OutKey::JoinKey,
        JoinPlan::Band(_) if matches!(kind, JoinType::Exists(_)) => OutKey::OuterPk { owned: false },
        JoinPlan::PureRange(_) if matches!(kind, JoinType::Exists(_)) => OutKey::OuterPk { owned: true },
        JoinPlan::Band(_) | JoinPlan::PureRange(..) | JoinPlan::Cross => OutKey::PairPk,
    }
}

/// `schema`'s PK as a reindex key: each column at its key slot's type.
fn pk_slots(schema: &Schema) -> Vec<ReindexSlot> {
    (schema.pk_cols().iter())
        .map(|&c| (c, schema.columns()[c as usize].ty.tc.reindex_output_type()))
        .collect()
}

/// Lower the join of `left` and `right` with `above` over each of its branches to
/// circuit pieces, whose output leads with the join step's [`OutKey`] slots, then
/// the projected payload in item order.
pub(super) fn lower_join_view(
    chain: &mut ViewChain,
    above: Peeled<'_>,
    [left, right]: [&Rc<RelExpr>; 2],
    kind: JoinType,
    class: &JoinClass,
) -> Result<EmitPieces, GnitzSqlError> {
    let plan = class.plan();
    let out_key = out_key(plan, kind);
    let mut cb = Circuit::default();
    let sides = emit_join_inputs(chain, &mut cb, &above.read, [left, right], kind, class, out_key)?;

    // The output PK columns: hidden, non-nullable, a source PK column re-keyed onto
    // at its key slot type.
    let rekeyed = |i: usize| {
        let s: &JoinSide = &sides[i];
        (s.frame.schema.hidden_key_columns().zip(&s.pk)).map(|(c, &(_, tc))| ColumnDef { ty: c.ty.with_code(tc), ..c })
    };
    let out_pk: Vec<ColumnDef> = match out_key {
        // One per key slot at its pair's common type.
        OutKey::JoinKey => {
            let named = |i| -> Arc<str> {
                match class.arity() {
                    1 => "_join_pk".into(),
                    _ => format!("_join_pk_{i}").into(),
                }
            };
            (class.keys().enumerate())
                .map(|(i, p)| ColumnDef::new(named(i), p.ty, false).hidden())
                .collect()
        }
        // A's then B's source PK, numbered across both sides.
        OutKey::PairPk => {
            let surface = match plan {
                JoinPlan::Cross => "CROSS JOIN output PK",
                _ => "range JOIN output PK",
            };
            reject_arity(
                surface,
                sides[0].pk.len() + sides[1].pk.len(),
                gnitz_wire::PK_LIST_MAX_COLS,
            )?;
            (rekeyed(0).chain(rekeyed(1)).enumerate())
                .map(|(i, c)| ColumnDef {
                    name: format!("_pair_pk_{i}").into(),
                    ..c
                })
                .collect()
        }
        // The outer source PK under its own names: it also rides the payload verbatim.
        OutKey::OuterPk { .. } => rekeyed(0).collect(),
    };

    if class.range.is_some() {
        // No frame covers the range terms `[key region, A, B]`: the join frame's
        // key may be narrower than theirs.
        reject_arity(
            "range JOIN terms",
            class.arity() + sides[0].keep.len() + sides[1].keep.len(),
            gnitz_wire::MAX_COLUMNS,
        )?;
    }
    let k = class.arity();
    let Terms { pairs, splits } = match plan {
        JoinPlan::Equi => emit_equi(&mut cb, k, kind, &sides),
        JoinPlan::Band(rel) => emit_band(&mut cb, k, rel, kind, &sides),
        JoinPlan::PureRange(rel) => emit_pure_range(&mut cb, rel, kind, &sides),
        // The keyless join `A × B`, INNER only. Each side is keyed on its own source
        // PK, which takes no part in the match; a keyless term keys on `[left PK…,
        // right PK…]`, which is the pair-PK itself, so both terms already share one
        // schema: `[pair-PK, A, B]`.
        JoinPlan::Cross => {
            let deltas = [sides[0].on_pk(&mut cb), sides[1].on_pk(&mut cb)];
            let traces = integrals(&mut cb, &sides, deltas);
            let pairs = cb.join(deltas, traces, JoinKind::Cross);
            Terms { pairs: Some(pairs), splits: [None, None] }
        }
    };
    // A decorrelated kind's branches split A by match existence. An outer kind's
    // is the pairs and each preserved side's ν, the other side's half all-NULL —
    // none where it keeps nothing — keyed like the pairs.
    let branches = match kind {
        JoinType::Exists(e) => exists_branches(&mut cb, e, splits[0].expect("a decorrelated kind splits A")),
        _ => {
            let mut acc = pairs.expect("a joining kind pairs its sides");
            for p in (0..2).filter(|&p| kind.preserves(p)) {
                let split = splits[p].expect("a preserved side has a ν");
                let nu = split.unmatched(&mut cb);
                let o_tcs: Vec<TypeCode> = sides[1 - p].kept_defs().map(|c| c.ty.tc).collect();
                let mut fill = match o_tcs.is_empty() {
                    true => nu,
                    false => cb.null_extend(nu, &o_tcs, p == 1),
                };
                if matches!(out_key, OutKey::PairPk) {
                    fill = rekey_pinned(&mut cb, fill, sides[p].pk.len(), &sides);
                }
                acc = cb.union(fill, acc);
            }
            vec![(acc, None)]
        }
    };

    // The frame every branch filters and projects against: the output PK, then both
    // sides' kept payload with the null-providing side widened per `kind`, through
    // the same `JoinType::widen_sides` the logical join output uses.
    let [mut l, mut r] = sides.each_ref().map(|s| s.kept_defs().cloned().collect::<Vec<_>>());
    kind.widen_sides(l.iter_mut(), r.iter_mut());
    let ids = (sides.iter()).flat_map(|s| s.keep.iter().map(|&i| s.frame.layout[i as usize]));
    let frame = Frame::keyed(out_pk, ids.zip(l.into_iter().chain(r)))?;

    let mut acc: Option<(NodeId, Frame)> = None;
    for (node, mark) in branches {
        let marked: Vec<_>;
        let levels = match (kind, mark) {
            // The mark column is in no layout: each branch reads it as its constant.
            (JoinType::Exists(ExistsKind::Mark(id)), Some(val)) => {
                let val = BExpr::LitBool(val);
                marked = (above.levels.iter())
                    .map(|(level, read)| (level.substituted(id, &val), read.clone()))
                    .collect();
                &marked
            }
            _ => &above.levels,
        };
        let (node, out) = spine::emit_levels(&mut cb, levels, Relabel::Whole, frame.clone(), Head::Node(node))?;
        acc = Some(match acc {
            None => (node, out),
            Some((prior, out)) => (cb.union(node, prior), out),
        });
    }
    let (node, out) = acc.expect("a join step emits at least one branch");
    // An output keyed by A's PK leading is made on that PK's owner when A's delta
    // is placed there.
    if !matches!(out_key, OutKey::JoinKey) && !sides[0].pk_placed {
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
    refs: &ColSet,
    rels: [&Rc<RelExpr>; 2],
    kind: JoinType,
    class: &JoinClass,
    out_key: OutKey,
) -> Result<[JoinSide; 2], GnitzSqlError> {
    let live = [0, 1].map(|i| -> ColSet { refs.iter().chain(class.key_cols(i)).collect() });
    let mut spines = [
        spine::peel(chain, rels[0], &live[0]).open(chain)?,
        spine::peel(chain, rels[1], &live[1]).open(chain)?,
    ];
    // Each outer row is decided on the worker owning its PK, the output key. An
    // INNER step with no equality slot against a side of at most one row — unique
    // on no columns — keeps the other side's delta on its PK owner: only the
    // one-row side is broadcast.
    let unique = [0, 1].map(|i| class.side_unique(rels[i], i));
    let one_row = match kind == JoinType::Inner && class.eq.is_empty() {
        true => [unique[1] && !unique[0], unique[0] && !unique[1]],
        false => [false, false],
    };
    let pk_placed = [
        matches!(out_key, OutKey::OuterPk { owned: true }) || one_row[0],
        one_row[1],
    ];
    // The key each side's delta is co-located by ahead of the circuit: its source
    // PK, or the equality slots the sides meet on — broadcast by none — each the
    // column of the opened relation it copies; a computed one states no key.
    let scatter = |spine: &Spine<'_>, i: usize| match pk_placed[i] {
        true => Some(pk_slots(&spine.seg.frame.schema)),
        false => (class.eq.iter())
            .map(|p| Some((spine.source_slot(p.cols[i])?, p.ty.tc)))
            .collect(),
    };
    // A scatter key the spine computes (`ON x.s = u.k` over `SELECT a + 1 AS s`)
    // is no column of the scanned relation, so nothing over that relation can state
    // where its delta scatters. Cut to a segment, the computed column is one. A join
    // reads two distinct sources, one delta per epoch: a right side over the left's
    // source is re-read as a second relation. A side cut here reads a fresh segment,
    // so it collides with nothing.
    let mut keys: [Vec<ReindexSlot>; 2] = Default::default();
    for i in 0..2 {
        let collides = i == 1 && spines[0].seg.tid == spines[1].seg.tid;
        keys[i] = match scatter(&spines[i], i).filter(|_| !collides) {
            Some(key) => key,
            None => {
                let seg = chain.materialize(rels[i], &live[i])?;
                spines[i] = Spine { seg, levels: Vec::new() };
                scatter(&spines[i], i)
                    .ok_or_else(|| GnitzSqlError::Internal("a segment states no scatter key".into()))?
            }
        };
    }
    let [s0, s1] = keys;
    let pk_repeats = spines.each_ref().map(|s| s.seg.pk_repeats);
    let reads_clock = spines.each_ref().map(|s| s.seg.tid == gnitz_wire::CLOCK.id);
    // A backfill feeds the sources in scan order, and the first is joined against
    // nothing: where one side alone outputs its unmatched rows, it goes second,
    // and so does a side the clock floors, so no row under the floor enters it.
    let [l, r] = spines;
    let [(a, frame_l), (b, frame_r)] = match kind.emits_unmatched(0) && !kind.emits_unmatched(1) || reads_clock[1] {
        true => {
            let b = r.emit(cb, Relabel::Slots, Some(s1))?;
            [l.emit(cb, Relabel::Slots, Some(s0))?, b]
        }
        false => [
            l.emit(cb, Relabel::Slots, Some(s0))?,
            r.emit(cb, Relabel::Slots, Some(s1))?,
        ],
    };
    let [kept_l, kept_r] = join_keep(refs, class, kind, out_key, [&frame_l, &frame_r])?;
    let side = |i: usize, input, frame: Frame, keep: Vec<u32>| -> Result<JoinSide, GnitzSqlError> {
        Ok(JoinSide {
            input,
            key: (class.keys())
                .map(|p| Ok((frame.slot(p.cols[i])? as u32, p.ty.tc)))
                .collect::<Result<_, GnitzSqlError>>()?,
            pk: pk_slots(&frame.schema),
            unique: unique[i],
            pk_repeats: pk_repeats[i],
            pk_placed: pk_placed[i],
            clock: reads_clock[i] && keep.iter().all(|c| frame.schema.pk_cols().contains(c)),
            keep,
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
    /// The source PK as a reindex key, each column at its key slot's type. A side
    /// the output key packs a PK out of keeps it at the front of `keep`.
    pk: Vec<ReindexSlot>,
    /// Rows unique on this join's equality columns, so a row of the other side
    /// matches at most one.
    unique: bool,
    /// Whether two of this side's source rows may share their leading key.
    pk_repeats: bool,
    /// This side's delta is scattered by its source PK, so each row is on its
    /// PK's owner already.
    pk_placed: bool,
    /// This side is `_system.clock` read in place, keeping no column but its
    /// constant PK: nothing above the join reads which time a row met.
    clock: bool,
}

impl JoinSide {
    /// The kept payload columns' defs, in keep order.
    fn kept_defs(&self) -> impl Iterator<Item = &ColumnDef> {
        self.keep.iter().map(|&i| &self.frame.schema.columns()[i as usize])
    }

    /// This side's input re-keyed onto its join key, carrying the kept payload.
    fn on_key(&self, cb: &mut Circuit, nulls: NullKeys) -> NodeId {
        cb.map_reindex(self.input, &self.key, &self.keep, nulls)
    }

    /// This side's input re-keyed onto its source's PK, carrying the kept payload.
    fn on_pk(&self, cb: &mut Circuit) -> NodeId {
        cb.map_reindex(self.input, &self.pk, &self.keep, NullKeys::Keep)
    }
}

/// Each side's reindex payload under the keep rule — which source columns survive
/// into the join's traces, and so onto disk.
/// A keep list is the pinned PK, then every other kept column in source order: it
/// need not ascend, and the PK at the front is what makes every pair-PK slot list
/// a range.
fn join_keep(
    refs: &ColSet,
    class: &JoinClass,
    kind: JoinType,
    out_key: OutKey,
    frames: [&Frame; 2],
) -> Result<[Vec<u32>; 2], GnitzSqlError> {
    let mut keep = frames.each_ref().map(|f| vec![false; f.layout.len()]);
    // Rules 1 + 2: the projection and the WHERE over the join. The mark column is
    // in neither layout: the shell substitutes it per branch by its BOOLEAN constant.
    for id in refs.iter().filter(|&id| kind != JoinType::Exists(ExistsKind::Mark(id))) {
        let side = usize::from(!frames[0].layout.contains(&Some(id)));
        keep[side][frames[side].slot(id)?] = true;
    }
    // Which sides the output key packs a source PK out of.
    let pins = match out_key {
        OutKey::JoinKey => [false, false],
        OutKey::PairPk => [true, true],
        OutKey::OuterPk { .. } => [true, false],
    };
    let banded = class.range.is_some();
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
    }
    Ok([0, 1].map(|side| {
        let pinned: &[u32] = if pins[side] { frames[side].schema.pk_cols() } else { &[] };
        let rest = (0..keep[side].len() as u32).filter(|&i| keep[side][i as usize] && !pinned.contains(&i));
        pinned.iter().copied().chain(rest).collect()
    }))
}

/// What an emitter hands the shell: the pair terms, which a decorrelated kind
/// has none of, and the split of each side [`JoinType::has_nu`] gives a ν.
struct Terms {
    pairs: Option<NodeId>,
    splits: [Option<Split>; 2],
}

/// A side's rows `all` split by match existence: `known` is the matched half
/// where `matched`, else the unmatched one; the other half is `all − known`.
#[derive(Clone, Copy)]
struct Split {
    all: NodeId,
    known: NodeId,
    matched: bool,
}

impl Split {
    fn half(self, cb: &mut Circuit, matched: bool) -> NodeId {
        match matched == self.matched {
            true => self.known,
            false => cb.difference(self.all, self.known),
        }
    }

    /// The ν: the rows no row of the other side matches.
    fn unmatched(self, cb: &mut Circuit) -> NodeId {
        self.half(cb, false)
    }
}

/// `kind`'s branches from A's split.
fn exists_branches(cb: &mut Circuit, kind: ExistsKind, split: Split) -> Vec<Branch> {
    match kind {
        ExistsKind::Semi => vec![(split.half(cb, true), None)],
        ExistsKind::Anti => vec![(split.unmatched(cb), None)],
        ExistsKind::Mark(_) => {
            let yes = split.half(cb, true);
            vec![(yes, Some(true)), (split.unmatched(cb), Some(false))]
        }
    }
}

// ── Re-keys ─────────────────────────────────────────────────────────────────────

/// Re-key `node`, laid out `[lead…, each of `sides`' kept payload in order]`, onto
/// those sides' pinned source PKs — each at the front of its kept payload —
/// keeping the payloads and dropping the lead region.
fn rekey_pinned(cb: &mut Circuit, node: NodeId, lead: usize, sides: &[JoinSide]) -> NodeId {
    let (mut key, mut at) = (Vec::new(), lead);
    for s in sides {
        key.extend(s.pk.iter().enumerate().map(|(j, &(_, tc))| ((at + j) as u32, tc)));
        at += s.keep.len();
    }
    let keep: Vec<u32> = (lead as u32..at as u32).collect();
    cb.map_reindex(node, &key, &keep, NullKeys::Keep)
}

// ── Equi emission ───────────────────────────────────────────────────────────────

/// The symmetric 2-term join over `[_join_pk, kept-A, kept-B]`, and each side
/// with a ν split by whether the other side's key set matches it.
fn emit_equi(cb: &mut Circuit, k: usize, kind: JoinType, sides: &[JoinSide; 2]) -> Terms {
    // `(all, reindex)`: a side re-keyed onto its join key, `reindex` without its
    // NULL-keyed rows and `all` with them where it emits its unmatched rows. With no
    // nullable key column the two re-keys keep the same rows.
    let keyed = |cb: &mut Circuit, i: usize| {
        let s = &sides[i];
        let reindex = s.on_key(cb, NullKeys::Drop);
        let nullable = (s.key.iter()).any(|&(c, _)| s.frame.schema.columns()[c as usize].is_nullable);
        let all = match kind.emits_unmatched(i) && nullable {
            true => s.on_key(cb, NullKeys::Keep),
            false => reindex,
        };
        (all, reindex)
    };
    let (all_b, reindex_b) = keyed(cb, 1);
    let (all_a, reindex_a) = keyed(cb, 0);
    let all = [all_a, all_b];
    let reindex = [reindex_a, reindex_b];
    // The re-key each side's delta joins through, and whose integral the other
    // side's delta probes. A decorrelated kind reads it only as A's matched rows
    // against a B unique on the key.
    let exists = matches!(kind, JoinType::Exists(_));
    let inner = (!exists || sides[1].unique).then(|| cb.join(reindex, reindex, JoinKind::Equi));
    let mut splits = [None, None];
    for p in (0..2).filter(|&p| kind.has_nu(p)) {
        let o = 1 - p;
        // P's keyed rows that some O row matches, each at its own weight
        // `w_P · [S > 0]`, laid out like `all[p]`. A side unique on the key
        // matches each P row at most once, so π_P(inner) = [_join_pk, P] is
        // that; otherwise P's reindex is joined against O's key set. Over an O
        // that keeps nothing, both projections are the identity.
        let bare = sides[o].keep.is_empty();
        let matched = match inner.filter(|_| sides[o].unique) {
            Some(inner) if bare => inner,
            Some(inner) => {
                let p0 = k + if p == 0 { 0 } else { sides[0].keep.len() };
                let proj: Vec<u32> = (p0..p0 + sides[p].keep.len()).map(|c| c as u32).collect();
                let slots = sides[p].kept_defs().map(|c| (c.ty.tc, c.is_nullable)).collect();
                cb.map_expr(inner, LogicalProgram::copy_cols(&proj).to_map(slots))
            }
            None => {
                let keys = match bare {
                    true => reindex[o],
                    false => cb.map_expr(reindex[o], LogicalProgram::copy_cols(&[]).to_map(Vec::new())),
                };
                let mut deltas = reindex;
                deltas[o] = cb.distinct(keys);
                cb.join(deltas, deltas, JoinKind::Equi)
            }
        };
        splits[p] = Some(Split {
            all: all[p],
            known: matched,
            matched: true,
        });
    }
    Terms { pairs: inner.filter(|_| !exists), splits }
}

// ── Range / band emission ───────────────────────────────────────────────────────

/// A band join over both sides re-keyed on `[eq slots…, range slot]`, NULL keys
/// dropped: the pair terms, and each side with a ν split by it, keyed on its
/// source PK.
fn emit_band(cb: &mut Circuit, k: usize, op: RangeRel, kind: JoinType, sides: &[JoinSide; 2]) -> Terms {
    let reindex = [sides[0].on_key(cb, NullKeys::Drop), sides[1].on_key(cb, NullKeys::Drop)];
    // The two range terms over `[key region, A, B]`, each carrying the ON clause's
    // left-to-right range relation verbatim.
    let merged = cb.join(reindex, reindex, JoinKind::range(op));
    // P = `sides[p]`: P's input and its rows no pair matches, both keyed on P's
    // source PK.
    let split = |cb: &mut Circuit, p: usize| {
        let base = k + if p == 0 { 0 } else { sides[0].keep.len() };
        let pi = rekey_pinned(cb, merged, base, std::slice::from_ref(&sides[p]));
        let all = sides[p].on_pk(cb);
        // `π` is P's matched multiplicity, which only a side unique on the
        // equality key bounds by `all`; otherwise the difference is clamped at 0.
        let nu = match sides[1 - p].unique {
            true => cb.difference(all, pi),
            false => cb.positive_diff(all, pi),
        };
        Split { all, known: nu, matched: false }
    };
    // The pairs re-keyed onto `_pair_pk`, dropping the key region: every branch
    // lands on `[pair-PK, kept-A, kept-B]`.
    let pairs = (!matches!(kind, JoinType::Exists(_))).then(|| rekey_pinned(cb, merged, k, sides));
    Terms {
        pairs,
        splits: [0, 1].map(|p| kind.has_nu(p).then(|| split(cb, p))),
    }
}

/// A pure-range join, keyed on the range slot alone. Its pair terms integrate
/// as [`integrals`] places them, or, LEFT, A's owned slice and each worker's own
/// slice of B. A splits by the rows of its owned slice the one-row [`threshold`]
/// matches.
fn emit_pure_range(cb: &mut Circuit, op: RangeRel, kind: JoinType, sides: &[JoinSide; 2]) -> Terms {
    let [a, b] = sides;
    let rel = JoinKind::range(op);
    if kind == JoinType::Inner {
        let clock = match (a.clock, b.clock) {
            (true, false) => Some(gnitz_wire::JoinSide::Left),
            (false, true) => Some(gnitz_wire::JoinSide::Right),
            _ => None,
        };
        let rel = JoinKind::Range { rel: op, clock };
        let reindex = [a.on_key(cb, NullKeys::Drop), b.on_key(cb, NullKeys::Drop)];
        let ints = integrals(cb, sides, reindex);
        let merged = cb.join(reindex, ints, rel);
        let pairs = rekey_pinned(cb, merged, 1, sides);
        return Terms { pairs: Some(pairs), splits: [None, None] };
    }
    // LEFT or decorrelated; `JoinClass::admit` refuses a pure-range RIGHT/FULL join.
    let reindex_a = (kind == JoinType::Left).then(|| a.on_key(cb, NullKeys::Drop));
    // A's rows re-keyed onto their source PK and kept on its owner — exchanged
    // unless A's delta is placed by its PK — `[a.pk, kept A]`, and that slice
    // re-keyed onto the range column, NULL range keys dropped: `[range key, kept A]`.
    let owned = a.on_pk(cb);
    let owned = if a.pk_placed { owned } else { cb.shard(owned) };
    let &[(range_slot, range_tc)] = a.key.as_slice() else {
        unreachable!("a pure range keys on its range column alone")
    };
    let npk = a.pk.len();
    let at = a.keep.iter().position(|&c| c == range_slot);
    let at = npk + at.expect("the keep rule keeps a pure range's range column");
    let keep: Vec<u32> = (npk as u32..(npk + a.keep.len()) as u32).collect();
    let int_a = cb.map_reindex(owned, &[(at as u32, range_tc)], &keep, NullKeys::Drop);
    let reindex_b = b.on_key(cb, NullKeys::Drop);
    // A LEFT join's pair terms integrate A's owned slice, which its ν subtracts
    // from; each pair lands on `[pair-PK, kept-A, kept-B]`.
    let pairs = reindex_a.map(|reindex_a| {
        let int_b = cb.shard(reindex_b);
        let merged = cb.join([reindex_a, reindex_b], [int_a, int_b], rel);
        rekey_pinned(cb, merged, 1, sides)
    });
    // The threshold decides existence over A's owned slice, already on the
    // worker owning its output key.
    let matched = threshold(cb, a, reindex_b, int_a, op);
    Terms {
        pairs,
        splits: [
            Some(Split {
                all: owned,
                known: matched,
                matched: true,
            }),
            None,
        ],
    }
}

/// The integrals an INNER keyless or pure-range step pairs each side's delta
/// against. Both deltas broadcast, each worker pairs against its own slice of
/// the other side, so every pair is made once. With one side placed by its PK —
/// the other then a broadcast side of at most one row — each side integrates its
/// delta where it lands: the one row whole on every worker, the placed side on
/// its PK owner, which pairs each match once with neither exchanged.
fn integrals(cb: &mut Circuit, sides: &[JoinSide; 2], [a, b]: [NodeId; 2]) -> [NodeId; 2] {
    match sides.iter().any(|s| s.pk_placed) {
        true => [a, b],
        false => [cb.shard(a), cb.shard(b)],
    }
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
    let red = cb.reduce(keys, agg, ReduceKind::Local);
    // `m` is a MIN/MAX of B's non-NULL range keys, so never NULL, and is typed as
    // the range pair's common type, which A's range key also carries.
    let reindex_m = cb.map_reindex(red, &[(1, side_a.key[0].1)], &[], NullKeys::Keep);
    // `m` carries no payload, so both terms are `[_join_pk, A]`.
    let deltas = [int_a, reindex_m];
    let matched = cb.join(deltas, deltas, JoinKind::range(op));
    rekey_pinned(cb, matched, 1, std::slice::from_ref(side_a))
}

#[cfg(test)]
#[path = "tests/join.rs"]
mod tests;
