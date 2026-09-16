//! HIR → circuit lowering: one circuit per combine-class node and the linear
//! nodes around it, with nested combine subtrees cut to hidden segments. [`fold`]
//! lowers the same `Reduce` to an ad-hoc read's fold sink instead.
//!
//! The rules every shell obeys, one home each:
//!
//! * input spine — `spine::open` / `Spine::emit`;
//! * segment cut — [`cut_segment`], through one [`CutMemo`];
//! * source collision — [`materialize`];
//! * reduce derivation — [`resolve_reduce_specs`], [`keyed_frame`];
//! * join keep — [`join_sides`];
//! * addressing — `physical::Frame`, [`project_front`], [`project_tail`].

mod exists;
pub(crate) mod fold;
mod join;
mod joincore;
mod prims;
mod reduce;
mod setop;
mod spine;
mod topn;

use super::chain::{EmitPieces, ViewChain};
use super::physical::{self, Frame};
use super::{as_col, slots_of, split_filter, ColId, GetSource, HirAgg, HirExpr, ProjEntry, RelExpr};
use super::{JoinClass, JoinShape, JoinType};
use crate::agg::group_pk_def;
use crate::codec::project_schema::{payload_map, ProjItem};
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::{ColumnDef, ReduceOutKey, RelDescriptor, Schema};
use gnitz_wire::{AggDescriptor, ReduceOutSlot};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

/// What a HIR `Reduce` becomes physically, before either lowering picks a sink:
/// its group columns as reduce-input positions, and one spec per distinct
/// physical aggregate column beside the output column it produces.
pub(crate) struct ReduceSpecs {
    /// Group columns as reduce-input positions, in GROUP BY order.
    pub(crate) group: Vec<u32>,
    pub(crate) specs: Vec<AggDescriptor>,
    /// Each spec's output column, parallel to `specs`.
    pub(crate) cols: Vec<(ColId, ColumnDef)>,
}

impl ReduceSpecs {
    pub(crate) fn push(&mut self, spec: AggDescriptor, id: ColId, def: ColumnDef) {
        self.specs.push(spec);
        self.cols.push((id, def));
    }
}

/// Resolve a `Reduce`'s group columns and aggregates against its input's layout
/// — the derivation `lower::reduce` and `lower::fold` share. The output defs are
/// `HirAgg`'s, the ones HAVING and finalize are typed against.
pub(crate) fn resolve_reduce_specs(
    group_cols: &[ColId],
    aggs: &[HirAgg],
    layout: &[ColId],
) -> Result<ReduceSpecs, GnitzSqlError> {
    let group = slots_of(layout, group_cols)?.into_iter().map(|c| c as u32).collect();
    let mut r = ReduceSpecs {
        group,
        specs: Vec::new(),
        cols: Vec::new(),
    };
    for c in aggs.iter().flat_map(HirAgg::cols) {
        if r.cols.iter().any(|(id, _)| *id == c.col.id) {
            continue; // shared with an earlier aggregate
        }
        // COUNT(*) reads no column; slot 0 is the placeholder.
        let col_idx = c.arg.map(|id| super::slot_of(layout, id)).transpose()?.unwrap_or(0) as u32;
        r.push(AggDescriptor { agg_op: c.op, col_idx }, c.col.id, c.col.def.clone());
    }
    Ok(r)
}

/// A group-keyed operator's output frame over `input`: its `output_layout` slots,
/// then `tail`.
pub(crate) fn keyed_frame(
    input: &Frame,
    out_key: ReduceOutKey,
    group: &[u32],
    row: impl IntoIterator<Item = u32>,
    tail: Vec<(ColId, ColumnDef)>,
    what: &str,
) -> Result<Frame, GnitzSqlError> {
    let (mut layout, mut cols, mut npk) = (Vec::new(), Vec::new(), 0u32);
    for slot in out_key.output_layout(&input.schema.pk_cols, group, row) {
        let (id, def) = match slot {
            ReduceOutSlot::SyntheticKey => (ColId::NONE, group_pk_def()),
            ReduceOutSlot::Key(c) | ReduceOutSlot::Carried(c) => {
                (input.layout[c as usize], input.schema.columns[c as usize].clone())
            }
        };
        npk += u32::from(!matches!(slot, ReduceOutSlot::Carried(_)));
        layout.push(id);
        cols.push(def);
    }
    let (ids, defs): (Vec<_>, Vec<_>) = tail.into_iter().unzip();
    layout.extend(ids);
    cols.extend(defs);
    let schema =
        Schema::from_parts(cols, (0..npk).collect()).map_err(|e| GnitzSqlError::Unsupported(format!("{what}: {e}")))?;
    Ok(Frame { layout, schema: Arc::new(schema) })
}

/// The downstream demand on a combine's output: the final projection and the
/// WHERE over the node — whatever predicate placement left above it.
#[derive(Clone, Copy)]
pub(crate) struct Demand<'a> {
    pub(crate) items: &'a [ProjEntry],
    pub(crate) where_preds: &'a [HirExpr],
}

impl Demand<'_> {
    /// Every `ColId` the demand reads, into `out`.
    pub(crate) fn refs(&self, out: &mut HashSet<ColId>) {
        collect_live_cols(self.items.iter().map(|i| &i.expr).chain(self.where_preds), out);
    }
}

/// A source a spine reads: a relation in place, or a subtree cut to its hidden
/// segment via [`cut_segment`], with the frame its references resolve against.
#[derive(Clone)]
pub(crate) struct SegInput {
    pub tid: u64,
    pub frame: Frame,
    /// The catalog descriptor `tid` resolved to, or `None` when `tid` is a
    /// chain-minted segment, which has no catalog rows until the chain commits.
    /// The lowering reads every catalog fact off this: the scan-bound index list
    /// and the reduce's replication flag.
    pub desc: Option<Arc<RelDescriptor>>,
}

/// The compilation-wide cut memo: a subtree reached twice (a CTE or window input
/// read through several `Alias`es) is cut to one hidden segment, which is why
/// bind shares such a subtree as one `Rc`. Every key is a node of the lowered tree.
pub(crate) type CutMemo = HashMap<*const RelExpr, SegInput>;

/// Insert every `ColId` referenced anywhere in `exprs` into `live` — the
/// one home for "which columns does this expression set demand", used to prune a
/// cut input's segment to exactly the columns its parent reads.
pub(crate) fn collect_live_cols<'a>(exprs: impl IntoIterator<Item = &'a HirExpr>, live: &mut HashSet<ColId>) {
    for e in exprs {
        e.for_each_ref(&mut |id| {
            live.insert(*id);
        });
    }
}

/// The cheapest node producing `items` over `input`, whose key region the engine
/// carries verbatim: the input itself, a copy list, or the payload expression map.
fn emit_projection(
    cb: &mut gnitz_core::Circuit,
    node: gnitz_core::NodeId,
    items: &[ProjItem],
    out: &Schema,
    input: &Schema,
) -> Result<gnitz_core::NodeId, GnitzSqlError> {
    let k = input.pk_count();
    // A payload slot naming a key column is a second copy of a value the key
    // region already carries; only the expression map can write one.
    let payload: Option<Vec<u32>> = items[k..]
        .iter()
        .map(|i| i.passthrough_src().filter(|&c| !input.is_pk_col(c)).map(|c| c as u32))
        .collect();
    let Some(payload) = payload else {
        return Ok(cb.map_expr(node, payload_map(&items[k..], &out.columns[k..], input)?));
    };
    let identity =
        items.len() == input.columns.len() && items.iter().enumerate().all(|(i, it)| it.passthrough_src() == Some(i));
    Ok(if identity { node } else { cb.map(node, &payload) })
}

/// `items` over `input` with `input`'s PK pinned to the front — the linear
/// projection.
pub(crate) fn project_front(
    cb: &mut gnitz_core::Circuit,
    node: gnitz_core::NodeId,
    items: &[ProjEntry],
    input: &Frame,
) -> Result<(gnitz_core::NodeId, Frame), GnitzSqlError> {
    let proj = physical::physicalize_projection(items, &input.layout, &input.schema)?;
    let node = emit_projection(cb, node, &proj.items, &proj.out.schema, &input.schema)?;
    Ok((node, proj.out))
}

/// A combine's projection over `frame`'s leading key region. The first item passing
/// a key-region column through renames that slot in place (a reduce's natural
/// group key); anything else lands in the payload.
pub(crate) fn project_tail(
    cb: &mut gnitz_core::Circuit,
    node: gnitz_core::NodeId,
    items: &[ProjEntry],
    frame: &Frame,
) -> Result<(gnitz_core::NodeId, Frame), GnitzSqlError> {
    let npk = frame.npk();
    debug_assert!(frame.schema.pk_cols.iter().enumerate().all(|(i, &c)| c as usize == i));
    let mut proj: Vec<ProjItem> = (0..npk).map(|src_col| ProjItem::PassThrough { src_col }).collect();
    let mut cols = frame.schema.columns[..npk].to_vec();
    let mut layout = frame.layout[..npk].to_vec();
    let mut renamed = vec![false; npk];
    for entry in items {
        let item = ProjItem::from_bound(physical::resolve_refs(&entry.expr, &frame.layout)?);
        if let Some(slot) = item.passthrough_src().filter(|&c| c < npk && !renamed[c]) {
            cols[slot].name = entry.out.def.name.clone();
            layout[slot] = entry.out.id;
            renamed[slot] = true;
            continue;
        }
        proj.push(item);
        cols.push(entry.out.def.clone());
        layout.push(entry.out.id);
    }
    let out = Frame::leading(layout, cols, npk);
    let node = emit_projection(cb, node, &proj, &out.schema, &frame.schema)?;
    Ok((node, out))
}

/// Lower a bound `RelExpr` tree to circuit pieces.
pub(crate) fn lower(chain: &mut ViewChain, rel: Rc<RelExpr>) -> Result<EmitPieces, GnitzSqlError> {
    if chain.bounded {
        reject_ineligible_capacity_body(&rel)?;
    }
    let mut memo = CutMemo::new();
    lower_body(chain, &mut memo, &rel)
}

/// Reject a root shape per-key hydration cannot replay: every shape but a
/// filter/projection over one relation and an inner equi-join. A cut below an
/// eligible root is `ViewChain::add_segment`'s to refuse.
fn reject_ineligible_capacity_body(rel: &RelExpr) -> Result<(), GnitzSqlError> {
    let shape = match rel {
        RelExpr::Project { input, items } => {
            let (_, source) = split_filter(input);
            // An alias reads what it aliases; the shape is the target's.
            let mut source = source;
            while let RelExpr::Alias { input, .. } = source.as_ref() {
                source = input;
            }
            match source.as_ref() {
                // Only a `Get` source can carry one: over a combine the projection
                // either cuts (bounding a projection that sits on an unbounded copy
                // of the same rows) or fuses into the join tail (leaving per-key
                // hydration to replay a skeleton row through a compute map).
                _ if items.iter().any(|it| as_col(&it.expr).is_none())
                    && !matches!(source.as_ref(), RelExpr::Get { .. }) =>
                {
                    "a computed projection over a combine"
                }
                // Eligible: filter/projection over one relation.
                RelExpr::Get { .. } => return Ok(()),
                RelExpr::Join { kind: JoinType::Inner, on, .. } => match on.shape() {
                    // A range/band join's null-fill threshold pipeline is not
                    // replayable per key; a cross join has no key at all, so a
                    // skeleton row names no trace group to replay.
                    JoinShape::Band | JoinShape::PureRange => "a range or band join",
                    JoinShape::Cross => "a cross join",
                    // Eligible: plain inner equi-join.
                    JoinShape::Equi => return Ok(()),
                },
                RelExpr::Join {
                    kind: JoinType::Left | JoinType::Right | JoinType::Full,
                    ..
                } => "an outer join",
                RelExpr::Join {
                    kind: JoinType::Semi | JoinType::Anti, ..
                } => "EXISTS / NOT EXISTS / IN",
                RelExpr::Join { kind: JoinType::Mark(_), .. } => "a mark join (IN / ANY over a subquery)",
                RelExpr::Reduce { .. } => "GROUP BY / an aggregate",
                _ => "a derived table / DISTINCT / set-operation subquery",
            }
        }
        RelExpr::Distinct { .. } => "a root DISTINCT",
        RelExpr::SetOp { .. } => "a root set operation",
        RelExpr::TopN { .. } => "ORDER BY … LIMIT",
        _ => "this body",
    };
    Err(GnitzSqlError::Unsupported(format!(
        "CREATE VIEW WITH (capacity …): {shape} is not supported; \
         only a filter/projection over one relation and an inner equi-join are"
    )))
}

/// Lower a complete view body to circuit pieces.
fn lower_body(chain: &mut ViewChain, memo: &mut CutMemo, rel: &Rc<RelExpr>) -> Result<EmitPieces, GnitzSqlError> {
    match rel.as_ref() {
        RelExpr::Project { input, items } => {
            let (fpreds, source) = split_filter(input);
            match source.as_ref() {
                RelExpr::Join { .. } => join::lower_join_view(chain, memo, items, fpreds, source),
                RelExpr::Reduce { .. } => reduce::lower_reduce(chain, memo, items, fpreds, source),
                _ => spine::lower_linear(chain, memo, rel, items),
            }
        }
        RelExpr::Distinct { input } => setop::lower_distinct(chain, memo, input),
        RelExpr::SetOp { .. } => setop::lower_setop(chain, memo, rel),
        RelExpr::TopN { .. } => topn::lower_topn(chain, memo, rel),
        _ => Err(GnitzSqlError::Internal(
            "HIR lowering has no body arm for this node".into(),
        )),
    }
}

/// `Project?(Filter?(Join | Reduce))`: what a join or reduce shell lowers as one body.
fn lowered_whole(rel: &RelExpr) -> bool {
    let rel = match rel {
        RelExpr::Project { input, .. } => input.as_ref(),
        rel => rel,
    };
    let rel = match rel {
        RelExpr::Filter { input, .. } => input.as_ref(),
        rel => rel,
    };
    matches!(rel, RelExpr::Join { .. } | RelExpr::Reduce { .. })
}

/// `input` read in place: a catalog `Get`, or an alias of its input's source (cut
/// whole, since every alias reads that one segment) under the alias's ids.
pub(crate) fn resolve_in_place(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    input: &Rc<RelExpr>,
) -> Result<Option<SegInput>, GnitzSqlError> {
    match input.as_ref() {
        RelExpr::Get {
            source: GetSource::Catalog { desc },
            schema,
            cols,
        } => Ok(Some(SegInput {
            tid: desc.tid,
            frame: Frame {
                layout: cols.iter().map(|c| c.id).collect(),
                schema: Arc::clone(schema),
            },
            desc: Some(Arc::clone(desc)),
        })),
        RelExpr::Alias { input: inner, cols } => {
            let inner_cols = inner.cols();
            let all: HashSet<ColId> = inner_cols.iter().map(|c| c.id).collect();
            let SegInput { tid, frame, desc } = match resolve_in_place(chain, memo, inner)? {
                Some(seg) => seg,
                None => cut_segment(chain, memo, inner, &all)?,
            };
            let layout = frame
                .layout
                .iter()
                .map(|id| {
                    inner_cols
                        .iter()
                        .position(|c| c.id == *id)
                        .map_or(ColId::NONE, |p| cols[p].id)
                })
                .collect();
            Ok(Some(SegInput {
                tid,
                frame: Frame { layout, schema: frame.schema },
                desc,
            }))
        }
        _ => Ok(None),
    }
}

/// Cut `rel` to a hidden segment pruned to `live`, bypassing the memo: the
/// source-collision rule, which needs a second relation where the memo holds one.
fn materialize(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    rel: &Rc<RelExpr>,
    live: &HashSet<ColId>,
) -> Result<SegInput, GnitzSqlError> {
    let body = as_body(rel, live);
    chain.add_segment(|chain| lower_body(chain, memo, &body))
}

/// Cut a subtree to a hidden segment pruned to `live`, once per subtree.
pub(crate) fn cut_segment(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    subtree: &Rc<RelExpr>,
    live: &HashSet<ColId>,
) -> Result<SegInput, GnitzSqlError> {
    let key = Rc::as_ptr(subtree);
    if let Some(cached) = memo.get(&key) {
        return Ok(cached.clone());
    }
    let seg = materialize(chain, memo, subtree, live)?;
    memo.insert(key, seg.clone());
    Ok(seg)
}

/// A subtree as a complete, lowerable body: `Project`/`Distinct`/`SetOp` already
/// are one; anything else is wrapped in an identity `Project` over its live cols.
///
/// A cut `Project` narrows its own items to the live set — the one pruning gap the
/// lowering-time demand did not already close (its parent's demand reaches the
/// identity-`Project` wrap below and the shell-built child-demand, but never a cut
/// `Project` segment's own entries). `Distinct`/`SetOp` stay full-column: their
/// content-hash identity spans every projected column (`π(distinct(X)) ≠
/// distinct(π(X))`), so narrowing would change the dedup/match classes.
fn as_body(subtree: &Rc<RelExpr>, live: &HashSet<ColId>) -> Rc<RelExpr> {
    match subtree.as_ref() {
        RelExpr::Project { input, items } => {
            let narrowed: Vec<ProjEntry> = items.iter().filter(|it| live.contains(&it.out.id)).cloned().collect();
            if narrowed.len() == items.len() {
                Rc::clone(subtree)
            } else {
                RelExpr::project(Rc::clone(input), narrowed)
            }
        }
        RelExpr::Distinct { .. } | RelExpr::SetOp { .. } | RelExpr::TopN { .. } => Rc::clone(subtree),
        // The HIR spelling of `SELECT <live cols> FROM <subtree>`.
        _ => {
            let items = RelExpr::passthrough_items(subtree.cols().into_iter().filter(|c| live.contains(&c.id)));
            RelExpr::project(Rc::clone(subtree), items)
        }
    }
}

/// Compile `preds` over `schema` and emit their AND — passing `node` through
/// untouched when nothing is left to test.
fn filter(
    cb: &mut gnitz_core::Circuit,
    node: gnitz_core::NodeId,
    preds: &[BoundExpr],
    schema: &Schema,
) -> Result<gnitz_core::NodeId, GnitzSqlError> {
    match crate::expr_lower::compile_filter_program(preds, &schema.columns)? {
        Some(prog) => Ok(cb.filter(node, prog.to_blob_bytes())),
        None => Ok(node),
    }
}

/// Resolve `preds` against `frame` and emit the filter. Shared by the emits that
/// filter an already-emitted node: HAVING, and the WHERE over each join branch.
pub(crate) fn emit_filter<'a>(
    cb: &mut gnitz_core::Circuit,
    node: gnitz_core::NodeId,
    preds: impl IntoIterator<Item = &'a HirExpr>,
    frame: &Frame,
) -> Result<gnitz_core::NodeId, GnitzSqlError> {
    filter(cb, node, &physical::resolve_preds(preds, &frame.layout)?, &frame.schema)
}

/// A join's two inputs opened through the spine and emitted into `cb`, each
/// carrying what `down` reads and its own keys, and kept under [`join_sides`].
pub(crate) fn emit_join_inputs(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    cb: &mut gnitz_core::Circuit,
    down: Demand<'_>,
    [left, right]: [&Rc<RelExpr>; 2],
    kind: JoinType,
    class: &JoinClass,
) -> Result<([gnitz_core::NodeId; 2], [JoinSide; 2]), GnitzSqlError> {
    let live = |is_left: bool| {
        let mut live = HashSet::new();
        down.refs(&mut live);
        live.extend(class.key_cols(is_left));
        live
    };
    let (live_l, live_r) = (live(true), live(false));
    let [l, r] = spine::open_pair(chain, memo, [left, right], [&live_l, &live_r], true)?;
    let (a, left_frame) = l.emit(cb, spine::Top::Slots, "join input")?;
    let (b, right_frame) = r.emit(cb, spine::Top::Slots, "join input")?;
    Ok(([a, b], join_sides(down, class, kind, [left_frame, right_frame])))
}

/// One side of a join as [`join_sides`] left it: the emitted input's frame, its
/// reindex-payload keep list in emission order, those columns' defs, and the
/// pinned source PK's arity (`0` when unpinned).
pub(crate) struct JoinSide {
    pub(crate) frame: Frame,
    pub(crate) keep: Vec<u32>,
    pub(crate) coldefs: Vec<ColumnDef>,
    pk_arity: usize,
}

impl JoinSide {
    /// The kept payload width.
    pub(crate) fn n(&self) -> usize {
        self.keep.len()
    }

    /// The pinned source PK's arity — `0` when this shape packs no PK out of the
    /// payload.
    pub(crate) fn pa(&self) -> usize {
        self.pk_arity
    }

    /// The kept payload's `ColId`s, in keep order.
    pub(crate) fn ids(&self) -> impl Iterator<Item = ColId> + '_ {
        self.keep.iter().map(|&i| self.frame.layout[i as usize])
    }

    /// The type codes of the kept payload columns — what `null_extend` needs to
    /// synthesize this side's NULL region.
    pub(crate) fn kept_type_codes(&self) -> Vec<u8> {
        self.coldefs.iter().map(|c| c.type_code as u8).collect()
    }
}

/// Both sides of a join step under the keep rule: which source columns survive
/// into the join's traces, and so onto disk. The five contributors are marked in
/// order below; a wildcard projection is already expanded into `ProjEntry` column
/// refs by bind, so Rule 1 covers `SELECT *`.
pub(crate) fn join_sides(down: Demand<'_>, class: &JoinClass, kind: JoinType, inputs: [Frame; 2]) -> [JoinSide; 2] {
    let [left, right] = inputs;
    let (left_layout, right_layout) = (&left.layout, &right.layout);
    // One keep list per side, so a rule cannot write into the region another rule
    // owns.
    let mut keep = [vec![false; left_layout.len()], vec![false; right_layout.len()]];
    let mark = |keep: &mut [Vec<bool>; 2], id: ColId| {
        if let Some(p) = left_layout.iter().position(|c| *c == id) {
            keep[0][p] = true;
        } else if let Some(p) = right_layout.iter().position(|c| *c == id) {
            keep[1][p] = true;
        }
        // An id in neither layout is the mark column, which the shell substitutes
        // per branch by its `0/1` constant before resolving anything.
    };
    // Rules 1 + 2: the projection and the WHERE over the join.
    let mut referenced: HashSet<ColId> = HashSet::new();
    down.refs(&mut referenced);
    for id in referenced {
        mark(&mut keep, id);
    }
    // Rule 3: a side with a ν keeps each key column its ν cannot do without.
    let keeps_key_col = |def: &ColumnDef, is_left: bool| match class.shape() {
        // The gate after the re-key reads a nullable key column, and it keeps a
        // NULL-keyed row apart from a 0-keyed one under the clamp.
        JoinShape::Equi => def.is_nullable && kind.emits_unmatched(is_left),
        // A band ν is keyed by the source PK, which a bag-valued side repeats; a
        // pure range re-keys its owned A slice onto the range column.
        JoinShape::Band | JoinShape::PureRange => true,
        JoinShape::Cross => false,
    };
    for (side, is_left, frame) in [(0, true, &left), (1, false, &right)] {
        if !kind.has_nu(is_left) {
            continue;
        }
        for id in class.key_cols(is_left) {
            if let Some(pos) = frame.layout.iter().position(|c| *c == id) {
                keep[side][pos] |= keeps_key_col(&frame.schema.columns[pos], is_left);
            }
        }
    }
    // Rule 4: a side whose source PK the output key packs out of the payload pins
    // it to the front of its keep list, which `one_side` prepends.
    let pins = class.out_key(kind).pins();
    // Rule 5, the fallback: a side with a ν needs an identity to subtract on, and
    // a pinned side already has one. A side without a ν keeps nothing — column 0
    // would split one trace element per key into one per row.
    for (side, is_left) in [(0, true), (1, false)] {
        if kind.has_nu(is_left) && !pins[side] && !keep[side].iter().any(|&b| b) {
            keep[side][0] = true;
        }
    }
    // A join reading nothing from either side still emits rows. `pins[1]` implies
    // `pins[0]`, so the left pin alone answers for both.
    if !pins[0] && !keep[0].iter().any(|&b| b) && !keep[1].iter().any(|&b| b) {
        keep[0][0] = true;
    }
    let [kl, kr] = keep;
    [one_side(left, &kl, pins[0]), one_side(right, &kr, pins[1])]
}

/// One side's keep list in emission order: its pinned PK columns first, then every
/// other kept column in source order. A keep list need not ascend, and the PK at
/// the front is what makes every pair-PK slot list a range.
fn one_side(frame: Frame, keep: &[bool], pin_pk: bool) -> JoinSide {
    let pinned: Vec<u32> = if pin_pk {
        frame.schema.pk_cols.clone()
    } else {
        Vec::new()
    };
    let mut cols = pinned.clone();
    cols.extend((0..keep.len() as u32).filter(|i| keep[*i as usize] && !pinned.contains(i)));
    let coldefs = cols.iter().map(|&i| frame.schema.columns[i as usize].clone()).collect();
    JoinSide {
        pk_arity: pinned.len(),
        frame,
        keep: cols,
        coldefs,
    }
}

#[cfg(test)]
#[path = "tests/lower.rs"]
mod tests;
