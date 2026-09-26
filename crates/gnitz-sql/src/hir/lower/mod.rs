//! HIR → circuit lowering: one circuit per combine-class node and the linear
//! nodes around it, with nested combine subtrees cut to hidden segments. [`fold`]
//! lowers the same `Reduce` to an ad-hoc read's fold sink instead.
//!
//! The rules every shell obeys, one home each:
//!
//! * input spine — `spine::open` / `Spine::emit`;
//! * segment cut — [`cut_segment`];
//! * source collision — [`materialize`];
//! * reduce derivation — [`resolve_reduce_specs`], [`keyed_frame`];
//! * join keep — [`join_sides`];
//! * addressing — `physical::Frame`, [`project_front`].

mod chain;
mod exists;
pub(crate) mod fold;
mod join;
mod joincore;
mod prims;
mod reduce;
mod setop;
mod spine;
mod topn;

use super::physical::{self, Frame};
use super::{slots_of, split_filter, AggCol, ColId, HirCol, HirExpr, ProjEntry, RelExpr};
use super::{JoinClass, JoinShape, JoinType};
use crate::agg::group_pk_def;
use crate::codec::project_schema::{payload_map, ProjItem};
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
pub(crate) use chain::{EmitPieces, ViewChain};
use gnitz_core::{ColumnDef, ReduceOutKey, RelDescriptor, Schema, ViewBundle};
use gnitz_expr::SchemaFacts;
use gnitz_wire::{AggDescriptor, ReduceOutSlot};
use spine::SourceOrigin;
use std::collections::HashSet;
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
    pub(crate) cols: Vec<HirCol>,
}

impl ReduceSpecs {
    pub(crate) fn push(&mut self, spec: AggDescriptor, col: HirCol) {
        self.specs.push(spec);
        self.cols.push(col);
    }
}

/// Resolve a `Reduce`'s group and aggregate columns against its input's layout.
pub(crate) fn resolve_reduce_specs(
    group_cols: &[ColId],
    aggs: &[AggCol],
    layout: &[ColId],
) -> Result<ReduceSpecs, GnitzSqlError> {
    let group = slots_of(layout, group_cols)?.into_iter().map(|c| c as u32).collect();
    let mut r = ReduceSpecs {
        group,
        specs: Vec::with_capacity(aggs.len() + 1),
        cols: Vec::with_capacity(aggs.len() + 1),
    };
    for c in aggs {
        // COUNT(*) reads no column; slot 0 is the placeholder.
        let col_idx = c.arg.map(|id| super::slot_of(layout, id)).transpose()?.unwrap_or(0) as u32;
        r.push(AggDescriptor { agg_op: c.op, col_idx }, c.col.clone());
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
    tail: Vec<HirCol>,
) -> Result<Frame, GnitzSqlError> {
    let (mut layout, mut cols, mut npk) = (Vec::new(), Vec::new(), 0usize);
    for slot in out_key.output_layout(group, row) {
        let (id, def) = match slot {
            ReduceOutSlot::SyntheticKey => (ColId::NONE, group_pk_def()),
            ReduceOutSlot::Key(c) | ReduceOutSlot::Carried(c) => {
                (input.layout[c as usize], input.schema.columns[c as usize].clone())
            }
        };
        npk += usize::from(!matches!(slot, ReduceOutSlot::Carried(_)));
        layout.push(id);
        cols.push(def);
    }
    layout.extend(tail.iter().map(|c| c.id));
    cols.extend(tail.into_iter().map(|c| c.def));
    Frame::leading(layout, cols, npk)
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

/// A source a spine reads, with the frame its references resolve against.
#[derive(Clone)]
pub(crate) struct SegInput {
    pub src: SegSource,
    pub frame: Frame,
}

/// Where a [`SegInput`]'s rows come from.
#[derive(Clone)]
pub(crate) enum SegSource {
    /// A relation read in place; its index list bounds the scan.
    Catalog(Arc<RelDescriptor>),
    /// A subtree cut to a hidden segment via [`cut_segment`], under a chain-minted
    /// id, which has no catalog rows until the chain commits.
    Segment { tid: u64, pk_repeats: bool },
}

impl SegSource {
    pub(crate) fn tid(&self) -> u64 {
        match self {
            SegSource::Catalog(d) => d.tid,
            SegSource::Segment { tid, .. } => *tid,
        }
    }

    /// Whether two of the source's rows may share its leading key.
    pub(crate) fn pk_repeats(&self) -> bool {
        match self {
            SegSource::Catalog(d) => d.pk_repeats,
            SegSource::Segment { pk_repeats, .. } => *pk_repeats,
        }
    }
}

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
        return Ok(cb.map_expr(node, payload_map(items, out, input)?));
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

/// Lower a bound view body to its bundle.
pub(crate) fn lower(rel: Rc<RelExpr>, bounded: bool) -> Result<ViewBundle, GnitzSqlError> {
    let mut chain = ViewChain::default();
    let top = lower_body(&mut chain, &rel)?;
    // A body whose inputs cut a segment of their own bounds a view over unbounded copies.
    if bounded && chain.has_segments() {
        return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW WITH (capacity …): a body that compiles to more than one view is not supported".into(),
        ));
    }
    Ok(chain.finish(top))
}

/// Lower a complete view body to circuit pieces.
fn lower_body(chain: &mut ViewChain, rel: &Rc<RelExpr>) -> Result<EmitPieces, GnitzSqlError> {
    match rel.as_ref() {
        RelExpr::Project { input, items } => {
            let (fpreds, source) = split_filter(input);
            match source.as_ref() {
                RelExpr::Join { .. } => join::lower_join_view(chain, items, fpreds, source),
                RelExpr::Reduce { .. } => reduce::lower_reduce(chain, items, fpreds, source),
                _ => spine::lower_linear(chain, rel, items),
            }
        }
        RelExpr::Distinct { input } => setop::lower_distinct(chain, input),
        RelExpr::SetOp { out, .. } => setop::lower_setop(chain, rel, out),
        RelExpr::TopN { .. } => topn::lower_topn(chain, rel),
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
pub(crate) fn resolve_in_place(chain: &mut ViewChain, input: &Rc<RelExpr>) -> Result<Option<SegInput>, GnitzSqlError> {
    match input.as_ref() {
        RelExpr::Get { desc, cols } => Ok(Some(SegInput {
            src: SegSource::Catalog(Arc::clone(desc)),
            frame: Frame {
                layout: cols.iter().map(|c| c.id).collect(),
                schema: Arc::clone(&desc.schema),
            },
        })),
        RelExpr::Alias { input: inner, cols } => {
            let inner_cols = inner.cols();
            let all: HashSet<ColId> = inner_cols.iter().map(|c| c.id).collect();
            let SegInput { src, frame } = match resolve_in_place(chain, inner)? {
                Some(seg) => seg,
                None => cut_segment(chain, inner, &all)?,
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
                src,
                frame: Frame { layout, schema: frame.schema },
            }))
        }
        _ => Ok(None),
    }
}

/// Cut `rel` to a fresh hidden segment pruned to `live`, never one [`cut_segment`]
/// already made.
fn materialize(chain: &mut ViewChain, rel: &Rc<RelExpr>, live: &HashSet<ColId>) -> Result<SegInput, GnitzSqlError> {
    let body = as_body(rel, live);
    let pieces = lower_body(chain, &body)?;
    Ok(chain.add_segment(pieces))
}

/// Cut a subtree to a hidden segment pruned to `live`, once per subtree.
pub(crate) fn cut_segment(
    chain: &mut ViewChain,
    subtree: &Rc<RelExpr>,
    live: &HashSet<ColId>,
) -> Result<SegInput, GnitzSqlError> {
    let key = Rc::as_ptr(subtree);
    if let Some(cached) = chain.cuts.get(&key) {
        return Ok(cached.clone());
    }
    let seg = materialize(chain, subtree, live)?;
    chain.cuts.insert(key, seg.clone());
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
    let mut l = spine::open(chain, left, &live_l)?;
    let mut r = spine::open(chain, right, &live_r)?;
    // A join reads two distinct sources, one delta per epoch: a right side over the
    // left's source is re-read as a second relation.
    if l.tid() == r.tid() {
        r = spine::Spine::segment(materialize(chain, right, &live_r)?);
    }
    // A key the spine computes (`ON x.s = u.k` over `SELECT a + 1 AS s`) is no
    // column of the scanned relation, so nothing over that relation can state
    // where its delta scatters. Cut to a segment, the computed column is one.
    if !l.keys_reach_source(class.key_cols(true)) {
        l = spine::Spine::segment(materialize(chain, left, &live_l)?);
    }
    if !r.keys_reach_source(class.key_cols(false)) {
        r = spine::Spine::segment(materialize(chain, right, &live_r)?);
    }
    let (origin_l, origin_r) = (l.origin(), r.origin());
    let (a, left_frame) = l.emit(cb, spine::Top::Slots)?;
    let (b, right_frame) = r.emit(cb, spine::Top::Slots)?;
    Ok((
        [a, b],
        join_sides(down, class, kind, [left_frame, right_frame], [origin_l, origin_r]),
    ))
}

/// One side of a join as [`join_sides`] left it: the emitted input's frame, its
/// reindex-payload keep list in emission order, those columns' defs, and the
/// pinned source PK's arity (`0` when unpinned).
pub(crate) struct JoinSide {
    pub(crate) frame: Frame,
    pub(crate) keep: Vec<u32>,
    pub(crate) coldefs: Vec<ColumnDef>,
    pk_arity: usize,
    origin: SourceOrigin,
}

impl JoinSide {
    /// `key`, this side's reindex key in its own emitted layout, restated over
    /// the relation the master scatters.
    pub(crate) fn scatter_key(
        &self,
        key: &[gnitz_core::ReindexSlot],
    ) -> Result<gnitz_core::ReindexRole, GnitzSqlError> {
        self.origin.scatter_role(&self.frame, key).ok_or_else(|| {
            GnitzSqlError::Internal("a join key column is not a column of the relation it scatters".into())
        })
    }

    /// The scatter key of a re-key onto this side's source PK.
    pub(crate) fn scatter_pk(&self) -> gnitz_core::ReindexRole {
        self.origin.scatter_pk()
    }

    /// The kept payload width.
    pub(crate) fn n(&self) -> usize {
        self.keep.len()
    }

    /// The pinned source PK's arity — `0` when this shape packs no PK out of the
    /// payload.
    pub(crate) fn pa(&self) -> usize {
        self.pk_arity
    }

    /// Whether two of this side's source rows may share their leading key.
    pub(crate) fn pk_repeats(&self) -> bool {
        self.origin.src.pk_repeats()
    }

    /// The kept payload's `ColId`s, in keep order.
    pub(crate) fn ids(&self) -> impl Iterator<Item = ColId> + '_ {
        self.keep.iter().map(|&i| self.frame.layout[i as usize])
    }

    /// The type codes of the kept payload columns — what `null_extend` needs to
    /// synthesize this side's NULL region.
    pub(crate) fn kept_type_codes(&self) -> Vec<gnitz_core::TypeCode> {
        self.coldefs.iter().map(|c| c.ty.tc).collect()
    }
}

/// Both sides of a join step under the keep rule: which source columns survive
/// into the join's traces, and so onto disk. The five contributors are marked in
/// order below; a wildcard projection is already expanded into `ProjEntry` column
/// refs by bind, so Rule 1 covers `SELECT *`.
pub(crate) fn join_sides(
    down: Demand<'_>,
    class: &JoinClass,
    kind: JoinType,
    inputs: [Frame; 2],
    origins: [SourceOrigin; 2],
) -> [JoinSide; 2] {
    let [left, right] = inputs;
    let [origin_l, origin_r] = origins;
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
    [
        one_side(left, origin_l, &kl, pins[0]),
        one_side(right, origin_r, &kr, pins[1]),
    ]
}

/// One side's keep list in emission order: its pinned PK columns first, then every
/// other kept column in source order. A keep list need not ascend, and the PK at
/// the front is what makes every pair-PK slot list a range.
fn one_side(frame: Frame, origin: SourceOrigin, keep: &[bool], pin_pk: bool) -> JoinSide {
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
        origin,
    }
}

#[cfg(test)]
#[path = "tests/lower.rs"]
mod tests;
