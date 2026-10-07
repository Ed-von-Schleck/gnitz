//! HIR → circuit lowering: one circuit per combine-class node and the linear
//! nodes around it, with nested combine subtrees cut to hidden segments. [`read`]
//! lowers an ad-hoc SELECT to one read of one relation instead, its `Reduce`
//! through [`fold`] to the fold sink.
//!
//! The rules every shell obeys, one home each:
//!
//! * input spine — `spine::open` / `Spine::emit`;
//! * segment cut — [`cut_segment`];
//! * source collision — [`materialize`];
//! * reduce derivation — [`resolve_reduce_specs`], [`keyed_frame`];
//! * addressing — `physical::Frame`, [`project_front`].

mod chain;
pub(crate) mod fold;
mod join;
mod joincore;
pub(crate) mod read;
mod reduce;
mod setop;
mod spine;
mod topn;

use super::physical::{self, Frame, Rename, Slot};
use super::{col_by_id, split_filter, AggCol, ColId, HirCol, HirExpr, ProjEntry, RelExpr};
use crate::agg::group_pk_def;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use crate::project::{compute_map, payload_program, ProjItem};
pub(crate) use chain::{EmitPieces, ViewChain};
use gnitz_core::{RelDescriptor, Schema, ViewBundle};
use gnitz_expr::SchemaFacts;
use gnitz_wire::ColumnDef;
use gnitz_wire::{AggDescriptor, ReduceOutSlot};
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;

/// What a HIR `Reduce` becomes physically, before either lowering picks a sink:
/// its group columns as reduce-input positions, and one spec per distinct
/// physical aggregate column beside the output column it produces.
pub(crate) struct ReduceSpecs {
    /// Group columns as reduce-input positions, normalized.
    pub(crate) group: Vec<u32>,
    pub(crate) specs: Vec<AggDescriptor>,
    /// Each spec's output column, parallel to `specs`; `None` for one no reference
    /// names.
    pub(crate) cols: Vec<Slot>,
}

impl ReduceSpecs {
    pub(crate) fn push(&mut self, spec: AggDescriptor, id: Option<ColId>, def: ColumnDef) {
        self.specs.push(spec);
        self.cols.push((id, def));
    }
}

/// Resolve a `Reduce`'s group and aggregate columns against its input.
pub(crate) fn resolve_reduce_specs(
    group_cols: &[ColId],
    aggs: &[AggCol],
    input: &Frame,
) -> Result<ReduceSpecs, GnitzSqlError> {
    let mut r = ReduceSpecs {
        group: input.reduce_group(group_cols)?,
        specs: Vec::with_capacity(aggs.len() + 1),
        cols: Vec::with_capacity(aggs.len() + 1),
    };
    for c in aggs {
        // COUNT(*) reads no column; slot 0 is the placeholder.
        let col_idx = c.arg.map(|id| input.slot(id)).transpose()?.unwrap_or(0) as u32;
        r.push(
            AggDescriptor { agg_op: c.op, col_idx },
            Some(c.col.id),
            c.col.def.clone(),
        );
    }
    Ok(r)
}

/// A group-keyed operator's output frame over `input`: the `output_layout` of
/// the key a reduce grouped by `group` gets, then `tail`.
pub(crate) fn keyed_frame(
    input: &Frame,
    group: &[u32],
    row: impl IntoIterator<Item = u32>,
    tail: Vec<Slot>,
) -> Result<Frame, GnitzSqlError> {
    let slots = input.schema.reduce_out_key(group).output_layout(group, row);
    let npk = slots.iter().filter(|s| !matches!(s, ReduceOutSlot::Carried(_))).count();
    let lead = slots.into_iter().map(|s| match s {
        ReduceOutSlot::SyntheticKey => (None, group_pk_def()),
        ReduceOutSlot::Key(c) | ReduceOutSlot::Carried(c) => {
            (input.layout[c as usize], input.schema.columns[c as usize].clone())
        }
    });
    Frame::new(lead.chain(tail), npk)
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

impl SegInput {
    fn renamed(self, rename: &Rename) -> Result<SegInput, GnitzSqlError> {
        Ok(SegInput {
            frame: self.frame.renamed(rename)?,
            src: self.src,
        })
    }
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

    /// Whether two of the source's rows may share its leading key, or one may stand
    /// at weight above 1.
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
    cb: &mut gnitz_wire::Circuit,
    node: gnitz_wire::NodeId,
    items: &[ProjItem],
    out: &Schema,
    input: &Schema,
) -> Result<gnitz_wire::NodeId, GnitzSqlError> {
    let k = input.pk_cols.len();
    // A payload slot naming a key column is a second copy of a value the key
    // region already carries; only the expression map can write one.
    let payload: Option<Vec<u32>> = items[k..]
        .iter()
        .map(|i| i.passthrough_src().filter(|&c| !input.is_pk_col(c)).map(|c| c as u32))
        .collect();
    let Some(payload) = payload else {
        return Ok(cb.map_expr(node, compute_map(payload_program(items, out, input)?, out)));
    };
    let identity =
        items.len() == input.columns.len() && items.iter().enumerate().all(|(i, it)| it.passthrough_src() == Some(i));
    Ok(if identity { node } else { cb.map(node, &payload) })
}

/// `items` over `input` with `input`'s PK pinned to the front — the linear
/// projection.
pub(crate) fn project_front(
    cb: &mut gnitz_wire::Circuit,
    node: gnitz_wire::NodeId,
    items: &[ProjEntry],
    input: &Frame,
) -> Result<(gnitz_wire::NodeId, Frame), GnitzSqlError> {
    let (items, out) = physical::physicalize_projection(items, input)?;
    let node = emit_projection(cb, node, &items, &out.schema, &input.schema)?;
    Ok((node, out))
}

/// Lower a bound view body to its bundle. The view lists its visible columns in
/// `names` order, each under the name its column of `names` gives it: the final
/// frame's regions under the SELECT list's numbering.
pub(crate) fn lower(rel: Rc<RelExpr>, bounded: bool, names: &[HirCol]) -> Result<ViewBundle, GnitzSqlError> {
    let mut chain = ViewChain::default();
    // A top-N carries its input's columns key first: the SELECT list is a
    // projection over it.
    let rel = match rel.as_ref() {
        RelExpr::TopN { .. } => RelExpr::project(Rc::clone(&rel), RelExpr::passthrough_items(rel.cols())),
        _ => rel,
    };
    let mut top = lower_body(&mut chain, &rel)?;
    let schema = Arc::make_mut(&mut top.out.schema);
    for (slot, id) in top.out.layout.iter().enumerate() {
        if let Some(c) = id.and_then(|id| col_by_id(names, id)) {
            schema.columns[slot].name = c.def.name.clone();
        }
    }
    let visible = names.iter().filter(|c| !c.def.is_hidden).map(|c| c.id);
    top.out.schema = Arc::new(top.out.schema_in_order(visible)?.0);
    // A body whose inputs cut a segment of their own bounds a view over unbounded copies.
    if bounded && chain.has_segments() {
        return Err(GnitzSqlError::Rejected(
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
                RelExpr::TopN { .. } => {
                    let above = topn::Above { alias: None, preds: fpreds, items };
                    topn::lower_topn(chain, source, Some(above))
                }
                // An alias nothing else reads: a window's cut under its outer read.
                RelExpr::Alias { input: cut, cols }
                    if matches!(cut.as_ref(), RelExpr::TopN { .. }) && Rc::strong_count(cut) == 1 =>
                {
                    let above = topn::Above { alias: Some(cols), preds: fpreds, items };
                    topn::lower_topn(chain, cut, Some(above))
                }
                _ => spine::lower_linear(chain, rel, items),
            }
        }
        RelExpr::Distinct { input } => setop::lower_distinct(chain, input),
        RelExpr::SetOp { out, .. } => setop::lower_setop(chain, rel, out),
        RelExpr::TopN { .. } => topn::lower_topn(chain, rel, None),
        _ => Err(GnitzSqlError::Internal(
            "HIR lowering has no body arm for this node".into(),
        )),
    }
}

/// `Project?(Filter?(Join | Reduce | TopN))`: what a join, reduce or top-N shell
/// lowers as one body.
fn lowered_whole(rel: &RelExpr) -> bool {
    let rel = match rel {
        RelExpr::Project { input, .. } => input.as_ref(),
        rel => rel,
    };
    let rel = match rel {
        RelExpr::Filter { input, .. } => input.as_ref(),
        rel => rel,
    };
    matches!(
        rel,
        RelExpr::Join { .. } | RelExpr::Reduce { .. } | RelExpr::TopN { .. }
    )
}

/// `input` read in place: a catalog `Get`, or a rename of what is read in place.
/// An alias renames its input's source, which is cut whole when it is not read
/// in place.
pub(crate) fn resolve_in_place(chain: &mut ViewChain, input: &Rc<RelExpr>) -> Result<Option<SegInput>, GnitzSqlError> {
    match input.as_ref() {
        RelExpr::Get { desc, cols } => Ok(Some(SegInput {
            src: SegSource::Catalog(Arc::clone(desc)),
            frame: Frame::scan(desc, cols),
        })),
        RelExpr::Project { input, items } => {
            let Some(rename) = Rename::of(items) else {
                return Ok(None);
            };
            resolve_in_place(chain, input)?
                .map(|seg| seg.renamed(&rename))
                .transpose()
        }
        RelExpr::Alias { input: inner, cols } => {
            let inner_cols = inner.cols();
            let seg = match resolve_in_place(chain, inner)? {
                Some(seg) => seg,
                None => cut_segment(chain, inner, &inner_cols.iter().map(|c| c.id).collect())?,
            };
            seg.renamed(&Rename::alias(&inner_cols, cols)).map(Some)
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
    cb: &mut gnitz_wire::Circuit,
    node: gnitz_wire::NodeId,
    preds: &[BoundExpr],
    schema: &Schema,
) -> Result<gnitz_wire::NodeId, GnitzSqlError> {
    match crate::expr_lower::compile_filter_program(preds, &schema.columns)? {
        Some(prog) => Ok(cb.filter(node, prog.to_blob_bytes())),
        None => Ok(node),
    }
}

/// Resolve `preds` against `frame` and emit the filter. Shared by the emits that
/// filter an already-emitted node: HAVING, and the WHERE over each join branch.
pub(crate) fn emit_filter(
    cb: &mut gnitz_wire::Circuit,
    node: gnitz_wire::NodeId,
    preds: &[HirExpr],
    frame: &Frame,
) -> Result<gnitz_wire::NodeId, GnitzSqlError> {
    filter(cb, node, &frame.resolve_preds(preds)?, &frame.schema)
}
