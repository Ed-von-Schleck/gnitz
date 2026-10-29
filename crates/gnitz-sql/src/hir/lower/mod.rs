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
//! * the WHERE and projection above a core — `Demand::emit`;
//! * addressing — `physical::Frame`, [`project_front`].

mod chain;
pub(crate) mod fold;
mod join;
mod ordered;
pub(crate) mod read;
mod reduce;
mod setop;
mod spine;

use super::physical::{self, Frame, Rename, Slot};
use super::{col_by_id, split_filter, AggCol, ColId, HirCol, HirExpr, ProjEntry, RelExpr};
use crate::agg::group_pk_def;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use crate::project::{compute_map, payload_program};
use chain::{EmitPieces, ViewChain};
use gnitz_core::{PlannedView, RelDescriptor, Schema};
use gnitz_expr::ReduceOutSlot;
use gnitz_wire::{AggDescriptor, ColumnDef};
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;

/// What a HIR `Reduce` becomes physically, before either lowering picks a sink:
/// its group columns as reduce-input positions, and one spec per distinct
/// physical aggregate column beside the output column it produces.
struct ReduceSpecs {
    /// Group columns as reduce-input positions, normalized.
    group: Vec<u32>,
    specs: Vec<AggDescriptor>,
    /// Each spec's output column, parallel to `specs`; `None` for one no reference
    /// names.
    cols: Vec<Slot>,
}

impl ReduceSpecs {
    fn push(&mut self, spec: AggDescriptor, id: Option<ColId>, def: ColumnDef) {
        self.specs.push(spec);
        self.cols.push((id, def));
    }
}

/// Resolve a `Reduce`'s group and aggregate columns against its input.
fn resolve_reduce_specs(group_cols: &[ColId], aggs: &[AggCol], input: &Frame) -> Result<ReduceSpecs, GnitzSqlError> {
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

/// A group-keyed operator's output frame over `input`: the reduce output layout of `group` and
/// `row`, then `tail`.
fn keyed_frame(
    input: &Frame,
    group: &[u32],
    row: impl IntoIterator<Item = u32>,
    tail: Vec<Slot>,
) -> Result<Frame, GnitzSqlError> {
    let slots = input.schema.layout().reduce_out_layout(group, row);
    let npk = slots.iter().filter(|s| !matches!(s, ReduceOutSlot::Carried(_))).count();
    let lead = slots.into_iter().map(|s| match s {
        ReduceOutSlot::SyntheticKey => (None, group_pk_def()),
        ReduceOutSlot::Key(c) | ReduceOutSlot::Carried(c) => {
            (input.layout[c as usize], input.schema.columns()[c as usize].clone())
        }
    });
    Frame::new(lead.chain(tail), npk)
}

/// The downstream demand on a combine's output: the final projection and the
/// WHERE over the node — whatever predicate placement left above it.
#[derive(Clone, Copy)]
struct Demand<'a> {
    items: &'a [ProjEntry],
    where_preds: &'a [HirExpr],
}

impl Demand<'_> {
    /// Every `ColId` the demand reads, into `out`.
    fn refs(&self, out: &mut HashSet<ColId>) {
        collect_live_cols(self.items.iter().map(|i| &i.expr).chain(self.where_preds), out);
    }

    /// The WHERE, then the projection, over the already-emitted `node` whose rows
    /// `frame` addresses.
    fn emit(
        &self,
        cb: &mut gnitz_wire::Circuit,
        node: gnitz_wire::NodeId,
        frame: &Frame,
    ) -> Result<(gnitz_wire::NodeId, Frame), GnitzSqlError> {
        let filtered = filter(cb, node, &frame.resolve_preds(self.where_preds)?, &frame.schema)?;
        project_front(cb, filtered, self.items, frame)
    }
}

/// A source a spine reads, with the frame its references resolve against.
#[derive(Clone)]
struct SegInput {
    src: SegSource,
    frame: Frame,
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
enum SegSource {
    /// A relation read in place; its index list bounds the scan.
    Catalog(Arc<RelDescriptor>),
    /// A subtree cut to a hidden segment via [`cut_segment`], under a chain-minted
    /// id, which has no catalog rows until the chain commits.
    Segment { tid: u64, pk_repeats: bool },
}

impl SegSource {
    fn tid(&self) -> u64 {
        match self {
            SegSource::Catalog(d) => d.tid,
            SegSource::Segment { tid, .. } => *tid,
        }
    }

    /// Whether two of the source's rows may share its leading key, or one may stand
    /// at weight above 1.
    fn pk_repeats(&self) -> bool {
        match self {
            SegSource::Catalog(d) => d.pk_repeats,
            SegSource::Segment { pk_repeats, .. } => *pk_repeats,
        }
    }
}

/// Insert every `ColId` referenced anywhere in `exprs` into `live` — the
/// one home for "which columns does this expression set demand", used to prune a
/// cut input's segment to exactly the columns its parent reads.
fn collect_live_cols<'a>(exprs: impl IntoIterator<Item = &'a HirExpr>, live: &mut HashSet<ColId>) {
    for e in exprs {
        e.for_each_ref(&mut |id| {
            live.insert(*id);
        });
    }
}

/// `items` over `input` with `input`'s PK pinned to the front, the linear
/// projection, as the cheapest node producing them: the input itself, a copy
/// list, or the payload expression map. The engine carries the key region
/// verbatim through each.
fn project_front(
    cb: &mut gnitz_wire::Circuit,
    node: gnitz_wire::NodeId,
    items: &[ProjEntry],
    input: &Frame,
) -> Result<(gnitz_wire::NodeId, Frame), GnitzSqlError> {
    let (items, out) = physical::physicalize_projection(items, input)?;
    let input = &input.schema;
    let k = input.pk_cols().len();
    // A payload slot naming a key column is a second copy of a value the key
    // region already carries; only the expression map can write one.
    let payload: Option<Vec<u32>> = items[k..]
        .iter()
        .map(|i| i.as_col().filter(|&c| !input.layout().is_pk_col(c)).map(|c| c as u32))
        .collect();
    let Some(payload) = payload else {
        let map = compute_map(payload_program(&items, &out.schema, input)?, &out.schema);
        return Ok((cb.map_expr(node, map), out));
    };
    let identity =
        items.len() == input.columns().len() && items.iter().enumerate().all(|(i, it)| it.as_col() == Some(i));
    let node = if identity { node } else { cb.map(node, &payload) };
    Ok((node, out))
}

/// Lower a bound view body to its segments and the user-named view. The view lists its visible columns in
/// `names` order, each under the name its column of `names` gives it: the final
/// frame's regions under the SELECT list's numbering.
pub(crate) fn lower(
    rel: Rc<RelExpr>,
    bounded: bool,
    names: &[HirCol],
) -> Result<(Vec<PlannedView>, PlannedView), GnitzSqlError> {
    let mut chain = ViewChain::default();
    // A top-N carries its input's columns key first: the SELECT list is a
    // projection over it. A window body always ends in a projection.
    let rel = match rel.as_ref() {
        RelExpr::Ordered { .. } => RelExpr::project(Rc::clone(&rel), ProjEntry::passthroughs(rel.cols())),
        _ => rel,
    };
    let top = lower_body(&mut chain, &rel)?;
    let visible = names.iter().filter(|c| !c.def.is_hidden).map(|c| c.id);
    let (mut schema, moved_to) = top.out.schema_in_order(visible)?;
    for (slot, id) in top.out.layout.iter().enumerate() {
        if let Some(c) = id.and_then(|id| col_by_id(names, id)) {
            schema.rename_column(moved_to[slot] as usize, c.def.name.clone());
        }
    }
    // A body whose inputs cut a segment of their own bounds a view over unbounded copies.
    if bounded && chain.has_segments() {
        return Err(GnitzSqlError::Rejected(
            "CREATE VIEW WITH (capacity …): a body that compiles to more than one view is not supported".into(),
        ));
    }
    Ok(chain.finish(top, Arc::new(schema)))
}

/// `rel` as `Project?(Filter?(core))`: the Project's items, the Filter's
/// predicates (empty without one), and the core below them.
fn peel(rel: &Rc<RelExpr>) -> (Option<&[ProjEntry]>, &[HirExpr], &Rc<RelExpr>) {
    let (items, below) = match rel.as_ref() {
        RelExpr::Project { input, items } => (Some(items.as_slice()), input),
        _ => (None, rel),
    };
    let (preds, core) = split_filter(below);
    (items, preds, core)
}

/// Lower a complete view body to circuit pieces.
fn lower_body(chain: &mut ViewChain, rel: &Rc<RelExpr>) -> Result<EmitPieces, GnitzSqlError> {
    let (items, preds, core) = peel(rel);
    let above = items.map(|items| Demand { items, where_preds: preds });
    let no_arm = || GnitzSqlError::Internal("HIR lowering has no body arm for this node".into());
    let mut pieces = match (above, core.as_ref()) {
        (None, _) if !preds.is_empty() => return Err(no_arm()),
        (Some(above), RelExpr::Join { left, right, kind, on }) => {
            return join::lower_join_view(chain, above, [left, right], *kind, on);
        }
        (Some(_), RelExpr::Reduce { input, group_cols, aggs }) => reduce::lower_reduce(chain, input, group_cols, aggs)?,
        (_, RelExpr::Ordered { .. }) => ordered::lower_ordered(chain, core)?,
        (_, RelExpr::Distinct { .. } | RelExpr::SetOp { .. }) => setop::lower_setop(chain, core)?,
        (Some(above), _) => return spine::lower_linear(chain, rel, above.items),
        (None, _) => return Err(no_arm()),
    };
    // The shell's circuit ends in its core node, which `above` reads.
    if let Some(above) = above {
        let node = pieces.circuit.out();
        pieces.out = above.emit(&mut pieces.circuit, node, &pieces.out)?.1;
    }
    Ok(pieces)
}

/// `Project?(Filter?(Join | Reduce | Ordered | Distinct | SetOp))`: what a
/// shell lowers as one body.
fn lowered_whole(rel: &Rc<RelExpr>) -> bool {
    matches!(
        peel(rel).2.as_ref(),
        RelExpr::Join { .. }
            | RelExpr::Reduce { .. }
            | RelExpr::Ordered { .. }
            | RelExpr::Distinct { .. }
            | RelExpr::SetOp { .. }
    )
}

/// `input` read in place: a catalog `Get`, or a rename of what is read in place.
/// An alias renames its input's source, which is cut whole when it is not read
/// in place.
fn resolve_in_place(chain: &mut ViewChain, input: &Rc<RelExpr>) -> Result<Option<SegInput>, GnitzSqlError> {
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
fn cut_segment(chain: &mut ViewChain, subtree: &Rc<RelExpr>, live: &HashSet<ColId>) -> Result<SegInput, GnitzSqlError> {
    let key = Rc::as_ptr(subtree);
    if let Some(cached) = chain.cuts.get(&key) {
        return Ok(cached.clone());
    }
    let seg = materialize(chain, subtree, live)?;
    chain.cuts.insert(key, seg.clone());
    Ok(seg)
}

/// `subtree` as a complete, lowerable body pruned to `live`: a `Project` keeps its
/// live items, a clamp, set operation, top-N or window is cut with every column
/// and no map over it, and anything else is wrapped in a projection of its live
/// columns.
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
        RelExpr::Distinct { .. } | RelExpr::SetOp { .. } | RelExpr::Ordered { .. } => Rc::clone(subtree),
        // The HIR spelling of `SELECT <live cols> FROM <subtree>`.
        _ => {
            let items = ProjEntry::passthroughs(subtree.cols().into_iter().filter(|c| live.contains(&c.id)));
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
    match crate::expr_lower::compile_filter_program(preds, schema.columns())? {
        Some(prog) => Ok(cb.filter(node, prog.to_blob_bytes())),
        None => Ok(node),
    }
}
