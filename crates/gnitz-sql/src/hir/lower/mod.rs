//! HIR → circuit lowering: one circuit per combine-class node and the linear
//! nodes around it, with nested combine subtrees cut to hidden segments. [`read`]
//! lowers an ad-hoc SELECT to one read of one relation instead, its `Reduce`
//! through [`fold`] to the fold sink.
//!
//! The rules every shell obeys, one home each:
//!
//! * input spine — `spine::open` / `Spine::emit`;
//! * segment cut — `Peeled::open`, [`cut_segment`];
//! * source collision — [`materialize`];
//! * reduce derivation — [`resolve_reduce_specs`], [`keyed_frame`];
//! * the linear levels above a core — `spine::peel` / `spine::emit_over`;
//! * addressing — `physical::Frame`, [`project_front`].

mod chain;
pub(crate) mod fold;
mod join;
mod ordered;
pub(crate) mod read;
mod reduce;
mod setop;
mod spine;

use super::physical::{self, Frame, Slot};
use super::{AggCol, ColId, ColSet, HirCol, HirExpr, ProjEntry, RelExpr, col_by_id};
use crate::agg::group_pk_def;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use chain::{EmitPieces, ViewChain};
use gnitz_core::{PlannedView, RelDescriptor, Schema};
use gnitz_expr::ReduceOutSlot;
use gnitz_wire::{AggDescriptor, ColumnDef, GroupAggs};
use spine::{Level, Top};
use std::rc::Rc;
use std::sync::Arc;

/// What a HIR `Reduce` becomes physically, before either lowering picks a sink:
/// its group columns as reduce-input positions, and one spec per distinct
/// physical aggregate column beside the output column it produces.
struct ReduceSpecs {
    /// Group columns as reduce-input positions, normalized, and the specs.
    agg: GroupAggs,
    /// Each spec's output column, parallel to `agg.aggs`; `None` for one no
    /// reference names.
    cols: Vec<Slot>,
}

impl ReduceSpecs {
    fn push(&mut self, spec: AggDescriptor, id: Option<ColId>, def: ColumnDef) {
        self.agg.aggs.push(spec);
        self.cols.push((id, def));
    }
}

/// Resolve a `Reduce`'s group and aggregate columns against its input.
fn resolve_reduce_specs(group_cols: &[ColId], aggs: &[AggCol], input: &Frame) -> Result<ReduceSpecs, GnitzSqlError> {
    let mut r = ReduceSpecs {
        agg: GroupAggs {
            group_cols: input.reduce_group(group_cols)?,
            aggs: Vec::with_capacity(aggs.len() + 1),
        },
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

/// A source a spine reads, with the frame its references resolve against.
#[derive(Clone)]
struct SegInput {
    src: SegSource,
    frame: Frame,
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
fn collect_live_cols<'a>(exprs: impl IntoIterator<Item = &'a HirExpr>, live: &mut ColSet) {
    for e in exprs {
        e.for_each_ref(&mut |id| {
            live.insert(*id);
        });
    }
}

/// `items` over `input` with `input`'s PK pinned to the front, the linear
/// projection, as the cheapest node producing them: the input itself or the
/// payload map.
fn project_front(
    cb: &mut gnitz_wire::Circuit,
    node: gnitz_wire::NodeId,
    items: &[ProjEntry],
    input: &Frame,
) -> Result<(gnitz_wire::NodeId, Frame), GnitzSqlError> {
    let (map, out) = physical::projection_map(items, input)?;
    Ok((map.map_or(node, |map| cb.map_expr(node, map)), out))
}

/// Lower a bound view body to its segments and the user-named view. The view lists its visible columns in
/// `names` order, each under the name its column of `names` gives it: the final
/// frame's regions under the SELECT list's numbering.
pub(crate) fn lower(rel: Rc<RelExpr>, names: &[HirCol]) -> Result<(Vec<PlannedView>, PlannedView), GnitzSqlError> {
    // A top-N carries its input's columns key first: the SELECT list is a
    // projection over it. A window body always ends in a projection.
    let rel = match rel.as_ref() {
        RelExpr::Ordered { .. } => RelExpr::project(Rc::clone(&rel), ProjEntry::passthroughs(rel.cols().into_owned())),
        _ => rel,
    };
    let mut chain = ViewChain::new(&rel);
    let top = lower_body(&mut chain, &rel)?;
    let visible = names.iter().filter(|c| !c.def.is_hidden).map(|c| c.id);
    let (schema, moved_to) = top.out.schema_in_order(visible)?;
    let mut schema = Arc::unwrap_or_clone(schema);
    for (slot, id) in top.out.layout.iter().enumerate() {
        if let Some(c) = id.and_then(|id| col_by_id(names, id)) {
            schema.rename_column(moved_to[slot] as usize, c.def.name.clone());
        }
    }
    Ok(chain.finish(top, Arc::new(schema)))
}

/// Lower a complete view body to circuit pieces: its linear levels over one
/// combine, in that combine's circuit, or over one source.
fn lower_body(chain: &mut ViewChain, rel: &Rc<RelExpr>) -> Result<EmitPieces, GnitzSqlError> {
    let live: ColSet = match rel.as_ref() {
        RelExpr::Project { items, .. } => items.iter().map(|it| it.out.id).collect(),
        _ => rel.cols().iter().map(|c| c.id).collect(),
    };
    let peeled = spine::peel(chain, rel, &live);
    let core = peeled.bottom;
    let mut pieces = match (peeled.owns_bottom(), core.as_ref()) {
        (true, RelExpr::Join { left, right, kind, on }) => {
            return join::lower_join_view(chain, peeled, [left, right], *kind, on);
        }
        (true, RelExpr::Reduce { input, group_cols, aggs }) => reduce::lower_reduce(chain, input, group_cols, aggs)?,
        (true, RelExpr::Ordered { input, partition, order, op }) => {
            ordered::lower_ordered(chain, input, partition, order, op)?
        }
        (true, RelExpr::Distinct { .. } | RelExpr::SetOp { .. }) => setop::lower_setop(chain, core)?,
        _ => return spine::lower_linear(peeled.open(chain)?),
    };
    // The body's circuit ends in its core node, which the levels read. A reduce
    // read only through renames stays the circuit's output, whose store holds its
    // history, so its rows are not copied into a second one: the columns no level
    // names are stored hidden beside the others.
    let node = pieces.circuit.out();
    let top = match core.as_ref() {
        RelExpr::Reduce { .. } if peeled.levels.iter().all(|(l, _)| matches!(l, Level::Project(_))) => Top::Store,
        _ => Top::Output,
    };
    pieces.out = spine::emit_over(&mut pieces.circuit, &peeled.levels, node, pieces.out, top)?.1;
    Ok(pieces)
}

/// Cut `rel` to a fresh hidden segment pruned to `live`, never one [`cut_segment`]
/// already made.
fn materialize(chain: &mut ViewChain, rel: &Rc<RelExpr>, live: &ColSet) -> Result<SegInput, GnitzSqlError> {
    let body = as_body(rel, live);
    let pieces = lower_body(chain, &body)?;
    Ok(chain.add_segment(pieces))
}

/// Cut a subtree to a hidden segment pruned to `live`, once per subtree.
fn cut_segment(chain: &mut ViewChain, subtree: &Rc<RelExpr>, live: &ColSet) -> Result<SegInput, GnitzSqlError> {
    // Every reader of a shared subtree reads all of it: its segment is one.
    let whole: ColSet;
    let live = match chain.is_shared(subtree) {
        true => {
            whole = subtree.cols().iter().map(|c| c.id).collect();
            &whole
        }
        false => live,
    };
    let key = Rc::as_ptr(subtree);
    if let Some(cached) = chain.cuts.get(&key) {
        debug_assert!(
            live.iter().all(|id| cached.frame.layout.contains(&Some(id))),
            "a shared subtree's segment holds every column a later reader needs"
        );
        return Ok(cached.clone());
    }
    let seg = materialize(chain, subtree, live)?;
    chain.cuts.insert(key, seg.clone());
    Ok(seg)
}

/// `subtree` as a complete, lowerable body pruned to `live`: a `Project` keeps its
/// live items, a clamp, set operation, top-N or window — or anything read whole —
/// is cut with every column and no map over it, and anything else is wrapped in a
/// projection of its live columns.
fn as_body(subtree: &Rc<RelExpr>, live: &ColSet) -> Rc<RelExpr> {
    match subtree.as_ref() {
        RelExpr::Project { input, items } => {
            let narrowed: Vec<ProjEntry> = items.iter().filter(|it| live.contains(it.out.id)).cloned().collect();
            if narrowed.len() == items.len() {
                Rc::clone(subtree)
            } else {
                RelExpr::project(Rc::clone(input), narrowed)
            }
        }
        RelExpr::Distinct { .. } | RelExpr::SetOp { .. } | RelExpr::Ordered { .. } => Rc::clone(subtree),
        _ if subtree.cols().iter().all(|c| live.contains(c.id)) => Rc::clone(subtree),
        // The HIR spelling of `SELECT <live cols> FROM <subtree>`.
        _ => {
            let items = ProjEntry::passthroughs(subtree.cols().iter().filter(|c| live.contains(c.id)).cloned());
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
