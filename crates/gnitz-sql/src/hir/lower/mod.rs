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
//! * addressing — `physical::Frame`, [`project_front`], [`project_tail`];
//! * exchange topology — asserted in `ViewChain::push`.

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
use super::{as_col, slots_of, split_filter, ColId, GetSource, HirAgg, HirExpr, HirRef, ProjEntry, RelExpr};
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
/// post-node WHERE (outer joins only; empty for INNER — the rewrite folded the
/// WHERE into the residual).
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

/// A resolved combine input: its delta source and the frame against which key /
/// group / projection references resolve. A base/segment `Get` maps directly; a
/// cut subtree resolves to its hidden segment via [`cut_segment`].
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
/// read through several `Alias`es) is cut to one hidden segment. It is why the
/// HIR→HIR rewrites preserve `Rc` identity. Every key is a node of the lowered tree.
pub(crate) type CutMemo = HashMap<*const RelExpr, SegInput>;

/// Insert every `HirRef::Col` id referenced anywhere in `exprs` into `live` — the
/// one home for "which columns does this expression set demand", used to prune a
/// cut input's segment to exactly the columns its parent reads.
pub(crate) fn collect_live_cols<'a>(exprs: impl IntoIterator<Item = &'a HirExpr>, live: &mut HashSet<ColId>) {
    for e in exprs {
        e.for_each_ref(&mut |r| {
            if let HirRef::Col(id) = r {
                live.insert(*id);
            }
        });
    }
}

/// The cheapest node producing `items` over `input`, whose key region the engine
/// carries verbatim: the input itself, a copy list, or the payload expression map.
fn emit_projection(
    cb: &mut gnitz_core::CircuitBuilder,
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
    cb: &mut gnitz_core::CircuitBuilder,
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
    cb: &mut gnitz_core::CircuitBuilder,
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

/// Lower a bound + classified `RelExpr` tree to circuit pieces.
pub(crate) fn lower(chain: &mut ViewChain, rel: Rc<RelExpr>, bounded: bool) -> Result<EmitPieces, GnitzSqlError> {
    if bounded {
        reject_ineligible_capacity_body(&rel)?;
    }
    let mut memo = CutMemo::new();
    lower_body(chain, &mut memo, &rel)
}

/// Reject a body a capacity-bounded view cannot have. Eligibility is a **positive
/// list** of exactly the two shapes per-key hydration can replay: a
/// filter/projection over one relation, and a plain inner equi-join. Anything else
/// falls through to a rejection, so a new body shape does not silently inherit
/// eligibility.
///
/// One classifier over the bound tree, asked once before lowering, rather than a
/// rule restated in each `lower_body` arm: a per-arm rule is one a new arm can
/// forget, and it reads as a whitelist while behaving as a blacklist.
/// `build_query_segments` supplies the other half of the rule (the body must not
/// have cut into hidden segments), which no single-tree walk can see.
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
                RelExpr::Join { kind: JoinType::Inner, on, .. } => match on.class()?.shape() {
                    // A range/band join's null-fill threshold pipeline is not
                    // replayable per key; a cross join has no key at all, so a
                    // skeleton row names no trace group to replay.
                    JoinShape::Range => "a range or band join",
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
                RelExpr::Join {
                    kind: JoinType::Semi | JoinType::Anti | JoinType::Mark(_),
                    ..
                } => exists::lower_exists_view(chain, memo, items, fpreds, source),
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

/// Resolve one join input to a `SegInput`: read in place when
/// [`resolve_in_place`] can, else cut to a hidden segment.
pub(crate) fn resolve_input(
    chain: &mut ViewChain,
    memo: &mut CutMemo,
    input: &Rc<RelExpr>,
    live: &HashSet<ColId>,
) -> Result<SegInput, GnitzSqlError> {
    match resolve_in_place(chain, memo, input)? {
        Some(seg) => Ok(seg),
        None => cut_segment(chain, memo, input, live),
    }
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
            let SegInput { tid, frame, desc } = resolve_input(chain, memo, inner, &all)?;
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
    cb: &mut gnitz_core::CircuitBuilder,
    node: gnitz_core::NodeId,
    preds: &[BoundExpr],
    schema: &Schema,
) -> Result<gnitz_core::NodeId, GnitzSqlError> {
    match crate::expr_lower::compile_filter_program(preds, &schema.columns)? {
        Some(prog) => Ok(cb.filter(node, prog)),
        None => Ok(node),
    }
}

/// Resolve `preds` against `frame` and emit the filter. Shared by the emits that
/// filter an already-emitted node: HAVING, the join and EXISTS residuals and
/// prefilters, and a mark branch's WHERE.
pub(crate) fn emit_filter<'a>(
    cb: &mut gnitz_core::CircuitBuilder,
    node: gnitz_core::NodeId,
    preds: impl IntoIterator<Item = &'a HirExpr>,
    frame: &Frame,
) -> Result<gnitz_core::NodeId, GnitzSqlError> {
    filter(cb, node, &physical::resolve_preds(preds, &frame.layout)?, &frame.schema)
}

/// One side of a join as [`join_sides`] left it: the resolved input, its
/// reindex-payload keep list in emission order, those columns' defs, and the
/// pinned source PK's arity (`0` when unpinned).
pub(crate) struct JoinSide {
    pub(crate) seg: SegInput,
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
        self.keep.iter().map(|&i| self.seg.frame.layout[i as usize])
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
pub(crate) fn join_sides(down: Demand<'_>, class: &JoinClass, kind: JoinType, inputs: [SegInput; 2]) -> [JoinSide; 2] {
    let [left_in, right_in] = inputs;
    let (left_layout, right_layout) = (&left_in.frame.layout, &right_in.frame.layout);
    // One keep list per side, so a rule cannot write into the region another rule
    // owns.
    let mut keep = [vec![false; left_layout.len()], vec![false; right_layout.len()]];
    let mark = |keep: &mut [Vec<bool>; 2], id: ColId| {
        if let Some(p) = left_layout.iter().position(|c| *c == id) {
            keep[0][p] = true;
        } else if let Some(p) = right_layout.iter().position(|c| *c == id) {
            keep[1][p] = true;
        }
        // An id in neither layout is the mark column, which the mark branch
        // substitutes by its `0/1` constant before resolving anything.
    };
    // Rules 1 + 2: projection, residual ON + top-level WHERE.
    let mut referenced: HashSet<ColId> = HashSet::new();
    down.refs(&mut referenced);
    collect_live_cols(&class.residual, &mut referenced);
    for id in referenced {
        mark(&mut keep, id);
    }
    // Rule 3: a side with a ν keeps its nullable join-key components. `map_reindex`
    // collapses a NULL key to synthetic PK 0, so pruning the key would make a
    // NULL-keyed row and a real `k = 0` row byte-identical and the NULL row's
    // null-fill would cancel against the real row's matched multiplicity. Only the
    // equi shapes need it — the band and pure-range ν key on the source PK, where
    // NULL and 0 cannot collide — so elsewhere it merely over-keeps.
    for (side, is_left, seg) in [(0, true, &left_in), (1, false, &right_in)] {
        if !kind.has_nu(is_left) {
            continue;
        }
        for p in &class.eq {
            let id = if is_left { p.left } else { p.right };
            if let Some(pos) = seg.frame.layout.iter().position(|c| *c == id) {
                if seg.frame.schema.columns[pos].is_nullable {
                    keep[side][pos] = true;
                }
            }
        }
    }
    // Rule 4: a shape that packs its output key out of the payload pins that
    // side's `pk_cols` to the front of its keep list, which `one_side` prepends. A
    // range-correlated EXISTS/IN reaches no null-fill tail, so only its outer PK.
    let packs_source_pk = matches!(class.shape(), JoinShape::Range | JoinShape::Cross);
    let outer_only = kind.is_decorrelated();
    let pins = [packs_source_pk, packs_source_pk && !outer_only];
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
    [one_side(left_in, &kl, pins[0]), one_side(right_in, &kr, pins[1])]
}

/// One side's keep list in emission order: its pinned PK columns first, then every
/// other kept column in source order. A keep list need not ascend, and the PK at
/// the front is what makes every pair-PK slot list a range.
fn one_side(seg: SegInput, keep: &[bool], pin_pk: bool) -> JoinSide {
    let pinned: Vec<u32> = if pin_pk {
        seg.frame.schema.pk_cols.clone()
    } else {
        Vec::new()
    };
    let mut cols = pinned.clone();
    cols.extend((0..keep.len() as u32).filter(|i| keep[*i as usize] && !pinned.contains(i)));
    let coldefs = cols
        .iter()
        .map(|&i| seg.frame.schema.columns[i as usize].clone())
        .collect();
    JoinSide {
        pk_arity: pinned.len(),
        seg,
        keep: cols,
        coldefs,
    }
}

#[cfg(test)]
#[path = "tests/lower.rs"]
mod tests;
