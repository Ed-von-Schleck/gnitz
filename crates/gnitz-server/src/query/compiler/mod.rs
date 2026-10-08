//! Circuit compiler: loads a view's circuit, derives its routing metadata, and
//! emits VM instructions.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use std::rc::Rc;

use gnitz_expr::SchemaFacts;
use rustc_hash::FxHashMap;

use crate::query::vm::{DeltaReg, Integral, Vm};
use gnitz_expr::LogicalProgram;
use gnitz_store::relation::{Relation, RelationRegistry, StateIdx, StateLayout};
use gnitz_wire::{AggDescriptor, NodeId, MAX_CIRCUIT_NODES};
use gnitz_zset::algebra::MapPlan;
use gnitz_zset::algebra::ScatterPlan;
use gnitz_zset::schema::{Placement, SchemaDescriptor};

mod emit;
mod hydration;
mod load;
mod routing;

#[cfg(test)]
#[path = "tests/fixtures.rs"]
mod fixtures;

use emit::*;
use hydration::derive_hydration;
pub(super) use hydration::Hydration;

// `pub(super)` by default: `dag` is the only module that names the compiler, so
// a `pub(crate)` would publish it to the catalog and runtime rungs too.
pub(super) use load::load_circuit;
pub(super) use routing::{Relay, ViewMeta};

// Register and child-store ids are `u16`: a plan allocates a bounded number of
// each per node.
const _: () = assert!(3 * MAX_CIRCUIT_NODES < u16::MAX as usize);

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// A loaded circuit: the client's graph, whose index order is a topological
/// order because every input names an earlier node. It holds at least one node,
/// and the last is its output.
pub(super) struct LoadedCircuit(gnitz_wire::Circuit);

impl LoadedCircuit {
    /// Every relation the circuit scans, once per scan.
    pub(in crate::query) fn sources(&self) -> impl Iterator<Item = u64> + '_ {
        self.0.sources()
    }

    fn len(&self) -> usize {
        self.0.nodes().len()
    }

    /// The node whose rows are the view's.
    fn out(&self) -> NodeId {
        self.len() - 1
    }

    fn op(&self, nid: NodeId) -> &gnitz_wire::OpNode {
        &self.0.nodes()[nid].op
    }

    /// `nid`'s producers, in slot order.
    fn inputs(&self, nid: NodeId) -> &[NodeId] {
        self.0.nodes()[nid].inputs()
    }

    /// Every operator, in topological order — identical on the master and on
    /// every worker, so which of several matches a walk takes is too.
    fn ops(&self) -> impl Iterator<Item = (NodeId, &gnitz_wire::OpNode)> {
        self.0.nodes().iter().map(|n| &n.op).enumerate()
    }

    /// Every `ExchangeShard`, in topological order.
    fn exchange_shards(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.ops()
            .filter(|(_, op)| matches!(op, gnitz_wire::OpNode::ExchangeShard))
            .map(|(nid, _)| nid)
    }

    /// The node ids restricted to `keep`, in topological order. Every node list a
    /// plan is built over is produced this way.
    fn ordered_where(&self, keep: impl Fn(NodeId) -> bool) -> Vec<NodeId> {
        (0..self.len()).filter(|&n| keep(n)).collect()
    }

    /// Every node reading `nid`, in topological order.
    fn readers(&self, nid: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        (nid + 1..self.len()).filter(move |&n| self.inputs(n).contains(&nid))
    }

    /// Backward pass: `start` and every node it reads, directly or transitively.
    fn ancestors_inclusive(&self, start: NodeId) -> Vec<bool> {
        let mut reached = vec![false; self.len()];
        reached[start] = true;
        for n in (0..=start).rev() {
            if reached[n] {
                for &p in self.inputs(n) {
                    reached[p] = true;
                }
            }
        }
        reached
    }
}

/// True iff `op` carries every row through on its own worker with the PK region
/// intact.
fn keeps_rows_and_pk_region(op: &gnitz_wire::OpNode) -> bool {
    use gnitz_wire::{MapKind, OpNode};
    matches!(
        op,
        OpNode::Filter(_) | OpNode::Map(MapKind::Projection(_) | MapKind::Compute(_))
    )
}

/// Walk back from `from` through nodes that keep rows and the PK region, to the
/// first node that does not.
fn row_local_origin(loaded: &LoadedCircuit, mut from: NodeId) -> NodeId {
    while keeps_rows_and_pk_region(loaded.op(from)) {
        from = loaded.inputs(from)[0];
    }
    from
}

// ---------------------------------------------------------------------------
// Carve — the circuit split at its exchanges
// ---------------------------------------------------------------------------

/// One side of a [`Carve`]: an `ExchangeShard` and the nodes computing its input
/// — the shard's ancestors — in topological order.
struct CarvedSide {
    shard: NodeId,
    nodes: Vec<NodeId>,
}

/// The circuit split at its exchanges: one side per `ExchangeShard`, and the
/// post phase — every node in no side, the shards excluded. For an
/// exchange-free circuit `post` is the whole circuit.
struct Carve {
    sides: Vec<CarvedSide>,
    post: Vec<NodeId>,
}

impl LoadedCircuit {
    fn carve(&self) -> Result<Carve, String> {
        let mut claimed = vec![false; self.len()];
        let mut sides = Vec::new();
        for shard in self.exchange_shards() {
            let ancestors = self.ancestors_inclusive(shard);
            // A node in two sides — a shared ancestor, or a shard upstream of
            // another shard — would declare one scratch child twice.
            if ancestors.iter().zip(&claimed).any(|(&a, &c)| a && c) {
                return Err("exchange sides share a node".into());
            }
            for (c, &a) in claimed.iter_mut().zip(&ancestors) {
                *c |= a;
            }
            let nodes = self.ordered_where(|n| n != shard && ancestors[n]);
            sides.push(CarvedSide { shard, nodes });
        }
        let post = self.ordered_where(|n| !claimed[n]);
        // A delta is routed to the side scanning its source, so a post-phase scan
        // would receive nothing.
        let post_scans = post
            .iter()
            .any(|&n| matches!(self.op(n), gnitz_wire::OpNode::ScanDelta { .. }));
        if !sides.is_empty() && post_scans {
            return Err("an exchanged plan scans a relation outside every exchange side".into());
        }
        Ok(Carve { sides, post })
    }

    /// The `Reduce` or `TopN` reading `shard`, and its group columns — the key the
    /// exchange co-locates by. `None` where no reader is one: the rows are placed by
    /// their own PK.
    fn keyed_reader(&self, shard: NodeId) -> Result<Option<(NodeId, &[u32])>, String> {
        use gnitz_wire::OpNode::{Reduce, TopN};
        let keyed = self.readers(shard).find_map(|n| match self.op(n) {
            Reduce { group_cols, .. } | TopN { group_cols, .. } => Some((n, group_cols.as_slice())),
            _ => None,
        });
        // A second reader would be handed rows placed by a key that is not its own.
        if keyed.is_some() && self.readers(shard).nth(1).is_some() {
            return Err("an exchange in front of a reduce or top-N has another reader".into());
        }
        Ok(keyed)
    }
}

// ---------------------------------------------------------------------------
// CompileOutput — typed compilation result
// ---------------------------------------------------------------------------

/// A compiled sub-pipeline: the VM and the sources whose delta seeds it. One
/// per `build_plan` call — an exchange side, or the post-combine phase, which
/// for an exchange-free circuit is the whole plan.
pub(super) struct SubPlan {
    pub(in crate::query) vm: Vm,
    /// source table id → the input register its delta seeds.
    pub(in crate::query) source_reg_map: FxHashMap<u64, DeltaReg>,
}

impl SubPlan {
    /// `src`'s delta seeds a register here that folds it.
    pub(in crate::query) fn seed_folds(&self, src: u64) -> bool {
        self.source_reg_map.get(&src).is_some_and(|&r| self.vm.folds(r))
    }
}

/// One exchanged side: a sub-plan whose output is relayed into `seed_reg` of the
/// post phase.
pub(super) struct Side {
    pub(in crate::query) plan: SubPlan,
    pub(in crate::query) seed_reg: DeltaReg,
    /// How the output reaches the post phase; `None` when it stays where it is.
    pub(in crate::query) relay: Option<Relay>,
}

/// Every relation the side scans is replicated and nothing trims its result, so
/// each worker's output is a copy of the others'.
fn emits_replica(plan: &SubPlan, registry: &RelationRegistry) -> bool {
    plan.source_reg_map
        .keys()
        .all(|tid| registry.relation(*tid).is_some_and(|r| r.placement().is_replicated()))
        && !plan.vm.trims_per_worker()
}

/// Output from `compile_view`, consumed directly by DagEngine as the cached
/// plan.
///
/// A source's routing lives once on the `ViewMeta` derived at the view's
/// registration; each side carries the relay its own output takes.
pub(super) struct CompileOutput {
    /// One per `ExchangeShard`, in circuit order, each relayed into `post`.
    pub(in crate::query) sides: Vec<Side>,
    /// The combine phase every side's relayed batch seeds — and, for a circuit
    /// with no `ExchangeShard`, the whole plan.
    pub(in crate::query) post: SubPlan,
    /// `Some` iff the view is capacity-bounded.
    pub(in crate::query) hydration: Option<Hydration>,
}

/// Compile one view's already-loaded circuit under the routing `meta` derived
/// from it: carve it at its exchanges, then `build_plan` each side and the post
/// phase. Opens nothing: the returned layout declares every child store the
/// plan's operators address.
pub(super) fn compile_view(
    loaded: &LoadedCircuit,
    registry: &RelationRegistry,
    view_schema: &SchemaDescriptor,
    meta: &ViewMeta,
    bounded: bool,
) -> Result<(CompileOutput, StateLayout), String> {
    let carve = loaded.carve()?;
    let mut layout = StateLayout::default();
    let mut side_plans = Vec::with_capacity(carve.sides.len());
    let mut seeds = Vec::with_capacity(carve.sides.len());
    for side in &carve.sides {
        let reader = loaded.keyed_reader(side.shard)?;
        // A worker holding the whole input has nothing to pre-aggregate.
        let out = match reader.filter(|(_, group)| group.is_empty() && !meta.self_contained) {
            Some((consumer, _)) => PlanOut::Split { consumer },
            None => PlanOut::Node(loaded.inputs(side.shard)[0]),
        };
        let Built { plan, partial, .. } = build_plan(loaded, &side.nodes, registry, &mut layout, meta, &[], out)?;
        let schema = *plan.vm.out_schema();
        let scatter = Rc::new(match reader {
            Some((_, group)) => ScatterPlan::group(&schema, group)?,
            None => ScatterPlan::native(Placement::full_pk(&schema)),
        });
        let stays = meta.self_contained
            || (carve.sides.len() == 1 && routing::skips_output_exchange(loaded, side.shard, &scatter, registry));
        let relay = match () {
            _ if stays => None,
            _ if emits_replica(&plan, registry) => Some(Relay::Share(scatter)),
            _ => Some(Relay::Round(scatter)),
        };
        seeds.push(Seed {
            shard: side.shard,
            schema,
            partials: partial,
        });
        side_plans.push((plan, relay));
    }
    let Built {
        plan: post,
        regs: post_regs,
        integrals: post_integrals,
        seed_regs,
        ..
    } = build_plan(
        loaded,
        &carve.post,
        registry,
        &mut layout,
        meta,
        &seeds,
        match bounded {
            true => PlanOut::Node(loaded.out()),
            false => PlanOut::Store(loaded.out()),
        },
    )?;
    // Column count alone is not enough: equal counts with mismatched types would
    // let the client read a string descriptor out of integer storage.
    if !post.vm.out_schema().same_region_types(view_schema) {
        return Err("the circuit's output schema is not the view's".into());
    }
    let sides = side_plans
        .into_iter()
        .zip(seed_regs)
        .map(|((plan, relay), seed_reg)| Side { plan, seed_reg, relay })
        .collect();
    let hydration = bounded
        .then(|| derive_hydration(loaded, registry, view_schema, &post, &post_regs, &post_integrals))
        .transpose()?;
    Ok((CompileOutput { sides, post, hydration }, layout))
}

#[cfg(test)]
#[path = "tests/compiler.rs"]
mod tests;
