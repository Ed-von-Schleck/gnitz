//! Circuit compiler: reads system tables, builds a DBSP circuit graph,
//! derives its routing metadata, and emits VM instructions.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use gnitz_expr::SchemaFacts;
use rustc_hash::FxHashMap;

use crate::query::vm::{DeltaReg, Vm};
use gnitz_expr::LogicalProgram;
use gnitz_store::expr::MapPlan;
use gnitz_store::ops::ScatterSpec;
use gnitz_store::relation::{Relation, RelationRegistry, StateIdx, StateLayout};
use gnitz_store::schema::{OpBuildErr, SchemaDescriptor};
use gnitz_wire::{AggDescriptor, NodeId, NodeInputs};

mod emit;
mod hydration;
mod load;
mod routing;

#[cfg(test)]
#[path = "tests/fixtures.rs"]
pub(in crate::query) mod fixtures;

use emit::*;
use hydration::derive_hydration;
pub(super) use hydration::{Hydration, HydrationSeed};

// `pub(super)` by default: `dag` is the only module that names the compiler, so
// a `pub(crate)` would publish it to the catalog and runtime rungs too.
pub(super) use load::{load_circuit, read_circuit_node_row};
pub(super) use routing::ViewMeta;
pub(crate) use routing::OUTPUT_RELAY;

/// The most nodes one view's circuit may hold.
pub(crate) const MAX_CIRCUIT_NODES: usize = 16_384;
// Registers — up to three per node, plus the seeds — and child stores — up to
// two per node — are `u16` ids.
const _: () = assert!(3 * MAX_CIRCUIT_NODES + 2 < u16::MAX as usize);
const _: () = assert!(2 * MAX_CIRCUIT_NODES <= u16::MAX as usize);

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// A loaded circuit: the client's graph, whose index order is a topological
/// order because every input names an earlier node.
///
/// Opaque outside this module: everything the rest of the engine wants from a
/// circuit is derived once into [`ViewMeta`], so nothing else can read a
/// second answer out of the graph.
pub(super) struct LoadedCircuit(gnitz_wire::Circuit);

impl LoadedCircuit {
    /// The only constructor, so the one place the node cap has to hold.
    fn new(circuit: gnitz_wire::Circuit) -> Result<Self, String> {
        if circuit.nodes().len() > MAX_CIRCUIT_NODES {
            return Err("circuit exceeds the node limit".into());
        }
        Ok(LoadedCircuit(circuit))
    }

    fn len(&self) -> usize {
        self.0.nodes().len()
    }

    fn op(&self, nid: NodeId) -> &gnitz_wire::OpNode {
        &self.0.nodes()[nid].op
    }

    fn inputs(&self, nid: NodeId) -> &NodeInputs {
        &self.0.nodes()[nid].inputs
    }

    /// Every operator, in topological order — identical on the master and on
    /// every worker, so which of several matches a walk takes is too.
    fn ops(&self) -> impl Iterator<Item = (NodeId, &gnitz_wire::OpNode)> {
        self.0.nodes().iter().map(|n| &n.op).enumerate()
    }

    /// Every `ExchangeShard` and its key, in topological order — so the last is
    /// the sink-nearest, the one whose key the view's output carries.
    fn exchange_shards(&self) -> impl Iterator<Item = (NodeId, &[u32])> {
        self.ops().filter_map(|(nid, op)| match op {
            gnitz_wire::OpNode::ExchangeShard { shard_cols } => Some((nid, shard_cols.as_slice())),
            _ => None,
        })
    }

    /// The node ids restricted to `keep`, in topological order. Every node list a
    /// plan is built over is produced this way.
    fn ordered_where(&self, keep: impl Fn(NodeId) -> bool) -> Vec<NodeId> {
        (0..self.len()).filter(|&n| keep(n)).collect()
    }

    /// Backward pass: `start` and every node it reads, directly or transitively.
    fn ancestors_inclusive(&self, start: NodeId) -> Vec<bool> {
        let mut reached = vec![false; self.len()];
        reached[start] = true;
        for n in (0..=start).rev() {
            if reached[n] {
                for p in self.inputs(n).iter() {
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
/// first node that does not: `(that node, whether a Map was crossed)`.
fn row_local_origin(loaded: &LoadedCircuit, mut from: NodeId) -> (NodeId, bool) {
    let mut mapped = false;
    while keeps_rows_and_pk_region(loaded.op(from)) {
        mapped |= matches!(loaded.op(from), gnitz_wire::OpNode::Map(_));
        from = loaded.inputs(from).unary();
    }
    (from, mapped)
}

// ---------------------------------------------------------------------------
// Carve — the circuit split at its exchanges
// ---------------------------------------------------------------------------

/// One side of a [`Carve`]: an `ExchangeShard`, its key, and the nodes computing
/// its input — the shard's ancestors — in topological order.
struct CarvedSide<'a> {
    shard: NodeId,
    cols: &'a [u32],
    nodes: Vec<NodeId>,
}

/// The circuit split at its exchanges: one side per `ExchangeShard`, and the
/// post phase — every node in no side, the shards excluded. For an
/// exchange-free circuit `post` is the whole circuit.
struct Carve<'a> {
    sides: Vec<CarvedSide<'a>>,
    post: Vec<NodeId>,
}

impl LoadedCircuit {
    fn carve(&self) -> Result<Carve<'_>, String> {
        let shards: Vec<(NodeId, &[u32])> = self.exchange_shards().collect();
        // The view routes every side's output by one key.
        if !shards.windows(2).all(|w| w[0].1 == w[1].1) {
            return Err("exchange sides shard on different keys".into());
        }
        let mut claimed = vec![false; self.len()];
        let mut sides = Vec::with_capacity(shards.len());
        for (shard, cols) in shards {
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
            sides.push(CarvedSide { shard, cols, nodes });
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

    /// `shard`'s one reader, when it is a global `Reduce` or `TopN` behind an empty
    /// key: the node whose partial a side may end in.
    fn global_split(&self, shard: NodeId) -> Option<NodeId> {
        if !matches!(self.op(shard), gnitz_wire::OpNode::ExchangeShard { shard_cols } if shard_cols.is_empty()) {
            return None;
        }
        let mut readers = (shard + 1..self.len()).filter(|&n| self.inputs(n).iter().any(|p| p == shard));
        let (Some(consumer), None) = (readers.next(), readers.next()) else {
            return None;
        };
        match self.op(consumer) {
            gnitz_wire::OpNode::Reduce { group_cols, .. } | gnitz_wire::OpNode::TopN { group_cols, .. }
                if group_cols.is_empty() =>
            {
                Some(consumer)
            }
            _ => None,
        }
    }

    /// The circuit's one `IntegrateSink`.
    fn sink(&self) -> Result<NodeId, String> {
        let mut sinks = self
            .ops()
            .filter(|(_, op)| matches!(op, gnitz_wire::OpNode::IntegrateSink))
            .map(|(nid, _)| nid);
        match (sinks.next(), sinks.next()) {
            (Some(sink), None) => Ok(sink),
            (None, _) => Err("circuit has no IntegrateSink".into()),
            (Some(_), Some(_)) => Err("circuit has more than one IntegrateSink".into()),
        }
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

/// One exchanged side: a sub-plan whose output is relayed into `seed_reg` of the
/// post phase.
pub(super) struct Side {
    pub(in crate::query) plan: SubPlan,
    pub(in crate::query) seed_reg: DeltaReg,
    /// Every worker computes this side's whole output, so a muted round must
    /// publish nothing rather than relay each row `W` times.
    pub(in crate::query) emits_replica: bool,
}

/// Every relation the side scans is replicated and nothing trims its result, so
/// each worker's output is a copy of the others'.
fn emits_replica(plan: &SubPlan, registry: &RelationRegistry) -> bool {
    plan.source_reg_map
        .keys()
        .all(|tid| registry.relation(*tid).is_some_and(Relation::is_replicated))
        && !plan.vm.program.trims_per_worker()
}

/// Output from `compile_view`, consumed directly by DagEngine as the cached
/// plan.
///
/// It carries no routing: that lives once on the `ViewMeta` derived at the
/// view's registration.
pub(super) struct CompileOutput {
    /// One per `ExchangeShard`, in circuit order, each relayed into `post`.
    pub(in crate::query) sides: Vec<Side>,
    /// The combine phase every side's relayed batch seeds — and, for a circuit
    /// with no `ExchangeShard`, the whole plan.
    pub(in crate::query) post: SubPlan,
    /// `Some` iff the view is capacity-bounded.
    pub(in crate::query) hydration: Option<Hydration>,
    /// This worker computes the view's whole result locally: it is replicated, or
    /// this process is the only worker.
    pub(in crate::query) self_contained: bool,
}

impl CompileOutput {
    /// Every sub-plan of the view, for whole-plan register clears.
    pub(super) fn sub_plans_mut(&mut self) -> impl Iterator<Item = &mut SubPlan> {
        let Self { sides, post, .. } = self;
        sides.iter_mut().map(|s| &mut s.plan).chain(std::iter::once(post))
    }
}

/// Compile one view's already-loaded circuit: carve it at its exchanges, then
/// `build_plan` each side and the post phase. Opens nothing: the returned
/// layout declares every child store the plan's operators address.
pub(super) fn compile_view(
    loaded: &LoadedCircuit,
    registry: &RelationRegistry,
    view_schema: &SchemaDescriptor,
    bounded: bool,
) -> Result<(CompileOutput, StateLayout), String> {
    let carve = loaded.carve()?;
    let self_contained = view_schema.placement().is_replicated() || registry.slot().of <= 1;
    let mut layout = StateLayout::default();
    let mut side_plans = Vec::with_capacity(carve.sides.len());
    let mut seeds = Vec::with_capacity(carve.sides.len());
    for side in &carve.sides {
        // A worker holding the whole input has nothing to pre-aggregate.
        let out = match loaded.global_split(side.shard).filter(|_| !self_contained) {
            Some(consumer) => PlanOut::Split { consumer },
            None => PlanOut::Node(loaded.inputs(side.shard).unary()),
        };
        let Built { plan, partial, .. } =
            build_plan(loaded, &side.nodes, registry, &mut layout, self_contained, &[], out)?;
        let schema = *plan.vm.program.out_schema();
        // Workers route by this spec mid-round, where a refusal is fatal.
        ScatterSpec::GroupKey(side.cols).check(&schema)?;
        seeds.push(Seed {
            shard: side.shard,
            schema,
            partials: partial,
        });
        side_plans.push(plan);
    }
    let Built { plan: post, regs: post_regs, .. } = build_plan(
        loaded,
        &carve.post,
        registry,
        &mut layout,
        self_contained,
        &seeds,
        PlanOut::Node(loaded.sink()?),
    )?;
    // Column count alone is not enough: equal counts with mismatched types would
    // let the client read a string descriptor out of integer storage.
    if !post.vm.program.out_schema().same_layout(view_schema) {
        return Err("sink schema does not match view output schema".into());
    }
    let sides = side_plans
        .into_iter()
        .zip(&carve.sides)
        .map(|(plan, c)| {
            Ok(Side {
                emits_replica: emits_replica(&plan, registry),
                plan,
                seed_reg: post_regs[c.shard]
                    .ok_or("an exchange side seeds no register of the post phase")?
                    .delta()?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let hydration = bounded
        .then(|| derive_hydration(loaded, registry, view_schema, &post, &post_regs))
        .transpose()?;
    Ok((CompileOutput { sides, post, hydration, self_contained }, layout))
}

#[cfg(test)]
#[path = "tests/compiler.rs"]
mod tests;
