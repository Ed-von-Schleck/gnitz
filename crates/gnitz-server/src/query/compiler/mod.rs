//! Circuit compiler: reads system tables, builds a DBSP circuit graph,
//! derives its routing metadata, and emits VM instructions.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use rustc_hash::FxHashMap;

use crate::query::vm::{DeltaReg, ProgramBuilder, RegisterMeta, TraceReg, VmHandle};
use gnitz_expr::LogicalProgram;
use gnitz_store::expr::MapPlan;
use gnitz_store::relation::{Relation, RelationRegistry, StateIdx};
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
pub(super) use load::read_circuit_node_row;
pub(crate) use routing::RelayRoute;
pub(super) use routing::ViewMeta;

/// The most nodes one view's circuit may hold. A real circuit is 15–40 nodes;
/// the headroom is what makes the `u16` register and table ids safe under any
/// future node kind minting up to three of each (`3 × 16_384 + 2 < u16::MAX`).
pub(in crate::query) const MAX_CIRCUIT_NODES: usize = 16_384;

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
        // No planner path emits more: set-ops are binary, GROUP BY/DISTINCT unary.
        if shards.len() > 2 {
            return Err("more than two exchange nodes".into());
        }
        // The view routes a pair's output by one key.
        if let [(_, a), (_, b)] = shards[..] {
            if a != b {
                return Err("exchange sides shard on different keys".into());
            }
        }
        let mut claimed = vec![false; self.len()];
        let mut sides = Vec::with_capacity(shards.len());
        for (shard, cols) in shards {
            let ancestors = self.ancestors_inclusive(shard);
            // A node in two sides — a shared ancestor, or a shard upstream of
            // another shard — would open one scratch child twice.
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

/// A compiled sub-pipeline: the VM program, its register layout, and its
/// source-to-input-register map. One per `build_plan` call — an exchange side,
/// or the post-combine phase, which for an exchange-free circuit is the whole
/// plan.
pub(super) struct SubPlan {
    pub(in crate::query) vm: Box<VmHandle>,
    /// source table id → the input register its delta seeds.
    pub(in crate::query) source_reg_map: FxHashMap<i64, DeltaReg>,
}

/// One exchanged side: a sub-plan whose output is relayed into `seed_reg` of the
/// post phase.
pub(super) struct Side {
    pub(in crate::query) plan: SubPlan,
    pub(in crate::query) seed_reg: DeltaReg,
}

/// What a compiled view repartitions through: one sub-pipeline per
/// `ExchangeShard` in its circuit, each computing up to that shard, relayed, and
/// seeding the post-combine phase.
pub(super) enum Sides {
    /// No `ExchangeShard`: `post` is the whole plan. `hydration` is `Some` iff the
    /// view is capacity-bounded, which requires this shape.
    Unexchanged { hydration: Option<Hydration> },
    /// GROUP BY / SELECT DISTINCT / PK redistribution / range join.
    Unary(Side),
    /// A binary set-op.
    Pair([Side; 2]),
}

impl Sides {
    fn sides_mut(&mut self) -> &mut [Side] {
        match self {
            Sides::Unexchanged { .. } => &mut [],
            Sides::Unary(side) => std::slice::from_mut(side),
            Sides::Pair(pair) => pair,
        }
    }
}

/// Output from `compile_view`, consumed directly by DagEngine as the cached
/// plan.
///
/// It carries no routing: that lives once on the memoized `ViewMeta`, which the
/// worker dispatch and the master relay both read.
pub(super) struct CompileOutput {
    /// What the circuit repartitions through, ahead of `post`.
    pub(in crate::query) sides: Sides,
    /// The combine phase every side's relayed batch seeds — and, for a circuit
    /// with no `ExchangeShard`, the whole plan.
    pub(in crate::query) post: SubPlan,
}

impl CompileOutput {
    /// Every sub-plan of the view, for whole-plan sweeps (regfile clears,
    /// checkpoint table collection).
    pub(super) fn sub_plans_mut(&mut self) -> impl Iterator<Item = &mut SubPlan> {
        let Self { sides, post } = self;
        sides
            .sides_mut()
            .iter_mut()
            .map(|s| &mut s.plan)
            .chain(std::iter::once(post))
    }
}

/// One compile's two products: the plan the dag caches, and the plan-free
/// `ViewMeta` the master relay and the worker dispatch read.
pub(super) struct CompiledView {
    pub(in crate::query) output: CompileOutput,
    pub(in crate::query) meta: ViewMeta,
}

/// Where a view's rederived children are created, and under what policy.
#[derive(Clone, Copy)]
pub(super) struct ViewSite<'a> {
    pub(in crate::query) dir: &'a str,
    pub(in crate::query) id: u64,
    /// Answers the schemas, the circuit and the slot, and opens every scratch child.
    pub(in crate::query) registry: &'a RelationRegistry,
}

/// Compile a circuit for a single view: read the circuit from the system
/// tables, derive its routing metadata, carve it at its exchanges, then
/// `build_plan` each side and the post phase.
pub(super) fn compile_view(
    site: ViewSite<'_>,
    view_schema: &SchemaDescriptor,
    bounded: bool,
) -> Result<CompiledView, String> {
    let loaded = load::load_circuit(site.registry, site.id)?;
    let meta = ViewMeta::derive(&loaded, site.registry)?;
    let carve = loaded.carve()?;
    // A per-key replay of an exchanged plan would need the exchange to run too.
    if bounded && !carve.sides.is_empty() {
        return Err("bounded view: only a linear body and an inner equi-join are supported".into());
    }
    let placement = view_schema.placement();
    let mut side_plans = Vec::with_capacity(carve.sides.len());
    let mut seeds = Vec::with_capacity(carve.sides.len());
    for side in &carve.sides {
        let ex_in = loaded.inputs(side.shard).unary();
        let (plan, _) = build_plan(&loaded, &side.nodes, site, placement, &[], ex_in)?;
        let schema = plan.vm.program.out_schema();
        // `ScatterKey::new` bounds the same columns, but only mid-round in the
        // master relay; here a corrupt node is a `CREATE VIEW` rejection.
        if let Some(&c) = side.cols.iter().find(|&&c| schema.column(c as usize).is_none()) {
            return Err(OpBuildErr::oob_col("exchange shard: column", c, &schema).into());
        }
        seeds.push((side.shard, schema));
        side_plans.push(plan);
    }
    let (post, post_regs) = build_plan(&loaded, &carve.post, site, placement, &seeds, loaded.sink()?)?;
    // Column count alone is not enough: equal counts with mismatched types would
    // let the client read a string descriptor out of integer storage.
    if !post.vm.program.out_schema().same_physical_layout(view_schema) {
        return Err("sink schema does not match view output schema".into());
    }
    let mut sides = side_plans
        .into_iter()
        .zip(&carve.sides)
        .map(|(plan, c)| {
            Ok(Side {
                plan,
                seed_reg: post_regs[c.shard]
                    .ok_or("an exchange side seeds no register of the post phase")?
                    .delta()?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let sides = match sides.len() {
        0 => Sides::Unexchanged {
            hydration: bounded
                .then(|| derive_hydration(&loaded, &post, &post_regs))
                .transpose()?,
        },
        1 => Sides::Unary(sides.pop().expect("one side")),
        _ => Sides::Pair(sides.try_into().ok().expect("carve admits at most two sides")),
    };
    let mut output = CompileOutput { sides, post };
    // Past every fallible step: from here the plan owns its child stores. Every
    // other exit leaves each sub-plan's state armed, so it erases what it opened.
    for sub in output.sub_plans_mut() {
        sub.vm.state.commit();
    }
    Ok(CompiledView { output, meta })
}

#[cfg(test)]
#[path = "tests/compiler.rs"]
mod tests;
