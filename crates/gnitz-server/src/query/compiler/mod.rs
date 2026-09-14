//! Circuit compiler: reads system tables, builds a DBSP circuit graph,
//! derives its routing metadata, and emits VM instructions.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use std::collections::VecDeque;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::query::vm::{DeltaReg, ProgramBuilder, RegisterMeta, TraceReg, VmHandle};
use gnitz_expr::LogicalProgram;
use gnitz_store::expr::MapPlan;
use gnitz_store::relation::{Relation, RelationRegistry, StateIdx};
use gnitz_store::schema::{OpBuildErr, SchemaDescriptor};
use gnitz_wire::AggDescriptor;

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
pub(super) use load::for_each_scan_edge;
pub(crate) use routing::RelayRoute;
pub(super) use routing::ViewMeta;

/// The most nodes one view's circuit may hold. A real circuit is 15–40 nodes;
/// the headroom is what makes the `u16` register and table ids safe under any
/// future node kind minting up to three of each (`3 × 16_384 + 2 < u16::MAX`).
pub(in crate::query) const MAX_CIRCUIT_NODES: usize = 16_384;

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Typed circuit graph with OpNode payloads. Only `load::topo_sorted` builds
/// one, so `ordered` and the adjacency maps are populated for every value that
/// exists — no caller has to state a sortedness precondition.
///
/// Opaque outside this module: everything the rest of the engine wants from a
/// circuit is derived once into [`ViewMeta`], so nothing else can read a
/// second answer out of the graph.
pub(super) struct LoadedCircuit {
    nodes: FxHashMap<i32, gnitz_wire::OpNode>,
    ordered: Vec<i32>,
    outgoing: FxHashMap<i32, Vec<i32>>,
    inputs: FxHashMap<i32, NodeInputs>,
}

impl LoadedCircuit {
    /// `nid`'s operator. Total over `ordered`: `topo_sorted` builds `ordered`
    /// out of `nodes`' own keys.
    fn op(&self, nid: i32) -> &gnitz_wire::OpNode {
        self.nodes.get(&nid).expect("topo_sorted builds one entry per node")
    }

    /// Every operator, in topological order — the circuit's only iteration. A
    /// walk over `nodes` would answer by the hasher's order, which differs
    /// between the master and each worker, so which of several matches wins
    /// would too.
    fn ops(&self) -> impl Iterator<Item = (i32, &gnitz_wire::OpNode)> {
        self.ordered.iter().map(|&nid| (nid, self.op(nid)))
    }

    /// `nid`'s inputs. Total over `nodes`: `topo_sorted` builds one per node.
    fn inputs(&self, nid: i32) -> &NodeInputs {
        self.inputs.get(&nid).expect("topo_sorted builds one entry per node")
    }

    /// Every `ExchangeShard` and its key, in topological order — so the last is
    /// the sink-nearest, the one whose key the view's output carries.
    fn exchange_shards(&self) -> impl Iterator<Item = (i32, &[u32])> {
        self.ops().filter_map(|(nid, op)| match op {
            gnitz_wire::OpNode::ExchangeShard { shard_cols } => Some((nid, shard_cols.as_slice())),
            _ => None,
        })
    }

    /// `ordered`, restricted to `keep`. Every node list a plan is built over is
    /// produced this way, so a plan always sees the circuit's own topological
    /// order rather than whatever order the caller's set iterates in.
    fn ordered_where(&self, keep: impl Fn(i32) -> bool) -> Vec<i32> {
        self.ordered.iter().copied().filter(|&n| keep(n)).collect()
    }

    /// Every node reachable backwards from `start` (inclusive) — the
    /// sub-pipeline that produces its value.
    fn ancestors_inclusive(&self, start: i32) -> FxHashSet<i32> {
        let mut set = FxHashSet::default();
        let mut queue = VecDeque::from([start]);
        while let Some(cur) = queue.pop_front() {
            if set.insert(cur) {
                queue.extend(self.inputs(cur).iter());
            }
        }
        set
    }
}

/// A node's inputs in the shape its operator's arity allows. `topo_sorted`
/// settles the arity against `OpNode::arity()` before building one, so a reader
/// destructures instead of re-checking — and a `Filter` wired only on its trace
/// slot is unrepresentable rather than merely rejected downstream.
pub(super) enum NodeInputs {
    /// A `ScanDelta`: fed by the source drive, not by a producer.
    Source,
    Unary(i32),
    /// `a` is slot 0 — a join's delta side, a union's left operand; `b` is slot
    /// 1, the trace / right operand.
    Binary {
        a: i32,
        b: i32,
    },
}

impl NodeInputs {
    /// The producer of a unary operator's operand.
    fn unary(&self) -> i32 {
        match self {
            NodeInputs::Unary(src) => *src,
            _ => unreachable!("a unary operator fills exactly its one input slot"),
        }
    }

    /// The producers of a binary operator's two operands, in port order.
    fn binary(&self) -> (i32, i32) {
        match self {
            NodeInputs::Binary { a, b } => (*a, *b),
            _ => unreachable!("a binary operator is wired on both ports"),
        }
    }

    /// Every producer, for the walks that do not care about the operator.
    fn iter(&self) -> impl Iterator<Item = i32> {
        match *self {
            NodeInputs::Source => [None, None],
            NodeInputs::Unary(src) => [Some(src), None],
            NodeInputs::Binary { a, b } => [Some(a), Some(b)],
        }
        .into_iter()
        .flatten()
    }
}

// ---------------------------------------------------------------------------
// Carve — the circuit split at its exchanges
// ---------------------------------------------------------------------------

/// One side of a [`Carve`]: an `ExchangeShard`, its key, and the nodes computing
/// its input — the shard's ancestors — in topological order.
struct CarvedSide<'a> {
    shard: i32,
    cols: &'a [u32],
    nodes: Vec<i32>,
}

/// The circuit split at its exchanges: one side per `ExchangeShard`, and the
/// post phase — every node in no side, the shards excluded. For an
/// exchange-free circuit `post` is the whole circuit.
struct Carve<'a> {
    sides: Vec<CarvedSide<'a>>,
    post: Vec<i32>,
}

impl LoadedCircuit {
    fn carve(&self) -> Result<Carve<'_>, String> {
        let shards: Vec<(i32, &[u32])> = self.exchange_shards().collect();
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
        let mut claimed: FxHashSet<i32> = FxHashSet::default();
        let mut sides = Vec::with_capacity(shards.len());
        for (shard, cols) in shards {
            let ancestors = self.ancestors_inclusive(shard);
            // A node in two sides — a shared ancestor, or a shard upstream of
            // another shard — would open one scratch child twice.
            if !ancestors.iter().all(|&n| claimed.insert(n)) {
                return Err("exchange sides share a node".into());
            }
            let nodes = self.ordered_where(|n| n != shard && ancestors.contains(&n));
            sides.push(CarvedSide { shard, cols, nodes });
        }
        let post = self.ordered_where(|n| !claimed.contains(&n));
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
    fn sink(&self) -> Result<i32, String> {
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
                seed_reg: post_regs[&c.shard].delta()?,
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
