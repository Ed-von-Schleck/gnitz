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
use gnitz_wire::{AggDescriptor, NodeId};
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

impl LoadedCircuit {
    /// `nid` is a reduce that owes a row over an empty input: one over no group
    /// columns reading an `ExchangeShard`.
    fn owes_ground_row(&self, nid: NodeId) -> bool {
        use gnitz_wire::OpNode;
        matches!(self.op(nid), OpNode::Reduce { group_cols, .. } if group_cols.is_empty())
            && matches!(self.op(self.inputs(nid)[0]), OpNode::ExchangeShard)
    }

    /// Some node `nid` reads, directly or transitively, owes a ground row.
    fn behind_ground_reduce(&self, nid: NodeId) -> bool {
        let behind = self.ancestors_inclusive(self.inputs(nid)[0]);
        (0..self.len()).any(|n| behind[n] && self.owes_ground_row(n))
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

/// Output from `compile_view`, consumed directly by DagEngine as the cached
/// plan: the view's one program, exchanges included.
pub(super) struct CompileOutput {
    pub(in crate::query) vm: Vm,
    /// source table id → the register its delta seeds.
    pub(in crate::query) source_reg_map: FxHashMap<u64, DeltaReg>,
    /// `Some` iff the view is capacity-bounded.
    pub(in crate::query) hydration: Option<Hydration>,
}

/// Compile view `view_id`'s already-loaded circuit under the routing `meta`
/// derived from it. Opens nothing: the returned layout declares every child
/// store the plan's operators address.
pub(super) fn compile_view(
    loaded: &LoadedCircuit,
    registry: &RelationRegistry,
    view_id: u64,
    view_schema: &SchemaDescriptor,
    meta: &ViewMeta,
    bounded: bool,
) -> Result<(CompileOutput, StateLayout), String> {
    let mut ctx = EmitCtx::new(loaded, registry, meta, (!bounded).then_some(view_id));
    for (nid, op) in loaded.ops() {
        let reg = emit_node(&mut ctx, nid, op)?;
        ctx.regs.push(reg);
    }
    let EmitCtx {
        prog, layout, regs, integrals, sources, ..
    } = ctx;
    let vm = prog.finish(regs[loaded.out()]);
    // Column count alone is not enough: equal counts with mismatched types would
    // let the client read a string descriptor out of integer storage.
    if !vm.out_schema().same_region_types(view_schema) {
        return Err("the circuit's output schema is not the view's".into());
    }
    let hydration = bounded
        .then(|| derive_hydration(loaded, registry, view_schema, &vm, &regs, &integrals))
        .transpose()?;
    let source_reg_map = sources.into_iter().map(|(tid, (seed, _))| (tid, seed)).collect();
    Ok((CompileOutput { vm, source_reg_map, hydration }, layout))
}

#[cfg(test)]
#[path = "tests/compiler.rs"]
mod tests;
