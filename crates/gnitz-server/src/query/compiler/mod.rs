//! Circuit compiler: derives a view's routing from its circuit and emits its VM
//! instructions. A plan reads the worker count and never this process's rank: the
//! master compiles before it forks, and every worker runs that plan.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use rustc_hash::FxHashMap;
use std::rc::Rc;

use crate::query::vm::{DeltaReg, Integral, Vm};
use gnitz_expr::LogicalProgram;
use gnitz_store::relation::{Relation, RelationRegistry, StateIdx, StateLayout};
use gnitz_wire::{AggDescriptor, Circuit, NodeId, ReadBound};
use gnitz_zset::algebra::MapPlan;
use gnitz_zset::algebra::Placement;
use gnitz_zset::algebra::ScatterPlan;
use gnitz_zset::schema::SchemaDescriptor;

mod emit;
mod hydration;
mod routing;

#[cfg(test)]
#[path = "tests/fixtures.rs"]
mod fixtures;

use emit::*;
use hydration::derive_hydration;
// `pub(super)`: `dag` is the only module that names the compiler, so a
// `pub(crate)` would publish it to the catalog and runtime rungs too.
pub(super) use hydration::Hydration;
use routing::ViewMeta;

// ---------------------------------------------------------------------------
// Circuit walks
// ---------------------------------------------------------------------------

/// Every `ExchangeShard`, in topological order.
fn exchange_shards(circuit: &Circuit) -> impl Iterator<Item = NodeId> + '_ {
    circuit
        .ops()
        .filter(|(_, op)| matches!(op, gnitz_wire::OpNode::ExchangeShard))
        .map(|(nid, _)| nid)
}

/// Backward pass: `start` and every node it reads, directly or transitively.
fn ancestors_inclusive(circuit: &Circuit, start: NodeId) -> Vec<bool> {
    let mut reached = vec![false; circuit.nodes().len()];
    reached[start] = true;
    for n in (0..=start).rev() {
        if reached[n] {
            for &p in circuit.inputs(n) {
                reached[p] = true;
            }
        }
    }
    reached
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
fn row_local_origin(circuit: &Circuit, mut from: NodeId) -> NodeId {
    while keeps_rows_and_pk_region(circuit.op(from)) {
        from = circuit.inputs(from)[0];
    }
    from
}

/// Some node `nid` reads, directly or transitively, owes a ground row.
fn behind_ground_reduce(circuit: &Circuit, nid: NodeId) -> bool {
    let behind = ancestors_inclusive(circuit, circuit.inputs(nid)[0]);
    (0..circuit.nodes().len()).any(|n| behind[n] && circuit.owes_ground_row(n))
}

/// The `Reduce` or `TopN` reading `shard`, and its group columns — the key the
/// exchange co-locates by. `None` where no reader is one: the rows are placed by
/// their own PK.
fn keyed_reader(circuit: &Circuit, shard: NodeId) -> Result<Option<(NodeId, &[u32])>, String> {
    use gnitz_wire::OpNode::{Reduce, TopN};
    let keyed = circuit.readers(shard).find_map(|n| match circuit.op(n) {
        Reduce { group_cols, .. } | TopN { group_cols, .. } => Some((n, group_cols.as_slice())),
        _ => None,
    });
    // A second reader would be handed rows placed by a key that is not its own.
    if keyed.is_some() && circuit.readers(shard).nth(1).is_some() {
        return Err("an exchange in front of a reduce or top-N has another reader".into());
    }
    Ok(keyed)
}

// ---------------------------------------------------------------------------
// CompileOutput — typed compilation result
// ---------------------------------------------------------------------------

/// Output from `compile_view`, held by DagEngine as the view's plan: its one
/// program, exchanges included.
pub(super) struct CompileOutput {
    pub(in crate::query) vm: Vm,
    /// Every child store the plan's operators address.
    pub(in crate::query) layout: StateLayout,
    /// source id → the register its delta seeds, and the bound its backfill scan
    /// narrows by.
    pub(in crate::query) sources: FxHashMap<u64, (DeltaReg, ReadBound)>,
    /// `Some` iff the view is capacity-bounded.
    pub(in crate::query) hydration: Option<Hydration>,
}

/// Compile view `view_id`'s circuit, and answer the placement its store registers
/// under. Opens nothing.
pub(super) fn compile_view(
    circuit: &Circuit,
    registry: &RelationRegistry,
    view_id: u64,
    view_schema: &SchemaDescriptor,
    bounded: bool,
) -> Result<(CompileOutput, Placement), String> {
    let (meta, placement) = ViewMeta::derive(circuit, registry, view_schema)?;
    let EmitCtx {
        prog, layout, regs, integrals, sources, ..
    } = EmitCtx::emit(circuit, registry, &meta, (!bounded).then_some(view_id))?;
    let vm = prog.finish(regs[circuit.out()]);
    // Column count alone is not enough: equal counts with mismatched types would
    // let the client read a string descriptor out of integer storage.
    if !vm.out_schema().same_region_types(view_schema) {
        return Err("the circuit's output schema is not the view's".into());
    }
    let hydration = bounded
        .then(|| derive_hydration(circuit, registry, view_schema, &vm, &regs, &integrals))
        .transpose()?;
    let sources = sources
        .into_iter()
        .map(|(tid, scan)| (tid, (scan.seed, scan.bound)))
        .collect();
    let output = CompileOutput { vm, layout, sources, hydration };
    Ok((output, placement))
}

#[cfg(test)]
#[path = "tests/compiler.rs"]
mod tests;
