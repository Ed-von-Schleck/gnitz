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
use gnitz_store::relation::{Relation, RelationRegistry, StateIdx};
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

/// The reader of `shard` that groups its input, and its group columns — the key
/// the exchange co-locates by. `None` where no reader is one: the rows are placed
/// by their own PK.
fn keyed_reader(circuit: &Circuit, shard: NodeId) -> Option<(NodeId, &[u32])> {
    circuit
        .readers(shard)
        .find_map(|n| Some((n, circuit.op(n).group_cols()?)))
}

// ---------------------------------------------------------------------------
// CompileOutput — typed compilation result
// ---------------------------------------------------------------------------

/// Output from `compile_view`, held by DagEngine as the view's plan: its one
/// program, exchanges included.
pub(super) struct CompileOutput {
    pub(in crate::query) vm: Vm,
    /// source id → the register its delta seeds, and the bound its backfill scan
    /// narrows by.
    pub(in crate::query) sources: FxHashMap<u64, (DeltaReg, ReadBound)>,
    /// `Some` iff the view is capacity-bounded.
    pub(in crate::query) hydration: Option<Hydration>,
    /// Each worker computes the view's whole result locally: it is replicated, or
    /// there is one worker.
    pub(in crate::query) self_contained: bool,
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
    let EmitCtx { prog, regs, integrals, sources, .. } = EmitCtx::emit(circuit, registry, &meta, view_id)?;
    let vm = prog.finish(regs[circuit.out()]);
    // Column count alone is not enough: equal counts with mismatched types would
    // let the client read a string descriptor out of integer storage.
    if !vm.out_schema().same_region_types(view_schema) {
        return Err("the circuit's output schema is not the view's".into());
    }
    let hydration = bounded
        .then(|| derive_hydration(circuit, registry, &vm, &regs, &integrals))
        .transpose()?;
    let sources = sources
        .into_iter()
        .map(|(tid, scan)| (tid, (scan.seed, scan.bound)))
        .collect();
    let output = CompileOutput {
        vm,
        sources,
        hydration,
        self_contained: meta.self_contained,
    };
    Ok((output, placement))
}

#[cfg(test)]
#[path = "tests/compiler.rs"]
mod tests;
