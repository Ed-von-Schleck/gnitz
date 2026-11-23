//! Circuit compiler: derives a view's routing from its circuit and emits its VM
//! instructions. A plan reads the worker count and never this process's rank: the
//! master compiles before it forks, and every worker runs that plan.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use std::rc::Rc;

use crate::query::vm::{DeltaReg, Integral, Replay, Vm};
use gnitz_expr::LogicalProgram;
use gnitz_store::relation::{Relation, RelationRegistry, StateIdx, StateLayout};
use gnitz_wire::{Circuit, ClampKind, GroupAggs, JoinKind, MapKind, NodeId, OpNode, ReadBound};
use gnitz_zset::algebra::{MapPlan, Placement, ScatterPlan};
use gnitz_zset::schema::SchemaDescriptor;

mod emit;
mod hydration;
mod routing;

#[cfg(test)]
#[path = "tests/fixtures.rs"]
mod fixtures;

use emit::*;
use routing::ViewMeta;

// ---------------------------------------------------------------------------
// CompileOutput — typed compilation result
// ---------------------------------------------------------------------------

/// Output from `compile_view`, held by DagEngine as the view's plan: its one
/// program, exchanges included.
pub(super) struct CompileOutput {
    pub(in crate::query) vm: Vm,
    /// The relations the circuit scans, each once, in the circuit's node order.
    pub(in crate::query) source_ids: Box<[u64]>,
    /// Each one's scan, in the same order.
    pub(in crate::query) scans: Box<[Scan]>,
    /// `Some` iff the view is capacity-bounded.
    pub(in crate::query) hydration: Option<Replay>,
}

impl CompileOutput {
    /// `source`'s scan. Panics unless the circuit scans `source`.
    pub(in crate::query) fn scan(&self, source: u64) -> &Scan {
        match self.source_ids.iter().position(|&s| s == source) {
            Some(at) => &self.scans[at],
            None => panic!("the plan scans no relation {source}"),
        }
    }
}

/// Compile view `view_id`'s circuit, and answer the placement its store registers
/// under and the traces it keeps. Opens nothing.
pub(super) fn compile_view(
    circuit: &Circuit,
    registry: &RelationRegistry,
    view_id: u64,
    view_schema: &SchemaDescriptor,
    bounded: bool,
) -> Result<(CompileOutput, Placement, StateLayout), String> {
    let meta = ViewMeta::derive(circuit, registry)?;
    let ctx = EmitCtx::emit(circuit, &meta, view_id)?;
    let replay = bounded.then(|| ctx.replay_seed()).transpose()?;
    let EmitCtx { prog, regs, sources, dists, .. } = ctx;
    let (vm, traces) = prog.finish(regs[circuit.out()]);
    // Column count alone is not enough: equal counts with mismatched types would
    // let the client read a string descriptor out of integer storage.
    if !vm.out_schema().same_region_types(view_schema) {
        return Err("the circuit's output schema is not the view's".into());
    }
    let placement = dists[circuit.out()].placement(vm.out_schema());
    let hydration = replay
        .map(|(in_reg, seed, clamp, read)| vm.replay(in_reg, seed, clamp, read))
        .transpose()?;
    let (source_ids, scans): (Vec<u64>, Vec<Scan>) = sources.into_iter().unzip();
    let output = CompileOutput {
        vm,
        source_ids: source_ids.into(),
        scans: scans.into(),
        hydration,
    };
    Ok((output, placement, traces))
}

#[cfg(test)]
#[path = "tests/compiler.rs"]
mod tests;
