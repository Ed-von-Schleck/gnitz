//! Circuit compiler: derives a view's routing from its circuit and emits its VM
//! instructions. A plan reads the worker count and never this process's rank: the
//! master compiles before it forks, and every worker runs that plan.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use rustc_hash::FxHashMap;
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
use hydration::derive_hydration;
use routing::ViewMeta;

// ---------------------------------------------------------------------------
// CompileOutput — typed compilation result
// ---------------------------------------------------------------------------

/// Output from `compile_view`, held by DagEngine as the view's plan: its one
/// program, exchanges included.
pub(super) struct CompileOutput {
    pub(in crate::query) vm: Vm,
    /// Each scanned source's scan, once per source, in first-scan order.
    sources: Box<[(u64, Scan)]>,
    /// `Some` iff the view is capacity-bounded.
    pub(in crate::query) hydration: Option<Replay>,
}

impl CompileOutput {
    /// `source`'s scan. Panics unless the circuit scans `source`.
    pub(in crate::query) fn scan(&self, source: u64) -> &Scan {
        match self.sources.iter().find(|(s, _)| *s == source) {
            Some((_, scan)) => scan,
            None => panic!("the plan scans no relation {source}"),
        }
    }

    /// The relations the circuit scans, each once, in first-scan order.
    pub(in crate::query) fn source_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.sources.iter().map(|&(source, _)| source)
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
    let EmitCtx {
        prog, regs, integrals, sources, dists, ..
    } = EmitCtx::emit(circuit, &meta, view_id)?;
    let (vm, traces) = prog.finish(regs[circuit.out()]);
    // Column count alone is not enough: equal counts with mismatched types would
    // let the client read a string descriptor out of integer storage.
    if !vm.out_schema().same_region_types(view_schema) {
        return Err("the circuit's output schema is not the view's".into());
    }
    let placement = meta.placement(&dists[circuit.out()], vm.out_schema());
    let mut output = CompileOutput {
        vm,
        sources: sources.into_boxed_slice(),
        hydration: None,
    };
    if bounded {
        output.hydration = Some(derive_hydration(circuit, &meta, &output, &regs, &integrals)?);
    }
    Ok((output, placement, traces))
}

#[cfg(test)]
#[path = "tests/compiler.rs"]
mod tests;
