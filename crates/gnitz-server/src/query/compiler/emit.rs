//! Instruction emission: one `emit_node` arm per operator, and `build_plan` (one
//! plan, pre or post exchange).
//!
//! An arm resolves its operands, asks the operator's own constructor for its
//! artifact, allocates a register and pushes an instruction. The artifacts —
//! output schemas, probes, packers, plans — and the guards a client-supplied
//! circuit clears to get one belong to `gnitz-store`, beside the kernels that
//! read them.

use super::*;
use crate::query::vm::{BakedReduce, BakedTopN, Instr, Op};
use gnitz_store::ops::{ClampPreset, JoinPlan};
use gnitz_store::relation::StateLayout;

// ---------------------------------------------------------------------------
// EmitCtx — the per-plan build state every emit arm works against
// ---------------------------------------------------------------------------

/// All state of one `build_plan` invocation. Owned by `build_plan` and threaded
/// to the emit arms as one `&mut` instead of a dozen parallel parameters.
pub(super) struct EmitCtx<'a> {
    loaded: &'a LoadedCircuit,
    /// This worker holds the view's whole input: the `WorkerFilter` arm emits
    /// nothing (the trim would drop rows it legitimately owns a copy of), and
    /// `emit_reduce` makes it the owner of the global-aggregate seed.
    self_contained: bool,
    /// Answers the schemas and the slot.
    registry: &'a RelationRegistry,
    /// Every child the view's sub-plans declare, in one index space.
    layout: &'a mut StateLayout,
    instructions: Vec<Instr>,
    integrates: Vec<(DeltaReg, StateIdx)>,
    delta_schemas: Vec<SchemaDescriptor>,
    out_reg_of: Vec<Option<OutReg>>,
    source_reg_map: FxHashMap<i64, DeltaReg>,
}

/// What a node's emission left its value in — the one place a delta and a trace
/// meet, so every emit arm names the port kind it means.
#[derive(Clone, Copy)]
pub(super) enum OutReg {
    Delta(DeltaReg),
    Trace(StateIdx),
}

impl OutReg {
    pub(super) fn delta(self) -> Result<DeltaReg, String> {
        match self {
            OutReg::Delta(r) => Ok(r),
            OutReg::Trace(_) => Err("operand port takes a delta, not an integral".into()),
        }
    }

    pub(super) fn trace(self) -> Result<StateIdx, String> {
        match self {
            OutReg::Trace(t) => Ok(t),
            OutReg::Delta(_) => Err("operand port takes an integral, not a delta".into()),
        }
    }
}

impl EmitCtx<'_> {
    /// Declare one child store of the view's operator state, named after `kind`
    /// and the node. The returned [`StateIdx`] is the only way to reach it.
    fn declare_child(&mut self, kind: &str, nid: NodeId, schema: SchemaDescriptor) -> StateIdx {
        self.layout.declare(format!("{kind}_{nid}"), schema)
    }

    fn push(&mut self, in_reg: DeltaReg, out_reg: DeltaReg, op: Op) {
        self.instructions.push(Instr::new(in_reg, out_reg, op));
    }

    /// The register `src` produced. The one rejection left after the load held
    /// every node to `OpNode::arity()`: a plan covers a *slice* of the circuit, so
    /// a producer outside this side has no register at all.
    fn reg_of(&self, src: NodeId) -> Result<OutReg, String> {
        self.out_reg_of[src].ok_or_else(|| "operand is produced outside this plan".to_string())
    }

    /// The delta feeding a unary operator.
    fn unary_delta_in(&self, nid: NodeId) -> Result<DeltaReg, String> {
        self.reg_of(self.loaded.inputs(nid).unary())?.delta()
    }

    /// The two deltas feeding a binary operator, in port order.
    fn binary_delta_in(&self, nid: NodeId) -> Result<(DeltaReg, DeltaReg), String> {
        let (a, b) = self.loaded.inputs(nid).binary();
        Ok((self.reg_of(a)?.delta()?, self.reg_of(b)?.delta()?))
    }

    /// A join's `(delta, trace)` operands.
    fn join_in(&self, nid: NodeId) -> Result<(DeltaReg, StateIdx), String> {
        let (d, t) = self.loaded.inputs(nid).binary();
        Ok((self.reg_of(d)?.delta()?, self.reg_of(t)?.trace()?))
    }

    /// The schema register `r` is labelled with — the emit-time twin of
    /// [`crate::query::vm::Program::schema_of`].
    fn reg_schema(&self, r: DeltaReg) -> SchemaDescriptor {
        self.delta_schemas[r.0 as usize]
    }

    /// Allocate a fresh delta register and return its id.
    fn push_delta_reg(&mut self, schema: SchemaDescriptor) -> DeltaReg {
        assert!(
            self.delta_schemas.len() < u16::MAX as usize,
            "register count exceeds u16::MAX; `LoadedCircuit::new` caps a circuit at {} nodes",
            super::MAX_CIRCUIT_NODES,
        );
        let id = DeltaReg(self.delta_schemas.len() as u16);
        self.delta_schemas.push(schema);
        id
    }
}

// ---------------------------------------------------------------------------
// Instruction emission — per-node handler
// ---------------------------------------------------------------------------

/// Emit `nid`'s instructions and return the register its output lands in — the
/// node's own fresh register, or an input's when the node emits nothing and
/// aliases it (an identity `Map`, a `WorkerFilter` this worker cannot narrow,
/// the sink). Returning it is what keeps a register from being reserved for a
/// node that never writes one.
pub(super) fn emit_node(ctx: &mut EmitCtx, nid: NodeId, op: &gnitz_wire::OpNode) -> Result<OutReg, String> {
    match op {
        // `bound` is a backfill-scan hint consumed by the source drive, not by the
        // VM: emission is identical bounded or not.
        gnitz_wire::OpNode::ScanDelta { source: tid, .. } => {
            // A circuit scanning an unknown table is corrupt: the planner
            // registers every source before shipping the circuit.
            let schema = ctx
                .registry
                .relation(*tid as i64)
                .map(Relation::schema)
                .ok_or("scan-delta: unknown source table")?;
            let reg = ctx.push_delta_reg(schema);
            ctx.source_reg_map.insert(*tid as i64, reg);
            Ok(OutReg::Delta(reg))
        }

        gnitz_wire::OpNode::Filter(blob) => {
            let in_reg = ctx.unary_delta_in(nid)?;
            let in_schema = ctx.reg_schema(in_reg);
            // A present-but-corrupt blob, or a rejected program, is catalog
            // corruption. Falling back to pass-all would silently turn a WHERE
            // into WHERE TRUE; fail the compile instead.
            let pred = LogicalProgram::from_blob(blob, "filter")
                .and_then(|p| p.resolve_filter(&in_schema))
                .map_err(|e| OpBuildErr::Program("filter: invalid predicate program", e))?;
            let out_reg = ctx.push_delta_reg(in_schema);
            ctx.push(in_reg, out_reg, Op::Filter(Box::new(pred)));
            Ok(OutReg::Delta(out_reg))
        }

        gnitz_wire::OpNode::Map(mk) => emit_map(ctx, nid, mk),

        gnitz_wire::OpNode::Negate => {
            let in_reg = ctx.unary_delta_in(nid)?;
            let out_reg = ctx.push_delta_reg(ctx.reg_schema(in_reg));
            ctx.push(in_reg, out_reg, Op::Negate);
            Ok(OutReg::Delta(out_reg))
        }

        gnitz_wire::OpNode::Union => {
            let (in_a, in_b) = ctx.binary_delta_in(nid)?;
            let out_schema = gnitz_store::ops::union_nullability_merge(&ctx.reg_schema(in_a), &ctx.reg_schema(in_b))?;
            let out_reg = ctx.push_delta_reg(out_schema);
            ctx.push(in_a, out_reg, Op::Union { in_b });
            Ok(OutReg::Delta(out_reg))
        }

        gnitz_wire::OpNode::Distinct => emit_clamp(ctx, nid, ClampPreset::Distinct),
        gnitz_wire::OpNode::PositivePart => emit_clamp(ctx, nid, ClampPreset::PositivePart),

        gnitz_wire::OpNode::Reduce { group_cols, agg, global_ground } => {
            emit_reduce(ctx, nid, group_cols, agg, *global_ground)
        }

        gnitz_wire::OpNode::TopN { group_cols, order, limit, offset } => {
            let in_reg = ctx.unary_delta_in(nid)?;
            let plan =
                gnitz_store::ops::TopNPlan::from_wire(&ctx.reg_schema(in_reg), group_cols, order, *limit, *offset)?;
            let out_schema = plan.output_schema;
            let out_trace = ctx.declare_child("topn", nid, out_schema);
            let out_reg = ctx.push_delta_reg(out_schema);
            // The ordered index of every input row — the operator's whole history.
            let index_table = ctx.declare_child("topnidx", nid, plan.index.schema);
            let baked = Box::new(BakedTopN { plan, index_table });
            ctx.push(in_reg, out_reg, Op::TopN { out_trace, plan: baked });
            ctx.integrates.push((out_reg, out_trace));
            Ok(OutReg::Delta(out_reg))
        }

        gnitz_wire::OpNode::Join { kind, delta_is_right } => {
            let (delta_reg, trace) = ctx.join_in(nid)?;
            // One constructor owns every kind's guards and its output layout.
            let plan = JoinPlan::from_wire(
                *kind,
                *delta_is_right,
                &ctx.reg_schema(delta_reg),
                ctx.layout.schema_of(trace),
            )?;
            let out_reg = ctx.push_delta_reg(plan.out_schema);
            ctx.push(delta_reg, out_reg, Op::JoinDT { trace, probe: plan.probe });
            Ok(OutReg::Delta(out_reg))
        }

        gnitz_wire::OpNode::IntegrateSink => {
            // Emits no instruction: the sink register's batch is what
            // `execute_epoch_multi` extracts at epoch end.
            Ok(OutReg::Delta(ctx.unary_delta_in(nid)?))
        }

        gnitz_wire::OpNode::IntegrateTrace => {
            let in_reg = ctx.unary_delta_in(nid)?;
            let in_reg_schema = ctx.reg_schema(in_reg);
            let trace = ctx.declare_child("int", nid, in_reg_schema);
            ctx.integrates.push((in_reg, trace));
            Ok(OutReg::Trace(trace))
        }

        gnitz_wire::OpNode::ExchangeShard { .. } => {
            unreachable!("carve keeps every ExchangeShard out of every plan's node list")
        }

        gnitz_wire::OpNode::WorkerFilter => {
            // Drops the rows this worker does not own before they reach
            // `integrate_trace`, by the compile-time slot.
            let in_reg = ctx.unary_delta_in(nid)?;
            let slot = ctx.registry.slot();
            // A replicated view runs correct-local over the full broadcast, and at
            // one worker every partition is owned here — the filter is the identity
            // either way, and executing it would clone the whole delta each epoch.
            if ctx.self_contained {
                return Ok(OutReg::Delta(in_reg));
            }
            let out_reg = ctx.push_delta_reg(ctx.reg_schema(in_reg));
            let op = Op::WorkerFilter {
                worker_id: slot.rank,
                num_workers: slot.of,
            };
            ctx.push(in_reg, out_reg, op);
            Ok(OutReg::Delta(out_reg))
        }

        gnitz_wire::OpNode::NullExtend { type_codes, nulls_first } => {
            let in_reg = ctx.unary_delta_in(nid)?;
            // Built once and homed in `reg_meta`; the op derives its
            // appended-column count from it.
            let out_schema =
                gnitz_store::ops::null_extend_output_schema(&ctx.reg_schema(in_reg), type_codes, *nulls_first)?;
            let out_reg = ctx.push_delta_reg(out_schema);
            ctx.push(in_reg, out_reg, Op::NullExtend { nulls_first: *nulls_first });
            Ok(OutReg::Delta(out_reg))
        }
    }
}

// ---------------------------------------------------------------------------
// WEIGHT-CLAMP emission
// ---------------------------------------------------------------------------

/// `distinct` and `positive_part` differ in nothing but their preset, which the
/// caller's own match arm supplies.
fn emit_clamp(ctx: &mut EmitCtx, nid: NodeId, preset: ClampPreset) -> Result<OutReg, String> {
    let in_reg = ctx.unary_delta_in(nid)?;
    let schema = ctx.reg_schema(in_reg);
    let hist = ctx.declare_child("hist", nid, schema);
    let out_reg = ctx.push_delta_reg(schema);
    ctx.push(in_reg, out_reg, Op::WeightClamp { hist, preset });
    Ok(OutReg::Delta(out_reg))
}

// ---------------------------------------------------------------------------
// MAP emission
// ---------------------------------------------------------------------------

/// Every `MapKind`'s output schema, program and PK source are one fact, derived
/// by [`MapPlan::from_wire`]; this arm resolves the operand, asks for the plan,
/// and either elides it or allocates a register for it.
fn emit_map(ctx: &mut EmitCtx, nid: NodeId, mk: &gnitz_wire::MapKind) -> Result<OutReg, String> {
    let in_reg = ctx.unary_delta_in(nid)?;
    let plan = MapPlan::from_wire(&ctx.reg_schema(in_reg), mk)?;
    // A MAP that reproduces its input row verbatim emits nothing; the node's
    // consumers read the input register instead.
    if plan.is_identity() {
        return Ok(OutReg::Delta(in_reg));
    }
    let out_schema = *plan.out_schema();
    let out_reg = ctx.push_delta_reg(out_schema);
    ctx.push(in_reg, out_reg, Op::Map(Box::new(plan)));
    Ok(OutReg::Delta(out_reg))
}

// ---------------------------------------------------------------------------
// REDUCE emission
// ---------------------------------------------------------------------------

fn emit_reduce(
    ctx: &mut EmitCtx,
    nid: NodeId,
    group_cols: &[u32],
    agg: &[AggDescriptor],
    global_ground: bool,
) -> Result<OutReg, String> {
    let loaded = ctx.loaded;
    let in_reg_id = ctx.unary_delta_in(nid)?;
    let in_reg_schema = ctx.reg_schema(in_reg_id);

    // A worker owns the global-aggregate ground row when it holds the whole
    // input: the view is replicated (correct-local everywhere, read
    // single-sourced from worker 0), or nothing shards into this reduce, or it is
    // the one worker a sharded funnel routes V₀ to. That a *grouped* reduce never
    // seeds one is `ReducePlan::from_wire`'s: it rejects `global_ground` over a
    // non-empty group set.
    //
    // V₀'s route and its owner both derive from the empty group key, so a shard
    // keyed on anything else would place the row on one worker and elect another.
    let shard_cols = match loaded.op(loaded.inputs(nid).unary()) {
        gnitz_wire::OpNode::ExchangeShard { shard_cols } => Some(shard_cols.as_slice()),
        _ => None,
    };
    if global_ground && shard_cols.is_some_and(|c| !c.is_empty()) {
        return Err("reduce: a global aggregate under a keyed exchange shard".into());
    }
    let unsharded = shard_cols.is_none();
    let slot = ctx.registry.slot();
    let i_am_owner = ctx.self_contained
        || unsharded
        || slot.rank as usize == gnitz_wire::worker_for_key(gnitz_wire::global_group_key(), slot.of as usize);

    let plan = gnitz_store::ops::ReducePlan::from_wire(&in_reg_schema, group_cols, agg, global_ground, i_am_owner)?;
    let reduce_out_schema = plan.shape.output_schema;

    let out_trace = ctx.declare_child("reduce", nid, reduce_out_schema);
    let out_reg = ctx.push_delta_reg(reduce_out_schema);

    // One table per reduce, serving every MIN/MAX of it — so per-aggregate entries
    // share a table_id, scratch dir and compaction namespace and cannot collide on
    // a memory-pressure flush.
    let avi_table = plan
        .avi
        .as_ref()
        .map(|bake| ctx.declare_child("avidx", nid, bake.schema));

    let baked = Box::new(BakedReduce::new(plan, avi_table));
    ctx.push(in_reg_id, out_reg, Op::Reduce { out_trace, plan: baked });
    ctx.integrates.push((out_reg, out_trace));
    Ok(OutReg::Delta(out_reg))
}

// ---------------------------------------------------------------------------
// build_plan — one plan, pre or post exchange
// ---------------------------------------------------------------------------

/// Emit `ordered` into one sub-plan whose output is node `out`'s register.
/// `seeds` names the exchange nodes the plan reads as relayed batches, each with
/// its batch schema; their registers are in the returned map.
pub(super) fn build_plan(
    loaded: &LoadedCircuit,
    ordered: &[NodeId],
    registry: &RelationRegistry,
    layout: &mut StateLayout,
    self_contained: bool,
    seeds: &[(NodeId, SchemaDescriptor)],
    out: NodeId,
) -> Result<(SubPlan, Vec<Option<OutReg>>), String> {
    let mut ctx = EmitCtx {
        loaded,
        self_contained,
        registry,
        layout,
        instructions: Vec::with_capacity(16),
        integrates: Vec::new(),
        delta_schemas: Vec::new(),
        out_reg_of: vec![None; loaded.len()],
        source_reg_map: FxHashMap::default(),
    };
    for &(ex_nid, schema) in seeds {
        let reg = ctx.push_delta_reg(schema);
        ctx.out_reg_of[ex_nid] = Some(OutReg::Delta(reg));
    }
    for &nid in ordered {
        let reg = emit_node(&mut ctx, nid, loaded.op(nid))?;
        ctx.out_reg_of[nid] = Some(reg);
    }
    let out_reg = ctx.reg_of(out)?.delta()?;
    let EmitCtx {
        instructions,
        integrates,
        delta_schemas,
        source_reg_map,
        out_reg_of,
        ..
    } = ctx;
    let plan = SubPlan {
        vm: crate::query::vm::build(instructions, integrates, delta_schemas, out_reg),
        source_reg_map,
    };
    Ok((plan, out_reg_of))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/emit.rs"]
mod tests;
