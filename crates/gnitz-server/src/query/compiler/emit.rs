//! Instruction emission: one `emit_node` arm per operator, and `build_plan` (one
//! plan, pre or post exchange).
//!
//! An arm resolves its operands, asks the operator's own constructor for its
//! artifact, and pushes an instruction into a fresh register. The artifacts —
//! output schemas, probes, packers, plans — and the guards a client-supplied
//! circuit clears to get one belong to `gnitz-store`, beside the kernels that
//! read them.

use super::*;
use crate::query::vm::{BakedReduce, BakedTopN, Instr, Op};
use gnitz_store::ops::JoinPlan;
use gnitz_store::relation::StateLayout;

// ---------------------------------------------------------------------------
// EmitCtx — the per-plan build state every emit arm works against
// ---------------------------------------------------------------------------

/// All state of one `build_plan` invocation. Owned by `build_plan` and threaded
/// to the emit arms as one `&mut` instead of a dozen parallel parameters.
pub(super) struct EmitCtx<'a> {
    loaded: &'a LoadedCircuit,
    /// This worker holds the view's whole input: the `WorkerFilter` arm emits
    /// nothing (the trim would drop rows it legitimately owns a copy of), and it
    /// owns the global-aggregate ground row.
    self_contained: bool,
    /// The [`Seed::partials`] seeds, whose reader combines.
    partial_seeds: Vec<NodeId>,
    /// Answers the schemas and the slot.
    registry: &'a RelationRegistry,
    /// Every child the view's sub-plans declare, in one index space.
    layout: &'a mut StateLayout,
    instructions: Vec<Instr>,
    integrates: Vec<(DeltaReg, StateIdx)>,
    delta_schemas: Vec<SchemaDescriptor>,
    out_reg_of: Vec<Option<OutReg>>,
    source_reg_map: FxHashMap<u64, DeltaReg>,
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

    /// Push `op` over `in_reg`, writing a fresh register of `out_schema` — the
    /// one way an instruction gets its output register, so each is written once.
    fn push(&mut self, in_reg: DeltaReg, out_schema: SchemaDescriptor, op: Op) -> DeltaReg {
        let out_reg = self.push_delta_reg(out_schema);
        self.instructions.push(Instr::new(in_reg, out_reg, op));
        out_reg
    }

    /// The register `src` produced. The one rejection left after the load held
    /// every node to `OpNode::arity()`: a plan covers a *slice* of the circuit, so
    /// a producer outside this side has no register at all.
    fn reg_of(&self, src: NodeId) -> Result<OutReg, String> {
        self.out_reg_of[src].ok_or_else(|| "operand is produced outside this plan".to_string())
    }

    fn reads_partials(&self, nid: NodeId) -> bool {
        self.partial_seeds.contains(&self.loaded.inputs(nid).unary())
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

    /// The schema register `r` is labelled with.
    fn reg_schema(&self, r: DeltaReg) -> SchemaDescriptor {
        self.delta_schemas[r.at()]
    }

    /// Allocate a fresh delta register and return its id. An instruction's output
    /// register comes from [`EmitCtx::push`].
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
            let schema = ctx.registry.relation_or_err(*tid)?.schema();
            let reg = ctx.push_delta_reg(schema);
            // The driver seeds one register per source, so a second scan would
            // silently see nothing.
            if ctx.source_reg_map.insert(*tid, reg).is_some() {
                return Err("scan-delta: a plan scans one source twice".into());
            }
            Ok(OutReg::Delta(reg))
        }

        gnitz_wire::OpNode::Filter(blob) => {
            let in_reg = ctx.unary_delta_in(nid)?;
            let in_schema = ctx.reg_schema(in_reg);
            // A present-but-corrupt blob, or a rejected program, is catalog
            // corruption. Falling back to pass-all would silently turn a WHERE
            // into WHERE TRUE; fail the compile instead.
            let pred = LogicalProgram::from_blob(blob)
                .and_then(|p| p.resolve_filter(&in_schema))
                .map_err(|e| format!("filter: invalid predicate program: {e}"))?;
            Ok(OutReg::Delta(ctx.push(in_reg, in_schema, Op::Filter(Box::new(pred)))))
        }

        gnitz_wire::OpNode::Map(mk) => emit_map(ctx, nid, mk),

        gnitz_wire::OpNode::Negate => {
            let in_reg = ctx.unary_delta_in(nid)?;
            Ok(OutReg::Delta(ctx.push(in_reg, ctx.reg_schema(in_reg), Op::Negate)))
        }

        gnitz_wire::OpNode::Union => {
            let (in_a, in_b) = ctx.binary_delta_in(nid)?;
            let out_schema = gnitz_store::ops::union_nullability_merge(&ctx.reg_schema(in_a), &ctx.reg_schema(in_b))?;
            Ok(OutReg::Delta(ctx.push(in_a, out_schema, Op::Union { in_b })))
        }

        gnitz_wire::OpNode::WeightClamp(kind) => {
            let in_reg = ctx.unary_delta_in(nid)?;
            let schema = ctx.reg_schema(in_reg);
            let hist = ctx.declare_child("hist", nid, schema);
            Ok(OutReg::Delta(ctx.push(
                in_reg,
                schema,
                Op::WeightClamp { hist, kind: *kind },
            )))
        }

        gnitz_wire::OpNode::Reduce { group_cols, agg, global_ground } => {
            emit_reduce(ctx, nid, group_cols, agg, *global_ground)
        }

        gnitz_wire::OpNode::TopN { group_cols, order, limit, offset } => {
            let in_reg = ctx.unary_delta_in(nid)?;
            let in_schema = ctx.reg_schema(in_reg);
            let plan = match ctx.reads_partials(nid) {
                true => gnitz_store::ops::TopNPlan::combine(&in_schema, order, *limit, *offset)?,
                false => gnitz_store::ops::TopNPlan::from_wire(&in_schema, group_cols, order, *limit, *offset)?,
            };
            Ok(OutReg::Delta(push_topn(ctx, nid, FUNNEL_TOPN, in_reg, plan)))
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
            Ok(OutReg::Delta(ctx.push(
                delta_reg,
                plan.out_schema,
                Op::JoinDT { trace, probe: plan.probe },
            )))
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
            let in_reg = ctx.unary_delta_in(nid)?;
            if ctx.self_contained {
                return Ok(OutReg::Delta(in_reg));
            }
            let slot = ctx.registry.slot();
            Ok(OutReg::Delta(ctx.push(
                in_reg,
                ctx.reg_schema(in_reg),
                Op::WorkerFilter { slot },
            )))
        }

        gnitz_wire::OpNode::NullExtend { type_codes, nulls_first } => {
            let in_reg = ctx.unary_delta_in(nid)?;
            let out_schema =
                gnitz_store::ops::null_extend_output_schema(&ctx.reg_schema(in_reg), type_codes, *nulls_first)?;
            let op = Op::NullExtend { nulls_first: *nulls_first };
            Ok(OutReg::Delta(ctx.push(in_reg, out_schema, op)))
        }
    }
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
    Ok(OutReg::Delta(ctx.push(in_reg, out_schema, Op::Map(Box::new(plan)))))
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
    let in_reg = ctx.unary_delta_in(nid)?;
    let in_schema = ctx.reg_schema(in_reg);
    let seeds_ground = global_ground && owns_ground(ctx, nid)?;
    let plan = match ctx.reads_partials(nid) {
        true => gnitz_store::ops::ReducePlan::combine(&in_schema, agg, seeds_ground)?,
        false => gnitz_store::ops::ReducePlan::from_wire(&in_schema, group_cols, agg, seeds_ground)?,
    };
    Ok(OutReg::Delta(push_reduce(ctx, nid, FUNNEL_REDUCE, in_reg, plan)))
}

/// Whether this worker seeds global reduce `nid`'s ground row: each worker that
/// holds the whole input does, else the one worker V₀'s empty-keyed shard routes to.
fn owns_ground(ctx: &EmitCtx, nid: NodeId) -> Result<bool, String> {
    let slot = ctx.registry.slot();
    match ctx.loaded.op(ctx.loaded.inputs(nid).unary()) {
        gnitz_wire::OpNode::ExchangeShard { shard_cols } if !shard_cols.is_empty() => {
            Err("reduce: a global aggregate under a keyed exchange shard".into())
        }
        _ if ctx.self_contained => Ok(true),
        gnitz_wire::OpNode::ExchangeShard { .. } => {
            Ok(slot.rank as usize == gnitz_store::schema::ground_owner(slot.of as usize))
        }
        _ => Err("reduce: a global aggregate over a partitioned input with no exchange".into()),
    }
}

/// The child-store names of a reduce or top-N: its output trace, then its index.
type StateKinds = [&'static str; 2];
const FUNNEL_REDUCE: StateKinds = ["reduce", "avidx"];
const FUNNEL_TOPN: StateKinds = ["topn", "topnidx"];
/// A split side's per-worker partial of either.
const PARTIAL: StateKinds = ["partial", "partialidx"];

/// Declare `plan`'s children under `kinds`, and push the reduce over `in_reg`.
fn push_reduce(
    ctx: &mut EmitCtx,
    nid: NodeId,
    [trace_kind, index_kind]: StateKinds,
    in_reg: DeltaReg,
    plan: gnitz_store::ops::ReducePlan,
) -> DeltaReg {
    let out_schema = plan.shape.output_schema;
    let out_trace = ctx.declare_child(trace_kind, nid, out_schema);
    // One table per reduce, serving every MIN/MAX of it — so per-aggregate entries
    // share a table_id, scratch dir and compaction namespace and cannot collide on
    // a memory-pressure flush.
    let avi_table = plan
        .avi
        .as_ref()
        .map(|bake| ctx.declare_child(index_kind, nid, bake.schema));
    let baked = Box::new(BakedReduce::new(plan, avi_table));
    ctx.push(in_reg, out_schema, Op::Reduce { out_trace, plan: baked })
}

/// Declare `plan`'s children under `kinds`, and push the top-N over `in_reg`.
fn push_topn(
    ctx: &mut EmitCtx,
    nid: NodeId,
    [trace_kind, index_kind]: StateKinds,
    in_reg: DeltaReg,
    plan: gnitz_store::ops::TopNPlan,
) -> DeltaReg {
    let out_schema = plan.output_schema;
    let out_trace = ctx.declare_child(trace_kind, nid, out_schema);
    // The ordered index of every input row — the operator's whole history.
    let index_table = ctx.declare_child(index_kind, nid, plan.index.schema);
    let baked = Box::new(BakedTopN { plan, index_table });
    ctx.push(in_reg, out_schema, Op::TopN { out_trace, plan: baked })
}

/// `consumer`'s per-worker partial over its shard's input, or `None` for a reduce
/// whose partials do not combine. Its children are the shard's.
fn emit_partial(ctx: &mut EmitCtx, consumer: NodeId) -> Result<Option<DeltaReg>, String> {
    let shard = ctx.loaded.inputs(consumer).unary();
    let in_reg = ctx.unary_delta_in(shard)?;
    let in_schema = ctx.reg_schema(in_reg);
    Ok(match ctx.loaded.op(consumer) {
        gnitz_wire::OpNode::Reduce { agg, .. } => gnitz_store::ops::ReducePlan::partial(&in_schema, agg)?
            .map(|plan| push_reduce(ctx, shard, PARTIAL, in_reg, plan)),
        gnitz_wire::OpNode::TopN { order, limit, offset, .. } => {
            let plan = gnitz_store::ops::TopNPlan::partial(&in_schema, order, *limit, *offset)?;
            Some(push_topn(ctx, shard, PARTIAL, in_reg, plan))
        }
        _ => unreachable!("`global_split` names only a Reduce or a TopN"),
    })
}

// ---------------------------------------------------------------------------
// build_plan — one plan, pre or post exchange
// ---------------------------------------------------------------------------

/// What a sub-plan outputs.
#[derive(Clone, Copy)]
pub(super) enum PlanOut {
    Node(NodeId),
    /// The per-worker partial of `consumer`, a global reduce or top-N behind an
    /// empty-keyed shard — or the shard's input, when it has none.
    Split {
        consumer: NodeId,
    },
}

/// An exchange node a plan reads as a relayed batch.
pub(super) struct Seed {
    pub(super) shard: NodeId,
    pub(super) schema: SchemaDescriptor,
    /// The batch is its reader's per-worker partials.
    pub(super) partials: bool,
}

/// One `build_plan`'s result.
pub(super) struct Built {
    pub(super) plan: SubPlan,
    /// Each node's register in this plan.
    pub(super) regs: Vec<Option<OutReg>>,
    /// The plan outputs a [`PlanOut::Split`]'s partial.
    pub(super) partial: bool,
}

/// Emit `ordered` into one sub-plan that outputs `out`.
pub(super) fn build_plan(
    loaded: &LoadedCircuit,
    ordered: &[NodeId],
    registry: &RelationRegistry,
    layout: &mut StateLayout,
    self_contained: bool,
    seeds: &[Seed],
    out: PlanOut,
) -> Result<Built, String> {
    let mut ctx = EmitCtx {
        loaded,
        self_contained,
        partial_seeds: seeds.iter().filter(|s| s.partials).map(|s| s.shard).collect(),
        registry,
        layout,
        instructions: Vec::new(),
        integrates: Vec::new(),
        delta_schemas: Vec::new(),
        out_reg_of: vec![None; loaded.len()],
        source_reg_map: FxHashMap::default(),
    };
    for seed in seeds {
        let reg = ctx.push_delta_reg(seed.schema);
        ctx.out_reg_of[seed.shard] = Some(OutReg::Delta(reg));
    }
    for &nid in ordered {
        let reg = emit_node(&mut ctx, nid, loaded.op(nid))?;
        ctx.out_reg_of[nid] = Some(reg);
    }
    let (out_reg, partial) = match out {
        PlanOut::Node(nid) => (ctx.reg_of(nid)?.delta()?, false),
        PlanOut::Split { consumer } => match emit_partial(&mut ctx, consumer)? {
            Some(reg) => (reg, true),
            None => (ctx.unary_delta_in(loaded.inputs(consumer).unary())?, false),
        },
    };
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
    Ok(Built { plan, regs: out_reg_of, partial })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/emit.rs"]
mod tests;
