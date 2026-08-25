//! Circuit compiler: reads system tables, builds a DBSP circuit graph,
//! runs annotation + optimization passes, and emits VM instructions.

use std::collections::{HashMap, HashSet, VecDeque};

use rustc_hash::FxHashSet;
use std::fmt;

use crate::expr::ScalarFunc;
use crate::foundation::worker_ctx::{num_workers, worker_rank};
use crate::ops::{build_reduce_output_schema, AggDescriptor};
use crate::query::vm::{Instr, ProgramBuilder, RegisterMeta, VmHandle};
use crate::schema::{
    build_map_output_schema, hashrow_output_schema, merge_schemas_for_join, null_extend_output_schema, SchemaColumn,
    SchemaDescriptor, TypeCode,
};
use crate::storage::{ReadCursor, RecoverySource, StorageError, Table};
use gnitz_expr::{ExprValidateErr, LogicalProgram};
use gnitz_wire::is_fixed_int;
use gnitz_wire::AggFunc;

mod emit;
mod load;
mod optimize;

use emit::*;
use optimize::*;

pub(crate) use load::{circuit_range_join_n_eq, for_each_scan_edge, load_circuit, output_exchange_shard};
// Compiler-internal: `pub(crate)` on these would publish them to the whole
// `query` layer, which reaches the compiler only through the exports above.
#[cfg(test)]
use load::co_partition_keys;
use load::{circuit_source_bound, scan_reaches_only_unflagged_reindexes, scan_tid_through_filters};
pub(crate) use optimize::{compute_scatter_routing, compute_skips_exchange, ScatterRouting};

// Engine-only port aliases (all equal to wire constants).
const PORT_IN: i32 = gnitz_wire::PORT_IN as i32;
const PORT_IN_A: i32 = gnitz_wire::PORT_IN_A as i32;
const PORT_IN_B: i32 = gnitz_wire::PORT_IN_B as i32;
const PORT_TRACE: i32 = gnitz_wire::PORT_TRACE as i32;

/// Why `compile_view` failed to turn a stored view circuit into a runnable plan.
/// Rendered by `Display` into the `CREATE VIEW` error the client receives, so
/// every variant's text is user-facing.
#[derive(Debug)]
pub(crate) enum CompileError {
    /// `load_circuit` could not read the circuit's system tables.
    LoadFailed,
    /// The circuit has no nodes.
    EmptyCircuit,
    /// The circuit graph contains a cycle (`topo_sort` failed).
    Cycle,
    /// A binary set-op side has no `ExchangeShard` input node (malformed circuit).
    MissingExchangeInput,
    /// More than two exchange boundaries — not produced by any planner path.
    TooManyExchanges,
    /// A compile-time guard rejected the circuit; the payload names the guard,
    /// so the rejection says *which* of the ~30 trust-boundary checks fired
    /// instead of a bare "build failed".
    Rejected(&'static str),
    /// An expression-program guard rejected the circuit: the payload names the
    /// guard and carries the validator's own reason, so the rejection can state
    /// *which* limit the program exceeded and not only which guard fired.
    RejectedExpr(&'static str, ExprValidateErr),
    /// The machine failed, not the circuit: a storage step the compile needs
    /// returned an error. The payload names the step and carries the errno.
    StorageFailed(&'static str, StorageError),
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompileError::LoadFailed => f.write_str("circuit load failed"),
            CompileError::EmptyCircuit => f.write_str("circuit has no nodes"),
            CompileError::Cycle => f.write_str("circuit graph has a cycle"),
            CompileError::MissingExchangeInput => f.write_str("exchange node lacks an input edge"),
            CompileError::TooManyExchanges => f.write_str("more than two exchange nodes"),
            CompileError::Rejected(guard) => f.write_str(guard),
            CompileError::RejectedExpr(guard, e) => write!(f, "{guard}: {e}"),
            CompileError::StorageFailed(step, e) => write!(f, "{step}: {e}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Typed circuit graph with OpNode payloads. Only `load::topo_sorted` builds
/// one, so `ordered` and the adjacency maps are populated for every value that
/// exists — no caller has to state a sortedness precondition.
pub(crate) struct LoadedCircuit {
    out_schema: SchemaDescriptor,
    pub(crate) nodes: HashMap<i32, gnitz_wire::OpNode>,
    ordered: Vec<i32>,
    pub(crate) outgoing: HashMap<i32, Vec<(i32, i32)>>,
    incoming: HashMap<i32, Vec<(i32, i32)>>,
}

/// Build a `LoadedCircuit` from raw nodes/edges. Test-only: the struct's fields
/// are module-private, so the `dag` tests (which exercise `ViewMeta::from_loaded`)
/// cannot construct one directly.
#[cfg(test)]
pub(crate) fn loaded_for_test(nodes: HashMap<i32, gnitz_wire::OpNode>, edges: Vec<(i32, i32, i32)>) -> LoadedCircuit {
    load::topo_sorted(SchemaDescriptor::default(), nodes, edges).expect("test circuit must be acyclic")
}

/// Handles for the three circuit system tables the compiler reads. No schemas
/// are threaded — the cursor readers source each table's schema from the cursor
/// itself (`ReadCursor::schema`).
#[derive(Clone, Copy)]
pub(crate) struct SysTableRefs {
    // Table handles (DagEngine borrows them).
    pub nodes: *mut Table,
    pub edges: *mut Table,
    pub node_columns: *mut Table,
}

// SAFETY: same single-thread guarantee.
unsafe impl Send for SysTableRefs {}

impl SysTableRefs {
    pub(crate) fn null() -> Self {
        SysTableRefs {
            nodes: std::ptr::null_mut(),
            edges: std::ptr::null_mut(),
            node_columns: std::ptr::null_mut(),
        }
    }
}

/// source table id → join/group reindex `(column, carried promotion tc)` pairs.
pub(crate) type JoinShardMap = HashMap<i64, Vec<(u32, u8)>>;

/// The registered relations visible to a compile: table id → schema.
pub(crate) type ExtTables = HashMap<i64, SchemaDescriptor>;

// ---------------------------------------------------------------------------
// CompileOutput — typed compilation result
// ---------------------------------------------------------------------------

/// A compiled sub-pipeline: the VM program, its register layout, and its
/// source-to-input-register map.
/// Used for: (a) the pre-exchange phase of every view, (b) each side of a
/// binary set-op, and (c) the post-combine phase (single- and two-exchange
/// views). All three are structurally identical; the difference is only which
/// part of the plan graph they cover.
pub(crate) struct SubPlan {
    pub vm: Box<VmHandle>,
    pub in_reg: u16,
    pub out_reg: u16,
    /// True iff the program can *still* emit output from an empty input epoch: it
    /// carries a global-ground `Reduce` and its ground row has not been minted
    /// yet. The empty pad round is the ONLY place the SQL-required ground row over
    /// an empty/fully-retracted source is minted (`op_reduce`'s `n == 0` branch).
    /// Every other opcode is inert on an empty input, so an empty epoch skips the
    /// VM machinery entirely.
    ///
    /// A latch, not a constant: the row is minted at most once — after the first
    /// empty epoch `trace_out` holds V₀ either way — so every later empty tick
    /// would pay `bind_trace_cursors`, `compact_owned_traces` and a dispatch to
    /// perform one seek that finds the row. Clearing it makes that cost finite.
    /// The other `global_ground` emission ("cardinality hit zero, emit the ground
    /// in place of the shed row") needs a *non-empty* delta carrying the
    /// retraction, so it is unaffected.
    ///
    /// Plan-lifetime state guarding a store fact: every path that empties a
    /// view's stores must drop the cached plan with them
    /// (`reset_view_output_for_rebuild` → `DagEngine::invalidate`; a worker-count
    /// relayout runs before any store opens, so no plan exists yet).
    pub can_emit_on_empty: std::cell::Cell<bool>,
    /// Maps a source table id to the input register that receives its delta.
    /// Empty for the post-combine phase (which has no source-level routing).
    pub source_reg_map: HashMap<i64, u16>,
}

/// One exchanged side of a [`PlanShape::Exchanged`] plan: a sub-pipeline whose
/// output is repartitioned (relayed through the exchange) into a post-phase
/// seed register.
pub(crate) struct Side {
    pub plan: SubPlan,
    /// Source table this side scans (`0` = multiple/unknown). For a two-sided
    /// set-op it keys the side's IPC rounds distinctly in the master
    /// accumulator and routes each delta to the side(s) scanning its source;
    /// a unary side takes every delta regardless.
    pub source_id: i64,
    /// Register in the post VM seeded with this side's relayed batch.
    pub seed_reg: u16,
}

impl Side {
    /// This side's pre-exchange output schema — what the wire encode labels its
    /// relayed batches (and empty placeholders) with. Never the view's final
    /// combine-widened schema, which is a different width for a JOIN and would
    /// mislabel the operand batch.
    pub(super) fn exchange_schema(&self) -> SchemaDescriptor {
        self.plan.vm.program.reg_meta[self.plan.out_reg as usize].schema
    }
}

/// The phase structure of a compiled view.
/// * `Single`: the whole plan is one sub-pipeline (no repartition).
/// * `Exchanged`: one side (GROUP BY / SELECT DISTINCT / PK redistribution /
///   range join) or two sides (binary set-ops) each computes up to its
///   `ExchangeShard`, is relayed, and seeds the post-combine phase.
///   `sides.len() ∈ {1, 2}` — more is planner-rejected (`TooManyExchanges`).
pub(crate) enum PlanShape {
    Single(SubPlan),
    Exchanged { sides: Vec<Side>, post: SubPlan },
}

/// What one `build_plan` call is producing — and with it whether the sink-schema
/// contract applies. A view's own output must match `loaded.out_schema`; a
/// subgraph's is an intermediate whose schema is whatever its last node derived,
/// so holding that to the view schema would reject every exchange side.
pub(super) enum PlanTarget<'a> {
    /// The whole view's output. `seeds` names one relayed-batch seed register per
    /// exchange input — empty for a `Single` plan, one or two entries for the post
    /// phase of an `Exchanged` one.
    ViewOutput { seeds: &'a [(i32, SchemaDescriptor)] },
    /// A fragment ending at node `out`, whose own schema is the plan's output.
    Subgraph { out: i32 },
}

/// Output from `compile_view`, consumed directly by DagEngine as the cached
/// plan.
///
/// It carries no **routing** annotations. Routing is a placement fact, the same
/// one the master relay needs and must derive without ever compiling; it lives
/// once on the memoized `ViewMeta`, which both the worker dispatch and the relay
/// read. Producing a second copy here would be two producers of one fact — the
/// defect one level up from a badly typed one.
pub(crate) struct CompileOutput {
    pub shape: PlanShape,
    /// The `(source table id, secondary-index range)` the planner pushed onto the
    /// primary source's `ScanDelta`, consulted only by the two circuit backfill
    /// drivers (a steady-state delta never opens the source cursor). A **physical
    /// access hint**: `None` means "full-scan", which is always correct, and the
    /// circuit's `Filter` carries the full predicate either way.
    pub source_bound: Option<(i64, gnitz_wire::ScanBound)>,
    /// How a capacity-bounded view recomputes one key's output rows. `None` for
    /// every unbounded view — the walk runs only under a capacity, and any
    /// structural mismatch there is a `Rejected` rather than a silent `None`: a
    /// bounded view must never reach its store with no way to hydrate it.
    pub hydration: Option<Hydration>,
}

/// Where a bounded view's per-key replay seeds. Both variants name a register to
/// feed and a store to feed it from; they differ only in where that store lives
/// and, for the join, in entering the program past its own prologue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Hydration {
    /// Linear (`ScanDelta → Filter? → Map? → IntegrateSink`): seed the
    /// `ScanDelta`'s register `in_reg` from relation `source`'s store and replay
    /// the whole program.
    Relation { in_reg: u16, source: i64 },
    /// Inner equi-join: seed `in_reg` from `owned_tables[seed_table]`'s store and
    /// dispatch from `start_pc`. The seed must enter mid-program because the join
    /// key is not the source PK, so a key-restricted feed at the `ScanDelta`
    /// register would mean scanning the whole source.
    Join {
        start_pc: usize,
        in_reg: u16,
        seed_table: u16,
    },
}

impl CompileOutput {
    /// Every sub-plan of the shape, for whole-plan sweeps (regfile clears,
    /// checkpoint table collection).
    pub(super) fn sub_plans_mut(&mut self) -> impl Iterator<Item = &mut SubPlan> {
        let (sides, single, post) = match &mut self.shape {
            PlanShape::Single(sub) => (&mut [][..], Some(sub), None),
            PlanShape::Exchanged { sides, post } => (&mut sides[..], None, Some(post)),
        };
        sides.iter_mut().map(|s| &mut s.plan).chain(single).chain(post)
    }
}

// ---------------------------------------------------------------------------
// Build a single plan (pre or post exchange)
// ---------------------------------------------------------------------------

pub(super) struct PlanBuildResult {
    vm: Box<VmHandle>,
    in_reg: u16,
    out_reg: u16,
    source_reg_map: HashMap<i64, u16>,
    can_emit_on_empty: bool,
    // (exchange-input node id → seed register) for each exchange input this plan
    // was built with. Lets `compile_view` wire each side's relayed batch to the
    // correct post-phase register.
    exchange_input_regs: Vec<(i32, u16)>,
    // Scratch dirs created for this plan. Dropping the result (a failed sibling
    // plan, an `Err` return from `compile_view`) removes them; `into_sub_plan`
    // defuses the guard — from then on the VM's owned tables keep them alive.
    scratch: emit::ScratchGuard,
    // Program offset just past each node's own instructions — where a hydration
    // replay that seeds that node's register resumes. See [`Hydration`].
    instr_end: HashMap<i32, usize>,
    // The emitter's resolved node → output register map, after every aliasing
    // rewrite (an elided identity `Map`, a `Filter(None)` pass-through, a skipped
    // `Distinct`). Reading it is how `derive_hydration` sees through elision
    // rather than re-deriving a register from graph position.
    out_reg_of: HashMap<i32, u16>,
}

impl PlanBuildResult {
    /// Convert into the runtime `SubPlan`, defusing the scratch guard.
    fn into_sub_plan(mut self) -> SubPlan {
        self.scratch.defuse();
        SubPlan {
            in_reg: self.in_reg,
            out_reg: self.out_reg,
            can_emit_on_empty: std::cell::Cell::new(self.can_emit_on_empty),
            source_reg_map: self.source_reg_map,
            vm: self.vm,
        }
    }

    /// The single source table this plan scans (empty/ambiguous → 0), used as
    /// the exchange `source_id` so each side's IPC rounds key distinctly.
    fn single_source(&self) -> i64 {
        if self.source_reg_map.len() == 1 {
            *self.source_reg_map.keys().next().unwrap()
        } else {
            0
        }
    }

    /// The seed register the post plan allocated for exchange input `nid`.
    /// `build_plan` pushes one entry per exchange input, so the lookup must hit;
    /// a miss is a compile bug — panic rather than silently seed register 0 (the
    /// delta reg) and corrupt the combine's input.
    fn seed_of(&self, nid: i32) -> u16 {
        self.exchange_input_regs
            .iter()
            .find(|&&(n, _)| n == nid)
            .map(|&(_, r)| r)
            .expect("post plan must allocate a seed register for each exchange input")
    }
}

/// Validate an exchange-input sub-plan and return its output schema. The out_reg
/// must index the plan's register file, and — when the exchange node is an
/// `ExchangeShard` (which emits no instruction, so nothing else checks it) — its
/// `shard_cols` must be in range for that schema. Both are consumed at runtime by
/// `extract_group_key` (`schema.columns[c]`), so a crafted/corrupt node is
/// rejected here rather than aborting at the first push. On `Err`, dropping the
/// plan removes its scratch dirs.
fn finalize_side(
    plan: &PlanBuildResult,
    loaded: &LoadedCircuit,
    ex_nid: i32,
) -> Result<SchemaDescriptor, CompileError> {
    if plan.out_reg as usize >= plan.vm.program.reg_meta.len() {
        return Err(CompileError::Rejected("exchange side output register out of bounds"));
    }
    let schema = plan.vm.program.reg_meta[plan.out_reg as usize].schema;
    if let Some(gnitz_wire::OpNode::ExchangeShard { shard_cols }) = loaded.nodes.get(&ex_nid) {
        if oob_cols(shard_cols, &schema) {
            return Err(CompileError::Rejected("exchange shard columns out of range"));
        }
    }
    Ok(schema)
}

/// The graph half of a bounded view's hydration plan: which relation a linear
/// body replays over, or which delta/trace node pair a join body seeds from.
///
/// The walk is over `loaded`, not the emitted instruction list, for the same
/// reason the five sibling `CompileOutput` annotations are: the instruction
/// stream elides nodes (`Filter(None)` aliases its input register, an identity
/// `Map` vanishes, `ScanDelta` and `IntegrateSink` emit nothing) and drops
/// `WorkerFilter` at `num_workers <= 1`, so it is worker-count dependent. Split
/// from [`derive_hydration`] because this half is the drift-prone one and needs
/// nothing but the circuit, so its rejections are directly testable.
fn hydration_nodes(loaded: &LoadedCircuit) -> Result<HydrationNodes, CompileError> {
    use gnitz_wire::{JoinKind, OpNode};

    // 1. From the sink's input, walk back through single-input Filter/Map nodes.
    let sink = loaded
        .ordered
        .iter()
        .copied()
        .find(|nid| matches!(loaded.nodes.get(nid), Some(OpNode::IntegrateSink)))
        .ok_or(CompileError::Rejected("bounded view: circuit has no IntegrateSink"))?;
    let mut cur =
        input_on_port(loaded, sink, PORT_IN).ok_or(CompileError::Rejected("bounded view: sink has no input"))?;
    loop {
        match loaded.nodes.get(&cur) {
            Some(OpNode::Filter(_)) | Some(OpNode::Map(_)) => {
                cur = input_on_port(loaded, cur, PORT_IN)
                    .ok_or(CompileError::Rejected("bounded view: filter/map has no input"))?;
            }
            // The linear shape: the whole program replays over the source store,
            // seeded at this `ScanDelta`'s own register.
            Some(OpNode::ScanDelta { source, .. }) => {
                return Ok(HydrationNodes::Relation {
                    nid: cur,
                    source: *source as i64,
                })
            }
            Some(OpNode::Union) => break,
            _ => return Err(CompileError::Rejected("bounded view: unsupported circuit shape")),
        }
    }
    let union = cur;

    // 2. Both `Union` inputs must be `Join(DeltaTrace)`, optionally behind one
    //    `Map` (`normalize_to_ab`'s per-branch projection, emitted for both
    //    branches but elidable as an identity).
    let ins = loaded
        .incoming
        .get(&union)
        .filter(|v| v.len() == 2)
        .ok_or(CompileError::Rejected("bounded view: union does not have two inputs"))?;
    let through_map = |mut nid: i32| -> Option<i32> {
        if matches!(loaded.nodes.get(&nid), Some(OpNode::Map(_))) {
            nid = input_on_port(loaded, nid, PORT_IN)?;
        }
        matches!(loaded.nodes.get(&nid), Some(OpNode::Join(JoinKind::DeltaTrace))).then_some(nid)
    };
    let j_a = through_map(ins[0].0).ok_or(CompileError::Rejected(
        "bounded view: union input is not an inner delta/trace join",
    ))?;

    // 3. Seed check, stated directly on the graph rather than through a
    //    register-identity or schema-equality proxy: the trace `J_a` joins
    //    against must be the integral of `J_a`'s *own* delta port. Either branch
    //    computes the same product and each carries its own normalization map
    //    back to canonical `[A, B]` order, so taking `J_a` needs no left/right
    //    inference. The unchosen branch stays inert: the dispatch clears every
    //    delta register on entry, so `D_b` is empty and `J_b` unions in nothing.
    // Each join's trace port, checked to be an integral. Two calls, two messages,
    // so a rejection names the branch it came from.
    let trace_of = |j: i32, whose: &'static str| -> Result<i32, CompileError> {
        let t = input_on_port(loaded, j, PORT_TRACE).ok_or(CompileError::Rejected(whose))?;
        matches!(loaded.nodes.get(&t), Some(OpNode::IntegrateTrace))
            .then_some(t)
            .ok_or(CompileError::Rejected(whose))
    };
    let d_a =
        input_on_port(loaded, j_a, PORT_IN_A).ok_or(CompileError::Rejected("bounded view: join has no delta input"))?;
    trace_of(j_a, "bounded view: the seeded join's trace port is not an integral")?;
    // `T_b` integrates the *other* branch's delta, so the seed is the trace whose
    // input node is `D_a` — found on the sibling join.
    let j_b = through_map(ins[1].0).ok_or(CompileError::Rejected(
        "bounded view: union input is not an inner delta/trace join",
    ))?;
    let t_a = trace_of(j_b, "bounded view: the sibling join's trace port is not an integral")?;
    if input_on_port(loaded, t_a, PORT_IN) != Some(d_a) {
        return Err(CompileError::Rejected(
            "bounded view: the join's trace port is not the other branch's delta integral",
        ));
    }

    Ok(HydrationNodes::Join { d_a, t_a })
}

/// What [`hydration_nodes`] resolved out of the graph, before any program
/// lookup: the seeding `ScanDelta` and its relation for a linear body, or the
/// delta and trace nodes of the join branch a replay seeds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HydrationNodes {
    Relation { nid: i32, source: i64 },
    Join { d_a: i32, t_a: i32 },
}

/// Resolve those nodes against the plan the emitter just produced.
///
/// Registers and offsets come from the emitter's own node-keyed maps, never from a
/// node's index in `ordered`: the two agree only for a node that emitted —
/// `Filter(None)` aliases its input's register, an identity `Map` vanishes, a
/// skipped `Distinct` aliases, and `Reduce` redirects.
fn derive_hydration(loaded: &LoadedCircuit, plan: &PlanBuildResult) -> Result<Hydration, CompileError> {
    let reg_of = |nid: i32| {
        plan.out_reg_of.get(&nid).copied().ok_or(CompileError::Rejected(
            "bounded view: a hydration node is not in the plan",
        ))
    };

    let (d_a, t_a) = match hydration_nodes(loaded)? {
        HydrationNodes::Relation { nid, source } => {
            let in_reg = reg_of(nid)?;
            reject_state_writers(plan, 0)?;
            return Ok(Hydration::Relation { in_reg, source });
        }
        HydrationNodes::Join { d_a, t_a } => (d_a, t_a),
    };

    // Registers and the start offset, off the emitter's own bookkeeping.
    let in_reg = reg_of(d_a)?;
    let seed_table = plan
        .vm
        .program
        .reg_meta
        .get(reg_of(t_a)? as usize)
        .and_then(|m| m.owned_table)
        .ok_or(CompileError::Rejected(
            "bounded view: trace register has no owned table",
        ))?;
    // The replay enters past the seeded node's own instructions.
    let start_pc = plan
        .instr_end
        .get(&d_a)
        .copied()
        .ok_or(CompileError::Rejected("bounded view: delta node is not in the plan"))?;
    reject_state_writers(plan, start_pc)?;

    Ok(Hydration::Join {
        start_pc,
        in_reg,
        seed_table,
    })
}

/// Trust boundary on the program the read-only dispatch will run from `start_pc`:
/// that dispatch suppresses `Integrate`, so any *other* state writer would make a
/// read mutate the state it reads. None is reachable from an eligible shape, so
/// this turns a planner that under-rejects into a loud DDL failure rather than a
/// silently-mutating read. `writes_state` is exhaustive over `Instr`, so a new
/// state-writing opcode cannot slip past this.
fn reject_state_writers(plan: &PlanBuildResult, start_pc: usize) -> Result<(), CompileError> {
    if plan.vm.program.instructions[start_pc..]
        .iter()
        .any(|i| !matches!(i, Instr::Integrate { .. }) && crate::query::vm::writes_state(i))
    {
        return Err(CompileError::Rejected(
            "bounded view: the replayed program writes operator state",
        ));
    }
    Ok(())
}

/// One exchange boundary's carve: the shard node, the node feeding it, and the
/// ancestor set that becomes that side's own plan.
struct Carve {
    ex_nid: i32,
    ex_in: i32,
    ancestors: HashSet<i32>,
}

/// Where a view's rederived children are created, and under what policy.
#[derive(Clone, Copy)]
pub(crate) struct ViewSite<'a> {
    pub dir: &'a str,
    pub id: u64,
    /// The policy the view's *output store* was opened under, handed down so its
    /// operator traces cannot end up looking for a different manifest generation.
    pub recovery: RecoverySource,
}

/// Compile a circuit for a single view: read the circuit from the system
/// tables, then annotate → optimize → `build_plan`.
///
/// # Safety
/// All table handles must be valid pointers or null.
pub(crate) unsafe fn compile_view(
    site: ViewSite<'_>,
    sys: SysTableRefs,
    view_schema: &SchemaDescriptor,
    ext_tables: &ExtTables,
    bounded: bool,
) -> Result<CompileOutput, CompileError> {
    let loaded = load_circuit(sys, site.id, *view_schema)?;
    if loaded.nodes.is_empty() {
        return Err(CompileError::EmptyCircuit);
    }
    if scan_reaches_only_unflagged_reindexes(&loaded) {
        return Err(CompileError::Rejected(
            "a source's reindex maps carry no route key, so its delta cannot be scattered",
        ));
    }

    let skip_nodes = compute_skip_nodes(&loaded);
    // Swept from `loaded`, not threaded out of `emit_node`: `EmitCtx` flows into
    // `SubPlan`, never into `CompileOutput`, and an `Exchanged` shape runs
    // `build_plan` once per side plus post — so an emit-sourced value would need
    // a cross-sub-plan merge.
    let source_bound = circuit_source_bound(&loaded);

    let annotated = |shape: PlanShape, hydration: Option<Hydration>| CompileOutput {
        shape,
        source_bound,
        hydration,
    };

    let exchange_nids: Vec<i32> = loaded
        .ordered
        .iter()
        .copied()
        .filter(|&nid| matches!(loaded.nodes.get(&nid), Some(gnitz_wire::OpNode::ExchangeShard { .. })))
        .collect();

    // On any `?` below, the failing/finished `PlanBuildResult`s drop and their
    // ScratchGuards remove every scratch directory the sibling plans created —
    // a rejected compile leaks no inodes.
    match exchange_nids.len() {
        0 => {
            let plan = build_plan(
                &loaded,
                &skip_nodes,
                &loaded.ordered,
                ext_tables,
                site,
                PlanTarget::ViewOutput { seeds: &[] },
            )?;
            // Only `PlanShape::Single` — which both eligible shapes are — can be
            // hydrated; a bounded view compiling to any other shape is rejected
            // in the arms below.
            let hydration = bounded.then(|| derive_hydration(&loaded, &plan)).transpose()?;
            Ok(annotated(PlanShape::Single(plan.into_sub_plan()), hydration))
        }
        // One or two exchange boundaries: carve each side out by the ancestors
        // of its exchange input (a binary set-op's two independent
        // HashRow→ExchangeShard sub-pipelines; the unary GROUP BY / DISTINCT /
        // redistribution pipeline is the one-side case — its ancestors set is
        // exactly the nodes before the exchange in topo order). Everything else
        // (the combine + sink, reading the relayed batches) is the post phase.
        n @ (1 | 2) => {
            // An exchanged plan splits the circuit across a repartition, so
            // neither eligible shape can produce one — and a per-key replay of
            // one would need the exchange to run too.
            if bounded {
                return Err(CompileError::Rejected(
                    "bounded view: only a linear body and an inner equi-join are supported",
                ));
            }
            // One pass: each shard's input node and the ancestor set carved off it,
            // resolved once rather than looked up again per side.
            let carves: Vec<Carve> = exchange_nids
                .iter()
                .map(|&ex_nid| {
                    let ex_in = exchange_input_node(&loaded, ex_nid).ok_or(CompileError::MissingExchangeInput)?;
                    Ok(Carve {
                        ex_nid,
                        ex_in,
                        ancestors: ancestors_inclusive(&loaded, ex_in),
                    })
                })
                .collect::<Result<_, CompileError>>()?;
            let post_ordered: Vec<i32> = loaded
                .ordered
                .iter()
                .copied()
                .filter(|nid| !exchange_nids.contains(nid) && !carves.iter().any(|c| c.ancestors.contains(nid)))
                .collect();

            let mut side_plans: Vec<PlanBuildResult> = Vec::with_capacity(n);
            let mut exchange_inputs: Vec<(i32, SchemaDescriptor)> = Vec::with_capacity(n);
            for carve in &carves {
                let side_ordered: Vec<i32> = loaded
                    .ordered
                    .iter()
                    .copied()
                    .filter(|n| carve.ancestors.contains(n))
                    .collect();
                let plan = build_plan(
                    &loaded,
                    &skip_nodes,
                    &side_ordered,
                    ext_tables,
                    site,
                    PlanTarget::Subgraph { out: carve.ex_in },
                )?;
                let schema = finalize_side(&plan, &loaded, carve.ex_nid)?;
                side_plans.push(plan);
                exchange_inputs.push((carve.ex_nid, schema));
            }

            let post = build_plan(
                &loaded,
                &skip_nodes,
                &post_ordered,
                ext_tables,
                site,
                PlanTarget::ViewOutput {
                    seeds: &exchange_inputs,
                },
            )?;

            let sides: Vec<Side> = side_plans
                .into_iter()
                .zip(&exchange_inputs)
                .map(|(plan, &(ex_nid, _))| Side {
                    source_id: plan.single_source(),
                    seed_reg: post.seed_of(ex_nid),
                    plan: plan.into_sub_plan(),
                })
                .collect();

            Ok(annotated(
                PlanShape::Exchanged {
                    sides,
                    post: post.into_sub_plan(),
                },
                None,
            ))
        }
        _ => {
            // More than two exchange boundaries is not produced by any current
            // planner path (set-ops are binary, GROUP BY/DISTINCT are unary).
            gnitz_warn!(
                "compile_view: view_id={} has {} exchange nodes; unsupported",
                site.id,
                exchange_nids.len()
            );
            Err(CompileError::TooManyExchanges)
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::type_code;

    /// An unbounded delta scan — every fixture circuit's source shape.
    fn scan_delta(source: u64) -> gnitz_wire::OpNode {
        gnitz_wire::OpNode::ScanDelta { source, bound: None }
    }

    /// The boot and rebuild sweeps find a scratch dir by parsing its name back,
    /// so what the compiler builds must survive the round trip — including a
    /// child name that itself contains underscores (`_reduce_in_{vid}_{nid}`).
    #[test]
    fn child_scratch_dir_round_trips_through_child_addr() {
        let built = child_scratch_dir("/d", "_reduce_in_9_3");
        let name = built.rsplit('/').next().unwrap();
        assert_eq!(
            crate::storage::ChildAddr::parse(name),
            Some(crate::storage::ChildAddr::Scratch {
                child: "_reduce_in_9_3",
                rank: worker_rank(),
            })
        );
    }

    #[test]
    fn test_topo_sort_simple() {
        let nodes = HashMap::from([
            (0, scan_delta(0)),
            (1, gnitz_wire::OpNode::Filter(None)),
            (2, gnitz_wire::OpNode::IntegrateSink),
        ]);
        let loaded = loaded_for_test(nodes, vec![(0, 1, 0), (1, 2, 0)]);
        assert_eq!(loaded.ordered, vec![0, 1, 2]);
    }

    #[test]
    fn test_topo_sort_cycle() {
        let nodes = HashMap::from([
            (0, gnitz_wire::OpNode::Filter(None)),
            (1, gnitz_wire::OpNode::Filter(None)),
        ]);
        assert!(matches!(
            load::topo_sorted(SchemaDescriptor::default(), nodes, vec![(0, 1, 0), (1, 0, 0)]),
            Err(CompileError::Cycle)
        ));
    }

    #[test]
    fn test_compute_co_partitioned_strict_full_pk_sequence() {
        // Compound PK (a, b) at columns 0, 1; column 2 is payload.
        let compound = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        );
        let ext: ExtTables = HashMap::from([(7, compound)]);
        let co = |cols: Vec<(u32, u8)>| {
            let mut m = HashMap::new();
            m.insert(7i64, cols);
            compute_co_partitioned(&m, &ext).contains(&7)
        };
        // Only the exact PK sequence in schema order co-partitions.
        assert!(
            co(vec![(0, 0), (1, 0)]),
            "shard [pk0, pk1] equals pk_indices() → co-partitioned"
        );
        assert!(!co(vec![(0, 0)]), "shard [pk0] alone is not the full PK");
        assert!(!co(vec![(1, 0)]), "shard [pk1] alone is not the full PK");
        assert!(!co(vec![(1, 0), (0, 0)]), "permuted [pk1, pk0] != pk_indices() order");
        // A promoted key (non-zero carried tc) never co-partitions: native PK
        // partitions are at the source width, not the T-wide trace key.
        assert!(
            !co(vec![(0, type_code::I64), (1, 0)]),
            "a promoted PK slot must go through the exchange"
        );

        // Single-PK source: [pk] stays co-partitioned (no regression).
        let single = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let ext1: ExtTables = HashMap::from([(9, single)]);
        let mut m = HashMap::new();
        m.insert(9i64, vec![(0, 0)]);
        assert!(
            compute_co_partitioned(&m, &ext1).contains(&9),
            "single-PK shard [pk] stays co-partitioned"
        );
    }

    #[test]
    fn test_compute_co_partitioned_replicated() {
        // Two single-PK (U64) join sides; the join key is a NON-PK payload column
        // (col 1), so neither side's shard key matches its distribution prefix —
        // the only reason to skip the exchange is replication.
        const COLS: [SchemaColumn; 2] = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ];
        let base = || SchemaDescriptor::new(&COLS, &[0]);
        let replicated = SchemaDescriptor::new_with_placement(&COLS, &[0], crate::schema::Placement::Replicated);
        let join_on_payload = || {
            let mut m = HashMap::new();
            m.insert(7i64, vec![(1u32, 0u8)]); // dim  shards on payload col 1
            m.insert(8i64, vec![(1u32, 0u8)]); // fact shards on payload col 1
            m
        };

        // partitioned ⋈ partitioned on a non-PK key: neither side skips.
        let ext_pp: ExtTables = HashMap::from([(7, base()), (8, base())]);
        let co = compute_co_partitioned(&join_on_payload(), &ext_pp);
        assert!(
            !co.contains(&7) && !co.contains(&8),
            "two partitioned sides on a non-PK key both go through the exchange"
        );

        // partitioned fact ⋈ REPLICATED dim: BOTH skip — the dim because it is
        // replicated, the fact because its join partner is replicated (it stays in
        // its own PK partitioning and joins the full local dim copy).
        let ext_pr: ExtTables = HashMap::from([(7, replicated), (8, base())]);
        let co = compute_co_partitioned(&join_on_payload(), &ext_pr);
        assert!(co.contains(&7), "a replicated source always skips its exchange");
        assert!(
            co.contains(&8),
            "a partitioned fact skips when its partner is replicated"
        );

        // replicated ⋈ replicated: both skip (output is replicated; single-sourced on read).
        let ext_rr: ExtTables = HashMap::from([(7, replicated), (8, replicated)]);
        let co = compute_co_partitioned(&join_on_payload(), &ext_rr);
        assert!(
            co.contains(&7) && co.contains(&8),
            "replicated ⋈ replicated: both sides skip"
        );

        // A replicated source skips even with a promoted (non-zero tc) key: the
        // write broadcast already placed its full trace on every worker, so the
        // tc-promotion exchange gate (which blocks a partitioned source) does not apply.
        let ext_r: ExtTables = HashMap::from([(7, replicated)]);
        let mut promoted = HashMap::new();
        promoted.insert(7i64, vec![(0u32, type_code::I64)]);
        assert!(
            compute_co_partitioned(&promoted, &ext_r).contains(&7),
            "replicated source skips regardless of carried type-promotion"
        );
    }

    #[test]
    fn test_sequential_copy_projection() {
        use gnitz_expr::{LogicalInstr, LogicalProgram};
        // num_regs covers the largest register index in the synthetic programs
        // below so LogicalProgram::new's register-bounds assert passes; this test
        // exercises sequential_copy_base, not register limits.
        let make = |instrs: Vec<LogicalInstr>| LogicalProgram::new(instrs, 16, 0, vec![]);
        let copy = |src_col: u32, out: u32| LogicalInstr::CopyCol { src_col, out };
        // src 1,2 → dst 0,1: base = 1.
        assert_eq!(make(vec![copy(1, 0), copy(2, 1)]).sequential_copy_base(), Some(1));
        // sources not sequential (2, then 1)
        assert_eq!(make(vec![copy(2, 0), copy(1, 1)]).sequential_copy_base(), None);
        // a non-COPY_COL instruction breaks the block copy
        assert_eq!(
            make(vec![copy(1, 0), LogicalInstr::LoadColInt { dst: 9, col: 2 }]).sequential_copy_base(),
            None
        );
        assert_eq!(make(vec![]).sequential_copy_base(), None); // empty
                                                               // Sequential sources but destinations swapped (1, 0) — a permutation, not an identity.
        assert_eq!(make(vec![copy(1, 1), copy(2, 0)]).sequential_copy_base(), None);
        // Compound PK (k = 2): finalize copies columns 2, 3 → destinations 0, 1.
        assert_eq!(make(vec![copy(2, 0), copy(3, 1)]).sequential_copy_base(), Some(2));
    }

    #[test]
    fn test_identity_map_detection() {
        let a = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let b = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        assert!(a.same_physical_layout(&b));

        let c = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::STRING, 0),
            ],
            &[0],
        );
        assert!(!a.same_physical_layout(&c));
    }

    #[test]
    fn test_build_plan_fails_when_child_table_fails() {
        // Circuit: SCAN(0) → DISTINCT(1) → INTEGRATE(2)
        // An invalid view_dir forces create_child_table to fail inside emit_node.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(99));
        nodes.insert(1, gnitz_wire::OpNode::Distinct);
        nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
        let loaded = loaded_for_test(nodes, vec![(0, 1, 0), (1, 2, 0)]);

        // Provide an external table so ScanDelta finds its schema and sets source_reg_map.
        let in_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        let ext_tables: ExtTables = HashMap::from([(99, in_schema)]);

        let result = build_plan(
            &loaded,
            &no_skips(),
            &loaded.ordered,
            &ext_tables,
            test_site("/nonexistent_gnitz_test_path_xyz_abc", 99),
            PlanTarget::ViewOutput { seeds: &[] },
        );
        assert!(result.is_err(), "build_plan must fail when child table creation fails");
    }

    #[test]
    fn test_build_plan_register_overflow_rejected() {
        // A circuit producing > u16::MAX registers must fail the compile rather
        // than wrap the u16 cast and panic in ProgramBuilder::build.
        let n = u16::MAX as i32 + 1; // 65536 nodes → 65536 registers
        let mut nodes = HashMap::new();
        for nid in 0..n {
            nodes.insert(nid, gnitz_wire::OpNode::Negate);
        }
        let loaded = loaded_for_test(nodes, Vec::new());
        assert_eq!(loaded.ordered.len(), n as usize);
        let result = build_plan(
            &loaded,
            &no_skips(),
            &loaded.ordered,
            &HashMap::new(),
            test_site("", 1),
            PlanTarget::ViewOutput { seeds: &[] },
        );
        assert!(
            result.is_err(),
            "build_plan must fail when register count exceeds u16::MAX"
        );
    }

    #[test]
    fn test_build_plan_min_max_over_non_encodable_rejected() {
        // emit_reduce's order-encodability guard must fail the compile (build_plan →
        // None) for a MIN/MAX whose aggregate column is not order-encodable (STRING /
        // 16-byte), rather than let the reduce reach encode_ordered's unreachable arm
        // and panic a worker at execution. The SQL binder rejects this upstream
        // (gnitz-sql); this covers the low-level CircuitBuilder path that bypasses it.
        //
        // Discriminating by construction: the ONLY difference between the two builds
        // is the aggregate column's type. The order-encodable I64 agg compiles (Some),
        // proving the circuit shape and view_dir are otherwise valid, so the STRING
        // agg's None is attributable solely to the guard.
        use crate::schema::ReduceOutKey;
        use gnitz_wire::{AggFunc, OpNode};
        let compiles = |agg_tc: u8| -> bool {
            // col 0: U64 PK + group key; col 1: the MAX aggregate column.
            let in_schema = SchemaDescriptor::new(
                &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(agg_tc, 0)],
                &[0],
            );
            compiles_mid_node(
                in_schema,
                OpNode::Reduce {
                    group_cols: vec![0],
                    agg: vec![(AggFunc::Max, 1)],
                    global_ground: false,
                    // group_cols = [0] = the single U64 PK ⇒ PkPermutation.
                    out_key: ReduceOutKey::PkPermutation,
                },
            )
        };

        assert!(
            compiles(type_code::I64),
            "control: MAX over an order-encodable I64 column must compile"
        );
        assert!(
            !compiles(type_code::STRING),
            "MAX over a non-order-encodable STRING column must fail the compile (engine guard)"
        );
    }

    /// The planner ships the reduce output-key kind; the engine validates it
    /// against the input schema and hard-rejects (build_plan → None) any kind the
    /// schema does not warrant — the guard that turns a silent output-column
    /// scramble into a compile failure. Covers all three schema shapes × all three
    /// kinds: the three matching kinds compile, the six cross pairings reject.
    #[test]
    fn reduce_out_key_validation_rejects_mismatch() {
        use crate::schema::ReduceOutKey;
        use gnitz_wire::{AggFunc, OpNode};
        let compiles = |in_schema: SchemaDescriptor, group: Vec<u16>, out_key: ReduceOutKey| -> bool {
            compiles_mid_node(
                in_schema,
                OpNode::Reduce {
                    group_cols: group,
                    // A linear COUNT keeps the MIN/MAX-eligibility guard out of the
                    // picture, isolating the out_key validation.
                    agg: vec![(AggFunc::Count, 0)],
                    global_ground: false,
                    out_key,
                },
            )
        };

        // (schema, group cols, the ONE kind the schema warrants, tag).
        let eq_pk = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let single_nat = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::U64, 0), // natural group col
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let synthetic = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::STRING, 0), // non-natural group col
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let cases = [
            (eq_pk, vec![0u16], ReduceOutKey::PkPermutation, "eqpk"),
            (single_nat, vec![1u16], ReduceOutKey::SingleNaturalCol, "single"),
            (synthetic, vec![1u16], ReduceOutKey::SyntheticFold, "synth"),
        ];
        let all_kinds = [
            ReduceOutKey::SyntheticFold,
            ReduceOutKey::PkPermutation,
            ReduceOutKey::SingleNaturalCol,
        ];
        for (schema, group, correct, tag) in cases {
            for kind in all_kinds {
                let ok = compiles(schema, group.clone(), kind);
                assert_eq!(
                    ok,
                    kind == correct,
                    "schema {tag}: out_key {kind:?} should {} (schema warrants {correct:?})",
                    if kind == correct { "compile" } else { "reject" },
                );
            }
        }
    }

    fn wide_pk_schema() -> SchemaDescriptor {
        // 3 × U64 = 24-byte PK (wide, stride 24).
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0, 1, 2],
        )
    }

    #[test]
    fn test_build_plan_wide_pk_join_accepted() {
        // After byte-API port: wide-PK Join(DeltaTrace) must compile successfully.
        // ScanDelta(wide) --port0--> Join(DT) <--port1-- IntegrateTrace(wide)
        // Join(DT) --> IntegrateSink.
        let schema = wide_pk_schema();
        let dir = tempfile::tempdir().unwrap();
        let view_dir = dir.path().to_str().unwrap();

        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, scan_delta(20));
        nodes.insert(2, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(3, gnitz_wire::OpNode::IntegrateSink);
        nodes.insert(4, gnitz_wire::OpNode::IntegrateTrace);
        let edges = vec![(0, 2, PORT_IN_A), (1, 4, PORT_IN), (4, 2, PORT_TRACE), (2, 3, PORT_IN)];
        let loaded = loaded_for_test(nodes, edges);
        let ext: ExtTables = HashMap::from([(10, schema), (20, schema)]);
        // The plan owns scratch dirs under `dir`, so it must drop first: build it
        // inside the assert rather than binding it past `dir`'s scope.
        assert!(
            build_plan(
                &loaded,
                &no_skips(),
                &loaded.ordered,
                &ext,
                test_site(view_dir, 1),
                PlanTarget::Subgraph { out: 2 }
            )
            .is_ok(),
            "wide-PK Join(DeltaTrace) must compile after byte-API port"
        );
    }

    #[test]
    fn test_build_plan_integrate_trace_child_fail_rejected() {
        // ScanDelta(99) → IntegrateTrace(1) → IntegrateSink(2). An invalid
        // view_dir forces create_child_table to fail; the Integrate must
        // not be silently dropped — build_plan must return None.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(99));
        nodes.insert(1, gnitz_wire::OpNode::IntegrateTrace);
        nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN)];
        let loaded = loaded_for_test(nodes, edges);
        let in_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        let ext: ExtTables = HashMap::from([(99, in_schema)]);
        let result = build_plan(
            &loaded,
            &no_skips(),
            &loaded.ordered,
            &ext,
            test_site("/nonexistent_gnitz_test_path_integrate_trace", 99),
            PlanTarget::ViewOutput { seeds: &[] },
        );
        assert!(
            result.is_err(),
            "IntegrateTrace child-table failure must fail the compile"
        );
    }

    // ── Item 32: sink schema type validation ────────────────────────────────

    #[test]
    fn test_build_plan_sink_schema_type_mismatch_rejected() {
        // ScanDelta(99) → IntegrateSink. The source schema is [U64 pk, I64];
        // the view's declared out_schema is [U64 pk, STRING]. Same column count,
        // different physical layout → must be rejected (item 32), else the client
        // reads a 16-byte string descriptor out of 8-byte integer storage.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(99));
        nodes.insert(1, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN)];
        let mut loaded = loaded_for_test(nodes, edges);
        loaded.out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::STRING, 0),
            ],
            &[0],
        );
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let ext: ExtTables = HashMap::from([(99, in_schema)]);
        let result = build_plan(
            &loaded,
            &no_skips(),
            &loaded.ordered,
            &ext,
            test_site("", 99),
            PlanTarget::ViewOutput { seeds: &[] },
        );
        assert!(result.is_err(), "type-mismatched sink schema must be rejected");
    }

    // ── Item 35: corrupt Filter/Map blob aborts compilation ─────────────────

    #[test]
    fn test_build_plan_corrupt_filter_blob_aborts() {
        // ScanDelta(99) → Filter(blob) → IntegrateSink. A present blob that
        // fails to decode must abort, not silently degrade to WHERE TRUE —
        // whether it is garbled or empty (a damaged catalog cell reads back
        // empty, and `load_circuit` hands it on as present).
        let in_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        // Hold the sink to the input schema so the sink-schema check passes; the
        // only thing that can fail this compile is the blob.
        let fixture = MidCircuit::new(in_schema).with_out_schema(in_schema);
        for blob in [vec![0xFFu8; 16], Vec::new()] {
            let what = if blob.is_empty() { "empty" } else { "garbled" };
            assert!(
                !fixture.compiles(gnitz_wire::OpNode::Filter(Some(blob))),
                "a {what} Filter blob must abort compilation"
            );
        }
    }

    #[test]
    fn test_build_plan_corrupt_map_blob_aborts() {
        // ScanDelta(99) → Map(Expression{corrupt blob}) → IntegrateSink.
        let in_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        let corrupt = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
            program: vec![0xFFu8; 16],
            reindex_cols: vec![],
            reindex_target_tcs: vec![],
            role: gnitz_wire::ReindexRole::Auxiliary,
        });
        assert!(
            !MidCircuit::new(in_schema).compiles(corrupt),
            "corrupt Map blob must abort compilation"
        );
    }

    /// A compound (len > 1) reindex Map now compiles end-to-end: the gate is
    /// lifted and `emit_node` builds a 2-slot-PK node schema, so `build_plan`
    /// returns `Some` (the sink's output schema matches the reindex output).
    #[test]
    fn test_build_plan_compound_reindex_accepted() {
        // Valid 2-col copy program so decode_expr_blob succeeds.
        let mut eb = gnitz_expr::ExprBuilder::new();
        eb.copy_col(0, 0);
        eb.copy_col(1, 1);
        let blob = eb.build(0).encode();

        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        // The sink validates against the reindex Map's output schema (2 synthetic
        // PK slots [U64, I64] + the two input columns).
        let out_schema = crate::ops::ReindexPacker::new(&in_schema, &[0, 1], &[])
            .unwrap()
            .output_schema(&in_schema, &[0, 1])
            .unwrap();
        let map = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
            program: blob,
            reindex_cols: vec![0, 1],
            reindex_target_tcs: vec![],
            role: gnitz_wire::ReindexRole::ScatterKey,
        });
        assert!(
            MidCircuit::new(in_schema).with_out_schema(out_schema).compiles(map),
            "compound (len > 1) reindex must compile after the gate lift"
        );
    }

    /// A reindex list longer than `MAX_PK_COLUMNS` overflows the output schema's
    /// fixed PK array; `emit_node` must fail the compile cleanly (`build_plan`
    /// returns None) rather than panic or build a truncated key.
    #[test]
    fn test_build_plan_reindex_exceeds_max_pk_columns_rejected() {
        // 6-column source, reindex on all 6 → pk_n (6) > MAX_PK_COLUMNS (5).
        let mut eb = gnitz_expr::ExprBuilder::new();
        eb.copy_col(0, 0);
        let blob = eb.build(0).encode();

        let n_cols = crate::schema::MAX_PK_COLUMNS + 1;
        let cols: Vec<SchemaColumn> = (0..n_cols).map(|_| SchemaColumn::new(type_code::U64, 0)).collect();
        let in_schema = SchemaDescriptor::new(&cols, &[0]);
        let reindex_cols: Vec<u16> = (0..n_cols as u16).collect();

        let map = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
            program: blob,
            reindex_cols,
            reindex_target_tcs: vec![],
            role: gnitz_wire::ReindexRole::ScatterKey,
        });
        assert!(
            !MidCircuit::new(in_schema).compiles(map),
            "reindex list > MAX_PK_COLUMNS must fail the compile"
        );
    }

    /// The reindex output payload schema is derived from the program's copy list:
    /// a program that copies only a subset of the input columns compiles to the
    /// pruned `[key slots ‖ kept columns]` layout end-to-end (`build_plan` returns
    /// `Some` against a sink schema built from the same kept list).
    #[test]
    fn test_build_plan_pruned_reindex_compiles() {
        // 3-column source; reindex on col0, program keeps only col 2 as payload.
        let mut eb = gnitz_expr::ExprBuilder::new();
        eb.copy_col(2, 0);
        let blob = eb.build(0).encode();
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let out_schema = crate::ops::ReindexPacker::new(&in_schema, &[0], &[])
            .unwrap()
            .output_schema(&in_schema, &[2])
            .unwrap();
        let map = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
            program: blob,
            reindex_cols: vec![0],
            reindex_target_tcs: vec![],
            role: gnitz_wire::ReindexRole::ScatterKey,
        });
        assert!(
            MidCircuit::new(in_schema).with_out_schema(out_schema).compiles(map),
            "pruned reindex must compile to the derived schema"
        );
    }

    /// A reindex program copying an out-of-range source column is a corrupt/forged
    /// catalog; `emit_node` must fail the compile cleanly (`build_plan` returns
    /// None) rather than read a zeroed schema slot.
    #[test]
    fn test_build_plan_reindex_program_oob_col_rejected() {
        // reindex on col0, program copies col 9 on a 2-column source.
        let mut eb = gnitz_expr::ExprBuilder::new();
        eb.copy_col(9, 0);
        let blob = eb.build(0).encode();
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let map = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
            program: blob,
            reindex_cols: vec![0],
            reindex_target_tcs: vec![],
            role: gnitz_wire::ReindexRole::ScatterKey,
        });
        assert!(
            !MidCircuit::new(in_schema).compiles(map),
            "out-of-range program copy must fail the compile"
        );
    }

    // ── Item 29: scratch dir cleanup on compile failure ─────────────────────

    #[test]
    fn test_build_plan_cleans_scratch_dirs_on_failure() {
        // ScanDelta → IntegrateTrace → Map → IntegrateSink, with the Map
        // projecting an out-of-bounds column so it fails the compile. The
        // IntegrateTrace before it has already created its scratch dir under
        // `view_dir`; `ScratchGuard`'s drop must remove it, so probing
        // unsupported queries can't leak inodes.
        //
        // The failing node must come *after* a node that creates scratch,
        // otherwise there is nothing for the cleanup to remove and the
        // assertion below holds vacuously.
        let dir = tempfile::tempdir().unwrap();
        let view_dir = dir.path();

        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, gnitz_wire::OpNode::IntegrateTrace);
        nodes.insert(2, gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Projection(vec![200])));
        nodes.insert(3, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN)];
        let loaded = loaded_for_test(nodes, edges);
        let ext: ExtTables = HashMap::from([(10, schema)]);
        let result = build_plan(
            &loaded,
            &no_skips(),
            &loaded.ordered,
            &ext,
            test_site(view_dir.to_str().unwrap(), 1),
            PlanTarget::Subgraph { out: 3 },
        );
        assert!(result.is_err(), "out-of-bounds projection must fail the compile");

        let leftover: Vec<String> = std::fs::read_dir(view_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("scratch_"))
            .collect();
        assert!(
            leftover.is_empty(),
            "scratch dirs must be removed on compile failure, found: {leftover:?}",
        );
    }

    /// `compile_view` filters the exchange nids out of the *post* phase's node
    /// list, but a side's list is `ancestors_inclusive` of its own exchange
    /// input with no such filter — so a shard upstream of another shard's input
    /// lands inside that side and reaches `emit_node`. It must reject, not
    /// panic: a panic there is a worker abort, and a worker crash takes the
    /// cluster down. No planner path emits the shape, and the planner asserts
    /// against it, but a circuit hand-built through `gnitz_core::CircuitBuilder`
    /// bypasses the planner entirely.
    #[test]
    fn chained_exchange_rejects_instead_of_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, gnitz_wire::OpNode::ExchangeShard { shard_cols: vec![0] });
        nodes.insert(2, gnitz_wire::OpNode::Filter(None));
        nodes.insert(3, gnitz_wire::OpNode::ExchangeShard { shard_cols: vec![0] });
        nodes.insert(4, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN), (3, 4, PORT_IN)];
        let loaded = loaded_for_test(nodes, edges);
        let ext: ExtTables = HashMap::from([(10, schema)]);

        // The carve `compile_view` performs for the sink-nearest shard.
        let ex_in = exchange_input_node(&loaded, 3).unwrap();
        let set = ancestors_inclusive(&loaded, ex_in);
        let side_ordered: Vec<i32> = loaded.ordered.iter().copied().filter(|n| set.contains(n)).collect();
        assert!(
            side_ordered.contains(&1),
            "fixture must place the upstream shard inside the side's node list, got {side_ordered:?}"
        );

        let result = build_plan(
            &loaded,
            &no_skips(),
            &side_ordered,
            &ext,
            test_site(dir.path().to_str().unwrap(), 1),
            PlanTarget::Subgraph { out: ex_in },
        );
        assert!(
            matches!(result, Err(CompileError::Rejected("chained exchange nodes"))),
            "chained exchange must be a named rejection"
        );
    }

    // ── Items 16 & 28: load_circuit robustness (real system tables) ─────────

    /// The three circuit system tables `load_circuit` reads, on one tempdir.
    /// All three must be live: `load_circuit` opens a cursor over each up front
    /// and returns `None` on a null one, which would pass these assertions
    /// vacuously. Their schemas differ (6/5/7 columns), so one cannot stand in
    /// for another.
    struct CircuitTables {
        _tmp: tempfile::TempDir,
        nodes: Table,
        edges: Table,
        cols: Table,
    }

    impl CircuitTables {
        const VIEW_ID: u64 = 1;

        /// Match `pack_view_pk`: view_id in the high half, so its at-rest OPK
        /// (big-endian) image leads the PK region where `load_circuit` seeks.
        fn pk(sub: u64) -> u128 {
            ((Self::VIEW_ID as u128) << 64) | (sub as u128)
        }

        fn schema(cols: &[gnitz_wire::WireSysCol]) -> SchemaDescriptor {
            crate::schema::from_wire_cols(cols, gnitz_wire::CIRCUIT_FAMILY_PK)
        }

        fn new() -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let open = |name: &str, cols: &[gnitz_wire::WireSysCol]| {
                // The `TempDir` outlives these: each `ShardIndex` holds the path.
                Table::new(
                    &format!("{}/{name}", tmp.path().to_str().unwrap()),
                    Self::schema(cols),
                    0,
                    RecoverySource::Rederive { resume_at: None },
                )
                .unwrap()
            };
            let nodes = open("nodes", gnitz_wire::CIRCUIT_NODES_COLS);
            let edges = open("edges", gnitz_wire::CIRCUIT_EDGES_COLS);
            let cols = open("cols", gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS);
            Self {
                _tmp: tmp,
                nodes,
                edges,
                cols,
            }
        }

        fn fill(tab: &mut Table, cols: &[gnitz_wire::WireSysCol], f: impl FnOnce(&mut crate::storage::BatchBuilder)) {
            let mut bb = crate::storage::BatchBuilder::new(Self::schema(cols));
            f(&mut bb);
            tab.ingest_owned_batch(bb.finish()).unwrap();
        }

        fn put_nodes(&mut self, f: impl FnOnce(&mut crate::storage::BatchBuilder)) -> &mut Self {
            Self::fill(&mut self.nodes, gnitz_wire::CIRCUIT_NODES_COLS, f);
            self
        }

        fn put_edges(&mut self, f: impl FnOnce(&mut crate::storage::BatchBuilder)) -> &mut Self {
            Self::fill(&mut self.edges, gnitz_wire::CIRCUIT_EDGES_COLS, f);
            self
        }

        fn load(&mut self) -> Result<LoadedCircuit, CompileError> {
            load_circuit(
                SysTableRefs {
                    nodes: &mut self.nodes,
                    edges: &mut self.edges,
                    node_columns: &mut self.cols,
                },
                Self::VIEW_ID,
                SchemaDescriptor::default(),
            )
        }
    }

    #[test]
    fn test_load_circuit_aborts_on_undecodable_node() {
        // An opcode `decode_op_node` rejects must abort the whole load, not be
        // skipped into a partial circuit.
        let mut c = CircuitTables::new();
        c.put_nodes(|bb| {
            bb.begin_row(CircuitTables::pk(1), 1);
            bb.put_u64(1); // node_id
            bb.put_u64(9999); // opcode — unknown → decode_op_node Err
            bb.put_null(); // source_table
            bb.put_null(); // expr_program
            bb.end_row();
        });
        assert!(c.load().is_err(), "an undecodable node must abort load_circuit");
    }

    #[test]
    fn test_load_circuit_keeps_empty_expr_blob_present() {
        // A non-NULL expr_program that reads back empty is a damaged blob. The
        // load must hand it on as `Some`, since `None` is how an absent program
        // is spelled and would turn this Filter into `WHERE TRUE`; rejecting the
        // undecodable blob is the compile's job (see the corrupt-blob tests).
        let mut c = CircuitTables::new();
        c.put_nodes(|bb| {
            bb.begin_row(CircuitTables::pk(0), 1);
            bb.put_u64(0); // node_id
            bb.put_u64(gnitz_wire::OPCODE_FILTER);
            bb.put_null(); // source_table
            bb.put_blob(&[]); // expr_program — non-NULL, zero length
            bb.end_row();
        });
        let loaded = c.load().expect("a damaged blob is not a load failure");
        assert!(
            matches!(loaded.nodes.get(&0), Some(gnitz_wire::OpNode::Filter(Some(b))) if b.is_empty()),
            "an empty blob must stay present, not collapse to a pass-all filter"
        );
    }

    #[test]
    fn test_load_circuit_aborts_on_orphan_edge() {
        // An edge whose dst does not exist must abort rather than create a
        // phantom node.
        let mut c = CircuitTables::new();
        c.put_nodes(|bb| {
            // node 0: ScanDelta(source 99)
            bb.begin_row(CircuitTables::pk(0), 1);
            bb.put_u64(0);
            bb.put_u64(gnitz_wire::OPCODE_SCAN_DELTA);
            bb.put_u64(99); // source_table
            bb.put_null(); // expr_program
            bb.end_row();
            // node 1: IntegrateSink
            bb.begin_row(CircuitTables::pk(1), 1);
            bb.put_u64(1);
            bb.put_u64(gnitz_wire::OPCODE_INTEGRATE);
            bb.put_null(); // source_table
            bb.put_null(); // expr_program
            bb.end_row();
        })
        .put_edges(|bb| {
            // Edge 0 → 7, but node 7 does not exist.
            bb.begin_row(CircuitTables::pk(0), 1);
            bb.put_u64(7); // dst_node (orphan)
            bb.put_u64(PORT_IN as u64);
            bb.put_u64(0); // src_node
            bb.end_row();
        });
        assert!(
            c.load().is_err(),
            "an edge to a non-existent node must abort load_circuit"
        );
    }

    // ── helpers shared by join tests ─────────────────────────────────────

    fn two_col_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        )
    }

    // ── Part B: crafted raw-field guards reject at compile, never abort at run ──
    //
    // Each guard is proven by construction: the ONLY difference between the two
    // builds is the crafted field, so a valid build's `Some` and the crafted
    // build's `None` are both attributable solely to that field.

    /// The `ScanDelta(10) → mid → IntegrateSink` fixture: everything a guard test
    /// needs to isolate one crafted field on `mid`.
    struct MidCircuit {
        in_schema: SchemaDescriptor,
        out_schema: Option<SchemaDescriptor>,
    }

    impl MidCircuit {
        fn new(in_schema: SchemaDescriptor) -> Self {
            MidCircuit {
                in_schema,
                out_schema: None,
            }
        }

        /// Hold the sink to `out_schema`. Left unset, `build_plan` is entered on
        /// its `output_node_id` path, which suppresses the sink-schema contract —
        /// what a test isolating a *mid-node* guard wants, since the mid node's
        /// output schema is exactly what it is varying.
        fn with_out_schema(mut self, out_schema: SchemaDescriptor) -> Self {
            self.out_schema = Some(out_schema);
            self
        }

        fn build(&self, mid: gnitz_wire::OpNode) -> Result<PlanBuildResult, CompileError> {
            let dir = tempfile::tempdir().unwrap();
            let mut nodes = HashMap::new();
            nodes.insert(0, scan_delta(10));
            nodes.insert(1, mid);
            nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
            let mut loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]);
            if let Some(out) = self.out_schema {
                loaded.out_schema = out;
            }
            let ext: ExtTables = HashMap::from([(10, self.in_schema)]);
            build_plan(
                &loaded,
                &no_skips(),
                &loaded.ordered,
                &ext,
                test_site(dir.path().to_str().unwrap(), 1),
                match self.out_schema {
                    Some(_) => PlanTarget::ViewOutput { seeds: &[] },
                    None => PlanTarget::Subgraph { out: 2 },
                },
            )
        }

        fn compiles(&self, mid: gnitz_wire::OpNode) -> bool {
            self.build(mid).is_ok()
        }
    }

    /// Build `ScanDelta(10) → mid → IntegrateSink` and report whether it compiles.
    fn compiles_mid_node(in_schema: SchemaDescriptor, mid: gnitz_wire::OpNode) -> bool {
        MidCircuit::new(in_schema).compiles(mid)
    }

    #[test]
    fn test_reduce_group_cols_out_of_bounds_rejected() {
        use crate::schema::ReduceOutKey;
        use gnitz_wire::{AggFunc, OpNode};
        let reduce = |group: Vec<u16>| OpNode::Reduce {
            group_cols: group,
            agg: vec![(AggFunc::Count, 0)],
            global_ground: false,
            out_key: ReduceOutKey::PkPermutation,
        };
        assert!(compiles_mid_node(two_col_schema(), reduce(vec![0])));
        assert!(!compiles_mid_node(two_col_schema(), reduce(vec![200])));
    }

    #[test]
    fn test_reduce_agg_spec_col_out_of_bounds_rejected() {
        use crate::schema::ReduceOutKey;
        use gnitz_wire::{AggFunc, OpNode};
        let reduce = |col: u16| OpNode::Reduce {
            group_cols: vec![0],
            agg: vec![(AggFunc::Count, col)],
            global_ground: false,
            out_key: ReduceOutKey::PkPermutation,
        };
        assert!(compiles_mid_node(two_col_schema(), reduce(1)));
        assert!(!compiles_mid_node(two_col_schema(), reduce(200)));
    }

    /// A reduce owns its trace-in. `reduce_node` wires `PORT_IN` only and
    /// `PORT_TRACE` is written solely by `binary_join`, but the circuit families
    /// carry no catalog precheck, so a forged bundle can land the edge here.
    /// Honouring it would make the reduce's trace-in a *delta* register with no
    /// `Integrate` behind it, and `cursor_mut!` hard-asserts on the null cursor —
    /// a worker abort. Reject instead.
    #[test]
    fn reduce_with_a_forged_trace_edge_rejects_instead_of_aborting() {
        use crate::schema::ReduceOutKey;
        use gnitz_wire::{AggFunc, OpNode};
        let dir = tempfile::tempdir().unwrap();
        let reduce = OpNode::Reduce {
            group_cols: vec![0],
            agg: vec![(AggFunc::Count, 1)],
            global_ground: false,
            out_key: ReduceOutKey::PkPermutation,
        };
        let build = |edges: Vec<(i32, i32, i32)>| {
            let mut nodes = HashMap::new();
            nodes.insert(0, scan_delta(10));
            nodes.insert(1, reduce.clone());
            nodes.insert(2, OpNode::IntegrateSink);
            let loaded = loaded_for_test(nodes, edges);
            let ext: ExtTables = HashMap::from([(10, two_col_schema())]);
            build_plan(
                &loaded,
                &no_skips(),
                &loaded.ordered,
                &ext,
                test_site(dir.path().to_str().unwrap(), 1),
                PlanTarget::Subgraph { out: 2 },
            )
            .map(|_| ())
        };
        // Control: the same circuit without the trace edge compiles.
        assert!(build(vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]).is_ok());
        assert!(matches!(
            build(vec![(0, 1, PORT_IN), (0, 1, PORT_TRACE), (1, 2, PORT_IN)]),
            Err(CompileError::Rejected("reduce: unexpected trace input port"))
        ));
    }

    #[test]
    fn test_reduce_sum_over_non_decodable_column_rejected() {
        use crate::schema::ReduceOutKey;
        use gnitz_wire::{AggFunc, OpNode};
        // col 0 = U64 PK + group key; col 1 = the SUM aggregate column.
        let schema = |agg_tc: u8| {
            SchemaDescriptor::new(
                &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(agg_tc, 0)],
                &[0],
            )
        };
        let reduce = OpNode::Reduce {
            group_cols: vec![0],
            agg: vec![(AggFunc::Sum, 1)],
            global_ground: false,
            out_key: ReduceOutKey::PkPermutation,
        };
        // Control: SUM over an order-encodable I64 column compiles.
        assert!(compiles_mid_node(schema(type_code::I64), reduce.clone()));
        // SUM over a 16-byte column would abort in `SumWiden::classify`.
        assert!(!compiles_mid_node(schema(type_code::U128), reduce.clone()));
        // SUM over a STRING column would silently mis-sum.
        assert!(!compiles_mid_node(schema(type_code::STRING), reduce));
    }

    #[test]
    fn test_projection_col_out_of_bounds_rejected() {
        use gnitz_wire::{MapKind, OpNode};
        let proj = |cols: Vec<u16>| OpNode::Map(MapKind::Projection(cols));
        assert!(compiles_mid_node(two_col_schema(), proj(vec![1])));
        assert!(!compiles_mid_node(two_col_schema(), proj(vec![200])));
        // A PK source: `build_map_output_schema` drops it while `copy_cols`
        // numbers destinations densely, so the copy addresses a slot that does
        // not exist — `from_map` would index past the fixed `[_; 65]`.
        assert!(!compiles_mid_node(two_col_schema(), proj(vec![0])));
        // `oob_cols` bounds each index but not the list length, and duplicates
        // are legal, so a long list overruns `build_map_output_schema`'s array.
        // Exactly MAX_COLUMNS payload sources already overflow — the schema also
        // carries the input's PK column, which a length-only bound misses.
        assert!(!compiles_mid_node(
            two_col_schema(),
            proj(vec![1; crate::schema::MAX_COLUMNS]),
        ));
    }

    /// The two derived map out-schemas are by construction ones `ScalarFunc`
    /// accepts. `HashRow` is the only site that emits a *promoting* `CopyCol`,
    /// so it is what makes `check_copy_types`' widening clause do work; the
    /// reindex case pins that `payload_copy_srcs` and
    /// `ReindexPacker::output_schema` cannot drift apart into a mixed-type copy.
    #[test]
    fn test_derived_map_schemas_satisfy_copy_types() {
        use crate::expr::ScalarFunc;
        use gnitz_expr::LogicalProgram;
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::STRING, 1),
                SchemaColumn::new(type_code::F32, 1),
                SchemaColumn::new(type_code::U128, 1),
                SchemaColumn::new(type_code::U32, 1),
            ],
            &[0],
        );
        // Every source column copied into a dense destination slot, the shape
        // `create_universal_projection` builds.
        let cols: Vec<u32> = vec![0, 1, 2, 3, 4];
        let prog = || LogicalProgram::copy_cols(&cols);
        let payload_cols = prog().payload_copy_srcs().unwrap().to_vec();
        let reindexed = crate::ops::ReindexPacker::new(&in_schema, &[4], &[type_code::U64])
            .unwrap()
            .output_schema(&in_schema, &payload_cols)
            .unwrap();
        assert!(ScalarFunc::from_map(prog(), &in_schema, &reindexed).is_ok());

        // A cross-width set-op coercion: the U32 column promoted to I64, every
        // other column carried verbatim (target 0). The promotion is one
        // `payload_promotion_invalid` admits, so a real HashRow can build it.
        let tcs = vec![0, 0, 0, 0, type_code::I64];
        let wire_cols: Vec<u16> = cols.iter().map(|&c| c as u16).collect();
        assert!(!optimize::payload_promotion_invalid(&wire_cols, &tcs, &in_schema));
        let hashed = hashrow_output_schema(&in_schema, &cols, &tcs).unwrap();
        assert!(ScalarFunc::from_map(prog(), &in_schema, &hashed).is_ok());
    }

    #[test]
    fn test_null_extend_overflow_rejected() {
        use gnitz_wire::OpNode;
        // A short type_codes list null-extends cleanly.
        assert!(compiles_mid_node(
            two_col_schema(),
            OpNode::NullExtend {
                type_codes: vec![type_code::I64],
            },
        ));
        // MAX_COLUMNS type_codes overflow the fixed `[_; 65]` schema array (guard 4).
        assert!(!compiles_mid_node(
            two_col_schema(),
            OpNode::NullExtend {
                type_codes: vec![type_code::I64; crate::schema::MAX_COLUMNS],
            },
        ));
        // (An undecodable type code is rejected at the wire decode boundary,
        // where the two sibling type-code lists are also validated.)
        // A near-max-width input plus a short extension overflows the *merged*
        // output width (guard 5, which guard 4 alone cannot catch): 64 + 2 > 65.
        let wide = {
            let mut cols = [SchemaColumn::EMPTY; crate::schema::MAX_COLUMNS];
            cols[0] = SchemaColumn::new(type_code::U64, 0);
            for c in cols.iter_mut().take(64).skip(1) {
                *c = SchemaColumn::new(type_code::I64, 0);
            }
            SchemaDescriptor::new(&cols[..64], &[0])
        };
        assert!(!compiles_mid_node(
            wide,
            OpNode::NullExtend {
                type_codes: vec![type_code::I64, type_code::I64],
            },
        ));
    }

    /// A minimal but structurally valid serialized expr-program blob for tests
    /// that need a `Map(Expression { program, .. })` or `Filter(Some(..))` to
    /// exist without ever executing it: magic `GNIT`, version 1, zero-length
    /// code, no constants.
    fn dummy_expr_blob() -> Vec<u8> {
        vec![
            0x47, 0x4e, 0x49, 0x54, // magic "GNIT"
            0x01, // version
            0, 0, 0, 0, 0, // reserved
            0, 0, 0, 0, // code_len = 0
            0, // nconst = 0
        ]
    }

    /// No optimizer-elided Distinct nodes — the skip set most tests build with.
    fn no_skips() -> HashSet<i32> {
        HashSet::new()
    }

    /// A view site over a throwaway directory: nothing under it was ever
    /// checkpointed, so the children it opens resume nothing.
    fn test_site(dir: &str, id: u64) -> ViewSite<'_> {
        ViewSite {
            dir,
            id,
            recovery: RecoverySource::Rederive { resume_at: None },
        }
    }

    // ── Destructive-register ordering invariant ─────────────────────────────
    //
    // Union/Distinct/PositivePart empty their input register in place.
    // When that register fans out to other consumers, the destructive op must
    // run LAST among the register's readers, or the co-readers see an emptied
    // batch. build_plan rejects violations (over the emitted instructions, so
    // register aliasing is seen through) in every build profile, release included.

    /// Build the INTERSECT/EXCEPT fan-out shape: ScanDelta(10)'s register fans
    /// into both a destructive `Distinct` and a non-destructive `Negate`
    /// co-reader (standing in for integrate_trace); the Distinct feeds the
    /// IntegrateSink at node 3. Caller picks `distinct_id`/`reader_id` — Kahn's
    /// ascending tie-break schedules the lower id first, so the ids decide which
    /// consumer the scheduler runs first. (The reader is a `Negate`, not a
    /// `Filter(None)`: a predicate-less Filter is elided by register aliasing
    /// and would no longer read the register at runtime.)
    fn make_dtor_fanout(distinct_id: i32, reader_id: i32) -> LoadedCircuit {
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(distinct_id, gnitz_wire::OpNode::Distinct);
        nodes.insert(reader_id, gnitz_wire::OpNode::Negate);
        nodes.insert(3, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![
            (0, distinct_id, PORT_IN),
            (0, reader_id, PORT_IN),
            (distinct_id, 3, PORT_IN),
        ];
        loaded_for_test(nodes, edges)
    }

    #[test]
    fn test_destructive_fanout_legit_ordering_compiles() {
        // Distinct id 2 > Filter id 1, so the destructive op is scheduled LAST:
        // the assert must NOT fire and the circuit compiles end to end.
        let dir = tempfile::tempdir().unwrap();
        let view_dir = dir.path().to_str().unwrap();

        let loaded = make_dtor_fanout(2, 1);
        // Precondition: the destructive Distinct really is scheduled after its co-reader.
        let pos = |n: i32| loaded.ordered.iter().position(|&x| x == n).unwrap();
        assert!(pos(1) < pos(2), "test precondition: co-reader must precede Distinct");

        let ext: ExtTables = HashMap::from([(10, two_col_schema())]);
        // The plan owns scratch dirs under `dir`, so it must drop first: build it
        // inside the assert rather than binding it past `dir`'s scope.
        assert!(
            build_plan(
                &loaded,
                &no_skips(),
                &loaded.ordered,
                &ext,
                test_site(view_dir, 1),
                PlanTarget::Subgraph { out: 3 }
            )
            .is_ok(),
            "legitimate destructive fan-out must compile without tripping the ordering assert"
        );
    }

    #[test]
    fn test_destructive_fanout_bad_ordering_rejected() {
        // Distinct id 1 < Filter id 2, so the ascending tie-break schedules the
        // destructive op FIRST — it would empty ScanDelta's register before the
        // Filter reads it. build_plan must reject this rather than emit it.
        let loaded = make_dtor_fanout(1, 2);
        let ext: ExtTables = HashMap::from([(10, two_col_schema())]);
        let result = build_plan(
            &loaded,
            &no_skips(),
            &loaded.ordered,
            &ext,
            test_site("", 1),
            PlanTarget::Subgraph { out: 3 },
        );
        assert!(
            result.is_err(),
            "destructive-first fan-out must be rejected (return None), not emitted"
        );
    }

    #[test]
    fn test_destructive_fanout_skipped_distinct_not_rejected() {
        // Distinct id 1 schedules before Filter id 2 — the destructive-first shape
        // the guard rejects. But opt_distinct has elided the Distinct (it is in
        // skip_nodes): it aliases ScanDelta's register and emits no destructive op,
        // so the guard must NOT reject it.
        let dir = tempfile::tempdir().unwrap();
        let view_dir = dir.path().to_str().unwrap();

        let loaded = make_dtor_fanout(1, 2);
        let mut skips = no_skips();
        skips.insert(1); // Distinct elided by the distinct-elision pass

        let ext: ExtTables = HashMap::from([(10, two_col_schema())]);
        // The plan owns scratch dirs under `dir`, so it must drop first: build it
        // inside the assert rather than binding it past `dir`'s scope.
        assert!(
            build_plan(
                &loaded,
                &skips,
                &loaded.ordered,
                &ext,
                test_site(view_dir, 1),
                PlanTarget::Subgraph { out: 3 }
            )
            .is_ok(),
            "a skipped (optimized-out) Distinct does not run destructively; \
             the guard must not reject it"
        );
    }

    // ── compute_scatter_routing covers ScanDelta (SQL-planner join pattern) ──

    /// The routing walk must find ScanDelta → Map(reindex) chains.
    #[test]
    fn test_scatter_routing_scan_delta() {
        use gnitz_wire::{MapKind, OpNode};

        // Minimal two-sided SQL join circuit skeleton:
        //   ScanDelta(left_tid=10) → Map(reindex_col=1) → Join → IntegrateSink
        //   ScanDelta(right_tid=20) → Map(reindex_col=0) → Join
        let dummy_blob = dummy_expr_blob();
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![1],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(2, scan_delta(20));
        nodes.insert(
            3,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![0],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(4, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(5, OpNode::IntegrateSink);
        let edges = vec![
            (0, 1, PORT_IN),
            (2, 3, PORT_IN),
            (1, 4, PORT_IN_A),
            (3, 4, PORT_TRACE),
            (4, 5, PORT_IN),
        ];
        let loaded = loaded_for_test(nodes, edges);

        let keys = compute_scatter_routing(&loaded, &ExtTables::default()).keys;

        assert_eq!(
            keys.get(&10),
            Some(&Some(vec![(1, 0)])),
            "left side (source 10) must map to reindex_col=1"
        );
        assert_eq!(
            keys.get(&20),
            Some(&Some(vec![(0, 0)])),
            "right side (source 20) must map to reindex_col=0"
        );
    }

    #[test]
    fn test_scatter_routing_through_filter() {
        use gnitz_wire::{MapKind, OpNode};
        // ScanDelta(42) → Filter → Map(reindex_col=1) → Join → IntegrateSink.
        // The reindex Map is two hops from the scan (a Filter sits between),
        // so the one-hop lookup misses it; BFS through Filter must find it.
        let dummy_blob = dummy_expr_blob();
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(42));
        nodes.insert(1, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(
            2,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![1],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(3, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(4, OpNode::IntegrateSink);
        let edges = vec![
            (0, 1, PORT_IN), // ScanDelta → Filter
            (1, 2, PORT_IN), // Filter → reindex Map
            (2, 3, PORT_IN_A),
            (3, 4, PORT_IN),
        ];
        let loaded = loaded_for_test(nodes, edges);

        let keys = compute_scatter_routing(&loaded, &ExtTables::default()).keys;
        assert_eq!(
            keys.get(&42),
            Some(&Some(vec![(1, 0)])),
            "ScanDelta → Filter → Map(reindex) must map source 42 to col 1"
        );
    }

    /// Two `ScanDelta` nodes on ONE source with DIFFERENT scatter keys. Under the
    /// old `loaded.nodes` walk, last-writer-wins over a per-process `RandomState`
    /// order picked one of the two at random — and the master and the worker
    /// derive their halves of the routing in separate processes. Accumulating
    /// over `loaded.ordered` makes the answer the same everywhere: two sequences,
    /// so no single pack key routes the source and the relay refuses.
    #[test]
    fn test_scatter_routing_two_keys_one_source_is_deterministic() {
        use gnitz_wire::{MapKind, OpNode};

        let dummy_blob = dummy_expr_blob();
        let reindex = |col: u32, blob| {
            OpNode::Map(MapKind::Expression {
                program: blob,
                reindex_cols: vec![col as u16],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            })
        };
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, reindex(1, dummy_blob.clone()));
        nodes.insert(2, scan_delta(10));
        nodes.insert(3, reindex(2, dummy_blob));
        nodes.insert(4, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(5, OpNode::IntegrateSink);
        let edges = vec![
            (0, 1, PORT_IN),
            (2, 3, PORT_IN),
            (1, 4, PORT_IN_A),
            (3, 4, PORT_TRACE),
            (4, 5, PORT_IN),
        ];
        let loaded = loaded_for_test(nodes, edges);

        let keys = compute_scatter_routing(&loaded, &ExtTables::default()).keys;
        assert_eq!(
            keys.get(&10),
            Some(&None),
            "two distinct keys on one source must refuse, not pick one at random"
        );
    }

    /// The same shape with the SAME key on both scans must still route. Today's
    /// last-writer-wins resolves it to the one correct sequence, so a naive
    /// accumulation that skipped the cross-node dedup would newly produce two
    /// sequences and refuse a round that works — a regression the rewrite must
    /// not introduce.
    #[test]
    fn test_scatter_routing_repeated_key_one_source_still_routes() {
        use gnitz_wire::{MapKind, OpNode};

        let dummy_blob = dummy_expr_blob();
        let reindex = |blob| {
            OpNode::Map(MapKind::Expression {
                program: blob,
                reindex_cols: vec![1],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            })
        };
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, reindex(dummy_blob.clone()));
        nodes.insert(2, scan_delta(10));
        nodes.insert(3, reindex(dummy_blob));
        nodes.insert(4, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(5, OpNode::IntegrateSink);
        let edges = vec![
            (0, 1, PORT_IN),
            (2, 3, PORT_IN),
            (1, 4, PORT_IN_A),
            (3, 4, PORT_TRACE),
            (4, 5, PORT_IN),
        ];
        let loaded = loaded_for_test(nodes, edges);

        let keys = compute_scatter_routing(&loaded, &ExtTables::default()).keys;
        assert_eq!(
            keys.get(&10),
            Some(&Some(vec![(1, 0)])),
            "one key reached twice is still one key"
        );
    }

    #[test]
    fn test_circuit_range_join_n_eq_discriminator() {
        use crate::schema::ReduceOutKey;
        use gnitz_wire::{AggFunc, JoinKind, MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();

        // A GROUP BY view: ScanDelta → Map(reindex) → ExchangeShard → Reduce →
        // IntegrateSink. It has BOTH a reindex Map (has_join_shard) AND an
        // ExchangeShard (has_exchange), so the wrong discriminator
        // `has_join_shard && has_exchange` would (incorrectly) call it a range
        // join. circuit_range_join_n_eq must return None — no DeltaTraceRange node.
        let mut gb = HashMap::new();
        gb.insert(0, scan_delta(7));
        gb.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![1],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        gb.insert(2, OpNode::ExchangeShard { shard_cols: vec![1] });
        gb.insert(
            3,
            OpNode::Reduce {
                group_cols: vec![1],
                // Only exercises range-join classification, never the reduce
                // output schema/validation; the spec is immaterial here.
                agg: vec![(AggFunc::Count, 0)],
                global_ground: false,
                out_key: ReduceOutKey::SyntheticFold,
            },
        );
        gb.insert(4, OpNode::IntegrateSink);
        let gb_edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN), (3, 4, PORT_IN)];
        let gb_loaded = loaded_for_test(gb, gb_edges);
        assert_eq!(
            circuit_range_join_n_eq(&gb_loaded),
            None,
            "GROUP BY view must NOT be classified as a range join"
        );

        // A range join: a Join(DeltaTraceRange) node makes it Some, carrying n_eq.
        let mut rj = HashMap::new();
        rj.insert(0, scan_delta(7));
        rj.insert(1, OpNode::IntegrateTrace);
        rj.insert(
            2,
            OpNode::Join(JoinKind::DeltaTraceRange {
                n_eq: 2,
                rel: gnitz_wire::RangeRel::Lt,
            }),
        );
        rj.insert(3, OpNode::IntegrateSink);
        rj.insert(4, scan_delta(8));
        let rj_edges = vec![(0, 2, PORT_IN_A), (4, 1, PORT_IN), (1, 2, PORT_TRACE), (2, 3, PORT_IN)];
        let rj_loaded = loaded_for_test(rj, rj_edges);
        assert_eq!(
            circuit_range_join_n_eq(&rj_loaded),
            Some(2),
            "a Join(DeltaTraceRange) node classifies the view as a range join, carrying its n_eq"
        );
    }

    #[test]
    fn test_co_partition_keys_with_worker_filter_after_map() {
        use gnitz_wire::{MapKind, OpNode};
        // A route-key Map followed by a WorkerFilter: the walk reaches the Map and
        // returns its cols, then stops — a WorkerFilter is not a Filter, so it is
        // never stepped through. What the Map *feeds* does not enter into it.
        let dummy_blob = dummy_expr_blob();
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(99));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(2, OpNode::WorkerFilter);
        nodes.insert(3, OpNode::IntegrateTrace);
        let edges = vec![
            (0, 1, PORT_IN), // ScanDelta → reindex Map
            (1, 2, PORT_IN), // Map → WorkerFilter
            (2, 3, PORT_IN), // WorkerFilter → IntegrateTrace
        ];
        let loaded = loaded_for_test(nodes, edges);
        assert_eq!(
            co_partition_keys(&loaded, 0),
            vec![(2, 0)],
            "WorkerFilter after the reindex Map must not change the walk result"
        );
    }

    /// Real pure-range-join shape (planner.rs, `n_eq == 0`): the reindex Map feeds
    /// the `Join(DeltaTraceRange)` node DIRECTLY as the delta term AND feeds a
    /// `WorkerFilter → IntegrateTrace` toward the trace term. Its key is collected
    /// once, from the flag — the fan-out is not a second contribution. This is what
    /// keeps the join-shard map non-empty for a pure range join (hence
    /// `prepare_relay`'s `is_join` / `range_n_eq` and the broadcast routing).
    #[test]
    fn test_co_partition_keys_range_join_feeds_join_directly() {
        use gnitz_wire::{JoinKind, MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(99) ─► Map(reindex=[2]) ─┬─► Join(DeltaTraceRange)  [delta, PORT_IN_A]
        //                                     └─► WorkerFilter ─► IntegrateTrace ─► Join  [PORT_TRACE]
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(99));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(2, OpNode::WorkerFilter);
        nodes.insert(3, OpNode::IntegrateTrace);
        nodes.insert(
            4,
            OpNode::Join(JoinKind::DeltaTraceRange {
                n_eq: 0,
                rel: gnitz_wire::RangeRel::Le,
            }),
        );
        let edges = vec![
            (0, 1, PORT_IN),    // ScanDelta → reindex Map
            (1, 4, PORT_IN_A),  // reindex Map → Join (delta term, DIRECT edge)
            (1, 2, PORT_IN),    // reindex Map → WorkerFilter (toward the trace)
            (2, 3, PORT_IN),    // WorkerFilter → IntegrateTrace
            (3, 4, PORT_TRACE), // IntegrateTrace → Join (trace term)
        ];
        let loaded = loaded_for_test(nodes, edges);
        assert_eq!(
            co_partition_keys(&loaded, 0),
            vec![(2, 0)],
            "the reindex feeds the Join directly, so feeds_trace_or_join is true \
             even with a WorkerFilter toward the trace"
        );
    }

    #[test]
    fn test_reindex_col_through_filters_trivial_and_absent() {
        use gnitz_wire::{MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        // Trivial: ScanDelta → Map(reindex) directly (no Filter).
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![3],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN)]);
        assert_eq!(co_partition_keys(&loaded, 0), vec![(3, 0)]);

        // Absent: ScanDelta → Map with no reindex columns.
        let mut nodes2 = HashMap::new();
        nodes2.insert(0, scan_delta(7));
        nodes2.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::Auxiliary,
            }),
        );
        let loaded2 = loaded_for_test(nodes2, vec![(0, 1, PORT_IN)]);
        assert!(co_partition_keys(&loaded2, 0).is_empty());
    }

    /// Multi-join: a single ScanDelta fans out through two reindex Maps on
    /// different columns. Both column IDs must be collected, not just the first.
    #[test]
    fn test_co_partition_keys_multi_join() {
        use gnitz_wire::{MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(0) ──► Map(reindex_col=2)
        //              └──► Filter ──► Map(reindex_col=5)
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(42));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(2, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(
            3,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![5],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        let edges = vec![(0, 1, PORT_IN), (0, 2, PORT_IN), (2, 3, PORT_IN)];
        let loaded = loaded_for_test(nodes, edges);
        let mut got = co_partition_keys(&loaded, 0);
        got.sort_unstable();
        assert_eq!(got, vec![(2, 0), (5, 0)], "both reindex columns must be collected");
    }

    /// An overlapping key (`a.x = b.p AND a.x = b.q`) reindexes `[x, x]`, possibly
    /// with distinct per-slot promotion targets. The sequence must survive
    /// VERBATIM — duplicates and all — so the scatter packer mirrors the trace-side
    /// ReindexPacker slot-for-slot; column-level dedup would collapse it to one.
    #[test]
    fn test_co_partition_keys_overlapping_key_verbatim() {
        use gnitz_wire::{MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(42));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![3, 3],
                reindex_target_tcs: vec![0, type_code::I64],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN)]);
        assert_eq!(
            co_partition_keys(&loaded, 0),
            vec![(3, 0), (3, type_code::I64)],
            "overlapping key sequence must survive verbatim, not be deduplicated"
        );
    }

    /// A nullable LEFT-join key fans its source to two sibling reindex Maps (the
    /// not-null match side and the null-key bypass) carrying an IDENTICAL sequence.
    /// They must collapse to ONE copy, never be concatenated (which would double
    /// the key columns and diverge from the trace).
    #[test]
    fn test_co_partition_keys_sibling_maps_collapse() {
        use gnitz_wire::{MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(7) ──► Filter(not-null) ──► Map(reindex [2])
        //              └──► Filter(is-null)  ──► Map(reindex [2])  (identical seq)
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(1, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(
            2,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![2],
                reindex_target_tcs: vec![0],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(3, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(
            4,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![2],
                reindex_target_tcs: vec![0],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (0, 3, PORT_IN), (3, 4, PORT_IN)];
        let loaded = loaded_for_test(nodes, edges);
        assert_eq!(
            co_partition_keys(&loaded, 0),
            vec![(2, 0)],
            "identical sibling sequences must collapse to one, not concatenate"
        );
    }

    /// Band LEFT join shape: the left scan feeds BOTH the join reindex (`[eq, range]`)
    /// AND an auxiliary `a.pk` re-key for the null-fill. Only the join reindex defines
    /// the input scatter key, and the planner says so by flagging one and not the
    /// other — concatenating both would corrupt the eq-prefix scatter.
    #[test]
    fn test_co_partition_keys_ignores_aux_rekey_in_join_view() {
        use gnitz_wire::{JoinKind, MapKind, OpNode, RangeRel};
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(10) ──► Map(reindex [1,2]) ──► Join(DeltaTraceRange)
        //              │                       └─► IntegrateTrace
        //              └──► Map(reindex [0]) ──► Map(Projection) ──► Distinct   (a_all → proj_a → D)
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![1, 2],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(
            2,
            OpNode::Join(JoinKind::DeltaTraceRange {
                n_eq: 1,
                rel: RangeRel::Le,
            }),
        );
        nodes.insert(3, OpNode::IntegrateTrace);
        nodes.insert(
            4,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![0],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::Auxiliary,
            }),
        );
        nodes.insert(5, OpNode::Map(MapKind::Projection(vec![])));
        nodes.insert(6, OpNode::Distinct);
        let edges = vec![
            (0, 1, PORT_IN),
            (1, 2, PORT_IN_A),
            (1, 3, PORT_IN), // join reindex → Join + trace
            (0, 4, PORT_IN),
            (4, 5, PORT_IN),
            (5, 6, PORT_IN), // aux a.pk re-key → proj → distinct
        ];
        let loaded = loaded_for_test(nodes, edges);
        assert_eq!(
            co_partition_keys(&loaded, 0),
            vec![(1, 0), (2, 0)],
            "only the trace/probe-feeding reindex defines the scatter key; the a.pk re-key is ignored"
        );
    }

    /// A source whose path to the join carries no reindex Map stays out of the map.
    #[test]
    fn test_scatter_routing_unreindexed_trace_side_absent() {
        use gnitz_wire::{MapKind, OpNode};

        let dummy_blob = dummy_expr_blob();
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, scan_delta(20));
        nodes.insert(
            2,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(3, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(4, OpNode::IntegrateSink);
        nodes.insert(5, OpNode::IntegrateTrace);
        let edges = vec![
            (0, 2, PORT_IN),    // ScanDelta → reindex Map
            (1, 5, PORT_IN),    // ScanDelta(20) → IntegrateTrace (no reindex)
            (5, 3, PORT_TRACE), // trace → join trace port
            (2, 3, PORT_IN_A),
            (3, 4, PORT_IN),
        ];
        let loaded = loaded_for_test(nodes, edges);

        let map = compute_scatter_routing(&loaded, &ExtTables::default()).keys;

        // ScanDelta(10) → Map(reindex_col=2) must be found.
        assert_eq!(
            map.get(&10),
            Some(&Some(vec![(2, 0)])),
            "ScanDelta source must be in the routing map"
        );
        // Source 20 has no downstream reindex Map — must NOT appear.
        assert!(
            !map.contains_key(&20),
            "a source with no reindex Map must not be in join_shard_map"
        );
    }

    // ── scan_tid_through_filters: the backward (shard → scan) Filter walk ───────
    //
    // The view exchange-skip detector's source resolution. The skip itself has no
    // observable "fired" signal at the E2E layer (exchanging is also correct), so
    // these unit tests are what pin that the walk engages exactly when it should:
    // through Filter chains, never across a re-keying Map / WorkerFilter / fan-in.

    /// A bare `ScanDelta → ExchangeShard` (the no-`WHERE` case) resolves to the source
    /// tid; `ScanDelta → Filter → ExchangeShard` (filtered `GROUP BY prefix`) does too,
    /// as does a chain of Filters.
    #[test]
    fn test_scan_tid_through_filters_filter_chain() {
        use gnitz_wire::OpNode;
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(7) → ExchangeShard, no Filter: the zero-hop base case (no `WHERE`),
        // which already co-partitioned before the walk reached through Filters.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(1, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 1),
            Some(7),
            "a bare scan feeding the shard resolves on the first hop (no-`WHERE` case)"
        );

        // ScanDelta(7) → Filter → ExchangeShard.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(1, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(2, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 2),
            Some(7),
            "one Filter between scan and shard is transparent to the shard key"
        );

        // ScanDelta(8) → Filter → Filter → ExchangeShard.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(8));
        nodes.insert(1, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(2, OpNode::Filter(Some(dummy_blob)));
        nodes.insert(3, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 3),
            Some(8),
            "a chain of Filters is transparent to the shard key"
        );
    }

    /// A re-keying `Map` rewrites the PK region, so the walk must bail there — even
    /// with a Filter below it (the DISTINCT / set-op `HashRow` reindex shape).
    #[test]
    fn test_scan_tid_through_filters_stops_at_map() {
        use gnitz_wire::{MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(7) → Map(reindex) → ExchangeShard.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(2, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 2),
            None,
            "a reindex Map re-keys the PK; the walk must bail rather than cross it"
        );

        // ScanDelta(7) → Map(reindex) → Filter → ExchangeShard: a Filter below the
        // Map does not rescue it — the walk still reaches the Map and bails.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(2, OpNode::Filter(Some(dummy_blob)));
        nodes.insert(3, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 3),
            None,
            "a Filter below a reindex Map does not make the Map transparent"
        );
    }

    /// A `WorkerFilter` (range-join broadcast input) is a distinct OpNode variant,
    /// not a `Filter`, so it is never crossed — the same exclusion
    /// `reindex_cols_through_filters` makes on the forward walk.
    #[test]
    fn test_scan_tid_through_filters_worker_filter() {
        use gnitz_wire::OpNode;
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(1, OpNode::WorkerFilter);
        nodes.insert(2, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 2),
            None,
            "WorkerFilter is not a Filter; the walk must bail"
        );
    }

    /// A fan-in (≠ 1 incoming edge) is not a linear chain — bail. Tested at the shard
    /// itself (two scans feeding it) and one hop in (two scans feeding a Filter).
    #[test]
    fn test_scan_tid_through_filters_fan_in() {
        use gnitz_wire::OpNode;
        let dummy_blob = dummy_expr_blob();
        // Two scans feed the ExchangeShard directly (set-op-like fan-in).
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(1, scan_delta(8));
        nodes.insert(2, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 2, PORT_IN_A), (1, 2, PORT_IN_B)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 2),
            None,
            "a shard with two incoming edges is a fan-in, not a linear chain"
        );

        // Fan-in one hop in: two scans feed a Filter that feeds the shard. The shard
        // has one incoming edge, but the Filter has two — bail at the Filter.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(7));
        nodes.insert(1, scan_delta(8));
        nodes.insert(2, OpNode::Filter(Some(dummy_blob)));
        nodes.insert(3, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = loaded_for_test(nodes, vec![(0, 2, PORT_IN_A), (1, 2, PORT_IN_B), (2, 3, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 3),
            None,
            "a fan-in at an intervening Filter also bails (the Filter has two incoming edges)"
        );
    }

    // ── Finding 2: load_circuit must return None for null system-table pointers ──

    /// Null system-table pointers are a programming error; the engine always supplies
    /// valid handles. `load_circuit` must fail so callers get an explicit error
    /// rather than silently reading an incomplete circuit and producing wrong results.
    #[test]
    fn test_load_circuit_fails_for_null_system_tables() {
        let result = load_circuit(SysTableRefs::null(), 0, SchemaDescriptor::default());
        assert!(
            matches!(result, Err(CompileError::LoadFailed)),
            "null system-table pointers must fail the load, not yield a silently empty circuit"
        );
    }

    // ── Union output-schema nullability merge (null-comparator honesty) ──

    /// `union_nullability_merge` ORs the two inputs' per-column nullability, so a
    /// null-carrying side reclassifies the output from the null-blind
    /// `FixedIntNonnull` fast comparator to the null-aware `Generic` one.
    #[test]
    fn test_union_nullability_merge_classification() {
        use crate::schema::PayloadCmpKind;
        let pk = SchemaColumn::new(type_code::U128, 0);
        let nonnull = SchemaDescriptor::new(&[pk, SchemaColumn::new(type_code::I64, 0)], &[0]);
        let nullable = SchemaDescriptor::new(&[pk, SchemaColumn::new(type_code::I64, 1)], &[0]);

        // Non-nullable A + nullable B → nullable output column, Generic comparator.
        let m = union_nullability_merge(&nonnull, &nullable);
        assert_eq!(m.columns[1].nullable, 1, "OR of non-nullable and nullable = nullable");
        assert_eq!(m.payload_cmp, PayloadCmpKind::Generic);

        // Both non-nullable → stays on the FixedIntNonnull fast path (byte-identical).
        let m2 = union_nullability_merge(&nonnull, &nonnull);
        assert_eq!(m2.columns[1].nullable, 0);
        assert_eq!(m2.payload_cmp, PayloadCmpKind::FixedIntNonnull);

        // Nullable A + non-nullable B → Generic too (OR is symmetric).
        let m3 = union_nullability_merge(&nullable, &nonnull);
        assert_eq!(m3.columns[1].nullable, 1);
        assert_eq!(m3.payload_cmp, PayloadCmpKind::Generic);
    }

    /// End-to-end mechanism: two rows that are both NULL in an `I64` payload
    /// column but carry DIFFERENT non-zero bytes under the null bit (the
    /// `NullGarbage` construction) and share one content-hash PK.
    ///
    /// Root-cause contrast at the dispatched row comparator: the merged schema's
    /// null-aware `Generic` comparator reads `null == null` and coalesces them;
    /// the pre-fix inherited `FixedIntNonnull` comparator orders by the raw
    /// garbage bytes and splits them — the bug this fix removes. (The split can't
    /// be shown by running consolidation to completion: the write path zero-fills
    /// null cells, so the two rows become byte-equal only *after* the fast
    /// comparator has already emitted them as two elements, tripping the
    /// consolidated-layout debug assert rather than yielding a clean 2-row batch.)
    ///
    /// Then end-to-end under the merged (null-aware) schema: the two NULL-garbage
    /// rows, fed one per `union` input, coalesce through the union + a distinct
    /// weight-clamp to a single weight-1 row.
    #[test]
    fn test_union_nullability_merge_coalesces_null_garbage() {
        use crate::ops::{op_distinct, op_union};
        use crate::storage::{compare_rows, compare_rows_fixedint_nonnull, Batch, Layout, ReadCursor};
        use std::cmp::Ordering;
        use std::rc::Rc;

        let pk = SchemaColumn::new(type_code::U128, 0);
        let schema_a = SchemaDescriptor::new(&[pk, SchemaColumn::new(type_code::I64, 0)], &[0]);
        let schema_b = SchemaDescriptor::new(&[pk, SchemaColumn::new(type_code::I64, 1)], &[0]);
        // Classification (merged → Generic, schema_a → FixedIntNonnull) is covered by
        // test_union_nullability_merge_classification; this test starts from that
        // given and exercises the row-comparator mechanism it selects.
        let merged = union_nullability_merge(&schema_a, &schema_b);

        // Append one NULL row carrying `garbage` bytes under the null bit, on a
        // fixed content-hash PK shared by every row here.
        let push_null_garbage = |bat: &mut Batch, garbage: i64| {
            bat.extend_pk(0x1234_5678_9abc_def0);
            bat.extend_weight(&1i64.to_le_bytes());
            bat.extend_null_bmp(&1u64.to_le_bytes()); // payload col 0 → NULL
            bat.extend_col(0, &garbage.to_le_bytes()); // non-zero bytes under the null bit
            bat.count += 1;
        };
        let g0 = 0x5555_5555_5555_5555u64 as i64;
        let g1 = 0xAAAA_AAAA_AAAA_AAAAu64 as i64;

        // Root-cause contrast: same PK, both NULL, different garbage bytes.
        let mut pair = Batch::with_capacity(merged, 2);
        push_null_garbage(&mut pair, g0);
        push_null_garbage(&mut pair, g1);
        assert_eq!(
            compare_rows(&merged, &pair, 0, &pair, 1),
            Ordering::Equal,
            "null-aware Generic comparator: two NULL rows are one element",
        );
        assert_ne!(
            compare_rows_fixedint_nonnull(&schema_a, &pair, 0, &pair, 1),
            Ordering::Equal,
            "null-blind FixedIntNonnull comparator: garbage bytes split them (the bug)",
        );

        // End-to-end under the merged schema: one NULL-garbage row per union input.
        let single = |garbage: i64| {
            let mut bat = Batch::with_capacity(merged, 1);
            push_null_garbage(&mut bat, garbage);
            bat.certify_layout(Layout::Consolidated, &merged);
            bat
        };
        let unioned = op_union(single(g0), &single(g1), &merged);
        assert_eq!(unioned.count, 2, "Z-Set + keeps both rows before consolidation");
        let empty = Rc::new(Batch::empty_with_schema(&merged));
        let mut ch = ReadCursor::over_batches(std::slice::from_ref(&empty), merged);
        let (out, _) = op_distinct(unioned, &mut ch, &merged);
        assert_eq!(out.count, 1, "null-aware comparator coalesces the two NULL rows");
        assert_eq!(out.get_weight(0), 1, "distinct clamps the coalesced weight 2 → 1");
    }

    // -----------------------------------------------------------------------
    // Bounded-view hydration: the graph walk
    // -----------------------------------------------------------------------

    use gnitz_wire::{JoinKind, MapKind, OpNode};

    /// The inner-equi-join shape `emit_equi_join_terms` produces, as node ids:
    /// two `ScanDelta`s, a reindex `Map` and an `IntegrateTrace` per side, the
    /// two cross-wired `Join(DeltaTrace)` terms behind their normalization maps,
    /// a `Union`, a residual `Filter`, a projection `Map`, and the sink.
    ///
    /// ```text
    ///   0 scanA → 2 reindexA ─┬─→ 4 traceA ──────────┐
    ///                         └─────────────┐        │
    ///   1 scanB → 3 reindexB ─┬─→ 5 traceB ─┼→ 6 J_ab│ (delta=2, trace=5)
    ///                         └─────────────┴────────┴→ 7 J_ba (delta=3, trace=4)
    ///   6 → 8 map → 10 union ← 9 map ← 7;  10 → 11 filter → 12 map → 13 sink
    /// ```
    fn equi_join_circuit() -> LoadedCircuit {
        let (nodes, edges) = equi_join_parts();
        loaded_for_test(nodes, edges)
    }

    /// The same circuit's raw parts, for the tests that break one wire before
    /// building it.
    #[allow(clippy::type_complexity)]
    fn equi_join_parts() -> (HashMap<i32, OpNode>, Vec<(i32, i32, i32)>) {
        let m = |cols: Vec<u16>| OpNode::Map(MapKind::Projection(cols));
        let nodes = HashMap::from([
            (0, scan_delta(100)),
            (1, scan_delta(200)),
            (2, m(vec![0])),
            (3, m(vec![0])),
            (4, OpNode::IntegrateTrace),
            (5, OpNode::IntegrateTrace),
            (6, OpNode::Join(JoinKind::DeltaTrace)),
            (7, OpNode::Join(JoinKind::DeltaTrace)),
            (8, m(vec![0])),
            (9, m(vec![0])),
            (10, OpNode::Union),
            (11, OpNode::Filter(None)),
            (12, m(vec![0])),
            (13, OpNode::IntegrateSink),
        ]);
        let edges = vec![
            (0, 2, PORT_IN),
            (1, 3, PORT_IN),
            (2, 4, PORT_IN),
            (3, 5, PORT_IN),
            (2, 6, PORT_IN_A),
            (5, 6, PORT_TRACE),
            (3, 7, PORT_IN_A),
            (4, 7, PORT_TRACE),
            (6, 8, PORT_IN),
            (7, 9, PORT_IN),
            (8, 10, PORT_IN_A),
            (9, 10, PORT_IN_B),
            (10, 11, PORT_IN),
            (11, 12, PORT_IN),
            (12, 13, PORT_IN),
        ];
        (nodes, edges)
    }

    /// The seed is resolved by the *cross-wiring*, not by position: `J_a`'s trace
    /// port is the other branch's integral, so the trace whose input is `J_a`'s
    /// own delta port lives on the sibling join. Getting that backwards would
    /// seed the replay from the wrong side and silently compute a different
    /// product.
    #[test]
    fn hydration_seeds_from_the_cross_wired_trace() {
        let lc = equi_join_circuit();
        // `d_a` is the reindex feeding `J_ab`'s delta port (node 2); `t_a` is the
        // trace that integrates *it* (node 4), which hangs off `J_ba`.
        assert_eq!(hydration_nodes(&lc).unwrap(), HydrationNodes::Join { d_a: 2, t_a: 4 },);
    }

    /// The linear shape resolves to its source relation, through any number of
    /// filter/map nodes.
    #[test]
    fn hydration_of_a_linear_circuit_names_its_source() {
        let lc = loaded_for_test(
            HashMap::from([
                (0, scan_delta(77)),
                (1, OpNode::Filter(None)),
                (2, OpNode::Map(MapKind::Projection(vec![0]))),
                (3, OpNode::IntegrateSink),
            ]),
            vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN)],
        );
        assert_eq!(
            hydration_nodes(&lc).unwrap(),
            HydrationNodes::Relation { nid: 0, source: 77 }
        );
    }

    /// Every structural mismatch is a `Rejected`, never a silent `None`: a
    /// bounded view must not reach its store with no way to hydrate it. These are
    /// the trust boundary behind the planner's own eligibility gate — no SQL
    /// reaches them, which is exactly why they are asserted here.
    #[test]
    fn a_malformed_circuit_is_rejected_rather_than_guessed_at() {
        let rejected = |lc: LoadedCircuit, what: &str| match hydration_nodes(&lc) {
            Err(CompileError::Rejected(_)) => {}
            other => panic!("{what}: expected Rejected, got {:?}", other.map(|h| format!("{h:?}"))),
        };

        // No sink at all.
        rejected(
            loaded_for_test(HashMap::from([(0, scan_delta(1))]), vec![]),
            "sinkless circuit",
        );
        // A shape the walk cannot replay (a Reduce under the sink).
        rejected(
            loaded_for_test(
                HashMap::from([
                    (0, scan_delta(1)),
                    (
                        1,
                        OpNode::Reduce {
                            group_cols: vec![0],
                            agg: vec![(gnitz_wire::AggFunc::Count, 0)],
                            global_ground: false,
                            out_key: crate::schema::ReduceOutKey::SyntheticFold,
                        },
                    ),
                    (2, OpNode::IntegrateSink),
                ]),
                vec![(0, 1, PORT_IN), (1, 2, PORT_IN)],
            ),
            "reduce under the sink",
        );
        // A union whose inputs are not delta/trace joins.
        rejected(
            loaded_for_test(
                HashMap::from([
                    (0, scan_delta(1)),
                    (1, scan_delta(2)),
                    (2, OpNode::Union),
                    (3, OpNode::IntegrateSink),
                ]),
                vec![(0, 2, PORT_IN_A), (1, 2, PORT_IN_B), (2, 3, PORT_IN)],
            ),
            "union of two scans",
        );

        // The cross-wiring broken: both joins trace against the SAME integral, so
        // no trace integrates `J_a`'s own delta port.
        let (nodes, mut edges) = equi_join_parts();
        edges.retain(|&(s, d, p)| !(s == 4 && d == 7 && p == PORT_TRACE));
        edges.push((5, 7, PORT_TRACE));
        rejected(
            loaded_for_test(nodes, edges),
            "trace port is not the other branch's delta integral",
        );

        // A join whose trace port is not an integral at all.
        let (mut nodes, edges) = equi_join_parts();
        nodes.insert(4, OpNode::Filter(None));
        rejected(loaded_for_test(nodes, edges), "trace port is not an integral");
    }
}
