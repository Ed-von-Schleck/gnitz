//! Circuit compiler: reads system tables, builds a DBSP circuit graph,
//! runs annotation + optimization passes, and emits VM instructions.

use std::collections::{HashMap, HashSet, VecDeque};

use rustc_hash::FxHashSet;
use std::fmt;

use crate::expr::MapPlan;
use crate::foundation::worker_ctx::{num_workers, worker_rank};
use crate::ops::AggDescriptor;
use crate::query::vm::{Instr, ProgramBuilder, RegisterMeta, VmHandle};
use crate::schema::{project_schema, SchemaDescriptor};
use crate::storage::{ReadCursor, RecoverySource, StorageError, Table};
use gnitz_expr::{ExprValidateErr, LogicalProgram};

mod emit;
mod hydration;
mod load;
mod optimize;

use emit::*;
use hydration::derive_hydration;
pub(crate) use hydration::Hydration;
use optimize::*;

pub(crate) use load::{for_each_scan_edge, load_circuit};
pub(crate) use optimize::CircuitFacts;
// Compiler-internal: `pub(crate)` on these would publish them to the whole
// `query` layer, which reaches the compiler only through the exports above.
use load::scan_tid_through_filters;

// Port numbers reach the compiler only in hand-written fixture edge lists; every
// production read of an operand goes through [`NodeInputs`].
#[cfg(test)]
pub(super) const PORT_IN: i32 = gnitz_wire::PORT_IN as i32;
#[cfg(test)]
pub(super) const PORT_IN_A: i32 = gnitz_wire::PORT_IN_A as i32;
#[cfg(test)]
pub(super) const PORT_IN_B: i32 = gnitz_wire::PORT_IN_B as i32;
#[cfg(test)]
pub(super) const PORT_TRACE: i32 = gnitz_wire::PORT_TRACE as i32;

/// Why `compile_view` failed to turn a stored view circuit into a runnable plan.
/// Rendered by `Display` into the `CREATE VIEW` error the client receives, so
/// every variant's text is user-facing.
#[derive(Debug)]
pub(crate) enum CompileError {
    /// A compile-time guard rejected the circuit; the payload names the guard,
    /// so the rejection says *which* trust-boundary check fired instead of a
    /// bare "build failed".
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
///
/// Opaque outside this module: everything the rest of the engine wants from a
/// circuit is derived once into [`CircuitFacts`], so nothing else can read a
/// second answer out of the graph.
pub(crate) struct LoadedCircuit {
    nodes: HashMap<i32, gnitz_wire::OpNode>,
    ordered: Vec<i32>,
    outgoing: HashMap<i32, Vec<(i32, i32)>>,
    inputs: HashMap<i32, NodeInputs>,
}

impl LoadedCircuit {
    /// `nid`'s inputs. Total over `nodes`: `topo_sorted` builds one per node.
    fn inputs(&self, nid: i32) -> &NodeInputs {
        self.inputs.get(&nid).expect("topo_sorted builds one entry per node")
    }
}

/// A node's inputs in the shape its operator's port set allows. `topo_sorted`
/// settles the arity against `OpNode::ports()` before building one, so a reader
/// destructures instead of re-checking — and a `Filter` wired only on
/// `PORT_TRACE` is unrepresentable rather than merely rejected downstream.
pub(super) enum NodeInputs {
    /// A `ScanDelta`: fed by the source drive, not by an edge.
    Source,
    Unary(i32),
    /// `a` is `PORT_IN_A` — a join's delta side, a union's left operand; `b` is
    /// `PORT_TRACE` / `PORT_IN_B`.
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
            _ => unreachable!("a unary operator's port set is [PORT_IN]"),
        }
    }

    /// The producers of a binary operator's two operands, in port order.
    fn binary(&self) -> (i32, i32) {
        match self {
            NodeInputs::Binary { a, b } => (*a, *b),
            _ => unreachable!("a binary operator's port set is [PORT_IN_A, PORT_TRACE]"),
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

/// Build a `LoadedCircuit` from raw nodes/edges. Test-only: the struct's fields
/// are module-private, so the `dag` tests (which exercise `ViewMeta::from_loaded`)
/// cannot construct one directly.
#[cfg(test)]
pub(crate) fn loaded_for_test(nodes: HashMap<i32, gnitz_wire::OpNode>, edges: Vec<(i32, i32, i32)>) -> LoadedCircuit {
    load::topo_sorted(nodes, edges).expect("test circuit must be a well-formed DAG")
}

/// An unbounded delta scan — every fixture circuit's source shape.
#[cfg(test)]
pub(super) fn scan_delta(source: u64) -> gnitz_wire::OpNode {
    gnitz_wire::OpNode::ScanDelta { source, bound: None }
}

/// An empty but decodable expr-program blob for tests that need a
/// `Map(Expression { program, .. })` or `Filter(Some(..))` to exist without
/// ever executing it. Built through the real encoder rather than spelled as a
/// byte literal, so it stays decodable when the blob header changes.
#[cfg(test)]
pub(super) fn dummy_expr_blob() -> Vec<u8> {
    gnitz_expr::ExprBuilder::new().build(0).encode()
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

/// The registered relations visible to a compile, as a lookup rather than a map:
/// a compile makes under ten of these calls, where an owned `HashMap` built per
/// compile would copy every registered relation's schema to answer them.
pub(crate) trait SchemaSource {
    fn schema_of(&self, tid: i64) -> Option<SchemaDescriptor>;
}

/// A standalone relation set — what the compiler's own tests build.
pub(crate) type ExtTables = HashMap<i64, SchemaDescriptor>;

impl SchemaSource for ExtTables {
    fn schema_of(&self, tid: i64) -> Option<SchemaDescriptor> {
        self.get(&tid).copied()
    }
}

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
    pub can_emit_on_empty: bool,
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
///   `sides.len() ∈ {1, 2}` — more is rejected at compile.
pub(crate) enum PlanShape {
    Single(SubPlan),
    Exchanged { sides: Vec<Side>, post: SubPlan },
}

/// What one `build_plan` call is producing — and with it whether the sink-schema
/// contract applies, and to what. A view's own output must match the schema the
/// view was declared with; a subgraph's is an intermediate whose schema is
/// whatever its last node derived, so holding that to the view schema would
/// reject every exchange side. The declared schema rides on the variant that
/// checks it, so a build that cannot use it never sees it.
pub(super) enum PlanTarget<'a> {
    /// The whole view's output. `seeds` names one relayed-batch seed register per
    /// exchange input — empty for a `Single` plan, one or two entries for the post
    /// phase of an `Exchanged` one.
    ViewOutput {
        out_schema: &'a SchemaDescriptor,
        seeds: &'a [(i32, SchemaDescriptor)],
    },
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
    // The seed register of each exchange input this plan was built with, in the
    // order the input list named them — so `compile_view` reads a side's seed at
    // the side's own index rather than searching for it.
    exchange_input_regs: Vec<u16>,
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
            can_emit_on_empty: self.can_emit_on_empty,
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
}

/// Validate an exchange-input sub-plan and return its output schema. When the
/// exchange node is an `ExchangeShard` (which emits no instruction, so nothing
/// else checks it) its `shard_cols` must be in range for that schema: the shard
/// key is read at runtime as `schema.columns[c]`, so a crafted/corrupt node is
/// rejected here rather than aborting at the first push. On `Err`, dropping the
/// plan removes its scratch dirs.
fn finalize_side(
    plan: &PlanBuildResult,
    loaded: &LoadedCircuit,
    ex_nid: i32,
) -> Result<SchemaDescriptor, CompileError> {
    let schema = plan.vm.program.reg_meta[plan.out_reg as usize].schema;
    if let Some(gnitz_wire::OpNode::ExchangeShard { shard_cols }) = loaded.nodes.get(&ex_nid) {
        if oob_cols(shard_cols.iter().copied(), &schema) {
            return Err(CompileError::Rejected("exchange shard columns out of range"));
        }
    }
    Ok(schema)
}

/// One exchange boundary's carve: the shard node, the node feeding it, and the
/// ancestor set that becomes that side's own plan.
struct Carve {
    ex_nid: i32,
    ex_in: i32,
    ancestors: HashSet<i32>,
}

/// One compile's two products: the plan the dag caches, and the plan-free facts
/// its `ViewMeta` is folded from.
///
/// The facts travel and the circuit does not, so nothing outside the compiler
/// ever holds a `LoadedCircuit` it could read a second, differently-derived
/// answer out of.
pub(crate) struct CompiledView {
    pub output: CompileOutput,
    pub facts: CircuitFacts,
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
    ext_tables: &dyn SchemaSource,
    bounded: bool,
) -> Result<CompiledView, CompileError> {
    let loaded = load_circuit(sys, site.id)?;
    if loaded.nodes.is_empty() {
        return Err(CompileError::Rejected("circuit has no nodes"));
    }
    // Derived from the circuit, not collected during emission: `EmitCtx` flows
    // into `SubPlan`, never into `CompileOutput`, and an `Exchanged` shape runs
    // `build_plan` once per side plus post — so an emit-sourced fact would need a
    // cross-sub-plan merge.
    let facts = CircuitFacts::derive(&loaded, ext_tables)?;
    let source_bound = facts.source_bound;

    let annotated = move |shape: PlanShape, hydration: Option<Hydration>| CompiledView {
        output: CompileOutput {
            shape,
            source_bound,
            hydration,
        },
        facts,
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
                &loaded.ordered,
                ext_tables,
                site,
                view_schema.placement(),
                PlanTarget::ViewOutput {
                    out_schema: view_schema,
                    seeds: &[],
                },
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
                    let ex_in = loaded.inputs(ex_nid).unary();
                    Carve {
                        ex_nid,
                        ex_in,
                        ancestors: ancestors_inclusive(&loaded, ex_in),
                    }
                })
                .collect();
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
                    &side_ordered,
                    ext_tables,
                    site,
                    view_schema.placement(),
                    PlanTarget::Subgraph { out: carve.ex_in },
                )?;
                let schema = finalize_side(&plan, &loaded, carve.ex_nid)?;
                side_plans.push(plan);
                exchange_inputs.push((carve.ex_nid, schema));
            }

            let post = build_plan(
                &loaded,
                &post_ordered,
                ext_tables,
                site,
                view_schema.placement(),
                PlanTarget::ViewOutput {
                    out_schema: view_schema,
                    seeds: &exchange_inputs,
                },
            )?;

            let sides: Vec<Side> = side_plans
                .into_iter()
                .enumerate()
                .map(|(i, plan)| Side {
                    source_id: plan.single_source(),
                    seed_reg: post.exchange_input_regs[i],
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
            Err(CompileError::Rejected("more than two exchange nodes"))
        }
    }
}
