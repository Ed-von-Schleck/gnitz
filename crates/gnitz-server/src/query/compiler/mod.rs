//! Circuit compiler: reads system tables, builds a DBSP circuit graph,
//! derives its routing metadata, and emits VM instructions.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use std::collections::{HashMap, HashSet, VecDeque};

use rustc_hash::FxHashMap;
use std::fmt;

use crate::query::vm::{ProgramBuilder, RegisterMeta, VmHandle};
use gnitz_expr::{ExprValidateErr, LogicalProgram};
use gnitz_store::expr::MapPlan;
use gnitz_store::foundation::worker_ctx::{num_workers, worker_rank};
use gnitz_store::ops::AggDescriptor;
use gnitz_store::schema::{project_schema, SchemaDescriptor};
use gnitz_store::storage::{ReadCursor, RecoverySource, StorageError, Table};

mod emit;
mod hydration;
mod load;
mod routing;

use emit::*;
use hydration::derive_hydration;
pub(super) use hydration::{Hydration, HydrationSeed};

// Everything here is `pub(super)`: `dag` is the only module that names the
// compiler, so a `pub(crate)` would publish it to the catalog and runtime rungs
// too. `RelayRoute` is the exception — the master relay in `gnitz-server`
// matches on it, so it is re-exported through `query`.
pub(super) use load::for_each_scan_edge;
use load::scan_tid_through_filters;
pub(crate) use routing::RelayRoute;
pub(super) use routing::ViewMeta;

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
pub(in crate::query) enum CompileError {
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
/// circuit is derived once into [`ViewMeta`], so nothing else can read a
/// second answer out of the graph.
pub(super) struct LoadedCircuit {
    nodes: HashMap<i32, gnitz_wire::OpNode>,
    ordered: Vec<i32>,
    outgoing: HashMap<i32, Vec<(i32, i32)>>,
    inputs: HashMap<i32, NodeInputs>,
    /// `Distinct` nodes the elision pass dropped. Derived by the constructor, so
    /// it cannot disagree with the circuit and a plan built over a slice of the
    /// circuit reads the same set as every other.
    skip_nodes: HashSet<i32>,
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
    fn ops(&self) -> impl DoubleEndedIterator<Item = (i32, &gnitz_wire::OpNode)> {
        self.ordered.iter().map(|&nid| (nid, self.op(nid)))
    }

    /// `nid`'s inputs. Total over `nodes`: `topo_sorted` builds one per node.
    fn inputs(&self, nid: i32) -> &NodeInputs {
        self.inputs.get(&nid).expect("topo_sorted builds one entry per node")
    }

    /// `ordered`, restricted to `keep`. Every node list a plan is built over is
    /// produced this way, so a plan always sees the circuit's own topological
    /// order rather than whatever order the caller's set iterates in.
    fn ordered_where(&self, keep: impl Fn(i32) -> bool) -> Vec<i32> {
        self.ordered.iter().copied().filter(|&n| keep(n)).collect()
    }

    /// Every node reachable backwards from `start` (inclusive) — the
    /// sub-pipeline that produces its value.
    fn ancestors_inclusive(&self, start: i32) -> HashSet<i32> {
        let mut set = HashSet::new();
        let mut queue = VecDeque::from([start]);
        while let Some(cur) = queue.pop_front() {
            if set.insert(cur) {
                queue.extend(self.inputs(cur).iter());
            }
        }
        set
    }

    /// The sub-pipeline producing `out`'s value — the carve `compile_view`
    /// plans each exchange side over.
    fn subgraph_ordered(&self, out: i32) -> Vec<i32> {
        let set = self.ancestors_inclusive(out);
        self.ordered_where(|n| set.contains(&n))
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

/// Build a `LoadedCircuit` from raw nodes/edges. Test-only: the struct's fields
/// are module-private, so a test outside this module cannot construct one.
#[cfg(test)]
pub(super) fn loaded_for_test(nodes: HashMap<i32, gnitz_wire::OpNode>, edges: Vec<(i32, i32, i32)>) -> LoadedCircuit {
    load::topo_sorted(nodes, edges).expect("test circuit must be a well-formed DAG")
}

/// An unbounded delta scan — every fixture circuit's source shape.
#[cfg(test)]
pub(super) fn scan_delta(source: u64) -> gnitz_wire::OpNode {
    gnitz_wire::OpNode::ScanDelta { source, bound: None }
}

/// A `ScatterKey` reindex on `cols` — the routing walks' only variable. The
/// fixtures that vary `keep`, the promotion targets or the role spell the
/// variant out instead, so the field they turn on stays visible.
#[cfg(test)]
pub(super) fn scatter_reindex(cols: &[u32]) -> gnitz_wire::OpNode {
    gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex {
        keep: vec![0],
        reindex_cols: cols.to_vec(),
        reindex_target_tcs: vec![],
        role: gnitz_wire::ReindexRole::ScatterKey,
    })
}

/// An empty but decodable expr-program blob for tests that need a
/// `Map(Expression { program, .. })` or `Filter(Some(..))` to exist without
/// ever executing it. Built through the real encoder rather than spelled as a
/// byte literal, so it stays decodable when the blob header changes.
#[cfg(test)]
pub(super) fn dummy_expr_blob() -> Vec<u8> {
    gnitz_expr::ExprBuilder::new()
        .build(None)
        .expect("a well-formed program")
        .to_blob_bytes()
}

/// What a compile may read out of the host: the registered relations' schemas,
/// and the circuit system tables the circuit itself is stored in. A lookup
/// rather than an owned map, which would copy every relation's schema per
/// compile to answer under ten calls.
pub(super) trait SchemaSource {
    fn schema_of(&self, tid: i64) -> Option<SchemaDescriptor>;

    /// A non-compacting cursor over system table `tid`, carrying its own schema.
    /// `None` — the host does not hold that table — aborts the load rather than
    /// yielding a silently empty circuit.
    fn open_sys_cursor(&self, tid: i64) -> Option<ReadCursor>;
}

/// A standalone relation set — what the compiler's own tests build. It holds no
/// circuit tables, so every load against one is refused.
pub(super) type ExtTables = HashMap<i64, SchemaDescriptor>;

impl SchemaSource for ExtTables {
    fn schema_of(&self, tid: i64) -> Option<SchemaDescriptor> {
        self.get(&tid).copied()
    }

    fn open_sys_cursor(&self, _tid: i64) -> Option<ReadCursor> {
        None
    }
}

/// The registry answers both lookups in place: the system families are entered
/// in it as `Borrowed` handles, so a circuit read is the same entry lookup a
/// user relation's schema is. The impl sits beside the trait because `relation`
/// is below `query` and could only name the trait upward.
impl SchemaSource for gnitz_store::relation::RelationRegistry {
    fn schema_of(&self, tid: i64) -> Option<SchemaDescriptor> {
        self.get_schema_desc(tid)
    }

    fn open_sys_cursor(&self, tid: i64) -> Option<ReadCursor> {
        self.open_store_cursor(tid)
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
pub(super) struct SubPlan {
    pub(in crate::query) vm: Box<VmHandle>,
    pub(in crate::query) in_reg: u16,
    /// Maps a source table id to the input register that receives its delta.
    /// Empty for the post-combine phase (which has no source-level routing).
    pub(in crate::query) source_reg_map: FxHashMap<i64, u16>,
}

/// One exchanged side of a [`Sides`] plan: a sub-pipeline whose output is
/// repartitioned (relayed through the exchange) into a post-phase seed register.
pub(super) struct Side {
    pub(in crate::query) plan: SubPlan,
    /// Register in the post VM seeded with this side's relayed batch.
    pub(in crate::query) seed_reg: u16,
}

impl Side {
    /// This side's pre-exchange output schema — what the wire encode labels its
    /// relayed batches (and empty placeholders) with. Never the view's final
    /// combine-widened schema, which is a different width for a JOIN and would
    /// mislabel the operand batch.
    pub(super) fn exchange_schema(&self) -> SchemaDescriptor {
        self.plan.vm.program.out_schema()
    }
}

/// What a compiled view repartitions through: one sub-pipeline per
/// `ExchangeShard` in its circuit, each computing up to that shard, relayed, and
/// seeding the post-combine phase. More than two is rejected at compile.
pub(super) enum Sides {
    /// No `ExchangeShard`: the whole plan is its post phase.
    Unexchanged,
    /// GROUP BY / SELECT DISTINCT / PK redistribution / range join, taking every
    /// delta.
    Unary(Side),
    /// A binary set-op, each side paired with the one source it scans: a side
    /// takes the delta iff that source is the delta's, so `a UNION a` runs both.
    Pair([(i64, Side); 2]),
}

impl Sides {
    /// Every side's sub-plan, in side order.
    fn plans_mut(&mut self) -> impl Iterator<Item = &mut SubPlan> {
        let (unary, pair) = match self {
            Sides::Unexchanged => (None, None),
            Sides::Unary(side) => (Some(&mut side.plan), None),
            Sides::Pair(pair) => (None, Some(pair)),
        };
        unary
            .into_iter()
            .chain(pair.into_iter().flatten().map(|(_, side)| &mut side.plan))
    }
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
pub(super) struct CompileOutput {
    /// What the circuit repartitions through, ahead of `post`.
    pub(in crate::query) sides: Sides,
    /// The combine phase every side's relayed batch seeds — and, for a circuit
    /// with no `ExchangeShard`, the whole plan.
    pub(in crate::query) post: SubPlan,
    /// The `(source table id, secondary-index range)` the planner pushed onto the
    /// primary source's `ScanDelta`, consulted only by the two circuit backfill
    /// drivers (a steady-state delta never opens the source cursor). A **physical
    /// access hint**: `None` means "full-scan", which is always correct, and the
    /// circuit's `Filter` carries the full predicate either way.
    pub(in crate::query) source_bound: Option<(i64, gnitz_wire::ScanBound)>,
    /// How a capacity-bounded view recomputes one key's output rows. `None` for
    /// every unbounded view — the walk runs only under a capacity, and any
    /// structural mismatch there is a `Rejected` rather than a silent `None`: a
    /// bounded view must never reach its store with no way to hydrate it.
    pub(in crate::query) hydration: Option<Hydration>,
}

impl CompileOutput {
    /// Every sub-plan of the view, for whole-plan sweeps (regfile clears,
    /// checkpoint table collection).
    pub(super) fn sub_plans_mut(&mut self) -> impl Iterator<Item = &mut SubPlan> {
        self.sides.plans_mut().chain(std::iter::once(&mut self.post))
    }
}

// ---------------------------------------------------------------------------
// Build a single plan (pre or post exchange)
// ---------------------------------------------------------------------------

pub(super) struct PlanBuildResult {
    vm: Box<VmHandle>,
    in_reg: u16,
    source_reg_map: FxHashMap<i64, u16>,
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
            source_reg_map: self.source_reg_map,
            vm: self.vm,
        }
    }

    /// The one source table this plan scans, or `None` where it scans none or
    /// several. A set-op side's exchange key, so each side's IPC rounds key
    /// distinctly.
    fn single_source(&self) -> Option<i64> {
        match self.source_reg_map.len() {
            1 => self.source_reg_map.keys().next().copied(),
            _ => None,
        }
    }
}

/// Validate an exchange-input sub-plan and return its output schema. An
/// `ExchangeShard` emits no instruction, so nothing else checks its
/// `shard_cols`: the shard key is read at runtime as `schema.columns[c]`, so a
/// crafted/corrupt node is rejected here rather than aborting at the first push.
/// On `Err`, dropping the plan removes its scratch dirs.
fn finalize_side(plan: &PlanBuildResult, shard_cols: &[u32]) -> Result<SchemaDescriptor, CompileError> {
    let schema = plan.vm.program.out_schema();
    if oob_cols(shard_cols.iter().copied(), &schema) {
        return Err(CompileError::Rejected("exchange shard columns out of range"));
    }
    Ok(schema)
}

/// One exchange boundary's carve: the shard node with its key, the node feeding
/// it, and the ancestor set that becomes that side's own plan.
struct Carve<'a> {
    ex_nid: i32,
    shard_cols: &'a [u32],
    ex_in: i32,
    ancestors: HashSet<i32>,
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
    /// The policy the view's *output store* was opened under, handed down so its
    /// operator traces cannot end up looking for a different manifest generation.
    pub(in crate::query) recovery: RecoverySource,
}

/// Assemble a compiled view's exchange sides, seeding each from `seed_regs` at
/// its own index. Every refusal precedes the first `into_sub_plan()` — that is
/// what still leaves the `Err` its `ScratchGuard`s to run.
fn build_sides(mut plans: Vec<PlanBuildResult>, seed_regs: &[u16]) -> Result<Sides, CompileError> {
    let side = |plan: PlanBuildResult, i: usize| Side {
        seed_reg: seed_regs[i],
        plan: plan.into_sub_plan(),
    };
    match plans.len() {
        0 => Ok(Sides::Unexchanged),
        1 => Ok(Sides::Unary(side(plans.pop().expect("one side"), 0))),
        // A side scanning no single source matches no delta at all. Only the raw
        // `CircuitBuilder` wire path can build one; three or more sides are
        // already refused above.
        _ => {
            let Some(keys) = plans
                .iter()
                .map(PlanBuildResult::single_source)
                .collect::<Option<Vec<_>>>()
            else {
                return Err(CompileError::Rejected(
                    "a two-sided plan has a side scanning no single source, so no delta can reach it",
                ));
            };
            let mut keyed = plans
                .into_iter()
                .zip(keys)
                .enumerate()
                .map(|(i, (plan, key))| (key, side(plan, i)));
            Ok(Sides::Pair([
                keyed.next().expect("two sides"),
                keyed.next().expect("two sides"),
            ]))
        }
    }
}

/// Compile a circuit for a single view: read the circuit from the system
/// tables, derive its routing metadata, then `build_plan`.
pub(super) fn compile_view(
    site: ViewSite<'_>,
    view_schema: &SchemaDescriptor,
    host: &dyn SchemaSource,
    bounded: bool,
) -> Result<CompiledView, CompileError> {
    let loaded = load::load_circuit(host, site.id)?;
    if loaded.nodes.is_empty() {
        return Err(CompileError::Rejected("circuit has no nodes"));
    }
    // Derived from the circuit, not collected during emission: `EmitCtx` flows
    // into `SubPlan`, never into `CompileOutput`, and an `Exchanged` shape runs
    // `build_plan` once per side plus post — so an emit-sourced fact would need a
    // cross-sub-plan merge.
    let meta = ViewMeta::derive(&loaded, host)?;

    let exchanges: Vec<(i32, &[u32])> = loaded
        .ops()
        .filter_map(|(nid, op)| match op {
            gnitz_wire::OpNode::ExchangeShard { shard_cols } => Some((nid, shard_cols.as_slice())),
            _ => None,
        })
        .collect();
    if exchanges.len() > 2 {
        // No planner path emits this: set-ops are binary, GROUP BY/DISTINCT unary.
        gnitz_warn!(
            "compile_view: view_id={} has {} exchange nodes; unsupported",
            site.id,
            exchanges.len()
        );
        return Err(CompileError::Rejected("more than two exchange nodes"));
    }
    // An exchanged plan splits the circuit across a repartition, so neither
    // hydratable shape can produce one — and a per-key replay of one would need
    // the exchange to run too. Checked before the carve, so the post phase below
    // is the whole plan whenever `bounded` holds.
    if bounded && !exchanges.is_empty() {
        return Err(CompileError::Rejected(
            "bounded view: only a linear body and an inner equi-join are supported",
        ));
    }

    // Each side is the ancestors of its own exchange input; everything else is
    // the post phase, which for an exchange-free circuit is the whole plan. On
    // any `?` below the finished `PlanBuildResult`s drop, and their ScratchGuards
    // take every scratch directory with them.
    let carves: Vec<Carve> = exchanges
        .iter()
        .map(|&(ex_nid, shard_cols)| {
            let ex_in = loaded.inputs(ex_nid).unary();
            Carve {
                ex_nid,
                shard_cols,
                ex_in,
                ancestors: loaded.ancestors_inclusive(ex_in),
            }
        })
        .collect();

    let mut side_plans: Vec<PlanBuildResult> = Vec::with_capacity(carves.len());
    let mut exchange_inputs: Vec<(i32, SchemaDescriptor)> = Vec::with_capacity(carves.len());
    for carve in &carves {
        let plan = build_plan(
            &loaded,
            &loaded.subgraph_ordered(carve.ex_in),
            host,
            site,
            view_schema.placement(),
            PlanTarget::Subgraph { out: carve.ex_in },
        )?;
        exchange_inputs.push((carve.ex_nid, finalize_side(&plan, carve.shard_cols)?));
        side_plans.push(plan);
    }

    let post_ordered =
        loaded.ordered_where(|nid| !carves.iter().any(|c| c.ex_nid == nid || c.ancestors.contains(&nid)));
    let post = build_plan(
        &loaded,
        &post_ordered,
        host,
        site,
        view_schema.placement(),
        PlanTarget::ViewOutput {
            out_schema: view_schema,
            seeds: &exchange_inputs,
        },
    )?;

    // A delta is routed to the side scanning its source, so a post-phase scan
    // reaches nothing: its rows are dropped (two-sided) or seeded to the wrong
    // register (unary). Only the raw `CircuitBuilder` wire path can build one.
    if !carves.is_empty() && !post.source_reg_map.is_empty() {
        return Err(CompileError::Rejected(
            "an exchanged plan scans a relation outside every exchange side",
        ));
    }

    // `bounded` forced `carves` empty above, so `post` is the whole plan here.
    let hydration = bounded.then(|| derive_hydration(&loaded, &post)).transpose()?;
    let sides = build_sides(side_plans, &post.exchange_input_regs)?;

    Ok(CompiledView {
        output: CompileOutput {
            sides,
            post: post.into_sub_plan(),
            source_bound: load::circuit_source_bound(&loaded),
            hydration,
        },
        meta,
    })
}
