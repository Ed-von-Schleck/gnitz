//! Circuit compiler: reads system tables, builds a DBSP circuit graph,
//! runs annotation + optimization passes, and emits VM instructions.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::expr::{ExprValidateErr, LogicalProgram, ScalarFunc};
use crate::foundation::worker_ctx::{num_workers, worker_rank};
use crate::ops::{AggDescriptor, AggOp};
use crate::query::vm::{Instr, ProgramBuilder, RegisterMeta, VmHandle};
use crate::schema::{is_fixed_int, type_code, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::{ReadCursor, RecoverySource, Table};

mod emit;
mod load;
mod optimize;

use emit::*;
use optimize::*;

pub(crate) use emit::is_worker_scratch_dir_name;
pub(crate) use load::{
    circuit_range_join_n_eq, load_circuit, reindex_cols_through_filters, scan_tid_through_filters, topo_sort,
};
pub(crate) use optimize::compute_join_shard_map;

// Engine-only port aliases (all equal to wire constants).
const PORT_IN: i32 = gnitz_wire::PORT_IN as i32;
const PORT_IN_A: i32 = gnitz_wire::PORT_IN_A as i32;
const PORT_IN_B: i32 = gnitz_wire::PORT_IN_B as i32;
const PORT_TRACE: i32 = gnitz_wire::PORT_TRACE as i32;

/// Why `compile_view` failed to turn a stored view circuit into a runnable plan.
/// The sole caller logs the variant and maps every error to `None`.
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
    /// so a rejected view's log line says *which* of the ~30 trust-boundary
    /// checks fired instead of a bare "build failed".
    Rejected(&'static str),
}

impl CompileError {
    /// The reject reason for the compile-failure log line.
    pub(crate) fn describe(&self) -> &'static str {
        match self {
            CompileError::LoadFailed => "circuit load failed",
            CompileError::EmptyCircuit => "circuit has no nodes",
            CompileError::Cycle => "circuit graph has a cycle",
            CompileError::MissingExchangeInput => "exchange node lacks an input edge",
            CompileError::TooManyExchanges => "more than two exchange nodes",
            CompileError::Rejected(guard) => guard,
        }
    }
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Typed circuit graph with OpNode payloads. `edges` is the raw edge list
/// before topological sort; the sorted helpers are populated by `topo_sort`.
#[derive(Default)]
pub(crate) struct LoadedCircuit {
    out_schema: SchemaDescriptor,
    pub(crate) nodes: HashMap<i32, gnitz_wire::OpNode>,
    /// Raw (src, dst, port) tuples read from CircuitEdges — input to topo_sort.
    pub(crate) edges: Vec<(i32, i32, i32)>,
    // Populated by topo_sort:
    ordered: Vec<i32>,
    pub(crate) outgoing: HashMap<i32, Vec<(i32, i32)>>,
    incoming: HashMap<i32, Vec<(i32, i32)>>,
}

/// source table id → join/group reindex `(column, carried promotion tc)` pairs.
pub(crate) type JoinShardMap = HashMap<i64, Vec<(i32, u8)>>;

/// The registered relations visible to a compile: table id → schema.
pub(crate) type ExtTables = HashMap<i64, SchemaDescriptor>;

// ---------------------------------------------------------------------------
// CompileOutput — typed compilation result
// ---------------------------------------------------------------------------

/// A compiled sub-pipeline: the VM program, its register layout, its
/// source-to-input-register map, and any external trace registers it reads.
/// Used for: (a) the pre-exchange phase of every view, (b) each side of a
/// binary set-op, and (c) the post-combine phase (single- and two-exchange
/// views). All three are structurally identical; the difference is only which
/// part of the plan graph they cover.
pub(crate) struct SubPlan {
    pub vm: Box<VmHandle>,
    pub in_reg: u16,
    pub out_reg: u16,
    /// True iff the program can emit output from an empty input epoch: it
    /// carries a `ScanTrace` (reads a trace regardless of the delta) or a
    /// global-ground `Reduce` — the empty pad round is the ONLY place the
    /// SQL-required ground row over an empty/fully-retracted source is minted
    /// (`op_reduce`'s `n == 0` branch). Every other opcode is inert on an
    /// empty input, so an empty epoch skips the VM machinery entirely.
    pub can_emit_on_empty: bool,
    /// Maps a source table id to the input register that receives its delta.
    /// Empty for the post-combine phase (which has no source-level routing).
    pub source_reg_map: HashMap<i64, u16>,
    pub ext_trace_regs: Vec<(u16, i64)>,
    /// Reusable per-epoch external-cursor buffers (see `ExtCursorScratch`).
    pub ext_cursors: ExtCursorScratch,
}

/// Reusable per-epoch external-cursor buffers — capacity-only reuse. The
/// cursors themselves are dropped at each epoch's end: holding them across
/// ticks would pin memtable snapshots (`Rc<Batch>`) and shard mmaps of
/// external base tables for the plan's lifetime.
#[derive(Default)]
pub(crate) struct ExtCursorScratch {
    pub ptrs: Vec<*mut ReadCursor>,
    /// The `Box` is load-bearing: `ptrs` holds raw pointers into these boxes,
    /// stable across the `Vec`'s growth.
    #[allow(clippy::vec_box)]
    pub cursors: Vec<Box<ReadCursor>>,
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
    pub fn exchange_schema(&self) -> SchemaDescriptor {
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

/// Output from `compile_view`, consumed directly by DagEngine as the cached
/// plan. Besides the executable shape it carries the compile-time routing
/// annotations the multi-worker dispatch reads — derived from the same loaded
/// circuit the plan was emitted from, so they cannot drift from it.
pub(crate) struct CompileOutput {
    pub shape: PlanShape,
    /// Join sources whose native distribution already matches the join key
    /// (or whose partner is replicated) — their deltas skip the join scatter.
    pub co_partitioned: HashSet<i64>,
    /// source table id → join/group reindex `(column, carried promotion tc)`
    /// pairs, for every source in one pass. Non-empty entries mark the
    /// join-scatter sources. (The master relay keeps its own plan-free copy —
    /// `ViewMeta` — since it must never compile.)
    pub join_shard_map: JoinShardMap,
    /// `Some(n_eq)` iff the view is a non-equi (range / band) join — the
    /// dispatch arm that relays its *input* delta (eq-prefix scatter or
    /// broadcast) before the pre phase.
    pub range_join_n_eq: Option<u8>,
    /// The unary output `ExchangeShard` is a proven no-op (every row already on
    /// the worker owning its distribution key) — the output IPC is elided.
    pub skips_exchange: bool,
}

impl CompileOutput {
    /// Every sub-plan of the shape, for whole-plan sweeps (regfile clears,
    /// checkpoint table collection).
    pub fn sub_plans_mut(&mut self) -> impl Iterator<Item = &mut SubPlan> {
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
    in_reg: i32,
    out_reg: i32,
    ext_trace_regs: Vec<(u16, i64)>,
    source_reg_map: HashMap<i64, i32>,
    // (exchange-input node id → seed register) for each exchange input this plan
    // was built with. Lets `compile_view` wire each side's relayed batch to the
    // correct post-phase register.
    exchange_input_regs: Vec<(i32, i32)>,
    // Scratch dirs created for this plan. Dropping the result (a failed sibling
    // plan, an `Err` return from `compile_view`) removes them; `into_sub_plan`
    // defuses the guard — from then on the VM's owned tables keep them alive.
    scratch: emit::ScratchGuard,
}

impl PlanBuildResult {
    /// Convert into the runtime `SubPlan`, defusing the scratch guard and
    /// narrowing the register ids to the `u16` width the runtime dispatch uses
    /// (`build_plan` bounds `reg_meta` below `u16::MAX`, so the casts are exact).
    fn into_sub_plan(mut self) -> SubPlan {
        self.scratch.defuse();
        let can_emit_on_empty = self.vm.program.instructions.iter().any(|i| {
            matches!(
                i,
                Instr::ScanTrace { .. }
                    | Instr::Reduce {
                        global_ground: true,
                        ..
                    }
            )
        });
        SubPlan {
            in_reg: self.in_reg as u16,
            out_reg: self.out_reg as u16,
            can_emit_on_empty,
            source_reg_map: self
                .source_reg_map
                .iter()
                .map(|(&tid, &reg)| (tid, reg as u16))
                .collect(),
            ext_trace_regs: self.ext_trace_regs,
            ext_cursors: ExtCursorScratch::default(),
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
            .map(|&(_, r)| r as u16)
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
    if plan.out_reg < 0 || plan.out_reg as usize >= plan.vm.program.reg_meta.len() {
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

/// Compile a circuit for a single view.
///
/// # Safety
/// All table handles must be valid pointers or null.
pub(crate) unsafe fn compile_view(
    view_id: u64,
    sys_nodes: *mut Table,
    sys_edges: *mut Table,
    sys_node_cols: *mut Table,
    view_dir: &str,
    view_schema: &SchemaDescriptor,
    ext_tables: &ExtTables,
) -> Result<CompileOutput, CompileError> {
    let mut loaded =
        load_circuit(sys_nodes, sys_edges, sys_node_cols, view_id, *view_schema).ok_or(CompileError::LoadFailed)?;
    if loaded.nodes.is_empty() {
        return Err(CompileError::EmptyCircuit);
    }
    topo_sort(&mut loaded)?;

    let (join_shard_map, co_partitioned) = annotate(&loaded, ext_tables);
    let skip_nodes = compute_skip_nodes(&loaded);
    let range_join_n_eq = circuit_range_join_n_eq(&loaded);
    let skips_exchange = compute_skips_exchange(&loaded, ext_tables);

    let annotated = |shape: PlanShape| CompileOutput {
        shape,
        co_partitioned,
        join_shard_map,
        range_join_n_eq,
        skips_exchange,
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
            let ordered = loaded.ordered.clone();
            let plan = build_plan(&loaded, &skip_nodes, &ordered, ext_tables, view_dir, view_id, None, &[])?;
            Ok(annotated(PlanShape::Single(plan.into_sub_plan())))
        }
        // One or two exchange boundaries: carve each side out by the ancestors
        // of its exchange input (a binary set-op's two independent
        // HashRow→ExchangeShard sub-pipelines; the unary GROUP BY / DISTINCT /
        // redistribution pipeline is the one-side case — its ancestors set is
        // exactly the nodes before the exchange in topo order). Everything else
        // (the combine + sink, reading the relayed batches) is the post phase.
        n @ (1 | 2) => {
            let mut side_sets: Vec<HashSet<i32>> = Vec::with_capacity(n);
            for &ex_nid in &exchange_nids {
                let ex_in = exchange_input_node(&loaded, ex_nid).ok_or(CompileError::MissingExchangeInput)?;
                side_sets.push(ancestors_inclusive(&loaded, ex_in));
            }
            let post_ordered: Vec<i32> = loaded
                .ordered
                .iter()
                .copied()
                .filter(|nid| !exchange_nids.contains(nid) && !side_sets.iter().any(|s| s.contains(nid)))
                .collect();

            let mut side_plans: Vec<PlanBuildResult> = Vec::with_capacity(n);
            let mut exchange_inputs: Vec<(i32, SchemaDescriptor)> = Vec::with_capacity(n);
            for (&ex_nid, set) in exchange_nids.iter().zip(&side_sets) {
                let side_ordered: Vec<i32> = loaded.ordered.iter().copied().filter(|n| set.contains(n)).collect();
                let ex_in = exchange_input_node(&loaded, ex_nid).unwrap();
                let plan = build_plan(
                    &loaded,
                    &skip_nodes,
                    &side_ordered,
                    ext_tables,
                    view_dir,
                    view_id,
                    Some(ex_in),
                    &[],
                )?;
                let schema = finalize_side(&plan, &loaded, ex_nid)?;
                side_plans.push(plan);
                exchange_inputs.push((ex_nid, schema));
            }

            let post = build_plan(
                &loaded,
                &skip_nodes,
                &post_ordered,
                ext_tables,
                view_dir,
                view_id,
                None,
                &exchange_inputs,
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

            Ok(annotated(PlanShape::Exchanged {
                sides,
                post: post.into_sub_plan(),
            }))
        }
        _ => {
            // More than two exchange boundaries is not produced by any current
            // planner path (set-ops are binary, GROUP BY/DISTINCT are unary).
            gnitz_warn!(
                "compile_view: view_id={} has {} exchange nodes; unsupported",
                view_id,
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

    #[test]
    fn test_topo_sort_simple() {
        let mut loaded = LoadedCircuit {
            out_schema: SchemaDescriptor::default(),
            nodes: {
                let mut m = HashMap::new();
                m.insert(0, gnitz_wire::OpNode::ScanDelta(0));
                m.insert(1, gnitz_wire::OpNode::Filter(None));
                m.insert(2, gnitz_wire::OpNode::IntegrateSink);
                m
            },
            edges: vec![(0, 1, 0), (1, 2, 0)],
            ..Default::default()
        };
        assert!(topo_sort(&mut loaded).is_ok());
        assert_eq!(loaded.ordered, vec![0, 1, 2]);
    }

    #[test]
    fn test_topo_sort_cycle() {
        let mut loaded = LoadedCircuit {
            out_schema: SchemaDescriptor::default(),
            nodes: {
                let mut m = HashMap::new();
                m.insert(0, gnitz_wire::OpNode::Filter(None));
                m.insert(1, gnitz_wire::OpNode::Filter(None));
                m
            },
            edges: vec![(0, 1, 0), (1, 0, 0)],
            ..Default::default()
        };
        assert!(topo_sort(&mut loaded).is_err());
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
        let co = |cols: Vec<(i32, u8)>| {
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
        let base = || {
            SchemaDescriptor::new(
                &[
                    SchemaColumn::new(type_code::U64, 0),
                    SchemaColumn::new(type_code::I64, 0),
                ],
                &[0],
            )
        };
        let replicated = base().with_replicated(true);
        let join_on_payload = || {
            let mut m = HashMap::new();
            m.insert(7i64, vec![(1i32, 0u8)]); // dim  shards on payload col 1
            m.insert(8i64, vec![(1i32, 0u8)]); // fact shards on payload col 1
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
        promoted.insert(7i64, vec![(0i32, type_code::I64)]);
        assert!(
            compute_co_partitioned(&promoted, &ext_r).contains(&7),
            "replicated source skips regardless of carried type-promotion"
        );
    }

    #[test]
    fn test_merge_schemas_for_join() {
        let left = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let right = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::STRING, 0),
            ],
            &[0],
        );
        let joined = merge_schemas_for_join(&left, &right, JoinNullFill::None).unwrap();
        assert_eq!(joined.num_columns(), 3); // PK + left_I64 + right_STRING
        assert_eq!(joined.columns[0].type_code, type_code::U128);
        assert_eq!(joined.columns[1].type_code, type_code::I64);
        assert_eq!(joined.columns[2].type_code, type_code::STRING);
    }

    #[test]
    fn test_merge_schemas_for_join_outer() {
        let left = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let right = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let joined = merge_schemas_for_join(&left, &right, JoinNullFill::RightNullable).unwrap();
        assert_eq!(joined.num_columns(), 3);
        assert_eq!(joined.columns[2].nullable, 1); // right side nullable
    }

    #[test]
    fn test_merge_schemas_for_join_compound_pk() {
        // Compound-PK left: 4 columns [U64, U64, U64, U64], PK = (col1, col2).
        let left = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[1, 2],
        );
        let right = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let joined = merge_schemas_for_join(&left, &right, JoinNullFill::None).unwrap();
        // Two PK columns up front, then left payload (2), then right payload (1) = 5.
        assert_eq!(joined.num_columns(), 5);
        assert_eq!(joined.pk_indices(), &[0, 1]);
        assert_eq!(joined.columns[0].type_code, type_code::U64);
        assert_eq!(joined.columns[1].type_code, type_code::U64);

        // Single-PK left collapses back to pk_indices = [0].
        let left_single = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let joined_single = merge_schemas_for_join(&left_single, &right, JoinNullFill::None).unwrap();
        assert_eq!(joined_single.pk_indices(), &[0]);
    }

    #[test]
    fn test_build_map_output_schema_compound_pk() {
        // Compound-PK input: 4 columns, PK = (col1, col2). Project [0, 3].
        let input = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[1, 2],
        );
        let out = build_map_output_schema(&input, &[0, 3]);
        // Two PK columns + two non-PK projected columns = 4 total.
        assert_eq!(out.num_columns(), 4);
        assert_eq!(out.pk_indices(), &[0, 1]);

        // Single-PK input collapses back to pk_indices = [0].
        let input_single = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let out_single = build_map_output_schema(&input_single, &[1]);
        assert_eq!(out_single.pk_indices(), &[0]);
    }

    #[test]
    fn test_agg_output_type() {
        assert_eq!(agg_output_type(AggOp::Count, TypeCode::I64), type_code::I64);
        assert_eq!(agg_output_type(AggOp::Sum, TypeCode::F64), type_code::F64);
        assert_eq!(agg_output_type(AggOp::Sum, TypeCode::I32), type_code::I64);
        assert_eq!(agg_output_type(AggOp::Max, TypeCode::F32), type_code::F64);
        // MIN/MAX select an existing row, so they preserve the source type: every
        // ≤8-byte integer keeps its own type (no widening to I64).
        assert_eq!(agg_output_type(AggOp::Min, TypeCode::I8), type_code::I8);
        assert_eq!(agg_output_type(AggOp::Max, TypeCode::I16), type_code::I16);
        assert_eq!(agg_output_type(AggOp::Min, TypeCode::I32), type_code::I32);
        assert_eq!(agg_output_type(AggOp::Max, TypeCode::U8), type_code::U8);
        assert_eq!(agg_output_type(AggOp::Min, TypeCode::U16), type_code::U16);
        assert_eq!(agg_output_type(AggOp::Max, TypeCode::U32), type_code::U32);
        // U64 folds into the general rule (the source type *is* U64); SUM over a
        // U64 source is also typed U64 (the i64 accumulator bit pattern is the
        // correct unsigned sum), so a downstream unsigned compare re-seeds right.
        assert_eq!(agg_output_type(AggOp::Min, TypeCode::U64), type_code::U64);
        assert_eq!(agg_output_type(AggOp::Max, TypeCode::U64), type_code::U64);
        assert_eq!(agg_output_type(AggOp::Sum, TypeCode::U64), type_code::U64);
        // Non-fixed-int sources (STRING / 16-byte) fall to the I64 arm as a
        // total-function default. MIN/MAX over them is rejected at compile (the
        // SQL binder, and emit_reduce's order-encodability guard — see
        // test_build_plan_min_max_over_non_encodable_rejected), so this result only
        // types a discarded schema and never reaches execution; agg_output_type
        // stays total, hence these asserts still hold.
        assert_eq!(agg_output_type(AggOp::Max, TypeCode::String), type_code::I64);
        assert_eq!(agg_output_type(AggOp::Min, TypeCode::U128), type_code::I64);
    }

    #[test]
    fn test_sequential_copy_projection() {
        use crate::expr::{LogicalInstr, LogicalProgram};
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
    fn test_build_reduce_output_schema_natural_pk() {
        let input = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::U64, 0), // group col
                SchemaColumn::new(type_code::I64, 0), // agg col
            ],
            &[0],
        );
        let aggs = vec![AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Sum,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        }];
        let out = build_reduce_output_schema(&input, &[1], &aggs, gnitz_wire::ReduceOutKey::SingleNaturalCol);
        // Natural PK (single U64 group col) → [U64_PK, I64_agg]
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.columns[0].type_code, type_code::U64);
        assert_eq!(out.columns[1].type_code, type_code::I64);
    }

    #[test]
    fn test_build_reduce_output_schema_compound_natural_pk() {
        // Input: pk_indices = [0, 1] (compound 2×U64), payload I64.
        let input = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        );
        let aggs = vec![AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        }];
        // group_cols = [1, 0] — permuted; the set still equals pk_indices.
        let out = build_reduce_output_schema(&input, &[1, 0], &aggs, gnitz_wire::ReduceOutKey::PkPermutation);
        // 2 PK cols + 1 agg col; pk_indices in source's pk-list order [0, 1].
        assert_eq!(out.num_columns(), 3);
        assert_eq!(out.pk_indices(), &[0, 1]);
        assert_eq!(out.columns[0].type_code, type_code::U64);
        assert_eq!(out.columns[1].type_code, type_code::U64);
        assert_eq!(out.columns[2].type_code, type_code::I64);
    }

    #[test]
    fn test_build_reduce_output_schema_single_pk_group_by_pk() {
        // Single-PK input grouped by its PK must collapse to the single-column
        // natural-PK shape (one PK col + agg).
        let input = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let aggs = vec![AggDescriptor {
            col_idx: 1,
            agg_op: AggOp::Sum,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        }];
        let out = build_reduce_output_schema(&input, &[0], &aggs, gnitz_wire::ReduceOutKey::PkPermutation);
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.pk_indices(), &[0]);
        assert_eq!(out.columns[0].type_code, type_code::U64);
        assert_eq!(out.columns[1].type_code, type_code::I64);
    }

    #[test]
    fn test_build_reduce_output_schema_synthetic_pk() {
        let input = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::STRING, 0), // group col
                SchemaColumn::new(type_code::I64, 0),    // agg col
            ],
            &[0],
        );
        let aggs = vec![AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        }];
        let out = build_reduce_output_schema(&input, &[1], &aggs, gnitz_wire::ReduceOutKey::SyntheticFold);
        // Synthetic PK (STRING group col) → [U128_hash, STRING_group, I64_count]
        assert_eq!(out.num_columns(), 3);
        assert_eq!(out.columns[0].type_code, type_code::U128);
        assert_eq!(out.columns[1].type_code, type_code::STRING);
        assert_eq!(out.columns[2].type_code, type_code::I64);
    }

    #[test]
    fn test_build_plan_fails_when_child_table_fails() {
        // Circuit: SCAN(0) → DISTINCT(1) → INTEGRATE(2)
        // An invalid view_dir forces create_child_table to fail inside emit_node.
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(99));
        nodes.insert(1, gnitz_wire::OpNode::Distinct);
        nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
        let loaded = make_loaded(nodes, vec![(0, 1, 0), (1, 2, 0)]);

        // Provide an external table so ScanDelta finds its schema and sets source_reg_map.
        let in_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        let ext_tables: ExtTables = HashMap::from([(99, in_schema)]);

        let ordered = loaded.ordered.clone();
        let result = build_plan(
            &loaded,
            &no_skips(),
            &ordered,
            &ext_tables,
            "/nonexistent_gnitz_test_path_xyz_abc",
            99,
            None,
            &[],
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
        let loaded = LoadedCircuit {
            out_schema: SchemaDescriptor::default(),
            nodes,
            edges: Vec::new(),
            ..Default::default()
        };
        let ordered: Vec<i32> = (0..n).collect();
        let result = build_plan(&loaded, &no_skips(), &ordered, &HashMap::new(), "", 1, None, &[]);
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
        use gnitz_wire::{AggFunc, AggKind, OpNode, ReduceOutKey};
        let base = format!("{}/git/gnitz/tmp", std::env::var("HOME").unwrap());
        std::fs::create_dir_all(&base).unwrap();

        let compiles = |agg_tc: u8, tag: &str| -> bool {
            let view_dir = format!("{base}/minmax_guard_{tag}_{}", std::process::id());
            let _ = std::fs::remove_dir_all(&view_dir);
            std::fs::create_dir_all(&view_dir).unwrap();
            // col 0: U64 PK + group key; col 1: the MAX aggregate column.
            let in_schema = SchemaDescriptor::new(
                &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(agg_tc, 0)],
                &[0],
            );
            let mut nodes = HashMap::new();
            nodes.insert(0, OpNode::ScanDelta(10));
            nodes.insert(
                1,
                OpNode::Reduce {
                    group_cols: vec![0],
                    agg: AggKind::Specs(vec![(AggFunc::Max, 1)]),
                    global_ground: false,
                    // group_cols = [0] = the single U64 PK ⇒ PkPermutation.
                    out_key: ReduceOutKey::PkPermutation,
                },
            );
            nodes.insert(2, OpNode::IntegrateSink);
            let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN)];
            let loaded = make_loaded(nodes, edges);
            let ext: ExtTables = HashMap::from([(10, in_schema)]);
            let ordered = loaded.ordered.clone();
            let some = build_plan(&loaded, &no_skips(), &ordered, &ext, &view_dir, 1, Some(2), &[]).is_ok();
            let _ = std::fs::remove_dir_all(&view_dir);
            some
        };

        assert!(
            compiles(type_code::I64, "i64"),
            "control: MAX over an order-encodable I64 column must compile"
        );
        assert!(
            !compiles(type_code::STRING, "str"),
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
        use gnitz_wire::{AggFunc, AggKind, OpNode, ReduceOutKey};
        let base = format!("{}/git/gnitz/tmp", std::env::var("HOME").unwrap());
        std::fs::create_dir_all(&base).unwrap();

        let compiles = |in_schema: SchemaDescriptor, group: Vec<u16>, out_key: ReduceOutKey, tag: &str| -> bool {
            let view_dir = format!("{base}/outkey_{tag}_{:?}_{}", out_key, std::process::id());
            let _ = std::fs::remove_dir_all(&view_dir);
            std::fs::create_dir_all(&view_dir).unwrap();
            let mut nodes = HashMap::new();
            nodes.insert(0, OpNode::ScanDelta(10));
            nodes.insert(
                1,
                OpNode::Reduce {
                    group_cols: group,
                    // A linear COUNT keeps the MIN/MAX-eligibility guard out of the
                    // picture, isolating the out_key validation.
                    agg: AggKind::Specs(vec![(AggFunc::Count, 0)]),
                    global_ground: false,
                    out_key,
                },
            );
            nodes.insert(2, OpNode::IntegrateSink);
            let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN)];
            let loaded = make_loaded(nodes, edges);
            let ext: ExtTables = HashMap::from([(10, in_schema)]);
            let ordered = loaded.ordered.clone();
            let some = build_plan(&loaded, &no_skips(), &ordered, &ext, &view_dir, 1, Some(2), &[]).is_ok();
            let _ = std::fs::remove_dir_all(&view_dir);
            some
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
                let ok = compiles(schema, group.clone(), kind, tag);
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
        // ScanDelta(wide) --port0--> Join(DT) <--port1-- ScanTrace(wide)
        // Join(DT) --> IntegrateSink.
        let schema = wide_pk_schema();
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(10));
        nodes.insert(1, gnitz_wire::OpNode::ScanTrace(20));
        nodes.insert(2, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(3, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 2, PORT_IN_A), (1, 2, PORT_TRACE), (2, 3, PORT_IN)];
        let loaded = make_loaded(nodes, edges);
        let ext: ExtTables = HashMap::from([(10, schema), (20, schema)]);
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &no_skips(), &ordered, &ext, "", 1, Some(2), &[]);
        assert!(
            result.is_ok(),
            "wide-PK Join(DeltaTrace) must compile after byte-API port"
        );
    }

    #[test]
    fn test_build_plan_integrate_trace_child_fail_rejected() {
        // ScanDelta(99) → IntegrateTrace(1) → IntegrateSink(2). An invalid
        // view_dir forces create_child_table to fail; the Integrate must
        // not be silently dropped — build_plan must return None.
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(99));
        nodes.insert(1, gnitz_wire::OpNode::IntegrateTrace);
        nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN)];
        let loaded = make_loaded(nodes, edges);
        let in_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        let ext: ExtTables = HashMap::from([(99, in_schema)]);
        let ordered = loaded.ordered.clone();
        let result = build_plan(
            &loaded,
            &no_skips(),
            &ordered,
            &ext,
            "/nonexistent_gnitz_test_path_integrate_trace",
            99,
            None,
            &[],
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
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(99));
        nodes.insert(1, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN)];
        let mut loaded = make_loaded(nodes, edges);
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
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &no_skips(), &ordered, &ext, "", 99, None, &[]);
        assert!(result.is_err(), "type-mismatched sink schema must be rejected");
    }

    // ── Item 35: corrupt Filter/Map blob aborts compilation ─────────────────

    #[test]
    fn test_build_plan_corrupt_filter_blob_aborts() {
        // ScanDelta(99) → Filter(corrupt blob) → IntegrateSink. A present blob
        // that fails to decode must abort, not silently degrade to WHERE TRUE.
        let corrupt = vec![0xFFu8; 16]; // valid length, invalid magic
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(99));
        nodes.insert(1, gnitz_wire::OpNode::Filter(Some(corrupt)));
        nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN)];
        let mut loaded = make_loaded(nodes, edges);
        let in_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        // Match out_schema to the sink so the item-32 column-count check passes;
        // the only thing that can fail this compile is the corrupt-blob abort.
        loaded.out_schema = in_schema;
        let ext: ExtTables = HashMap::from([(99, in_schema)]);
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &no_skips(), &ordered, &ext, "", 99, None, &[]);
        assert!(result.is_err(), "corrupt Filter blob must abort compilation");
    }

    #[test]
    fn test_build_plan_corrupt_map_blob_aborts() {
        // ScanDelta(99) → Map(Expression{corrupt blob}) → IntegrateSink.
        let corrupt = vec![0xFFu8; 16];
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(99));
        nodes.insert(
            1,
            gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
                program: corrupt,
                reindex_cols: vec![],
                reindex_target_tcs: vec![],
            }),
        );
        nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN)];
        let loaded = make_loaded(nodes, edges);
        let in_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        let ext: ExtTables = HashMap::from([(99, in_schema)]);
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &no_skips(), &ordered, &ext, "", 99, None, &[]);
        assert!(result.is_err(), "corrupt Map blob must abort compilation");
    }

    /// `reindex_output_schema` carries the shared width policy through to the exact
    /// schema `emit_node` builds: a ≤8-byte integer key narrows to its native width;
    /// STRING/BLOB, U128/UUID, and floats (PK-ineligible) stay U128, so `pk_stride()`
    /// reflects the reindex output stride.
    #[test]
    fn test_reindex_output_pk_width_policy() {
        // (key column type, expected output PK type, expected pk_stride)
        let cases = [
            (type_code::U64, type_code::U64, 8u8),
            (type_code::I32, type_code::I32, 4),
            (type_code::U16, type_code::U16, 2),
            (type_code::STRING, type_code::U128, 16),
            (type_code::BLOB, type_code::U128, 16),
            (type_code::U128, type_code::U128, 16),
            (type_code::UUID, type_code::U128, 16),
            (type_code::F64, type_code::U128, 16),
        ];
        for (key_tc, want_tc, want_stride) in cases {
            // in_schema: [U64 PK, <key col>]; reindex on the payload col so the
            // PK-ineligible key types (STRING/BLOB/float) are exercisable as keys.
            let in_schema = SchemaDescriptor::new(
                &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(key_tc, 0)],
                &[0],
            );
            let node_schema = reindex_output_schema(&in_schema, &[1u16], &[], &[0, 1]);
            assert_eq!(node_schema.columns[0].type_code, want_tc, "key {key_tc} → PK type");
            assert_eq!(node_schema.pk_stride(), want_stride, "key {key_tc} → pk_stride");
        }
    }

    /// `reindex_output_schema` for a compound (len > 1) key: each output PK slot
    /// takes its source column's `reindex_output_type_code`, the input columns
    /// follow, and `pk_stride` is the sum of the per-column slot widths.
    #[test]
    fn test_reindex_output_schema_compound() {
        // in_schema: [U64 pk, I32, U128]; reindex on (col1 I32, col2 U128).
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::U128, 0),
            ],
            &[0],
        );
        let out = reindex_output_schema(&in_schema, &[1u16, 2u16], &[], &[0, 1, 2]);
        assert_eq!(out.pk_indices(), &[0, 1], "2-slot compound PK");
        assert_eq!(out.columns[0].type_code, type_code::I32, "slot0 keeps I32 native width");
        assert_eq!(out.columns[1].type_code, type_code::U128, "slot1 U128");
        assert_eq!(out.pk_stride(), 4 + 16, "compound stride = Σ slot widths");
        // Input columns follow the synthetic PK slots.
        assert_eq!(out.num_columns(), 2 + 3);
        assert_eq!(out.columns[2].type_code, type_code::U64);
        assert_eq!(out.columns[3].type_code, type_code::I32);
        assert_eq!(out.columns[4].type_code, type_code::U128);
    }

    /// A carried cross-width target `T` overrides the per-column default policy:
    /// the slot takes `T`'s width (here I32 → I64), not the narrow source width.
    /// A `0` slot still derives from the source.
    #[test]
    fn test_reindex_output_schema_cross_width_promotes() {
        // in_schema: [U64 pk, I32, I64]; reindex on (col1 I32, col2 I64) with
        // slot 0 promoted to I64 (carried) and slot 1 self-deriving.
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let out = reindex_output_schema(&in_schema, &[1u16, 2u16], &[type_code::I64, 0], &[0, 1, 2]);
        assert_eq!(out.columns[0].type_code, type_code::I64, "slot0 carried T = I64");
        assert_eq!(out.columns[1].type_code, type_code::I64, "slot1 self-derives I64");
        assert_eq!(out.pk_stride(), 8 + 8, "both slots 8 bytes after promotion");
    }

    /// `payload_cols` places exactly the listed source columns (in order) behind
    /// the synthetic PK slots — the dead-column-eliding equi-join layout.
    #[test]
    fn test_reindex_output_schema_payload_prune() {
        // in_schema: [U64 pk, I32, U128, I16]; reindex on col1; keep payload {0, 3}.
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I16, 0),
            ],
            &[0],
        );
        let out = reindex_output_schema(&in_schema, &[1u16], &[], &[0u16, 3u16]);
        assert_eq!(out.pk_indices(), &[0], "single synthetic PK slot");
        assert_eq!(out.columns[0].type_code, type_code::I32, "PK slot = reindex col1 (I32)");
        // Only the two kept payload columns follow — not all four input columns.
        assert_eq!(out.num_columns(), 1 + 2, "1 PK + 2 kept payload");
        assert_eq!(out.columns[1].type_code, type_code::U64, "kept payload col 0");
        assert_eq!(out.columns[2].type_code, type_code::I16, "kept payload col 3");
    }

    /// A compound (len > 1) reindex Map now compiles end-to-end: the gate is
    /// lifted and `emit_node` builds a 2-slot-PK node schema, so `build_plan`
    /// returns `Some` (the sink's output schema matches the reindex output).
    #[test]
    fn test_build_plan_compound_reindex_accepted() {
        // Valid 2-col copy program so decode_expr_blob succeeds.
        let mut eb = gnitz_core::ExprBuilder::new();
        eb.copy_col(type_code::U64 as u32, 0, 0);
        eb.copy_col(type_code::I64 as u32, 1, 1);
        let blob = eb.build(0).encode();

        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(99));
        nodes.insert(
            1,
            gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
                program: blob,
                reindex_cols: vec![0, 1],
                reindex_target_tcs: vec![],
            }),
        );
        nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN)];
        let mut loaded = make_loaded(nodes, edges);
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        // The sink validates against the reindex Map's output schema (2 synthetic
        // PK slots [U64, I64] + the two input columns).
        loaded.out_schema = reindex_output_schema(&in_schema, &[0u16, 1u16], &[], &[0, 1]);
        let ext: ExtTables = HashMap::from([(99, in_schema)]);
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &no_skips(), &ordered, &ext, "", 99, None, &[]);
        assert!(
            result.is_ok(),
            "compound (len > 1) reindex must compile after the gate lift"
        );
    }

    /// A reindex list longer than `MAX_PK_COLUMNS` overflows the output schema's
    /// fixed PK array; `emit_node` must fail the compile cleanly (`build_plan`
    /// returns None) rather than panic or build a truncated key.
    #[test]
    fn test_build_plan_reindex_exceeds_max_pk_columns_rejected() {
        // 6-column source, reindex on all 6 → pk_n (6) > MAX_PK_COLUMNS (5).
        let mut eb = gnitz_core::ExprBuilder::new();
        eb.copy_col(type_code::U64 as u32, 0, 0);
        let blob = eb.build(0).encode();

        let n_cols = crate::schema::MAX_PK_COLUMNS + 1;
        let cols: Vec<SchemaColumn> = (0..n_cols).map(|_| SchemaColumn::new(type_code::U64, 0)).collect();
        let in_schema = SchemaDescriptor::new(&cols, &[0]);
        let reindex_cols: Vec<u16> = (0..n_cols as u16).collect();

        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(99));
        nodes.insert(
            1,
            gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
                program: blob,
                reindex_cols,
                reindex_target_tcs: vec![],
            }),
        );
        nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN)];
        let loaded = make_loaded(nodes, edges);
        let ext: ExtTables = HashMap::from([(99, in_schema)]);
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &no_skips(), &ordered, &ext, "", 99, None, &[]);
        assert!(result.is_err(), "reindex list > MAX_PK_COLUMNS must fail the compile");
    }

    /// The reindex output payload schema is derived from the program's copy list:
    /// a program that copies only a subset of the input columns compiles to the
    /// pruned `[key slots ‖ kept columns]` layout end-to-end (`build_plan` returns
    /// `Some` against a sink schema built from the same kept list).
    #[test]
    fn test_build_plan_pruned_reindex_compiles() {
        // 3-column source; reindex on col0, program keeps only col 2 as payload.
        let mut eb = gnitz_core::ExprBuilder::new();
        eb.copy_col(type_code::I64 as u32, 2, 0);
        let blob = eb.build(0).encode();
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(99));
        nodes.insert(
            1,
            gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
                program: blob,
                reindex_cols: vec![0],
                reindex_target_tcs: vec![],
            }),
        );
        nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN)];
        let mut loaded = make_loaded(nodes, edges);
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        loaded.out_schema = reindex_output_schema(&in_schema, &[0u16], &[], &[2]);
        let ext: ExtTables = HashMap::from([(99, in_schema)]);
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &no_skips(), &ordered, &ext, "", 99, None, &[]);
        assert!(result.is_ok(), "pruned reindex must compile to the derived schema");
    }

    /// A reindex program copying an out-of-range source column is a corrupt/forged
    /// catalog; `emit_node` must fail the compile cleanly (`build_plan` returns
    /// None) rather than read a zeroed schema slot.
    #[test]
    fn test_build_plan_reindex_program_oob_col_rejected() {
        // reindex on col0, program copies col 9 on a 2-column source.
        let mut eb = gnitz_core::ExprBuilder::new();
        eb.copy_col(type_code::U64 as u32, 9, 0);
        let blob = eb.build(0).encode();
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(99));
        nodes.insert(
            1,
            gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
                program: blob,
                reindex_cols: vec![0],
                reindex_target_tcs: vec![],
            }),
        );
        nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN)];
        let loaded = make_loaded(nodes, edges);
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let ext: ExtTables = HashMap::from([(99, in_schema)]);
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &no_skips(), &ordered, &ext, "", 99, None, &[]);
        assert!(result.is_err(), "out-of-range program copy must fail the compile");
    }

    // ── Item 29: scratch dir cleanup on compile failure ─────────────────────

    #[test]
    fn test_build_plan_cleans_scratch_dirs_on_failure() {
        // ScanDelta → IntegrateTrace → IntegrateSink. IntegrateTrace creates a
        // scratch dir. An invalid view_dir causes IntegrateTrace to fail the
        // compile. Scratch dirs created before the failure must be removed so
        // probing unsupported queries can't leak inodes.
        //
        // Failure is triggered via an invalid view_dir (nonexistent path), not
        // via a wide-PK rejection (the compiler no longer rejects wide PKs after
        // the byte-API port).
        let base = format!("{}/git/gnitz/tmp", std::env::var("HOME").unwrap());
        std::fs::create_dir_all(&base).unwrap();
        let view_dir = format!("{}/scratch_cleanup_test_{}", base, std::process::id());
        let _ = std::fs::remove_dir_all(&view_dir);
        std::fs::create_dir_all(&view_dir).unwrap();

        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        // ScanDelta → IntegrateTrace → IntegrateSink. Using an invalid sub-path
        // for the trace table so create_child_table fails the compile.
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(10));
        nodes.insert(1, gnitz_wire::OpNode::IntegrateTrace);
        nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN)];
        let loaded = make_loaded(nodes, edges);
        let ext: ExtTables = HashMap::from([(10, schema)]);
        let ordered = loaded.ordered.clone();
        // /nonexistent_path forces create_child_table to fail.
        let result = build_plan(
            &loaded,
            &no_skips(),
            &ordered,
            &ext,
            "/nonexistent_gnitz_scratch_cleanup_test_path",
            1,
            Some(2),
            &[],
        );
        assert!(result.is_err(), "IntegrateTrace failure must fail the compile");

        let leftover: Vec<String> = std::fs::read_dir(&view_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("scratch_"))
            .collect();
        let _ = std::fs::remove_dir_all(&view_dir);
        assert!(
            leftover.is_empty(),
            "scratch dirs must be removed on compile failure, found: {leftover:?}",
        );
    }

    // ── Items 16 & 28: load_circuit robustness (real system tables) ─────────

    fn wire_sys_schema(cols: &[gnitz_wire::WireSysCol]) -> SchemaDescriptor {
        let mut buf = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
        for (i, c) in cols.iter().enumerate() {
            buf[i] = SchemaColumn::new(c.type_code as u8, if c.nullable { 1 } else { 0 });
        }
        SchemaDescriptor::new(&buf[..cols.len()], &[0, 1])
    }

    fn load_circuit_test_dir(tag: &str) -> String {
        let base = format!("{}/git/gnitz/tmp", std::env::var("HOME").unwrap());
        std::fs::create_dir_all(&base).unwrap();
        let dir = format!("{}/load_circuit_{}_{}", base, tag, std::process::id());
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_load_circuit_aborts_on_undecodable_node() {
        // A single CircuitNodes row with an opcode decode_op_node rejects (item
        // 16). Previously the node was silently skipped; load_circuit must now
        // return None rather than emit a partial circuit.
        use crate::storage::BatchBuilder;
        let dir = load_circuit_test_dir("baddecode");
        let nodes_schema = wire_sys_schema(gnitz_wire::CIRCUIT_NODES_COLS);
        let edges_schema = wire_sys_schema(gnitz_wire::CIRCUIT_EDGES_COLS);
        let cols_schema = wire_sys_schema(gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS);

        let view_id: u64 = 1;
        // Match pack_view_pk: view_id in the high half so its at-rest OPK
        // (big-endian) image leads the PK region (where load_circuit seeks).
        let pk = |sub: u64| -> u128 { ((view_id as u128) << 64) | (sub as u128) };

        let mut nodes_tab = Table::new(
            &format!("{dir}/nodes"),
            nodes_schema,
            0,
            256 * 1024,
            RecoverySource::Rederive,
        )
        .unwrap();
        {
            let mut bb = BatchBuilder::new(nodes_schema);
            bb.begin_row(pk(1), 1);
            bb.put_u64(1); // node_id
            bb.put_u64(9999); // opcode — unknown → decode_op_node Err
            bb.put_null(); // source_table
            bb.put_null(); // expr_program
            bb.end_row();
            nodes_tab.ingest_owned_batch(bb.finish()).unwrap();
        }
        let mut edges_tab = Table::new(
            &format!("{dir}/edges"),
            edges_schema,
            0,
            256 * 1024,
            RecoverySource::Rederive,
        )
        .unwrap();
        let _ = &mut edges_tab; // empty
        let mut cols_tab = Table::new(
            &format!("{dir}/cols"),
            cols_schema,
            0,
            256 * 1024,
            RecoverySource::Rederive,
        )
        .unwrap();
        let _ = &mut cols_tab; // empty

        let result = load_circuit(
            &mut nodes_tab,
            &mut edges_tab,
            &mut cols_tab,
            view_id,
            SchemaDescriptor::default(),
        );
        let _ = std::fs::remove_dir_all(&dir);
        assert!(result.is_none(), "an undecodable node must abort load_circuit");
    }

    #[test]
    fn test_load_circuit_aborts_on_orphan_edge() {
        // Two valid nodes plus an edge whose dst (node 7) does not exist (item
        // 28). load_circuit must return None rather than create a phantom node.
        use crate::storage::BatchBuilder;
        let dir = load_circuit_test_dir("orphanedge");
        let nodes_schema = wire_sys_schema(gnitz_wire::CIRCUIT_NODES_COLS);
        let edges_schema = wire_sys_schema(gnitz_wire::CIRCUIT_EDGES_COLS);
        let cols_schema = wire_sys_schema(gnitz_wire::CIRCUIT_NODE_COLUMNS_COLS);

        let view_id: u64 = 1;
        // Match pack_view_pk: view_id in the high half so its at-rest OPK
        // (big-endian) image leads the PK region (where load_circuit seeks).
        let pk = |sub: u64| -> u128 { ((view_id as u128) << 64) | (sub as u128) };

        let mut nodes_tab = Table::new(
            &format!("{dir}/nodes"),
            nodes_schema,
            0,
            256 * 1024,
            RecoverySource::Rederive,
        )
        .unwrap();
        {
            let mut bb = BatchBuilder::new(nodes_schema);
            // node 0: ScanDelta(source 99)
            bb.begin_row(pk(0), 1);
            bb.put_u64(0);
            bb.put_u64(gnitz_wire::OPCODE_SCAN_DELTA);
            bb.put_u64(99); // source_table
            bb.put_null(); // expr_program
            bb.end_row();
            // node 1: IntegrateSink
            bb.begin_row(pk(1), 1);
            bb.put_u64(1);
            bb.put_u64(gnitz_wire::OPCODE_INTEGRATE);
            bb.put_null(); // source_table
            bb.put_null(); // expr_program
            bb.end_row();
            nodes_tab.ingest_owned_batch(bb.finish()).unwrap();
        }
        let mut edges_tab = Table::new(
            &format!("{dir}/edges"),
            edges_schema,
            0,
            256 * 1024,
            RecoverySource::Rederive,
        )
        .unwrap();
        {
            let mut bb = BatchBuilder::new(edges_schema);
            // Edge 0 → 7, but node 7 does not exist.
            bb.begin_row(pk(0), 1);
            bb.put_u64(7); // dst_node (orphan)
            bb.put_u64(PORT_IN as u64);
            bb.put_u64(0); // src_node
            bb.end_row();
            edges_tab.ingest_owned_batch(bb.finish()).unwrap();
        }
        let mut cols_tab = Table::new(
            &format!("{dir}/cols"),
            cols_schema,
            0,
            256 * 1024,
            RecoverySource::Rederive,
        )
        .unwrap();
        let _ = &mut cols_tab;

        let result = load_circuit(
            &mut nodes_tab,
            &mut edges_tab,
            &mut cols_tab,
            view_id,
            SchemaDescriptor::default(),
        );
        let _ = std::fs::remove_dir_all(&dir);
        assert!(
            result.is_none(),
            "an edge to a non-existent node must abort load_circuit"
        );
    }

    #[test]
    fn test_merge_schemas_for_join_column_overflow() {
        // A merged column count over MAX_COLUMNS returns None (compile rejected),
        // rather than aborting on the old assert.
        use crate::schema::MAX_COLUMNS;
        let half = MAX_COLUMNS / 2 + 2;
        let make = |n: usize| {
            let mut cols = [SchemaColumn::new(0, 0); MAX_COLUMNS];
            cols[0] = SchemaColumn::new(type_code::U128, 0);
            for col in cols.iter_mut().take(n).skip(1) {
                *col = SchemaColumn::new(type_code::I64, 0);
            }
            SchemaDescriptor::new(&cols[..n], &[0])
        };
        assert!(
            merge_schemas_for_join(&make(half), &make(half), JoinNullFill::None).is_none(),
            "an over-wide join output must be rejected (None), not aborted"
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

    /// Build `ScanDelta(10) → mid → IntegrateSink` and report whether it compiles.
    fn compiles_mid_node(in_schema: SchemaDescriptor, mid: gnitz_wire::OpNode, tag: &str) -> bool {
        let base = format!("{}/git/gnitz/tmp", std::env::var("HOME").unwrap());
        std::fs::create_dir_all(&base).unwrap();
        let view_dir = format!("{base}/pb_{tag}_{}", std::process::id());
        let _ = std::fs::remove_dir_all(&view_dir);
        std::fs::create_dir_all(&view_dir).unwrap();
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(10));
        nodes.insert(1, mid);
        nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN)];
        let loaded = make_loaded(nodes, edges);
        let ext: ExtTables = HashMap::from([(10, in_schema)]);
        let ordered = loaded.ordered.clone();
        let some = build_plan(&loaded, &no_skips(), &ordered, &ext, &view_dir, 1, Some(2), &[]).is_ok();
        let _ = std::fs::remove_dir_all(&view_dir);
        some
    }

    #[test]
    fn test_reduce_group_cols_out_of_bounds_rejected() {
        use gnitz_wire::{AggFunc, AggKind, OpNode, ReduceOutKey};
        let reduce = |group: Vec<u16>| OpNode::Reduce {
            group_cols: group,
            agg: AggKind::Specs(vec![(AggFunc::Count, 0)]),
            global_ground: false,
            out_key: ReduceOutKey::PkPermutation,
        };
        assert!(compiles_mid_node(two_col_schema(), reduce(vec![0]), "grp_ok"));
        assert!(!compiles_mid_node(two_col_schema(), reduce(vec![200]), "grp_oob"));
    }

    #[test]
    fn test_reduce_agg_spec_col_out_of_bounds_rejected() {
        use gnitz_wire::{AggFunc, AggKind, OpNode, ReduceOutKey};
        let reduce = |col: u16| OpNode::Reduce {
            group_cols: vec![0],
            agg: AggKind::Specs(vec![(AggFunc::Count, col)]),
            global_ground: false,
            out_key: ReduceOutKey::PkPermutation,
        };
        assert!(compiles_mid_node(two_col_schema(), reduce(1), "aggcol_ok"));
        assert!(!compiles_mid_node(two_col_schema(), reduce(200), "aggcol_oob"));
    }

    #[test]
    fn test_reduce_sum_over_non_decodable_column_rejected() {
        use gnitz_wire::{AggFunc, AggKind, OpNode, ReduceOutKey};
        // col 0 = U64 PK + group key; col 1 = the SUM aggregate column.
        let schema = |agg_tc: u8| {
            SchemaDescriptor::new(
                &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(agg_tc, 0)],
                &[0],
            )
        };
        let reduce = OpNode::Reduce {
            group_cols: vec![0],
            agg: AggKind::Specs(vec![(AggFunc::Sum, 1)]),
            global_ground: false,
            out_key: ReduceOutKey::PkPermutation,
        };
        // Control: SUM over an order-encodable I64 column compiles.
        assert!(compiles_mid_node(schema(type_code::I64), reduce.clone(), "sum_i64"));
        // SUM over a 16-byte column would abort at the first push in `decode_signed`.
        assert!(!compiles_mid_node(schema(type_code::U128), reduce.clone(), "sum_u128"));
        // SUM over a STRING column would silently mis-sum.
        assert!(!compiles_mid_node(schema(type_code::STRING), reduce, "sum_str"));
    }

    #[test]
    fn test_projection_col_out_of_bounds_rejected() {
        use gnitz_wire::{MapKind, OpNode};
        let proj = |cols: Vec<u16>| OpNode::Map(MapKind::Projection(cols));
        assert!(compiles_mid_node(two_col_schema(), proj(vec![1]), "proj_ok"));
        assert!(!compiles_mid_node(two_col_schema(), proj(vec![200]), "proj_oob"));
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
            "nx_ok",
        ));
        // MAX_COLUMNS type_codes overflow the fixed `[_; 65]` schema array (guard 4).
        assert!(!compiles_mid_node(
            two_col_schema(),
            OpNode::NullExtend {
                type_codes: vec![type_code::I64; crate::schema::MAX_COLUMNS],
            },
            "nx_len",
        ));
        // A near-max-width input plus a short extension overflows the *merged*
        // output width (guard 5, which guard 4 alone cannot catch): 64 + 2 > 65.
        let wide = {
            let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
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
            "nx_merge",
        ));
    }

    fn make_loaded(nodes: HashMap<i32, gnitz_wire::OpNode>, edges: Vec<(i32, i32, i32)>) -> LoadedCircuit {
        let mut lc = LoadedCircuit {
            out_schema: SchemaDescriptor::default(),
            nodes,
            edges,
            ..Default::default()
        };
        topo_sort(&mut lc).expect("test circuit must be acyclic");
        lc
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

    // ── §5: destructive-register ordering invariant ─────────────────────────
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
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(10));
        nodes.insert(distinct_id, gnitz_wire::OpNode::Distinct);
        nodes.insert(reader_id, gnitz_wire::OpNode::Negate);
        nodes.insert(3, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![
            (0, distinct_id, PORT_IN),
            (0, reader_id, PORT_IN),
            (distinct_id, 3, PORT_IN),
        ];
        make_loaded(nodes, edges)
    }

    #[test]
    fn test_destructive_fanout_legit_ordering_compiles() {
        // Distinct id 2 > Filter id 1, so the destructive op is scheduled LAST:
        // the assert must NOT fire and the circuit compiles end to end.
        let base = format!("{}/git/gnitz/tmp", std::env::var("HOME").unwrap());
        std::fs::create_dir_all(&base).unwrap();
        let view_dir = format!("{}/dtor_legit_{}", base, std::process::id());
        let _ = std::fs::remove_dir_all(&view_dir);
        std::fs::create_dir_all(&view_dir).unwrap();

        let loaded = make_dtor_fanout(2, 1);
        // Precondition: the destructive Distinct really is scheduled after its co-reader.
        let pos = |n: i32| loaded.ordered.iter().position(|&x| x == n).unwrap();
        assert!(pos(1) < pos(2), "test precondition: co-reader must precede Distinct");

        let ext: ExtTables = HashMap::from([(10, two_col_schema())]);
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &no_skips(), &ordered, &ext, &view_dir, 1, Some(3), &[]);
        let _ = std::fs::remove_dir_all(&view_dir);
        assert!(
            result.is_ok(),
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
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &no_skips(), &ordered, &ext, "", 1, Some(3), &[]);
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
        let base = format!("{}/git/gnitz/tmp", std::env::var("HOME").unwrap());
        std::fs::create_dir_all(&base).unwrap();
        let view_dir = format!("{}/dtor_skip_{}", base, std::process::id());
        let _ = std::fs::remove_dir_all(&view_dir);
        std::fs::create_dir_all(&view_dir).unwrap();

        let loaded = make_dtor_fanout(1, 2);
        let mut skips = no_skips();
        skips.insert(1); // Distinct elided by the distinct-elision pass

        let ext: ExtTables = HashMap::from([(10, two_col_schema())]);
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &skips, &ordered, &ext, &view_dir, 1, Some(3), &[]);
        let _ = std::fs::remove_dir_all(&view_dir);
        assert!(
            result.is_ok(),
            "a skipped (optimized-out) Distinct does not run destructively; \
             the guard must not reject it"
        );
    }

    // ── ScanTrace join-trace-side: no add_scan_trace when feeding port=1 ──

    /// A ScanTrace node feeding a Join via PORT_TRACE must not emit add_scan_trace
    /// or allocate an extra delta register.
    #[test]
    fn test_scan_trace_join_trace_side_no_extra_reg() {
        // Circuit: ScanDelta(10) --port0--> Join(DT)(2)
        //          ScanTrace(20) --port1--> Join(DT)(2)
        //          Join(2) --port0--> IntegrateSink(3)
        let schema = two_col_schema();
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(10));
        nodes.insert(1, gnitz_wire::OpNode::ScanTrace(20));
        nodes.insert(2, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(3, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![
            (0, 2, PORT_IN_A),  // delta side
            (1, 2, PORT_TRACE), // trace side — must NOT emit add_scan_trace
            (2, 3, PORT_IN),
        ];
        let loaded = make_loaded(nodes, edges);

        let ext: ExtTables = HashMap::from([(10, schema), (20, schema)]);
        let ordered = loaded.ordered.clone();
        let result = build_plan(
            &loaded,
            &no_skips(),
            &ordered,
            &ext,
            "",
            1,
            Some(2), // bypass out_schema mismatch check; sink_reg already set by IntegrateSink
            &[],
        );
        let plan = result.expect("build_plan must succeed for this circuit");

        // The trace-side ScanTrace (node 1) uses reg_id as the trace register;
        // no extra delta register is allocated for it.  The minimum register
        // count is: one per node (4) + zero extras from ScanTrace on trace side.
        // (There are no Distinct/Reduce nodes adding extras.)
        assert!(
            plan.vm.program.reg_meta.len() == 4,
            "expected exactly 4 regs (one per node, no extra for trace-side ScanTrace), got {}",
            plan.vm.program.reg_meta.len()
        );
    }

    /// A ScanTrace node that does NOT feed a join's TRACE port must still emit
    /// add_scan_trace and allocate an extra delta register.
    #[test]
    fn test_scan_trace_non_join_side_emits_scan_trace() {
        // Circuit: ScanDelta(10) --port0--> Union(2)
        //          ScanTrace(20) --port1--> Union(2)   [port=1 but Union ≠ Join → NOT join-trace-side]
        //          Union(2) --port0--> IntegrateSink(3)
        //
        // ScanDelta provides input_delta_reg_id via source_reg_map.
        // ScanTrace feeds Union on PORT_IN_B (=1), but Union is not a Join,
        // so is_join_trace_side = false →
        // add_scan_trace is emitted and one extra delta register is allocated.
        let schema = two_col_schema();
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(10));
        nodes.insert(1, gnitz_wire::OpNode::ScanTrace(20));
        nodes.insert(2, gnitz_wire::OpNode::Union);
        nodes.insert(3, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![
            (0, 2, PORT_IN_A), // ScanDelta → Union left
            (1, 2, PORT_IN_B), // ScanTrace → Union right (port=1, not a join)
            (2, 3, PORT_IN),
        ];

        let loaded = make_loaded(nodes, edges);
        let ext: ExtTables = HashMap::from([(10, schema), (20, schema)]);
        let ordered = loaded.ordered.clone();
        let result = build_plan(
            &loaded,
            &no_skips(),
            &ordered,
            &ext,
            "",
            1,
            Some(2), // bypass out_schema mismatch check
            &[],
        );
        let plan = result.expect("build_plan must succeed");

        // 4 nodes → base regs 0-3, plus 1 extra delta reg for ScanTrace.
        assert!(
            plan.vm.program.reg_meta.len() == 5,
            "expected 5 regs (4 base + 1 extra delta for non-join-side ScanTrace), got {}",
            plan.vm.program.reg_meta.len()
        );
    }

    // ── §3: Join must be emitted before the Integrate that writes the trace ──
    //
    // Within an epoch a DeltaTrace join reads `z⁻¹(I(B))` — the trace state
    // BEFORE this epoch's delta is integrated. That old-state guarantee is
    // enforced purely by compiled instruction order: the JoinDT instruction must
    // appear before any Integrate instruction that writes a trace this epoch.
    // build_plan emits in topological order, and the `Join → Integrate*` edge
    // makes the join a strict predecessor — but nothing else asserts it, so a
    // future reordering of the emit loop could silently invert it and feed the
    // join post-delta state. Pin the program-order relation.

    /// Index of the first `Instr` matching `pred` in a built program.
    fn first_instr_pos(plan: &PlanBuildResult, pred: impl Fn(&crate::query::vm::Instr) -> bool) -> usize {
        plan.vm
            .program
            .instructions
            .iter()
            .position(pred)
            .expect("program must contain the expected instruction")
    }

    #[test]
    fn test_join_emitted_before_integrate_sink() {
        // ScanDelta(10) --PORT_IN_A--> Join(DeltaTrace)(2) --PORT_IN--> IntegrateSink(3)
        // ScanTrace(20) --PORT_TRACE-> Join(DeltaTrace)(2)
        // The `Join → IntegrateSink` edge makes the join a topological predecessor
        // of the sink Integrate, so its JoinDT instruction must precede the
        // sink Integrate (which writes this epoch's output delta into view storage).
        let schema = two_col_schema();
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(10));
        nodes.insert(1, gnitz_wire::OpNode::ScanTrace(20));
        nodes.insert(2, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(3, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![
            (0, 2, PORT_IN_A),  // delta side
            (1, 2, PORT_TRACE), // trace side
            (2, 3, PORT_IN),    // join feeds the sink
        ];
        let loaded = make_loaded(nodes, edges);

        let ext: ExtTables = HashMap::from([(10, schema), (20, schema)]);
        let ordered = loaded.ordered.clone();
        let plan = build_plan(
            &loaded,
            &no_skips(),
            &ordered,
            &ext,
            "",
            1,
            Some(2), // bypass out_schema mismatch; sink_reg set by IntegrateSink
            &[],
        )
        .expect("build_plan must succeed for the ScanDelta→Join(DT)←ScanTrace→sink circuit");

        // Exactly one JoinDT and one Integrate (the sink) are emitted.
        let n_join = plan
            .vm
            .program
            .instructions
            .iter()
            .filter(|i| matches!(i, crate::query::vm::Instr::JoinDT { .. }))
            .count();
        let n_int = plan
            .vm
            .program
            .instructions
            .iter()
            .filter(|i| matches!(i, crate::query::vm::Instr::Integrate { .. }))
            .count();
        assert_eq!(n_join, 1, "expected exactly one JoinDT instruction, got {n_join}");
        assert_eq!(
            n_int, 1,
            "expected exactly one (sink) Integrate instruction, got {n_int}"
        );

        let jpos = first_instr_pos(&plan, |i| matches!(i, crate::query::vm::Instr::JoinDT { .. }));
        let ipos = first_instr_pos(&plan, |i| matches!(i, crate::query::vm::Instr::Integrate { .. }));
        assert!(
            jpos < ipos,
            "JoinDT (program pos {jpos}) must be emitted before the sink Integrate \
             (program pos {ipos}): the join reads z⁻¹(I) — trace state before this \
             epoch's delta is integrated — and instruction order is what enforces it",
        );
    }

    #[test]
    fn test_join_emitted_before_integrate_trace() {
        // Shared-source / chained-trace shape: the join's output is itself fed into
        // an IntegrateTrace (a downstream operator's z⁻¹ history) AND the view sink.
        //
        // ScanDelta(10) --PORT_IN_A--> Join(DeltaTrace)(2) ─┬─PORT_IN─► IntegrateTrace(3)
        // ScanTrace(20) --PORT_TRACE-> Join(DeltaTrace)(2)  └─PORT_IN─► IntegrateSink(4)
        //
        // Both Integrate* nodes are strict topological successors of the join, so
        // its JoinDT instruction must precede BOTH Integrate instructions — i.e.
        // it must precede the FIRST one. (IntegrateTrace writes a real trace table;
        // IntegrateSink writes view storage with a null target.)
        let schema = two_col_schema();
        let base = format!("{}/git/gnitz/tmp", std::env::var("HOME").unwrap());
        std::fs::create_dir_all(&base).unwrap();
        let view_dir = format!("{}/join_before_int_trace_{}", base, std::process::id());
        let _ = std::fs::remove_dir_all(&view_dir);
        std::fs::create_dir_all(&view_dir).unwrap();

        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(10));
        nodes.insert(1, gnitz_wire::OpNode::ScanTrace(20));
        nodes.insert(2, gnitz_wire::OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(3, gnitz_wire::OpNode::IntegrateTrace);
        nodes.insert(4, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![
            (0, 2, PORT_IN_A),  // delta side
            (1, 2, PORT_TRACE), // trace side
            (2, 3, PORT_IN),    // join feeds an IntegrateTrace (downstream z⁻¹ history)
            (2, 4, PORT_IN),    // join also feeds the view sink
        ];
        let loaded = make_loaded(nodes, edges);

        let ext: ExtTables = HashMap::from([(10, schema), (20, schema)]);
        let ordered = loaded.ordered.clone();
        let result = build_plan(
            &loaded,
            &no_skips(),
            &ordered,
            &ext,
            &view_dir,
            1,
            Some(2), // bypass out_schema mismatch; sink_reg set by IntegrateSink
            &[],
        );
        let _ = std::fs::remove_dir_all(&view_dir);
        let plan = result.expect("build_plan must succeed for the join→IntegrateTrace+sink circuit");

        // Two Integrate instructions are emitted (the trace and the sink); the JoinDT
        // must precede the first of them.
        let n_int = plan
            .vm
            .program
            .instructions
            .iter()
            .filter(|i| matches!(i, crate::query::vm::Instr::Integrate { .. }))
            .count();
        assert_eq!(
            n_int, 2,
            "expected two Integrate instructions (trace + sink), got {n_int}"
        );

        let jpos = first_instr_pos(&plan, |i| matches!(i, crate::query::vm::Instr::JoinDT { .. }));
        let first_ipos = first_instr_pos(&plan, |i| matches!(i, crate::query::vm::Instr::Integrate { .. }));
        assert!(
            jpos < first_ipos,
            "JoinDT (program pos {jpos}) must be emitted before the first Integrate \
             (program pos {first_ipos}): the join reads z⁻¹(I) — trace state before \
             this epoch's delta is integrated into any trace — and instruction order \
             is what enforces it",
        );
    }

    // ── compute_join_shard_map covers ScanDelta (SQL-planner join pattern) ──

    /// compute_join_shard_map must find ScanDelta → Map(reindex) chains, not
    /// just ScanTrace sources.
    #[test]
    fn test_compute_join_shard_map_scan_delta() {
        use gnitz_wire::{MapKind, OpNode};

        // Minimal two-sided SQL join circuit skeleton:
        //   ScanDelta(left_tid=10) → Map(reindex_col=1) → Join → IntegrateSink
        //   ScanDelta(right_tid=20) → Map(reindex_col=0) → Join
        let dummy_blob = dummy_expr_blob();
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(10));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![1],
                reindex_target_tcs: vec![],
            }),
        );
        nodes.insert(2, OpNode::ScanDelta(20));
        nodes.insert(
            3,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![0],
                reindex_target_tcs: vec![],
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
        let loaded = make_loaded(nodes, edges);

        let map = compute_join_shard_map(&loaded);

        assert_eq!(
            map.get(&10).cloned().unwrap_or_default(),
            vec![(1, 0)],
            "left side (source 10) must map to reindex_col=1"
        );
        assert_eq!(
            map.get(&20).cloned().unwrap_or_default(),
            vec![(0, 0)],
            "right side (source 20) must map to reindex_col=0"
        );
    }

    #[test]
    fn test_compute_join_shard_map_through_filter() {
        use gnitz_wire::{MapKind, OpNode};
        // ScanDelta(42) → Filter → Map(reindex_col=1) → Join → IntegrateSink.
        // The reindex Map is two hops from the scan (a Filter sits between),
        // so the one-hop lookup misses it; BFS through Filter must find it.
        let dummy_blob = dummy_expr_blob();
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(42));
        nodes.insert(1, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(
            2,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![1],
                reindex_target_tcs: vec![],
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
        let loaded = make_loaded(nodes, edges);

        // Shared helper used by both compute_join_shard_map and
        // DagEngine::get_join_shard_cols.
        assert_eq!(reindex_cols_through_filters(&loaded, 0), vec![(1, 0)]);

        let map = compute_join_shard_map(&loaded);
        assert_eq!(
            map.get(&42).cloned().unwrap_or_default(),
            vec![(1, 0)],
            "ScanDelta → Filter → Map(reindex) must map source 42 to col 1"
        );
    }

    #[test]
    fn test_circuit_range_join_n_eq_discriminator() {
        use gnitz_wire::{AggKind, JoinKind, MapKind, OpNode, ReduceOutKey};
        let dummy_blob = dummy_expr_blob();

        // A GROUP BY view: ScanDelta → Map(reindex) → ExchangeShard → Reduce →
        // IntegrateSink. It has BOTH a reindex Map (has_join_shard) AND an
        // ExchangeShard (has_exchange), so the wrong discriminator
        // `has_join_shard && has_exchange` would (incorrectly) call it a range
        // join. circuit_range_join_n_eq must return None — no DeltaTraceRange node.
        let mut gb = HashMap::new();
        gb.insert(0, OpNode::ScanDelta(7));
        gb.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![1],
                reindex_target_tcs: vec![],
            }),
        );
        gb.insert(2, OpNode::ExchangeShard { shard_cols: vec![1] });
        gb.insert(
            3,
            OpNode::Reduce {
                group_cols: vec![1],
                agg: AggKind::Null,
                global_ground: false,
                // Only exercises range-join classification, never the reduce
                // output schema/validation; the kind is immaterial here.
                out_key: ReduceOutKey::SyntheticFold,
            },
        );
        gb.insert(4, OpNode::IntegrateSink);
        let gb_edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN), (3, 4, PORT_IN)];
        let gb_loaded = make_loaded(gb, gb_edges);
        assert_eq!(
            circuit_range_join_n_eq(&gb_loaded),
            None,
            "GROUP BY view must NOT be classified as a range join"
        );

        // A range join: a Join(DeltaTraceRange) node makes it Some, carrying n_eq.
        let mut rj = HashMap::new();
        rj.insert(0, OpNode::ScanDelta(7));
        rj.insert(1, OpNode::ScanTrace(8));
        rj.insert(
            2,
            OpNode::Join(JoinKind::DeltaTraceRange {
                n_eq: 2,
                rel: gnitz_wire::RangeRel::Lt,
            }),
        );
        rj.insert(3, OpNode::IntegrateSink);
        let rj_edges = vec![(0, 2, PORT_IN_A), (1, 2, PORT_TRACE), (2, 3, PORT_IN)];
        let rj_loaded = make_loaded(rj, rj_edges);
        assert_eq!(
            circuit_range_join_n_eq(&rj_loaded),
            Some(2),
            "a Join(DeltaTraceRange) node classifies the view as a range join, carrying its n_eq"
        );
    }

    #[test]
    fn test_reindex_cols_through_filters_with_partition_filter_after_map() {
        use gnitz_wire::{MapKind, OpNode};
        // A reindex Map followed by a PartitionFilter, with NO join node present
        // (so is_join_view == false and the feeds_trace_or_join guard is off): the
        // walk reaches the Map and returns its cols, then stops — a PartitionFilter
        // is not a Filter, so it is never stepped through. The real range circuit,
        // where a Join node IS present (is_join_view == true) and the reindex feeds
        // it directly, is covered by
        // test_reindex_cols_through_filters_range_join_feeds_join_directly.
        let dummy_blob = dummy_expr_blob();
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(99));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
            }),
        );
        nodes.insert(2, OpNode::PartitionFilter);
        nodes.insert(3, OpNode::IntegrateTrace);
        let edges = vec![
            (0, 1, PORT_IN), // ScanDelta → reindex Map
            (1, 2, PORT_IN), // Map → PartitionFilter
            (2, 3, PORT_IN), // PartitionFilter → IntegrateTrace
        ];
        let loaded = make_loaded(nodes, edges);
        assert_eq!(
            reindex_cols_through_filters(&loaded, 0),
            vec![(2, 0)],
            "PartitionFilter after the reindex Map must not change the walk result"
        );
    }

    /// Real pure-range-join shape (planner.rs, `n_eq == 0`): the reindex Map feeds
    /// the `Join(DeltaTraceRange)` node DIRECTLY as the delta term AND feeds a
    /// `PartitionFilter → IntegrateTrace` toward the trace term. A Join node is
    /// present, so `is_join_view == true` and the `feeds_trace_or_join` guard is
    /// live — and it returns true via the DIRECT Map→Join edge, so the reindex key
    /// is still collected as the scatter key. This is what keeps
    /// `get_join_shard_cols` non-empty for a pure range join (hence
    /// `prepare_relay`'s `is_join` / `range_n_eq` and the broadcast routing). Guards
    /// against the misreading that the reindex only feeds the PartitionFilter.
    #[test]
    fn test_reindex_cols_through_filters_range_join_feeds_join_directly() {
        use gnitz_wire::{JoinKind, MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(99) ─► Map(reindex=[2]) ─┬─► Join(DeltaTraceRange)  [delta, PORT_IN_A]
        //                                     └─► PartitionFilter ─► IntegrateTrace ─► Join  [PORT_TRACE]
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(99));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
            }),
        );
        nodes.insert(2, OpNode::PartitionFilter);
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
            (1, 2, PORT_IN),    // reindex Map → PartitionFilter (toward the trace)
            (2, 3, PORT_IN),    // PartitionFilter → IntegrateTrace
            (3, 4, PORT_TRACE), // IntegrateTrace → Join (trace term)
        ];
        let loaded = make_loaded(nodes, edges);
        assert_eq!(
            reindex_cols_through_filters(&loaded, 0),
            vec![(2, 0)],
            "the reindex feeds the Join directly, so feeds_trace_or_join is true \
             even with a PartitionFilter toward the trace"
        );
    }

    #[test]
    fn test_reindex_col_through_filters_trivial_and_absent() {
        use gnitz_wire::{MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        // Trivial: ScanDelta → Map(reindex) directly (no Filter).
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(7));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![3],
                reindex_target_tcs: vec![],
            }),
        );
        let loaded = make_loaded(nodes, vec![(0, 1, PORT_IN)]);
        assert_eq!(reindex_cols_through_filters(&loaded, 0), vec![(3, 0)]);

        // Absent: ScanDelta → Map with no reindex columns.
        let mut nodes2 = HashMap::new();
        nodes2.insert(0, OpNode::ScanDelta(7));
        nodes2.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![],
                reindex_target_tcs: vec![],
            }),
        );
        let loaded2 = make_loaded(nodes2, vec![(0, 1, PORT_IN)]);
        assert!(reindex_cols_through_filters(&loaded2, 0).is_empty());
    }

    /// Multi-join: a single ScanDelta fans out through two reindex Maps on
    /// different columns. Both column IDs must be collected, not just the first.
    #[test]
    fn test_reindex_cols_through_filters_multi_join() {
        use gnitz_wire::{MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(0) ──► Map(reindex_col=2)
        //              └──► Filter ──► Map(reindex_col=5)
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(42));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
            }),
        );
        nodes.insert(2, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(
            3,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![5],
                reindex_target_tcs: vec![],
            }),
        );
        let edges = vec![(0, 1, PORT_IN), (0, 2, PORT_IN), (2, 3, PORT_IN)];
        let loaded = make_loaded(nodes, edges);
        let mut got = reindex_cols_through_filters(&loaded, 0);
        got.sort_unstable();
        assert_eq!(got, vec![(2, 0), (5, 0)], "both reindex columns must be collected");
    }

    /// An overlapping key (`a.x = b.p AND a.x = b.q`) reindexes `[x, x]`, possibly
    /// with distinct per-slot promotion targets. The sequence must survive
    /// VERBATIM — duplicates and all — so the scatter packer mirrors the trace-side
    /// ReindexPacker slot-for-slot; column-level dedup would collapse it to one.
    #[test]
    fn test_reindex_cols_through_filters_overlapping_key_verbatim() {
        use gnitz_wire::{MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(42));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![3, 3],
                reindex_target_tcs: vec![0, type_code::I64],
            }),
        );
        let loaded = make_loaded(nodes, vec![(0, 1, PORT_IN)]);
        assert_eq!(
            reindex_cols_through_filters(&loaded, 0),
            vec![(3, 0), (3, type_code::I64)],
            "overlapping key sequence must survive verbatim, not be deduplicated"
        );
    }

    /// A nullable LEFT-join key fans its source to two sibling reindex Maps (the
    /// not-null match side and the null-key bypass) carrying an IDENTICAL sequence.
    /// They must collapse to ONE copy, never be concatenated (which would double
    /// the key columns and diverge from the trace).
    #[test]
    fn test_reindex_cols_through_filters_sibling_maps_collapse() {
        use gnitz_wire::{MapKind, OpNode};
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(7) ──► Filter(not-null) ──► Map(reindex [2])
        //              └──► Filter(is-null)  ──► Map(reindex [2])  (identical seq)
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(7));
        nodes.insert(1, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(
            2,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![2],
                reindex_target_tcs: vec![0],
            }),
        );
        nodes.insert(3, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(
            4,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![2],
                reindex_target_tcs: vec![0],
            }),
        );
        let edges = vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (0, 3, PORT_IN), (3, 4, PORT_IN)];
        let loaded = make_loaded(nodes, edges);
        assert_eq!(
            reindex_cols_through_filters(&loaded, 0),
            vec![(2, 0)],
            "identical sibling sequences must collapse to one, not concatenate"
        );
    }

    /// Band LEFT join shape: the left scan feeds BOTH the join reindex (`[eq, range]`,
    /// feeding the DeltaTraceRange) AND an auxiliary `a.pk` re-key (feeding only the
    /// null-fill's Map → Distinct). Only the join reindex defines the input scatter
    /// key; the auxiliary re-key must be ignored (it re-keys already-scattered rows in
    /// place). A naive walk would concatenate both and corrupt the eq-prefix scatter.
    #[test]
    fn test_reindex_cols_through_filters_ignores_aux_rekey_in_join_view() {
        use gnitz_wire::{JoinKind, MapKind, OpNode, RangeRel};
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(10) ──► Map(reindex [1,2]) ──► Join(DeltaTraceRange)
        //              │                       └─► IntegrateTrace
        //              └──► Map(reindex [0]) ──► Map(Projection) ──► Distinct   (a_all → proj_a → D)
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(10));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![1, 2],
                reindex_target_tcs: vec![],
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
        let loaded = make_loaded(nodes, edges);
        assert_eq!(
            reindex_cols_through_filters(&loaded, 0),
            vec![(1, 0), (2, 0)],
            "only the trace/probe-feeding reindex defines the scatter key; the a.pk re-key is ignored"
        );
    }

    /// Pure ScanTrace sources (Python-API joins) must also appear in the map.
    #[test]
    fn test_compute_join_shard_map_scan_trace_unchanged() {
        use gnitz_wire::{MapKind, OpNode};

        let dummy_blob = dummy_expr_blob();
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(10));
        nodes.insert(1, OpNode::ScanTrace(20));
        nodes.insert(
            2,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob,
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
            }),
        );
        nodes.insert(3, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(4, OpNode::IntegrateSink);
        let edges = vec![
            (0, 2, PORT_IN),    // ScanDelta → reindex Map
            (1, 3, PORT_TRACE), // ScanTrace → join trace port (no reindex)
            (2, 3, PORT_IN_A),
            (3, 4, PORT_IN),
        ];
        let loaded = make_loaded(nodes, edges);

        let map = compute_join_shard_map(&loaded);

        // ScanDelta(10) → Map(reindex_col=2) must be found.
        assert_eq!(
            map.get(&10).cloned().unwrap_or_default(),
            vec![(2, 0)],
            "ScanDelta source must be in join_shard_map"
        );
        // ScanTrace(20) has no downstream reindex Map — must NOT appear.
        assert!(
            !map.contains_key(&20),
            "ScanTrace-only source with no reindex Map must not be in join_shard_map"
        );
    }

    // ── scan_tid_through_filters: the backward (shard → scan) Filter walk ───────
    //
    // The view exchange-skip detector's source resolution. The skip itself has no
    // observable "fired" signal at the E2E layer (exchanging is also correct), so
    // these unit tests are what pin that the walk engages exactly when it should:
    // through Filter chains, never across a re-keying Map / PartitionFilter / fan-in.

    /// A bare `ScanDelta → ExchangeShard` (the no-`WHERE` case) resolves to the source
    /// tid; `ScanDelta → Filter → ExchangeShard` (filtered `GROUP BY prefix`) does too,
    /// as do a chain of Filters and a `ScanTrace` source.
    #[test]
    fn test_scan_tid_through_filters_filter_chain() {
        use gnitz_wire::OpNode;
        let dummy_blob = dummy_expr_blob();
        // ScanDelta(7) → ExchangeShard, no Filter: the zero-hop base case (no `WHERE`),
        // which already co-partitioned before the walk reached through Filters.
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(7));
        nodes.insert(1, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = make_loaded(nodes, vec![(0, 1, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 1),
            Some(7),
            "a bare scan feeding the shard resolves on the first hop (no-`WHERE` case)"
        );

        // ScanDelta(7) → Filter → ExchangeShard.
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(7));
        nodes.insert(1, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(2, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = make_loaded(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 2),
            Some(7),
            "one Filter between scan and shard is transparent to the shard key"
        );

        // ScanTrace(8) → Filter → Filter → ExchangeShard.
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanTrace(8));
        nodes.insert(1, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(2, OpNode::Filter(Some(dummy_blob)));
        nodes.insert(3, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = make_loaded(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 3),
            Some(8),
            "a chain of Filters is transparent, and ScanTrace sources resolve too"
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
        nodes.insert(0, OpNode::ScanDelta(7));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
            }),
        );
        nodes.insert(2, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = make_loaded(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 2),
            None,
            "a reindex Map re-keys the PK; the walk must bail rather than cross it"
        );

        // ScanDelta(7) → Map(reindex) → Filter → ExchangeShard: a Filter below the
        // Map does not rescue it — the walk still reaches the Map and bails.
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(7));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Expression {
                program: dummy_blob.clone(),
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
            }),
        );
        nodes.insert(2, OpNode::Filter(Some(dummy_blob)));
        nodes.insert(3, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = make_loaded(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 3),
            None,
            "a Filter below a reindex Map does not make the Map transparent"
        );
    }

    /// A `PartitionFilter` (range-join broadcast input) is a distinct OpNode variant,
    /// not a `Filter`, so it is never crossed — the same exclusion
    /// `reindex_cols_through_filters` makes on the forward walk.
    #[test]
    fn test_scan_tid_through_filters_partition_filter() {
        use gnitz_wire::OpNode;
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(7));
        nodes.insert(1, OpNode::PartitionFilter);
        nodes.insert(2, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = make_loaded(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 2),
            None,
            "PartitionFilter is not a Filter; the walk must bail"
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
        nodes.insert(0, OpNode::ScanDelta(7));
        nodes.insert(1, OpNode::ScanDelta(8));
        nodes.insert(2, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = make_loaded(nodes, vec![(0, 2, PORT_IN_A), (1, 2, PORT_IN_B)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 2),
            None,
            "a shard with two incoming edges is a fan-in, not a linear chain"
        );

        // Fan-in one hop in: two scans feed a Filter that feeds the shard. The shard
        // has one incoming edge, but the Filter has two — bail at the Filter.
        let mut nodes = HashMap::new();
        nodes.insert(0, OpNode::ScanDelta(7));
        nodes.insert(1, OpNode::ScanDelta(8));
        nodes.insert(2, OpNode::Filter(Some(dummy_blob)));
        nodes.insert(3, OpNode::ExchangeShard { shard_cols: vec![0] });
        let loaded = make_loaded(nodes, vec![(0, 2, PORT_IN_A), (1, 2, PORT_IN_B), (2, 3, PORT_IN)]);
        assert_eq!(
            scan_tid_through_filters(&loaded, 3),
            None,
            "a fan-in at an intervening Filter also bails (the Filter has two incoming edges)"
        );
    }

    // ── Finding 2: load_circuit must return None for null system-table pointers ──

    /// Null system-table pointers are a programming error; the engine always supplies
    /// valid handles.  load_circuit must return None so callers get an explicit failure
    /// rather than silently reading an incomplete circuit and producing wrong results.
    #[test]
    fn test_load_circuit_returns_none_for_null_system_tables() {
        let result = load_circuit(
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
            SchemaDescriptor::default(),
        );
        assert!(
            result.is_none(),
            "null system-table pointers must return None, not a silently empty circuit"
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
        let mut pair = Batch::with_schema(merged, 2);
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
            let mut bat = Batch::with_schema(merged, 1);
            push_null_garbage(&mut bat, garbage);
            bat.certify_layout(Layout::Consolidated, &merged);
            bat
        };
        let unioned = op_union(single(g0), Some(&single(g1)), &merged);
        assert_eq!(unioned.count, 2, "Z-Set + keeps both rows before consolidation");
        let empty = Rc::new(Batch::empty_with_schema(&merged));
        let mut ch = ReadCursor::from_owned(std::slice::from_ref(&empty), merged);
        let (out, _) = op_distinct(unioned, &mut ch, &merged);
        assert_eq!(out.count, 1, "null-aware comparator coalesces the two NULL rows");
        assert_eq!(out.get_weight(0), 1, "distinct clamps the coalesced weight 2 → 1");
    }
}
