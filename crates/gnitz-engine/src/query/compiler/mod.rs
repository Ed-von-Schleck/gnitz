//! Circuit compiler: reads system tables, builds a DBSP circuit graph,
//! runs annotation + optimization passes, and emits VM instructions.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::expr::{LogicalProgram, ResolvedProgram, ScalarFunc};
use crate::foundation::worker_ctx::{num_workers, worker_rank};
use crate::ops::{AggDescriptor, AggOp};
use crate::query::vm::{ProgramBuilder, RegisterMeta, VmHandle};
use crate::schema::{is_fixed_int, type_code, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::{CursorHandle, Persistence, Table};

mod emit;
mod load;
mod optimize;

use emit::*;
use optimize::*;

pub(crate) use load::{
    circuit_range_join_n_eq, load_circuit, reindex_cols_through_filters, scan_tid_through_filters, topo_sort,
};

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
    /// `build_plan` returned `None` for the single, unexchanged pipeline.
    NoExchangeBuildFailed,
    /// `build_plan` returned `None` for an exchange sub-phase (pre / post / side).
    PlanBuildFailed,
    /// A sub-plan's output register is negative or past `reg_meta`'s length.
    OutRegOutOfBounds,
    /// A binary set-op side has no `ExchangeShard` input node (malformed circuit).
    MissingExchangeInput,
    /// More than two exchange boundaries — not produced by any planner path.
    TooManyExchanges,
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Typed circuit graph with OpNode payloads. `edges` is the raw edge list
/// before topological sort; the sorted helpers are populated by `topo_sort`.
pub(crate) struct LoadedCircuit {
    out_schema: SchemaDescriptor,
    pub(crate) nodes: HashMap<i32, gnitz_wire::OpNode>,
    /// Raw (src, dst, port) tuples read from CircuitEdges — input to topo_sort.
    pub(crate) edges: Vec<(i32, i32, i32)>,
    // Populated by topo_sort:
    ordered: Vec<i32>,
    pub(crate) outgoing: HashMap<i32, Vec<(i32, i32)>>,
    incoming: HashMap<i32, Vec<(i32, i32)>>,
    consumers: HashMap<i32, Vec<i32>>,
    /// Raw node-column rows for GatherReduce nodes only (stays on legacy path
    /// until OpNode::GatherReduce gains an `agg: AggKind` field).
    gather_reduce_cols: HashMap<i32, Vec<(u64, u16, u64, u64)>>,
}

impl LoadedCircuit {
    pub(crate) fn empty() -> Self {
        LoadedCircuit {
            out_schema: SchemaDescriptor::default(),
            nodes: HashMap::new(),
            edges: Vec::new(),
            ordered: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            consumers: HashMap::new(),
            gather_reduce_cols: HashMap::new(),
        }
    }
}

/// Annotation results from pre-passes.
struct Annotation {
    co_partitioned: HashSet<i64>,
    is_distinct_at: HashSet<i32>,
}

/// Optimization rewrite decisions.
struct Rewrites {
    skip_nodes: HashSet<i32>,
    fold_finalize: HashMap<i32, usize>, // reduce_nid → index into owned_expr_progs
    folded_maps: HashMap<i32, i32>,     // map_nid → reduce_nid
}

/// External table handle + schema.
pub(crate) struct ExternalTable {
    pub table_id: i64,
    pub schema: SchemaDescriptor,
}

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
    pub num_regs: u32,
    pub in_reg: u16,
    pub out_reg: u16,
    /// Maps a source table id to the input register that receives its delta.
    /// Empty for the post-combine phase (which has no source-level routing).
    pub source_reg_map: HashMap<i64, u16>,
    pub ext_trace_regs: Vec<(u16, i64)>,
}

/// A second exchange side, present only for binary set-op views (UNION /
/// EXCEPT / INTERSECT) which repartition **two** `HashRow`-reindexed inputs.
/// Side A reuses [`CompileOutput::pre`]; this struct carries the parallel
/// right-hand sub-pipeline.
pub(crate) struct SideBPlan {
    pub plan: SubPlan,
    pub exchange_schema: SchemaDescriptor,
    /// Register in the post VM seeded with this side's relayed/exchanged batch.
    pub post_seed_reg: u16,
    /// Source table this side scans — used as the exchange `source_id` so the
    /// two sides' IPC rounds key distinctly in the master accumulator.
    pub source_id: i64,
}

/// Output from `compile_view`, consumed directly by DagEngine to build `CachedPlan`.
///
/// Phase model:
/// * **No exchange** (`post == None`): `pre` is the whole plan.
/// * **One exchange** (GROUP BY, SELECT DISTINCT): `pre` computes up to the
///   shard, `post` consumes the relayed batch.
/// * **Two exchanges** (binary set-ops, `side_b.is_some()`): `pre` is side A,
///   `side_b` is side B; each is exchanged independently, then `post` runs
///   the combine over both relayed inputs.
pub(crate) struct CompileOutput {
    pub pre: SubPlan,
    /// Post-combine phase; `None` for views with no exchange.
    /// `SubPlan::in_reg` is the seed register for the relayed batch.
    /// For two-sided set-ops it is side A's seed; side B's lives in
    /// [`SideBPlan::post_seed_reg`].
    pub post: Option<SubPlan>,
    pub exchange_in_schema: Option<SchemaDescriptor>,
    pub co_partitioned: HashSet<i64>,
    /// Source table side A (`pre`) scans, for exchange keying. 0 when unknown
    /// (single-exchange views keep passing source_id=0 to the exchange).
    pub side_a_source_id: i64,
    /// Right-hand exchange side for binary set-ops; `None` otherwise.
    pub side_b: Option<SideBPlan>,
}

/// Decoded `ExprProgram` blob (inline copy of the gnitz-core wire shape so the
/// engine doesn't take a dependency on the client crate).
struct DecodedExprProgram {
    num_regs: u32,
    result_reg: u32,
    code: Vec<u32>,
    const_strings: Vec<Vec<u8>>,
}

const EXPR_BLOB_MAGIC: u32 = 0x5258_5045; // "EXPR" little-endian
const EXPR_BLOB_VERSION: u8 = 1;
const EXPR_BLOB_HEADER_SIZE: usize = 16;

fn decode_expr_blob(blob: &[u8]) -> Option<DecodedExprProgram> {
    if blob.len() < EXPR_BLOB_HEADER_SIZE {
        return None;
    }
    if u32::from_le_bytes(blob[0..4].try_into().unwrap()) != EXPR_BLOB_MAGIC {
        return None;
    }
    if blob[4] != EXPR_BLOB_VERSION {
        return None;
    }
    if blob[5] != 0 || blob[10] != 0 || blob[11] != 0 {
        return None;
    }
    let num_regs = u16::from_le_bytes(blob[6..8].try_into().unwrap()) as u32;
    let result_reg = u16::from_le_bytes(blob[8..10].try_into().unwrap()) as u32;
    let n = u32::from_le_bytes(blob[12..16].try_into().unwrap());
    if n % 4 != 0 {
        return None;
    }
    let code_bytes = (n as usize) * 4;
    let code_end = EXPR_BLOB_HEADER_SIZE + code_bytes;
    if blob.len() < code_end + 4 {
        return None;
    }
    let mut code = Vec::with_capacity(n as usize);
    for i in 0..n as usize {
        let off = EXPR_BLOB_HEADER_SIZE + i * 4;
        code.push(u32::from_le_bytes(blob[off..off + 4].try_into().unwrap()));
    }
    let s_count = u32::from_le_bytes(blob[code_end..code_end + 4].try_into().unwrap());
    let mut cur = code_end + 4;
    // Each string needs at least a 4-byte length prefix. Bound s_count before
    // reserving so a corrupt blob with a huge count can't trigger an OOM in
    // Vec::with_capacity before the per-string length checks run.
    if (s_count as usize) > blob.len().saturating_sub(cur) / 4 {
        return None;
    }
    let mut const_strings = Vec::with_capacity(s_count as usize);
    for _ in 0..s_count {
        if blob.len() < cur + 4 {
            return None;
        }
        let l = u32::from_le_bytes(blob[cur..cur + 4].try_into().unwrap()) as usize;
        cur += 4;
        if blob.len() < cur + l {
            return None;
        }
        const_strings.push(blob[cur..cur + l].to_vec());
        cur += l;
    }
    Some(DecodedExprProgram {
        num_regs,
        result_reg,
        code,
        const_strings,
    })
}

// ---------------------------------------------------------------------------
// Build a single plan (pre or post exchange)
// ---------------------------------------------------------------------------

struct PlanBuildResult {
    vm: Box<VmHandle>,
    num_regs: u32,
    in_reg: i32,
    out_reg: i32,
    ext_trace_regs: Vec<(u16, i64)>,
    source_reg_map: HashMap<i64, i32>,
    // (exchange-input node id → seed register) for each exchange input this plan
    // was built with. Lets `compile_view` wire each side's relayed batch to the
    // correct post-phase register.
    exchange_input_regs: Vec<(i32, i32)>,
    // Scratch dirs created for this (successful) plan, kept alive by `vm`'s
    // owned tables. Used by `compile_view` to clean up if a *sibling* plan
    // (pre/post split) later fails after this one already succeeded.
    scratch_dirs: Vec<String>,
}

/// Compile a circuit for a single view.
///
/// # Safety
/// All table handles must be valid pointers or null.
#[allow(clippy::too_many_arguments)]
pub(crate) unsafe fn compile_view(
    view_id: u64,
    sys_nodes: *mut Table,
    sys_edges: *mut Table,
    sys_node_cols: *mut Table,
    view_dir: &str,
    view_table_id: u32,
    view_schema: &SchemaDescriptor,
    ext_tables: &[ExternalTable],
) -> Result<CompileOutput, CompileError> {
    let mut loaded =
        load_circuit(sys_nodes, sys_edges, sys_node_cols, view_id, *view_schema).ok_or(CompileError::LoadFailed)?;
    if loaded.nodes.is_empty() {
        return Err(CompileError::EmptyCircuit);
    }
    topo_sort(&mut loaded)?;

    let ann = annotate(&loaded, ext_tables);

    let mut owned_expr_progs_for_rw: Vec<Box<LogicalProgram>> = Vec::new();
    let mut rw = Rewrites {
        skip_nodes: HashSet::new(),
        fold_finalize: HashMap::new(),
        folded_maps: HashMap::new(),
    };
    opt_distinct(&loaded, &ann, &mut rw);
    opt_fold_reduce_map(&loaded, &mut rw, &mut owned_expr_progs_for_rw);

    let exchange_nids: Vec<i32> = loaded
        .ordered
        .iter()
        .copied()
        .filter(|&nid| matches!(loaded.nodes.get(&nid), Some(gnitz_wire::OpNode::ExchangeShard { .. })))
        .collect();

    // Helper: the single source table a sub-plan scans (empty/ambiguous → 0).
    let single_source = |srm: &HashMap<i64, i32>| -> i64 {
        if srm.len() == 1 {
            *srm.keys().next().unwrap()
        } else {
            0
        }
    };

    match exchange_nids.len() {
        0 => {
            let ordered = loaded.ordered.clone();
            let plan = build_plan(
                &loaded,
                &rw,
                &ordered,
                ext_tables,
                view_dir,
                view_table_id,
                view_id,
                None,
                &[],
                owned_expr_progs_for_rw,
            )
            .ok_or(CompileError::NoExchangeBuildFailed)?;

            let source_reg_map = source_reg_map_u16(&plan.source_reg_map);

            Ok(CompileOutput {
                pre: SubPlan {
                    vm: plan.vm,
                    num_regs: plan.num_regs,
                    in_reg: plan.in_reg as u16,
                    out_reg: plan.out_reg as u16,
                    source_reg_map,
                    ext_trace_regs: plan.ext_trace_regs,
                },
                post: None,
                exchange_in_schema: None,
                co_partitioned: ann.co_partitioned,
                side_a_source_id: 0,
                side_b: None,
            })
        }
        1 => {
            let ex_nid = exchange_nids[0];
            let exchange_input_nid = exchange_input_node(&loaded, ex_nid);

            let mut pre_ordered = Vec::new();
            let mut post_ordered = Vec::new();
            let mut found_exchange = false;
            for &nid in &loaded.ordered {
                if nid == ex_nid {
                    found_exchange = true;
                    continue;
                }
                if found_exchange {
                    post_ordered.push(nid);
                } else {
                    pre_ordered.push(nid);
                }
            }

            let pre_nids: HashSet<i32> = pre_ordered.iter().copied().collect();
            let (rw_pre, pre_progs, rw_post, post_progs) = split_fold_programs(rw, owned_expr_progs_for_rw, &pre_nids);

            let pre = build_plan(
                &loaded,
                &rw_pre,
                &pre_ordered,
                ext_tables,
                view_dir,
                view_table_id,
                view_id,
                exchange_input_nid,
                &[],
                pre_progs,
            )
            .ok_or(CompileError::PlanBuildFailed)?;

            if pre.out_reg < 0 || pre.out_reg as usize >= pre.vm.program.reg_meta.len() {
                for d in &pre.scratch_dirs {
                    let _ = std::fs::remove_dir_all(d);
                }
                return Err(CompileError::OutRegOutOfBounds);
            }
            let exchange_schema = pre.vm.program.reg_meta[pre.out_reg as usize].schema;
            let side_a_source_id = single_source(&pre.source_reg_map);

            let post = match build_plan(
                &loaded,
                &rw_post,
                &post_ordered,
                ext_tables,
                view_dir,
                view_table_id,
                view_id,
                None,
                &[(ex_nid, exchange_schema)],
                post_progs,
            ) {
                Some(p) => p,
                None => {
                    for d in &pre.scratch_dirs {
                        let _ = std::fs::remove_dir_all(d);
                    }
                    return Err(CompileError::PlanBuildFailed);
                }
            };

            let source_reg_map = source_reg_map_u16(&pre.source_reg_map);

            Ok(CompileOutput {
                pre: SubPlan {
                    vm: pre.vm,
                    num_regs: pre.num_regs,
                    in_reg: pre.in_reg as u16,
                    out_reg: pre.out_reg as u16,
                    source_reg_map,
                    ext_trace_regs: pre.ext_trace_regs,
                },
                post: Some(SubPlan {
                    vm: post.vm,
                    num_regs: post.num_regs,
                    in_reg: post.in_reg as u16,
                    out_reg: post.out_reg as u16,
                    source_reg_map: HashMap::new(),
                    ext_trace_regs: post.ext_trace_regs,
                }),
                exchange_in_schema: Some(exchange_schema),
                co_partitioned: ann.co_partitioned,
                side_a_source_id,
                side_b: None,
            })
        }
        2 => {
            // Binary set-op: two independent HashRow→ExchangeShard sub-pipelines
            // meeting at a combine (Union / SemiJoin / AntiJoin). Carve each
            // side out by the ancestors of its exchange input; everything else
            // (the combine + sink, reading both relayed batches) is the post
            // phase.
            let ea = exchange_nids[0];
            let eb = exchange_nids[1];
            let ea_in = exchange_input_node(&loaded, ea).ok_or(CompileError::MissingExchangeInput)?;
            let eb_in = exchange_input_node(&loaded, eb).ok_or(CompileError::MissingExchangeInput)?;

            let side_a_set = ancestors_inclusive(&loaded, ea_in);
            let side_b_set = ancestors_inclusive(&loaded, eb_in);

            let side_a_ordered: Vec<i32> = loaded
                .ordered
                .iter()
                .copied()
                .filter(|n| side_a_set.contains(n))
                .collect();
            let side_b_ordered: Vec<i32> = loaded
                .ordered
                .iter()
                .copied()
                .filter(|n| side_b_set.contains(n))
                .collect();
            let post_ordered: Vec<i32> = loaded
                .ordered
                .iter()
                .copied()
                .filter(|n| !side_a_set.contains(n) && !side_b_set.contains(n) && *n != ea && *n != eb)
                .collect();

            let rw_a = phase_rewrites(&rw, &side_a_set);
            let rw_b = phase_rewrites(&rw, &side_b_set);
            let post_nids: HashSet<i32> = post_ordered.iter().copied().collect();
            let rw_post = phase_rewrites(&rw, &post_nids);

            let cleanup = |dirs: &[String]| {
                for d in dirs {
                    let _ = std::fs::remove_dir_all(d);
                }
            };

            let side_a = build_plan(
                &loaded,
                &rw_a,
                &side_a_ordered,
                ext_tables,
                view_dir,
                view_table_id,
                view_id,
                Some(ea_in),
                &[],
                Vec::new(),
            )
            .ok_or(CompileError::PlanBuildFailed)?;
            if side_a.out_reg < 0 || side_a.out_reg as usize >= side_a.vm.program.reg_meta.len() {
                cleanup(&side_a.scratch_dirs);
                return Err(CompileError::OutRegOutOfBounds);
            }
            let schema_a = side_a.vm.program.reg_meta[side_a.out_reg as usize].schema;
            let side_a_source_id = single_source(&side_a.source_reg_map);

            let side_b = match build_plan(
                &loaded,
                &rw_b,
                &side_b_ordered,
                ext_tables,
                view_dir,
                view_table_id,
                view_id,
                Some(eb_in),
                &[],
                Vec::new(),
            ) {
                Some(p) => p,
                None => {
                    cleanup(&side_a.scratch_dirs);
                    return Err(CompileError::PlanBuildFailed);
                }
            };
            if side_b.out_reg < 0 || side_b.out_reg as usize >= side_b.vm.program.reg_meta.len() {
                cleanup(&side_a.scratch_dirs);
                cleanup(&side_b.scratch_dirs);
                return Err(CompileError::OutRegOutOfBounds);
            }
            let schema_b = side_b.vm.program.reg_meta[side_b.out_reg as usize].schema;
            let side_b_source_id = single_source(&side_b.source_reg_map);

            let post = match build_plan(
                &loaded,
                &rw_post,
                &post_ordered,
                ext_tables,
                view_dir,
                view_table_id,
                view_id,
                None,
                &[(ea, schema_a), (eb, schema_b)],
                Vec::new(),
            ) {
                Some(p) => p,
                None => {
                    cleanup(&side_a.scratch_dirs);
                    cleanup(&side_b.scratch_dirs);
                    return Err(CompileError::PlanBuildFailed);
                }
            };

            // Resolve each side's seed register in the post plan. build_plan
            // pushes one entry per exchange input, so both lookups must hit; a
            // miss is a compile bug — panic rather than silently seed register 0
            // (the delta reg) and corrupt the combine's input.
            let seed_of = |nid: i32| -> u16 {
                post.exchange_input_regs
                    .iter()
                    .find(|&&(n, _)| n == nid)
                    .map(|&(_, r)| r as u16)
                    .expect("post plan must allocate a seed register for each exchange input")
            };
            let post_seed_a = seed_of(ea);
            let post_seed_b = seed_of(eb);

            let source_reg_map_a = source_reg_map_u16(&side_a.source_reg_map);
            let source_reg_map_b = source_reg_map_u16(&side_b.source_reg_map);

            Ok(CompileOutput {
                pre: SubPlan {
                    vm: side_a.vm,
                    num_regs: side_a.num_regs,
                    in_reg: side_a.in_reg as u16,
                    out_reg: side_a.out_reg as u16,
                    source_reg_map: source_reg_map_a,
                    ext_trace_regs: side_a.ext_trace_regs,
                },
                post: Some(SubPlan {
                    vm: post.vm,
                    num_regs: post.num_regs,
                    in_reg: post_seed_a,
                    out_reg: post.out_reg as u16,
                    source_reg_map: HashMap::new(),
                    ext_trace_regs: post.ext_trace_regs,
                }),
                exchange_in_schema: Some(schema_a),
                co_partitioned: ann.co_partitioned,
                side_a_source_id,
                side_b: Some(SideBPlan {
                    plan: SubPlan {
                        vm: side_b.vm,
                        num_regs: side_b.num_regs,
                        in_reg: side_b.in_reg as u16,
                        out_reg: side_b.out_reg as u16,
                        source_reg_map: source_reg_map_b,
                        ext_trace_regs: side_b.ext_trace_regs,
                    },
                    exchange_schema: schema_b,
                    post_seed_reg: post_seed_b,
                    source_id: side_b_source_id,
                }),
            })
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
            ordered: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            consumers: HashMap::new(),
            gather_reduce_cols: HashMap::new(),
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
            ordered: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            consumers: HashMap::new(),
            gather_reduce_cols: HashMap::new(),
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
        let ext = vec![ExternalTable {
            table_id: 7,
            schema: compound,
        }];
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
        let ext1 = vec![ExternalTable {
            table_id: 9,
            schema: single,
        }];
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
        let ext_pp = vec![
            ExternalTable {
                table_id: 7,
                schema: base(),
            },
            ExternalTable {
                table_id: 8,
                schema: base(),
            },
        ];
        let co = compute_co_partitioned(&join_on_payload(), &ext_pp);
        assert!(
            !co.contains(&7) && !co.contains(&8),
            "two partitioned sides on a non-PK key both go through the exchange"
        );

        // partitioned fact ⋈ REPLICATED dim: BOTH skip — the dim because it is
        // replicated, the fact because its join partner is replicated (it stays in
        // its own PK partitioning and joins the full local dim copy).
        let ext_pr = vec![
            ExternalTable {
                table_id: 7,
                schema: replicated,
            },
            ExternalTable {
                table_id: 8,
                schema: base(),
            },
        ];
        let co = compute_co_partitioned(&join_on_payload(), &ext_pr);
        assert!(co.contains(&7), "a replicated source always skips its exchange");
        assert!(
            co.contains(&8),
            "a partitioned fact skips when its partner is replicated"
        );

        // replicated ⋈ replicated: both skip (output is replicated; single-sourced on read).
        let ext_rr = vec![
            ExternalTable {
                table_id: 7,
                schema: replicated,
            },
            ExternalTable {
                table_id: 8,
                schema: replicated,
            },
        ];
        let co = compute_co_partitioned(&join_on_payload(), &ext_rr);
        assert!(
            co.contains(&7) && co.contains(&8),
            "replicated ⋈ replicated: both sides skip"
        );

        // A replicated source skips even with a promoted (non-zero tc) key: the
        // write broadcast already placed its full trace on every worker, so the
        // tc-promotion exchange gate (which blocks a partitioned source) does not apply.
        let ext_r = vec![ExternalTable {
            table_id: 7,
            schema: replicated,
        }];
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
        let joined = merge_schemas_for_join(&left, &right, JoinNullFill::None);
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
        let joined = merge_schemas_for_join(&left, &right, JoinNullFill::RightNullable);
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
        let joined = merge_schemas_for_join(&left, &right, JoinNullFill::None);
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
        let joined_single = merge_schemas_for_join(&left_single, &right, JoinNullFill::None);
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
        // U64 folds into the general rule (the source type *is* U64); SUM over
        // U64 still widens to I64.
        assert_eq!(agg_output_type(AggOp::Min, TypeCode::U64), type_code::U64);
        assert_eq!(agg_output_type(AggOp::Max, TypeCode::U64), type_code::U64);
        assert_eq!(agg_output_type(AggOp::Sum, TypeCode::U64), type_code::I64);
        // Non-fixed-int sources (STRING / 16-byte) keep the 8-byte I64 slot: the
        // accumulator holds an 8-byte compare key, not a value of the source
        // type. SQL rejects these for MIN/MAX, but the low-level circuit API
        // allows MAX(STRING), so the I64 fallback must hold.
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
        let copy = |src_col: u32, out: u32| LogicalInstr::CopyCol { src_col, out, tc: 9 };
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
        let out = build_reduce_output_schema(&input, &[1], &aggs);
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
        let out = build_reduce_output_schema(&input, &[1, 0], &aggs);
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
        let out = build_reduce_output_schema(&input, &[0], &aggs);
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
        let out = build_reduce_output_schema(&input, &[1], &aggs);
        // Synthetic PK (STRING group col) → [U128_hash, STRING_group, I64_count]
        assert_eq!(out.num_columns(), 3);
        assert_eq!(out.columns[0].type_code, type_code::U128);
        assert_eq!(out.columns[1].type_code, type_code::STRING);
        assert_eq!(out.columns[2].type_code, type_code::I64);
    }

    #[test]
    fn test_split_fold_programs_routes_to_pre() {
        use crate::expr::LogicalInstr;
        let prog = LogicalProgram::new(vec![LogicalInstr::LoadConst { dst: 0, val: 0 }], 1, 0, Vec::new());
        let progs: Vec<Box<LogicalProgram>> = vec![Box::new(prog)];

        let mut fold_finalize = HashMap::new();
        fold_finalize.insert(1i32, 0usize);

        let mut folded_maps = HashMap::new();
        folded_maps.insert(2i32, 1i32);

        let rw = Rewrites {
            skip_nodes: HashSet::new(),
            fold_finalize,
            folded_maps,
        };

        let mut pre_nids = HashSet::new();
        pre_nids.insert(1i32);

        let (rw_pre, pre_progs, rw_post, post_progs) = split_fold_programs(rw, progs, &pre_nids);

        assert_eq!(pre_progs.len(), 1);
        assert_eq!(post_progs.len(), 0);
        assert_eq!(rw_pre.fold_finalize.get(&1), Some(&0usize));
        assert!(!rw_post.fold_finalize.contains_key(&1));
        assert!(rw_pre.folded_maps.contains_key(&2));
        assert!(rw_post.folded_maps.contains_key(&2));
    }

    #[test]
    fn test_build_plan_returns_none_when_child_table_fails() {
        // Circuit: SCAN(0) → DISTINCT(1) → INTEGRATE(2)
        // An invalid view_dir forces create_child_table to fail inside emit_node.
        let mut loaded = LoadedCircuit {
            out_schema: SchemaDescriptor::default(),
            nodes: {
                let mut m = HashMap::new();
                m.insert(0, gnitz_wire::OpNode::ScanDelta(99));
                m.insert(1, gnitz_wire::OpNode::Distinct);
                m.insert(2, gnitz_wire::OpNode::IntegrateSink);
                m
            },
            edges: vec![(0, 1, 0), (1, 2, 0)],
            ordered: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            consumers: HashMap::new(),
            gather_reduce_cols: HashMap::new(),
        };
        topo_sort(&mut loaded).unwrap();

        // Provide an external table so ScanDelta finds its schema and sets source_reg_map.
        let in_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        let ext_tables = [ExternalTable {
            table_id: 99,
            schema: in_schema,
        }];
        let rw = Rewrites {
            skip_nodes: HashSet::new(),
            fold_finalize: HashMap::new(),
            folded_maps: HashMap::new(),
        };

        let ordered = loaded.ordered.clone();
        let result = build_plan(
            &loaded,
            &rw,
            &ordered,
            &ext_tables,
            "/nonexistent_gnitz_test_path_xyz_abc",
            0,
            99,
            None,
            &[],
            vec![],
        );
        assert!(
            result.is_none(),
            "build_plan must return None when child table creation fails"
        );
    }

    #[test]
    fn test_decode_expr_blob_rejects_huge_s_count() {
        // Minimal valid header (16 bytes): [0..4] magic, [4] version, [5] 0,
        // [6..8] num_regs, [8..10] result_reg, [10]=0, [11]=0, [12..16] n=0.
        // s_count = u32::MAX with no string bytes must return None, not OOM.
        let mut header = [0u8; 16];
        header[0..4].copy_from_slice(&EXPR_BLOB_MAGIC.to_le_bytes());
        header[4] = EXPR_BLOB_VERSION;
        // n = 0 at [12..16]
        let mut b = header.to_vec();
        b.extend_from_slice(&u32::MAX.to_le_bytes()); // s_count
        assert!(decode_expr_blob(&b).is_none(), "huge s_count must be rejected");

        // s_count = 1 but no string length prefix bytes remaining → None.
        let mut b2 = header.to_vec();
        b2.extend_from_slice(&1u32.to_le_bytes());
        assert!(
            decode_expr_blob(&b2).is_none(),
            "s_count with too few bytes must be rejected"
        );

        // Sanity: s_count = 0 with a valid header decodes successfully.
        let mut b3 = header.to_vec();
        b3.extend_from_slice(&0u32.to_le_bytes());
        assert!(decode_expr_blob(&b3).is_some(), "valid empty program must decode");
    }

    #[test]
    fn test_build_plan_register_overflow_rejected() {
        // A circuit producing > u16::MAX registers must compile to None rather
        // than wrap the u16 cast and panic in ProgramBuilder::build.
        let n = u16::MAX as i32 + 1; // 65536 nodes → 65536 registers
        let mut nodes = HashMap::new();
        for nid in 0..n {
            nodes.insert(nid, gnitz_wire::OpNode::ClearDeltas);
        }
        let loaded = LoadedCircuit {
            out_schema: SchemaDescriptor::default(),
            nodes,
            edges: Vec::new(),
            ordered: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            consumers: HashMap::new(),
            gather_reduce_cols: HashMap::new(),
        };
        let ordered: Vec<i32> = (0..n).collect();
        let result = build_plan(&loaded, &empty_rw(), &ordered, &[], "", 0, 1, None, &[], vec![]);
        assert!(
            result.is_none(),
            "build_plan must return None when register count exceeds u16::MAX"
        );
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
        let ext = [
            ExternalTable { table_id: 10, schema },
            ExternalTable { table_id: 20, schema },
        ];
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &empty_rw(), &ordered, &ext, "", 0, 1, Some(2), &[], vec![]);
        assert!(
            result.is_some(),
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
        let ext = [ExternalTable {
            table_id: 99,
            schema: in_schema,
        }];
        let ordered = loaded.ordered.clone();
        let result = build_plan(
            &loaded,
            &empty_rw(),
            &ordered,
            &ext,
            "/nonexistent_gnitz_test_path_integrate_trace",
            0,
            99,
            None,
            &[],
            vec![],
        );
        assert!(
            result.is_none(),
            "IntegrateTrace child-table failure must fail the compile"
        );
    }

    // ── Item 6: GATHER_REDUCE col_idx ───────────────────────────────────────

    #[test]
    fn test_build_gather_agg_descs_col_idx_is_agg_column() {
        // Partial schema = group key (col 0, U64) + one SUM aggregate (col 1, I64).
        // The aggregate descriptor's col_idx must point at the aggregate column
        // (1), not the PK (0).
        let partial = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let descs = build_gather_agg_descs(&partial, &[(AggOp::Sum as u64, 0)]);
        assert_eq!(descs.len(), 1);
        assert_eq!(
            descs[0].col_idx, 1,
            "agg col_idx must be the aggregate column, not the PK"
        );
        assert_eq!(descs[0].agg_op, AggOp::Sum);
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
        let ext = [ExternalTable {
            table_id: 99,
            schema: in_schema,
        }];
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &empty_rw(), &ordered, &ext, "", 0, 99, None, &[], vec![]);
        assert!(result.is_none(), "type-mismatched sink schema must be rejected");
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
        let ext = [ExternalTable {
            table_id: 99,
            schema: in_schema,
        }];
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &empty_rw(), &ordered, &ext, "", 0, 99, None, &[], vec![]);
        assert!(result.is_none(), "corrupt Filter blob must abort compilation");
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
        let ext = [ExternalTable {
            table_id: 99,
            schema: in_schema,
        }];
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &empty_rw(), &ordered, &ext, "", 0, 99, None, &[], vec![]);
        assert!(result.is_none(), "corrupt Map blob must abort compilation");
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
            let node_schema = reindex_output_schema(&in_schema, &[1u16], &[]);
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
        let out = reindex_output_schema(&in_schema, &[1u16, 2u16], &[]);
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
        let out = reindex_output_schema(&in_schema, &[1u16, 2u16], &[type_code::I64, 0]);
        assert_eq!(out.columns[0].type_code, type_code::I64, "slot0 carried T = I64");
        assert_eq!(out.columns[1].type_code, type_code::I64, "slot1 self-derives I64");
        assert_eq!(out.pk_stride(), 8 + 8, "both slots 8 bytes after promotion");
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
        loaded.out_schema = reindex_output_schema(&in_schema, &[0u16, 1u16], &[]);
        let ext = [ExternalTable {
            table_id: 99,
            schema: in_schema,
        }];
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &empty_rw(), &ordered, &ext, "", 0, 99, None, &[], vec![]);
        assert!(
            result.is_some(),
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
        let ext = [ExternalTable {
            table_id: 99,
            schema: in_schema,
        }];
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &empty_rw(), &ordered, &ext, "", 0, 99, None, &[], vec![]);
        assert!(result.is_none(), "reindex list > MAX_PK_COLUMNS must fail the compile");
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
        let ext = [ExternalTable { table_id: 10, schema }];
        let ordered = loaded.ordered.clone();
        // /nonexistent_path forces create_child_table to fail.
        let result = build_plan(
            &loaded,
            &empty_rw(),
            &ordered,
            &ext,
            "/nonexistent_gnitz_scratch_cleanup_test_path",
            0,
            1,
            Some(2),
            &[],
            vec![],
        );
        assert!(result.is_none(), "IntegrateTrace failure must fail the compile");

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
            "nodes",
            nodes_schema,
            0,
            256 * 1024,
            Persistence::Ephemeral,
        )
        .unwrap();
        {
            let mut bb = BatchBuilder::new(nodes_schema);
            bb.begin_row(pk(1), 1);
            bb.put_u64(1); // node_id
            bb.put_u64(9999); // opcode — unknown → decode_op_node Err
            bb.put_null(); // source_table
            bb.put_null(); // reindex_col
            bb.put_null(); // expr_program
            bb.end_row();
            nodes_tab.ingest_owned_batch(bb.finish()).unwrap();
        }
        let mut edges_tab = Table::new(
            &format!("{dir}/edges"),
            "edges",
            edges_schema,
            0,
            256 * 1024,
            Persistence::Ephemeral,
        )
        .unwrap();
        let _ = &mut edges_tab; // empty
        let mut cols_tab = Table::new(
            &format!("{dir}/cols"),
            "cols",
            cols_schema,
            0,
            256 * 1024,
            Persistence::Ephemeral,
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
            "nodes",
            nodes_schema,
            0,
            256 * 1024,
            Persistence::Ephemeral,
        )
        .unwrap();
        {
            let mut bb = BatchBuilder::new(nodes_schema);
            // node 0: ScanDelta(source 99)
            bb.begin_row(pk(0), 1);
            bb.put_u64(0);
            bb.put_u64(gnitz_wire::OPCODE_SCAN_DELTA);
            bb.put_u64(99); // source_table
            bb.put_null();
            bb.put_null();
            bb.end_row();
            // node 1: IntegrateSink
            bb.begin_row(pk(1), 1);
            bb.put_u64(1);
            bb.put_u64(gnitz_wire::OPCODE_INTEGRATE);
            bb.put_null();
            bb.put_null();
            bb.put_null();
            bb.end_row();
            nodes_tab.ingest_owned_batch(bb.finish()).unwrap();
        }
        let mut edges_tab = Table::new(
            &format!("{dir}/edges"),
            "edges",
            edges_schema,
            0,
            256 * 1024,
            Persistence::Ephemeral,
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
            "cols",
            cols_schema,
            0,
            256 * 1024,
            Persistence::Ephemeral,
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
    #[should_panic(expected = "join output schema exceeds")]
    fn test_merge_schemas_for_join_column_overflow() {
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
        merge_schemas_for_join(&make(half), &make(half), JoinNullFill::None);
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

    fn make_loaded(nodes: HashMap<i32, gnitz_wire::OpNode>, edges: Vec<(i32, i32, i32)>) -> LoadedCircuit {
        let mut lc = LoadedCircuit {
            out_schema: SchemaDescriptor::default(),
            nodes,
            edges,
            ordered: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            consumers: HashMap::new(),
            gather_reduce_cols: HashMap::new(),
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

    fn empty_rw() -> Rewrites {
        Rewrites {
            skip_nodes: HashSet::new(),
            fold_finalize: HashMap::new(),
            folded_maps: HashMap::new(),
        }
    }

    // ── §5: destructive-register ordering invariant ─────────────────────────
    //
    // Union/Distinct/Delay (and the trace-absent AntiJoin(DeltaTrace)) empty
    // their input register in place. When that register fans out to other
    // consumers, the destructive op must be scheduled LAST among them, or the
    // co-readers see an emptied batch. build_plan rejects violations (returns
    // None) in every build profile, release included.

    /// Build the INTERSECT/EXCEPT fan-out shape: ScanDelta(10)'s register fans
    /// into both a destructive `Distinct` and a non-destructive `Filter`
    /// co-reader (standing in for integrate_trace); the Distinct feeds the
    /// IntegrateSink at node 3. Caller picks `distinct_id`/`filter_id` — Kahn's
    /// ascending tie-break schedules the lower id first, so the ids decide which
    /// consumer the scheduler runs first.
    fn make_dtor_fanout(distinct_id: i32, filter_id: i32) -> LoadedCircuit {
        let mut nodes = HashMap::new();
        nodes.insert(0, gnitz_wire::OpNode::ScanDelta(10));
        nodes.insert(distinct_id, gnitz_wire::OpNode::Distinct);
        nodes.insert(filter_id, gnitz_wire::OpNode::Filter(None));
        nodes.insert(3, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![
            (0, distinct_id, PORT_IN),
            (0, filter_id, PORT_IN),
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

        let ext = [ExternalTable {
            table_id: 10,
            schema: two_col_schema(),
        }];
        let ordered = loaded.ordered.clone();
        let result = build_plan(
            &loaded,
            &empty_rw(),
            &ordered,
            &ext,
            &view_dir,
            0,
            1,
            Some(3),
            &[],
            vec![],
        );
        let _ = std::fs::remove_dir_all(&view_dir);
        assert!(
            result.is_some(),
            "legitimate destructive fan-out must compile without tripping the ordering assert"
        );
    }

    #[test]
    fn test_destructive_fanout_bad_ordering_rejected() {
        // Distinct id 1 < Filter id 2, so the ascending tie-break schedules the
        // destructive op FIRST — it would empty ScanDelta's register before the
        // Filter reads it. build_plan must reject this rather than emit it.
        let loaded = make_dtor_fanout(1, 2);
        let ext = [ExternalTable {
            table_id: 10,
            schema: two_col_schema(),
        }];
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &empty_rw(), &ordered, &ext, "", 0, 1, Some(3), &[], vec![]);
        assert!(
            result.is_none(),
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
        let mut rw = empty_rw();
        rw.skip_nodes.insert(1); // Distinct elided by opt_distinct

        let ext = [ExternalTable {
            table_id: 10,
            schema: two_col_schema(),
        }];
        let ordered = loaded.ordered.clone();
        let result = build_plan(&loaded, &rw, &ordered, &ext, &view_dir, 0, 1, Some(3), &[], vec![]);
        let _ = std::fs::remove_dir_all(&view_dir);
        assert!(
            result.is_some(),
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

        let ext = [
            ExternalTable { table_id: 10, schema },
            ExternalTable { table_id: 20, schema },
        ];
        let ordered = loaded.ordered.clone();
        let result = build_plan(
            &loaded,
            &empty_rw(),
            &ordered,
            &ext,
            "",
            0,
            1,
            Some(2), // bypass out_schema mismatch check; sink_reg already set by IntegrateSink
            &[],
            vec![],
        );
        let plan = result.expect("build_plan must succeed for this circuit");

        // The trace-side ScanTrace (node 1) uses reg_id as the trace register;
        // no extra delta register is allocated for it.  The minimum register
        // count is: one per node (4) + zero extras from ScanTrace on trace side.
        // (There are no Distinct/Reduce/Delay nodes adding extras.)
        assert!(
            plan.num_regs == 4,
            "expected exactly 4 regs (one per node, no extra for trace-side ScanTrace), got {}",
            plan.num_regs
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
        // ScanTrace feeds Union on PORT_IN_B (=1), but Union is not in
        // {Join, AntiJoin, SemiJoin, SeekTrace}, so is_join_trace_side = false →
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
        let ext = [
            ExternalTable { table_id: 10, schema },
            ExternalTable { table_id: 20, schema },
        ];
        let ordered = loaded.ordered.clone();
        let result = build_plan(
            &loaded,
            &empty_rw(),
            &ordered,
            &ext,
            "",
            0,
            1,
            Some(2), // bypass out_schema mismatch check
            &[],
            vec![],
        );
        let plan = result.expect("build_plan must succeed");

        // 4 nodes → base regs 0-3, plus 1 extra delta reg for ScanTrace.
        assert!(
            plan.num_regs == 5,
            "expected 5 regs (4 base + 1 extra delta for non-join-side ScanTrace), got {}",
            plan.num_regs
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

        let ext = [
            ExternalTable { table_id: 10, schema },
            ExternalTable { table_id: 20, schema },
        ];
        let ordered = loaded.ordered.clone();
        let plan = build_plan(
            &loaded,
            &empty_rw(),
            &ordered,
            &ext,
            "",
            0,
            1,
            Some(2), // bypass out_schema mismatch; sink_reg set by IntegrateSink
            &[],
            vec![],
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

        let ext = [
            ExternalTable { table_id: 10, schema },
            ExternalTable { table_id: 20, schema },
        ];
        let ordered = loaded.ordered.clone();
        let result = build_plan(
            &loaded,
            &empty_rw(),
            &ordered,
            &ext,
            &view_dir,
            0,
            1,
            Some(2), // bypass out_schema mismatch; sink_reg set by IntegrateSink
            &[],
            vec![],
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
        use gnitz_wire::{AggKind, JoinKind, MapKind, OpNode};
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

    // ── Finding 1: ExchangeGather must forward its output to exchange-input reg ──

    /// ExchangeGather is a logical passthrough: at runtime the exchange mechanism
    /// injects gathered data into the exchange-input register (`plan.in_reg`).
    /// GatherReduce reads from ExchangeGather's output, so ExchangeGather's output
    /// register must alias the exchange-input register — not a separate unwritten one.
    #[test]
    fn test_exchange_gather_routes_to_exchange_input_register() {
        let schema = two_col_schema();
        let dir = tempfile::tempdir().unwrap();

        // Post-exchange circuit: ExchangeShard(0) → ExchangeGather(1) → GatherReduce(2).
        // We build only the post-plan: post_ordered = [1, 2], exchange_input = Some((0, schema)).
        let loaded = {
            let mut nodes = HashMap::new();
            nodes.insert(0, gnitz_wire::OpNode::ExchangeShard { shard_cols: vec![] });
            nodes.insert(1, gnitz_wire::OpNode::ExchangeGather);
            nodes.insert(2, gnitz_wire::OpNode::GatherReduce);
            make_loaded(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)])
        };

        let post_ordered = vec![1, 2];
        let plan = build_plan(
            &loaded,
            &empty_rw(),
            &post_ordered,
            &[],
            dir.path().to_str().unwrap(),
            0,
            1,
            Some(2), // GatherReduce(2) is the output node; skips schema mismatch check
            &[(0, schema)],
            vec![],
        )
        .expect("post-plan must compile");

        let gather_in_reg = plan
            .vm
            .program
            .instructions
            .iter()
            .find_map(|instr| {
                if let crate::query::vm::Instr::GatherReduce { in_reg, .. } = instr {
                    Some(*in_reg)
                } else {
                    None
                }
            })
            .expect("post-plan must contain a GatherReduce instruction");

        assert_eq!(
            gather_in_reg as i32, plan.in_reg,
            "GatherReduce reads from register {} but exchange data arrives at register {}: \
             ExchangeGather did not forward its output to the exchange-input register",
            gather_in_reg, plan.in_reg
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
}
