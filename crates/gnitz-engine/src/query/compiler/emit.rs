//! Instruction emission: per-node `emit_*`, the expression/scalar-func
//! constructors, and `build_plan` (one plan, pre or post exchange).

use super::*;
use crate::query::vm::{reads_reg, Instr, IntegrateAvi, ReindexOperand};

// ---------------------------------------------------------------------------
// Expression + scalar function construction helpers
// ---------------------------------------------------------------------------

/// Log an expr-program reject and name the failed guard in the compile error.
/// A rejected program is bad client input — a bad opcode / register / const
/// index, or an out-of-range / PK-routed column.
fn expr_reject(what: &'static str) -> impl Fn(ExprValidateErr) -> CompileError {
    move |e| {
        gnitz_info!("expr program rejected ({what}): {e:?}");
        CompileError::Rejected(what)
    }
}

/// True iff any raw wire column index in `cols` is out of range for `schema`.
/// Bound is `num_columns()`, not `MAX_COLUMNS`, so the silent `[num_columns, 65)`
/// zeroed-slot zone is rejected too. The recurring guard for a client-controlled
/// column list that indexes (or slices) the fixed `[_; 65]` schema array.
pub(super) fn oob_cols(cols: &[u16], schema: &SchemaDescriptor) -> bool {
    cols.iter().any(|&c| c as usize >= schema.num_columns())
}

/// The schema of a registered external table, or the named compile rejection —
/// a circuit scanning an unknown table is corrupt (the planner registers every
/// source before shipping the circuit).
pub(super) fn ext_schema(
    ext_tables: &ExtTables,
    tid: i64,
    what: &'static str,
) -> Result<SchemaDescriptor, CompileError> {
    ext_tables.get(&tid).copied().ok_or(CompileError::Rejected(what))
}

/// Both inputs of a `Union` share a physical layout (equal type codes, sizes, and
/// PK indices); only per-column nullability may differ. Return `a`'s schema with
/// each column's nullability OR-ed with `b`'s, so a null-carrying side forces the
/// null-aware `Generic` row comparator instead of the null-blind `FixedIntNonnull`
/// fast path (which orders by raw payload bytes and would fail to coalesce two
/// logically-NULL rows carrying non-zero bytes under the null bit).
pub(super) fn union_nullability_merge(a: &SchemaDescriptor, b: &SchemaDescriptor) -> SchemaDescriptor {
    debug_assert_eq!(a.num_columns(), b.num_columns(), "union inputs must share a layout");
    debug_assert_eq!(a.pk_indices(), b.pk_indices(), "union inputs must share a layout");
    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    for (c, col) in cols[..a.num_columns()].iter_mut().enumerate() {
        let ac = a.columns[c];
        let bc = b.columns[c];
        debug_assert_eq!(ac.type_code, bc.type_code, "union inputs must share a layout");
        *col = SchemaColumn::new(ac.type_code, ac.nullable | bc.nullable);
    }
    SchemaDescriptor::new(&cols[..a.num_columns()], a.pk_indices())
}

/// Path of a per-worker scratch directory under `view_dir`. Rank-stamped because
/// forked workers share `view_dir`; an un-stamped path would have every worker
/// open the same directory and clobber each other's shard files. Single source
/// of truth for the convention — `EmitCtx::create_child_table` derives its path
/// from it, so the rank stamp can never drift between child tables.
pub(super) fn child_scratch_dir(view_dir: &str, child_name: &str) -> String {
    format!("{}/scratch_{}_w{}", view_dir, child_name, worker_rank())
}

/// True iff `name` is one of THIS worker's rank-stamped scratch dirs — the
/// inverse of the `child_scratch_dir` convention above, kept adjacent so the
/// recovery reset sweep can never drift from the naming it must match.
pub(crate) fn is_worker_scratch_dir_name(name: &str) -> bool {
    name.starts_with("scratch_") && name.ends_with(&format!("_w{}", worker_rank()))
}

// ---------------------------------------------------------------------------
// ScratchGuard — drop-based cleanup of a failed compile's scratch directories
// ---------------------------------------------------------------------------

/// Scratch directories created via `create_child_table` during a plan build.
/// Removes every tracked directory on drop, so any failed compile path — every
/// `?` in the emit layer, and a failing sibling sub-plan in `compile_view` —
/// leaks no inodes. On success the guard is `defuse`d: the directories stay
/// alive under the VM's owned tables instead.
pub(super) struct ScratchGuard(Vec<String>);

impl ScratchGuard {
    pub fn new() -> Self {
        ScratchGuard(Vec::new())
    }
    fn track(&mut self, dir: String) {
        self.0.push(dir);
    }
    pub fn defuse(&mut self) {
        self.0.clear();
    }
}

impl Drop for ScratchGuard {
    fn drop(&mut self) {
        for d in &self.0 {
            let _ = std::fs::remove_dir_all(d);
        }
    }
}

// ---------------------------------------------------------------------------
// EmitCtx — the per-plan build state every emit arm works against
// ---------------------------------------------------------------------------

/// All state of one `build_plan` invocation: the circuit being compiled, the
/// instruction builder, register metadata, and the owned resources the finished
/// VM will keep alive. Owned by `build_plan` and threaded to the emit arms as
/// one `&mut` instead of a dozen parallel parameters.
/// The `Box`es are load-bearing (raw-pointer stability across `Vec` growth).
#[allow(clippy::vec_box)]
pub(super) struct EmitCtx<'a> {
    pub loaded: &'a LoadedCircuit,
    pub skip_nodes: &'a HashSet<i32>,
    pub ext_tables: &'a ExtTables,
    pub view_dir: &'a str,
    pub view_id: u64,
    // True iff every `ScanDelta` source of this view is a replicated base table.
    // Consumed by the `PartitionFilter` emit arm to bake the trim as a keep-all
    // identity (an all-replicated view runs correct-local on every worker).
    pub all_sources_replicated: bool,
    pub builder: ProgramBuilder,
    pub out_reg_of: HashMap<i32, i32>,
    pub reg_meta: Vec<RegisterMeta>,
    pub owned_tables: Vec<Box<Table>>,
    pub owned_funcs: Vec<Box<ScalarFunc>>,
    pub owned_trace_regs: Vec<(u16, usize)>,
    pub ext_trace_regs: Vec<(u16, i64)>,
    pub source_reg_map: HashMap<i64, i32>,
    pub sink_reg_id: i32,
    pub scratch: ScratchGuard,
    /// Registers of finalize-folded MAP nodes: map_nid → the finalize output
    /// register, filled by `emit_reduce` when it absorbs its sole consuming Map.
    pub folded_map_regs: HashMap<i32, i32>,
}

impl EmitCtx<'_> {
    /// Box `func`, keep it alive in `owned_funcs`, and return a raw pointer into
    /// the box. Valid for the box's lifetime — the heap `ScalarFunc` is stable
    /// across the `Vec`'s growth — which is what the VM's raw `*const ScalarFunc`
    /// handles rely on. The `Box` is load-bearing (pointer stability).
    fn push_func(&mut self, func: ScalarFunc) -> *const ScalarFunc {
        self.owned_funcs.push(Box::new(func));
        &**self.owned_funcs.last().unwrap() as *const ScalarFunc
    }

    /// Decode + validate a client filter blob against `schema`, then build its
    /// predicate `ScalarFunc`.
    fn create_expr_predicate(
        &mut self,
        dep: gnitz_wire::ExprBlob,
        schema: &SchemaDescriptor,
    ) -> Result<*const ScalarFunc, CompileError> {
        let prog = LogicalProgram::from_wire(&dep.code, dep.num_regs, dep.result_reg, dep.const_strings)
            .and_then(|p| p.validate(Some(schema), None).map(|()| p))
            .map_err(expr_reject("filter: invalid predicate program"))?;
        Ok(self.push_func(ScalarFunc::from_predicate(prog, schema)))
    }

    /// A MAP whose program is all-`CopyCol`: output payload `i` ← input column
    /// `src_indices[i]`, widening into a promoted slot when the output column is
    /// wider. The source type is derived per-column in `resolve` from `in_schema`.
    fn create_universal_projection(
        &mut self,
        src_indices: &[i32],
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
    ) -> *const ScalarFunc {
        let copies: Vec<u32> = src_indices.iter().map(|&ci| ci as u32).collect();
        let prog = LogicalProgram::copy_cols(&copies);
        self.push_func(ScalarFunc::from_map(prog, in_schema, out_schema))
    }

    /// Create a child table in a rank-stamped subdirectory of the view's
    /// directory, tracked by the scratch guard (so a later compile failure
    /// removes it).
    fn create_child_table(&mut self, child_name: &str, schema: SchemaDescriptor) -> Result<Table, CompileError> {
        let child_dir = child_scratch_dir(self.view_dir, child_name);
        // Track the path before creating so cleanup also removes a partially
        // created directory if Table::new fails.
        self.scratch.track(child_dir.clone());
        // `RederiveCheckpointed`: the ephemeral checkpoint round force-persists
        // these view operator-trace tables with generation-stamped manifests.
        // `create_child_table` is reached only from view-plan compilation, never
        // index-circuit compilation (index tables stay plain `Rederive`).
        Table::new(
            &child_dir,
            schema,
            self.view_id as u32,
            256 * 1024,
            RecoverySource::RederiveCheckpointed {
                committed: crate::foundation::worker_ctx::committed_generation(),
            },
        )
        .map_err(|_| CompileError::Rejected("child table create failed"))
    }

    /// Create a child table, keep it alive in `owned_tables`, and return a raw
    /// pointer into the box. When `trace_reg` is given, register it as an owned
    /// trace register backed by the new table (`refresh_owned_cursors` opens a
    /// cursor on it each epoch).
    fn add_owned_trace_table(
        &mut self,
        child_name: &str,
        schema: SchemaDescriptor,
        trace_reg: Option<i32>,
    ) -> Result<*mut Table, CompileError> {
        let t = self.create_child_table(child_name, schema)?;
        let idx = self.owned_tables.len();
        self.owned_tables.push(Box::new(t));
        let ptr = &*self.owned_tables[idx] as *const Table as *mut Table;
        if let Some(reg) = trace_reg {
            self.reg_meta[reg as usize] = RegisterMeta::trace(schema);
            self.owned_trace_regs.push((reg as u16, idx));
        }
        Ok(ptr)
    }

    /// Allocate a fresh delta register and return its id.
    fn push_delta_reg(&mut self, schema: SchemaDescriptor) -> i32 {
        let id = self.reg_meta.len() as i32;
        self.reg_meta.push(RegisterMeta::delta(schema));
        id
    }

    /// A plain integrate of `in_reg` into `table` (null = sink integrate).
    fn push_integrate(&mut self, in_reg: u16, table: *mut Table) {
        let table_idx = self.builder.table_idx(table);
        self.builder.push(Instr::Integrate {
            in_reg,
            table_idx,
            avi: None,
        });
    }
}

// ---------------------------------------------------------------------------
// Instruction emission — per-node handler
// ---------------------------------------------------------------------------

/// The one edge shape that makes a `ScanTrace` skippable: feeding a Join's
/// PORT_TRACE. Shared by the skip decision and the mixed-consumer assert below
/// so the two can never drift.
fn is_trace_port_edge(loaded: &LoadedCircuit, dst: i32, port: i32) -> bool {
    port == PORT_TRACE && matches!(loaded.nodes.get(&dst), Some(gnitz_wire::OpNode::Join(_)))
}

pub(super) fn is_join_trace_side(loaded: &LoadedCircuit, nid: i32) -> bool {
    // `.all()` (not `.any()`): a ScanTrace node feeding a join on PORT_TRACE
    // skips its delta-register allocation, leaving no `out_reg_of` entry. If the
    // SAME node also feeds a normal-port consumer (Filter/Map/Union on PORT_IN),
    // an `.any()` test would still skip, and the normal consumer's `in_regs`
    // lookup would miss — reading unrelated payload. The skip is only safe when
    // EVERY outgoing edge is a join trace side.
    loaded
        .outgoing
        .get(&nid)
        .map(|outs| !outs.is_empty() && outs.iter().all(|&(dst, port)| is_trace_port_edge(loaded, dst, port)))
        .unwrap_or(false)
}

/// The resolved input register of `port`, or the named compile rejection: a
/// missing input edge would otherwise silently fall back to reading node-0's
/// register — the wrong-results failure class every other emit guard exists to
/// prevent.
fn in_reg(in_regs: &HashMap<i32, i32>, port: i32, what: &'static str) -> Result<i32, CompileError> {
    in_regs.get(&port).copied().ok_or(CompileError::Rejected(what))
}

pub(super) fn emit_node(ctx: &mut EmitCtx, nid: i32, reg_id: i32) -> Result<(), CompileError> {
    let loaded = ctx.loaded;
    let in_regs = compute_in_regs(loaded, nid, &ctx.out_reg_of);
    let Some(op) = loaded.nodes.get(&nid) else {
        return Ok(());
    };

    match op {
        gnitz_wire::OpNode::ScanDelta(tid) => {
            let schema = ext_schema(ctx.ext_tables, *tid as i64, "scan-delta: unknown source table")?;
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(schema);
            ctx.source_reg_map.insert(*tid as i64, reg_id);
        }

        gnitz_wire::OpNode::ScanTrace(tid) => {
            let schema = ext_schema(ctx.ext_tables, *tid as i64, "scan-trace: unknown source table")?;
            ctx.reg_meta[reg_id as usize] = RegisterMeta::trace(schema);
            ctx.ext_trace_regs.push((reg_id as u16, *tid as i64));

            if !is_join_trace_side(loaded, nid) {
                // Overwriting out_reg_of below points this node at the
                // cursorless delta register. out_reg_of holds one register
                // per node, so a consumer reading this node's PORT_TRACE
                // would resolve to that single delta register and read an
                // empty trace — silently emitting empty output. Routing both
                // would require emitting two output registers for the node.
                // The graph builder emits trace and delta scans as separate
                // nodes, so a ScanTrace never has both a PORT_TRACE join
                // consumer and a non-join consumer; assert it rather than
                // corrupt results if that ever changes.
                assert!(
                    loaded
                        .outgoing
                        .get(&nid)
                        .is_none_or(|outs| { !outs.iter().any(|&(dst, port)| is_trace_port_edge(loaded, dst, port)) }),
                    "ScanTrace node {nid} has mixed consumers: a PORT_TRACE join consumer would \
                     misroute to the cursorless delta register"
                );
                let out_delta_id = ctx.push_delta_reg(schema);
                ctx.out_reg_of.insert(nid, out_delta_id);
                ctx.builder.push(Instr::ScanTrace {
                    trace_reg: reg_id as u16,
                    out_reg: out_delta_id as u16,
                });
            }
        }

        gnitz_wire::OpNode::Filter(blob) => {
            let in_reg = in_reg(&in_regs, PORT_IN, "filter: missing input port")?;
            let Some(blob) = blob else {
                // Absent blob = no WHERE clause. Pass-through: alias the input
                // register instead of emitting a clone-the-batch instruction.
                ctx.out_reg_of.insert(nid, in_reg);
                return Ok(());
            };
            let in_schema = ctx.reg_meta[in_reg as usize].schema;
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(in_schema);
            // A present-but-corrupt blob, or a rejected program, is catalog
            // corruption. Falling back to pass-all would silently turn a WHERE
            // into WHERE TRUE; fail the compile instead.
            let dep = gnitz_wire::decode_expr_blob(blob).ok_or(CompileError::Rejected("filter: corrupt expr blob"))?;
            let func_ptr = ctx.create_expr_predicate(dep, &in_schema)?;
            let func_idx = ctx.builder.func_idx(func_ptr);
            ctx.builder.push(Instr::Filter {
                in_reg: in_reg as u16,
                out_reg: reg_id as u16,
                func_idx,
            });
        }

        gnitz_wire::OpNode::Map(mk) => {
            // Finalize-folded MAP (absorbed into its upstream REDUCE, which
            // already emitted the finalize program into this register).
            if let Some(&fin_reg) = ctx.folded_map_regs.get(&nid) {
                ctx.out_reg_of.insert(nid, fin_reg);
                return Ok(());
            }
            let in_reg = in_reg(&in_regs, PORT_IN, "map: missing input port")?;
            let in_reg_schema = ctx.reg_meta[in_reg as usize].schema;
            match mk {
                gnitz_wire::MapKind::Expression {
                    program,
                    reindex_cols,
                    reindex_target_tcs,
                } => {
                    // A corrupt MAP blob would otherwise be skipped, leaving the
                    // output register at the default empty schema and silently
                    // producing wrong/empty downstream results. Decode + structurally
                    // validate once, with the real const pool; a corrupt blob or an
                    // invalid program fails the compile. The structural scans below
                    // (`sequential_copy_base` / `payload_copy_srcs`) never touch the
                    // const pool, so this one decode serves both them and `from_map`.
                    let dep = gnitz_wire::decode_expr_blob(program)
                        .ok_or(CompileError::Rejected("map: corrupt expr blob"))?;
                    let prog = LogicalProgram::from_wire(&dep.code, dep.num_regs, 0, dep.const_strings)
                        .map_err(expr_reject("map: invalid program"))?;
                    // Identity MAP: elide only when there is no reindex, the schemas
                    // match, and the block copy skips exactly the inherited PK region
                    // (`base == pk_count`) — the MAP carries the PK region verbatim and
                    // copies payload columns 1:1.
                    if reindex_cols.is_empty()
                        && in_reg_schema.same_physical_layout(&loaded.out_schema)
                        && prog.sequential_copy_base() == Some(in_reg_schema.pk_indices().len())
                    {
                        ctx.out_reg_of.insert(nid, in_reg);
                        return Ok(());
                    }
                    // The reindex output schema prepends `pk_n` synthetic PK
                    // columns to the kept payload columns, written into a fixed
                    // [_; MAX_COLUMNS] / [_; MAX_PK_COLUMNS] array. A hand-assembled
                    // or planner-built list could exceed those bounds, or name a
                    // column >= num_columns() (which `columns[c]` would read as a
                    // zeroed slot — a silently wrong key). Fail the compile cleanly.
                    // The byte stride needs no separate MAX_PK_BYTES check: each
                    // output PK column is reindex_output_type_code(tc).wire_stride()
                    // <= 16 bytes, so pk_n <= MAX_PK_COLUMNS (5) bounds the packed
                    // stride at 5 × 16 = 80 = MAX_PK_BYTES (ReindexPacker::new's
                    // assert is the tripwire).
                    let pk_n = reindex_cols.len();
                    if pk_n > crate::schema::MAX_PK_COLUMNS || oob_cols(reindex_cols, &in_reg_schema) {
                        return Err(CompileError::Rejected("map: reindex columns out of range"));
                    }
                    // A carried target `t` must be exactly the promotion the planner
                    // derives for a key of this source type. Rather than re-deriving
                    // the sign/width ladder by hand (and drifting from the planner),
                    // validate against the single shared rule: `t` is a value-
                    // preserving promotion of `src` iff `join_key_common_type` maps
                    // the pair `(src, t)` back to `t`. The promotion is idempotent —
                    // promoting a source against its own carried target is a no-op —
                    // so this is exactly the planner's `carried_reindex_tc` contract
                    // read back (a `#[test]` pins the idempotency). It also screens
                    // PK-ineligible targets for free, since the function only ever
                    // yields PK-eligible types. A mismatch means a corrupt/forged
                    // catalog, so abort the compile cleanly rather than panic in
                    // encode_pk_column_promoted (a narrowing target would slice
                    // scratch[src_width..target_width] with src_width > target_width).
                    // Runs after the in-bounds column check above, so columns[c] is
                    // always in range here.
                    let promotion_invalid = reindex_cols.iter().enumerate().any(|(i, &c)| {
                        let t = reindex_target_tcs.get(i).copied().unwrap_or(0);
                        t != 0 && {
                            let src = in_reg_schema.columns[c as usize].type_code;
                            gnitz_wire::join_key_common_type(src, t) != Some(t)
                        }
                    });
                    if promotion_invalid {
                        return Err(CompileError::Rejected("map: invalid reindex promotion target"));
                    }
                    let node_schema = if !reindex_cols.is_empty() {
                        // The output payload is exactly what the program copies: a
                        // reindex program is one `COPY_COL(src, out)` per kept payload
                        // column with dense outs `0..n` (the planner's
                        // `build_reindex_program`), so the kept-column list is read
                        // straight off the decoded program — the single source of
                        // truth. A program of any other shape falls back to "all input
                        // columns" (the range-join `nf_rekey` copies at an offset and
                        // lands here). An out-of-range source column means a
                        // corrupt/forged catalog — `columns[c]` would read a zeroed
                        // slot — so fail the compile.
                        let n_in = in_reg_schema.num_columns();
                        let payload_cols: Vec<u16> = match prog.payload_copy_srcs() {
                            Some(srcs) => {
                                if srcs.iter().any(|&c| c as usize >= n_in) {
                                    return Err(CompileError::Rejected("map: reindex payload column out of range"));
                                }
                                srcs.iter().map(|&c| c as u16).collect()
                            }
                            None => (0..n_in as u16).collect(),
                        };
                        if pk_n + payload_cols.len() > crate::schema::MAX_COLUMNS {
                            return Err(CompileError::Rejected("map: reindex output exceeds MAX_COLUMNS"));
                        }
                        reindex_output_schema(&in_reg_schema, reindex_cols, reindex_target_tcs, &payload_cols)
                    } else {
                        loaded.out_schema
                    };
                    // Validate the decoded program against the resolved node schema,
                    // then build its MAP `ScalarFunc` — the decode above is reused here
                    // rather than re-lowering the blob a second time.
                    prog.validate(Some(&in_reg_schema), Some(&node_schema))
                        .map_err(expr_reject("map: program/schema mismatch"))?;
                    let fp = ctx.push_func(ScalarFunc::from_map(prog, &in_reg_schema, &node_schema));
                    ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(node_schema);
                    let func_idx = ctx.builder.func_idx(fp);
                    let out_schema_idx = ctx.builder.schema_idx(node_schema);
                    // An Expression map without reindex columns is a plain
                    // columnar map — the bulk PK copy stands.
                    let reindex = if reindex_cols.is_empty() {
                        ReindexOperand::None
                    } else {
                        let cols_u32: Vec<u32> = reindex_cols.iter().map(|&c| c as u32).collect();
                        let (off, cnt) = ctx.builder.add_reindex_cols(&cols_u32, reindex_target_tcs);
                        ReindexOperand::Pack { off, cnt }
                    };
                    ctx.builder.push(Instr::Map {
                        in_reg: in_reg as u16,
                        out_reg: reg_id as u16,
                        func_idx,
                        out_schema_idx,
                        reindex,
                    });
                }

                gnitz_wire::MapKind::HashRow(proj_cols, target_tcs, branch_id) => {
                    // Keep the listed columns as payload (positions 0..k), like a
                    // Projection, but prepend a synthetic U128 PK that op_map sets
                    // to a hash of those payload columns (reindex_hash path).
                    // `target_tcs[j] != 0` promotes payload column j to that
                    // ≤8-byte integer type (cross-width set-op coercion): the node
                    // schema declares the wider slot and `create_universal_projection`
                    // widens the source into it, so both set-op sides hash one physical layout.
                    //
                    // Same trust-boundary guards as the Expression reindex arm above:
                    // an out-of-range column would read a zeroed schema slot (a
                    // silently wrong hash), and a carried target must be exactly the
                    // planner's value-preserving promotion of its source —
                    // `join_key_common_type(src, t) == Some(t)` — restricted to the
                    // ≤8-byte fixed-int domain the payload widen supports. A
                    // violation means a corrupt/forged catalog; fail the compile
                    // cleanly rather than truncate in `copy_column`.
                    if 1 + proj_cols.len() > crate::schema::MAX_COLUMNS || oob_cols(proj_cols, &in_reg_schema) {
                        return Err(CompileError::Rejected("hash-row map: columns out of range"));
                    }
                    let promotion_invalid = proj_cols.iter().enumerate().any(|(j, &c)| {
                        let t = target_tcs.get(j).copied().unwrap_or(0);
                        t != 0 && {
                            let src = in_reg_schema.columns[c as usize].type_code;
                            !gnitz_wire::is_fixed_int(t) || gnitz_wire::join_key_common_type(src, t) != Some(t)
                        }
                    });
                    if promotion_invalid {
                        return Err(CompileError::Rejected("hash-row map: invalid promotion target"));
                    }
                    let src_indices: Vec<i32> = proj_cols.iter().map(|&c| c as i32).collect();
                    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
                    cols[0] = SchemaColumn::new(type_code::U128, 0);
                    for (j, &i) in src_indices.iter().enumerate() {
                        let src = in_reg_schema.columns[i as usize];
                        let tgt = target_tcs.get(j).copied().unwrap_or(0);
                        let out_tc = if tgt != 0 { tgt } else { src.type_code };
                        // `new` re-derives size/signedness for the promoted type;
                        // keep THIS SIDE's nullability. Per-side, not the
                        // operator-merged view nullability: an INTERSECT/EXCEPT leaf
                        // is `distinct`-ed on its own before the tuple-tightening
                        // combine, so its row comparator must classify by what this
                        // side can actually emit.
                        cols[1 + j] = SchemaColumn::new(out_tc, src.nullable);
                    }
                    let node_schema = SchemaDescriptor::new(&cols[..1 + src_indices.len()], &[0]);
                    let fp = ctx.create_universal_projection(&src_indices, &in_reg_schema, &node_schema);
                    ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(node_schema);
                    let func_idx = ctx.builder.func_idx(fp);
                    let out_schema_idx = ctx.builder.schema_idx(node_schema);
                    ctx.builder.push(Instr::Map {
                        in_reg: in_reg as u16,
                        out_reg: reg_id as u16,
                        func_idx,
                        out_schema_idx,
                        reindex: ReindexOperand::HashRow { branch_id: *branch_id },
                    });
                }

                gnitz_wire::MapKind::Projection(cols) => {
                    if oob_cols(cols, &in_reg_schema) {
                        return Err(CompileError::Rejected("projection map: columns out of range"));
                    }
                    let src_indices: Vec<i32> = cols.iter().map(|&c| c as i32).collect();
                    let schema = build_map_output_schema(&in_reg_schema, &src_indices);
                    let fp = ctx.create_universal_projection(&src_indices, &in_reg_schema, &schema);
                    ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(schema);
                    let func_idx = ctx.builder.func_idx(fp);
                    let out_schema_idx = ctx.builder.schema_idx(schema);
                    ctx.builder.push(Instr::Map {
                        in_reg: in_reg as u16,
                        out_reg: reg_id as u16,
                        func_idx,
                        out_schema_idx,
                        reindex: ReindexOperand::None,
                    });
                }
            }
        }

        gnitz_wire::OpNode::Negate => {
            let in_reg = in_reg(&in_regs, PORT_IN, "negate: missing input port")?;
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(ctx.reg_meta[in_reg as usize].schema);
            ctx.builder.push(Instr::Negate {
                in_reg: in_reg as u16,
                out_reg: reg_id as u16,
            });
        }

        gnitz_wire::OpNode::Union => {
            let in_a = in_reg(&in_regs, PORT_IN_A, "union: missing left input port")?;
            let has_b = in_regs.contains_key(&PORT_IN_B);
            let in_b = in_regs.get(&PORT_IN_B).copied().unwrap_or(0);
            let a_schema = ctx.reg_meta[in_a as usize].schema;
            let out_schema = if has_b {
                union_nullability_merge(&a_schema, &ctx.reg_meta[in_b as usize].schema)
            } else {
                // O(1) identity pass-through (op_union returns PORT_IN_A unchanged); no B
                // schema to merge.
                a_schema
            };
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(out_schema);
            ctx.builder.push(Instr::Union {
                in_a: in_a as u16,
                in_b: in_b as u16,
                has_b,
                out_reg: reg_id as u16,
            });
        }

        gnitz_wire::OpNode::Distinct | gnitz_wire::OpNode::PositivePart => {
            let in_reg = in_reg(&in_regs, PORT_IN, "weight-clamp: missing input port")?;
            let in_reg_schema = ctx.reg_meta[in_reg as usize].schema;
            // `distinct` is the only one the optimizer elides (its input is already
            // distinct); `positive_part` is never seeded into the skip set, so
            // this check is simply false for it.
            if ctx.skip_nodes.contains(&nid) {
                ctx.out_reg_of.insert(nid, in_reg);
                return Ok(());
            }
            // Set-membership clamp `[-1, 1]` for distinct; bag clamp `[0, i64::MAX]`
            // (negative part only) for positive_part. The two presets are the sole
            // difference between the operators; both emit one `WeightClamp` instr.
            let (lo, hi) = if matches!(op, gnitz_wire::OpNode::PositivePart) {
                (0, i64::MAX)
            } else {
                (-1, 1)
            };
            let child_name = format!("_hist_{}_{nid}", ctx.view_id);
            let hist_table_ptr = ctx.add_owned_trace_table(&child_name, in_reg_schema, Some(reg_id))?;
            let out_delta_id = ctx.push_delta_reg(in_reg_schema);
            ctx.out_reg_of.insert(nid, out_delta_id);
            let hist_table_idx = ctx.builder.table_idx(hist_table_ptr) as i16;
            ctx.builder.push(Instr::WeightClamp {
                in_reg: in_reg as u16,
                hist_reg: reg_id as u16,
                out_reg: out_delta_id as u16,
                hist_table_idx,
                lo,
                hi,
            });
        }

        gnitz_wire::OpNode::Reduce {
            group_cols,
            agg,
            global_ground,
            out_key,
        } => {
            emit_reduce(ctx, nid, reg_id, group_cols, agg, *global_ground, *out_key, &in_regs)?;
        }

        gnitz_wire::OpNode::Join(kind) => {
            // PORT_IN_A == 0 (delta side); PORT_TRACE == PORT_IN_B == 1 (trace/right side).
            let a_reg = in_reg(&in_regs, PORT_IN_A, "join: missing delta input port")?;
            let b_reg = in_reg(&in_regs, PORT_TRACE, "join: missing trace input port")?;
            let a_schema = ctx.reg_meta[a_reg as usize].schema;
            let b_schema = ctx.reg_meta[b_reg as usize].schema;
            match kind {
                gnitz_wire::JoinKind::DeltaTrace => {
                    let out_schema = merge_schemas_for_join(&a_schema, &b_schema)
                        .ok_or(CompileError::Rejected("join: merged schema exceeds MAX_COLUMNS"))?;
                    ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(out_schema);
                    let right_schema_idx = ctx.builder.schema_idx(b_schema);
                    ctx.builder.push(Instr::JoinDT {
                        delta_reg: a_reg as u16,
                        trace_reg: b_reg as u16,
                        out_reg: reg_id as u16,
                        right_schema_idx,
                    });
                }
                gnitz_wire::JoinKind::DeltaTraceRange { n_eq, rel } => {
                    // The trace side's reindexed key is `[eq slots…, range slot]`, so
                    // its PK arity is exactly `n_eq + 1` (ops/join/range.rs). A crafted
                    // `n_eq` otherwise slices `columns[..n_eq]` out of range at runtime;
                    // reject here, promoting the release-stripped `debug_assert` to a
                    // compile-time guard (also closing the `dispatch.rs` slice).
                    if *n_eq as usize + 1 != b_schema.pk_indices().len() {
                        return Err(CompileError::Rejected(
                            "range join: n_eq does not match trace key arity",
                        ));
                    }
                    // Same output layout as the equi delta-trace join; only the
                    // probe differs. `n_eq`/`rel` ride to the op so it can derive
                    // the eq-prefix / range-slot split and the cut direction.
                    let out_schema = merge_schemas_for_join(&a_schema, &b_schema)
                        .ok_or(CompileError::Rejected("join: merged schema exceeds MAX_COLUMNS"))?;
                    ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(out_schema);
                    let right_schema_idx = ctx.builder.schema_idx(b_schema);
                    ctx.builder.push(Instr::JoinDTRange {
                        delta_reg: a_reg as u16,
                        trace_reg: b_reg as u16,
                        out_reg: reg_id as u16,
                        right_schema_idx,
                        n_eq: *n_eq,
                        rel: *rel,
                    });
                }
            }
        }

        gnitz_wire::OpNode::IntegrateSink => {
            let in_reg = in_reg(&in_regs, PORT_IN, "integrate-sink: missing input port")?;
            ctx.sink_reg_id = in_reg;
            ctx.push_integrate(in_reg as u16, std::ptr::null_mut());
        }

        gnitz_wire::OpNode::IntegrateTrace => {
            let in_reg = in_reg(&in_regs, PORT_IN, "integrate-trace: missing input port")?;
            let in_reg_schema = ctx.reg_meta[in_reg as usize].schema;
            let child_name = format!("_int_{}_{nid}", ctx.view_id);
            // Must fail the compile on a table-open error: emitting the view without
            // the Integrate would compile a view that never persists its differential
            // state, leaving its output permanently empty.
            let table_ptr = ctx.add_owned_trace_table(&child_name, in_reg_schema, Some(reg_id))?;
            ctx.push_integrate(in_reg as u16, table_ptr);
        }

        gnitz_wire::OpNode::ExchangeShard { .. } => {
            unreachable!("ExchangeShard is excised from build_plan's node list in compile_view")
        }

        gnitz_wire::OpNode::PartitionFilter => {
            // Pass-through schema; drops the rows this worker does not own before
            // they reach `integrate_trace`. Worker identity is the compile-time
            // `(worker_rank, num_workers)` of this process (default `(0, 1)` =
            // keep-all for single-process / unit tests). An all-replicated view runs
            // correct-local over the full broadcast on every worker, so it bakes
            // `(0, 1)` too — `op_partition_filter` degenerates to a keep-all identity
            // at `num_workers <= 1`, integrating the full input instead of trimming
            // away the rows this worker does not own.
            let in_reg = in_reg(&in_regs, PORT_IN, "partition-filter: missing input port")?;
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(ctx.reg_meta[in_reg as usize].schema);
            let (worker_id, num_workers) = if ctx.all_sources_replicated {
                (0, 1)
            } else {
                (worker_rank(), num_workers())
            };
            ctx.builder.push(Instr::PartitionFilter {
                in_reg: in_reg as u16,
                out_reg: reg_id as u16,
                worker_id,
                num_workers,
            });
        }

        gnitz_wire::OpNode::ExchangeGather => {
            if let Some(&in_reg) = in_regs.get(&PORT_IN) {
                ctx.reg_meta[reg_id as usize] = ctx.reg_meta[in_reg as usize];
                // ExchangeGather is a logical passthrough: the exchange mechanism
                // injects gathered data directly into the exchange-input register.
                // Redirect downstream reads to that register; reg_id is never written.
                ctx.out_reg_of.insert(nid, in_reg);
            }
        }

        gnitz_wire::OpNode::NullExtend { type_codes } => {
            let in_reg = in_reg(&in_regs, PORT_IN, "null-extend: missing input port")?;
            let in_schema = ctx.reg_meta[in_reg as usize].schema;
            // The output schema — input columns followed by the null-fill columns
            // (nullable), keyed by the input PK — is built here once and homed in
            // `reg_meta`; the op derives its appended-column count from it. A
            // crafted `type_codes` list would overflow the fixed `[_; 65]` schema
            // array; reject rather than abort.
            if in_schema.num_columns() + type_codes.len() > crate::schema::MAX_COLUMNS {
                return Err(CompileError::Rejected("null-extend: merged schema exceeds MAX_COLUMNS"));
            }
            let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
            let mut pk_idx = [0u32; crate::schema::MAX_PK_COLUMNS];
            let pk_len = copy_pk_columns_into(&in_schema, &mut cols, &mut pk_idx);
            let mut n = pk_len;
            for (_, _, c) in in_schema.payload_columns() {
                cols[n] = *c;
                n += 1;
            }
            for &tc in type_codes {
                cols[n] = SchemaColumn::new(tc, 1);
                n += 1;
            }
            let out_schema = SchemaDescriptor::new(&cols[..n], &pk_idx[..pk_len]);
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(out_schema);
            ctx.builder.push(Instr::NullExtend {
                in_reg: in_reg as u16,
                out_reg: reg_id as u16,
            });
        }
    }
    Ok(())
}

pub(super) fn compute_in_regs(loaded: &LoadedCircuit, nid: i32, out_reg_of: &HashMap<i32, i32>) -> HashMap<i32, i32> {
    let mut in_regs = HashMap::new();
    if let Some(in_edges) = loaded.incoming.get(&nid) {
        for &(src, port) in in_edges {
            if let Some(&reg) = out_reg_of.get(&src) {
                in_regs.insert(port, reg);
            }
        }
    }
    in_regs
}

// ---------------------------------------------------------------------------
// REDUCE emission
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_reduce(
    ctx: &mut EmitCtx,
    nid: i32,
    reg_id: i32,
    group_cols: &[u16],
    agg: &gnitz_wire::AggKind,
    global_ground: bool,
    out_key: gnitz_wire::ReduceOutKey,
    in_regs: &HashMap<i32, i32>,
) -> Result<(), CompileError> {
    let loaded = ctx.loaded;
    let in_reg_id = in_reg(in_regs, PORT_IN, "reduce: missing input port")?;
    let in_reg_schema = ctx.reg_meta[in_reg_id as usize].schema;

    let gcols_u32: Vec<u32> = group_cols.iter().map(|&c| c as u32).collect();

    // Raw wire column indices below index the fixed `[_; 65]` schema array. Reject
    // an out-of-range group column or aggregate column before `agg_descs` is built
    // (which reads `columns[col_idx]`), so a crafted/corrupt node fails the compile
    // rather than reading a zeroed slot or aborting at the first push.
    if oob_cols(group_cols, &in_reg_schema) {
        return Err(CompileError::Rejected("reduce: group columns out of range"));
    }
    if let gnitz_wire::AggKind::Specs(specs) = agg {
        if specs.iter().any(|&(_, c)| c as usize >= in_reg_schema.num_columns()) {
            return Err(CompileError::Rejected("reduce: aggregate column out of range"));
        }
    }

    let mut agg_descs: Vec<AggDescriptor> = Vec::new();

    match agg {
        gnitz_wire::AggKind::Null => {
            agg_descs.push(AggDescriptor {
                col_idx: 0,
                agg_op: AggOp::Null,
                col_type_code: TypeCode::U64,
            });
        }
        gnitz_wire::AggKind::Specs(specs) => {
            for &(ref func, col_idx) in specs {
                let agg_op = AggOp::from(*func);
                let col_type_code = TypeCode::from_validated_u8(in_reg_schema.columns[col_idx as usize].type_code);
                agg_descs.push(AggDescriptor {
                    col_idx: col_idx as u32,
                    agg_op,
                    col_type_code,
                });
            }
        }
    }

    // Every aggregate that decodes its column value needs an order-encodable
    // (≤8-byte int/float) scalar. SUM/SUM_ZERO sum it — a 16-byte source would abort
    // at the first push in `decode_signed` (`unreachable!`) and a string would
    // silently mis-sum; MIN/MAX compare it via `encode_ordered`, which has no
    // monotone key for STRING / U128 / UUID / BLOB. COUNT / COUNT_NON_NULL and the
    // NULL placeholder never read the value. The SQL binder already rejects these,
    // so this is the defensive guard for the low-level CircuitBuilder path that
    // bypasses it: a failure fails the compile (so the view compiles to nothing)
    // rather than panicking a worker at execution.
    if agg_descs.iter().any(|ad| {
        matches!(ad.agg_op, AggOp::Sum | AggOp::SumZero | AggOp::Min | AggOp::Max)
            && !agg_value_idx_eligible(ad.col_type_code)
    }) {
        return Err(CompileError::Rejected(
            "reduce: aggregate column is not order-encodable",
        ));
    }

    // Validate the planner's shipped output-key kind against the input schema
    // before building the output schema. Everything downstream — the output
    // schema layout and `op_reduce`'s row keying — obeys `out_key`, so a kind
    // the schema does not warrant would silently scramble the output columns;
    // reject the circuit instead (same failure class as the
    // MIN/MAX-eligibility guard above).
    if out_key != in_reg_schema.reduce_out_key(&gcols_u32) {
        return Err(CompileError::Rejected("reduce: out_key does not match input schema"));
    }
    let reduce_out_schema = build_reduce_output_schema(&in_reg_schema, &gcols_u32, &agg_descs, out_key);

    let trace_table_ptr = ctx.add_owned_trace_table(
        &format!("_reduce_{}_{nid}", ctx.view_id),
        reduce_out_schema,
        Some(reg_id),
    )?;

    let raw_delta_id = ctx.push_delta_reg(reduce_out_schema);

    // Finalize fold: absorb this reduce's sole consuming eligible MAP into the
    // reduce's finalize program, saving one intermediate batch materialization
    // per tick. Derived here — where the consumer, blob, and schemas are all in
    // scope — instead of a separate rewrite pass threaded through the phase split.
    let fold = reduce_finalize_fold(loaded, nid);
    let mut fin_delta_id: i32 = -1;
    if fold.is_some() {
        fin_delta_id = ctx.push_delta_reg(loaded.out_schema);
        ctx.out_reg_of.insert(nid, fin_delta_id);
    } else {
        ctx.out_reg_of.insert(nid, raw_delta_id);
    }

    let all_linear = agg_descs.iter().all(|a| a.agg_op.is_linear());
    let has_value_indexed = agg_descs.iter().any(|a| a.agg_op.uses_value_index());
    // Serve every MIN/MAX aggregate (grouped or global) from one combined value
    // index, keyed `group_cols ‖ ordinal ‖ av_encoded`, when the group key is
    // byte-form-eligible (the ordinal column is accounted for in
    // `avi_group_key_eligible`'s budget). Every value-indexed aggregate is already
    // order-encodable — the combined value-decode guard above rejected any that
    // were not — so AVI use turns only on the group key. The empty global key is
    // eligible, so a global MIN/MAX always resolves via the index; nothing
    // value-indexed is left on the trace-scan fallback below.
    //
    // No nullable check on the aggregate columns: NULL aggregate values never
    // reach the AVI. The reduce accumulator skips NULL inputs (ops/reduce/agg.rs)
    // and AVI population skips a NULL aggregate value before encoding the index
    // key (ops/index.rs), whose value column is a non-nullable PK. Moving either
    // filter without revisiting this would write a zeroed key and corrupt MIN/MAX.
    let use_avi = has_value_indexed && avi_group_key_eligible(&in_reg_schema, &gcols_u32);

    let mut tr_in_reg_id: i32 = -1;
    let mut tr_in_table_ptr: *mut Table = std::ptr::null_mut();

    if let Some(&existing) = in_regs.get(&PORT_TRACE) {
        tr_in_reg_id = existing;
    } else if !all_linear && !use_avi {
        tr_in_reg_id = ctx.reg_meta.len() as i32;
        ctx.reg_meta.push(RegisterMeta::delta(in_reg_schema)); // overwritten to trace below
        tr_in_table_ptr = ctx.add_owned_trace_table(
            &format!("_reduce_in_{}_{nid}", ctx.view_id),
            in_reg_schema,
            Some(tr_in_reg_id),
        )?;
    }

    // One combined value index per reduce: a single table keyed
    // `group_cols ‖ ordinal ‖ av_encoded`, serving every MIN/MAX aggregate.
    // `avi_aggs` is the value-indexed subset of `agg_descs`, in descriptor order
    // (ordinal = position) — the same order, selected by the same predicate, that
    // the reduce read side walks into its seek prefix. One table → one table_id,
    // one scratch dir, one compaction-filename namespace, so per-aggregate entries
    // cannot collide on a memory-pressure flush.
    let mut avi_table_ptr: *mut Table = std::ptr::null_mut();
    let mut avi_aggs: Vec<AggDescriptor> = Vec::new();

    if use_avi {
        avi_aggs = agg_descs
            .iter()
            .filter(|d| d.agg_op.uses_value_index())
            .copied()
            .collect();
        let avi_child = format!("_avidx_{}_{nid}", ctx.view_id);
        let avi_schema = crate::ops::make_avi_schema(&in_reg_schema, &gcols_u32);
        // A failed AVI table create degrades to the no-index reduce path rather
        // than failing the compile (tolerated since the index is derivable).
        if let Ok(ptr) = ctx.add_owned_trace_table(&avi_child, avi_schema, None) {
            avi_table_ptr = ptr;
        }
    }

    // The combined index integrates BEFORE the reduce reads it, so a prefix seek
    // returns the post-delta extreme directly. (The trace_in integrate below runs
    // after the reduce and carries no index.)
    if !avi_table_ptr.is_null() {
        let (group_cols_offset, group_cols_count) = ctx.builder.add_group_cols(&gcols_u32);
        let (agg_descs_offset, agg_descs_count) = ctx.builder.add_agg_descs(&avi_aggs);
        let avi = IntegrateAvi {
            table_idx: ctx.builder.table_idx(avi_table_ptr) as u16,
            group_cols_offset,
            group_cols_count,
            agg_descs_offset,
            agg_descs_count,
        };
        ctx.builder.push(Instr::Integrate {
            in_reg: in_reg_id as u16,
            table_idx: -1,
            avi: Some(avi),
        });
    }

    // Finalize is a MAP from the raw reduce output to the view's output schema:
    // validate the folded logical program against those schemas (exactly as a MAP
    // blob is validated in `emit_node`), then build the same `ScalarFunc` any
    // other MAP node gets. It is kept alive in `owned_funcs`, which the reduce op
    // holds a raw `*const ScalarFunc` into.
    let fin_func_ptr: *const ScalarFunc = if let Some((map_nid, prog)) = fold {
        prog.validate(Some(&reduce_out_schema), Some(&loaded.out_schema))
            .map_err(expr_reject("reduce finalize: program/schema mismatch"))?;
        ctx.folded_map_regs.insert(map_nid, fin_delta_id);
        ctx.push_func(ScalarFunc::from_map(prog, &reduce_out_schema, &loaded.out_schema))
    } else {
        std::ptr::null()
    };

    // Bake worker ownership of the global-aggregate seed exactly as the
    // `PartitionFilter` arm bakes `(worker_rank(), num_workers())`. A reduce whose
    // input has no upstream `ExchangeShard` is **replicated** (`reduce_multi_local`
    // — the full source is on every worker), so every worker is its own owner and
    // seeds its local copy; the load-bearing disjunct, since a replicated view is
    // single-source-read from worker 0, not `worker_for_partition(V₀)`. A sharded
    // global aggregate (`reduce_multi`) funnels all rows to partition
    // `partition_for_key(V₀)`'s owner, so only that worker seeds. Checked against
    // the static `loaded.incoming` graph (which retains the `ExchangeShard →
    // Reduce` edge across the post-phase split), NOT the post-phase register wiring
    // (the ExchangeShard node emits no instruction). `i_am_owner` is meaningful
    // only when `global_ground`; left `false` otherwise so a grouped reduce never
    // pays the bake.
    let i_am_owner = global_ground && {
        let replicated = !loaded.incoming.get(&nid).is_some_and(|ins| {
            ins.iter().any(|&(src, port)| {
                port == PORT_IN && matches!(loaded.nodes.get(&src), Some(gnitz_wire::OpNode::ExchangeShard { .. }))
            })
        });
        replicated
            || worker_rank() as usize
                == crate::ops::worker_for_partition(
                    crate::storage::partition_for_key(crate::ops::global_group_key()),
                    num_workers() as usize,
                )
    };

    let avi_table_idx = (!avi_table_ptr.is_null()).then(|| ctx.builder.table_idx(avi_table_ptr) as u16);
    let finalize = (!fin_func_ptr.is_null()).then(|| crate::query::vm::FinalizeIdx {
        func_idx: ctx.builder.func_idx(fin_func_ptr),
        schema_idx: ctx.builder.schema_idx(loaded.out_schema),
    });

    // Bake the reduce plan — the one construction site for everything the
    // operator would otherwise re-derive per epoch from the instruction operands.
    let plan_idx = ctx.builder.add_reduce_plan(crate::ops::ReducePlan::new(
        &in_reg_schema,
        &reduce_out_schema,
        &gcols_u32,
        &agg_descs,
        out_key,
        avi_table_idx.is_some(),
        global_ground,
        i_am_owner,
    ));

    ctx.builder.push(Instr::Reduce {
        in_reg: in_reg_id as u16,
        trace_in_reg: (tr_in_reg_id >= 0).then_some(tr_in_reg_id as u16),
        trace_out_reg: reg_id as u16,
        out_reg: raw_delta_id as u16,
        fin_out_reg: (fin_delta_id >= 0).then_some(fin_delta_id as u16),
        plan_idx,
        avi_table_idx,
        finalize,
    });

    // The trace_in integrate (non-linear, non-AVI fallback) carries no value
    // index — tr_in and the AVI are mutually exclusive (the tr_in gate is
    // `!all_linear && !use_avi`).
    if !tr_in_table_ptr.is_null() {
        ctx.push_integrate(in_reg_id as u16, tr_in_table_ptr);
    }

    ctx.push_integrate(raw_delta_id as u16, trace_table_ptr);
    Ok(())
}

// ---------------------------------------------------------------------------
// build_plan — one plan, pre or post exchange
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(super) fn build_plan(
    loaded: &LoadedCircuit,
    skip_nodes: &HashSet<i32>,
    ordered: &[i32],
    ext_tables: &ExtTables,
    view_dir: &str,
    view_id: u64,
    output_node_id: Option<i32>,
    exchange_inputs: &[(i32, SchemaDescriptor)],
) -> Result<PlanBuildResult, CompileError> {
    let mut out_reg_of: HashMap<i32, i32> = HashMap::new();
    let mut next_reg: i32 = 0;
    for &nid in ordered {
        out_reg_of.insert(nid, next_reg);
        next_reg += 1;
    }

    // One seed register per exchange input (the post phase of an exchange view
    // reads each side's relayed batch from its own register).
    let mut exchange_input_regs: Vec<(i32, i32)> = Vec::with_capacity(exchange_inputs.len());
    let first_exchange_input_reg_id: i32 = if exchange_inputs.is_empty() { -1 } else { next_reg };
    for (ex_nid, _) in exchange_inputs {
        exchange_input_regs.push((*ex_nid, next_reg));
        next_reg += 1;
    }

    // Register ids are u16 instruction fields. `reg_meta` is sized to the base
    // register per node plus the exchange seeds here; the emitters push the extras
    // on demand — ScanTrace/Distinct push 1, Reduce up to 3
    // (raw_delta + finalize + trace-in). Each node pushes at most 3, so reserving
    // `next_reg + 3 * ordered.len()` holds the whole program in a single
    // allocation, and that same bound — rejected here before the emit loop creates
    // any scratch tables — guarantees the final `reg_meta.len()` can never exceed
    // u16, so no register id truncates when cast.
    let reg_cap = next_reg as usize + 3 * ordered.len();
    if reg_cap > u16::MAX as usize {
        return Err(CompileError::Rejected("register count exceeds u16::MAX"));
    }
    let mut reg_meta = Vec::with_capacity(reg_cap);
    reg_meta.resize(next_reg as usize, RegisterMeta::delta(SchemaDescriptor::default()));

    for ((ex_nid, ex_schema), &(_, reg)) in exchange_inputs.iter().zip(&exchange_input_regs) {
        out_reg_of.insert(*ex_nid, reg);
        reg_meta[reg as usize] = RegisterMeta::delta(*ex_schema);
    }

    // Are all of this view's `ScanDelta` sources replicated base tables? The
    // `ScanDelta` set is exactly `circuit.dependencies()` (`ScanTrace` excluded),
    // the same source set `DagEngine::view_all_sources_replicated` reads from the
    // dep map — so this compile-time flag equals the runtime decision that
    // short-circuits the view to the local epoch path, and cannot drift from it.
    // `loaded` is the whole circuit in every phase, so the flag is phase-independent.
    let mut has_source = false;
    let all_sources_replicated = loaded
        .nodes
        .values()
        .filter_map(|op| match op {
            gnitz_wire::OpNode::ScanDelta(tid) => Some(*tid as i64),
            _ => None,
        })
        .all(|tid| {
            has_source = true;
            ext_tables.get(&tid).is_some_and(|schema| schema.replicated())
        })
        && has_source;

    let mut ctx = EmitCtx {
        loaded,
        skip_nodes,
        ext_tables,
        view_dir,
        view_id,
        all_sources_replicated,
        builder: ProgramBuilder::new(),
        out_reg_of,
        reg_meta,
        owned_tables: Vec::new(),
        owned_funcs: Vec::new(),
        owned_trace_regs: Vec::new(),
        ext_trace_regs: Vec::new(),
        source_reg_map: HashMap::new(),
        sink_reg_id: -1,
        scratch: ScratchGuard::new(),
        folded_map_regs: HashMap::new(),
    };

    for &nid in ordered {
        // `compile_view` excises every `ExchangeShard` from the `ordered` slice
        // (its `emit_node` arm is `unreachable!`). A node that fails to emit
        // (corrupt/unsupported catalog) stops the build at once; the ScratchGuard
        // removes the scratch dirs created so far, so a rejected compile leaks no
        // inodes (on success they are handed to the caller in PlanBuildResult,
        // kept alive by the VM's tables).
        let reg_id = *ctx.out_reg_of.get(&nid).unwrap();
        emit_node(&mut ctx, nid, reg_id)?;
    }

    ctx.builder.push(Instr::Halt);

    // Destructive-register ordering invariant. `Union` and `WeightClamp`
    // (distinct / positive_part) empty their input register in place
    // (`Batch::take`, to avoid allocation). Every register has one writer and its
    // readers run in instruction order, so a destructively-consumed register is
    // correct only when the destructive read is the LAST read of that register.
    // Checked over the EMITTED instructions with resolved registers, so register
    // aliasing from elided nodes — identity/folded MAPs, `Filter(None)`
    // pass-throughs, skipped Distincts, ExchangeGather — is seen through rather
    // than reasoned about via graph edges. (A self-union reads in_a == in_b in
    // one instruction; the exec arm handles that before the take.)
    let instrs = ctx.builder.instructions();
    for (i, instr) in instrs.iter().enumerate() {
        let dtor_reg = match instr {
            Instr::Union { in_a, .. } => *in_a,
            Instr::WeightClamp { in_reg, .. } => *in_reg,
            _ => continue,
        };
        if instrs[i + 1..].iter().any(|later| reads_reg(later, dtor_reg)) {
            gnitz_warn!(
                "build_plan: instruction {i} destructively consumes register {dtor_reg}, \
                 but a later instruction still reads it; every other reader of a \
                 destructively-consumed register must precede it"
            );
            return Err(CompileError::Rejected(
                "destructive register read is not the last reader",
            ));
        }
    }

    let mut input_delta_reg_id = first_exchange_input_reg_id;
    if input_delta_reg_id == -1 && !ctx.source_reg_map.is_empty() {
        input_delta_reg_id = *ctx.source_reg_map.values().next().unwrap();
    }

    let mut sink_reg = ctx.sink_reg_id;
    if sink_reg == -1 {
        if let Some(out_nid) = output_node_id {
            if let Some(&reg) = ctx.out_reg_of.get(&out_nid) {
                sink_reg = reg;
            }
        }
    }

    if input_delta_reg_id == -1 {
        return Err(CompileError::Rejected("plan has no input delta register"));
    }
    if sink_reg == -1 {
        return Err(CompileError::Rejected("plan has no output register"));
    }

    if output_node_id.is_none() {
        let sink_schema = &ctx.reg_meta[sink_reg as usize].schema;
        let out_schema = &loaded.out_schema;
        // A column-count match is not enough: two schemas with equal column
        // counts but mismatched types (e.g. I64 vs German-string) let the client
        // read a 16-byte string descriptor out of 8-byte integer storage.
        if sink_schema.num_columns() > 0 && !sink_schema.same_physical_layout(out_schema) {
            return Err(CompileError::Rejected("sink schema does not match view output schema"));
        }
    }

    // The pre-loop bound already guaranteed `reg_meta.len()` could not exceed u16
    // during emission. `reg_meta` is moved into `build_with_owned` below.
    debug_assert!(ctx.reg_meta.len() <= u16::MAX as usize);

    let EmitCtx {
        builder,
        reg_meta,
        owned_tables,
        owned_funcs,
        owned_trace_regs,
        ext_trace_regs,
        source_reg_map,
        scratch,
        ..
    } = ctx;
    let vm = builder.build_with_owned(reg_meta, owned_tables, owned_funcs, owned_trace_regs);

    Ok(PlanBuildResult {
        vm,
        in_reg: input_delta_reg_id,
        out_reg: sink_reg,
        ext_trace_regs,
        source_reg_map,
        exchange_input_regs,
        scratch,
    })
}

// ---------------------------------------------------------------------------
// compile_view helpers
// ---------------------------------------------------------------------------

/// All nodes reachable backwards from `start` (inclusive) via incoming edges —
/// i.e. the sub-pipeline that produces `start`'s value. Used to carve out each
/// set-op side's independent single-source pipeline.
pub(super) fn ancestors_inclusive(loaded: &LoadedCircuit, start: i32) -> HashSet<i32> {
    let mut set = HashSet::new();
    let mut queue = VecDeque::from([start]);
    while let Some(cur) = queue.pop_front() {
        if !set.insert(cur) {
            continue;
        }
        if let Some(ins) = loaded.incoming.get(&cur) {
            for &(src, _port) in ins {
                queue.push_back(src);
            }
        }
    }
    set
}

/// The node feeding an `ExchangeShard` on `PORT_IN` (the value to repartition),
/// or `None` if the exchange has no such input edge (a malformed circuit).
pub(super) fn exchange_input_node(loaded: &LoadedCircuit, ex_nid: i32) -> Option<i32> {
    loaded
        .incoming
        .get(&ex_nid)
        .and_then(|ins| ins.iter().find(|&&(_, port)| port == PORT_IN))
        .map(|&(src, _)| src)
}
