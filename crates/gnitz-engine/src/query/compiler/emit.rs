//! Instruction emission: per-node `emit_*`, the expression/scalar-func
//! constructors, and `build_plan` (one plan, pre or post exchange).

use super::*;
use crate::query::vm::{reads_reg, Instr, IntegrateAvi, ReindexOperand};

// ---------------------------------------------------------------------------
// Expression + scalar function construction helpers
// ---------------------------------------------------------------------------

/// Name the failed guard in the compile error and carry the validator's reason
/// with it. A rejected program is bad client input — a bad opcode / register /
/// const index, an out-of-range or PK-routed column, or a program over the
/// register cap — and the pre-flight renders the whole error to the client.
fn expr_reject(what: &'static str) -> impl Fn(ExprValidateErr) -> CompileError {
    move |e| CompileError::RejectedExpr(what, e)
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
    let cols: Vec<SchemaColumn> = (0..a.num_columns())
        .map(|c| {
            let (ac, bc) = (a.columns[c], b.columns[c]);
            debug_assert_eq!(ac.type_code, bc.type_code, "union inputs must share a layout");
            SchemaColumn::new(ac.type_code, ac.nullable | bc.nullable)
        })
        .collect();
    SchemaDescriptor::new(&cols, a.pk_indices())
}

/// Path of a per-worker scratch directory under `view_dir`. Rank-stamped because
/// forked workers share `view_dir`; an un-stamped path would have every worker
/// open the same directory and clobber each other's shard files. The name comes
/// from `ChildAddr`, which also parses it back for the boot and rebuild sweeps.
pub(super) fn child_scratch_dir(view_dir: &str, child_name: &str) -> String {
    crate::storage::ChildAddr::Scratch {
        child: child_name,
        rank: worker_rank(),
    }
    .dir(view_dir)
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
/// The `Box`es keep the raw pointers into them valid across the `Vec`s' growth.
#[allow(clippy::vec_box)]
pub(super) struct EmitCtx<'a> {
    pub loaded: &'a LoadedCircuit,
    pub skip_nodes: &'a HashSet<i32>,
    pub ext_tables: &'a ExtTables,
    pub view_dir: &'a str,
    pub view_id: u64,
    pub builder: ProgramBuilder,
    pub out_reg_of: HashMap<i32, i32>,
    pub reg_meta: Vec<RegisterMeta>,
    pub owned_tables: Vec<Box<Table>>,
    pub owned_funcs: Vec<Box<ScalarFunc>>,
    pub source_reg_map: HashMap<i64, i32>,
    pub sink_reg_id: i32,
    /// Set by `emit_reduce` when it emits a global-ground aggregate — the one
    /// operator that produces output from an empty input epoch. See
    /// `SubPlan::can_emit_on_empty`.
    pub can_emit_on_empty: bool,
    pub scratch: ScratchGuard,
}

impl EmitCtx<'_> {
    /// The view's stamped `replicated` bit — every `ScanDelta` source is
    /// replicated, so the view runs correct-local on every worker. The
    /// `WorkerFilter` arm bakes its trim as a keep-all identity, and
    /// `emit_reduce` makes every worker the owner of the global-aggregate seed.
    /// Read through `loaded` rather than cached, so the compile-time flag and the
    /// runtime decision cannot drift apart.
    fn replicated(&self) -> bool {
        self.loaded.out_schema.placement().is_replicated()
    }

    /// Box `func`, keep it alive in `owned_funcs`, and return a raw pointer into
    /// the box. Valid for the box's lifetime — the heap `ScalarFunc` is stable
    /// across the `Vec`'s growth — which is what the VM's raw `*const ScalarFunc`
    /// handles rely on.
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
        let func = LogicalProgram::from_wire(&dep.code, dep.num_regs, dep.result_reg, dep.const_strings)
            .and_then(|p| ScalarFunc::from_predicate(p, schema))
            .map_err(expr_reject("filter: invalid predicate program"))?;
        Ok(self.push_func(func))
    }

    /// A MAP whose program is all-`CopyCol`: output payload `i` ← input column
    /// `src_indices[i]`, widening into a promoted slot when the output column is
    /// wider. The source type is derived per-column in `resolve` from `in_schema`.
    fn create_universal_projection(
        &mut self,
        src_indices: &[i32],
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
    ) -> Result<*const ScalarFunc, CompileError> {
        let copies: Vec<u32> = src_indices.iter().map(|&ci| ci as u32).collect();
        let prog = LogicalProgram::copy_cols(&copies);
        // `from_map` validates. The out-schema is derived here rather than
        // supplied, but from a client column list: `build_map_output_schema` drops
        // PK sources while `copy_cols` numbers destinations densely, so a PK index
        // leaves a copy addressing a slot that does not exist.
        let func = ScalarFunc::from_map(prog, in_schema, out_schema)
            .map_err(expr_reject("projection map: program/schema mismatch"))?;
        Ok(self.push_func(func))
    }

    /// Create a child table in a rank-stamped subdirectory of the view's
    /// directory, tracked by the scratch guard (so a later compile failure
    /// removes it).
    fn create_child_table(&mut self, child_name: &str, schema: SchemaDescriptor) -> Result<Table, CompileError> {
        let child_dir = child_scratch_dir(self.view_dir, child_name);
        // Track the path before creating so cleanup also removes a partially
        // created directory if Table::new fails.
        self.scratch.track(child_dir.clone());
        // Only views compile plans, so a view's recovery policy applies: its
        // operator-trace tables are `Rederive` (the ephemeral
        // checkpoint round force-persists them with generation-stamped
        // manifests). Never reached from index-circuit compilation, which builds
        // its one table through `CatalogEngine::new_index_table` — also
        // checkpointed, but against a verdict the catalog computes rather than
        // this ambient read.
        let recovery = RecoverySource::rederive_checkpointed_now();
        Table::new(&child_dir, schema, self.view_id as u32, recovery)
            .map_err(|_| CompileError::Rejected("child table create failed"))
    }

    /// Create a child table, keep it alive in `owned_tables`, and return a raw
    /// pointer into the box. When `trace_reg` is given, mark it a trace register
    /// backed by the new table (`bind_trace_cursors` opens a cursor on it
    /// each epoch).
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
            // Each node contributes at most three owned tables and `build_plan`
            // already bounds the node count well below `u16::MAX / 3`.
            debug_assert!(idx <= u16::MAX as usize);
            self.reg_meta[reg as usize] = RegisterMeta::trace(schema, idx as u16);
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
        // `bound` is a backfill-scan hint consumed by the source drive, not by the
        // VM: emission is identical bounded or not.
        gnitz_wire::OpNode::ScanDelta { source: tid, .. } => {
            let schema = ext_schema(ctx.ext_tables, *tid as i64, "scan-delta: unknown source table")?;
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(schema);
            ctx.source_reg_map.insert(*tid as i64, reg_id);
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
                    // columns to the kept payload columns. A hand-assembled or
                    // planner-built list could name a column >= num_columns()
                    // (which `columns[c]` would read as a zeroed slot — a silently
                    // wrong key). Fail the compile cleanly. The column/PK-slot
                    // counts are bounded by `reindex_output_schema` itself. The
                    // byte stride needs no separate MAX_PK_BYTES check: each
                    // output PK column is reindex_output_type_code(tc).wire_stride()
                    // <= 16 bytes, so pk_n <= MAX_PK_COLUMNS (5) bounds the packed
                    // stride at 5 × 16 = 80 = MAX_PK_BYTES (ReindexPacker::new's
                    // assert is the tripwire).
                    if oob_cols(reindex_cols, &in_reg_schema) {
                        return Err(CompileError::Rejected("map: reindex columns out of range"));
                    }
                    // A mismatched carried promotion target would panic in
                    // encode_pk_column_promoted (a narrowing target slices
                    // scratch[src_width..target_width] with src_width > target_width);
                    // see `reindex_promotion_invalid`. Runs after the in-bounds
                    // column check above, so columns[c] is always in range here.
                    if reindex_promotion_invalid(reindex_cols, reindex_target_tcs, &in_reg_schema, false) {
                        return Err(CompileError::Rejected("map: invalid reindex promotion target"));
                    }
                    let node_schema = if !reindex_cols.is_empty() {
                        // The output payload is exactly what the program copies: a
                        // reindex program is one `COPY_COL(src, out)` per kept payload
                        // column with dense outs `0..n` (the planner's
                        // `build_reindex_program`), so the kept-column list is read
                        // straight off the decoded program — the single source of
                        // truth. Every reindex program the planner emits comes from
                        // that one builder and is therefore dense; any other shape is
                        // a corrupt/forged catalog, as is an out-of-range source
                        // column (`columns[c]` would read a zeroed slot). Both fail
                        // the compile rather than deriving a schema the program does
                        // not fully write.
                        let n_in = in_reg_schema.num_columns();
                        let Some(srcs) = prog.payload_copy_srcs() else {
                            return Err(CompileError::Rejected("map: reindex program is not a dense copy list"));
                        };
                        if srcs.iter().any(|&c| c as usize >= n_in) {
                            return Err(CompileError::Rejected("map: reindex payload column out of range"));
                        }
                        let payload_cols: Vec<u16> = srcs.iter().map(|&c| c as u16).collect();
                        reindex_output_schema(&in_reg_schema, reindex_cols, reindex_target_tcs, &payload_cols)
                            .ok_or(CompileError::Rejected("map: reindex output exceeds MAX_COLUMNS"))?
                    } else {
                        loaded.out_schema
                    };
                    // `from_map` validates the decoded program against the resolved
                    // node schema — including that it writes every declared output
                    // slot, since the map output arena is uninitialized. The decode
                    // above is reused here rather than re-lowering the blob a second
                    // time. Rejected, not asserted: circuits are client-supplied
                    // catalog data.
                    let func = ScalarFunc::from_map(prog, &in_reg_schema, &node_schema)
                        .map_err(expr_reject("map: program/schema mismatch"))?;
                    // A reindex-free map inherits the input PK region verbatim
                    // (`PkFill::Copy`), so the strides must agree. Both reindex arms
                    // overwrite every row's PK, so only this arm cares.
                    if reindex_cols.is_empty() && func.map_out_schema().pk_stride() != in_reg_schema.pk_stride() {
                        return Err(CompileError::Rejected("map: output PK stride differs from the input's"));
                    }
                    let fp = ctx.push_func(func);
                    ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(node_schema);
                    let func_idx = ctx.builder.func_idx(fp);
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
                        reindex,
                    });
                }

                gnitz_wire::MapKind::HashRow(proj_cols, target_tcs, branch_id) => {
                    // Keep the listed columns as payload (positions 0..k), like a
                    // Projection, but prepend a synthetic U128 PK that op_map sets
                    // to a hash of those payload columns (reindex_hash path): the
                    // node schema declares the (possibly promoted) slot and
                    // `create_universal_projection` widens the source into it, so
                    // both set-op sides hash one physical layout.
                    //
                    // Same trust-boundary guards as the Expression reindex arm above:
                    // an out-of-range column would read a zeroed schema slot (a
                    // silently wrong hash), and the shared `reindex_promotion_invalid`
                    // (restricted to the ≤8-byte fixed-int domain the payload widen
                    // supports) rejects a corrupt/forged carried target cleanly
                    // rather than truncating in `copy_column`.
                    if oob_cols(proj_cols, &in_reg_schema) {
                        return Err(CompileError::Rejected("hash-row map: columns out of range"));
                    }
                    if reindex_promotion_invalid(proj_cols, target_tcs, &in_reg_schema, true) {
                        return Err(CompileError::Rejected("hash-row map: invalid promotion target"));
                    }
                    let src_indices: Vec<i32> = proj_cols.iter().map(|&c| c as i32).collect();
                    let node_schema = hashrow_output_schema(&in_reg_schema, proj_cols, target_tcs)
                        .ok_or(CompileError::Rejected("hash-row map: output exceeds MAX_COLUMNS"))?;
                    let fp = ctx.create_universal_projection(&src_indices, &in_reg_schema, &node_schema)?;
                    ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(node_schema);
                    let func_idx = ctx.builder.func_idx(fp);
                    ctx.builder.push(Instr::Map {
                        in_reg: in_reg as u16,
                        out_reg: reg_id as u16,
                        func_idx,
                        reindex: ReindexOperand::HashRow { branch_id: *branch_id },
                    });
                }

                gnitz_wire::MapKind::Projection(cols) => {
                    if oob_cols(cols, &in_reg_schema) {
                        return Err(CompileError::Rejected("projection map: columns out of range"));
                    }
                    let src_indices: Vec<i32> = cols.iter().map(|&c| c as i32).collect();
                    // `oob_cols` bounds each index but not the list length, and
                    // duplicates are allowed, so a long list would overrun the
                    // fixed `[_; MAX_COLUMNS]` schema array — the bound is
                    // PK-inclusive and lives inside the builder.
                    let schema = build_map_output_schema(&in_reg_schema, &src_indices)
                        .ok_or(CompileError::Rejected("projection map: output exceeds MAX_COLUMNS"))?;
                    let fp = ctx.create_universal_projection(&src_indices, &in_reg_schema, &schema)?;
                    ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(schema);
                    let func_idx = ctx.builder.func_idx(fp);
                    ctx.builder.push(Instr::Map {
                        in_reg: in_reg as u16,
                        out_reg: reg_id as u16,
                        func_idx,
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
            let in_b = in_reg(&in_regs, PORT_IN_B, "union: missing right input port")?;
            let a_schema = ctx.reg_meta[in_a as usize].schema;
            let out_schema = union_nullability_merge(&a_schema, &ctx.reg_meta[in_b as usize].schema);
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(out_schema);
            ctx.builder.push(Instr::Union {
                in_a: in_a as u16,
                in_b: in_b as u16,
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
            let hist_table_idx = ctx.builder.table_idx(hist_table_ptr) as u16;
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
                    ctx.builder.push(Instr::JoinDT {
                        delta_reg: a_reg as u16,
                        trace_reg: b_reg as u16,
                        out_reg: reg_id as u16,
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
                    ctx.builder.push(Instr::JoinDTRange {
                        delta_reg: a_reg as u16,
                        trace_reg: b_reg as u16,
                        out_reg: reg_id as u16,
                        n_eq: *n_eq,
                        rel: *rel,
                    });
                }
            }
        }

        gnitz_wire::OpNode::IntegrateSink => {
            // Emits no instruction: the sink register's batch is what
            // `execute_epoch_multi` extracts at epoch end.
            let in_reg = in_reg(&in_regs, PORT_IN, "integrate-sink: missing input port")?;
            ctx.sink_reg_id = in_reg;
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
            // `compile_view` excises the exchange nids from the *post* phase's
            // node list, but each side's list is the ancestors of its own
            // exchange input with no such filter — so an exchange upstream of
            // another exchange's input stays in that side's list and arrives
            // here. No planner path emits that shape; the C circuit-builder API
            // (`gnitz_circuit_shard`) can, and rejecting is what keeps it from
            // aborting a worker.
            return Err(CompileError::Rejected("chained exchange nodes"));
        }

        gnitz_wire::OpNode::WorkerFilter => {
            // Pass-through schema; drops the rows this worker does not own before
            // they reach `integrate_trace`. Worker identity is the compile-time
            // `(worker_rank, num_workers)` of this process (default `(0, 1)` =
            // keep-all for single-process / unit tests). An all-replicated view runs
            // correct-local over the full broadcast on every worker, so it bakes
            // `(0, 1)` too — `op_worker_filter` degenerates to a keep-all identity
            // at `num_workers <= 1`, integrating the full input instead of trimming
            // away the rows this worker does not own.
            let in_reg = in_reg(&in_regs, PORT_IN, "worker-filter: missing input port")?;
            let (worker_id, num_workers) = if ctx.replicated() {
                (0, 1)
            } else {
                (worker_rank(), num_workers())
            };
            if num_workers <= 1 {
                // One worker owns every partition, so the filter is the identity —
                // and executing it would clone the whole delta each epoch. Alias
                // the input register instead, as an absent WHERE does.
                ctx.out_reg_of.insert(nid, in_reg);
                return Ok(());
            }
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(ctx.reg_meta[in_reg as usize].schema);
            ctx.builder.push(Instr::WorkerFilter {
                in_reg: in_reg as u16,
                out_reg: reg_id as u16,
                worker_id,
                num_workers,
            });
        }

        gnitz_wire::OpNode::NullExtend { type_codes } => {
            let in_reg = in_reg(&in_regs, PORT_IN, "null-extend: missing input port")?;
            let in_schema = ctx.reg_meta[in_reg as usize].schema;
            // The output schema is built here once and homed in `reg_meta`; the
            // op derives its appended-column count from it.
            let out_schema = null_extend_output_schema(&in_schema, type_codes)
                .ok_or(CompileError::Rejected("null-extend: merged schema exceeds MAX_COLUMNS"))?;
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
    agg: &[(gnitz_wire::AggFunc, u16)],
    global_ground: bool,
    out_key: crate::schema::ReduceOutKey,
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
    if agg.is_empty() {
        return Err(CompileError::Rejected("reduce: no aggregate spec"));
    }
    // The low-level `CircuitBuilder` API and the wire decode take `global_ground`
    // and the group columns independently and cross-check neither, but the ground
    // row is only well-formed group-less: `emit_global_ground` writes the aggregate
    // columns at payload index 0, which with a non-empty group set overwrites the
    // exemplar slots and leaves their regions short — a malformed batch in release.
    // The implication only runs one way (`ground ⇒ empty`; the threshold reduce and
    // the two-phase phase-1 partial are group-less with `global_ground = false`), so
    // the flag cannot simply be derived — this is the one cross-check.
    if global_ground && !group_cols.is_empty() {
        return Err(CompileError::Rejected(
            "reduce: global_ground with a non-empty group set",
        ));
    }
    ctx.can_emit_on_empty |= global_ground;
    if agg.iter().any(|&(_, c)| c as usize >= in_reg_schema.num_columns()) {
        return Err(CompileError::Rejected("reduce: aggregate column out of range"));
    }

    let agg_descs: Vec<AggDescriptor> = agg
        .iter()
        .map(|&(func, col_idx)| AggDescriptor {
            col_idx: col_idx as u32,
            agg_op: func,
            col_type_code: TypeCode::from_validated_u8(in_reg_schema.columns[col_idx as usize].type_code),
        })
        .collect();

    // Every aggregate that decodes its column value needs an order-encodable
    // (≤8-byte int/float) scalar. SUM/SUM_ZERO sum it — a 16-byte source would abort
    // when the accumulator classifies its widening (`SumWiden::classify`) and a string would
    // silently mis-sum; MIN/MAX compare it via `encode_ordered`, which has no
    // monotone key for STRING / U128 / UUID / BLOB. COUNT / COUNT_NON_NULL never
    // read the value. The SQL binder already rejects these,
    // so this is the defensive guard for the low-level CircuitBuilder path that
    // bypasses it: a failure fails the compile (so the view compiles to nothing)
    // rather than panicking a worker at execution.
    if agg_descs.iter().any(|ad| {
        matches!(ad.agg_op, AggFunc::Sum | AggFunc::SumZero | AggFunc::Min | AggFunc::Max)
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
    // `oob_cols` bounds each group/agg column index but not the list lengths,
    // and duplicates are legal, so the schema builder owns the column-count
    // bound (see `build_reduce_output_schema`).
    let reduce_out_schema = build_reduce_output_schema(&in_reg_schema, &gcols_u32, &agg_descs, out_key)
        .ok_or(CompileError::Rejected("reduce: output exceeds MAX_COLUMNS"))?;

    let trace_table_ptr = ctx.add_owned_trace_table(
        &format!("_reduce_{}_{nid}", ctx.view_id),
        reduce_out_schema,
        Some(reg_id),
    )?;

    let raw_delta_id = ctx.push_delta_reg(reduce_out_schema);
    ctx.out_reg_of.insert(nid, raw_delta_id);

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
        // Not optional: `use_avi` was decided above and suppressed the `_reduce_in`
        // trace table, so a swallowed failure would leave the non-linear reduce with
        // neither an index nor a history to replay — MIN/MAX computed from the delta
        // alone, with the old row still retracted.
        avi_table_ptr = ctx.add_owned_trace_table(&avi_child, avi_schema, None)?;
    }

    // The combined index integrates BEFORE the reduce reads it, so a prefix seek
    // returns the post-delta extreme directly. (The trace_in integrate below runs
    // after the reduce and carries no index.)
    if !avi_table_ptr.is_null() {
        let bake_idx = ctx
            .builder
            .add_avi_bake(crate::ops::AviBake::new(&in_reg_schema, &gcols_u32, &avi_aggs));
        let avi = IntegrateAvi {
            table_idx: ctx.builder.table_idx(avi_table_ptr) as u16,
            bake_idx,
        };
        ctx.builder.push(Instr::Integrate {
            in_reg: in_reg_id as u16,
            table_idx: -1,
            avi: Some(avi),
        });
    }

    // Bake worker ownership of the global-aggregate seed, exactly as the
    // `WorkerFilter` arm bakes `(worker_rank(), num_workers())`. A worker seeds
    // when it holds the whole input: either the view is stamped replicated (it runs
    // correct-local everywhere and the read single-sources worker 0, which is not
    // `worker_for_key(V₀)`) or the reduce has no upstream `ExchangeShard`
    // (`reduce_multi_local`, also reachable over a partitioned table through the raw
    // `reduce()` binding — which is why the two tests stay separate). A sharded
    // global aggregate funnels every row to `worker_for_key(V₀)`, so only
    // that worker seeds. The shard test reads the static `loaded.incoming` graph,
    // which keeps the `ExchangeShard → Reduce` edge across the post-phase split (the
    // ExchangeShard node itself emits no instruction). Meaningful only when
    // `global_ground`; left `false` otherwise so a grouped reduce never pays the bake.
    let unsharded = !loaded.incoming.get(&nid).is_some_and(|ins| {
        ins.iter().any(|&(src, port)| {
            port == PORT_IN && matches!(loaded.nodes.get(&src), Some(gnitz_wire::OpNode::ExchangeShard { .. }))
        })
    });
    let i_am_owner = global_ground
        && (ctx.replicated()
            || unsharded
            || worker_rank() as usize
                == gnitz_wire::worker_for_key(crate::ops::global_group_key(), num_workers() as usize));

    let avi_table_idx = (!avi_table_ptr.is_null()).then(|| ctx.builder.table_idx(avi_table_ptr) as u16);

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
        plan_idx,
        avi_table_idx,
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
    // on demand — Distinct pushes 1, Reduce up to 2
    // (raw_delta + trace-in). Each node pushes at most 2, so reserving
    // `next_reg + 2 * ordered.len()` holds the whole program in a single
    // allocation, and that same bound — rejected here before the emit loop creates
    // any scratch tables — guarantees the final `reg_meta.len()` can never exceed
    // u16, so no register id truncates when cast.
    let reg_cap = next_reg as usize + 2 * ordered.len();
    if reg_cap > u16::MAX as usize {
        return Err(CompileError::Rejected("register count exceeds u16::MAX"));
    }
    let mut reg_meta = Vec::with_capacity(reg_cap);
    reg_meta.resize(next_reg as usize, RegisterMeta::delta(SchemaDescriptor::default()));

    for ((ex_nid, ex_schema), &(_, reg)) in exchange_inputs.iter().zip(&exchange_input_regs) {
        out_reg_of.insert(*ex_nid, reg);
        reg_meta[reg as usize] = RegisterMeta::delta(*ex_schema);
    }

    let mut ctx = EmitCtx {
        loaded,
        skip_nodes,
        ext_tables,
        view_dir,
        view_id,
        builder: ProgramBuilder::new(),
        out_reg_of,
        reg_meta,
        owned_tables: Vec::new(),
        owned_funcs: Vec::new(),
        source_reg_map: HashMap::new(),
        sink_reg_id: -1,
        can_emit_on_empty: false,
        scratch: ScratchGuard::new(),
    };

    // Instruction count after each node has emitted — the node → program offset
    // map, keyed by node id like `out_reg_of` so the two are read the same way.
    // Recorded in the loop rather than reconstructed afterwards, so a node that
    // emits nothing (an aliasing `Filter(None)`, an elided identity `Map`, a
    // skipped `Distinct`, a `ScanDelta`, an `IntegrateSink`) simply leaves the
    // running length unchanged and the offsets stay correct with no one reasoning
    // about which nodes emit.
    let mut instr_end: HashMap<i32, usize> = HashMap::with_capacity(ordered.len());
    for &nid in ordered {
        // `compile_view` excises every `ExchangeShard` from the `ordered` slice
        // (its `emit_node` arm is `unreachable!`). A node that fails to emit
        // (corrupt/unsupported catalog) stops the build at once; the ScratchGuard
        // removes the scratch dirs created so far, so a rejected compile leaks no
        // inodes (on success they are handed to the caller in PlanBuildResult,
        // kept alive by the VM's tables).
        let reg_id = *ctx.out_reg_of.get(&nid).unwrap();
        emit_node(&mut ctx, nid, reg_id)?;
        instr_end.insert(nid, ctx.builder.instructions().len());
    }

    ctx.builder.push(Instr::Halt);

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

    // Destructive-register ordering invariant. `Union` and `WeightClamp`
    // (distinct / positive_part) empty their input register in place
    // (`Batch::take`, to avoid allocation). Every register has one writer and its
    // readers run in instruction order, so a destructively-consumed register is
    // correct only when the destructive read is the LAST read of that register.
    // Checked over the EMITTED instructions with resolved registers, so register
    // aliasing from elided nodes — identity MAPs, `Filter(None)`
    // pass-throughs, skipped Distincts — is seen through rather
    // than reasoned about via graph edges. (A self-union reads in_a == in_b in
    // one instruction; the exec arm handles that before the take.) The resolved
    // sink register is read once more at epoch end (the output extraction), so
    // it counts as one trailing reader of every instruction.
    let instrs = ctx.builder.instructions();
    for (i, instr) in instrs.iter().enumerate() {
        let dtor_reg = match instr {
            Instr::Union { in_a, .. } => *in_a,
            Instr::WeightClamp { in_reg, .. } => *in_reg,
            _ => continue,
        };
        if dtor_reg as i32 == sink_reg || instrs[i + 1..].iter().any(|later| reads_reg(later, dtor_reg)) {
            gnitz_warn!(
                "build_plan: instruction {i} destructively consumes register {dtor_reg}, \
                 but a later instruction (or the epoch-end sink extraction) still reads \
                 it; every other reader of a destructively-consumed register must \
                 precede it"
            );
            return Err(CompileError::Rejected(
                "destructive register read is not the last reader",
            ));
        }
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
        source_reg_map,
        can_emit_on_empty,
        scratch,
        out_reg_of,
        ..
    } = ctx;
    let vm = builder.build_with_owned(reg_meta, owned_tables, owned_funcs);

    Ok(PlanBuildResult {
        vm,
        in_reg: input_delta_reg_id,
        out_reg: sink_reg,
        source_reg_map,
        can_emit_on_empty,
        exchange_input_regs,
        scratch,
        instr_end,
        out_reg_of,
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
    super::optimize::input_on_port(loaded, ex_nid, PORT_IN)
}
