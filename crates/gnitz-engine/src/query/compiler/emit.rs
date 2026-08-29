//! Instruction emission: per-node `emit_*`, the predicate/map-plan
//! constructors, and `build_plan` (one plan, pre or post exchange).

use super::*;
use crate::expr::PkSource;
use crate::ops::{merge_schemas_for_join, JoinProbe, RangeProbe};
use crate::query::vm::{consume_slots, reads_reg, Instr, TableIdx};
use crate::schema::{DerivedSchema, SchemaColumn};

// ---------------------------------------------------------------------------
// Derived operator-output schemas
// ---------------------------------------------------------------------------
//
// The operators whose output layout has no independent writer to sit beside, so
// the emitter — their sole caller — is its home. Where a writer does exist the
// schema comes off that instead: `ReindexPacker::output_schema`,
// `ops::merge_schemas_for_join`, `build_reduce_output_schema`, `project_schema`.

/// Output schema of a HashRow (set-op full-row identity) Map: a synthetic U128
/// PK at slot 0, then the projected payload columns. `target_tcs[j] != 0`
/// promotes payload column `j` to that <=8-byte integer type (cross-width
/// set-op coercion) — `new` re-derives size/signedness for the promoted type —
/// keeping THIS SIDE's nullability. Per-side, not the operator-merged view
/// nullability: an INTERSECT/EXCEPT leaf is `distinct`-ed on its own before
/// the tuple-tightening combine, so its row comparator must classify by what
/// this side can actually emit.
fn hashrow_output_schema(
    in_schema: &SchemaDescriptor,
    proj_cols: &[u32],
    target_tcs: &[u8],
) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    b.push_pk(SchemaColumn::new(crate::schema::type_code::U128, 0))?;
    for (j, &c) in proj_cols.iter().enumerate() {
        let src = in_schema.columns[c as usize];
        let tgt = target_tcs.get(j).copied().unwrap_or(0);
        let out_tc = if tgt != 0 { tgt } else { src.type_code };
        b.push(SchemaColumn::new(out_tc, src.nullable))?;
    }
    Some(b.finish())
}

/// Both inputs of a `Union` must share a physical layout (equal column count, PK
/// indices, and per-column `type_code`); `None` rejects a circuit whose branches
/// do not. On success the result is `a`'s schema with each column's nullability
/// OR-ed with `b`'s, so a null-carrying side forces the null-aware `Generic` row
/// comparator instead of the null-blind `FixedIntNonnull` fast path (which orders
/// by raw payload bytes and would fail to coalesce two logically-NULL rows
/// carrying non-zero bytes under the null bit).
///
/// Built through `SchemaDescriptor::new` and not `DerivedSchema`, which forces
/// `pk_indices = 0..pk_len`: a `Union` input's PK need not be a column prefix.
fn union_nullability_merge(a: &SchemaDescriptor, b: &SchemaDescriptor) -> Option<SchemaDescriptor> {
    if !a.same_physical_layout(b) {
        return None;
    }
    let cols: Vec<SchemaColumn> = (0..a.num_columns())
        .map(|c| {
            let (ac, bc) = (a.columns[c], b.columns[c]);
            SchemaColumn::new(ac.type_code, ac.nullable | bc.nullable)
        })
        .collect();
    Some(SchemaDescriptor::new(&cols, a.pk_indices()))
}

/// Output schema of a computed-projection `Map`: the input's PK region (the map
/// inherits it verbatim, `PkSource::Inherit`), then one payload column per declared
/// `(type_code, nullable)` slot. `decode_op_node` rejects an undecodable type
/// code, so every entry is a real column type.
fn compute_map_output_schema(in_schema: &SchemaDescriptor, out_cols: &[(u8, bool)]) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    b.push_pk_of(in_schema)?;
    for &(tc, nullable) in out_cols {
        b.push(SchemaColumn::new(tc, nullable as u8))?;
    }
    Some(b.finish())
}

/// Output schema of an outer-join NULL_EXTEND: the input schema verbatim (PK
/// region unchanged), then one nullable column per null-fill `type_codes` entry.
/// `decode_op_node` rejects an undecodable type code, so every entry is a real
/// column type.
fn null_extend_output_schema(in_schema: &SchemaDescriptor, type_codes: &[u8]) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    b.push_pk_of(in_schema)?;
    for (_, c) in in_schema.payload_columns() {
        b.push(*c)?;
    }
    for &tc in type_codes {
        b.push(SchemaColumn::new(tc, 1))?;
    }
    Some(b.finish())
}

// ---------------------------------------------------------------------------
// Expression construction helpers
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
pub(super) fn oob_cols(cols: impl IntoIterator<Item = u32>, schema: &SchemaDescriptor) -> bool {
    cols.into_iter().any(|c| c as usize >= schema.num_columns())
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
    pub(super) fn new() -> Self {
        ScratchGuard(Vec::new())
    }
    fn track(&mut self, dir: String) {
        self.0.push(dir);
    }
    pub(super) fn defuse(&mut self) {
        self.0.clear();
    }
}

impl Drop for ScratchGuard {
    fn drop(&mut self) {
        for d in &self.0 {
            // `remove_child`, not a bare `remove_dir_all`: a scratch child that
            // resumed a checkpointed manifest at the current generation is trusted
            // by the next open, and `remove_dir_all` deletes in readdir order — so
            // a crash mid-removal could leave that manifest pointing at shards
            // that are already gone.
            crate::storage::remove_child(d);
        }
    }
}

// ---------------------------------------------------------------------------
// EmitCtx — the per-plan build state every emit arm works against
// ---------------------------------------------------------------------------

/// All state of one `build_plan` invocation. Owned by `build_plan` and threaded
/// to the emit arms as one `&mut` instead of a dozen parallel parameters.
pub(super) struct EmitCtx<'a> {
    pub loaded: &'a LoadedCircuit,
    /// The view's placement, stamped at registration. Every `ScanDelta` source of
    /// a replicated view is replicated too, so the view runs correct-local on
    /// every worker: the `WorkerFilter` arm emits nothing (the trim would drop
    /// rows this worker legitimately owns a copy of), and `emit_reduce` makes
    /// every worker the owner of the global-aggregate seed.
    pub placement: crate::schema::Placement,
    /// Nodes the optimizer elided. A pure function of `loaded` — derived here
    /// rather than threaded in, so no caller can hand a build a skip set that
    /// disagrees with its circuit.
    pub skip_nodes: HashSet<i32>,
    pub ext_tables: &'a dyn SchemaSource,
    pub site: super::ViewSite<'a>,
    pub builder: ProgramBuilder,
    pub out_reg_of: HashMap<i32, u16>,
    pub reg_meta: Vec<RegisterMeta>,
    pub source_reg_map: HashMap<i64, u16>,
    pub sink_reg_id: Option<u16>,
    /// Set by `emit_reduce` when it emits a global-ground aggregate — the one
    /// operator that produces output from an empty input epoch. See
    /// `SubPlan::can_emit_on_empty`.
    pub can_emit_on_empty: bool,
    pub scratch: ScratchGuard,
}

impl EmitCtx<'_> {
    /// Create a child table in a [`crate::storage::ChildAddr::Scratch`]
    /// subdirectory of the view's directory, tracked by the scratch guard (so a
    /// later compile failure removes it).
    fn create_child_table(&mut self, child_name: &str, schema: SchemaDescriptor) -> Result<Table, CompileError> {
        let child_dir = crate::storage::ChildAddr::Scratch {
            child: child_name,
            rank: worker_rank(),
        }
        .dir(self.site.dir);
        // Track the path before creating so cleanup also removes a partially
        // created directory if Table::new fails.
        self.scratch.track(child_dir.clone());
        Table::new(&child_dir, schema, self.site.id as u32, self.site.recovery)
            .map_err(|e| CompileError::StorageFailed("child table create failed", e))
    }

    /// Create a child table backing `trace_reg`, which becomes a trace register
    /// (`bind_trace_cursors` opens a cursor on it each epoch).
    fn add_trace_table(
        &mut self,
        child_name: &str,
        schema: SchemaDescriptor,
        trace_reg: u16,
    ) -> Result<TableIdx, CompileError> {
        let idx = self.add_registerless_table(child_name, schema)?;
        self.reg_meta[trace_reg as usize] = RegisterMeta::trace(schema, idx);
        Ok(idx)
    }

    /// Create a child table that **no** register names: only the one instruction
    /// holding the returned `TableIdx` can reach it, so nothing else in the
    /// program can read or write it and no cursor is bound to it per epoch.
    fn add_registerless_table(&mut self, child_name: &str, schema: SchemaDescriptor) -> Result<TableIdx, CompileError> {
        let t = self.create_child_table(child_name, schema)?;
        Ok(self.builder.push_table(t))
    }

    /// The register `src` produced. The one rejection left after `topo_sorted`
    /// held every edge set to `OpNode::ports()`: a plan is built over a *slice* of
    /// the circuit, and a producer outside this side's slice has no register.
    /// Falling back to node 0's register instead would be the wrong-results
    /// failure class every other emit guard exists to prevent.
    fn reg_of(&self, src: i32) -> Result<u16, CompileError> {
        self.out_reg_of
            .get(&src)
            .copied()
            .ok_or(CompileError::Rejected("operand is produced outside this plan"))
    }

    /// The register feeding a unary operator.
    fn unary_in(&self, nid: i32) -> Result<u16, CompileError> {
        self.reg_of(self.loaded.inputs(nid).unary())
    }

    /// Allocate a fresh delta register and return its id.
    fn push_delta_reg(&mut self, schema: SchemaDescriptor) -> u16 {
        let id = self.reg_meta.len() as u16;
        self.reg_meta.push(RegisterMeta::delta(schema));
        id
    }
}

// ---------------------------------------------------------------------------
// Instruction emission — per-node handler
// ---------------------------------------------------------------------------

pub(super) fn emit_node(ctx: &mut EmitCtx, nid: i32, op: &gnitz_wire::OpNode, reg_id: u16) -> Result<(), CompileError> {
    match op {
        // `bound` is a backfill-scan hint consumed by the source drive, not by the
        // VM: emission is identical bounded or not.
        gnitz_wire::OpNode::ScanDelta { source: tid, .. } => {
            // A circuit scanning an unknown table is corrupt: the planner
            // registers every source before shipping the circuit.
            let schema = ctx
                .ext_tables
                .schema_of(*tid as i64)
                .ok_or(CompileError::Rejected("scan-delta: unknown source table"))?;
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(schema);
            ctx.source_reg_map.insert(*tid as i64, reg_id);
        }

        gnitz_wire::OpNode::Filter(blob) => {
            let in_reg = ctx.unary_in(nid)?;
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
            let pred = LogicalProgram::from_blob(blob, "filter")
                .and_then(|p| p.resolve_filter(&in_schema))
                .map_err(expr_reject("filter: invalid predicate program"))?;
            let pred_idx = ctx.builder.push_predicate(pred);
            ctx.builder.push(Instr::Filter {
                in_reg,
                out_reg: reg_id,
                pred_idx,
            });
        }

        gnitz_wire::OpNode::Map(mk) => {
            emit_map(ctx, nid, reg_id, mk)?;
        }

        gnitz_wire::OpNode::Negate => {
            let in_reg = ctx.unary_in(nid)?;
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(ctx.reg_meta[in_reg as usize].schema);
            ctx.builder.push(Instr::Negate {
                in_reg,
                out_reg: reg_id,
                // Non-consuming is always correct, sometimes one clone too many;
                // `build_plan`'s liveness pass upgrades every destructive flag
                // once the whole instruction list exists.
                consume: false,
            });
        }

        gnitz_wire::OpNode::Union => {
            let (a, b) = ctx.loaded.inputs(nid).binary();
            let (in_a, in_b) = (ctx.reg_of(a)?, ctx.reg_of(b)?);
            let a_schema = ctx.reg_meta[in_a as usize].schema;
            let out_schema = union_nullability_merge(&a_schema, &ctx.reg_meta[in_b as usize].schema)
                .ok_or(CompileError::Rejected("union: inputs do not share a physical layout"))?;
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(out_schema);
            ctx.builder.push(Instr::Union {
                in_a,
                in_b,
                out_reg: reg_id,
                consume_a: false, // see `Instr::Negate` above
                consume_b: false,
            });
        }

        gnitz_wire::OpNode::Distinct | gnitz_wire::OpNode::PositivePart => {
            let in_reg = ctx.unary_in(nid)?;
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
            let child_name = format!("_hist_{}_{nid}", ctx.site.id);
            let hist_table_idx = ctx.add_trace_table(&child_name, in_reg_schema, reg_id)?;
            let out_delta_id = ctx.push_delta_reg(in_reg_schema);
            ctx.out_reg_of.insert(nid, out_delta_id);
            ctx.builder.push(Instr::WeightClamp {
                in_reg,
                hist_reg: reg_id,
                out_reg: out_delta_id,
                hist_table_idx,
                lo,
                hi,
                consume: false, // see `Instr::Negate` above
            });
        }

        gnitz_wire::OpNode::Reduce {
            group_cols,
            agg,
            global_ground,
            out_key,
        } => {
            emit_reduce(ctx, nid, reg_id, group_cols, agg, *global_ground, *out_key)?;
        }

        gnitz_wire::OpNode::Join(kind) => {
            let (delta, trace) = ctx.loaded.inputs(nid).binary();
            let (a_reg, b_reg) = (ctx.reg_of(delta)?, ctx.reg_of(trace)?);
            let a_schema = ctx.reg_meta[a_reg as usize].schema;
            let b_schema = ctx.reg_meta[b_reg as usize].schema;
            // Both kinds produce the same output layout — only the probe differs —
            // so the schema, the register meta and the operand registers are shared.
            let probe = match kind {
                gnitz_wire::JoinKind::DeltaTrace => JoinProbe::Equi,
                gnitz_wire::JoinKind::DeltaTraceRange { n_eq, rel } => JoinProbe::Range(
                    RangeProbe::new(&a_schema, &b_schema, *n_eq, *rel).map_err(CompileError::Rejected)?,
                ),
            };
            let out_schema = merge_schemas_for_join(&a_schema, &b_schema)
                .ok_or(CompileError::Rejected("join: merged schema exceeds MAX_COLUMNS"))?;
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(out_schema);
            ctx.builder.push(Instr::JoinDT {
                delta_reg: a_reg,
                trace_reg: b_reg,
                out_reg: reg_id,
                probe,
            });
        }

        gnitz_wire::OpNode::IntegrateSink => {
            // Emits no instruction: the sink register's batch is what
            // `execute_epoch_multi` extracts at epoch end.
            let in_reg = ctx.unary_in(nid)?;
            ctx.sink_reg_id = Some(in_reg);
        }

        gnitz_wire::OpNode::IntegrateTrace => {
            let in_reg = ctx.unary_in(nid)?;
            let in_reg_schema = ctx.reg_meta[in_reg as usize].schema;
            let child_name = format!("_int_{}_{nid}", ctx.site.id);
            // Must fail the compile on a table-open error: emitting the view without
            // the Integrate would compile a view that never persists its differential
            // state, leaving its output permanently empty.
            let table_idx = ctx.add_trace_table(&child_name, in_reg_schema, reg_id)?;
            ctx.builder.push(Instr::Integrate { in_reg, table_idx });
        }

        gnitz_wire::OpNode::ExchangeShard { .. } => {
            // `compile_view` excises the exchange nids from the *post* phase's
            // node list, but each side's list is the ancestors of its own
            // exchange input with no such filter — so an exchange upstream of
            // another exchange's input stays in that side's list and arrives
            // here. No planner path emits that shape; a circuit hand-built
            // through `gnitz_core::CircuitBuilder` can, and rejecting is what
            // keeps it from aborting a worker.
            return Err(CompileError::Rejected("chained exchange nodes"));
        }

        gnitz_wire::OpNode::WorkerFilter => {
            // Pass-through schema; drops the rows this worker does not own before
            // they reach `integrate_trace`. Worker identity is the compile-time
            // `(worker_rank, num_workers)` of this process. An all-replicated view
            // runs correct-local over the full broadcast on every worker, so it
            // integrates the full input rather than trimming — the same outcome the
            // single-worker case reaches, so both alias the input register and emit
            // no instruction at all.
            let in_reg = ctx.unary_in(nid)?;
            if ctx.placement.is_replicated() || num_workers() <= 1 {
                // One worker owns every partition, so the filter is the identity —
                // and executing it would clone the whole delta each epoch. Alias
                // the input register instead, as an absent WHERE does.
                ctx.out_reg_of.insert(nid, in_reg);
                return Ok(());
            }
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(ctx.reg_meta[in_reg as usize].schema);
            ctx.builder.push(Instr::WorkerFilter {
                in_reg,
                out_reg: reg_id,
                worker_id: worker_rank(),
                num_workers: num_workers(),
            });
        }

        gnitz_wire::OpNode::NullExtend { type_codes } => {
            let in_reg = ctx.unary_in(nid)?;
            let in_schema = ctx.reg_meta[in_reg as usize].schema;
            // The output schema is built here once and homed in `reg_meta`; the
            // op derives its appended-column count from it.
            let out_schema = null_extend_output_schema(&in_schema, type_codes)
                .ok_or(CompileError::Rejected("null-extend: merged schema exceeds MAX_COLUMNS"))?;
            ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(out_schema);
            ctx.builder.push(Instr::NullExtend {
                in_reg,
                out_reg: reg_id,
            });
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// MAP emission
// ---------------------------------------------------------------------------

/// Decode a MAP's expression blob and lower it. Rejected, not asserted: circuits
/// are client-supplied catalog data, and skipping a corrupt blob would leave the
/// output register at the default empty schema — silently wrong results downstream.
fn decode_map_program(program: &[u8]) -> Result<LogicalProgram, CompileError> {
    LogicalProgram::from_map_blob(program, "map").map_err(expr_reject("map: invalid program"))
}

/// True iff `prog` reproduces its input row unchanged under `out_schema`: same
/// physical layout, and a block copy that skips exactly the inherited PK region
/// and carries payload columns 1:1. Such a MAP is elided entirely.
fn copies_input_verbatim(prog: &LogicalProgram, in_schema: &SchemaDescriptor, out_schema: &SchemaDescriptor) -> bool {
    in_schema.same_physical_layout(out_schema) && prog.sequential_copy_base() == Some(in_schema.pk_indices().len())
}

/// The in-range source columns of a program that is one `COPY_COL(src, out)` per
/// payload column with dense outs `0..n` — the shape the planner's
/// `build_reindex_program` is the sole producer of. Reading the kept-column list
/// off the program keeps the program the single source of truth for the reindex
/// output schema; deriving a schema the program does not fully write would leave
/// uninitialized output slots.
fn dense_copy_srcs(prog: &LogicalProgram, in_schema: &SchemaDescriptor) -> Result<Vec<u32>, CompileError> {
    let srcs = prog
        .payload_copy_srcs()
        .ok_or(CompileError::Rejected("map: reindex program is not a dense copy list"))?;
    if oob_cols(srcs.iter().copied(), in_schema) {
        return Err(CompileError::Rejected("map: reindex payload column out of range"));
    }
    Ok(srcs)
}

/// The `MapKind`s differ only in how they derive `(output schema, map program,
/// PK source)`; building the `MapPlan`, eliding an identity and the emission are
/// shared. Each arm hands its `LogicalProgram` down rather than
/// consuming it, so the shared exit's identity check sees every arm.
fn emit_map(ctx: &mut EmitCtx, nid: i32, reg_id: u16, mk: &gnitz_wire::MapKind) -> Result<(), CompileError> {
    let in_reg = ctx.unary_in(nid)?;
    let in_reg_schema = ctx.reg_meta[in_reg as usize].schema;
    let (node_schema, prog, pk_source) = match mk {
        gnitz_wire::MapKind::Compute { program, out_cols } => {
            // The declared payload slots ARE the layout: a computed projection has
            // no dense copy list to derive one from. `from_map` below validates the
            // program against them, which is what catches a false declaration.
            let node_schema = compute_map_output_schema(&in_reg_schema, out_cols)
                .ok_or(CompileError::Rejected("compute map: output exceeds MAX_COLUMNS"))?;
            (node_schema, decode_map_program(program)?, PkSource::Inherit)
        }

        gnitz_wire::MapKind::Reindex {
            program,
            reindex_cols,
            reindex_target_tcs,
            ..
        } => {
            // One decode serves the copy-list scan below and `from_map`; the scan
            // never touches the const pool, but `from_map` does.
            let prog = decode_map_program(program)?;
            if oob_cols(reindex_cols.iter().copied(), &in_reg_schema) {
                return Err(CompileError::Rejected("map: reindex columns out of range"));
            }
            // Must follow the in-bounds check: it reads `columns[c]`.
            if key_promotion_invalid(reindex_cols, reindex_target_tcs, &in_reg_schema) {
                return Err(CompileError::Rejected("map: invalid reindex promotion target"));
            }
            // The packer is built first because it *is* the layout: its output
            // schema reads the promoters the per-row pack writes through, so the
            // reindexed `_join_pk` and the delta scatter co-partition by
            // construction. Same packer the exchange scatter builds from `ViewMeta`.
            let packer = crate::schema::key::ReindexPacker::new(&in_reg_schema, reindex_cols, reindex_target_tcs)
                .ok_or(CompileError::Rejected("map: invalid reindex key"))?;
            let node_schema = packer
                .output_schema(&in_reg_schema, &dense_copy_srcs(&prog, &in_reg_schema)?)
                .ok_or(CompileError::Rejected("map: reindex output exceeds MAX_COLUMNS"))?;
            (node_schema, prog, PkSource::Pack(packer))
        }

        gnitz_wire::MapKind::HashRow(proj_cols, target_tcs, branch_id) => {
            // `hashrow_output_schema` declares the synthetic U128 PK the map
            // hashes, and `create_universal_projection` widens each source into
            // its (possibly promoted) slot — so both set-op sides hash one
            // physical layout.
            if oob_cols(proj_cols.iter().copied(), &in_reg_schema) {
                return Err(CompileError::Rejected("hash-row map: columns out of range"));
            }
            if payload_promotion_invalid(proj_cols, target_tcs, &in_reg_schema) {
                return Err(CompileError::Rejected("hash-row map: invalid promotion target"));
            }
            let node_schema = hashrow_output_schema(&in_reg_schema, proj_cols, target_tcs)
                .ok_or(CompileError::Rejected("hash-row map: output exceeds MAX_COLUMNS"))?;
            (
                node_schema,
                LogicalProgram::copy_cols(proj_cols),
                PkSource::HashRow { branch_id: *branch_id },
            )
        }

        gnitz_wire::MapKind::Projection(cols) => {
            if oob_cols(cols.iter().copied(), &in_reg_schema) {
                return Err(CompileError::Rejected("projection map: columns out of range"));
            }
            // `oob_cols` bounds each index but not the list length, and
            // duplicates are allowed, so a long list would overrun the
            // fixed `[_; MAX_COLUMNS]` schema array — the bound is
            // PK-inclusive and lives inside the builder.
            let node_schema = project_schema(&in_reg_schema, cols)
                .ok_or(CompileError::Rejected("projection map: output exceeds MAX_COLUMNS"))?;
            (node_schema, LogicalProgram::copy_cols(cols), PkSource::Inherit)
        }
    };

    // A MAP that reproduces its input row verbatim emits nothing; the node's
    // consumers read the input register instead. A reindex overwrites every row's
    // PK, so it is never an identity.
    if matches!(pk_source, PkSource::Inherit) && copies_input_verbatim(&prog, &in_reg_schema, &node_schema) {
        ctx.out_reg_of.insert(nid, in_reg);
        return Ok(());
    }
    // A `PkSource::Inherit` map copies the input PK region verbatim, so the two
    // strides must agree.
    if matches!(pk_source, PkSource::Inherit) && node_schema.pk_stride() != in_reg_schema.pk_stride() {
        return Err(CompileError::Rejected("map: output PK stride differs from the input's"));
    }
    // For the projection arms the output schema is derived from a client column
    // list rather than supplied, and `project_schema` drops PK sources while
    // `copy_cols` numbers destinations densely — so a PK index leaves a copy
    // addressing a slot that does not exist. This is what catches it.
    let plan = MapPlan::from_map(prog, &in_reg_schema, &node_schema, pk_source)
        .map_err(expr_reject("map: program/schema mismatch"))?;
    let map_idx = ctx.builder.push_map(plan);

    ctx.reg_meta[reg_id as usize] = RegisterMeta::delta(node_schema);
    ctx.builder.push(Instr::Map {
        in_reg,
        out_reg: reg_id,
        map_idx,
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// REDUCE emission
// ---------------------------------------------------------------------------

pub(super) fn emit_reduce(
    ctx: &mut EmitCtx,
    nid: i32,
    reg_id: u16,
    group_cols: &[u32],
    agg: &[(gnitz_wire::AggFunc, u32)],
    global_ground: bool,
    out_key: crate::schema::ReduceOutKey,
) -> Result<(), CompileError> {
    let loaded = ctx.loaded;
    let in_reg_id = ctx.unary_in(nid)?;
    let in_reg_schema = ctx.reg_meta[in_reg_id as usize].schema;

    // Raw wire column indices below index the fixed `[_; 65]` schema array. Reject
    // an out-of-range group column or aggregate column before `agg_descs` is built
    // (which reads `columns[col_idx]`), so a crafted/corrupt node fails the compile
    // rather than reading a zeroed slot or aborting at the first push.
    if oob_cols(group_cols.iter().copied(), &in_reg_schema) {
        return Err(CompileError::Rejected("reduce: group columns out of range"));
    }
    debug_assert!(!agg.is_empty(), "decode_op_node rejects a spec-less REDUCE");
    ctx.can_emit_on_empty |= global_ground;
    if oob_cols(agg.iter().map(|&(_, c)| c), &in_reg_schema) {
        return Err(CompileError::Rejected("reduce: aggregate column out of range"));
    }

    let agg_descs: Vec<AggDescriptor> = agg
        .iter()
        .map(|&(agg_op, col_idx)| AggDescriptor { col_idx, agg_op })
        .collect();

    // The output layout and `op_reduce`'s row keying both obey `out_key`, so a
    // kind the schema does not warrant would silently scramble the output columns.
    if out_key != in_reg_schema.reduce_out_key(group_cols) {
        return Err(CompileError::Rejected("reduce: out_key does not match input schema"));
    }

    // A worker owns the global-aggregate ground row when it holds the whole
    // input: the view is replicated (correct-local everywhere, read
    // single-sourced from worker 0), or nothing shards into this reduce, or it is
    // the one worker a sharded funnel routes V₀ to. `ReducePlan::new` conjoins
    // this with `global_ground`, so a grouped reduce cannot carry a live seed.
    let unsharded = !matches!(
        loaded.nodes.get(&loaded.inputs(nid).unary()),
        Some(gnitz_wire::OpNode::ExchangeShard { .. })
    );
    let i_am_owner = ctx.placement.is_replicated()
        || unsharded
        || worker_rank() as usize == gnitz_wire::worker_for_key(gnitz_wire::global_group_key(), num_workers() as usize);

    // `oob_cols` above bounds each column index but not the list lengths, and the
    // SQL binder — which the low-level CircuitBuilder path bypasses — is not the
    // only thing that must reject an unaggregatable column type. `ReducePlan::new`
    // owns both, so a bad circuit fails the compile instead of aborting a worker.
    let plan = crate::ops::ReducePlan::new(
        &in_reg_schema,
        group_cols,
        &agg_descs,
        out_key,
        global_ground,
        i_am_owner,
    )
    .map_err(CompileError::Rejected)?;
    let reduce_out_schema = plan.output_schema;

    let trace_table_idx = ctx.add_trace_table(&format!("_reduce_{}_{nid}", ctx.site.id), reduce_out_schema, reg_id)?;

    let raw_delta_id = ctx.push_delta_reg(reduce_out_schema);
    ctx.out_reg_of.insert(nid, raw_delta_id);

    // One table per reduce, serving every MIN/MAX of it — so per-aggregate entries
    // share a table_id, scratch dir and compaction namespace and cannot collide on
    // a memory-pressure flush. `?`, not a swallowed failure: a non-linear reduce
    // has no other history, and would otherwise compute MIN/MAX from the delta
    // alone while still retracting the old row.
    let avi = match &plan.avi {
        Some(bake) => Some(ctx.add_registerless_table(&format!("_avidx_{}_{nid}", ctx.site.id), bake.schema)?),
        None => None,
    };

    let plan_idx = ctx.builder.add_reduce_plan(plan);

    ctx.builder.push(Instr::Reduce {
        in_reg: in_reg_id,
        trace_out_reg: reg_id,
        out_reg: raw_delta_id,
        plan_idx,
        avi,
    });

    ctx.builder.push(Instr::Integrate {
        in_reg: raw_delta_id,
        table_idx: trace_table_idx,
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// build_plan — one plan, pre or post exchange
// ---------------------------------------------------------------------------

pub(super) fn build_plan(
    loaded: &LoadedCircuit,
    ordered: &[i32],
    ext_tables: &dyn SchemaSource,
    site: super::ViewSite<'_>,
    placement: crate::schema::Placement,
    target: PlanTarget,
) -> Result<PlanBuildResult, CompileError> {
    let exchange_inputs: &[(i32, SchemaDescriptor)] = match target {
        PlanTarget::ViewOutput { seeds, .. } => seeds,
        PlanTarget::Subgraph { .. } => &[],
    };
    // Register ids are u16 instruction fields. `reg_meta` gets one base register
    // per node plus one seed per exchange input, and `push_delta_reg` — its only
    // growth site — runs at most once per node. Rejected here, before the first id
    // is handed out and before the emit loop creates any scratch table, so every
    // register id below fits `u16`; the post-loop assert holds the bound.
    let reg_cap = 2 * ordered.len() + exchange_inputs.len();
    if reg_cap > u16::MAX as usize {
        return Err(CompileError::Rejected("register count exceeds u16::MAX"));
    }

    let mut out_reg_of: HashMap<i32, u16> = HashMap::new();
    let mut next_reg: u16 = 0;
    for &nid in ordered {
        out_reg_of.insert(nid, next_reg);
        next_reg += 1;
    }

    // One seed register per exchange input (the post phase of an exchange view
    // reads each side's relayed batch from its own register).
    let mut exchange_input_regs: Vec<u16> = Vec::with_capacity(exchange_inputs.len());
    let first_exchange_input_reg_id: Option<u16> = (!exchange_inputs.is_empty()).then_some(next_reg);
    for _ in exchange_inputs {
        exchange_input_regs.push(next_reg);
        next_reg += 1;
    }

    let mut reg_meta = Vec::with_capacity(reg_cap);
    // Filler for slots the loop below overwrites: every live register's meta is
    // assigned by its own emit arm, and every exchange-input slot two lines on.
    reg_meta.resize(next_reg as usize, RegisterMeta::delta(SchemaDescriptor::minimal_u64()));

    for ((ex_nid, ex_schema), &reg) in exchange_inputs.iter().zip(&exchange_input_regs) {
        out_reg_of.insert(*ex_nid, reg);
        reg_meta[reg as usize] = RegisterMeta::delta(*ex_schema);
    }

    let mut ctx = EmitCtx {
        loaded,
        placement,
        skip_nodes: compute_skip_nodes(loaded),
        ext_tables,
        site,
        builder: ProgramBuilder::new(),
        out_reg_of,
        reg_meta,
        source_reg_map: HashMap::new(),
        sink_reg_id: None,
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
        let reg_id = *ctx.out_reg_of.get(&nid).unwrap();
        let op = loaded
            .nodes
            .get(&nid)
            .expect("topo_sorted builds `ordered` out of `nodes`' own keys");
        emit_node(&mut ctx, nid, op, reg_id)?;
        instr_end.insert(nid, ctx.builder.instructions().len());
    }

    ctx.builder.push(Instr::Halt);

    // The exchange seeds come first; failing that, the plan is driven from a
    // source register. `min_by_key` rather than an arbitrary map entry so a
    // multi-source plan picks the same register on every worker.
    let input_delta_reg_id = first_exchange_input_reg_id
        .or_else(|| {
            ctx.source_reg_map
                .iter()
                .min_by_key(|&(&tid, _)| tid)
                .map(|(_, &reg)| reg)
        })
        .ok_or(CompileError::Rejected("plan has no input delta register"))?;

    let sink_reg = match target {
        PlanTarget::ViewOutput { .. } => ctx.sink_reg_id,
        // A side's node list is the ancestors of its exchange input, so the sink
        // is downstream of the shard and outside it. One that got in would give
        // the plan an output register that is not `out`.
        PlanTarget::Subgraph { out } => match ctx.sink_reg_id {
            Some(_) => return Err(CompileError::Rejected("subgraph contains the sink")),
            None => ctx.out_reg_of.get(&out).copied(),
        },
    }
    .ok_or(CompileError::Rejected("plan has no output register"))?;

    // Destructive-register liveness: `consume_slots` names the registers an opcode
    // could empty; this decides whether it may. Over the EMITTED instructions, so
    // register aliasing from elided nodes is seen through rather than re-derived
    // from graph edges. The sink is a reader no instruction spells.
    let instrs = ctx.builder.instructions_mut();
    for i in 0..instrs.len() {
        let (head, tail) = instrs.split_at_mut(i + 1);
        for (reg, consume) in consume_slots(&mut head[i]).into_iter().flatten() {
            *consume = reg != sink_reg && !tail.iter().any(|later| reads_reg(later, reg));
        }
    }

    if let PlanTarget::ViewOutput { out_schema, .. } = target {
        let sink_schema = &ctx.reg_meta[sink_reg as usize].schema;
        // A column-count match is not enough: two schemas with equal column
        // counts but mismatched types (e.g. I64 vs German-string) let the client
        // read a 16-byte string descriptor out of 8-byte integer storage.
        if !sink_schema.same_physical_layout(out_schema) {
            return Err(CompileError::Rejected("sink schema does not match view output schema"));
        }
    }

    debug_assert!(
        ctx.reg_meta.len() <= reg_cap,
        "emission grew reg_meta past the reservation, so a register id may not fit u16",
    );
    // The reservation is the worst case (every node pushing its extra register);
    // the typical plan uses two or three. `reg_meta` is moved verbatim
    // into `Program` and held for as long as the plan stays cached, and a
    // `RegisterMeta` is a whole `SchemaDescriptor`, so the unused tail would be
    // tens of KB of dead heap per sub-plan, per view, per worker.
    ctx.reg_meta.shrink_to_fit();

    let EmitCtx {
        builder,
        reg_meta,
        source_reg_map,
        can_emit_on_empty,
        scratch,
        out_reg_of,
        ..
    } = ctx;
    let vm = builder.build(reg_meta);

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
        queue.extend(loaded.inputs(cur).iter());
    }
    set
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn};

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
        let m = union_nullability_merge(&nonnull, &nullable).expect("shared layout");
        assert_eq!(m.columns[1].nullable, 1, "OR of non-nullable and nullable = nullable");
        assert_eq!(m.payload_cmp, PayloadCmpKind::Generic);

        // Both non-nullable → stays on the FixedIntNonnull fast path (byte-identical).
        let m2 = union_nullability_merge(&nonnull, &nonnull).expect("shared layout");
        assert_eq!(m2.columns[1].nullable, 0);
        assert_eq!(m2.payload_cmp, PayloadCmpKind::FixedIntNonnull);

        // Nullable A + non-nullable B → Generic too (OR is symmetric).
        let m3 = union_nullability_merge(&nullable, &nonnull).expect("shared layout");
        assert_eq!(m3.columns[1].nullable, 1);
        assert_eq!(m3.payload_cmp, PayloadCmpKind::Generic);
    }

    /// `union_nullability_merge` is the Union arm's whole layout contract, and in
    /// release there is nothing else: a mismatched pair would adopt `a`'s schema
    /// and let `op_union` read `b`'s bytes through it.
    #[test]
    fn test_union_mismatched_input_layout_rejected() {
        let one = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        let two = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, scan_delta(11));
        nodes.insert(2, gnitz_wire::OpNode::Union);
        let loaded = loaded_for_test(nodes, vec![(0, 2, PORT_IN_A), (1, 2, PORT_IN_B)]);

        // `Subgraph` so the union guard is the only one that can fire — the sink
        // contract never runs.
        let plan = |b: SchemaDescriptor| {
            build_plan(
                &loaded,
                &subgraph_ordered(&loaded, 2),
                &HashMap::from([(10, one), (11, b)]),
                test_site("", 1),
                crate::schema::Placement::KEYED_DEFAULT,
                PlanTarget::Subgraph { out: 2 },
            )
        };
        assert!(plan(one).is_ok(), "a matched pair must compile");
        match plan(two) {
            Err(CompileError::Rejected(guard)) => {
                assert_eq!(guard, "union: inputs do not share a physical layout")
            }
            other => panic!("expected a rejection, got {:?}", other.map(|_| "a plan")),
        }
    }

    /// A `Subgraph`'s output register is the named node's, and a node list that
    /// reached the sink is rejected — production carves an exchange side out of
    /// the shard input's ancestors, so the sink is never in one.
    #[test]
    fn test_subgraph_output_is_the_named_node() {
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, gnitz_wire::OpNode::Negate);
        nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]);
        let ext: ExtTables = HashMap::from([(10, schema)]);
        let plan = |ordered: &[i32]| {
            build_plan(
                &loaded,
                ordered,
                &ext,
                test_site("", 1),
                crate::schema::Placement::KEYED_DEFAULT,
                PlanTarget::Subgraph { out: 1 },
            )
        };

        let carved = plan(&subgraph_ordered(&loaded, 1)).expect("the carve production performs");
        assert_eq!(
            carved.out_reg,
            *carved.out_reg_of.get(&1).unwrap(),
            "a subgraph outputs the register of the node it names"
        );
        assert!(
            matches!(
                plan(&loaded.ordered),
                Err(CompileError::Rejected("subgraph contains the sink"))
            ),
            "a node list reaching the sink is a mis-carve, not a plan"
        );
    }

    /// A sink register whose schema is not the view's output schema is rejected
    /// rather than emitted: the view store would then be written through a
    /// descriptor its rows do not match.
    #[test]
    fn test_mismatched_sink_schema_rejected() {
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, gnitz_wire::OpNode::IntegrateSink);
        let view_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        // The scan source carries an extra payload column, so the sink register
        // reaches `build_plan` with a schema the view's does not equal.
        let source_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN)]);

        let result = build_plan(
            &loaded,
            &loaded.ordered,
            &HashMap::from([(10, source_schema)]),
            test_site("", 1),
            crate::schema::Placement::KEYED_DEFAULT,
            PlanTarget::ViewOutput {
                out_schema: &view_schema,
                seeds: &[],
            },
        );
        match result {
            Err(CompileError::Rejected(guard)) => {
                assert_eq!(guard, "sink schema does not match view output schema")
            }
            other => panic!("expected a rejection, got {:?}", other.map(|_| "a plan")),
        }
    }

    #[test]
    fn test_build_plan_register_overflow_rejected() {
        // A circuit producing > u16::MAX registers must fail the compile rather
        // than wrap the u16 cast and panic in ProgramBuilder::build.
        let n = u16::MAX as i32 + 1; // 65536 nodes → 65536 registers
        let mut nodes = HashMap::from([(0, scan_delta(10))]);
        let mut edges = Vec::new();
        for nid in 1..n {
            nodes.insert(nid, gnitz_wire::OpNode::Negate);
            edges.push((nid - 1, nid, PORT_IN));
        }
        let loaded = loaded_for_test(nodes, edges);
        assert_eq!(loaded.ordered.len(), n as usize);
        let result = build_plan(
            &loaded,
            &loaded.ordered,
            &HashMap::new(),
            test_site("", 1),
            crate::schema::Placement::KEYED_DEFAULT,
            PlanTarget::ViewOutput {
                out_schema: &SchemaDescriptor::minimal_u64(),
                seeds: &[],
            },
        );
        assert!(
            result.is_err(),
            "build_plan must fail when register count exceeds u16::MAX"
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
        let compiles = |in_schema: SchemaDescriptor, group: Vec<u32>, out_key: ReduceOutKey| -> bool {
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
            (eq_pk, vec![0u32], ReduceOutKey::PkPermutation, "eqpk"),
            (single_nat, vec![1u32], ReduceOutKey::SingleNaturalCol, "single"),
            (synthetic, vec![1u32], ReduceOutKey::SyntheticFold, "synth"),
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

    #[test]
    fn test_build_plan_wide_pk_join_accepted() {
        // After byte-API port: wide-PK Join(DeltaTrace) must compile successfully.
        // ScanDelta(wide) --port0--> Join(DT) <--port1-- IntegrateTrace(wide)
        // Join(DT) --> IntegrateSink.
        // 3 × U64 = a 24-byte PK, wide.
        let schema = crate::test_support::pk_only_schema(&[type_code::U64; 3]);
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
                &subgraph_ordered(&loaded, 2),
                &ext,
                test_site(view_dir, 1),
                crate::schema::Placement::KEYED_DEFAULT,
                PlanTarget::Subgraph { out: 2 }
            )
            .is_ok(),
            "wide-PK Join(DeltaTrace) must compile after byte-API port"
        );
    }

    // ── Item 32: sink schema type validation ────────────────────────────────

    #[test]
    fn test_build_plan_sink_schema_type_mismatch_rejected() {
        // ScanDelta(99) → IntegrateSink. The source schema is [U64 pk, I64];
        // the view's declared out_schema is [U64 pk, STRING]. Same column count,
        // different physical layout → must be rejected, else the client
        // reads a 16-byte string descriptor out of 8-byte integer storage.
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(99));
        nodes.insert(1, gnitz_wire::OpNode::IntegrateSink);
        let edges = vec![(0, 1, PORT_IN)];
        let loaded = loaded_for_test(nodes, edges);
        let view_schema = SchemaDescriptor::new(
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
            &loaded.ordered,
            &ext,
            test_site("", 99),
            crate::schema::Placement::KEYED_DEFAULT,
            PlanTarget::ViewOutput {
                out_schema: &view_schema,
                seeds: &[],
            },
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
        let corrupt = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Compute {
            program: vec![0xFFu8; 16],
            out_cols: vec![],
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
        let out_schema = crate::schema::key::ReindexPacker::new(&in_schema, &[0, 1], &[])
            .unwrap()
            .output_schema(&in_schema, &[0, 1])
            .unwrap();
        let map = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex {
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
        let reindex_cols: Vec<u32> = (0..n_cols as u32).collect();

        let map = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex {
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
        let out_schema = crate::schema::key::ReindexPacker::new(&in_schema, &[0], &[])
            .unwrap()
            .output_schema(&in_schema, &[2])
            .unwrap();
        let map = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex {
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
        let map = gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex {
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
            &subgraph_ordered(&loaded, 2),
            &ext,
            test_site(view_dir.to_str().unwrap(), 1),
            crate::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: 2 },
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
        let ex_in = loaded.inputs(3).unary();
        let set = ancestors_inclusive(&loaded, ex_in);
        let side_ordered: Vec<i32> = loaded.ordered.iter().copied().filter(|n| set.contains(n)).collect();
        assert!(
            side_ordered.contains(&1),
            "fixture must place the upstream shard inside the side's node list, got {side_ordered:?}"
        );

        let result = build_plan(
            &loaded,
            &side_ordered,
            &ext,
            test_site(dir.path().to_str().unwrap(), 1),
            crate::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: ex_in },
        );
        assert!(
            matches!(result, Err(CompileError::Rejected("chained exchange nodes"))),
            "chained exchange must be a named rejection"
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
        /// Where the mid node's scratch children are created. `None` = a fresh
        /// tempdir; a bad path is how a test reaches `create_child_table`'s
        /// failure arm.
        dir: Option<String>,
    }

    impl MidCircuit {
        fn new(in_schema: SchemaDescriptor) -> Self {
            MidCircuit {
                in_schema,
                out_schema: None,
                dir: None,
            }
        }

        /// Hold the sink to `out_schema`. Left unset, `build_plan` is entered on
        /// its `PlanTarget::Subgraph` path, which suppresses the sink-schema
        /// contract — what a test isolating a *mid-node* guard wants, since the mid
        /// node's output schema is exactly what it is varying.
        fn with_out_schema(mut self, out_schema: SchemaDescriptor) -> Self {
            self.out_schema = Some(out_schema);
            self
        }

        /// Home the scratch children at `dir` instead of a fresh tempdir.
        fn with_dir(mut self, dir: &str) -> Self {
            self.dir = Some(dir.to_owned());
            self
        }

        fn build(&self, mid: gnitz_wire::OpNode) -> Result<PlanBuildResult, CompileError> {
            let tmp = tempfile::tempdir().unwrap();
            let dir = self
                .dir
                .clone()
                .unwrap_or_else(|| tmp.path().to_str().unwrap().to_owned());
            let mut nodes = HashMap::new();
            nodes.insert(0, scan_delta(10));
            nodes.insert(1, mid);
            nodes.insert(2, gnitz_wire::OpNode::IntegrateSink);
            let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN)]);
            let ext: ExtTables = HashMap::from([(10, self.in_schema)]);
            let (ordered, target) = match &self.out_schema {
                Some(out_schema) => (
                    loaded.ordered.clone(),
                    PlanTarget::ViewOutput { out_schema, seeds: &[] },
                ),
                None => (subgraph_ordered(&loaded, 1), PlanTarget::Subgraph { out: 1 }),
            };
            build_plan(
                &loaded,
                &ordered,
                &ext,
                test_site(&dir, 1),
                crate::schema::Placement::KEYED_DEFAULT,
                target,
            )
        }

        fn compiles(&self, mid: gnitz_wire::OpNode) -> bool {
            self.build(mid).is_ok()
        }
    }

    /// `ScanDelta(10) ⋈range IntegrateTrace(ScanDelta(11)) → IntegrateSink`,
    /// compiled against the two given source schemas. Reports whether it
    /// compiles.
    fn range_join_plan(
        delta_schema: SchemaDescriptor,
        trace_schema: SchemaDescriptor,
        n_eq: u8,
    ) -> Result<PlanBuildResult, CompileError> {
        use gnitz_wire::{JoinKind, OpNode, RangeRel};
        let dir = tempfile::tempdir().unwrap();
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, scan_delta(11));
        nodes.insert(2, OpNode::IntegrateTrace);
        nodes.insert(
            3,
            OpNode::Join(JoinKind::DeltaTraceRange {
                n_eq,
                rel: RangeRel::Lt,
            }),
        );
        nodes.insert(4, OpNode::IntegrateSink);
        let loaded = loaded_for_test(
            nodes,
            vec![(0, 3, PORT_IN_A), (1, 2, PORT_IN), (2, 3, PORT_TRACE), (3, 4, PORT_IN)],
        );
        let ext: ExtTables = HashMap::from([(10, delta_schema), (11, trace_schema)]);
        build_plan(
            &loaded,
            &subgraph_ordered(&loaded, 3),
            &ext,
            test_site(dir.path().to_str().unwrap(), 1),
            crate::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: 3 },
        )
    }

    /// The range probe's preconditions are enforced at compile time, not by a
    /// `debug_assert` a release build strips. The walk slices both sides' PK
    /// regions at one equality width, so a crafted circuit whose two sides
    /// reindex to different strides — or whose `n_eq` leaves no range slot — must
    /// be rejected rather than reach the operator.
    #[test]
    fn test_range_join_probe_preconditions_rejected() {
        // The shape the reindex packer produces: `[eq slot, range slot]` PK, then
        // payload.
        let band = |eq_tc: u8| {
            SchemaDescriptor::new(
                &[
                    SchemaColumn::new(eq_tc, 0),
                    SchemaColumn::new(type_code::U64, 0),
                    SchemaColumn::new(type_code::I64, 0),
                ],
                &[0, 1],
            )
        };
        let wide = band(type_code::U64); // pk_stride 16
        let narrow = band(type_code::U32); // pk_stride 12
        let rejection = |d, t, n_eq| match range_join_plan(d, t, n_eq) {
            Err(CompileError::Rejected(guard)) => guard,
            other => panic!("expected a rejection, got {:?}", other.map(|_| "a plan")),
        };
        assert!(
            range_join_plan(wide, wide, 1).is_ok(),
            "a matched pair at the common promoted type must compile"
        );
        const STRIDE: &str =
            "range join: delta and trace PK strides differ (both sides must reindex at the pair's common type)";
        assert_eq!(rejection(narrow, wide, 1), STRIDE, "delta side narrower than the trace");
        assert_eq!(rejection(wide, narrow, 1), STRIDE, "delta side wider than the trace");

        // `leading_key_size` sums *schema*-order columns, so a PK-last column
        // order — which the packer never emits but a crafted circuit can name —
        // puts the whole 8-byte key inside the `n_eq = 1` prefix while the key
        // arity is still `n_eq + 1`. That leaves no range slot.
        let pk_last = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::U32, 0),
            ],
            &[1, 2],
        );
        assert_eq!(
            rejection(pk_last, pk_last, 1),
            "range join: eq prefix covers the whole key",
            "an eq prefix leaving no range slot",
        );
    }

    /// Build `ScanDelta(10) → mid → IntegrateSink` and report whether it compiles.
    fn compiles_mid_node(in_schema: SchemaDescriptor, mid: gnitz_wire::OpNode) -> bool {
        MidCircuit::new(in_schema).compiles(mid)
    }

    /// The guard that rejected `ScanDelta(10) → mid → IntegrateSink`. Naming it is
    /// what makes a guard test attributable: a bare `is_err()` also passes when an
    /// unrelated guard fires, which is why these used to need a control build.
    fn mid_node_rejection(in_schema: SchemaDescriptor, mid: gnitz_wire::OpNode) -> String {
        match MidCircuit::new(in_schema).build(mid) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected a rejection, got a plan"),
        }
    }

    #[test]
    fn test_reduce_group_cols_out_of_bounds_rejected() {
        use crate::schema::ReduceOutKey;
        use gnitz_wire::{AggFunc, OpNode};
        let reduce = |group: Vec<u32>| OpNode::Reduce {
            group_cols: group,
            agg: vec![(AggFunc::Count, 0)],
            global_ground: false,
            out_key: ReduceOutKey::PkPermutation,
        };
        assert_eq!(
            mid_node_rejection(two_col_schema(), reduce(vec![200])),
            "reduce: group columns out of range"
        );
    }

    #[test]
    fn test_reduce_agg_spec_col_out_of_bounds_rejected() {
        use crate::schema::ReduceOutKey;
        use gnitz_wire::{AggFunc, OpNode};
        let reduce = |col: u32| OpNode::Reduce {
            group_cols: vec![0],
            agg: vec![(AggFunc::Count, col)],
            global_ground: false,
            out_key: ReduceOutKey::PkPermutation,
        };
        assert_eq!(
            mid_node_rejection(two_col_schema(), reduce(200)),
            "reduce: aggregate column out of range"
        );
    }

    /// Every aggregate that decodes its column value needs a scalar register
    /// image (`ScalarKind`) — the ≤8-byte int/float set. The SQL binder rejects
    /// the rest upstream, so this covers the low-level `CircuitBuilder` path
    /// that bypasses it.
    #[test]
    fn test_value_reading_aggregate_over_non_encodable_column_rejected() {
        use crate::schema::ReduceOutKey;
        use gnitz_wire::{AggFunc, OpNode};
        // col 0 = U64 PK and the whole group key (⇒ PkPermutation); col 1 = the
        // aggregate column, whose type is the only thing varying.
        let schema = |agg_tc: u8| {
            SchemaDescriptor::new(
                &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(agg_tc, 0)],
                &[0],
            )
        };
        let reduce = |func| OpNode::Reduce {
            group_cols: vec![0],
            agg: vec![(func, 1)],
            global_ground: false,
            out_key: ReduceOutKey::PkPermutation,
        };
        for func in [AggFunc::Sum, AggFunc::SumZero, AggFunc::Min, AggFunc::Max] {
            assert!(
                compiles_mid_node(schema(type_code::I64), reduce(func)),
                "{func:?} over I64"
            );
            for tc in [type_code::U128, type_code::STRING] {
                assert_eq!(
                    mid_node_rejection(schema(tc), reduce(func)),
                    "reduce: aggregate column type has no scalar register image",
                    "{func:?} over type code {tc}",
                );
            }
        }
        // COUNT never reads the value, so no type excludes it.
        assert!(compiles_mid_node(schema(type_code::STRING), reduce(AggFunc::Count)));
    }

    #[test]
    fn test_projection_col_out_of_bounds_rejected() {
        use gnitz_wire::{MapKind, OpNode};
        let rejection = |cols: Vec<u32>| mid_node_rejection(two_col_schema(), OpNode::Map(MapKind::Projection(cols)));
        assert_eq!(rejection(vec![200]), "projection map: columns out of range");
        // A PK source: `project_schema` drops it while `copy_cols`
        // numbers destinations densely, so the copy addresses a slot that does
        // not exist — `from_map` would index past the fixed `[_; 65]`.
        assert!(rejection(vec![0]).starts_with("map: program/schema mismatch"));
        // `oob_cols` bounds each index but not the list length, and duplicates
        // are legal, so a long list overruns `project_schema`'s array.
        // Exactly MAX_COLUMNS payload sources already overflow — the schema also
        // carries the input's PK column, which a length-only bound misses.
        assert_eq!(
            rejection(vec![1; crate::schema::MAX_COLUMNS]),
            "projection map: output exceeds MAX_COLUMNS"
        );
    }

    #[test]
    fn test_null_extend_overflow_rejected() {
        use gnitz_wire::OpNode;
        const GUARD: &str = "null-extend: merged schema exceeds MAX_COLUMNS";
        let extend = |n: usize| OpNode::NullExtend {
            type_codes: vec![type_code::I64; n],
        };
        // A short type_codes list null-extends cleanly.
        assert!(compiles_mid_node(two_col_schema(), extend(1)));
        // MAX_COLUMNS type_codes overflow the fixed `[_; 65]` schema array.
        assert_eq!(
            mid_node_rejection(two_col_schema(), extend(crate::schema::MAX_COLUMNS)),
            GUARD
        );
        // (An undecodable type code is rejected at the wire decode boundary,
        // where the two sibling type-code lists are also validated.)
        // A near-max-width input plus a short extension overflows the *merged*
        // output width, which a bound on the list length alone cannot catch:
        // 64 + 2 > 65.
        let wide = {
            let mut cols = [SchemaColumn::new(type_code::I64, 0); 64];
            cols[0] = SchemaColumn::new(type_code::U64, 0);
            SchemaDescriptor::new(&cols, &[0])
        };
        assert_eq!(mid_node_rejection(wide, extend(2)), GUARD);
    }

    /// A view site over a throwaway directory: nothing under it was ever
    /// checkpointed, so the children it opens resume nothing.
    /// The node list of a subgraph ending at `out` — the same carve
    /// `compile_view` performs for an exchange side, so a fixture cannot hand
    /// `build_plan` a list production would never produce (one holding the sink).
    fn subgraph_ordered(loaded: &LoadedCircuit, out: i32) -> Vec<i32> {
        let set = ancestors_inclusive(loaded, out);
        loaded.ordered.iter().copied().filter(|n| set.contains(n)).collect()
    }

    fn test_site(dir: &str, id: u64) -> ViewSite<'_> {
        ViewSite {
            dir,
            id,
            recovery: RecoverySource::Rederive { resume_at: None },
        }
    }

    // ── Destructive-register liveness ───────────────────────────────────────
    //
    // A register may be emptied in place iff it has no later reader and is not the
    // sink the epoch extracts — a property of the emitted list, decided per input
    // register rather than per view.

    /// Every `consume` verdict in program order, labelled by its slot; a `Union`
    /// contributes both operands.
    fn consume_flags(plan: &PlanBuildResult) -> Vec<(&'static str, bool)> {
        plan.vm
            .program
            .instructions
            .iter()
            .flat_map(|i| match i {
                Instr::Union {
                    consume_a, consume_b, ..
                } => vec![("union.a", *consume_a), ("union.b", *consume_b)],
                Instr::WeightClamp { consume, .. } => vec![("clamp", *consume)],
                Instr::Negate { consume, .. } => vec![("negate", *consume)],
                _ => vec![],
            })
            .collect()
    }

    /// The INTERSECT/EXCEPT fan-out shape: ScanDelta(10)'s register fans into both
    /// a destructive `Distinct` and a non-destructive `Negate` co-reader (standing
    /// in for integrate_trace). Kahn's ascending tie-break schedules the lower id
    /// first, so the ids decide which consumer runs first. (The reader is a
    /// `Negate`, not a `Filter(None)`: a predicate-less Filter is elided by
    /// register aliasing and would no longer read the register at runtime.)
    fn make_dtor_fanout(distinct_id: i32, reader_id: i32) -> LoadedCircuit {
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(distinct_id, gnitz_wire::OpNode::Distinct);
        nodes.insert(reader_id, gnitz_wire::OpNode::Negate);
        let edges = vec![(0, distinct_id, PORT_IN), (0, reader_id, PORT_IN)];
        loaded_for_test(nodes, edges)
    }

    #[test]
    fn a_destructive_op_takes_its_input_only_when_it_is_the_last_reader() {
        let dir = tempfile::tempdir().unwrap();
        let ext: ExtTables = HashMap::from([(10, two_col_schema())]);
        let flags = |distinct_id: i32, reader_id: i32| {
            let loaded = make_dtor_fanout(distinct_id, reader_id);
            let plan = build_plan(
                &loaded,
                &loaded.ordered,
                &ext,
                test_site(dir.path().to_str().unwrap(), 1),
                crate::schema::Placement::KEYED_DEFAULT,
                PlanTarget::Subgraph { out: distinct_id },
            )
            .expect("both orderings compile");
            consume_flags(&plan)
        };
        assert_eq!(
            flags(2, 1),
            vec![("negate", false), ("clamp", true)],
            "the co-reader ran first and had to clone; the clamp's take is then free"
        );
        assert_eq!(
            flags(1, 2),
            vec![("clamp", false), ("negate", true)],
            "the clamp runs first while the co-reader still has to read, so it must clone"
        );
    }

    /// The set-operation shape: two sources meet at one `Union`, neither operand has
    /// a later reader, so both sides are taken. `consume_b` carries as much as
    /// `consume_a`: one operand is empty every epoch and the other is returned whole.
    #[test]
    fn a_union_of_two_unread_operands_takes_both_sides() {
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, scan_delta(11));
        nodes.insert(2, gnitz_wire::OpNode::Union);
        let loaded = loaded_for_test(nodes, vec![(0, 2, PORT_IN_A), (1, 2, PORT_IN_B)]);
        let plan = build_plan(
            &loaded,
            &loaded.ordered,
            &HashMap::from([(10, two_col_schema()), (11, two_col_schema())]),
            test_site("", 1),
            crate::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: 2 },
        )
        .expect("a two-source union compiles");
        assert_eq!(consume_flags(&plan), vec![("union.a", true), ("union.b", true)]);
    }

    /// The epoch-end output extraction reads the sink register, and it is not an
    /// instruction — so a `Union` whose operand *is* the sink would otherwise take
    /// it empty and emit nothing, silently, every epoch.
    #[test]
    fn a_union_over_the_sink_register_does_not_take_it() {
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, gnitz_wire::OpNode::Negate);
        nodes.insert(2, gnitz_wire::OpNode::Union);
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN_A), (0, 2, PORT_IN_B)]);
        let plan = build_plan(
            &loaded,
            &loaded.ordered,
            &HashMap::from([(10, two_col_schema())]),
            test_site("", 1),
            crate::schema::Placement::KEYED_DEFAULT,
            PlanTarget::Subgraph { out: 1 },
        )
        .expect("a union over the plan's own output register compiles");
        assert_eq!(
            consume_flags(&plan),
            vec![("negate", false), ("union.a", false), ("union.b", true)],
            "operand A is the sink and must not be taken; the Negate's own input is \
             still read by operand B, which has no later reader of its own",
        );
    }

    #[test]
    fn test_destructive_fanout_skipped_distinct_not_rejected() {
        use crate::schema::ReduceOutKey;
        // `ScanDelta → Reduce → {Distinct, Negate}`: the Distinct schedules before
        // its co-reader, the destructive-first shape the guard rejects. But a
        // Reduce's output is already distinct, so the elision pass drops the
        // Distinct — it aliases the Reduce's register and emits no destructive op,
        // and the guard must not reject it.
        let dir = tempfile::tempdir().unwrap();
        let view_dir = dir.path().to_str().unwrap();

        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(
            1,
            gnitz_wire::OpNode::Reduce {
                group_cols: vec![],
                agg: vec![(gnitz_wire::AggFunc::Count, 0)],
                global_ground: false,
                out_key: ReduceOutKey::SyntheticFold,
            },
        );
        nodes.insert(2, gnitz_wire::OpNode::Distinct);
        nodes.insert(3, gnitz_wire::OpNode::Negate);
        let loaded = loaded_for_test(nodes, vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (1, 3, PORT_IN)]);
        assert!(
            compute_skip_nodes(&loaded).contains(&2),
            "test precondition: the elision pass must drop the Reduce-fed Distinct"
        );

        let ext: ExtTables = HashMap::from([(10, two_col_schema())]);
        // The plan owns scratch dirs under `dir`, so it must drop first: build it
        // inside the assert rather than binding it past `dir`'s scope.
        assert!(
            build_plan(
                &loaded,
                &loaded.ordered,
                &ext,
                test_site(view_dir, 1),
                crate::schema::Placement::KEYED_DEFAULT,
                PlanTarget::Subgraph { out: 2 }
            )
            .is_ok(),
            "a skipped (optimized-out) Distinct does not run destructively; \
             the guard must not reject it"
        );
    }

    /// Every operator that creates a scratch child must fail the compile when the
    /// creation fails, never emit a plan without it: a dropped `Integrate` would
    /// compile a view that never persists its differential state, leaving its
    /// output permanently empty.
    #[test]
    fn test_build_plan_child_table_failure_rejected() {
        let one_col = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        let fixture = MidCircuit::new(one_col).with_dir("/nonexistent_gnitz_test_path_xyz_abc");
        for mid in [gnitz_wire::OpNode::Distinct, gnitz_wire::OpNode::IntegrateTrace] {
            assert!(
                matches!(fixture.build(mid.clone()), Err(CompileError::StorageFailed(..))),
                "{mid:?} must fail the compile when its child table cannot be created",
            );
        }
    }

    /// The two derived map out-schemas are by construction ones `MapPlan`
    /// accepts. `HashRow` is the only site that emits a *promoting* `CopyCol`,
    /// so it is what makes `check_copy_types`' widening clause do work; the
    /// reindex case pins that `payload_copy_srcs` and
    /// `ReindexPacker::output_schema` cannot drift apart into a mixed-type copy.
    #[test]
    fn test_derived_map_schemas_satisfy_copy_types() {
        use crate::expr::{MapPlan, PkSource};
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
        let reindexed = crate::schema::key::ReindexPacker::new(&in_schema, &[4], &[type_code::U64])
            .unwrap()
            .output_schema(&in_schema, &payload_cols)
            .unwrap();
        assert!(MapPlan::from_map(prog(), &in_schema, &reindexed, PkSource::Inherit).is_ok());

        // A cross-width set-op coercion: the U32 column promoted to I64, every
        // other column carried verbatim (target 0). The promotion is one
        // `payload_promotion_invalid` admits, so a real HashRow can build it.
        let tcs = vec![0, 0, 0, 0, type_code::I64];
        let wire_cols: Vec<u32> = cols.to_vec();
        assert!(!optimize::payload_promotion_invalid(&wire_cols, &tcs, &in_schema));
        let hashed = hashrow_output_schema(&in_schema, &cols, &tcs).unwrap();
        assert!(MapPlan::from_map(prog(), &in_schema, &hashed, PkSource::Inherit).is_ok());
    }
}
