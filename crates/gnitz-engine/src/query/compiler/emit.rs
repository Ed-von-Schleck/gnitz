//! Instruction emission: per-node `emit_*`, the predicate/map-plan
//! constructors, and `build_plan` (one plan, pre or post exchange).

use super::*;
use crate::query::vm::{Instr, TableIdx};
use gnitz_store::expr::PkSource;
use gnitz_store::ops::{merge_schemas_for_join, JoinProbe, RangeProbe};
use gnitz_store::schema::{DerivedSchema, SchemaColumn};

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
    b.push_pk(SchemaColumn::new(gnitz_store::schema::type_code::U128, 0))?;
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
            gnitz_store::storage::remove_child(d);
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
    pub placement: gnitz_store::schema::Placement,
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
    /// Create a child table in a [`gnitz_store::storage::ChildAddr::Scratch`]
    /// subdirectory of the view's directory, tracked by the scratch guard (so a
    /// later compile failure removes it).
    fn create_child_table(&mut self, child_name: &str, schema: SchemaDescriptor) -> Result<Table, CompileError> {
        let child_dir = gnitz_store::storage::ChildAddr::Scratch {
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
    /// (`bind_trace_cursors` opens a cursor on it each epoch). Returns no index:
    /// the register is how every instruction reaches the table
    /// ([`crate::query::vm::Program::trace_table_idx`]).
    fn add_trace_table(
        &mut self,
        child_name: &str,
        schema: SchemaDescriptor,
        trace_reg: u16,
    ) -> Result<(), CompileError> {
        let idx = self.add_registerless_table(child_name, schema)?;
        self.reg_meta[trace_reg as usize] = RegisterMeta::trace(schema, idx);
        Ok(())
    }

    /// Create a child table that **no** register names: only the baked reduce
    /// plan holding the returned `TableIdx` can reach it, so nothing else in the
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
            ctx.add_trace_table(&child_name, in_reg_schema, reg_id)?;
            let out_delta_id = ctx.push_delta_reg(in_reg_schema);
            ctx.out_reg_of.insert(nid, out_delta_id);
            ctx.builder.push(Instr::WeightClamp {
                in_reg,
                hist_reg: reg_id,
                out_reg: out_delta_id,
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
            emit_reduce(ctx, nid, reg_id, group_cols, agg, *global_ground, *out_key)?;
        }

        gnitz_wire::OpNode::Join(kind) => {
            let (delta, trace) = ctx.loaded.inputs(nid).binary();
            let (a_reg, b_reg) = (ctx.reg_of(delta)?, ctx.reg_of(trace)?);
            let a_schema = ctx.reg_meta[a_reg as usize].schema;
            let b_schema = ctx.reg_meta[b_reg as usize].schema;
            // `resolve_inputs` validates a Join's port arity but not the producer's
            // kind, so a hand-built circuit can feed the trace port from a `Filter`.
            // That register owns no table, gets no cursor from `bind_trace_cursors`,
            // and would meet the dispatch as a mid-epoch worker panic.
            if ctx.reg_meta[b_reg as usize].owned_table.is_none() {
                return Err(CompileError::Rejected("join: trace port is not an integral"));
            }
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
            ctx.add_trace_table(&child_name, in_reg_schema, reg_id)?;
            ctx.builder.push(Instr::Integrate {
                in_reg,
                trace_reg: reg_id,
            });
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

/// True iff `prog` reproduces its input row unchanged under `out_schema`: same
/// physical layout, and a block copy that skips exactly the inherited PK region
/// and carries payload columns 1:1. Such a MAP is elided entirely.
fn copies_input_verbatim(prog: &LogicalProgram, in_schema: &SchemaDescriptor, out_schema: &SchemaDescriptor) -> bool {
    in_schema.same_physical_layout(out_schema) && prog.sequential_copy_base() == Some(in_schema.pk_indices().len())
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
            // The only arm whose program is client bytes; the rest build theirs
            // from a column list. Rejected, not asserted: skipping a corrupt blob
            // would leave the output register at the default empty schema.
            let prog = LogicalProgram::from_map_blob(program, "map").map_err(expr_reject("map: invalid program"))?;
            (node_schema, prog, PkSource::Inherit)
        }

        gnitz_wire::MapKind::Reindex {
            keep,
            reindex_cols,
            reindex_target_tcs,
            ..
        } => {
            if oob_cols(reindex_cols.iter().copied(), &in_reg_schema) {
                return Err(CompileError::Rejected("map: reindex columns out of range"));
            }
            if oob_cols(keep.iter().copied(), &in_reg_schema) {
                return Err(CompileError::Rejected("map: reindex payload column out of range"));
            }
            // Must follow the in-bounds check: it reads `columns[c]`.
            if key_promotion_invalid(reindex_cols, reindex_target_tcs, &in_reg_schema) {
                return Err(CompileError::Rejected("map: invalid reindex promotion target"));
            }
            // The packer is built first because it *is* the layout: its output
            // schema reads the promoters the per-row pack writes through, so the
            // reindexed `_join_pk` and the delta scatter co-partition by
            // construction. Same packer the exchange scatter builds from `ViewMeta`.
            let packer = gnitz_store::schema::key::ReindexPacker::new(&in_reg_schema, reindex_cols, reindex_target_tcs)
                .ok_or(CompileError::Rejected("map: invalid reindex key"))?;
            let node_schema = packer
                .output_schema(&in_reg_schema, keep)
                .ok_or(CompileError::Rejected("map: reindex output exceeds MAX_COLUMNS"))?;
            (node_schema, LogicalProgram::copy_cols(keep), PkSource::Pack(packer))
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
    out_key: gnitz_store::schema::ReduceOutKey,
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
    let plan = gnitz_store::ops::ReducePlan::new(
        &in_reg_schema,
        group_cols,
        &agg_descs,
        out_key,
        global_ground,
        i_am_owner,
    )
    .map_err(CompileError::Rejected)?;
    let reduce_out_schema = plan.output_schema;

    ctx.add_trace_table(&format!("_reduce_{}_{nid}", ctx.site.id), reduce_out_schema, reg_id)?;

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

    let plan_idx = ctx.builder.add_reduce_plan(plan, avi);

    ctx.builder.push(Instr::Reduce {
        in_reg: in_reg_id,
        trace_out_reg: reg_id,
        out_reg: raw_delta_id,
        plan_idx,
    });

    ctx.builder.push(Instr::Integrate {
        in_reg: raw_delta_id,
        trace_reg: reg_id,
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
    placement: gnitz_store::schema::Placement,
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
        instr_end.insert(nid, ctx.builder.instr_count());
    }

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
    let vm = builder.build(reg_meta, sink_reg);

    Ok(PlanBuildResult {
        vm,
        in_reg: input_delta_reg_id,
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
#[path = "tests/emit.rs"]
mod tests;
