//! Instruction emission: per-node `emit_*`, the compile-time guards they reject
//! through, and `build_plan` (one plan, pre or post exchange).

use super::*;
use crate::query::vm::{Instr, TableIdx};
use gnitz_store::expr::PkSource;
use gnitz_store::ops::{merge_schemas_for_join, JoinProbe, RangeProbe};
use gnitz_store::schema::{DerivedSchema, SchemaColumn};

// ---------------------------------------------------------------------------
// Derived operator-output schemas
// ---------------------------------------------------------------------------
//
// One caller each — the emit arm below it. A schema with a home of its own is
// read from there instead (`ReindexPacker::output_schema`,
// `ops::merge_schemas_for_join`, `build_reduce_output_schema`, `project_schema`).

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

/// `a`'s schema with each column's nullability OR-ed with `b`'s, so a
/// null-carrying side forces the null-aware `Generic` row comparator instead of
/// the null-blind `FixedIntNonnull` one, which would fail to coalesce two
/// logically-NULL rows carrying different bytes under the null bit. `None` when
/// the branches do not share a physical layout.
///
/// Not `DerivedSchema`, which forces `pk_indices = 0..pk_len`: a `Union` input's
/// PK need not be a column prefix.
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
// Compile-time guards
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

/// True iff any carried target in `target_tcs` is invalid for its source column
/// in `cols` under `valid`, the domain predicate of the promotion's destination.
/// A violation means a corrupt/forged catalog; callers abort the compile cleanly
/// rather than panic/truncate in the copy kernels. An out-of-range column is a
/// violation too, so the check is total — the callers' own `oob_cols` runs first
/// only to name the more specific guard. A zero target carries no promotion and
/// is always accepted. Shared body of [`key_promotion_invalid`] and
/// [`payload_promotion_invalid`], which differ only in `valid`.
fn promotion_invalid(
    cols: &[u32],
    target_tcs: &[u8],
    schema: &SchemaDescriptor,
    valid: impl Fn(u8, u8) -> bool,
) -> bool {
    cols.iter().enumerate().any(|(i, &c)| {
        let t = target_tcs.get(i).copied().unwrap_or(0);
        t != 0
            && !schema
                .columns
                .get(c as usize)
                .is_some_and(|col| valid(col.type_code, t))
    })
}

/// The reindex **key** domain: `t` must be the promotion the planner derives for
/// a key of this source type. Read back off the planner's own rule rather than
/// re-deriving the sign/width ladder — `t` is value-preserving for `src` iff
/// `join_key_common_type(src, t) == Some(t)` — which also screens PK-ineligible
/// targets for free, since that function only yields PK-eligible types.
fn key_promotion_invalid(cols: &[u32], target_tcs: &[u8], schema: &SchemaDescriptor) -> bool {
    promotion_invalid(cols, target_tcs, schema, |src, t| {
        gnitz_wire::join_key_common_type(src, t) == Some(t)
    })
}

/// The **payload** copy domain: the ≤8-byte fixed-int widen, which is the only
/// promotion the copy kernel supports. Identical to the rule `check_copy_types`
/// holds a column sink's destination to — the HashRow payload widen is that same
/// kernel — so it is narrower than [`key_promotion_invalid`], not a mode of it.
pub(super) fn payload_promotion_invalid(cols: &[u32], target_tcs: &[u8], schema: &SchemaDescriptor) -> bool {
    promotion_invalid(cols, target_tcs, schema, gnitz_wire::is_widening_promotion)
}

// ---------------------------------------------------------------------------
// ScratchGuard — drop-based cleanup of a failed compile's scratch directories
// ---------------------------------------------------------------------------

/// Scratch directories created during a plan build, removed on drop — so any
/// failed compile path leaks no inodes. On success the guard is `defuse`d and
/// the directories stay alive under the VM's owned tables.
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
    pub(in crate::query) loaded: &'a LoadedCircuit,
    /// The view's placement, stamped at registration. Every `ScanDelta` source of
    /// a replicated view is replicated too, so the view runs correct-local on
    /// every worker: the `WorkerFilter` arm emits nothing (the trim would drop
    /// rows this worker legitimately owns a copy of), and `emit_reduce` makes
    /// every worker the owner of the global-aggregate seed.
    pub(in crate::query) placement: gnitz_store::schema::Placement,
    pub(in crate::query) ext_tables: &'a dyn SchemaSource,
    pub(in crate::query) site: super::ViewSite<'a>,
    pub(in crate::query) builder: ProgramBuilder,
    pub(in crate::query) out_reg_of: HashMap<i32, u16>,
    pub(in crate::query) reg_meta: Vec<RegisterMeta>,
    pub(in crate::query) source_reg_map: FxHashMap<i64, u16>,
    pub(in crate::query) sink_reg_id: Option<u16>,
    pub(in crate::query) scratch: ScratchGuard,
}

impl EmitCtx<'_> {
    /// Create a child table in a [`gnitz_store::storage::ChildAddr::Scratch`]
    /// subdirectory of the view's directory, tracked by the scratch guard (so a
    /// later compile failure removes it).
    fn create_child_table(&mut self, child_name: &str, schema: SchemaDescriptor) -> Result<Table, CompileError> {
        let child_dir = gnitz_store::storage::ChildAddr::Scratch {
            child: child_name,
            rank: self.site.slot.rank,
        }
        .dir(self.site.dir);
        // Track the path before creating so cleanup also removes a partially
        // created directory if the open fails.
        self.scratch.track(child_dir.clone());
        Table::with_budgets(
            &child_dir,
            schema,
            self.site.id as u32,
            self.site.recovery,
            self.site.ram,
        )
        .map_err(|e| CompileError::StorageFailed("child table create failed", e))
    }

    /// Allocate a trace register and the child table backing it
    /// (`bind_trace_cursors` opens a cursor on it each epoch). Returns the
    /// register and no index: the register is how every instruction reaches the
    /// table ([`crate::query::vm::Program::trace_table_idx`]).
    fn push_trace_reg(&mut self, child_name: &str, schema: SchemaDescriptor) -> Result<u16, CompileError> {
        let idx = self.add_registerless_table(child_name, schema)?;
        let id = self.reg_meta.len() as u16;
        self.reg_meta.push(RegisterMeta::trace(schema, idx));
        Ok(id)
    }

    /// Create a child table that **no** register names: only the baked reduce
    /// plan holding the returned `TableIdx` can reach it, so nothing else in the
    /// program can read or write it and no cursor is bound to it per epoch.
    fn add_registerless_table(&mut self, child_name: &str, schema: SchemaDescriptor) -> Result<TableIdx, CompileError> {
        let t = self.create_child_table(child_name, schema)?;
        Ok(self.builder.push_table(t))
    }

    /// The register `src` produced. The one rejection left after `topo_sorted`
    /// held every edge set to `OpNode::ports()`: a plan covers a *slice* of the
    /// circuit, so a producer outside this side has no register at all.
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

/// Emit `nid`'s instructions and return the register its output lands in — the
/// node's own fresh register, or an input's when the node emits nothing and
/// aliases it (`Filter(None)`, an identity `Map`, an elided `Distinct`, a
/// `WorkerFilter` this worker cannot narrow, the sink). Returning it is what
/// keeps a register from being reserved for a node that never writes one.
pub(super) fn emit_node(ctx: &mut EmitCtx, nid: i32, op: &gnitz_wire::OpNode) -> Result<u16, CompileError> {
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
            let reg = ctx.push_delta_reg(schema);
            ctx.source_reg_map.insert(*tid as i64, reg);
            Ok(reg)
        }

        gnitz_wire::OpNode::Filter(blob) => {
            let in_reg = ctx.unary_in(nid)?;
            let Some(blob) = blob else {
                // Absent blob = no WHERE clause. Pass-through: alias the input
                // register instead of emitting a clone-the-batch instruction.
                return Ok(in_reg);
            };
            let in_schema = ctx.reg_meta[in_reg as usize].schema;
            // A present-but-corrupt blob, or a rejected program, is catalog
            // corruption. Falling back to pass-all would silently turn a WHERE
            // into WHERE TRUE; fail the compile instead.
            let pred = LogicalProgram::from_blob(blob, "filter")
                .and_then(|p| p.resolve_filter(&in_schema))
                .map_err(expr_reject("filter: invalid predicate program"))?;
            let pred_idx = ctx.builder.push_predicate(pred);
            let out_reg = ctx.push_delta_reg(in_schema);
            ctx.builder.push(Instr::Filter {
                in_reg,
                out_reg,
                pred_idx,
            });
            Ok(out_reg)
        }

        gnitz_wire::OpNode::Map(mk) => emit_map(ctx, nid, mk),

        gnitz_wire::OpNode::Negate => {
            let in_reg = ctx.unary_in(nid)?;
            let out_reg = ctx.push_delta_reg(ctx.reg_meta[in_reg as usize].schema);
            ctx.builder.push(Instr::Negate { in_reg, out_reg });
            Ok(out_reg)
        }

        gnitz_wire::OpNode::Union => {
            let (a, b) = ctx.loaded.inputs(nid).binary();
            let (in_a, in_b) = (ctx.reg_of(a)?, ctx.reg_of(b)?);
            let a_schema = ctx.reg_meta[in_a as usize].schema;
            let out_schema = union_nullability_merge(&a_schema, &ctx.reg_meta[in_b as usize].schema)
                .ok_or(CompileError::Rejected("union: inputs do not share a physical layout"))?;
            let out_reg = ctx.push_delta_reg(out_schema);
            ctx.builder.push(Instr::Union { in_a, in_b, out_reg });
            Ok(out_reg)
        }

        gnitz_wire::OpNode::Distinct | gnitz_wire::OpNode::PositivePart => {
            let in_reg = ctx.unary_in(nid)?;
            let in_reg_schema = ctx.reg_meta[in_reg as usize].schema;
            // `distinct` is the only one the optimizer elides (its input is already
            // distinct); `positive_part` is never seeded into the skip set, so
            // this check is simply false for it.
            if ctx.loaded.skip_nodes.contains(&nid) {
                return Ok(in_reg);
            }
            // Set-membership clamp `[-1, 1]` for distinct; bag clamp `[0, i64::MAX]`
            // (negative part only) for positive_part. The two presets are the sole
            // difference between the operators; both emit one `WeightClamp` instr.
            let (lo, hi) = if matches!(op, gnitz_wire::OpNode::PositivePart) {
                (0, i64::MAX)
            } else {
                (-1, 1)
            };
            let hist_reg = ctx.push_trace_reg(&format!("_hist_{}_{nid}", ctx.site.id), in_reg_schema)?;
            let out_reg = ctx.push_delta_reg(in_reg_schema);
            ctx.builder.push(Instr::WeightClamp {
                in_reg,
                hist_reg,
                out_reg,
                lo,
                hi,
            });
            Ok(out_reg)
        }

        gnitz_wire::OpNode::Reduce {
            group_cols,
            agg,
            global_ground,
            out_key,
        } => emit_reduce(ctx, nid, group_cols, agg, *global_ground, *out_key),

        gnitz_wire::OpNode::Join(kind) => {
            let (delta, trace) = ctx.loaded.inputs(nid).binary();
            let (a_reg, b_reg) = (ctx.reg_of(delta)?, ctx.reg_of(trace)?);
            let a_schema = ctx.reg_meta[a_reg as usize].schema;
            let b_schema = ctx.reg_meta[b_reg as usize].schema;
            // `topo_sorted` validates a Join's port arity but not the producer's
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
            let out_reg = ctx.push_delta_reg(out_schema);
            ctx.builder.push(Instr::JoinDT {
                delta_reg: a_reg,
                trace_reg: b_reg,
                out_reg,
                probe,
            });
            Ok(out_reg)
        }

        gnitz_wire::OpNode::IntegrateSink => {
            // Emits no instruction: the sink register's batch is what
            // `execute_epoch_multi` extracts at epoch end.
            let in_reg = ctx.unary_in(nid)?;
            ctx.sink_reg_id = Some(in_reg);
            Ok(in_reg)
        }

        gnitz_wire::OpNode::IntegrateTrace => {
            let in_reg = ctx.unary_in(nid)?;
            let in_reg_schema = ctx.reg_meta[in_reg as usize].schema;
            // Must fail the compile on a table-open error: emitting the view without
            // the Integrate would compile a view that never persists its differential
            // state, leaving its output permanently empty.
            let trace_reg = ctx.push_trace_reg(&format!("_int_{}_{nid}", ctx.site.id), in_reg_schema)?;
            ctx.builder.push(Instr::Integrate { in_reg, trace_reg });
            Ok(trace_reg)
        }

        gnitz_wire::OpNode::ExchangeShard { .. } => {
            // A side's node list is the ancestors of its own exchange input, with
            // no exchange filter — so a shard upstream of another shard's input
            // lands here. Only a hand-built circuit produces that shape, and
            // rejecting is what keeps it from aborting a worker.
            Err(CompileError::Rejected("chained exchange nodes"))
        }

        gnitz_wire::OpNode::WorkerFilter => {
            // Drops the rows this worker does not own before they reach
            // `integrate_trace`, by the compile-time slot.
            let in_reg = ctx.unary_in(nid)?;
            let slot = ctx.site.slot;
            // A replicated view runs correct-local over the full broadcast, and at
            // one worker every partition is owned here — the filter is the identity
            // either way, and executing it would clone the whole delta each epoch.
            if ctx.placement.is_replicated() || slot.of <= 1 {
                return Ok(in_reg);
            }
            let out_reg = ctx.push_delta_reg(ctx.reg_meta[in_reg as usize].schema);
            ctx.builder.push(Instr::WorkerFilter {
                in_reg,
                out_reg,
                worker_id: slot.rank,
                num_workers: slot.of,
            });
            Ok(out_reg)
        }

        gnitz_wire::OpNode::NullExtend { type_codes } => {
            let in_reg = ctx.unary_in(nid)?;
            let in_schema = ctx.reg_meta[in_reg as usize].schema;
            // The output schema is built here once and homed in `reg_meta`; the
            // op derives its appended-column count from it.
            let out_schema = null_extend_output_schema(&in_schema, type_codes)
                .ok_or(CompileError::Rejected("null-extend: merged schema exceeds MAX_COLUMNS"))?;
            let out_reg = ctx.push_delta_reg(out_schema);
            ctx.builder.push(Instr::NullExtend { in_reg, out_reg });
            Ok(out_reg)
        }
    }
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
fn emit_map(ctx: &mut EmitCtx, nid: i32, mk: &gnitz_wire::MapKind) -> Result<u16, CompileError> {
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
        return Ok(in_reg);
    }
    // For the projection arms the output schema is derived from a client column
    // list rather than supplied, and `project_schema` drops PK sources while
    // `copy_cols` numbers destinations densely — so a PK index leaves a copy
    // addressing a slot that does not exist. This is what catches it.
    let plan = MapPlan::from_map(prog, &in_reg_schema, &node_schema, pk_source)
        .map_err(expr_reject("map: program/schema mismatch"))?;
    let map_idx = ctx.builder.push_map(plan);

    let out_reg = ctx.push_delta_reg(node_schema);
    ctx.builder.push(Instr::Map {
        in_reg,
        out_reg,
        map_idx,
    });
    Ok(out_reg)
}

// ---------------------------------------------------------------------------
// REDUCE emission
// ---------------------------------------------------------------------------

fn emit_reduce(
    ctx: &mut EmitCtx,
    nid: i32,
    group_cols: &[u32],
    agg: &[(gnitz_wire::AggFunc, u32)],
    global_ground: bool,
    out_key: gnitz_store::schema::ReduceOutKey,
) -> Result<u16, CompileError> {
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
        loaded.op(loaded.inputs(nid).unary()),
        gnitz_wire::OpNode::ExchangeShard { .. }
    );
    let slot = ctx.site.slot;
    let i_am_owner = ctx.placement.is_replicated()
        || unsharded
        || slot.rank as usize == gnitz_wire::worker_for_key(gnitz_wire::global_group_key(), slot.of as usize);

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

    let trace_reg = ctx.push_trace_reg(&format!("_reduce_{}_{nid}", ctx.site.id), reduce_out_schema)?;
    let out_reg = ctx.push_delta_reg(reduce_out_schema);

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
        trace_out_reg: trace_reg,
        out_reg,
        plan_idx,
    });

    ctx.builder.push(Instr::Integrate {
        in_reg: out_reg,
        trace_reg,
    });
    Ok(out_reg)
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
    // Register ids are u16 instruction fields. Every node allocates at most two
    // registers (an operator with a trace plus its output delta) and each
    // exchange input one seed. Rejected here, before the first id is handed out
    // and before the emit loop creates any scratch table, so every register id
    // below fits `u16`; the post-loop assert holds the bound.
    let reg_cap = 2 * ordered.len() + exchange_inputs.len();
    if reg_cap > u16::MAX as usize {
        return Err(CompileError::Rejected("register count exceeds u16::MAX"));
    }

    // One seed register per exchange input, allocated first (the post phase of an
    // exchange view reads each side's relayed batch from its own register).
    let mut out_reg_of: HashMap<i32, u16> = HashMap::new();
    let mut reg_meta: Vec<RegisterMeta> = Vec::new();
    let exchange_input_regs: Vec<u16> = exchange_inputs
        .iter()
        .map(|(ex_nid, ex_schema)| {
            let reg = reg_meta.len() as u16;
            reg_meta.push(RegisterMeta::delta(*ex_schema));
            out_reg_of.insert(*ex_nid, reg);
            reg
        })
        .collect();

    let mut ctx = EmitCtx {
        loaded,
        placement,
        ext_tables,
        site,
        builder: ProgramBuilder::new(),
        out_reg_of,
        reg_meta,
        source_reg_map: FxHashMap::default(),
        sink_reg_id: None,
        scratch: ScratchGuard::new(),
    };

    // Instruction count after each node has emitted. Recorded in the loop, so a
    // node that emits nothing leaves the running length unchanged and no one has
    // to reason about which nodes emit.
    let mut instr_end: HashMap<i32, usize> = HashMap::with_capacity(ordered.len());
    for &nid in ordered {
        let reg = emit_node(&mut ctx, nid, loaded.op(nid))?;
        ctx.out_reg_of.insert(nid, reg);
        instr_end.insert(nid, ctx.builder.instr_count());
    }

    // The exchange seeds come first; failing that, the plan is driven from a
    // source register. `min_by_key` rather than an arbitrary map entry so a
    // multi-source plan picks the same register on every worker.
    let input_delta_reg_id = exchange_input_regs
        .first()
        .copied()
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
    // `reg_meta` is moved verbatim into `Program` and held for as long as the plan
    // stays cached, and a `RegisterMeta` is a whole `SchemaDescriptor` — so the
    // slack a growing `Vec` leaves behind would be tens of KB of dead heap per
    // sub-plan, per view, per worker.
    ctx.reg_meta.shrink_to_fit();

    let EmitCtx {
        builder,
        reg_meta,
        source_reg_map,
        scratch,
        out_reg_of,
        ..
    } = ctx;
    let vm = builder.build(reg_meta, sink_reg);

    Ok(PlanBuildResult {
        vm,
        in_reg: input_delta_reg_id,
        source_reg_map,
        exchange_input_regs,
        scratch,
        instr_end,
        out_reg_of,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/emit.rs"]
mod tests;
