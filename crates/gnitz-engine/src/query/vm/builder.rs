//! `ProgramBuilder` — constructs a `Program` via incremental `add_*()` calls.

use super::*;
use crate::expr::ScalarFunc;
use crate::ops::AggDescriptor;
use crate::schema::SchemaDescriptor;
use crate::storage::Table;

// ---------------------------------------------------------------------------
// ProgramBuilder — constructs a Program via incremental add_*() calls.
// ---------------------------------------------------------------------------

pub(crate) struct ProgramBuilder {
    instructions: Vec<Instr>,
    funcs: Vec<*const ScalarFunc>,
    tables: Vec<*mut Table>,
    schemas: Vec<SchemaDescriptor>,
    agg_descs: Vec<AggDescriptor>,
    group_cols: Vec<u32>,
    reindex_cols: Vec<u32>,
    /// Parallel to `reindex_cols`: per-column carried promotion target tc
    /// (`0` = derive from source). Sliced alongside `reindex_cols` for `op_map`.
    reindex_target_tcs: Vec<u8>,
}

// SAFETY: Same justification as Program — single-thread access, stable pointers.
unsafe impl Send for ProgramBuilder {}

impl ProgramBuilder {
    pub fn new() -> Self {
        ProgramBuilder {
            instructions: Vec::with_capacity(16),
            funcs: Vec::new(),
            tables: Vec::new(),
            schemas: Vec::new(),
            agg_descs: Vec::new(),
            group_cols: Vec::new(),
            reindex_cols: Vec::new(),
            reindex_target_tcs: Vec::new(),
        }
    }

    // ── Resource dedup (linear scan, small N) ────────────────────────────

    fn func_idx(&mut self, ptr: *const ScalarFunc) -> u16 {
        for (i, &f) in self.funcs.iter().enumerate() {
            if f == ptr {
                return i as u16;
            }
        }
        let idx = self.funcs.len() as u16;
        self.funcs.push(ptr);
        idx
    }

    fn table_idx(&mut self, ptr: *mut Table) -> i32 {
        if ptr.is_null() {
            return -1;
        }
        for (i, &t) in self.tables.iter().enumerate() {
            if t == ptr {
                return i as i32;
            }
        }
        let idx = self.tables.len() as i32;
        self.tables.push(ptr);
        idx
    }

    fn schema_idx(&mut self, desc: SchemaDescriptor) -> u16 {
        if let Some(i) = self.schemas.iter().position(|s| s == &desc) {
            return i as u16;
        }
        let idx = self.schemas.len() as u16;
        self.schemas.push(desc);
        idx
    }

    fn add_agg_descs(&mut self, descs: &[AggDescriptor]) -> (u32, u16) {
        let offset = self.agg_descs.len() as u32;
        self.agg_descs.extend_from_slice(descs);
        (offset, descs.len() as u16)
    }

    fn add_group_cols(&mut self, cols: &[u32]) -> (u32, u16) {
        let offset = self.group_cols.len() as u32;
        self.group_cols.extend_from_slice(cols);
        (offset, cols.len() as u16)
    }

    fn add_reindex_cols(&mut self, cols: &[u32], target_tcs: &[u8]) -> (u32, u16) {
        let offset = self.reindex_cols.len() as u32;
        // This is the only mutator of either pool, so they enter in lockstep.
        debug_assert_eq!(
            self.reindex_target_tcs.len(),
            offset as usize,
            "reindex pools must stay in lockstep"
        );
        self.reindex_cols.extend_from_slice(cols);
        // Keep the target-tc pool in lockstep with reindex_cols (same offset and
        // count): a shorter or empty `target_tcs` zero-fills (= derive from source),
        // so the `Instr::Map` slice always has one tc per reindex column.
        self.reindex_target_tcs.extend_from_slice(target_tcs);
        self.reindex_target_tcs.resize(self.reindex_cols.len(), 0);
        (offset, cols.len() as u16)
    }

    // ── Instruction methods ──────────────────────────────────────────────

    pub fn add_halt(&mut self) {
        self.instructions.push(Instr::Halt);
    }

    pub fn add_clear_deltas(&mut self) {
        self.instructions.push(Instr::ClearDeltas);
    }

    pub fn add_scan_trace(&mut self, trace_reg: u16, out_reg: u16, chunk_limit: i32) {
        self.instructions.push(Instr::ScanTrace {
            trace_reg,
            out_reg,
            chunk_limit,
        });
    }

    pub fn add_seek_trace(&mut self, trace_reg: u16, key_reg: u16) {
        self.instructions.push(Instr::SeekTrace { trace_reg, key_reg });
    }

    pub fn add_filter(&mut self, in_reg: u16, out_reg: u16, func_ptr: *const ScalarFunc) {
        let func_idx = self.func_idx(func_ptr);
        self.instructions.push(Instr::Filter {
            in_reg,
            out_reg,
            func_idx,
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_map(
        &mut self,
        in_reg: u16,
        out_reg: u16,
        func_ptr: *const ScalarFunc,
        out_schema: SchemaDescriptor,
        reindex_cols: &[u32],
        reindex_target_tcs: &[u8],
        reindex_hash: bool,
        branch_id: u8,
    ) {
        let func_idx = self.func_idx(func_ptr);
        let out_schema_idx = self.schema_idx(out_schema);
        let (reindex_off, reindex_cnt) = self.add_reindex_cols(reindex_cols, reindex_target_tcs);
        self.instructions.push(Instr::Map {
            in_reg,
            out_reg,
            func_idx,
            out_schema_idx,
            reindex_off,
            reindex_cnt,
            reindex_hash,
            branch_id,
        });
    }

    pub fn add_negate(&mut self, in_reg: u16, out_reg: u16) {
        self.instructions.push(Instr::Negate { in_reg, out_reg });
    }

    pub fn add_union(&mut self, in_a: u16, in_b: u16, has_b: bool, out_reg: u16) {
        self.instructions.push(Instr::Union {
            in_a,
            in_b,
            has_b,
            out_reg,
        });
    }

    /// `(lo, hi) = (-1, 1)` ⇒ `distinct` (set membership); `(0, i64::MAX)` ⇒
    /// `positive_part` (bag multiplicity). Both compile to the one `WeightClamp`
    /// instruction / exec arm.
    pub fn add_weight_clamp(
        &mut self,
        in_reg: u16,
        hist_reg: u16,
        out_reg: u16,
        hist_table: *mut Table,
        lo: i64,
        hi: i64,
    ) {
        let hist_table_idx = self.table_idx(hist_table) as i16;
        self.instructions.push(Instr::WeightClamp {
            in_reg,
            hist_reg,
            out_reg,
            hist_table_idx,
            lo,
            hi,
        });
    }

    pub fn add_join_dt(&mut self, delta_reg: u16, trace_reg: u16, out_reg: u16, right_schema: SchemaDescriptor) {
        let right_schema_idx = self.schema_idx(right_schema);
        self.instructions.push(Instr::JoinDT {
            delta_reg,
            trace_reg,
            out_reg,
            right_schema_idx,
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_join_dt_range(
        &mut self,
        delta_reg: u16,
        trace_reg: u16,
        out_reg: u16,
        right_schema: SchemaDescriptor,
        n_eq: u8,
        rel: gnitz_wire::RangeRel,
    ) {
        let right_schema_idx = self.schema_idx(right_schema);
        self.instructions.push(Instr::JoinDTRange {
            delta_reg,
            trace_reg,
            out_reg,
            right_schema_idx,
            n_eq,
            rel,
        });
    }

    pub fn add_partition_filter(&mut self, in_reg: u16, out_reg: u16, worker_id: u32, num_workers: u32) {
        self.instructions.push(Instr::PartitionFilter {
            in_reg,
            out_reg,
            worker_id,
            num_workers,
        });
    }

    pub fn add_null_extend(&mut self, in_reg: u16, out_reg: u16, right_schema: SchemaDescriptor) {
        let right_schema_idx = self.schema_idx(right_schema);
        self.instructions.push(Instr::NullExtend {
            in_reg,
            out_reg,
            right_schema_idx,
        });
    }

    /// `avi_aggs` is the value-indexed subset of the reduce's descriptors in
    /// `agg_descs` order (ordinal = position); empty (with a null `avi_table`)
    /// when the integrate populates no value index.
    pub fn add_integrate(
        &mut self,
        in_reg: u16,
        target_table: *mut Table,
        avi_table: *mut Table,
        avi_group_cols: &[u32],
        avi_aggs: &[AggDescriptor],
    ) {
        let table_idx = self.table_idx(target_table);
        let avi = (!avi_table.is_null()).then(|| IntegrateAvi {
            table_idx: self.table_idx(avi_table) as u16,
            group_by_cols: avi_group_cols.to_vec(),
            aggs: avi_aggs.to_vec(),
        });
        self.instructions.push(Instr::Integrate { in_reg, table_idx, avi });
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_reduce(
        &mut self,
        in_reg: u16,
        trace_in_reg: Option<u16>,
        trace_out_reg: u16,
        out_reg: u16,
        fin_out_reg: Option<u16>,
        agg_descs: &[AggDescriptor],
        group_cols: &[u32],
        output_schema: SchemaDescriptor,
        out_key: gnitz_wire::ReduceOutKey,
        // The combined value index table (null if no AVI). `for_max`/type per
        // aggregate are read from `agg_descs` by ordinal on the read side.
        avi_table: *mut Table,
        // Post-reduce finalize MAP (null if the reduce has none).
        finalize_func: *const ScalarFunc,
        finalize_schema: *const SchemaDescriptor,
        // Global-aggregate ground machinery
        global_ground: bool,
        i_am_owner: bool,
    ) {
        let (agg_off, agg_cnt) = self.add_agg_descs(agg_descs);
        let (gc_off, gc_cnt) = self.add_group_cols(group_cols);
        let output_schema_idx = self.schema_idx(output_schema);

        let avi = (!avi_table.is_null()).then(|| ReduceAvi {
            table_idx: self.table_idx(avi_table) as u16,
        });

        let finalize_func_idx = (!finalize_func.is_null()).then(|| self.func_idx(finalize_func));
        let finalize_schema_idx = if !finalize_schema.is_null() {
            Some(self.schema_idx(unsafe { *finalize_schema }))
        } else {
            None
        };

        self.instructions.push(Instr::Reduce {
            in_reg,
            trace_in_reg,
            trace_out_reg,
            out_reg,
            fin_out_reg,
            agg_descs_offset: agg_off,
            agg_descs_count: agg_cnt,
            group_cols_offset: gc_off,
            group_cols_count: gc_cnt,
            output_schema_idx,
            out_key,
            avi,
            finalize_func_idx,
            finalize_schema_idx,
            global_ground,
            i_am_owner,
        });
    }

    // ── Build ────────────────────────────────────────────────────────────

    /// Consume the builder, producing a simple VmHandle (no owned resources).
    ///
    /// Used by test code — production code uses `build_with_owned`.
    #[cfg(test)]
    pub(crate) fn build(self, reg_meta: &[RegisterMeta]) -> Box<VmHandle> {
        self.build_with_owned(reg_meta.to_vec(), Vec::new(), Vec::new(), Vec::new())
    }

    /// Consume the builder, producing a VmHandle that owns the child tables and
    /// scalar functions created by the compiler.
    #[allow(clippy::vec_box)]
    pub fn build_with_owned(
        self,
        reg_meta: Vec<RegisterMeta>,
        owned_tables: Vec<Box<Table>>,
        owned_funcs: Vec<Box<ScalarFunc>>,
        owned_trace_regs: Vec<(u16, usize)>,
    ) -> Box<VmHandle> {
        let regfile = RegisterFile::new(&reg_meta);

        let program = Program {
            instructions: self.instructions,
            reg_meta,
            funcs: self.funcs,
            tables: self.tables,
            schemas: self.schemas,
            agg_descs: self.agg_descs,
            group_cols: self.group_cols,
            reindex_cols: self.reindex_cols,
            reindex_target_tcs: self.reindex_target_tcs,
        };

        let num_owned = owned_trace_regs.len();
        Box::new(VmHandle {
            program,
            regfile,
            owned_tables,
            owned_funcs,
            owned_trace_regs,
            owned_cursor_handles: Vec::with_capacity(num_owned),
        })
    }
}
