//! `ProgramBuilder` — constructs a `Program` via incremental `add_*()` calls.

use super::*;
use crate::expr::ScalarFuncKind;
use crate::ops::AggDescriptor;
use crate::schema::SchemaDescriptor;
use crate::storage::Table;

// ---------------------------------------------------------------------------
// ProgramBuilder — constructs a Program via incremental add_*() calls.
// ---------------------------------------------------------------------------

pub(crate) struct ProgramBuilder {
    num_registers: u16,
    instructions: Vec<Instr>,
    funcs: Vec<*const ScalarFuncKind>,
    tables: Vec<*mut Table>,
    schemas: Vec<SchemaDescriptor>,
    agg_descs: Vec<AggDescriptor>,
    group_cols: Vec<u32>,
    reindex_cols: Vec<u32>,
    /// Parallel to `reindex_cols`: per-column carried promotion target tc
    /// (`0` = derive from source). Sliced alongside `reindex_cols` for `op_map`.
    reindex_target_tcs: Vec<u8>,
    expr_progs: Vec<*const crate::expr::ExprProgram>,
}

// SAFETY: Same justification as Program — single-thread access, stable pointers.
unsafe impl Send for ProgramBuilder {}

impl ProgramBuilder {
    pub fn new(num_registers: u16) -> Self {
        ProgramBuilder {
            num_registers,
            instructions: Vec::with_capacity(16),
            funcs: Vec::new(),
            tables: Vec::new(),
            schemas: Vec::new(),
            agg_descs: Vec::new(),
            group_cols: Vec::new(),
            reindex_cols: Vec::new(),
            reindex_target_tcs: Vec::new(),
            expr_progs: Vec::new(),
        }
    }

    // ── Resource dedup (linear scan, small N) ────────────────────────────

    fn func_idx(&mut self, ptr: *const ScalarFuncKind) -> u16 {
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

    fn expr_idx(&mut self, ptr: *const crate::expr::ExprProgram) -> Option<u16> {
        if ptr.is_null() {
            return None;
        }
        for (i, &e) in self.expr_progs.iter().enumerate() {
            if e == ptr {
                return Some(i as u16);
            }
        }
        let idx = self.expr_progs.len() as u16;
        self.expr_progs.push(ptr);
        Some(idx)
    }

    /// Build an optional GI descriptor: intern `gi_table` and pair it with
    /// `col_idx`, or `None` when there is no GI table. Shared by `add_integrate`
    /// and `add_reduce`.
    fn gi_desc(&mut self, gi_table: *mut Table, col_idx: u32) -> Option<Gi> {
        (!gi_table.is_null()).then(|| Gi {
            table_idx: self.table_idx(gi_table) as u16,
            col_idx,
        })
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

    pub fn add_delay(&mut self, src: u16, state_reg: u16, dst: u16) {
        self.instructions.push(Instr::Delay { src, state_reg, dst });
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

    pub fn add_filter(&mut self, in_reg: u16, out_reg: u16, func_ptr: *const ScalarFuncKind) {
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
        func_ptr: *const ScalarFuncKind,
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

    pub fn add_distinct(&mut self, in_reg: u16, hist_reg: u16, out_reg: u16, hist_table: *mut Table) {
        let hist_table_idx = self.table_idx(hist_table) as i16;
        self.instructions.push(Instr::Distinct {
            in_reg,
            hist_reg,
            out_reg,
            hist_table_idx,
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

    pub fn add_join_dd(&mut self, a_reg: u16, b_reg: u16, out_reg: u16, right_schema: SchemaDescriptor) {
        let right_schema_idx = self.schema_idx(right_schema);
        self.instructions.push(Instr::JoinDD {
            a_reg,
            b_reg,
            out_reg,
            right_schema_idx,
        });
    }

    pub fn add_join_dt_outer(&mut self, delta_reg: u16, trace_reg: u16, out_reg: u16, right_schema: SchemaDescriptor) {
        let right_schema_idx = self.schema_idx(right_schema);
        self.instructions.push(Instr::JoinDTOuter {
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

    pub fn add_anti_join_dt(&mut self, delta_reg: u16, trace_reg: u16, out_reg: u16) {
        self.instructions.push(Instr::AntiJoinDT {
            delta_reg,
            trace_reg,
            out_reg,
        });
    }

    pub fn add_anti_join_dd(&mut self, a_reg: u16, b_reg: u16, out_reg: u16) {
        self.instructions.push(Instr::AntiJoinDD { a_reg, b_reg, out_reg });
    }

    pub fn add_semi_join_dt(&mut self, delta_reg: u16, trace_reg: u16, out_reg: u16) {
        self.instructions.push(Instr::SemiJoinDT {
            delta_reg,
            trace_reg,
            out_reg,
        });
    }

    pub fn add_semi_join_dd(&mut self, a_reg: u16, b_reg: u16, out_reg: u16) {
        self.instructions.push(Instr::SemiJoinDD { a_reg, b_reg, out_reg });
    }

    pub fn add_null_extend(&mut self, in_reg: u16, out_reg: u16, right_schema: SchemaDescriptor) {
        let right_schema_idx = self.schema_idx(right_schema);
        self.instructions.push(Instr::NullExtend {
            in_reg,
            out_reg,
            right_schema_idx,
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_integrate(
        &mut self,
        in_reg: u16,
        target_table: *mut Table,
        // GI params (all NULL/0 if no GI)
        gi_table: *mut Table,
        gi_col_idx: u32,
        // AVI params (all NULL/0 if no AVI)
        avi_table: *mut Table,
        avi_for_max: bool,
        avi_agg_col_type_code: u8,
        avi_group_cols: &[u32],
        avi_agg_col_idx: u32,
    ) {
        let table_idx = self.table_idx(target_table);
        let gi = self.gi_desc(gi_table, gi_col_idx);
        let avi = if !avi_table.is_null() {
            let (gc_off, gc_cnt) = self.add_group_cols(avi_group_cols);
            Some(IntegrateAvi {
                table_idx: self.table_idx(avi_table) as u16,
                for_max: avi_for_max,
                agg_col_type_code: avi_agg_col_type_code,
                group_cols_offset: gc_off,
                group_cols_count: gc_cnt,
                agg_col_idx: avi_agg_col_idx,
            })
        } else {
            None
        };
        self.instructions.push(Instr::Integrate {
            in_reg,
            table_idx,
            gi,
            avi,
        });
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
        // AVI params
        avi_table: *mut Table,
        avi_for_max: bool,
        avi_agg_col_type_code: u8,
        // GI params
        gi_table: *mut Table,
        gi_col_idx: u32,
        // Finalize
        finalize_prog: *const crate::expr::ExprProgram,
        finalize_schema: *const SchemaDescriptor,
    ) {
        let (agg_off, agg_cnt) = self.add_agg_descs(agg_descs);
        let (gc_off, gc_cnt) = self.add_group_cols(group_cols);
        let output_schema_idx = self.schema_idx(output_schema);

        let gi = self.gi_desc(gi_table, gi_col_idx);
        let avi = if !avi_table.is_null() {
            Some(ReduceAvi {
                table_idx: self.table_idx(avi_table) as u16,
                for_max: avi_for_max,
                agg_col_type_code: avi_agg_col_type_code,
            })
        } else {
            None
        };

        let finalize_func_idx = self.expr_idx(finalize_prog);
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
            gi,
            avi,
            finalize_func_idx,
            finalize_schema_idx,
        });
    }

    pub fn add_gather_reduce(&mut self, in_reg: u16, trace_out_reg: u16, out_reg: u16, agg_descs: &[AggDescriptor]) {
        let (agg_off, agg_cnt) = self.add_agg_descs(agg_descs);
        self.instructions.push(Instr::GatherReduce {
            in_reg,
            trace_out_reg,
            out_reg,
            agg_descs_offset: agg_off,
            agg_descs_count: agg_cnt,
        });
    }

    // ── Build ────────────────────────────────────────────────────────────

    /// Consume the builder, producing a simple VmHandle (no owned resources).
    ///
    /// Used by test code — production code uses `build_with_owned`.
    #[cfg(test)]
    pub(crate) fn build(self, reg_meta: &[RegisterMeta]) -> Box<VmHandle> {
        self.build_with_owned(reg_meta.to_vec(), Vec::new(), Vec::new(), Vec::new(), Vec::new())
    }

    /// Consume the builder, producing a VmHandle that owns child tables,
    /// scalar functions, and expression programs created by the compiler.
    #[allow(clippy::vec_box)]
    pub fn build_with_owned(
        self,
        reg_meta: Vec<RegisterMeta>,
        owned_tables: Vec<Box<Table>>,
        owned_funcs: Vec<Box<ScalarFuncKind>>,
        owned_expr_progs: Vec<Box<crate::expr::ExprProgram>>,
        owned_trace_regs: Vec<(u16, usize)>,
    ) -> Box<VmHandle> {
        assert_eq!(reg_meta.len(), self.num_registers as usize);

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
            expr_progs: self.expr_progs,
        };

        let num_owned = owned_trace_regs.len();
        Box::new(VmHandle {
            program,
            regfile,
            owned_tables,
            owned_funcs,
            owned_expr_progs,
            owned_trace_regs,
            owned_cursor_handles: Vec::with_capacity(num_owned),
        })
    }
}
