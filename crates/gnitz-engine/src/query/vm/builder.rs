//! `ProgramBuilder` — the interning pools behind a `Program`, plus `push`.
//!
//! Emission constructs `Instr` literals directly (there is deliberately no
//! per-opcode constructor mirror); the builder's job is interning the shared
//! resources those instructions index — funcs, tables, schemas, and the
//! agg/group/reindex column pools — and assembling the final `Program`.

use super::*;
use crate::expr::ScalarFunc;
use crate::ops::AggDescriptor;
use crate::schema::SchemaDescriptor;
use crate::storage::Table;

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
    reduce_plans: Vec<crate::ops::ReducePlan>,
    avi_extractors: Vec<crate::ops::GroupKeyExtractor>,
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
            reduce_plans: Vec::new(),
            avi_extractors: Vec::new(),
        }
    }

    pub fn push(&mut self, instr: Instr) {
        self.instructions.push(instr);
    }

    /// The instructions pushed so far — read by `build_plan`'s post-emission
    /// destructive-register ordering check.
    pub fn instructions(&self) -> &[Instr] {
        &self.instructions
    }

    // ── Resource interning (linear scan, small N) ────────────────────────

    pub fn func_idx(&mut self, ptr: *const ScalarFunc) -> u16 {
        for (i, &f) in self.funcs.iter().enumerate() {
            if f == ptr {
                return i as u16;
            }
        }
        let idx = self.funcs.len() as u16;
        self.funcs.push(ptr);
        idx
    }

    pub fn table_idx(&mut self, ptr: *mut Table) -> i32 {
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

    pub fn schema_idx(&mut self, desc: SchemaDescriptor) -> u16 {
        if let Some(i) = self.schemas.iter().position(|s| s == &desc) {
            return i as u16;
        }
        let idx = self.schemas.len() as u16;
        self.schemas.push(desc);
        idx
    }

    pub fn add_agg_descs(&mut self, descs: &[AggDescriptor]) -> (u32, u16) {
        let offset = self.agg_descs.len() as u32;
        self.agg_descs.extend_from_slice(descs);
        (offset, descs.len() as u16)
    }

    pub fn add_group_cols(&mut self, cols: &[u32]) -> (u32, u16) {
        let offset = self.group_cols.len() as u32;
        self.group_cols.extend_from_slice(cols);
        (offset, cols.len() as u16)
    }

    /// Store a baked reduce plan, returning its `Instr::Reduce::plan_idx`.
    pub fn add_reduce_plan(&mut self, plan: crate::ops::ReducePlan) -> u16 {
        let idx = self.reduce_plans.len() as u16;
        self.reduce_plans.push(plan);
        idx
    }

    /// Store a baked AVI write-side group-key extractor, returning its
    /// `IntegrateAvi::extractor_idx`.
    pub fn add_avi_extractor(&mut self, extractor: crate::ops::GroupKeyExtractor) -> u16 {
        let idx = self.avi_extractors.len() as u16;
        self.avi_extractors.push(extractor);
        idx
    }

    pub fn add_reindex_cols(&mut self, cols: &[u32], target_tcs: &[u8]) -> (u32, u16) {
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
        mut reg_meta: Vec<RegisterMeta>,
        owned_tables: Vec<Box<Table>>,
        owned_funcs: Vec<Box<ScalarFunc>>,
        owned_trace_regs: Vec<(u16, usize)>,
    ) -> Box<VmHandle> {
        // Bake ownership into the metas so the per-epoch `bind_cursors` does no
        // per-register scan of the owned list.
        for &(reg_id, _) in &owned_trace_regs {
            reg_meta[reg_id as usize].is_owned = true;
        }
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
            reduce_plans: self.reduce_plans,
            avi_extractors: self.avi_extractors,
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
