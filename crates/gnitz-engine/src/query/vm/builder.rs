//! `ProgramBuilder` — the interning pools behind a `Program`, plus `push`.
//!
//! Emission constructs `Instr` literals directly (there is deliberately no
//! per-opcode constructor mirror); the builder's job is interning the shared
//! resources those instructions index — funcs, tables, and the reindex column
//! pools — and assembling the final `Program`.

use super::*;
use crate::expr::ScalarFunc;
use crate::storage::Table;

pub(crate) struct ProgramBuilder {
    instructions: Vec<Instr>,
    funcs: Vec<*const ScalarFunc>,
    tables: Vec<*mut Table>,
    reindex_packers: Vec<crate::ops::ReindexPacker>,
    reduce_plans: Vec<crate::ops::ReducePlan>,
    avi_bakes: Vec<crate::ops::AviBake>,
}

// SAFETY: Same justification as Program — single-thread access, stable pointers.
unsafe impl Send for ProgramBuilder {}

impl ProgramBuilder {
    pub fn new() -> Self {
        ProgramBuilder {
            instructions: Vec::with_capacity(16),
            funcs: Vec::new(),
            tables: Vec::new(),
            reindex_packers: Vec::new(),
            reduce_plans: Vec::new(),
            avi_bakes: Vec::new(),
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

    /// Intern `ptr` into `Program::tables` and return its instruction operand.
    /// The `u16` bound holds because each node contributes at most three owned
    /// tables and `build_plan` bounds the node count well below `u16::MAX / 3`.
    pub fn table_idx(&mut self, ptr: *mut Table) -> u16 {
        for (i, &t) in self.tables.iter().enumerate() {
            if t == ptr {
                return i as u16;
            }
        }
        debug_assert!(self.tables.len() < u16::MAX as usize);
        let idx = self.tables.len() as u16;
        self.tables.push(ptr);
        idx
    }

    /// Store a baked reduce plan, returning its `Instr::Reduce::plan_idx`.
    pub fn add_reduce_plan(&mut self, plan: crate::ops::ReducePlan) -> u16 {
        let idx = self.reduce_plans.len() as u16;
        self.reduce_plans.push(plan);
        idx
    }

    /// Store the baked AVI write-side resources, returning
    /// `IntegrateAvi::bake_idx`.
    pub fn add_avi_bake(&mut self, bake: crate::ops::AviBake) -> u16 {
        let idx = self.avi_bakes.len() as u16;
        self.avi_bakes.push(bake);
        idx
    }

    /// Store a baked reindex packer, returning its `ReindexOperand::Pack` index.
    pub fn add_reindex_packer(&mut self, packer: crate::ops::ReindexPacker) -> u16 {
        let idx = self.reindex_packers.len() as u16;
        self.reindex_packers.push(packer);
        idx
    }

    // ── Build ────────────────────────────────────────────────────────────

    /// Consume the builder, producing a simple VmHandle (no owned resources).
    ///
    /// Used by test code — production code uses `build_with_owned`.
    #[cfg(test)]
    pub(crate) fn build(self, reg_meta: &[RegisterMeta]) -> Box<VmHandle> {
        self.build_with_owned(reg_meta.to_vec(), Vec::new(), Vec::new())
    }

    /// Consume the builder, producing a VmHandle that owns the child tables and
    /// scalar functions created by the compiler.
    #[allow(clippy::vec_box)]
    pub fn build_with_owned(
        self,
        reg_meta: Vec<RegisterMeta>,
        owned_tables: Vec<Box<Table>>,
        owned_funcs: Vec<Box<ScalarFunc>>,
    ) -> Box<VmHandle> {
        let regfile = RegisterFile::new(&reg_meta);
        // The trace registers name their own backing tables, so the refresh list
        // is read off the metas rather than tracked alongside them.
        let trace_regs: Vec<(u16, usize)> = reg_meta
            .iter()
            .enumerate()
            .filter_map(|(reg, m)| m.owned_table.map(|t| (reg as u16, t as usize)))
            .collect();

        let program = Program {
            instructions: self.instructions,
            reg_meta,
            funcs: self.funcs,
            tables: self.tables,
            reindex_packers: self.reindex_packers,
            reduce_plans: self.reduce_plans,
            avi_bakes: self.avi_bakes,
        };

        Box::new(VmHandle {
            program,
            regfile,
            owned_tables,
            owned_funcs,
            owned_cursor_handles: Vec::with_capacity(trace_regs.len()),
            trace_regs,
        })
    }
}
