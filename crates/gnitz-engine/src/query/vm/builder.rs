//! `ProgramBuilder` — the resource pools behind a `Program`, plus `push`.
//!
//! Emission constructs `Instr` literals directly (there is deliberately no
//! per-opcode constructor mirror); the builder's job is holding the resources
//! those instructions index — funcs, tables, and the baked operator pools —
//! and assembling the final `Program`. Each `push`/`add` returns the index that
//! *is* the instruction operand, so nothing has to be numbered twice.

use super::*;
use crate::expr::ScalarFunc;
use crate::storage::Table;

#[allow(clippy::vec_box)]
pub(crate) struct ProgramBuilder {
    instructions: Vec<Instr>,
    funcs: Vec<Box<ScalarFunc>>,
    tables: Vec<UnsafeCell<Box<Table>>>,
    reindex_packers: Vec<crate::ops::ReindexPacker>,
    reduce_plans: Vec<crate::ops::ReducePlan>,
    avi_bakes: Vec<crate::ops::AviBake>,
}

// SAFETY: Same justification as Program — single-thread access.
unsafe impl Send for ProgramBuilder {}

impl ProgramBuilder {
    pub(crate) fn new() -> Self {
        ProgramBuilder {
            instructions: Vec::with_capacity(16),
            funcs: Vec::new(),
            tables: Vec::new(),
            reindex_packers: Vec::new(),
            reduce_plans: Vec::new(),
            avi_bakes: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, instr: Instr) {
        self.instructions.push(instr);
    }

    /// The instructions pushed so far — read by `build_plan`'s destructive-
    /// register ordering check.
    pub(crate) fn instructions(&self) -> &[Instr] {
        &self.instructions
    }

    // ── Resources ────────────────────────────────────────────────────────

    /// Take ownership of `func`, returning its `Instr` operand.
    pub(crate) fn push_func(&mut self, func: ScalarFunc) -> FuncIdx {
        let idx = FuncIdx(self.funcs.len() as u16);
        self.funcs.push(Box::new(func));
        idx
    }

    /// Take ownership of `table`, returning its `Instr` operand — which is also
    /// what `RegisterMeta::trace` names, so a trace register and the instruction
    /// writing it cannot disagree about which table they mean.
    ///
    /// The `u16` bound holds because each node contributes at most three tables
    /// and `build_plan` bounds the node count well below `u16::MAX / 3`.
    pub(crate) fn push_table(&mut self, table: Table) -> TableIdx {
        debug_assert!(self.tables.len() < u16::MAX as usize);
        let idx = TableIdx(self.tables.len() as u16);
        self.tables.push(UnsafeCell::new(Box::new(table)));
        idx
    }

    /// Store a baked reduce plan, returning its `Instr::Reduce::plan_idx`.
    pub(crate) fn add_reduce_plan(&mut self, plan: crate::ops::ReducePlan) -> PlanIdx {
        let idx = PlanIdx(self.reduce_plans.len() as u16);
        self.reduce_plans.push(plan);
        idx
    }

    /// Store the baked AVI write-side resources, returning
    /// `IntegrateAvi::bake_idx`.
    pub(crate) fn add_avi_bake(&mut self, bake: crate::ops::AviBake) -> BakeIdx {
        let idx = BakeIdx(self.avi_bakes.len() as u16);
        self.avi_bakes.push(bake);
        idx
    }

    /// Store a baked reindex packer, returning its `ReindexOperand::Pack` index.
    pub(crate) fn add_reindex_packer(&mut self, packer: crate::ops::ReindexPacker) -> PackerIdx {
        let idx = PackerIdx(self.reindex_packers.len() as u16);
        self.reindex_packers.push(packer);
        idx
    }

    // ── Build ────────────────────────────────────────────────────────────

    /// Consume the builder into a runnable `VmHandle`.
    pub(crate) fn build(self, reg_meta: Vec<RegisterMeta>) -> Box<VmHandle> {
        let regfile = RegisterFile::new(&reg_meta);
        // The trace registers name their own backing tables, so the refresh list
        // is read off the metas rather than tracked alongside them.
        let trace_regs: Vec<(u16, TableIdx)> = reg_meta
            .iter()
            .enumerate()
            .filter_map(|(reg, m)| m.owned_table.map(|t| (reg as u16, t)))
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
            owned_cursor_handles: Vec::with_capacity(trace_regs.len()),
            program,
            regfile,
            trace_regs,
        })
    }
}
