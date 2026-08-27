//! `ProgramBuilder` — the resource pools behind a `Program`, plus `push`.
//!
//! Emission constructs `Instr` literals directly (there is deliberately no
//! per-opcode constructor mirror); the builder's job is holding the resources
//! those instructions index — predicates, map plans, tables, and the baked
//! operator pools — and assembling the final `Program`. Each `push`/`add`
//! returns the index that *is* the instruction operand, so nothing has to be
//! numbered twice.

use super::*;
use crate::expr::MapPlan;
use crate::storage::Table;

pub(crate) struct ProgramBuilder {
    instructions: Vec<Instr>,
    predicates: Vec<gnitz_expr::Evaluator>,
    maps: Vec<MapPlan>,
    tables: Vec<UnsafeCell<Box<Table>>>,
    reduce_plans: Vec<crate::ops::ReducePlan>,
    avi_bakes: Vec<crate::ops::AviBake>,
}

// SAFETY: Same justification as Program — single-thread access.
unsafe impl Send for ProgramBuilder {}

impl ProgramBuilder {
    pub(crate) fn new() -> Self {
        ProgramBuilder {
            instructions: Vec::with_capacity(16),
            predicates: Vec::new(),
            maps: Vec::new(),
            tables: Vec::new(),
            reduce_plans: Vec::new(),
            avi_bakes: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, instr: Instr) {
        self.instructions.push(instr);
    }

    /// The instructions pushed so far, and the same list to write back into.
    /// `build_plan`'s liveness pass needs both: whether a destructive instruction
    /// may take its input depends on instructions that were not yet pushed when it
    /// was emitted.
    pub(crate) fn instructions(&self) -> &[Instr] {
        &self.instructions
    }

    pub(crate) fn instructions_mut(&mut self) -> &mut [Instr] {
        &mut self.instructions
    }

    // ── Resources ────────────────────────────────────────────────────────

    /// Take ownership of `pred`, returning its `Instr::Filter` operand.
    pub(crate) fn push_predicate(&mut self, pred: gnitz_expr::Evaluator) -> PredIdx {
        let idx = PredIdx(self.predicates.len() as u16);
        self.predicates.push(pred);
        idx
    }

    /// Take ownership of `plan`, returning its `Instr::Map` operand.
    pub(crate) fn push_map(&mut self, plan: MapPlan) -> MapIdx {
        let idx = MapIdx(self.maps.len() as u16);
        self.maps.push(plan);
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
            predicates: self.predicates,
            maps: self.maps,
            tables: self.tables,
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
