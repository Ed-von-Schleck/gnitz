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
    tables: Vec<Table>,
    reduce_plans: Vec<BakedReduce>,
}

impl ProgramBuilder {
    pub(crate) fn new() -> Self {
        ProgramBuilder {
            instructions: Vec::with_capacity(16),
            predicates: Vec::new(),
            maps: Vec::new(),
            tables: Vec::new(),
            reduce_plans: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, instr: Instr) {
        self.instructions.push(instr);
    }

    /// How many instructions are emitted so far — the program offset a node's
    /// emission ends at.
    pub(crate) fn instr_count(&self) -> usize {
        self.instructions.len()
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

    /// Take ownership of `table`, returning the index `RegisterMeta::trace` names
    /// it by. The `u16` holds: a node contributes at most two tables, and
    /// `build_plan` has already rejected above 32767 nodes.
    pub(crate) fn push_table(&mut self, table: Table) -> TableIdx {
        debug_assert!(self.tables.len() < u16::MAX as usize);
        let idx = TableIdx(self.tables.len() as u16);
        self.tables.push(table);
        idx
    }

    /// Store a baked reduce plan with the table its value index lives in,
    /// returning its `Instr::Reduce::plan_idx`.
    pub(crate) fn add_reduce_plan(&mut self, plan: crate::ops::ReducePlan, avi_table: Option<TableIdx>) -> PlanIdx {
        debug_assert_eq!(
            plan.avi.is_some(),
            avi_table.is_some(),
            "a value-index table exists iff the plan carries the bake that keys it",
        );
        let idx = PlanIdx(self.reduce_plans.len() as u16);
        self.reduce_plans.push(BakedReduce { plan, avi_table });
        idx
    }

    // ── Build ────────────────────────────────────────────────────────────

    /// Consume the builder into a runnable `VmHandle`, `out_reg` being the
    /// register the epoch's output is extracted from.
    pub(crate) fn build(self, reg_meta: Vec<RegisterMeta>, out_reg: u16) -> Box<VmHandle> {
        let regfile = RegisterFile::new(&reg_meta);
        // The trace registers name their own backing tables, so the bind list is
        // read off the metas rather than tracked alongside them.
        let trace_regs: Vec<(u16, TableIdx)> = reg_meta
            .iter()
            .enumerate()
            .filter_map(|(reg, m)| m.owned_table.map(|t| (reg as u16, t)))
            .collect();

        // Destructive-register liveness, over the EMITTED instructions — so an
        // elided node's register aliasing is seen through, not re-derived from
        // graph edges. Forward, so the last write wins.
        let mut last_read = vec![u32::MAX; reg_meta.len()];
        for (pc, instr) in self.instructions.iter().enumerate() {
            for reg in reads(instr).into_iter().flatten() {
                last_read[reg as usize] = pc as u32;
            }
        }
        // After the scan, not before: the sink can itself be an operand, and the
        // epoch epilogue reads it after the last instruction has run.
        last_read[out_reg as usize] = u32::MAX;

        let program = Program {
            instructions: self.instructions,
            reg_meta,
            predicates: self.predicates,
            maps: self.maps,
            reduce_plans: self.reduce_plans,
            last_read,
            out_reg,
        };

        Box::new(VmHandle {
            program,
            regfile,
            tables: self.tables,
            trace_regs,
        })
    }
}
