//! `ProgramBuilder` — the resource pools behind a `Program`, plus `push`.
//!
//! Emission constructs `Instr` literals directly (there is deliberately no
//! per-opcode constructor mirror); the builder's job is holding the resources
//! those instructions index — predicates, map plans, tables, and the baked
//! operator pools — and assembling the final `Program`. Each `push`/`add`
//! returns the index that *is* the instruction operand, so nothing has to be
//! numbered twice.

use super::*;
use gnitz_store::expr::MapPlan;
use gnitz_store::storage::Table;

pub(in crate::query) struct ProgramBuilder {
    instructions: Vec<Instr>,
    predicates: Vec<gnitz_expr::Evaluator>,
    maps: Vec<MapPlan>,
    tables: Vec<Table>,
    reduce_plans: Vec<BakedReduce>,
    topn_plans: Vec<BakedTopN>,
}

impl ProgramBuilder {
    pub(crate) fn new() -> Self {
        ProgramBuilder {
            instructions: Vec::with_capacity(16),
            predicates: Vec::new(),
            maps: Vec::new(),
            tables: Vec::new(),
            reduce_plans: Vec::new(),
            topn_plans: Vec::new(),
        }
    }

    pub(in crate::query) fn push(&mut self, instr: Instr) {
        self.instructions.push(instr);
    }

    // ── Resources ────────────────────────────────────────────────────────

    /// Take ownership of `pred`, returning its `Instr::Filter` operand.
    pub(in crate::query) fn push_predicate(&mut self, pred: gnitz_expr::Evaluator) -> PredIdx {
        let idx = PredIdx(self.predicates.len() as u16);
        self.predicates.push(pred);
        idx
    }

    /// Take ownership of `plan`, returning its `Instr::Map` operand.
    pub(in crate::query) fn push_map(&mut self, plan: MapPlan) -> MapIdx {
        let idx = MapIdx(self.maps.len() as u16);
        self.maps.push(plan);
        idx
    }

    /// Take ownership of `table`, returning the index `RegisterMeta::trace` names
    /// it by. The `u16` holds under `compiler::MAX_CIRCUIT_NODES`.
    pub(in crate::query) fn push_table(&mut self, table: Table) -> TableIdx {
        debug_assert!(self.tables.len() < u16::MAX as usize);
        let idx = TableIdx(self.tables.len() as u16);
        self.tables.push(table);
        idx
    }

    /// Store a baked reduce plan with the table its value index lives in,
    /// returning its `Instr::Reduce::plan_idx`.
    pub(in crate::query) fn add_reduce_plan(
        &mut self,
        plan: gnitz_store::ops::ReducePlan,
        avi_table: Option<TableIdx>,
    ) -> PlanIdx {
        debug_assert_eq!(
            plan.avi.is_some(),
            avi_table.is_some(),
            "a value-index table exists iff the plan carries the bake that keys it",
        );
        let idx = PlanIdx(self.reduce_plans.len() as u16);
        self.reduce_plans.push(BakedReduce { plan, avi_table });
        idx
    }

    /// Store a baked top-N plan with the table its ordered index lives in,
    /// returning its `Instr::TopN::plan_idx`.
    pub(in crate::query) fn add_topn_plan(
        &mut self,
        plan: gnitz_store::ops::TopNPlan,
        index_table: TableIdx,
    ) -> TopNIdx {
        let idx = TopNIdx(self.topn_plans.len() as u16);
        self.topn_plans.push(BakedTopN { plan, index_table });
        idx
    }

    // ── Build ────────────────────────────────────────────────────────────

    /// Consume the builder into a runnable `VmHandle`, `out_reg` being the
    /// register the epoch's output is extracted from.
    pub(in crate::query) fn build(mut self, mut reg_meta: Vec<RegisterMeta>, out_reg: DeltaReg) -> Box<VmHandle> {
        // Each pool grows by pushes and is then held for the cached plan's
        // lifetime, so its slack is dead heap per sub-plan, per view, per worker.
        // (`reduce_plans` has none: its stride is over 1024, so it grows exactly.)
        reg_meta.shrink_to_fit();
        self.instructions.shrink_to_fit();
        self.predicates.shrink_to_fit();
        self.maps.shrink_to_fit();
        self.tables.shrink_to_fit();

        let regfile = RegisterFile::new(&reg_meta);
        // The trace registers name their own backing tables, so the bind list is
        // read off the metas rather than tracked alongside them.
        let trace_regs: Vec<(TraceReg, TableIdx)> = reg_meta
            .iter()
            .enumerate()
            .filter_map(|(reg, m)| m.owned_table.map(|t| (TraceReg(reg as u16), t)))
            .collect();

        // Destructive-register liveness, over the EMITTED instructions — so an
        // elided node's register aliasing is seen through, not re-derived from
        // graph edges. Forward, so the last write wins.
        let mut last_read = vec![u32::MAX; reg_meta.len()];
        for (pc, instr) in self.instructions.iter().enumerate() {
            for reg in reads(instr).into_iter().flatten() {
                last_read[reg.at()] = pc as u32;
            }
        }
        // After the scan, not before: the sink can itself be an operand, and the
        // epoch epilogue reads it after the last instruction has run.
        last_read[out_reg.at()] = u32::MAX;

        // Read off the baked plans rather than accumulated during emission, so
        // `ReducePlan::from_wire` stays the single home of the conjunction.
        let pending_ground_row = self.reduce_plans.iter().any(|b| b.plan.seeds_ground);

        let program = Program {
            instructions: self.instructions,
            reg_meta,
            predicates: self.predicates,
            maps: self.maps,
            reduce_plans: self.reduce_plans,
            topn_plans: self.topn_plans,
            last_read,
            out_reg,
        };

        Box::new(VmHandle {
            program,
            regfile,
            tables: self.tables,
            trace_regs,
            pending_ground_row,
        })
    }
}
