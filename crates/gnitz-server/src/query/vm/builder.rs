//! [`ProgramBuilder`]: assembles one emitted plan over the registers it
//! allocates, then derives the register analysis that makes it a runnable
//! [`Vm`].

use super::*;

/// One plan under construction. Every register is allocated here — a seeded one
/// by [`Self::seed`], an instruction's output by [`Self::push`] — so each is
/// written once, before any instruction reads it: a fold at the write serves
/// every reader, and a take by the last reader robs none.
#[derive(Default)]
pub(in crate::query) struct ProgramBuilder {
    instructions: Vec<Instr>,
    /// The traces the circuit itself integrates; each op's own integral is
    /// added by [`Self::finish`].
    integrates: Vec<(DeltaReg, StateIdx)>,
    schemas: Vec<SchemaDescriptor>,
    /// Per register, the seeded registers whose delta reaches it.
    reach: Vec<Vec<DeltaReg>>,
}

impl ProgramBuilder {
    /// A fresh register no instruction writes: the epoch seeds it.
    pub(in crate::query) fn seed(&mut self, schema: SchemaDescriptor) -> DeltaReg {
        let reg = DeltaReg(self.schemas.len());
        self.schemas.push(schema);
        self.reach.push(vec![reg]);
        reg
    }

    /// Push `op` over `in_reg`, writing a fresh register of `out_schema`.
    pub(in crate::query) fn push(&mut self, in_reg: DeltaReg, out_schema: SchemaDescriptor, op: Op) -> DeltaReg {
        let out_reg = self.seed(out_schema);
        let facts = facts(&op);
        let instr = Instr { in_reg, out_reg, op, facts };
        let mut reach: Vec<DeltaReg> = Vec::new();
        for seed in instr.reads().into_iter().flatten().flat_map(|r| &self.reach[r.at()]) {
            if !reach.contains(seed) {
                reach.push(*seed);
            }
        }
        self.reach[out_reg.at()] = reach;
        self.instructions.push(instr);
        out_reg
    }

    /// Some seeded register's delta reaches both `a` and `b`.
    pub(in crate::query) fn share_a_seed(&self, a: DeltaReg, b: DeltaReg) -> bool {
        self.reach[a.at()].iter().any(|seed| self.reach[b.at()].contains(seed))
    }

    /// Push an exchange round of `in_reg` under `plan`.
    pub(in crate::query) fn round(&mut self, in_reg: DeltaReg, plan: Rc<ScatterPlan>) -> DeltaReg {
        let seeds = self.reach[in_reg.at()].as_slice().into();
        self.push(in_reg, self.schema_of(in_reg), Op::Round { plan, seeds })
    }

    /// Accumulate `reg` into `trace` after every instruction of each epoch.
    pub(in crate::query) fn integrate(&mut self, reg: DeltaReg, trace: StateIdx) {
        self.integrates.push((reg, trace));
    }

    /// The schema register `reg` is labelled with.
    pub(in crate::query) fn schema_of(&self, reg: DeltaReg) -> SchemaDescriptor {
        self.schemas[reg.at()]
    }

    /// The runnable plan, whose epoch output is extracted from `out_reg`.
    pub(in crate::query) fn finish(self, out_reg: DeltaReg) -> Vm {
        let ProgramBuilder {
            instructions, mut integrates, schemas, ..
        } = self;
        integrates.extend(instructions.iter().filter_map(Instr::own_integral));

        // Over the emitted instructions, which already name the register an
        // elided node's readers read.
        let mut regs: Vec<Reg> = schemas
            .into_iter()
            .map(|schema| Reg {
                schema,
                fold: false,
                last_read: LastRead::Nobody,
                feeds_round: false,
            })
            .collect();
        // Forward, so a later reader replaces an earlier one.
        for (pc, instr) in instructions.iter().enumerate() {
            for reg in instr.reads().into_iter().flatten() {
                regs[reg.at()].last_read = LastRead::Instr(pc);
            }
            regs[instr.in_reg.at()].fold |= instr.facts.consolidates_in;
        }
        // A broadcast hands every worker every row, so the fold its gathering
        // register owes is the sender's: one sort, not one per worker. A keyed
        // round's rows are each gathered once and folded there. Backward, so a
        // broadcast behind a broadcast hands the fold on.
        for instr in instructions.iter().rev() {
            if let Op::Round { plan, seeds } = &instr.op {
                if plan.is_broadcast() {
                    regs[instr.in_reg.at()].fold |= regs[instr.out_reg.at()].fold;
                }
                for seed in seeds.iter() {
                    regs[seed.at()].feeds_round = true;
                }
            }
        }
        for (i, &(reg, _)) in integrates.iter().enumerate() {
            regs[reg.at()].last_read = LastRead::Integrate(i);
        }
        // After the scan, not before: the output register can itself be an operand.
        regs[out_reg.at()].last_read = LastRead::Nobody;

        Vm {
            pending_ground_row: instructions.iter().any(|i| !i.facts.inert_on_empty),
            batches: regs.iter().map(|r| Batch::empty_with_schema(&r.schema)).collect(),
            instructions: instructions.into_boxed_slice(),
            integrates: integrates.into_boxed_slice(),
            regs: regs.into_boxed_slice(),
            out_reg,
        }
    }
}

#[cfg(test)]
#[path = "tests/builder.rs"]
mod tests;
