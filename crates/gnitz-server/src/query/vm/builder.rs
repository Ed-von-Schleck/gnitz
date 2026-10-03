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
}

impl ProgramBuilder {
    /// A fresh register no instruction writes: the epoch seeds it.
    pub(in crate::query) fn seed(&mut self, schema: SchemaDescriptor) -> DeltaReg {
        let reg = DeltaReg(
            u16::try_from(self.schemas.len()).expect("`MAX_CIRCUIT_NODES` bounds a plan's registers below u16::MAX"),
        );
        self.schemas.push(schema);
        reg
    }

    /// Push `op` over `in_reg`, writing a fresh register of `out_schema`.
    pub(in crate::query) fn push(&mut self, in_reg: DeltaReg, out_schema: SchemaDescriptor, op: Op) -> DeltaReg {
        let out_reg = self.seed(out_schema);
        let facts = facts(&op);
        self.instructions.push(Instr { in_reg, out_reg, op, facts });
        out_reg
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
        let ProgramBuilder { instructions, mut integrates, schemas } = self;
        integrates.extend(instructions.iter().filter_map(Instr::own_integral));

        // Over the emitted instructions, which already name the register an
        // elided node's readers read.
        let mut regs: Vec<Reg> = schemas
            .into_iter()
            .map(|schema| Reg {
                schema,
                fold: false,
                last_read: LastRead::Nobody,
            })
            .collect();
        // Forward, so a later reader replaces an earlier one.
        for (pc, instr) in instructions.iter().enumerate() {
            for reg in instr.reads().into_iter().flatten() {
                regs[reg.at()].last_read = LastRead::Instr(pc);
            }
            regs[instr.in_reg.at()].fold |= instr.facts.consolidates_in;
        }
        for (i, &(reg, _)) in integrates.iter().enumerate() {
            regs[reg.at()].last_read = LastRead::Integrate(i);
        }
        // After the scan, not before: the sink can itself be an operand.
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
