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
    /// The traces the circuit itself integrates.
    integrates: Vec<(DeltaReg, StateIdx)>,
    schemas: Vec<SchemaDescriptor>,
    /// Per register, the seeded registers whose delta reaches it.
    reach: Vec<Vec<DeltaReg>>,
    /// The child stores the program addresses.
    layout: StateLayout,
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
        let out_reg = DeltaReg(self.schemas.len());
        let instr = Instr { in_reg, out_reg, op };
        let mut reach: Vec<DeltaReg> = Vec::new();
        for seed in instr.reads().into_iter().flatten().flat_map(|r| &self.reach[r.0]) {
            if !reach.contains(seed) {
                reach.push(*seed);
            }
        }
        // The integral an operator keeps of its own output or input.
        match &instr.op {
            Op::Reduce { out_trace: Integral::Own(trace), .. } => self.integrate(out_reg, *trace),
            Op::WeightClamp { hist: ClampHistory::Own(hist), .. } => self.integrate(in_reg, *hist),
            _ => {}
        }
        self.schemas.push(out_schema);
        self.reach.push(reach);
        self.instructions.push(instr);
        out_reg
    }

    /// The operator writing `reg`, with its first operand; `None` for a seeded
    /// register. Instructions are in out-register order: [`Self::push`] gives
    /// each a fresh register above every earlier one.
    pub(in crate::query) fn writer(&self, reg: DeltaReg) -> Option<(&Op, DeltaReg)> {
        let pc = self.instructions.binary_search_by_key(&reg.0, |i| i.out_reg.0).ok()?;
        let instr = &self.instructions[pc];
        Some((&instr.op, instr.in_reg))
    }

    /// Some seeded register's delta reaches both `a` and `b`.
    pub(in crate::query) fn share_a_seed(&self, a: DeltaReg, b: DeltaReg) -> bool {
        self.reach[a.0].iter().any(|seed| self.reach[b.0].contains(seed))
    }

    /// Push the sum of `a` and `b`, under the schema both widen to.
    pub(in crate::query) fn union(&mut self, a: DeltaReg, b: DeltaReg) -> Result<DeltaReg, String> {
        let schema = gnitz_zset::algebra::union_nullability_merge(&self.schema_of(a), &self.schema_of(b))?;
        Ok(self.push(a, schema, Op::Union { in_b: b }))
    }

    /// Push an exchange round of `in_reg` under `plan`, a keyed one: the round's
    /// receiver folds what it gathers.
    pub(in crate::query) fn round(&mut self, in_reg: DeltaReg, plan: Rc<ScatterPlan>) -> DeltaReg {
        debug_assert!(!plan.is_broadcast(), "a broadcast's fold is its sender's");
        self.push(in_reg, self.schema_of(in_reg), Op::Round(plan))
    }

    /// Accumulate `reg` into `trace` after every instruction of each epoch: a
    /// join's integrand. A reduce's own output trace and a clamp's own history
    /// are named by their operator, and [`Self::push`] integrates them.
    pub(in crate::query) fn integrate(&mut self, reg: DeltaReg, trace: StateIdx) {
        self.integrates.push((reg, trace));
    }

    /// Declare a child store of `schema` the program addresses as `name`.
    pub(in crate::query) fn declare(&mut self, name: String, schema: SchemaDescriptor) -> StateIdx {
        self.layout.declare(name, schema)
    }

    /// The schema register `reg` is labelled with.
    pub(in crate::query) fn schema_of(&self, reg: DeltaReg) -> SchemaDescriptor {
        self.schemas[reg.0]
    }

    /// The runnable plan, whose epoch output is extracted from `out_reg`, and
    /// the child stores it addresses, to be registered as its view's traces.
    pub(in crate::query) fn finish(self, out_reg: DeltaReg) -> (Vm, StateLayout) {
        let ProgramBuilder {
            instructions,
            integrates,
            schemas,
            layout,
            ..
        } = self;
        let n = instructions.len();
        // Forward from `reg`: an instruction is on its run iff it reads a
        // register the run has reached.
        let run = |reg: usize| -> Box<[usize]> {
            let mut reached = vec![false; schemas.len()];
            reached[reg] = true;
            let mut run = Vec::new();
            for (pc, instr) in instructions.iter().enumerate() {
                if instr.reads().into_iter().flatten().any(|r| reached[r.0]) {
                    reached[instr.out_reg.0] = true;
                    run.push(pc);
                }
            }
            let integrated = integrates.iter().enumerate().filter(|(_, (r, _))| reached[r.0]);
            run.extend(integrated.map(|(i, _)| n + i));
            run.into()
        };
        let mut regs: Vec<Reg> = (0..schemas.len())
            .map(|reg| Reg {
                schema: schemas[reg],
                fold: false,
                last_read: 0,
                run: run(reg),
            })
            .collect();
        // Forward, so a later reader replaces an earlier one. Over the emitted
        // instructions, which already name the register an elided node's
        // readers read.
        for (pc, instr) in instructions.iter().enumerate() {
            for reg in instr.reads().into_iter().flatten() {
                regs[reg.0].last_read = pc;
            }
            // Whether the operator reads its input at net weights.
            regs[instr.in_reg.0].fold |= match &instr.op {
                Op::WeightClamp { .. } | Op::JoinDT { .. } => true,
                Op::Reduce { plan, .. } => !plan.is_exact_linear(),
                Op::Filter(_)
                | Op::Map(_)
                | Op::Negate
                | Op::Union { .. }
                | Op::Share(_)
                | Op::Round(_)
                | Op::NullExtend { .. }
                | Op::Ordered { .. } => false,
            };
        }
        for (i, &(reg, _)) in integrates.iter().enumerate() {
            regs[reg.0].last_read = n + i;
        }
        // After the scan, not before: the output register can itself be an operand.
        regs[out_reg.0].last_read = usize::MAX;

        let vm = Vm {
            instructions: instructions.into_boxed_slice(),
            integrates: integrates.into_boxed_slice(),
            regs: regs.into_boxed_slice(),
            spare: Box::default(),
            out_reg,
        };
        (vm, layout)
    }
}
