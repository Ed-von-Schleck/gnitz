//! `build`: the register analysis that turns an emitted plan into a runnable
//! [`Vm`].

use super::*;

/// Assemble one emitted plan. `out_reg` is the register the epoch's output is
/// extracted from; `integrates` are the join traces the circuit declares; each
/// op's own integral is added here. All of them run after the whole instruction
/// range.
pub(in crate::query) fn build(
    instructions: Vec<Instr>,
    mut integrates: Vec<(DeltaReg, StateIdx)>,
    delta_schemas: Vec<SchemaDescriptor>,
    out_reg: DeltaReg,
) -> Vm {
    // Every reader of a register runs after its one write, so a fold at the
    // write serves every reader and a take by the last reader robs none.
    #[cfg(debug_assertions)]
    {
        let mut seen = vec![false; delta_schemas.len()];
        for instr in &instructions {
            for r in instr.reads().into_iter().flatten() {
                seen[r.at()] = true;
            }
            assert!(
                !std::mem::replace(&mut seen[instr.out_reg.at()], true),
                "a register is written once, before any read",
            );
        }
    }

    integrates.extend(instructions.iter().filter_map(Instr::own_integral));

    // Over the EMITTED instructions — so an elided node's register aliasing is
    // seen through, not re-derived from graph edges.
    let mut regs: Vec<Reg> = delta_schemas
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
        regs[instr.in_reg.at()].fold |= facts(&instr.op).consolidates_in;
    }
    for (i, &(reg, _)) in integrates.iter().enumerate() {
        regs[reg.at()].last_read = LastRead::Integrate(i);
    }
    // After the scan, not before: the sink can itself be an operand.
    regs[out_reg.at()].last_read = LastRead::Nobody;

    let pending_ground_row = instructions.iter().any(|i| !i.inert_on_empty);

    Vm {
        batches: regs.iter().map(|r| Batch::empty_with_schema(&r.schema)).collect(),
        program: Program {
            instructions: instructions.into_boxed_slice(),
            integrates: integrates.into_boxed_slice(),
            regs: regs.into_boxed_slice(),
            out_reg,
        },
        pending_ground_row,
    }
}

#[cfg(test)]
#[path = "tests/builder.rs"]
mod tests;
