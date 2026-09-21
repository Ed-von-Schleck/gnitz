//! `build`: the liveness and consolidation passes that turn an emitted plan into
//! a runnable [`VmHandle`].

use super::*;

/// Who last reads a register: an instruction, an integrate — which run after
/// every instruction — or nobody.
#[derive(Clone, Copy, PartialEq)]
enum LastRead {
    Nobody,
    Instr(usize),
    Integrate(usize),
}

/// Assemble one emitted plan. `out_reg` is the register the epoch's output is
/// extracted from; `integrates` run after the whole instruction range.
pub(in crate::query) fn build(
    mut instructions: Vec<Instr>,
    integrates: Vec<(DeltaReg, StateIdx)>,
    delta_schemas: Vec<SchemaDescriptor>,
    out_reg: DeltaReg,
) -> Box<VmHandle> {
    // Destructive-register liveness, over the EMITTED instructions — so an
    // elided node's register aliasing is seen through, not re-derived from
    // graph edges. Forward, so the last write wins.
    let mut last_read = vec![LastRead::Nobody; delta_schemas.len()];
    for (pc, instr) in instructions.iter().enumerate() {
        for (_, reg) in instr.operands() {
            last_read[reg.at()] = LastRead::Instr(pc);
        }
    }
    for (i, (reg, _)) in integrates.iter().enumerate() {
        last_read[reg.at()] = LastRead::Integrate(i);
    }
    // After the scan, not before: the sink can itself be an operand, and the
    // epoch epilogue reads it after the last integrate has run.
    last_read[out_reg.at()] = LastRead::Nobody;

    // Consolidation is the Z-set identity, so folding at the first reader
    // serves every later one.
    let mut fold_at = vec![None; delta_schemas.len()];
    for instr in &instructions {
        if facts(&instr.op).consolidates_in {
            fold_at[instr.in_reg.at()] = Some(first_read_of(&instructions, instr.in_reg));
        }
    }

    for (pc, instr) in instructions.iter_mut().enumerate() {
        instr.inert_on_empty = facts(&instr.op).inert_on_empty;
        for (i, reg) in instr.operands() {
            instr.takes[i] = last_read[reg.at()] == LastRead::Instr(pc);
            instr.folds[i] = fold_at[reg.at()] == Some(pc);
        }
    }

    let integrates: Vec<Integrate> = integrates
        .into_iter()
        .enumerate()
        .map(|(i, (reg, trace))| Integrate {
            reg,
            trace,
            take: last_read[reg.at()] == LastRead::Integrate(i),
        })
        .collect();

    let pending_ground_row = instructions
        .iter()
        .any(|i| matches!(&i.op, Op::Reduce { plan, .. } if plan.plan.seeds_ground));

    Box::new(VmHandle {
        // Each vector grew by pushes and is then held for the cached plan's
        // lifetime, so its slack is dead heap per sub-plan, per view, per worker.
        batches: delta_schemas.iter().map(Batch::empty_with_schema).collect(),
        program: Program {
            instructions: shrunk(instructions),
            integrates: shrunk(integrates),
            delta_schemas: shrunk(delta_schemas),
            out_reg,
        },
        pending_ground_row,
    })
}

fn shrunk<T>(mut v: Vec<T>) -> Vec<T> {
    v.shrink_to_fit();
    v
}

#[cfg(test)]
#[path = "tests/builder.rs"]
mod tests;
