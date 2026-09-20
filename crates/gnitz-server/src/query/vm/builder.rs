//! `build`: the liveness and consolidation passes that turn an emitted plan into
//! a runnable [`VmHandle`].

use super::*;

/// Assemble one emitted plan. `out_reg` is the register the epoch's output is
/// extracted from; `integrates` run after the whole instruction range.
pub(in crate::query) fn build(
    instructions: Vec<Instr>,
    integrates: Vec<(DeltaReg, StateIdx)>,
    delta_schemas: Vec<SchemaDescriptor>,
    state: CircuitState,
    out_reg: DeltaReg,
) -> Box<VmHandle> {
    // Destructive-register liveness, over the EMITTED instructions — so an
    // elided node's register aliasing is seen through, not re-derived from
    // graph edges. Forward, so the last write wins.
    let mut last_read = vec![u32::MAX; delta_schemas.len()];
    for (pc, instr) in instructions.iter().enumerate() {
        for reg in instr.reads().into_iter().flatten() {
            last_read[reg.at()] = pc as u32;
        }
    }
    for (i, (in_reg, _)) in integrates.iter().enumerate() {
        last_read[in_reg.at()] = (instructions.len() + i) as u32;
    }
    // After the scan, not before: the sink can itself be an operand, and the
    // epoch epilogue reads it after the last instruction has run.
    last_read[out_reg.at()] = u32::MAX;

    // Consolidation is the Z-set identity, so folding at the first reader
    // serves every later one.
    let mut consolidate_at = vec![u32::MAX; delta_schemas.len()];
    for instr in &instructions {
        if facts(&instr.op).consolidates_in {
            consolidate_at[instr.in_reg.at()] = first_read_of(&instructions, instr.in_reg) as u32;
        }
    }

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
            last_read,
            consolidate_at,
            out_reg,
        },
        state,
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
