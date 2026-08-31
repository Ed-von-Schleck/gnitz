//! Hydration: how a capacity-bounded view replays one key's output rows.
//!
//! A bounded view's sweep can reduce a stored row to a PK and one summed weight,
//! so a read that touches such a key recomputes it. This module resolves *where*
//! that recomputation seeds — off the circuit graph, then against the plan the
//! emitter produced.

use super::*;

/// Where a bounded view's per-key replay seeds: a register to feed, a store to
/// feed it from, and the program offset to dispatch from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Hydration {
    /// The offset the replay enters at, past the prologue whose output the seed
    /// replaces. `0` replays the whole program.
    pub start_pc: usize,
    pub in_reg: u16,
    pub seed: HydrationSeed,
}

/// The store a [`Hydration`] seeds from — the only axis the two eligible view
/// bodies differ on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HydrationSeed {
    /// Linear (`ScanDelta → Filter? → Map? → IntegrateSink`): the source
    /// relation's own store, feeding the `ScanDelta`'s register.
    Relation(i64),
    /// Inner equi-join: one branch's trace. The seed enters mid-program because
    /// the join key is not the source PK, so a key-restricted feed at the
    /// `ScanDelta` register would mean scanning the whole source.
    Trace(crate::query::vm::TableIdx),
}

/// The graph half of a bounded view's hydration plan: which relation a linear
/// body replays over, or which delta/trace node pair a join body seeds from.
///
/// The walk is over `loaded`, not the emitted instruction list, for the same
/// reason the five sibling `CompileOutput` annotations are: the instruction
/// stream elides nodes (`Filter(None)` aliases its input register, an identity
/// `Map` vanishes, `ScanDelta` and `IntegrateSink` emit nothing) and drops
/// `WorkerFilter` at `num_workers <= 1`, so it is worker-count dependent. Split
/// from [`derive_hydration`] because this half is the drift-prone one and needs
/// nothing but the circuit, so its rejections are directly testable.
fn hydration_nodes(loaded: &LoadedCircuit) -> Result<HydrationNodes, CompileError> {
    use gnitz_wire::{JoinKind, OpNode};

    // 1. From the sink's input, walk back through single-input Filter/Map nodes.
    let sink = loaded
        .ordered
        .iter()
        .copied()
        .find(|nid| matches!(loaded.nodes.get(nid), Some(OpNode::IntegrateSink)))
        .ok_or(CompileError::Rejected("bounded view: circuit has no IntegrateSink"))?;
    let mut cur = loaded.inputs(sink).unary();
    loop {
        match loaded.nodes.get(&cur) {
            Some(OpNode::Filter(_)) | Some(OpNode::Map(_)) => cur = loaded.inputs(cur).unary(),
            // The linear shape: the whole program replays over the source store,
            // seeded at this `ScanDelta`'s own register.
            Some(OpNode::ScanDelta { source, .. }) => {
                return Ok(HydrationNodes::Relation {
                    nid: cur,
                    source: *source as i64,
                })
            }
            Some(OpNode::Union) => break,
            _ => return Err(CompileError::Rejected("bounded view: unsupported circuit shape")),
        }
    }
    let union = cur;

    // 2. Both `Union` inputs must be `Join(DeltaTrace)`, optionally behind one
    //    `Map` — the per-branch projection that puts each branch's output back
    //    into canonical `[A, B]` column order. The node is in the circuit on both
    //    branches whatever `emit_map` does with it: the AB one is an identity the
    //    emitter elides, the BA one a real column permutation that emits.
    let (branch_a, branch_b) = loaded.inputs(union).binary();
    let through_map = |mut nid: i32| -> Option<i32> {
        if matches!(loaded.nodes.get(&nid), Some(OpNode::Map(_))) {
            nid = loaded.inputs(nid).unary();
        }
        matches!(loaded.nodes.get(&nid), Some(OpNode::Join(JoinKind::DeltaTrace))).then_some(nid)
    };
    let j_a = through_map(branch_a).ok_or(CompileError::Rejected(
        "bounded view: union input is not an inner delta/trace join",
    ))?;

    // 3. Seed check, stated directly on the graph rather than through a
    //    register-identity or schema-equality proxy: the trace `J_a` joins
    //    against must be the integral of `J_a`'s *own* delta port. Either branch
    //    computes the same product and each carries its own normalization map
    //    back to canonical `[A, B]` order, so taking `J_a` needs no left/right
    //    inference. The unchosen branch stays inert: the dispatch clears every
    //    delta register on entry, so `D_b` is empty and `J_b` unions in nothing.
    // Each join's trace port, checked to be an integral. Two calls, two messages,
    // so a rejection names the branch it came from.
    let trace_of = |j: i32, whose: &'static str| -> Result<i32, CompileError> {
        let t = loaded.inputs(j).binary().1;
        matches!(loaded.nodes.get(&t), Some(OpNode::IntegrateTrace))
            .then_some(t)
            .ok_or(CompileError::Rejected(whose))
    };
    let d_a = loaded.inputs(j_a).binary().0;
    trace_of(j_a, "bounded view: the seeded join's trace port is not an integral")?;
    // `T_b` integrates the *other* branch's delta, so the seed is the trace whose
    // input node is `D_a` — found on the sibling join.
    let j_b = through_map(branch_b).ok_or(CompileError::Rejected(
        "bounded view: union input is not an inner delta/trace join",
    ))?;
    let t_a = trace_of(j_b, "bounded view: the sibling join's trace port is not an integral")?;
    if loaded.inputs(t_a).unary() != d_a {
        return Err(CompileError::Rejected(
            "bounded view: the join's trace port is not the other branch's delta integral",
        ));
    }

    Ok(HydrationNodes::Join { d_a, t_a })
}

/// What [`hydration_nodes`] resolved out of the graph, before any program
/// lookup: the seeding `ScanDelta` and its relation for a linear body, or the
/// delta and trace nodes of the join branch a replay seeds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HydrationNodes {
    Relation { nid: i32, source: i64 },
    Join { d_a: i32, t_a: i32 },
}

/// Resolve those nodes against the plan the emitter just produced.
///
/// Registers and offsets come from the emitter's own node-keyed maps, never from a
/// node's index in `ordered`: the two agree only for a node that emitted —
/// `Filter(None)` aliases its input's register, an identity `Map` vanishes, a
/// skipped `Distinct` aliases, and `Reduce` redirects.
pub(super) fn derive_hydration(loaded: &LoadedCircuit, plan: &PlanBuildResult) -> Result<Hydration, CompileError> {
    let reg_of = |nid: i32| {
        plan.out_reg_of.get(&nid).copied().ok_or(CompileError::Rejected(
            "bounded view: a hydration node is not in the plan",
        ))
    };

    let hydration = match hydration_nodes(loaded)? {
        HydrationNodes::Relation { nid, source } => Hydration {
            start_pc: 0,
            in_reg: reg_of(nid)?,
            seed: HydrationSeed::Relation(source),
        },
        HydrationNodes::Join { d_a, t_a } => {
            // Registers and the start offset, off the emitter's own bookkeeping.
            let seed_table = plan
                .vm
                .program
                .reg_meta
                .get(reg_of(t_a)? as usize)
                .and_then(|m| m.owned_table)
                .ok_or(CompileError::Rejected(
                    "bounded view: trace register has no owned table",
                ))?;
            Hydration {
                // The replay enters past the seeded node's own instructions.
                start_pc: plan
                    .instr_end
                    .get(&d_a)
                    .copied()
                    .ok_or(CompileError::Rejected("bounded view: delta node is not in the plan"))?,
                in_reg: reg_of(d_a)?,
                seed: HydrationSeed::Trace(seed_table),
            }
        }
    };
    reject_state_writers(plan, hydration.start_pc)?;

    Ok(hydration)
}

/// Trust boundary on the program the read-only dispatch will run from `start_pc`:
/// that dispatch suppresses `Integrate`, so any *other* state writer would make a
/// read mutate the state it reads. None is reachable from an eligible shape, so
/// this turns a planner that under-rejects into a loud DDL failure rather than a
/// silently-mutating read. `writes_state_during_replay` is exhaustive over
/// `Instr` and lives beside the arm that does the suppressing, so a new
/// state-writing opcode cannot slip past this and the two cannot drift.
fn reject_state_writers(plan: &PlanBuildResult, start_pc: usize) -> Result<(), CompileError> {
    if plan.vm.program.instructions[start_pc..]
        .iter()
        .any(crate::query::vm::writes_state_during_replay)
    {
        return Err(CompileError::Rejected(
            "bounded view: the replayed program writes operator state",
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/hydration.rs"]
mod tests;
