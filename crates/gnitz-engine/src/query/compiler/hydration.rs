//! Hydration: how a capacity-bounded view replays one key's output rows.
//!
//! A bounded view's sweep can reduce a stored row to a PK and one summed weight,
//! so a read that touches such a key recomputes it. This module resolves *where*
//! that recomputation seeds — off the circuit graph, then against the plan the
//! emitter produced.

use super::*;

/// Where a bounded view's per-key replay seeds. Both variants name a register to
/// feed and a store to feed it from; they differ only in where that store lives
/// and, for the join, in entering the program past its own prologue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Hydration {
    /// Linear (`ScanDelta → Filter? → Map? → IntegrateSink`): seed the
    /// `ScanDelta`'s register `in_reg` from relation `source`'s store and replay
    /// the whole program.
    Relation { in_reg: u16, source: i64 },
    /// Inner equi-join: seed `in_reg` from the trace at `seed_table` and
    /// dispatch from `start_pc`. The seed must enter mid-program because the join
    /// key is not the source PK, so a key-restricted feed at the `ScanDelta`
    /// register would mean scanning the whole source.
    Join {
        start_pc: usize,
        in_reg: u16,
        seed_table: crate::query::vm::TableIdx,
    },
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

    let (d_a, t_a) = match hydration_nodes(loaded)? {
        HydrationNodes::Relation { nid, source } => {
            let in_reg = reg_of(nid)?;
            reject_state_writers(plan, 0)?;
            return Ok(Hydration::Relation { in_reg, source });
        }
        HydrationNodes::Join { d_a, t_a } => (d_a, t_a),
    };

    // Registers and the start offset, off the emitter's own bookkeeping.
    let in_reg = reg_of(d_a)?;
    let seed_table = plan
        .vm
        .program
        .reg_meta
        .get(reg_of(t_a)? as usize)
        .and_then(|m| m.owned_table)
        .ok_or(CompileError::Rejected(
            "bounded view: trace register has no owned table",
        ))?;
    // The replay enters past the seeded node's own instructions.
    let start_pc = plan
        .instr_end
        .get(&d_a)
        .copied()
        .ok_or(CompileError::Rejected("bounded view: delta node is not in the plan"))?;
    reject_state_writers(plan, start_pc)?;

    Ok(Hydration::Join {
        start_pc,
        in_reg,
        seed_table,
    })
}

/// Trust boundary on the program the read-only dispatch will run from `start_pc`:
/// that dispatch suppresses `Integrate`, so any *other* state writer would make a
/// read mutate the state it reads. None is reachable from an eligible shape, so
/// this turns a planner that under-rejects into a loud DDL failure rather than a
/// silently-mutating read. `writes_state` is exhaustive over `Instr`, so a new
/// state-writing opcode cannot slip past this.
fn reject_state_writers(plan: &PlanBuildResult, start_pc: usize) -> Result<(), CompileError> {
    if plan.vm.program.instructions[start_pc..]
        .iter()
        .any(|i| !matches!(i, Instr::Integrate { .. }) && crate::query::vm::writes_state(i))
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
mod tests {
    use super::*;
    use gnitz_wire::{JoinKind, MapKind, OpNode};

    /// The inner-equi-join shape `emit_equi_join_terms` produces, as node ids:
    /// two `ScanDelta`s, a reindex `Map` and an `IntegrateTrace` per side, the
    /// two cross-wired `Join(DeltaTrace)` terms behind their normalization maps,
    /// a `Union`, a residual `Filter`, a projection `Map`, and the sink.
    ///
    /// ```text
    ///   0 scanA → 2 reindexA ─┬─→ 4 traceA ──────────┐
    ///                         └─────────────┐        │
    ///   1 scanB → 3 reindexB ─┬─→ 5 traceB ─┼→ 6 J_ab│ (delta=2, trace=5)
    ///                         └─────────────┴────────┴→ 7 J_ba (delta=3, trace=4)
    ///   6 → 8 map → 10 union ← 9 map ← 7;  10 → 11 filter → 12 map → 13 sink
    /// ```
    fn equi_join_circuit() -> LoadedCircuit {
        let (nodes, edges) = equi_join_parts();
        loaded_for_test(nodes, edges)
    }

    /// The same circuit's raw parts, for the tests that break one wire before
    /// building it.
    #[allow(clippy::type_complexity)]
    fn equi_join_parts() -> (HashMap<i32, OpNode>, Vec<(i32, i32, i32)>) {
        let m = |cols: Vec<u32>| OpNode::Map(MapKind::Projection(cols));
        let nodes = HashMap::from([
            (0, scan_delta(100)),
            (1, scan_delta(200)),
            (2, m(vec![0])),
            (3, m(vec![0])),
            (4, OpNode::IntegrateTrace),
            (5, OpNode::IntegrateTrace),
            (6, OpNode::Join(JoinKind::DeltaTrace)),
            (7, OpNode::Join(JoinKind::DeltaTrace)),
            (8, m(vec![0])),
            (9, m(vec![0])),
            (10, OpNode::Union),
            (11, OpNode::Filter(None)),
            (12, m(vec![0])),
            (13, OpNode::IntegrateSink),
        ]);
        let edges = vec![
            (0, 2, PORT_IN),
            (1, 3, PORT_IN),
            (2, 4, PORT_IN),
            (3, 5, PORT_IN),
            (2, 6, PORT_IN_A),
            (5, 6, PORT_TRACE),
            (3, 7, PORT_IN_A),
            (4, 7, PORT_TRACE),
            (6, 8, PORT_IN),
            (7, 9, PORT_IN),
            (8, 10, PORT_IN_A),
            (9, 10, PORT_IN_B),
            (10, 11, PORT_IN),
            (11, 12, PORT_IN),
            (12, 13, PORT_IN),
        ];
        (nodes, edges)
    }

    /// The seed is resolved by the *cross-wiring*, not by position: `J_a`'s trace
    /// port is the other branch's integral, so the trace whose input is `J_a`'s
    /// own delta port lives on the sibling join. Getting that backwards would
    /// seed the replay from the wrong side and silently compute a different
    /// product.
    #[test]
    fn hydration_seeds_from_the_cross_wired_trace() {
        let lc = equi_join_circuit();
        // `d_a` is the reindex feeding `J_ab`'s delta port (node 2); `t_a` is the
        // trace that integrates *it* (node 4), which hangs off `J_ba`.
        assert_eq!(hydration_nodes(&lc).unwrap(), HydrationNodes::Join { d_a: 2, t_a: 4 },);
    }

    /// The linear shape resolves to its source relation, through any number of
    /// filter/map nodes.
    #[test]
    fn hydration_of_a_linear_circuit_names_its_source() {
        let lc = loaded_for_test(
            HashMap::from([
                (0, scan_delta(77)),
                (1, OpNode::Filter(None)),
                (2, OpNode::Map(MapKind::Projection(vec![0]))),
                (3, OpNode::IntegrateSink),
            ]),
            vec![(0, 1, PORT_IN), (1, 2, PORT_IN), (2, 3, PORT_IN)],
        );
        assert_eq!(
            hydration_nodes(&lc).unwrap(),
            HydrationNodes::Relation { nid: 0, source: 77 }
        );
    }

    /// Every structural mismatch is a `Rejected`, never a silent `None`: a
    /// bounded view must not reach its store with no way to hydrate it. These are
    /// the trust boundary behind the planner's own eligibility gate — no SQL
    /// reaches them, which is exactly why they are asserted here.
    #[test]
    fn a_malformed_circuit_is_rejected_rather_than_guessed_at() {
        let rejected = |lc: LoadedCircuit, what: &str| match hydration_nodes(&lc) {
            Err(CompileError::Rejected(_)) => {}
            other => panic!("{what}: expected Rejected, got {:?}", other.map(|h| format!("{h:?}"))),
        };

        // No sink at all.
        rejected(
            loaded_for_test(HashMap::from([(0, scan_delta(1))]), vec![]),
            "sinkless circuit",
        );
        // A shape the walk cannot replay (a Reduce under the sink).
        rejected(
            loaded_for_test(
                HashMap::from([
                    (0, scan_delta(1)),
                    (
                        1,
                        OpNode::Reduce {
                            group_cols: vec![0],
                            agg: vec![(gnitz_wire::AggFunc::Count, 0)],
                            global_ground: false,
                            out_key: crate::schema::ReduceOutKey::SyntheticFold,
                        },
                    ),
                    (2, OpNode::IntegrateSink),
                ]),
                vec![(0, 1, PORT_IN), (1, 2, PORT_IN)],
            ),
            "reduce under the sink",
        );
        // A union whose inputs are not delta/trace joins.
        rejected(
            loaded_for_test(
                HashMap::from([
                    (0, scan_delta(1)),
                    (1, scan_delta(2)),
                    (2, OpNode::Union),
                    (3, OpNode::IntegrateSink),
                ]),
                vec![(0, 2, PORT_IN_A), (1, 2, PORT_IN_B), (2, 3, PORT_IN)],
            ),
            "union of two scans",
        );

        // The cross-wiring broken: both joins trace against the SAME integral, so
        // no trace integrates `J_a`'s own delta port.
        let (nodes, mut edges) = equi_join_parts();
        edges.retain(|&(s, d, p)| !(s == 4 && d == 7 && p == PORT_TRACE));
        edges.push((5, 7, PORT_TRACE));
        rejected(
            loaded_for_test(nodes, edges),
            "trace port is not the other branch's delta integral",
        );

        // A join whose trace port is not an integral at all.
        let (mut nodes, edges) = equi_join_parts();
        nodes.insert(4, OpNode::Filter(None));
        rejected(loaded_for_test(nodes, edges), "trace port is not an integral");
    }
}
