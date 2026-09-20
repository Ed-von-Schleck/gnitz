use super::fixtures::*;
use super::*;
use crate::test_support::make_schema_u128_i64;
use gnitz_wire::{AggDescriptor, AggFunc};

/// A delta register nothing else names, so each instruction below reads a
/// register of its own.
fn reg(i: u16) -> DeltaReg {
    DeltaReg(i)
}

/// `Integrate` runs after every other instruction whatever order the emitter
/// pushed them in, which is what makes it the last reader of its register.
#[test]
fn every_integrate_is_scheduled_last() {
    let dir = tempfile::tempdir().unwrap();
    let registry = vm_registry(dir.path());
    let schema = make_schema_u128_i64();
    let mut b = ProgramBuilder::new();

    let t0 = owned_table(&mut b, &registry, dir.path(), "t0", schema);
    let t1 = owned_table(&mut b, &registry, dir.path(), "t1", schema);
    // Emission order: integrate, negate, integrate, negate.
    b.push(Instr::Integrate {
        in_reg: reg(0),
        trace_reg: TraceReg(1),
    });
    b.push(Instr::Negate {
        in_reg: reg(0),
        out_reg: reg(2),
    });
    b.push(Instr::Integrate {
        in_reg: reg(2),
        trace_reg: TraceReg(3),
    });
    b.push(Instr::Negate {
        in_reg: reg(2),
        out_reg: reg(4),
    });

    let meta = vec![
        RegisterMeta::delta(schema),
        RegisterMeta::trace(schema, t0),
        RegisterMeta::delta(schema),
        RegisterMeta::trace(schema, t1),
        RegisterMeta::delta(schema),
    ];
    let vm = b.build(meta, reg(4));
    let is_integrate: Vec<bool> = vm
        .program
        .instructions
        .iter()
        .map(|i| matches!(i, Instr::Integrate { .. }))
        .collect();
    assert_eq!(is_integrate, [false, false, true, true]);

    // Being last is what earns the move: each integrate's register is released
    // by it rather than cloned into it.
    for (pc, instr) in vm.program.instructions.iter().enumerate() {
        if let Instr::Integrate { in_reg, .. } = instr {
            assert_eq!(vm.program.last_read[in_reg.at()], pc as u32);
        }
    }
}

/// `consolidate_at` marks each consolidating instruction's input at its FIRST
/// reader, and nothing else. Here every marked register is read by a cheaper
/// instruction first, so the mark cannot be confused with the reader that needs
/// the fold.
#[test]
fn consolidate_at_marks_the_first_reader_of_every_folded_register() {
    let dir = tempfile::tempdir().unwrap();
    let registry = vm_registry(dir.path());
    let schema = make_schema_u128_i64();
    let mut b = ProgramBuilder::new();

    let hist = owned_table(&mut b, &registry, dir.path(), "hist", schema);
    let join_trace = owned_table(&mut b, &registry, dir.path(), "jt", schema);
    let red_trace = owned_table(&mut b, &registry, dir.path(), "rt", schema);
    let lin_trace = owned_table(&mut b, &registry, dir.path(), "lt", schema);

    // MIN carries a value index, so `op_reduce` consolidates; COUNT alone does
    // not.
    let avi_plan = gnitz_store::ops::ReducePlan::from_wire(
        &schema,
        &[],
        &[
            AggDescriptor { col_idx: 1, agg_op: AggFunc::Min },
            AggDescriptor::COUNT_STAR,
        ],
        false,
        true,
    )
    .unwrap();
    assert!(avi_plan.avi.is_some());
    let avi_table = owned_table(&mut b, &registry, dir.path(), "avi", avi_plan.avi.as_ref().unwrap().schema);
    let avi_idx = b.add_reduce_plan(avi_plan, Some(avi_table));

    let linear_plan =
        gnitz_store::ops::ReducePlan::from_wire(&schema, &[], &[AggDescriptor::COUNT_STAR], false, true).unwrap();
    assert!(linear_plan.avi.is_none());
    let linear_idx = b.add_reduce_plan(linear_plan, None);

    let probe = gnitz_store::ops::JoinPlan::from_wire(gnitz_wire::JoinKind::Equi, false, &schema, &schema)
        .unwrap()
        .probe;

    // reg 0 = clamp input, reg 4 = join delta, reg 8 = avi-reduce input,
    // reg 11 = linear-reduce input. Each is negated first, so the pc that folds
    // it is that negate and not the consolidating instruction.
    let negate = |b: &mut ProgramBuilder, from: u16, to: u16| {
        b.push(Instr::Negate {
            in_reg: reg(from),
            out_reg: reg(to),
        })
    };
    negate(&mut b, 0, 2);
    b.push(Instr::WeightClamp {
        in_reg: reg(0),
        hist_reg: TraceReg(1),
        out_reg: reg(3),
        preset: gnitz_store::ops::ClampPreset::Distinct,
    });
    negate(&mut b, 4, 6);
    b.push(Instr::JoinDT {
        delta_reg: reg(4),
        trace_reg: TraceReg(5),
        out_reg: reg(7),
        probe,
    });
    negate(&mut b, 8, 10);
    b.push(Instr::Reduce {
        in_reg: reg(8),
        trace_out_reg: TraceReg(9),
        out_reg: reg(13),
        plan_idx: avi_idx,
    });
    negate(&mut b, 11, 14);
    b.push(Instr::Reduce {
        in_reg: reg(11),
        trace_out_reg: TraceReg(12),
        out_reg: reg(15),
        plan_idx: linear_idx,
    });

    let mut meta = vec![RegisterMeta::delta(schema); 16];
    for (i, t) in [(1, hist), (5, join_trace), (9, red_trace), (12, lin_trace)] {
        meta[i] = RegisterMeta::trace(schema, t);
    }
    let vm = b.build(meta, reg(15));

    let marked: Vec<(usize, u32)> = vm
        .program
        .consolidate_at
        .iter()
        .enumerate()
        .filter(|&(_, &pc)| pc != u32::MAX)
        .map(|(r, &pc)| (r, pc))
        .collect();
    // The clamp input, the join delta and the value-indexed reduce's input, each
    // at the negate that reads it first. The linear reduce's input (reg 11) is
    // absent.
    assert_eq!(marked, vec![(0, 0), (4, 2), (8, 4)]);
}
