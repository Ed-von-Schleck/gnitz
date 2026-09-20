use super::fixtures::*;
use super::*;
use crate::test_support::make_schema_u128_i64;
use gnitz_wire::{AggDescriptor, AggFunc};

/// An integrate runs after the whole instruction range, so it is the last reader
/// of its register and ingests by move rather than by copy.
#[test]
fn an_integrate_is_the_last_reader_of_its_register() {
    let dir = tempfile::tempdir().unwrap();
    let registry = vm_registry(dir.path());
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();

    let t0 = p.table(&registry, dir.path(), "t0", schema);
    let t1 = p.table(&registry, dir.path(), "t1", schema);
    p.push(0, 1, Op::Negate);
    p.push(1, 2, Op::Negate);
    p.integrate(0, t0);
    p.integrate(1, t1);

    let vm = p.build(vec![schema; 3], 2);
    let n = vm.program.instructions.len();
    assert_eq!(vm.program.last_read[0], n as u32);
    assert_eq!(vm.program.last_read[1], n as u32 + 1);
    assert_eq!(
        vm.program.last_read[2],
        u32::MAX,
        "the sink is never taken out from under the epoch epilogue"
    );
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
    let mut p = TestPlan::default();

    let hist = p.table(&registry, dir.path(), "hist", schema);
    let join_trace = p.table(&registry, dir.path(), "jt", schema);
    let red_trace = p.table(&registry, dir.path(), "rt", schema);
    let lin_trace = p.table(&registry, dir.path(), "lt", schema);

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
    assert!(avi_plan.consolidates_input());
    let avi_table = p.table(&registry, dir.path(), "avi", avi_plan.avi.as_ref().unwrap().schema);

    let linear_plan =
        gnitz_store::ops::ReducePlan::from_wire(&schema, &[], &[AggDescriptor::COUNT_STAR], false, true).unwrap();
    assert!(!linear_plan.consolidates_input());

    let probe = gnitz_store::ops::JoinPlan::from_wire(gnitz_wire::JoinKind::Equi, false, &schema, &schema)
        .unwrap()
        .probe;

    // reg 0 = clamp input, reg 2 = join delta, reg 4 = avi-reduce input,
    // reg 6 = linear-reduce input. Each is negated first, so the pc that folds
    // it is that negate and not the consolidating instruction.
    p.push(0, 1, Op::Negate);
    p.push(
        0,
        8,
        Op::WeightClamp {
            hist,
            preset: gnitz_store::ops::ClampPreset::Distinct,
        },
    );
    p.push(2, 3, Op::Negate);
    p.push(2, 9, Op::JoinDT { trace: join_trace, probe });
    p.push(4, 5, Op::Negate);
    p.push(
        4,
        10,
        Op::Reduce {
            out_trace: red_trace,
            plan: Box::new(BakedReduce {
                plan: avi_plan,
                avi_table: Some(avi_table),
            }),
        },
    );
    p.push(6, 7, Op::Negate);
    p.push(
        6,
        11,
        Op::Reduce {
            out_trace: lin_trace,
            plan: Box::new(BakedReduce { plan: linear_plan, avi_table: None }),
        },
    );

    let vm = p.build(vec![schema; 12], 11);
    let marked: Vec<(usize, u32)> = vm
        .program
        .consolidate_at
        .iter()
        .enumerate()
        .filter(|&(_, &pc)| pc != u32::MAX)
        .map(|(r, &pc)| (r, pc))
        .collect();
    // The clamp input, the join delta and the value-indexed reduce's input, each
    // at the negate that reads it first. The linear reduce's input (reg 6) is
    // absent.
    assert_eq!(marked, vec![(0, 0), (2, 2), (4, 4)]);
}
