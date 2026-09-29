use super::fixtures::*;
use super::*;
use crate::test_support::make_schema_u128_i64;
use gnitz_wire::{AggDescriptor, AggFunc};

/// An integrate runs after every instruction, so it takes its register and the
/// instructions reading it copy — unless that register is the sink.
#[test]
fn an_integrate_takes_its_register_unless_it_is_the_sink() {
    let dir = tempfile::tempdir().unwrap();
    let registry = vm_registry(dir.path());
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();

    let t0 = p.table("t0", schema);
    let t1 = p.table("t1", schema);
    let t2 = p.table("t2", schema);
    p.push(0, 1, Op::Negate);
    p.push(1, 2, Op::Negate);
    p.integrate(0, t0);
    p.integrate(1, t1);
    p.integrate(2, t2);

    let vm = p.build_in(&registry, vec![schema; 3], 2);
    let last_reads: Vec<LastRead> = vm.program.regs.iter().map(|r| r.last_read).collect();
    assert_eq!(
        last_reads,
        vec![LastRead::Integrate(0), LastRead::Integrate(1), LastRead::Nobody],
        "an integrate reads after every instruction, and the sink is never taken \
         out from under the epoch epilogue",
    );
}

/// A register is folded iff some instruction reads it at net weights — not
/// merely because a cheaper instruction reads it too.
#[test]
fn only_a_register_read_at_net_weights_is_folded() {
    let dir = tempfile::tempdir().unwrap();
    let registry = vm_registry(dir.path());
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();

    let hist = p.table("hist", schema);
    let join_trace = p.table("jt", schema);
    let red_trace = p.table("rt", schema);
    let lin_trace = p.table("lt", schema);

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
    )
    .unwrap();
    assert!(!avi_plan.is_exact_linear());
    let avi_table = p.table("avi", avi_plan.avi.as_ref().unwrap().schema);

    let linear_plan =
        gnitz_store::ops::ReducePlan::from_wire(&schema, &[], &[AggDescriptor::COUNT_STAR], false).unwrap();
    assert!(linear_plan.is_exact_linear());

    let probe = gnitz_store::ops::JoinPlan::from_wire(gnitz_wire::JoinKind::Equi, false, &schema, &schema)
        .unwrap()
        .probe;

    // reg 0 = clamp input, reg 2 = join delta, reg 4 = avi-reduce input,
    // reg 6 = linear-reduce input, each also read by a negate.
    p.push(0, 1, Op::Negate);
    p.push(
        0,
        8,
        Op::WeightClamp {
            hist,
            kind: gnitz_wire::ClampKind::Distinct,
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
            plan: Box::new(BakedReduce::new(avi_plan, Some(avi_table))),
        },
    );
    p.push(6, 7, Op::Negate);
    p.push(
        6,
        11,
        Op::Reduce {
            out_trace: lin_trace,
            plan: Box::new(BakedReduce::new(linear_plan, None)),
        },
    );

    let vm = p.build_in(&registry, vec![schema; 12], 11);
    let folded: Vec<usize> = (0..12).filter(|&r| vm.program.regs[r].fold).collect();
    // The clamp input, the join delta and the value-indexed reduce's input. The
    // linear reduce's input (reg 6) is absent.
    assert_eq!(folded, vec![0, 2, 4]);
}
