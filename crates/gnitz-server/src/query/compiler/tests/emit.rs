use super::*;
use crate::query::compiler::fixtures::*;
use crate::test_support::make_schema_u64_i64;
use gnitz_store::relation::StateLayout;
use gnitz_wire::{Circuit, JoinKind};

/// Exchange-free `build` emitted whole, as one worker's plan over sources 10 and 11.
fn plan(build: Build) -> Result<SubPlan, String> {
    let mut c = Circuit::default();
    build(&mut c);
    let loaded = loaded(c);
    let schema = make_schema_u64_i64();
    build_plan(
        &loaded,
        &loaded.ordered_where(|_| true),
        &sources([(10, schema), (11, schema)]),
        &mut StateLayout::default(),
        true,
        &[],
        PlanOut::Node(loaded.sink()?),
    )
    .map(|built| built.plan)
}

const TAKES_A_DELTA: &str = "operand port takes a delta, not an integral";

/// The load holds a node to its arity, not to its producers' kinds: an integral
/// names a store and a delta a batch, so every port refuses the other.
#[test]
fn a_port_refuses_the_other_kind_of_operand() {
    let cases: [(&str, Build, &str); 6] = [
        (
            "a unary delta operator over an integral",
            |c: &mut Circuit| {
                let t = integral(c, 10);
                let n = c.negate(t);
                c.sink(n);
            },
            TAKES_A_DELTA,
        ),
        (
            "a union's left operand",
            |c: &mut Circuit| {
                let (t, b) = (integral(c, 10), scan(c, 11));
                let u = c.union(t, b);
                c.sink(u);
            },
            TAKES_A_DELTA,
        ),
        (
            "a union's right operand is a delta port, not a trace one",
            |c: &mut Circuit| {
                let (a, t) = (scan(c, 10), integral(c, 11));
                let u = c.union(a, t);
                c.sink(u);
            },
            TAKES_A_DELTA,
        ),
        (
            "a join's delta port",
            |c: &mut Circuit| {
                let (ta, tb) = (integral(c, 10), integral(c, 11));
                let j = c.join(ta, tb, JoinKind::Equi, false);
                c.sink(j);
            },
            TAKES_A_DELTA,
        ),
        (
            "the sink",
            |c: &mut Circuit| {
                let t = integral(c, 10);
                c.sink(t);
            },
            TAKES_A_DELTA,
        ),
        (
            "a join's trace port",
            |c: &mut Circuit| {
                let (a, b) = (scan(c, 10), scan(c, 11));
                let j = c.join(a, b, JoinKind::Equi, false);
                c.sink(j);
            },
            "operand port takes an integral, not a delta",
        ),
    ];
    for (why, build, guard) in cases {
        assert_eq!(rejection(plan(build)), guard, "{why}");
    }
    plan(|c| {
        let (a, tb) = (scan(c, 10), integral(c, 11));
        let j = c.join(a, tb, JoinKind::Equi, false);
        c.sink(j);
    })
    .expect("a delta probing an integral");
}

fn integral(c: &mut Circuit, source: u64) -> NodeId {
    let s = scan(c, source);
    c.integrate_trace(s)
}

/// The driver seeds one register per source, so a second scan of one would
/// silently see nothing.
#[test]
fn a_plan_scanning_one_source_twice_is_rejected() {
    let twice = plan(|c| {
        let (a, b) = (scan(c, 10), scan(c, 10));
        let u = c.union(a, b);
        c.sink(u);
    });
    assert_eq!(rejection(twice), "scan-delta: a plan scans one source twice");
}

/// A `Filter` program that does not decode aborts the compile: passing every row
/// instead would turn the WHERE into WHERE TRUE.
#[test]
fn a_corrupt_filter_program_aborts_the_compile() {
    let garbled = plan(|c| {
        let a = scan(c, 10);
        let f = c.filter(a, vec![0xFF; 16]);
        c.sink(f);
    });
    let empty = plan(|c| {
        let a = scan(c, 10);
        let f = c.filter(a, Vec::new());
        c.sink(f);
    });
    for built in [garbled, empty] {
        assert!(rejection(built).starts_with("filter: invalid predicate program"));
    }
}
