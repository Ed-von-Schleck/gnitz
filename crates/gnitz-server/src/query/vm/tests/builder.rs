use super::fixtures::*;
use super::*;
use crate::test_support::{make_batch_u128, make_schema_u128_i64, zset_of};
use gnitz_wire::TypeCode;
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::SchemaColumn;

/// An integrate runs after every instruction, so it takes its register from the
/// instructions reading it — unless that register is the plan's output, which
/// the epoch extracts after the integrates.
#[test]
fn an_integrate_takes_its_register_unless_it_is_the_output() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let r0 = p.seed(schema);
    let r1 = p.push(r0, schema, Op::Negate);
    let r2 = p.push(r1, schema, Op::Negate);
    let traces = [r0, r1, r2].map(|r| {
        let t = p.table(&format!("t{}", r.at()), schema);
        p.integrate(r, t);
        t
    });
    let mut vm = p.open(r2);

    let input = [(1, 1, 10), (2, 3, 20)];
    let negated = [(1, -1, 10), (2, -3, 20)];
    assert_rows(&vm.epoch(r0, make_batch_u128(&schema, &input)), &input);
    for (t, rows) in traces.into_iter().zip([&input, &negated, &input]) {
        assert_eq!(vm.trace(t), zset_of(&make_batch_u128(&schema, rows), &schema));
    }
}

/// An instruction takes a register only as its last reader, and never the output:
/// each op below reads `r0` before a later `Negate` does, and must leave it a
/// full batch to read.
#[test]
fn only_the_last_reader_takes_a_register() {
    let schema = make_schema_u128_i64();
    let input = [(1, 1, 10), (2, 3, 20)];
    let negated = [(1, -1, 10), (2, -3, 20)];
    let early_readers: [fn(&SchemaDescriptor, DeltaReg, DeltaReg) -> Op; 4] = [
        // Every row passes, so the filter hands its input on.
        |s, _, _| filter_gt(s, 1, 0),
        |_, _, _| Op::Negate,
        |_, r0, _| Op::Union { in_b: r0 },
        |_, _, empty| Op::Union { in_b: empty },
    ];
    for reader in early_readers {
        let mut p = TestPlan::default();
        let r0 = p.seed(schema);
        let empty = p.seed(schema);
        let op = reader(&schema, r0, empty);
        p.push(r0, schema, op);
        let out = p.push(r0, schema, Op::Negate);
        let mut vm = p.open(out);
        assert_rows(&vm.epoch(r0, make_batch_u128(&schema, &input)), &negated);
    }

    // A union reading the output register, which the epoch extracts after every
    // instruction, must not take it — or the epoch would emit nothing.
    let mut p = TestPlan::default();
    let r0 = p.seed(schema);
    let out = p.push(r0, schema, Op::Negate);
    p.push(out, schema, Op::Union { in_b: r0 });
    let mut vm = p.open(out);
    assert_rows(&vm.epoch(r0, make_batch_u128(&schema, &input)), &negated);
}

/// A register some instruction reads at net weights is folded when written — an
/// instruction's output as much as a seed, and whatever reader follows. Here the
/// projection collapses two rows onto one, which the distinct must see as one
/// row of weight 2.
#[test]
fn a_register_read_at_net_weights_is_folded_when_written() {
    let pk = SchemaColumn::new(TypeCode::U128, false);
    let int = SchemaColumn::new(TypeCode::I64, false);
    let wide = SchemaDescriptor::new(&[pk, int, int], &[0]);
    let map = MapPlan::from_wire(&wide, &gnitz_wire::MapKind::Projection(vec![2])).unwrap();
    let narrow = *map.out_schema();

    let mut p = TestPlan::default();
    let hist = p.table("hist", narrow);
    let r0 = p.seed(wide);
    let projected = p.push(r0, narrow, Op::Map(Box::new(map)));
    let distinct = p.push(
        projected,
        narrow,
        Op::WeightClamp {
            hist,
            kind: gnitz_wire::ClampKind::Distinct,
        },
    );
    // A later reader that reads at any weights.
    p.push(projected, narrow, Op::Negate);
    let mut vm = p.open(distinct);

    let mut b = BatchBuilder::new(&wide);
    for c0 in [10, 20] {
        b.begin_row(1u128, 1);
        b.put_int(c0);
        b.put_int(100);
        b.end_row();
    }
    let mut input = b.finish();
    input.certify_consolidated();

    assert_rows(&vm.epoch(r0, input), &[(1, 1, 100)]);
    assert_eq!(
        vm.trace(hist),
        zset_of(&make_batch_u128(&narrow, &[(1, 2, 100)]), &narrow)
    );
}
