use super::*;
use crate::{IntArithOp, LogicalInstr as L};

/// Two identical instructions hold one value, so the second `emit` answers the
/// first's register, and two equal literals are one pool entry — which is what
/// keeps a desugar that names an operand twice within the register budget.
#[test]
fn the_builder_folds_identical_instructions_and_pool_entries() {
    let mut b = ExprBuilder::new();
    let a = b.emit(L::LoadColInt { col: 1 });
    assert_eq!(b.emit(L::LoadColInt { col: 1 }), a);
    let c = b.emit(L::LoadColInt { col: 2 });
    assert_ne!(c, a);
    let sum = b.emit(L::IntArith { op: IntArithOp::Add, a, b: c });
    assert_eq!(b.emit(L::IntArith { op: IntArithOp::Add, a, b: c }), sum);
    let s1 = b.add_const_bytes(b"x");
    assert_eq!(b.add_const_bytes(b"x"), s1);
    let l1 = b.emit(L::LoadConstStr { const_idx: s1 });
    assert_eq!(b.emit(L::LoadConstStr { const_idx: s1 }), l1);
    let prog = b.build(vec![Sink::Reg(sum)]).expect("a well-formed program");
    assert_eq!(prog.instrs().len(), 4);
    assert_eq!(prog.const_strings().len(), 1);
}

/// A lift of a constant is another constant, folded at emit rather than at
/// resolve: the `IntToFloat` never enters the program, so no schema-resolution
/// pass has to read back what an earlier instruction wrote. An unsigned
/// constant lifts as the `u64` its bits are.
#[test]
fn a_lift_over_a_constant_folds_to_a_float_constant() {
    for (val, unsigned, want) in [(-7, false, -7.0), (-1, true, u64::MAX as f64)] {
        let mut b = ExprBuilder::new();
        let c = b.emit(L::LoadConst { val, unsigned });
        let lifted = b.emit(L::IntToFloat { a: c });
        let prog = b.build(vec![Sink::Reg(lifted)]).expect("a well-formed program");
        assert!(
            matches!(prog.instrs(), [_, L::LoadConst { val, unsigned: false }] if f64::from_bits(*val as u64) == want),
            "{val} unsigned={unsigned}"
        );
    }

    // A lift over a computed register is untouched.
    let mut b = ExprBuilder::new();
    let col = b.emit(L::LoadColInt { col: 1 });
    let lifted = b.emit(L::IntToFloat { a: col });
    let prog = b.build(vec![Sink::Reg(lifted)]).expect("a well-formed program");
    assert!(matches!(prog.instrs(), [L::LoadColInt { .. }, L::IntToFloat { .. }]));
}

/// A range check over a value already range-checked into the same type is that
/// value, so a narrowing cast feeding a slot of its own width costs one check;
/// over a different target it is a real narrowing and stays.
#[test]
fn a_range_check_over_the_same_range_check_folds() {
    use gnitz_wire::FixedInt;
    let mut b = ExprBuilder::new();
    let col = b.emit(L::LoadColInt { col: 1 });
    let narrow = b.emit(L::IntCast { a: col, fi: FixedInt::I16 });
    assert_eq!(b.emit(L::IntCast { a: narrow, fi: FixedInt::I16 }), narrow);
    let narrower = b.emit(L::IntCast { a: narrow, fi: FixedInt::I8 });
    assert_ne!(narrower, narrow);
    let prog = b.build(vec![Sink::Reg(narrower)]).expect("a well-formed program");
    assert_eq!(prog.instrs().len(), 3);
}

/// A day count widened to microseconds folds to its product, and an overflowing
/// one stays the kernel that makes it NULL.
#[test]
fn a_to_micros_over_a_constant_folds_unless_it_overflows() {
    let to_micros = |days: i64| {
        let mut b = ExprBuilder::new();
        let c = b.emit(L::LoadConst { val: days, unsigned: false });
        let r = b.emit(L::Calendar {
            op: crate::CalendarOp::ToMicros,
            a: c,
            micros: false,
        });
        b.build(vec![Sink::Reg(r)])
            .expect("a well-formed program")
            .instrs()
            .to_vec()
    };
    assert!(matches!(
        to_micros(2)[..],
        [_, L::LoadConst { val: 172_800_000_000, unsigned: false }]
    ));
    assert!(matches!(to_micros(i64::MAX)[..], [_, L::Calendar { .. }]));
}
