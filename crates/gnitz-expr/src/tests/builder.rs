use super::*;
use crate::{CmpOp, IntArithOp, LogicalInstr as L, LogicalProgram};

/// The blob a built program encodes to must decode back to the instructions the
/// builder recorded — which is what makes `to_blob_bytes` and the engine's
/// `from_blob` two views of one program rather than two programs.
#[test]
fn the_blob_round_trips_through_the_wire_decoder() {
    let mut b = ExprBuilder::new();
    let c = b.emit(L::LoadConst { val: 1_234_567_890_123, unsigned: false });
    let col = b.emit(L::LoadColInt { col: 0 });
    let cond = b.emit(L::Cmp { op: CmpOp::Gt, a: col, b: c });
    let s_idx = b.add_const_string("längre sträng");
    let _ = b.emit(L::StrColConst { op: CmpOp::Eq, col: 1, const_idx: s_idx });
    let _ = b.add_const_string("");
    let sel = b.emit(L::Select { cond, a: col, b: c });
    let prog = b.build(Some(sel)).expect("a well-formed program");

    let decoded = LogicalProgram::from_blob(&prog.to_blob_bytes(), "test").expect("the blob must decode");
    assert_eq!(decoded.instrs(), prog.instrs());
}

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
    let s1 = b.add_const_string("x");
    assert_eq!(b.add_const_string("x"), s1);
    let l1 = b.emit(L::LoadConstStr { const_idx: s1 });
    assert_eq!(b.emit(L::LoadConstStr { const_idx: s1 }), l1);
    let prog = b.build(Some(sum)).expect("a well-formed program");
    assert_eq!(prog.instrs().len(), 4);
    assert_eq!(prog.const_strings().len(), 1);
}

/// A lift of a constant is another constant, folded at emit rather than at
/// resolve: the `IntToFloat` never enters the program, so no schema-resolution
/// pass has to read back what an earlier instruction wrote.
#[test]
fn a_lift_over_a_constant_folds_to_a_float_constant() {
    let mut b = ExprBuilder::new();
    let c = b.emit(L::LoadConst { val: -7, unsigned: false });
    let lifted = b.emit(L::IntToFloat { a: c });
    let prog = b.build(Some(lifted)).expect("a well-formed program");
    assert!(matches!(
        prog.instrs(),
        [L::LoadConst { val: -7, unsigned: false }, L::LoadConst { val, unsigned: false }] if f64::from_bits(*val as u64) == -7.0
    ));

    // A lift over a computed register is untouched.
    let mut b = ExprBuilder::new();
    let col = b.emit(L::LoadColInt { col: 1 });
    let lifted = b.emit(L::IntToFloat { a: col });
    let prog = b.build(Some(lifted)).expect("a well-formed program");
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
    let prog = b.build(Some(narrower)).expect("a well-formed program");
    assert_eq!(prog.instrs().len(), 3);
}

/// An unsigned constant lifts as the `u64` its bits are.
#[test]
fn a_lift_over_an_unsigned_constant_reads_it_unsigned() {
    let mut b = ExprBuilder::new();
    let c = b.emit(L::LoadConst { val: -1, unsigned: true });
    let lifted = b.emit(L::IntToFloat { a: c });
    let prog = b.build(Some(lifted)).expect("a well-formed program");
    assert!(matches!(
        prog.instrs(),
        [_, L::LoadConst { val, unsigned: false }] if f64::from_bits(*val as u64) == 1.8446744073709552e19
    ));
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
        b.build(Some(r)).expect("a well-formed program").instrs().to_vec()
    };
    assert!(matches!(
        to_micros(2)[..],
        [_, L::LoadConst { val: 172_800_000_000, unsigned: false }]
    ));
    assert!(matches!(to_micros(i64::MAX)[..], [_, L::Calendar { .. }]));
}
