use super::*;
use crate::{CmpOp, IntArithOp, LogicalInstr as L, LogicalProgram};

/// The blob a built program encodes to must decode back to the instructions the
/// builder recorded — which is what makes `to_blob_bytes` and the engine's
/// `from_blob` two views of one program rather than two programs.
#[test]
fn the_blob_round_trips_through_the_wire_decoder() {
    let mut b = ExprBuilder::new();
    let c = b.emit(L::LoadConst { val: 1_234_567_890_123 });
    let col = b.emit(L::LoadColInt { col: 0 });
    let cond = b.emit(L::Cmp {
        op: CmpOp::Gt,
        a: col,
        b: c,
    });
    let s_idx = b.add_const_string("längre sträng".to_string());
    let _ = b.emit(L::StrColConst {
        op: CmpOp::Eq,
        col: 1,
        const_idx: s_idx,
    });
    let _ = b.add_const_string(String::new());
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
    let sum = b.emit(L::IntArith {
        op: IntArithOp::Add,
        a,
        b: c,
    });
    assert_eq!(
        b.emit(L::IntArith {
            op: IntArithOp::Add,
            a,
            b: c,
        }),
        sum
    );
    let s1 = b.add_const_string("x".to_string());
    assert_eq!(b.add_const_string("x".to_string()), s1);
    let l1 = b.emit(L::LoadConstStr { const_idx: s1 });
    assert_eq!(b.emit(L::LoadConstStr { const_idx: s1 }), l1);
    let prog = b.build(Some(sum)).expect("a well-formed program");
    assert_eq!(prog.instrs().len(), 4);
    assert_eq!(prog.const_strings().len(), 1);
}
