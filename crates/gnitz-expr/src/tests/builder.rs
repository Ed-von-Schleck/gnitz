use super::*;
use crate::{CmpOp, LogicalInstr as L, LogicalProgram, StrOp};

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
        op: StrOp::Eq,
        col: 1,
        const_idx: s_idx,
    });
    let _ = b.add_const_string(String::new());
    let sel = b.emit(L::Select { cond, a: col, b: c });
    let prog = b.build(Some(sel)).expect("a well-formed program");

    let decoded = LogicalProgram::from_blob(&prog.to_blob_bytes(), "test").expect("the blob must decode");
    assert_eq!(decoded.instrs(), prog.instrs());
}
