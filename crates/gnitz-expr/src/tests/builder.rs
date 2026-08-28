use super::*;
use crate::{CmpOp, LogicalProgram};

/// The builder's own output must decode back to the instructions it recorded,
/// which is what makes [`ExprBuilder::build`] and
/// [`ExprBuilder::build_logical`] two views of one program rather than two
/// programs.
#[test]
fn build_and_build_logical_describe_the_same_program() {
    // Built twice rather than cloned: the builder is consumed by both exits,
    // and a `Clone` derive kept only for a test would be a use nothing in
    // production has.
    let build = || {
        let mut b = ExprBuilder::new();
        let c = b.load_const(1_234_567_890_123);
        let col = b.load_col_int(0);
        let cond = b.cmp(CmpOp::Gt, col, c);
        let sel = b.select(cond, col, c);
        (b, sel)
    };
    let (b, sel) = build();
    let wire = b.build(sel);
    let (b, sel) = build();
    let logical = b.build_logical(sel).expect("a well-formed program");

    let decoded = LogicalProgram::from_wire(&wire.code, wire.num_regs, wire.result_reg, wire.const_strings)
        .expect("the builder must only emit decodable programs");
    assert_eq!(decoded.instrs(), logical.instrs());
}

#[test]
fn encode_round_trips_through_wire_decoder() {
    let mut b = ExprBuilder::new();
    let c = b.load_const(1_234_567_890_123);
    let col = b.load_col_int(0);
    let cond = b.cmp(CmpOp::Gt, col, c);
    let s_idx = b.add_const_string("längre sträng".to_string());
    let _ = b.str_col_const(StrOp::Eq, 1, s_idx);
    let _ = b.add_const_string(String::new());
    let sel = b.select(cond, col, c);
    let prog = b.build(sel);

    let blob = prog.encode();
    let dec = gnitz_wire::decode_expr_blob(&blob).unwrap();
    assert_eq!(dec.num_regs, prog.num_regs);
    assert_eq!(dec.result_reg, prog.result_reg);
    assert_eq!(dec.code, prog.code);
    // Both sides are now `Vec<Vec<u8>>` — compare the byte-transparent pool directly.
    assert_eq!(dec.const_strings, prog.const_strings);
}
