use gnitz_wire::{FixedInt, TypeCode};

use crate::test_support::{result_prog, schema_pk};
use crate::{
    CmpOp, ExprValidateErr, IntUnaryOp, LogicalInstr, LogicalProgram, Reg, SchemaColumn, SchemaDescriptor, Sink,
};

/// A map's sinks against its output slots. A copy byte-copies at equal width
/// and otherwise widens a narrower integer: no narrowing, no representation
/// change — exactly the cross-width set-op coercion the client emits. A register
/// sink's slot holds the register's class and whole value, fewer bytes only
/// where the producer range-checked the value into the slot's own type — a
/// same-width check of another type is refused; a BOOLEAN slot stores any scalar
/// register's truth. The sinks cover exactly the output's
/// payload slots.
#[test]
fn each_map_sink_is_held_to_its_output_slot() {
    use LogicalInstr as L;
    use TypeCode as T;
    let copy_err = |src_tc, out_tc| Err(ExprValidateErr::CopyTypeMismatch { col: 1, src_tc, out: 0, out_tc });
    let width = |type_code| Err(ExprValidateErr::EmitSlotWidth { out: 0, type_code });
    let class = |type_code| Err(ExprValidateErr::EmitClassMismatch { out: 0, type_code });
    // Register 1 of each, over `[I64, STRING]`.
    let int = || vec![L::LoadCol { col: 1 }, L::IntUnary { op: IntUnaryOp::Neg, a: Reg(0) }];
    let to_i16 = || vec![L::LoadCol { col: 1 }, L::IntCast { a: Reg(0), fi: FixedInt::I16 }];
    let to_u16 = || vec![L::LoadCol { col: 1 }, L::IntCast { a: Reg(0), fi: FixedInt::U16 }];
    let to_u8 = || vec![L::LoadCol { col: 1 }, L::IntCast { a: Reg(0), fi: FixedInt::U8 }];
    let text = || vec![L::LoadColStr { col: 2 }, L::StrReverse { a: Reg(0) }];
    let truth = || vec![L::LoadCol { col: 1 }, L::Cmp { op: CmpOp::Eq, a: Reg(0), b: Reg(0) }];
    let reg1 = |instrs| result_prog(instrs, vec![]);
    let sinks = |sinks| LogicalProgram::new(vec![], sinks, vec![]).unwrap();
    let reg_in = vec![T::I64, T::String];
    type Case = (
        Vec<TypeCode>,
        LogicalProgram,
        Vec<TypeCode>,
        Result<(), ExprValidateErr>,
    );
    let mut cases: Vec<Case> = vec![
        // A PK source into a payload slot of its own type is a copy.
        (vec![T::I64], LogicalProgram::copy_cols(&[0]), vec![T::U64], Ok(())),
        (
            vec![T::I64, T::I64],
            sinks(vec![Sink::Col(1), Sink::Col(2)]),
            vec![T::I64, T::I64],
            Ok(()),
        ),
        (
            vec![T::I64, T::I64],
            sinks(vec![Sink::Col(1)]),
            vec![T::I64, T::I64],
            Err(ExprValidateErr::OutputSlotCountMismatch { sinks: 1, num_payload_cols: 2 }),
        ),
    ];
    for (src, dst, want) in [
        (T::I64, T::I64, Ok(())),
        (T::String, T::String, Ok(())),
        (T::Blob, T::Blob, Ok(())),
        (T::U128, T::U128, Ok(())),
        (T::F64, T::F64, Ok(())),
        (T::U32, T::U64, Ok(())),
        (T::U32, T::I64, Ok(())),
        (T::U8, T::I16, Ok(())),
        (T::String, T::U64, copy_err(T::String, T::U64)),
        (T::U64, T::String, copy_err(T::U64, T::String)),
        (T::U128, T::U64, copy_err(T::U128, T::U64)),
        (T::U64, T::F64, copy_err(T::U64, T::F64)),
        (T::F32, T::F64, copy_err(T::F32, T::F64)),
        (T::U64, T::I64, copy_err(T::U64, T::I64)),
    ] {
        cases.push((vec![src], LogicalProgram::copy_cols(&[1]), vec![dst], want));
    }
    for (tc, instrs, want) in [
        (T::I64, int(), Ok(())),
        (T::U64, int(), Ok(())),
        (T::F64, int(), Ok(())),
        (T::I16, to_i16(), Ok(())),
        (T::I16, int(), width(T::I16)),
        (T::I16, to_u16(), width(T::I16)),
        (T::Bool, truth(), Ok(())),
        (T::Bool, int(), Ok(())),
        (T::Bool, text(), class(T::Bool)),
        (T::U8, to_u8(), Ok(())),
        (T::U8, truth(), width(T::U8)),
        (T::I32, to_i16(), width(T::I32)),
        (T::U128, int(), width(T::U128)),
        (T::F32, int(), width(T::F32)),
        (T::String, int(), class(T::String)),
        (T::Blob, int(), class(T::Blob)),
        (T::String, text(), Ok(())),
        (T::I64, text(), class(T::I64)),
    ] {
        cases.push((reg_in.clone(), reg1(instrs), vec![tc], want));
    }
    for (input, prog, output, want) in cases {
        let label = format!("{:?} into {output:?}: {:?}", prog.instrs(), prog.sinks());
        let got = prog.resolve_map(&schema_pk(&input, true), &schema_pk(&output, true));
        assert_eq!(got.map(drop), want, "{label}");
    }
}

/// A map reproduces its input only when it copies every payload column into its
/// own slot, computes no output slot, and the two schemas locate every column
/// alike. Every case resolves, so each of the three conditions decides one.
#[test]
fn is_identity_requires_an_in_order_copy_over_one_layout() {
    let ident = |p: LogicalProgram, a: &SchemaDescriptor, b: &SchemaDescriptor| {
        p.resolve_map(a, b).expect("the map resolves").is_identity()
    };
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    assert!(ident(LogicalProgram::copy_cols(&[1, 2]), &schema, &schema));
    assert!(
        !ident(LogicalProgram::copy_cols(&[2, 1]), &schema, &schema),
        "reordered copy"
    );
    let computed = LogicalProgram::new(
        vec![LogicalInstr::LoadCol { col: 2 }],
        vec![Sink::Col(1), Sink::Reg(Reg(0))],
        vec![],
    )
    .unwrap();
    assert!(!ident(computed, &schema, &schema), "computed sink");
    // An instruction no sink reads is never run.
    let dead = LogicalProgram::new(
        vec![LogicalInstr::LoadCol { col: 2 }],
        vec![Sink::Col(1), Sink::Col(2)],
        vec![],
    )
    .unwrap();
    assert!(ident(dead, &schema, &schema), "dead instruction");
    // A copy widened into a wider payload type.
    let narrow_in = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I32, false),
        ],
        &[0],
    );
    assert!(
        !ident(LogicalProgram::copy_cols(&[1, 2]), &narrow_in, &schema),
        "different payload type"
    );
    // Slot for slot the same copy, but the PK sits at another column.
    let ints = |pk: u32| {
        let cols = [0, 1, 2].map(|ci| SchemaColumn::new(TypeCode::I64, ci != pk));
        SchemaDescriptor::new(&cols, &[pk])
    };
    assert!(
        !ident(LogicalProgram::copy_cols(&[1, 2]), &ints(0), &ints(1)),
        "different PK layout"
    );
    // A PK that does not lead: the payload columns are 0 and 2.
    let mid_pk = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[1],
    );
    assert!(ident(LogicalProgram::copy_cols(&[0, 2]), &mid_pk, &mid_pk));
    assert!(
        !ident(LogicalProgram::copy_cols(&[1, 2]), &mid_pk, &mid_pk),
        "a PK column copied into a payload slot"
    );
    // A PK-only schema: nothing to copy, so the empty program is its identity.
    let pk_only = SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false)], &[0]);
    assert!(ident(LogicalProgram::copy_cols(&[]), &pk_only, &pk_only));
}
