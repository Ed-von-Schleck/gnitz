// 3.14 is a deliberate float-bit-pattern test fixture, not an approximation
// of PI meant to be replaced with std::f64::consts::PI.
#![allow(clippy::approx_constant)]

use gnitz_wire::{FixedInt, TypeCode};

// `tests/program.rs` is `#[path]`-attached to `program.rs`, so `super` is that
// module — one import line rather than three spellings of it.
use super::{
    ColKind, ExprOp, FloatUnaryOp, IntOrder, IntUnaryOp, ProgramFacts, ReadAs, INSTR_WORDS, MAX_CONST_POOL, SINK_WORDS,
};
use crate::batch::{decode_f64, encode_f64};
use crate::eval::Resolved;
use crate::test_support::{
    filter_prog, is_not_null_op, is_null_op, make_int_view, make_string_view, map_prog, row_values, scalar_prog,
    schema_pk_ints, schema_pk_strings, TestOut, TestSchema, TestView,
};
use crate::{
    CmpOp, ColumnLocator, ConstIdx, ExprValidateErr, FloatArithOp, Instr, IntArithOp, LogicalInstr, LogicalProgram,
    MapEval, NullPerm, Reg, ScalarEval, Sink,
};

/// The phrase a `ColKindMismatch` renders for `kind`, read from its one source
/// rather than re-spelled here — so widening a kind's predicate without its
/// wording fails these tests instead of passing them.
fn type_phrase(kind: ColKind) -> &'static str {
    kind.type_test().expect("a kind whose rejection has a sentence").1
}

/// `ev`'s scalar emits for `(mb, row)`, as `(null mask over their slots, values)`.
fn eval_with_emit(ev: &mut MapEval, mb: &TestView, row: usize) -> (u64, Vec<i64>) {
    let sinks = ev.sinks();
    let emits: Vec<usize> = sinks.scalar_emits.iter().map(|e| e.slot).collect();
    let slots = sinks.copies.len() + emits.len() + sinks.str_emits.len();
    let mut out = TestOut::new(1, &vec![16; slots]);
    ev.write_computed(mb, row, 1, &mut out, 0);
    let emit_mask = emits.iter().fold(0u64, |m, &slot| m | 1u64 << slot);
    let emit_vals = emits
        .iter()
        .map(|&slot| i64::from_le_bytes(out.cols[slot][..8].try_into().unwrap()))
        .collect();
    (gnitz_wire::read_u64_le(&out.nulls, 0) & emit_mask, emit_vals)
}

#[test]
fn int_add_and_negate() {
    let schema = schema_pk_ints(2, true);
    let mb = make_int_view(&schema, &[(1, 0, &[10, 3])]);

    // ADD: 10 + 3 = 13
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadColInt { col: 2 },
        LogicalInstr::IntArith {
            op: IntArithOp::Add,
            a: Reg(0),
            b: Reg(1),
        },
    ];
    let mut prog = scalar_prog(&schema, instrs, Reg(2), vec![]);
    let val = row_values(&mut prog, &mb)[0].expect("not NULL") as i64;
    assert_eq!(val, 13);

    // NEG: -10
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::IntUnary { op: IntUnaryOp::Neg, a: Reg(0) },
    ];
    let mut prog = scalar_prog(&schema, instrs, Reg(1), vec![]);
    let val = row_values(&mut prog, &mb)[0].expect("not NULL") as i64;
    assert_eq!(val, -10);
}

#[test]
fn float_add() {
    let schema = TestSchema::with_pk_at(0, &[TypeCode::U64, TypeCode::F64, TypeCode::F64]);
    // Store floats as i64 bits
    let a_bits = encode_f64(3.14);
    let b_bits = encode_f64(2.0);
    let mb = make_int_view(&schema, &[(1, 0, &[a_bits, b_bits])]);

    // FLOAT_ADD: 3.14 + 2.0
    let instrs = vec![
        LogicalInstr::LoadColFloat { col: 1 },
        LogicalInstr::LoadColFloat { col: 2 },
        LogicalInstr::FloatArith {
            op: FloatArithOp::Add,
            a: Reg(0),
            b: Reg(1),
        },
    ];
    let mut prog = scalar_prog(&schema, instrs, Reg(2), vec![]);
    let val = row_values(&mut prog, &mb)[0].expect("not NULL") as i64;
    let result = decode_f64(val);
    assert!((result - 5.14).abs() < 1e-10);
}

#[test]
fn load_const_carries_a_full_width_i64() {
    let schema = schema_pk_ints(1, true);
    let mb = make_int_view(&schema, &[(1, 0, &[0])]);

    // Test large constant: 0x00000001_00000002 = (1 << 32) | 2 = 4294967298
    // Wire form: lo 32 bits = 2, hi 32 bits = 1.
    let instrs = vec![LogicalInstr::LoadConst {
        val: ((1i64) << 32) | (2i64 & 0xFFFF_FFFF),
        unsigned: false,
    }];
    let mut prog = scalar_prog(&schema, instrs, Reg(0), vec![]);
    let val = row_values(&mut prog, &mb)[0].expect("not NULL") as i64;
    assert_eq!(val, (1i64 << 32) | 2);

    // Test negative constant: -1 (the wire low/high split is reconstructed by
    // `from_blob`; the typed instruction carries the full i64 value directly).
    let instrs = vec![LogicalInstr::LoadConst { val: -1, unsigned: false }];
    let mut prog = scalar_prog(&schema, instrs, Reg(0), vec![]);
    let val = row_values(&mut prog, &mb)[0].expect("not NULL") as i64;
    assert_eq!(val, -1);
}

#[test]
fn int_to_float_widens_the_register() {
    let schema = schema_pk_ints(1, true);
    let mb = make_int_view(&schema, &[(1, 0, &[42])]);

    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::IntToFloat { a: Reg(0) },
    ];
    let mut prog = scalar_prog(&schema, instrs, Reg(1), vec![]);
    let val = row_values(&mut prog, &mb)[0].expect("not NULL") as i64;
    assert_eq!(decode_f64(val), 42.0);
}

/// A `U64` column tracks its register as unsigned, which must select the
/// unsigned form of every opcode that has one. Asserted on the *value*, not on
/// the resolved instruction: at `u64::MAX` each unsigned result differs from its
/// signed twin, so the value alone pins the arm — and it does not couple the
/// test to the position an instruction lands at after resolution.
///
/// The comparison opcodes are swept separately, over both signednesses and every
/// operator, by `int_compare_agrees_with_the_rust_operator_at_both_signednesses`.
#[test]
fn a_u64_column_selects_the_unsigned_arm_of_div_mod_and_the_float_cast() {
    let schema = TestSchema::with_pk_at(0, &[TypeCode::U64, TypeCode::U64]);
    // u64::MAX is -1 as i64, so every signed reading below is a different number.
    let mb = make_int_view(&schema, &[(1, 0, &[u64::MAX as i64])]);
    let run = |instrs: Vec<LogicalInstr>, result: u16| {
        row_values(&mut scalar_prog(&schema, instrs, Reg(result), vec![]), &mb)[0].expect("not NULL") as i64
    };
    let load = LogicalInstr::LoadColInt { col: 1 };
    let two = LogicalInstr::LoadConst { val: 2, unsigned: false };

    assert_eq!(
        run(
            vec![
                load,
                two,
                LogicalInstr::IntArith {
                    op: IntArithOp::Div,
                    a: Reg(0),
                    b: Reg(1)
                }
            ],
            2
        ),
        i64::MAX,
        "u64::MAX / 2 unsigned; signed -1/2 would be 0",
    );
    assert_eq!(
        run(
            vec![
                load,
                two,
                LogicalInstr::IntArith {
                    op: IntArithOp::Mod,
                    a: Reg(0),
                    b: Reg(1)
                }
            ],
            2
        ),
        1,
        "u64::MAX % 2 unsigned; signed -1 % 2 would be -1",
    );
    assert_eq!(
        decode_f64(run(vec![load, LogicalInstr::IntToFloat { a: Reg(0) }], 1)),
        u64::MAX as f64,
        "unsigned cast is ~1.8e19; signed would be -1.0",
    );
}

#[test]
fn emit_writes_each_named_output_slot() {
    let schema = schema_pk_ints(2, true);
    // One payload slot out, which the one sink covers.
    let out = schema_pk_ints(1, true);
    let mb = make_int_view(&schema, &[(1, 0, &[10, 20])]);

    // Compute col1 + col2, store into payload col 0
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadColInt { col: 2 },
        LogicalInstr::IntArith {
            op: IntArithOp::Add,
            a: Reg(0),
            b: Reg(1),
        },
    ];
    // Sink 0 stores r2 into payload col 0.
    let mut prog = map_prog(&schema, &out, instrs, vec![Sink::Reg(Reg(2))], vec![]);

    let (mask, emit_vals) = eval_with_emit(&mut prog, &mb, 0);
    assert_eq!(mask, 0);
    assert_eq!(emit_vals[0], 30); // 10 + 20
}

/// `r5 = (col1 > 1) AND (col2 > 1)` over `schema_pk_ints(2, _)`, using registers
/// 0-5. The classification tests below and the sink test share it.
fn conjunction_over_two_cols() -> Vec<LogicalInstr> {
    vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadConst { val: 1, unsigned: false },
        LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
        LogicalInstr::LoadColInt { col: 2 },
        LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(3), b: Reg(1) },
        LogicalInstr::BoolBinary { is_or: false, a: Reg(2), b: Reg(4) },
    ]
}

/// A sink counts as a non-bool read of its register, so a boolean it ships is not
/// `bit_only` and its producer writes `regs`. Were the source missing from the
/// operand table, `BoolBinary` would skip the unpack and the sink would read a stale
/// lane.
#[test]
fn an_emitted_boolean_lands_in_regs() {
    // Nullable payload columns: under `no_nulls`, `BoolBinary` writes `regs`
    // whatever `bit_only` says, which would make the value assertion vacuous.
    let schema = schema_pk_ints(2, true);
    let mb = make_int_view(&schema, &[(1, 0, &[10, 20])]);
    let instrs = conjunction_over_two_cols();
    let mut prog = map_prog(
        &schema,
        &schema_pk_ints(1, true),
        instrs,
        vec![Sink::Reg(Reg(5))],
        vec![],
    );
    assert!(
        !prog.prog().no_nulls,
        "nullable columns keep the test off the no_nulls arm"
    );
    assert!(!prog.prog().is_bit_only(5), "a register a sink stores is not bit_only");
    let (_, emit_vals) = eval_with_emit(&mut prog, &mb, 0);
    assert_eq!(emit_vals[0], 1, "the sink must ship the AND value, not a stale lane");
}

/// A NULL result reaching a sink sets the output slot's null bit and stores 0 —
/// the one place a NULL row's value half is defined, since the output column
/// must hold something.
#[test]
fn emit_of_a_null_result_sets_the_slot_bit_and_stores_zero() {
    let schema = schema_pk_ints(2, true);
    let mb = make_int_view(&schema, &[(1, 0, &[10, 3])]);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadConst { val: 0, unsigned: false },
        LogicalInstr::IntArith {
            op: IntArithOp::Div,
            a: Reg(0),
            b: Reg(1),
        },
    ];
    let mut prog = map_prog(
        &schema,
        &schema_pk_ints(1, true),
        instrs,
        vec![Sink::Reg(Reg(2))],
        vec![],
    );
    let (mask, emit_vals) = eval_with_emit(&mut prog, &mb, 0);
    assert_eq!((mask & 1, emit_vals[0]), (1, 0));
}

/// A register-free program is exactly what `copy_cols` builds — legitimate as a
/// map, meaningless as a scalar, whose two readers both read a register back.
/// Both directions, because rejecting it in *either* role would be wrong: the
/// map is the shape that must keep working, the scalar the one that must not
/// silently answer NULL.
#[test]
fn a_register_free_program_is_rejected() {
    let schema = schema_pk_ints(1, true);
    // One column copy: input col 1 → payload slot 0 (source type derived in resolve)
    let prog = || LogicalProgram::new(Vec::new(), vec![Sink::Col(1)], vec![]);
    // A scalar reads its result out of a register, so a program writing output
    // slots is not one — whether it writes one slot or none.
    assert_eq!(
        prog().resolve_scalar(&schema).err(),
        Some(ExprValidateErr::OutputRoleMismatch),
    );
    assert_eq!(
        LogicalProgram::new(Vec::new(), Vec::new(), vec![])
            .resolve_scalar(&schema)
            .err(),
        Some(ExprValidateErr::OutputRoleMismatch),
    );
    assert!(prog().resolve_map(&schema, &schema).is_ok());
}

/// A logical column index resolves either to the PK read or to a *dense payload
/// slot*, renumbered around wherever the PK sits — so the slot is neither `ci`
/// nor `ci - 1` in general. Both PK positions are swept, because a leading PK is
/// the one arrangement where the two closed forms happen to agree.
#[test]
fn a_column_index_resolves_to_the_pk_or_to_its_dense_payload_slot() {
    // (PK column index, column types, PK value, payload values, and per logical
    // column its expected payload slot — `None` for the PK — and the value read)
    type Case = (
        usize,
        &'static [TypeCode],
        u64,
        &'static [i64],
        &'static [(Option<u8>, i64)],
    );
    let cases: [Case; 2] = [
        (
            0,
            &[TypeCode::U64, TypeCode::I64, TypeCode::I64],
            42,
            &[10, 20],
            &[(None, 42), (Some(0), 10), (Some(1), 20)],
        ),
        (
            1,
            &[TypeCode::I64, TypeCode::U64, TypeCode::I64],
            99,
            &[5, 7],
            &[(Some(0), 5), (None, 99), (Some(1), 7)],
        ),
    ];

    for (pk_index, cols, pk_val, payloads, want) in cases {
        let schema = TestSchema::with_pk_at(pk_index, cols);
        let mb = make_int_view(&schema, &[(pk_val, 0, payloads)]);
        for (ci, &(want_slot, want_val)) in want.iter().enumerate() {
            let instrs = vec![LogicalInstr::LoadColInt { col: ci as u32 }];
            let mut prog = scalar_prog(&schema, instrs, Reg(0), vec![]);
            let got_slot = match prog.prog().instrs[0].1 {
                Instr::LoadPk { .. } => None,
                Instr::LoadPayloadInt { pi, .. } => Some(pi),
                ref other => panic!("pk_index={pk_index} col {ci} resolved to {other:?}"),
            };
            assert_eq!(got_slot, want_slot, "pk_index={pk_index} col {ci}: wrong slot");
            assert_eq!(
                row_values(&mut prog, &mb)[0].map(|v| v as i64),
                Some(want_val),
                "pk_index={pk_index} col {ci}: wrong value"
            );
        }
    }
}

#[test]
fn str_col_const_is_classified_never_null() {
    // STR_COL_*_CONST and STR_COL_*_COL must flip no_nulls off when any
    // operand column is nullable; without that, the batch path skips null-bit
    // tracking and null rows leak through string predicates as definite results.
    let nullable_schema = schema_pk_strings(2, true);
    let nonnull_schema = schema_pk_strings(2, false);

    // STR_COL_*_CONST on col1
    for (op, _name) in &[
        (CmpOp::Eq, "EQ_CONST"),
        (CmpOp::Ne, "NE_CONST"),
        (CmpOp::Gt, "GT_CONST"),
        (CmpOp::Ge, "GE_CONST"),
        (CmpOp::Lt, "LT_CONST"),
        (CmpOp::Le, "LE_CONST"),
    ] {
        let instrs = vec![LogicalInstr::StrColConst { op: *op, col: 1, const_idx: ConstIdx(0) }];
        let prog = scalar_prog(&nullable_schema, instrs.clone(), Reg(0), vec![b"x".to_vec()]);
        assert!(
            !prog.prog().no_nulls,
            "{_name}: nullable col1 must yield no_nulls=false"
        );
        let prog = scalar_prog(&nonnull_schema, instrs, Reg(0), vec![b"x".to_vec()]);
        assert!(
            prog.prog().no_nulls,
            "{_name}: non-nullable col1 must yield no_nulls=true"
        );
    }

    // STR_COL_*_COL — both operands matter
    for (op, _name) in &[
        (CmpOp::Eq, "EQ_COL"),
        (CmpOp::Ne, "NE_COL"),
        (CmpOp::Gt, "GT_COL"),
        (CmpOp::Ge, "GE_COL"),
        (CmpOp::Lt, "LT_COL"),
        (CmpOp::Le, "LE_COL"),
    ] {
        let instrs = vec![LogicalInstr::StrColCol { op: *op, col_a: 1, col_b: 2 }];
        let prog = scalar_prog(&nullable_schema, instrs.clone(), Reg(0), vec![]);
        assert!(
            !prog.prog().no_nulls,
            "{_name}: nullable operands must yield no_nulls=false"
        );
        let prog = scalar_prog(&nonnull_schema, instrs, Reg(0), vec![]);
        assert!(
            prog.prog().no_nulls,
            "{_name}: non-nullable operands must yield no_nulls=true"
        );
    }
}

// ---------------------------------------------------------------------------
// Select / LoadNull (SQL CASE blend)
// ---------------------------------------------------------------------------

/// `CASE WHEN cond THEN 42 END` (implicit ELSE NULL) lowers to
/// `select(cond, 42, load_null())`: truthy → 42, else → NULL.
#[test]
fn load_null_else_branch_eval() {
    let schema = schema_pk_ints(1, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },                  // cond
        LogicalInstr::LoadConst { val: 42, unsigned: false }, // a
        LogicalInstr::LoadNull,                               // else NULL
        LogicalInstr::Select { cond: Reg(0), a: Reg(1), b: Reg(2) },
    ];
    let mut prog = scalar_prog(&schema, instrs, Reg(3), vec![]);

    // cond truthy → 42.
    let batch = make_int_view(&schema, &[(1, 0, &[5])]);
    let v = row_values(&mut prog, &batch)[0].expect("not NULL") as i64;
    assert_eq!(v, 42);

    // cond false → else NULL.
    let batch = make_int_view(&schema, &[(1, 0, &[0])]);
    let n = row_values(&mut prog, &batch)[0].is_none();
    assert!(n, "false cond → else NULL");

    // cond NULL → else NULL.
    let batch = make_int_view(&schema, &[(1, 1, &[9])]);
    let n = row_values(&mut prog, &batch)[0].is_none();
    assert!(n, "NULL cond → else NULL");
}

/// LoadNull forces the nullable eval path even when every column is NOT NULL:
/// `analyze` must clear `no_nulls` whenever a program manufactures
/// a NULL.
#[test]
fn load_null_forces_the_nullable_path() {
    let nonnull_schema = schema_pk_ints(1, false);
    // CASE WHEN col1 THEN col1 END: r0=col1, r1=load_null, r2=select(r0, r0, r1).
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadNull,
        LogicalInstr::Select { cond: Reg(0), a: Reg(0), b: Reg(1) },
    ];
    let prog = scalar_prog(&nonnull_schema, instrs, Reg(2), vec![]);
    assert!(!prog.prog().no_nulls, "LoadNull must force the nullable path");
}

/// A Select over only NOT NULL branches (no LoadNull) stays strictly-non-nullable:
/// Select copies branch values and adds no NULL of its own.
#[test]
fn select_is_non_nullable_when_both_branches_are() {
    let nonnull_schema = schema_pk_ints(3, false);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadColInt { col: 2 },
        LogicalInstr::LoadColInt { col: 3 },
        LogicalInstr::Select { cond: Reg(0), a: Reg(1), b: Reg(2) },
    ];
    let prog = scalar_prog(&nonnull_schema, instrs, Reg(3), vec![]);
    assert!(
        prog.prog().no_nulls,
        "Select over NOT NULL branches must stay strictly-non-nullable"
    );
}

/// U64-ness flows through Select (U64 if either branch is U64), so a downstream
/// ordered compare on the CASE result picks the unsigned variant.
#[test]
fn select_propagates_u64_tracking_to_its_reader() {
    // Schema: pk(u64), u64col(U64), i64col(I64).
    let schema = TestSchema::with_pk_at(0, &[TypeCode::U64, TypeCode::U64, TypeCode::I64]);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 2 }, // cond (i64)
        LogicalInstr::LoadColInt { col: 1 }, // a (U64)
        LogicalInstr::LoadColInt { col: 2 }, // b (i64)
        LogicalInstr::Select { cond: Reg(0), a: Reg(1), b: Reg(2) },
        LogicalInstr::LoadConst { val: 100, unsigned: false },
        LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(3), b: Reg(4) },
    ];
    let prog = scalar_prog(&schema, instrs, Reg(5), vec![]);
    // Found by shape, not by position: which index resolution lands the compare
    // at is an internal detail, and three sibling tests already read it this way.
    let cmp = prog.prog().instrs.iter().find_map(|(_, i)| match i {
        Instr::Cmp { op, order, .. } => Some((*op, *order)),
        _ => None,
    });
    assert_eq!(
        cmp,
        Some((CmpOp::Gt, IntOrder::UnsignedSigned)),
        "Select carrying a U64 branch must make the downstream compare unsigned"
    );
}

/// `Select` feeding `BoolBinary`: `cond` is a bool_input, the select result feeds the
/// AND as a bool_input, and the select dst (a value register) is never bit_only.
#[test]
fn select_classification_of_cond_and_result() {
    let schema = schema_pk_ints(4, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 }, // cond
        LogicalInstr::LoadColInt { col: 2 }, // a
        LogicalInstr::LoadColInt { col: 3 }, // b
        LogicalInstr::Select { cond: Reg(0), a: Reg(1), b: Reg(2) },
        LogicalInstr::LoadColInt { col: 4 }, // other bool
        LogicalInstr::BoolBinary { is_or: false, a: Reg(3), b: Reg(4) },
    ];
    // `analyze` runs off the assembled program, and the same one must be a
    // legal filter.
    let prog = LogicalProgram::new(instrs.clone(), vec![Sink::Reg(Reg(5))], vec![]);
    let ProgramFacts { bit_only, bool_pack, .. } = prog.analyze(&schema, ReadAs::Bool);
    filter_prog(&schema, instrs, Reg(5), vec![]);
    // Neither r0 nor r3 is bool-produced, so neither can be bit_only and their
    // `bool_pack` bits can only come from the bool_input half.
    assert_ne!(bool_pack & (1 << 0), 0, "cond is read as a bool_input");
    assert_ne!(bool_pack & (1 << 3), 0, "select result feeds BoolBinary as bool_input");
    assert_eq!(bit_only & (1 << 3), 0, "select dst is a value register, never bit_only");
}

/// SELECT blends branch values, so its destination is a `WriteAs::Value`. As a
/// `WriteAs::Bool` it would be `bit_only` here — read by nobody, and
/// `is_filter = false` keeps `result_reg` out of `bool_input` — and the read-back
/// would return the packed truth bit instead of the branch value.
#[test]
fn a_select_result_reads_back_as_a_value() {
    // The nullable column load turns `no_nulls` off; the read-back consults
    // `bit_only` only on the nullable arm.
    let schema = schema_pk_ints(1, true);
    let mb = make_int_view(&schema, &[(1, 0, &[1])]);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 }, // cond, truthy
        LogicalInstr::LoadConst { val: 5, unsigned: false },
        LogicalInstr::LoadConst { val: 7, unsigned: false },
        LogicalInstr::Select { cond: Reg(0), a: Reg(1), b: Reg(2) },
    ];
    let mut prog = scalar_prog(&schema, instrs, Reg(3), vec![]);
    assert!(
        !prog.prog().no_nulls,
        "the nullable load keeps this off the no_nulls arm"
    );
    let val = row_values(&mut prog, &mb)[0].expect("not NULL") as i64;
    assert_eq!(val, 5, "SELECT returns the chosen branch value, not a truth bit");
}

/// Nested SELECT — `CASE WHEN c1 THEN v1 WHEN c2 THEN v2 ELSE v3 END` lowered as
/// `select(c1, v1, select(c2, v2, v3))` — must pick the first truthy WHEN.
#[test]
fn nested_selects_evaluate_inside_out() {
    // Schema: pk, c1, c2, v1, v2, v3.
    let schema = schema_pk_ints(5, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },                         // c1
        LogicalInstr::LoadColInt { col: 2 },                         // c2
        LogicalInstr::LoadColInt { col: 3 },                         // v1
        LogicalInstr::LoadColInt { col: 4 },                         // v2
        LogicalInstr::LoadColInt { col: 5 },                         // v3
        LogicalInstr::Select { cond: Reg(1), a: Reg(3), b: Reg(4) }, // inner = c2 ? v2 : v3
        LogicalInstr::Select { cond: Reg(0), a: Reg(2), b: Reg(5) }, // outer = c1 ? v1 : inner
    ];
    let mut prog = scalar_prog(&schema, instrs, Reg(6), vec![]);
    // payload cols: c1, c2, v1=10, v2=20, v3=30.
    // c1 truthy → v1.
    let batch = make_int_view(&schema, &[(1, 0, &[1, 1, 10, 20, 30])]);
    let v = row_values(&mut prog, &batch)[0].expect("not NULL") as i64;
    assert_eq!(v, 10, "c1 truthy → v1");
    // c1 false, c2 truthy → v2.
    let batch = make_int_view(&schema, &[(1, 0, &[0, 1, 10, 20, 30])]);
    let v = row_values(&mut prog, &batch)[0].expect("not NULL") as i64;
    assert_eq!(v, 20, "c1 false, c2 truthy → v2");
    // both false → v3.
    let batch = make_int_view(&schema, &[(1, 0, &[0, 0, 10, 20, 30])]);
    let v = row_values(&mut prog, &batch)[0].expect("not NULL") as i64;
    assert_eq!(v, 30, "both false → else v3");
}

// ---------------------------------------------------------------------------
// Expr-program validation (ExprValidateErr): one crafted input per vector.
// Wire code is flat five-word instructions `[opcode, selector, a1, a2, a3]`;
// several opcodes read an operand word as a full u32 (col / const_idx), not as a
// register's truncated u16.
// ---------------------------------------------------------------------------

/// The instruction words of `(opcode, selector, operands)` entries, each
/// instruction's unused operand words zeroed as the encoder leaves them — and
/// the forgeries the typed builders cannot spell, an unknown opcode among them.
fn code(instrs: &[(ExprOp, u32, &[u32])]) -> Vec<[u32; INSTR_WORDS]> {
    instrs
        .iter()
        .map(|&(op, sel, ops)| {
            assert!(ops.len() <= 3, "an instruction carries at most three operands");
            let mut w = [op.as_wire(), sel, 0, 0, 0];
            w[2..2 + ops.len()].copy_from_slice(ops);
            w
        })
        .collect()
}

/// One instruction, the common case of [`code`].
fn one(op: ExprOp, sel: u32, ops: &[u32]) -> Vec<[u32; INSTR_WORDS]> {
    code(&[(op, sel, ops)])
}

/// Encode the regions as a blob and decode it back.
fn from_regions(
    code: &[[u32; INSTR_WORDS]],
    sinks: &[[u32; SINK_WORDS]],
    pool: Vec<Vec<u8>>,
) -> Result<LogicalProgram, ExprValidateErr> {
    let blob = crate::encode_expr_blob(code.iter().copied(), sinks.iter().copied(), &pool);
    LogicalProgram::from_blob(&blob)
}

/// [`from_regions`]' `Ok` value (`LogicalProgram`) is not `Debug`/`PartialEq`, so
/// extract the `Err` to compare the variant directly.
fn wire_err(r: Result<LogicalProgram, ExprValidateErr>) -> ExprValidateErr {
    match r {
        Ok(_) => panic!("expected Err, got a valid program"),
        Err(e) => e,
    }
}

/// A program round-trips through its own blob. The pool is byte-transparent — an
/// empty entry, multi-byte UTF-8 and a non-UTF-8 string all survive — and an
/// entry no instruction names survives too.
#[test]
fn a_program_round_trips_through_its_blob() {
    let pool = vec![
        b"alpha".to_vec(),
        Vec::new(),
        "längre sträng".as_bytes().to_vec(),
        vec![0xFF, 0x00, 0xFE, 0x80],
    ];
    let instrs = vec![
        LogicalInstr::LoadColStr { col: 1 },
        LogicalInstr::StrColConst {
            op: CmpOp::Eq,
            col: 1,
            const_idx: ConstIdx(2),
        },
    ];
    let prog = LogicalProgram::new(instrs.clone(), vec![Sink::Reg(Reg(1))], pool.clone());
    let back = LogicalProgram::from_blob(&prog.to_blob_bytes()).expect("its own blob decodes");
    assert_eq!(back.instrs(), instrs.as_slice());
    assert_eq!(back.const_strings(), pool.as_slice());
    assert_eq!(back.sinks, vec![Sink::Reg(Reg(1))]);

    // The degenerate program is a valid one, not an absence.
    let empty = LogicalProgram::new(Vec::new(), Vec::new(), Vec::new());
    let back = LogicalProgram::from_blob(&empty.to_blob_bytes()).expect("a valid empty program decodes");
    assert!(back.instrs().is_empty() && back.const_strings().is_empty());
    assert!(back.sinks.is_empty());
}

/// The blob layout and the opcode vocabulary, pinned to the version word.
#[test]
fn the_blob_layout_is_pinned_to_its_version_word() {
    // One instruction, both sink kinds, one pool entry — every region non-empty.
    let prog = LogicalProgram::new(
        vec![LogicalInstr::LoadColInt { col: 1 }],
        vec![Sink::Col(1), Sink::Reg(Reg(0))],
        vec![b"ab".to_vec()],
    );
    #[rustfmt::skip]
    let want: Vec<u8> = vec![
        1, 0, 0, 0,             // instruction count
        1, 0, 0, 0,             // LoadColInt: opcode
        0, 0, 0, 0,             //   selector
        1, 0, 0, 0,             //   a1 = column 1
        0, 0, 0, 0,             //   a2
        0, 0, 0, 0,             //   a3
        2, 0, 0, 0,             // sink count
        0, 0, 0, 0,             // Sink::Col kind
        1, 0, 0, 0,             //   source column 1
        1, 0, 0, 0,             // Sink::Reg kind
        0, 0, 0, 0,             //   register 0
        1, 0, 0, 0,             // const-pool count
        2, 0, 0, 0,             // entry 0 length
        b'a', b'b',             // entry 0 bytes
    ];
    // Every decodable word and what it decodes to.
    let vocabulary: Vec<u8> = decodable_words()
        .iter()
        .flat_map(|(w, x)| {
            gnitz_wire::as_le_bytes(w)
                .iter()
                .copied()
                .chain(format!("{x:?}").into_bytes())
        })
        .collect();
    assert_eq!(
        (
            prog.to_blob_bytes(),
            gnitz_wire::checksum(&vocabulary),
            gnitz_wire::EXPR_BLOB_VERSION
        ),
        (want, 1858342113949671658, 6),
        "the expr-blob layout or opcode vocabulary changed: bump EXPR_BLOB_VERSION if a number moved, \
         then paste what is reported here"
    );
}

/// A valid empty program's blob; the guard table below mutates a clone, so each
/// case differs from a decodable blob by exactly one flaw.
fn valid_empty_blob() -> Vec<u8> {
    crate::encode_expr_blob(std::iter::empty(), std::iter::empty(), &[])
}

/// Every framing guard in [`LogicalProgram::from_blob`], against the forgery
/// that trips it — and the message it answers with, so a corrupt blob is
/// diagnosable from the log alone.
#[test]
fn each_framing_guard_rejects_its_own_forgery() {
    // An empty program's three counts: instructions at [0..4], sinks at [4..8],
    // the const pool at [8..12].
    let count = |off: usize, n: u32| {
        let mut b = valid_empty_blob();
        gnitz_wire::write_u32_le(&mut b, off, n);
        b
    };
    let short_entry = {
        let mut b = count(8, 1);
        b.extend_from_slice(&5u32.to_le_bytes());
        b.extend_from_slice(&[0xAA, 0xBB]); // only 2 of the 5 declared bytes
        b
    };
    let trailing = {
        let mut b = valid_empty_blob();
        b.push(0);
        b
    };
    let cases: &[(&str, Vec<u8>, &str)] = &[
        // Bytes before cap: an over-cap count with nothing behind it reports
        // the truncation.
        ("truncated before the code region", count(0, 5), "truncated"),
        ("truncated before the sink region", count(4, 2), "truncated"),
        (
            "an over-cap instruction count with no bytes",
            count(0, u32::MAX),
            "truncated",
        ),
        // The pool is the one count whose cap comes first: no region to bound it.
        (
            "a huge declared pool count",
            count(8, u32::MAX),
            "declared const-pool count",
        ),
        ("a pool entry declared but absent", count(8, 1), "truncated"),
        ("truncated mid pool entry", short_entry, "truncated"),
        // The trailing-bytes guard, which no truncation case reaches — those
        // trip the reader first.
        ("trailing bytes", trailing, "trailing"),
    ];
    for (what, blob, want) in cases {
        let err = LogicalProgram::from_blob(blob).expect_err(&format!("{what} must be rejected"));
        let ExprValidateErr::CorruptBlob(msg) = &err else {
            panic!("{what}: expected a CorruptBlob, got {err:?}");
        };
        assert!(
            msg.starts_with("expr blob: ") && msg.matches("expr blob").count() == 1 && msg.contains(want),
            "{what}: the message must be labelled once and name the fault, got: {msg}"
        );
    }
}

/// A pool of exactly `MAX_CONST_POOL` entries is reachable — a projection of
/// that many distinct string literals — so the cap is `>`, never `>=`.
#[test]
fn a_const_pool_at_the_cap_is_accepted_and_one_past_it_is_not() {
    let pool: Vec<Vec<u8>> = (0..MAX_CONST_POOL).map(|i| format!("c{i}").into_bytes()).collect();
    let code: Vec<[u32; INSTR_WORDS]> = (0..MAX_CONST_POOL as u32)
        .map(|i| LogicalInstr::LoadConstStr { const_idx: ConstIdx(i) }.to_wire())
        .collect();
    let last = Reg(MAX_CONST_POOL as u16 - 1);
    assert!(from_regions(&code, &[[1, last.0 as u32]], pool).is_ok());

    // Refused off the declared count alone, so the blob need carry no entry.
    let mut b = valid_empty_blob();
    gnitz_wire::write_u32_le(&mut b, 8, MAX_CONST_POOL as u32 + 1);
    let err = LogicalProgram::from_blob(&b).expect_err("an over-cap pool must be refused");
    let ExprValidateErr::CorruptBlob(msg) = &err else {
        panic!("expected a CorruptBlob, got {err:?}");
    };
    assert!(msg.contains("declared const-pool count 65"), "got: {msg}");
}

#[test]
fn from_blob_rejects_an_unknown_opcode() {
    // 0 and u32::MAX are holes permanently. Deliberately NOT "one past the
    // current maximum": that couples the test to every opcode addition while
    // adding no coverage a new opcode's own decode test does not already give.
    assert_eq!(
        wire_err(from_regions(&[[0, 0, 0, 0, 0]], &[], vec![])),
        ExprValidateErr::UnknownOpcode(0)
    );
    assert_eq!(
        wire_err(from_regions(&[[u32::MAX, 0, 0, 0, 0]], &[], vec![])),
        ExprValidateErr::UnknownOpcode(u32::MAX)
    );
    // A valid opcode lowers (control): LOAD_COL_INT col0.
    assert!(from_regions(&one(ExprOp::LoadColInt, 0, &[0]), &[], vec![]).is_ok());
}

/// The sink forgeries the split into a code and a sink region makes possible.
/// The sink region is a space of its own, so a word in it is wrong in its own
/// terms rather than as an instruction.
#[test]
fn from_blob_rejects_a_malformed_sink() {
    // A sink naming a register no instruction writes.
    assert_eq!(
        wire_err(from_regions(&[], &[[1, 3]], vec![])),
        ExprValidateErr::RegOutOfRange { reg: 3, num_regs: 0 }
    );
    // A sink kind outside the two the encoder writes.
    assert_eq!(
        wire_err(from_regions(&[], &[[7, 0]], vec![])),
        ExprValidateErr::BadSinkKind(7)
    );
}

#[test]
fn validate_err_display_names_the_register_limit() {
    assert_eq!(
        ExprValidateErr::TooManyRegs(66).to_string(),
        format!(
            "expression needs 66 registers; the limit is {} — split the predicate, or project fewer computed columns",
            crate::MAX_REGS
        )
    );
    // The type half of the requirement. The region half is its own variant, and
    // its own sentence below — a PK column is exactly what a client is likely to
    // have named.
    let mismatch = |want| ExprValidateErr::ColKindMismatch { col: 2, type_code: TypeCode::U64, want }.to_string();
    assert_eq!(
        mismatch(type_phrase(ColKind::FixedIntCol)),
        "column 2 (type code U64) cannot be used here; this operator needs a fixed-width integer column"
    );
    assert_eq!(
        mismatch(type_phrase(ColKind::FloatPayload)),
        "column 2 (type code U64) cannot be used here; this operator needs a floating-point column"
    );
    assert_eq!(
        ExprValidateErr::ColNotPayload { col: 0 }.to_string(),
        "column 0 is part of the primary key; this operator needs a payload column"
    );
    // Everything else is an internal-shape violation with no user action:
    // rendered as its Debug form.
    assert_eq!(
        ExprValidateErr::ColOutOfRange { col: 7, num_columns: 3 }.to_string(),
        "ColOutOfRange { col: 7, num_columns: 3 }"
    );
}

#[test]
fn from_blob_rejects_a_bad_register_file() {
    // One instruction past the 64-register limit: the register file *is* the
    // instruction list, so a 65th instruction is what overflows it. The count is
    // the header's, so the cap is applied to it without decoding one word.
    let load_const = one(ExprOp::LoadConst, 0, &[0]);
    let over_cap = load_const.repeat(crate::MAX_REGS + 1);
    assert_eq!(
        wire_err(from_regions(&over_cap, &[], vec![])),
        ExprValidateErr::TooManyRegs(65)
    );
    // A register sink no instruction writes.
    assert_eq!(
        wire_err(from_regions(&load_const, &[[1, 3]], vec![])),
        ExprValidateErr::RegOutOfRange { reg: 3, num_regs: 1 }
    );
    // A register word past `u16` whose low half names a written register.
    assert_eq!(
        wire_err(from_regions(&load_const, &[[1, 0x1_0000]], vec![])),
        ExprValidateErr::RegOutOfRange { reg: u16::MAX, num_regs: 1 }
    );
    let neg = code(&[
        (ExprOp::LoadConst, 0, &[0]),
        (ExprOp::IntUnary, IntUnaryOp::Neg.as_wire(), &[0x1_0000]),
    ]);
    assert_eq!(
        wire_err(from_regions(&neg, &[], vec![])),
        ExprValidateErr::RegReadBeforeWrite { reg: u16::MAX }
    );
}

/// A register is the index of the instruction that writes it, so reading one at
/// or above the reader's own index is a forward reference into a lane the morsel
/// has not filled. It is the one rejection that covers the out-of-range, the
/// self-referencing and the not-yet-written operand alike.
#[test]
fn from_blob_rejects_a_forward_register_reference() {
    let add = IntArithOp::Add.as_wire();
    // IntAdd a5 b1 as the first instruction: no register exists yet.
    assert_eq!(
        wire_err(from_regions(&one(ExprOp::IntArith, add, &[5, 1]), &[], vec![])),
        ExprValidateErr::RegReadBeforeWrite { reg: 5 }
    );
    // LoadConst then IntAdd reading its own register.
    assert_eq!(
        wire_err(from_regions(
            &code(&[(ExprOp::LoadConst, 0, &[0]), (ExprOp::IntArith, add, &[1, 0])]),
            &[],
            vec![]
        )),
        ExprValidateErr::RegReadBeforeWrite { reg: 1 }
    );
}

#[test]
fn from_blob_rejects_an_out_of_range_const_idx() {
    // STR_COL_CONST col0 const_idx9, pool of length 1.
    assert_eq!(
        wire_err(from_regions(
            &one(ExprOp::StrColConst, CmpOp::Eq.as_wire(), &[0, 9]),
            &[],
            vec![b"x".to_vec()]
        )),
        ExprValidateErr::ConstIdxOutOfRange { const_idx: 9, n: 1 }
    );
}

#[test]
fn validate_rejects_an_out_of_range_column() {
    // LOAD_COL_INT col=200 against a 3-column schema.
    let s3 = schema_pk_ints(2, true);
    let prog = from_regions(&one(ExprOp::LoadColInt, 0, &[200]), &[], vec![]).unwrap();
    assert_eq!(
        prog.validate(&s3, None),
        Err(ExprValidateErr::ColOutOfRange { col: 200, num_columns: 3 })
    );
}

#[test]
fn validate_rejects_a_pk_column_for_a_payload_only_opcode() {
    // A PK at column 0 (non-nullable) plus a payload string column.
    let s_pk = schema_pk_strings(1, true);
    // Each payload-only opcode that routes a PK column to the pi=255 sentinel.
    let eq = CmpOp::Eq.as_wire();
    let pool = vec![b"x".to_vec()];
    for (op, sel, ops) in [
        (ExprOp::LoadColFloat, 0, &[0u32] as &[u32]),
        (ExprOp::StrColConst, eq, &[0, 0]),
        (ExprOp::StrColCol, eq, &[0, 1]),
    ] {
        let prog = from_regions(&one(op, sel, ops), &[], pool.clone()).unwrap();
        assert_eq!(
            prog.validate(&s_pk, None),
            Err(ExprValidateErr::ColNotPayload { col: 0 }),
            "{op:?} must reject a PK source column"
        );
    }
}

#[test]
fn validate_rejects_a_wide_column_register_load() {
    // LOAD_COL_INT on a 16-byte U128 column: a register holds 8 bytes, so the
    // load is rejected at validation rather than silently truncating.
    let schema = TestSchema::new(&[(TypeCode::U64, false), (TypeCode::U128, true)], &[0]);
    let prog = from_regions(&one(ExprOp::LoadColInt, 0, &[1]), &[], vec![]).unwrap();
    assert_eq!(
        prog.validate(&schema, None),
        Err(ExprValidateErr::ColKindMismatch {
            col: 1,
            type_code: TypeCode::U128,
            want: type_phrase(ColKind::FixedIntCol),
        })
    );
}

/// `LoadColInt`'s kernel decodes little-endian integer bytes; a width test alone
/// let two shapes through that it cannot decode — a float column (whose bit
/// pattern would become an integer; both widths take different kernel arms) and
/// a string cell.
#[test]
fn load_col_int_requires_a_fixed_int_column() {
    for tc in [TypeCode::F64, TypeCode::F32, TypeCode::String] {
        let schema = TestSchema::with_pk_at(0, &[TypeCode::U64, tc]);
        let prog = from_regions(&one(ExprOp::LoadColInt, 0, &[1]), &[], vec![]).unwrap();
        assert_eq!(
            prog.validate(&schema, None),
            Err(ExprValidateErr::ColKindMismatch {
                col: 1,
                type_code: tc,
                want: type_phrase(ColKind::FixedIntCol),
            }),
            "LOAD_COL_INT must reject type code {tc}"
        );
    }
    // Every fixed-width integer is loadable, PK column included.
    for tc in [
        TypeCode::U8,
        TypeCode::I8,
        TypeCode::U16,
        TypeCode::I16,
        TypeCode::U32,
        TypeCode::I32,
        TypeCode::U64,
        TypeCode::I64,
    ] {
        let schema = TestSchema::with_pk_at(0, &[TypeCode::U64, tc]);
        let prog = from_regions(&one(ExprOp::LoadColInt, 0, &[1]), &[], vec![]).unwrap();
        assert_eq!(prog.validate(&schema, None), Ok(()), "type code {tc}");
    }
}

/// `LoadColFloat`'s kernel branches on width alone: a 1- or 2-byte integer
/// column makes it slice an 8-byte stride out of a narrower region (a panic),
/// and every other non-float width is silent garbage.
#[test]
fn load_col_float_requires_a_float_column() {
    let case = |tc: TypeCode| {
        let schema = TestSchema::with_pk_at(0, &[TypeCode::U64, tc]);
        from_regions(&one(ExprOp::LoadColFloat, 0, &[1]), &[], vec![])
            .unwrap()
            .validate(&schema, None)
    };
    for tc in [TypeCode::U16, TypeCode::U64, TypeCode::String] {
        assert_eq!(
            case(tc),
            Err(ExprValidateErr::ColKindMismatch {
                col: 1,
                type_code: tc,
                want: type_phrase(ColKind::FloatPayload),
            }),
            "LOAD_COL_FLOAT must reject type code {tc}"
        );
    }
    assert_eq!(case(TypeCode::F32), Ok(()));
    assert_eq!(case(TypeCode::F64), Ok(()));
}

/// The string compares read 16-byte German-string cells with `col_data(pi, 16)`,
/// which over-reads (and eventually runs off) a narrower column's region.
#[test]
fn str_opcodes_require_a_german_string_column() {
    let schema = |a: TypeCode, b: TypeCode| TestSchema::with_pk_at(0, &[TypeCode::U64, a, b]);
    // STR_COL_CONST col=1.
    let vs_const = |a: TypeCode| {
        from_regions(
            &one(ExprOp::StrColConst, CmpOp::Eq.as_wire(), &[1, 0]),
            &[],
            vec![b"x".to_vec()],
        )
        .unwrap()
        .validate(&schema(a, TypeCode::String), None)
    };
    assert_eq!(
        vs_const(TypeCode::U64),
        Err(ExprValidateErr::ColKindMismatch {
            col: 1,
            type_code: TypeCode::U64,
            want: type_phrase(ColKind::StringPayload),
        })
    );
    assert_eq!(vs_const(TypeCode::String), Ok(()));
    assert_eq!(vs_const(TypeCode::Blob), Ok(()));

    // STR_COL_COL col_a=1 col_b=2 — both operands are checked.
    let vs_col = |a: TypeCode, b: TypeCode| {
        from_regions(&one(ExprOp::StrColCol, CmpOp::Eq.as_wire(), &[1, 2]), &[], vec![])
            .unwrap()
            .validate(&schema(a, b), None)
    };
    assert_eq!(
        vs_col(TypeCode::U64, TypeCode::String),
        Err(ExprValidateErr::ColKindMismatch {
            col: 1,
            type_code: TypeCode::U64,
            want: type_phrase(ColKind::StringPayload),
        })
    );
    assert_eq!(
        vs_col(TypeCode::String, TypeCode::U64),
        Err(ExprValidateErr::ColKindMismatch {
            col: 2,
            type_code: TypeCode::U64,
            want: type_phrase(ColKind::StringPayload),
        })
    );
    assert_eq!(vs_col(TypeCode::String, TypeCode::Blob), Ok(()));
}

/// `IsNull`/`IsNotNull` read the NULL bitmap and nothing else, so any payload
/// column is legitimate — splitting them out of the shared payload-only arm must
/// not have narrowed them to a type class.
#[test]
fn null_tests_accept_any_payload_column() {
    for tc in [TypeCode::U128, TypeCode::String, TypeCode::F32] {
        let schema = TestSchema::with_pk_at(0, &[TypeCode::U64, tc]);
        for invert in [0u32, 1] {
            let prog = from_regions(&one(ExprOp::IsNull, invert, &[1]), &[], vec![]).unwrap();
            assert_eq!(prog.validate(&schema, None), Ok(()), "invert {invert} type {tc}");
        }
    }
}

/// `copy_column` byte-copies at equal width and otherwise widens a narrower
/// integer into a wider slot: no narrowing, no representation change. The
/// widening set is exactly the cross-width set-op coercion the client emits.
#[test]
fn copy_col_admits_only_a_widening_destination() {
    // in: [U64 PK, <src>]; out: [U64 PK, <dst>] — one payload slot each.
    let pair = |src: TypeCode, dst: TypeCode| {
        let in_schema = TestSchema::with_pk_at(0, &[TypeCode::U64, src]);
        let out_schema = TestSchema::with_pk_at(0, &[TypeCode::U64, dst]);
        LogicalProgram::copy_cols(&[1]).validate(&in_schema, Some(&out_schema))
    };
    for (src, dst) in [
        (TypeCode::I64, TypeCode::I64),
        (TypeCode::String, TypeCode::String),
        (TypeCode::Blob, TypeCode::Blob),
        (TypeCode::U128, TypeCode::U128),
        (TypeCode::F64, TypeCode::F64),
        (TypeCode::U32, TypeCode::U64),
        (TypeCode::U32, TypeCode::I64),
        (TypeCode::U8, TypeCode::I16),
    ] {
        assert_eq!(pair(src, dst), Ok(()), "{src} -> {dst} must be accepted");
    }
    for (src, dst) in [
        (TypeCode::String, TypeCode::U64),
        (TypeCode::U64, TypeCode::String),
        (TypeCode::U128, TypeCode::U64),
        (TypeCode::U64, TypeCode::F64),
        (TypeCode::F32, TypeCode::F64),
        (TypeCode::U64, TypeCode::I64),
    ] {
        assert_eq!(
            pair(src, dst),
            Err(ExprValidateErr::CopyTypeMismatch { col: 1, src_tc: src, out: 0, out_tc: dst }),
            "{src} -> {dst} must be rejected"
        );
    }
    // A PK source into a payload slot of the same type is a copy, not a promotion.
    let in_pk = TestSchema::with_pk_at(0, &[TypeCode::U64, TypeCode::I64]);
    let out_pk = TestSchema::with_pk_at(0, &[TypeCode::I64, TypeCode::U64]);
    let prog = LogicalProgram::copy_cols(&[0]);
    assert_eq!(prog.validate(&in_pk, Some(&out_pk)), Ok(()));
    // Both output-side checks are inert for a filter (`out_schema = None`).
    assert_eq!(prog.validate(&in_pk, None), Ok(()));
}

/// A register sink stores a whole 8-byte register image: a 16-byte slot panics
/// on the first row and a narrower one truncates, so the destination stride is
/// exactly 8.
#[test]
fn a_register_sink_slot_must_be_eight_bytes() {
    // out: [U64 PK, <slot>] — one payload slot, written by the single sink.
    let case = |tc: TypeCode| {
        let schema = TestSchema::with_pk_at(0, &[TypeCode::U64, tc]);
        // LOAD_CONST into reg 0, stored into output slot 0.
        from_regions(&one(ExprOp::LoadConst, 0, &[0]), &[[1, 0]], vec![])
            .unwrap()
            .validate(&schema, Some(&schema))
    };
    for tc in [TypeCode::I64, TypeCode::U64, TypeCode::F64] {
        assert_eq!(case(tc), Ok(()), "type code {tc}");
    }
    for tc in [TypeCode::U128, TypeCode::F32] {
        assert_eq!(
            case(tc),
            Err(ExprValidateErr::EmitSlotWidth { out: 0, type_code: tc }),
            "type code {tc}"
        );
    }
    // A German-string slot is refused on class, before the stride rule: the
    // source register is scalar, and the two errors name different faults.
    for tc in [TypeCode::String, TypeCode::Blob] {
        assert_eq!(
            case(tc),
            Err(ExprValidateErr::EmitClassMismatch { out: 0, type_code: tc }),
            "type code {tc}"
        );
    }
}

/// Output coverage: with an `out_schema`, the sink list must be exactly as long
/// as the declared payload. A sink writes the slot at its own position, so a
/// permuted or duplicated destination cannot be expressed and a short list is
/// the only failure left.
#[test]
fn validate_rejects_a_sink_list_that_does_not_cover_the_output() {
    // in/out: [U64 PK, I64, I64] — two output payload slots.
    let schema = schema_pk_ints(2, false);
    let map = |sinks: &[[u32; SINK_WORDS]]| from_regions(&[], sinks, vec![]).unwrap();

    // Both slots written — accepted.
    assert_eq!(map(&[[0, 1], [0, 2]]).validate(&schema, Some(&schema)), Ok(()));

    // Only slot 0 written.
    assert_eq!(
        map(&[[0, 1]]).validate(&schema, Some(&schema)),
        Err(ExprValidateErr::OutputSlotCountMismatch { sinks: 1, num_payload_cols: 2 })
    );

    // A predicate over the same program is unaffected — no output plan.
    assert_eq!(map(&[[0, 1]]).validate(&schema, None), Ok(()));
}

#[test]
#[should_panic(expected = "RegReadBeforeWrite")]
fn new_panics_on_a_forward_register_reference() {
    // A compiler-built (trusted) program that reads an unwritten register still
    // panics from `new`.
    let _ = LogicalProgram::new(
        vec![LogicalInstr::IntArith {
            op: IntArithOp::Add,
            a: Reg(0),
            b: Reg(1),
        }],
        Vec::new(),
        vec![],
    );
}

// ---------------------------------------------------------------------------
// IntInSet — set membership as one opcode (O(1) registers, O(log N) per row)
// ---------------------------------------------------------------------------

/// `r0 = col1; r1 = r0 IN set`, result_reg = 1 — the compiled shape of
/// `col1 IN (…)`. `col_tc` picks col1's type (I64 / U64 / …).
fn in_set_prog(col_tc: TypeCode, set: &[i64]) -> (TestSchema, ScalarEval) {
    let schema = TestSchema::with_pk_at(0, &[TypeCode::U64, col_tc]);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::IntInSet { value_reg: Reg(0), set_idx: ConstIdx(0) },
    ];
    let prog = scalar_prog(&schema, instrs, Reg(1), vec![gnitz_wire::as_le_bytes(set).to_vec()]);
    (schema, prog)
}

/// Membership at every pool shape that can change the answer, plus the 3VL rule
/// that a NULL operand is NULL out whatever the pool holds.
///
/// The `U64` case is the one that is not plain integer comparison: the column
/// loads as a bare bit-reinterpret and the folded literal `-1` is `i64::-1`, so
/// `u64::MAX` matches — the same bit-equality `col = -1` gives. The 1000-element
/// pool is here because membership is a binary search rather than an OR-chain,
/// so pool size costs no registers; at ~4000 registers the OR-chain form would
/// exceed the cap.
#[test]
fn int_in_set_membership_over_every_pool_shape() {
    // (column type, pool, [(value, expected membership)]) — aliased because the
    // inline tuple trips `clippy::type_complexity`.
    type Case<'a> = (TypeCode, &'a [i64], &'a [(i64, i64)]);
    let big: Vec<i64> = (0..1000).collect();
    let cases: [Case<'_>; 4] = [
        (
            TypeCode::I64,
            &[-5, -1, 0, 3],
            &[(-5, 1), (-1, 1), (0, 1), (3, 1), (2, 0), (100, 0)],
        ),
        (TypeCode::U64, &[-1], &[(u64::MAX as i64, 1), (7, 0)]),
        (TypeCode::I64, &[], &[(42, 0)]),
        (TypeCode::I64, &big, &[(0, 1), (777, 1), (999, 1), (1000, 0), (-1, 0)]),
    ];

    for (col_tc, pool, values) in cases {
        let (schema, mut prog) = in_set_prog(col_tc, pool);
        for &(v, want) in values {
            let mb = make_int_view(&schema, &[(1, 0, &[v])]);
            assert_eq!(
                row_values(&mut prog, &mb)[0],
                Some(i128::from(want)),
                "tc={col_tc} pool_len={} value={v}",
                pool.len()
            );
        }
        // col1 is payload slot 0, so bit 0 of the null word is its NULL.
        let mb = make_int_view(&schema, &[(1, 0b1, &[0])]);
        assert!(
            row_values(&mut prog, &mb)[0].is_none(),
            "tc={col_tc}: a NULL operand must produce a NULL membership result"
        );
    }
}

/// `NOT IN` is `bool_not(IN)`, so the 3VL rule is the composition's: a NULL
/// operand makes `IN` NULL and `NOT(NULL)` NULL, which excludes the row — the
/// membership sweep above already pins `IN`'s own NULL answer per pool shape.
#[test]
fn not_in_set_excludes_a_null_operand() {
    let schema = schema_pk_ints(1, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::IntInSet { value_reg: Reg(0), set_idx: ConstIdx(0) },
        LogicalInstr::BoolNot { a: Reg(1) },
    ];
    let mut prog = scalar_prog(
        &schema,
        instrs,
        Reg(2),
        vec![gnitz_wire::as_le_bytes(&[1i64, 2, 3]).to_vec()],
    );
    for (null_word, val, want) in [(0b1, 0, None), (0, 9, Some(1)), (0, 2, Some(0))] {
        let mb = make_int_view(&schema, &[(1, null_word, &[val])]);
        assert_eq!(
            row_values(&mut prog, &mb)[0],
            want,
            "NOT IN over {val} (null_word {null_word:#b})"
        );
    }
}

#[test]
fn a_non_canonical_in_set_pool_is_rejected() {
    for pool in [&[42i64, 7, 3][..], &[1, 3, 3, 7]] {
        assert_eq!(
            wire_err(from_regions(
                &one(ExprOp::IntInSet, 0, &[0, 0]),
                &[],
                vec![gnitz_wire::as_le_bytes(pool).to_vec()]
            )),
            ExprValidateErr::IntSetNotCanonical { set_idx: 0 },
            "pool {pool:?}"
        );
    }
}

#[test]
fn validate_rejects_a_misaligned_in_set_pool() {
    assert_eq!(
        wire_err(from_regions(
            &one(ExprOp::IntInSet, 0, &[0, 0]),
            &[],
            vec![vec![0u8; 5]]
        )),
        ExprValidateErr::IntSetNotCanonical { set_idx: 0 }
    );
}

#[test]
fn validate_rejects_an_out_of_range_set_idx() {
    // set_idx = 9 against a pool of length 1 → ConstIdxOutOfRange.
    assert_eq!(
        wire_err(from_regions(
            &one(ExprOp::IntInSet, 0, &[0, 9]),
            &[],
            vec![vec![0u8; 8]]
        )),
        ExprValidateErr::ConstIdxOutOfRange { const_idx: 9, n: 1 }
    );
}

/// Classifier: every CMP and every AND in a pure conjunction is bit_only
/// (with `is_filter=true`); result_reg stays bit_only.
#[test]
fn classifier_pure_conjunction_filter() {
    let schema = schema_pk_ints(2, true);
    let instrs = conjunction_over_two_cols();
    // `analyze` runs off the assembled program, and the same one must be a
    // legal filter.
    let prog = LogicalProgram::new(instrs.clone(), vec![Sink::Reg(Reg(5))], vec![]);
    let ProgramFacts { bit_only, bool_pack, .. } = prog.analyze(&schema, ReadAs::Bool);
    filter_prog(&schema, instrs, Reg(5), vec![]);
    // Bool producers: r2 (`Cmp` Gt), r4 (`Cmp` Gt), r5 (`BoolBinary`).
    // Non-bool readers consume r0/r1/r3 (CMPs read them as i64), so those
    // never qualify for bit_only. r2/r4 are read only by `BoolBinary`, and r5
    // (result_reg) stays bit_only under is_filter=true.
    let expected_bit_only = (1u64 << 2) | (1u64 << 4) | (1u64 << 5);
    assert_eq!(
        bit_only, expected_bit_only,
        "expected r2/r4/r5 bit_only; got mask {bit_only:#010b}"
    );
    // r2 and r4 are read by `BoolBinary`; r5 (result_reg) is force-marked a bool
    // input so the filter's nullable word merge always finds it packed. Every
    // bit_only register is packed too, and here the two halves coincide.
    assert_eq!(bool_pack, (1 << 2) | (1 << 4) | (1 << 5));
}

// ---------------------------------------------------------------------------
// Numeric scalar functions and numeric CAST: validation and the resolve-time
// analyses (U64 tracking, nullability classification).
// ---------------------------------------------------------------------------

/// The cast target is the selector, so a forged blob can put anything there. It
/// must be rejected before eval, where it would index a bounds table that has no
/// arm for it.
#[test]
fn decode_rejects_a_forged_cast_target() {
    for op in [ExprOp::IntCast, ExprOp::FloatToInt] {
        for tc in [
            0u32,
            TypeCode::String.as_wire() as u32,
            TypeCode::U128.as_wire() as u32,
            TypeCode::F64.as_wire() as u32,
            255,
            // A `>= 255` word must not be truncated into a valid code on the
            // way in: the full u32 has to reach the narrowing, or these would
            // pass as I64.
            0x100u32 | TypeCode::I64.as_wire() as u32,
            0x1_0000u32 | TypeCode::I64.as_wire() as u32,
        ] {
            let c = code(&[(ExprOp::LoadColInt, 0, &[1]), (op, tc, &[0])]);
            assert_eq!(
                wire_err(from_regions(&c, &[], vec![])),
                ExprValidateErr::BadSelector { op: op.as_wire(), selector: tc },
                "{op:?} tc {tc}"
            );
        }
        // Control: every fixed-int target is accepted.
        for tc in [
            TypeCode::I8,
            TypeCode::U8,
            TypeCode::I16,
            TypeCode::U16,
            TypeCode::I32,
            TypeCode::U32,
            TypeCode::I64,
            TypeCode::U64,
        ] {
            let c = code(&[(ExprOp::LoadColInt, 0, &[1]), (op, tc as u32, &[0])]);
            assert!(from_regions(&c, &[], vec![]).is_ok(), "{op:?} tc {tc} must be accepted");
        }
    }
}

#[test]
fn validate_bounds_checks_every_register_operand() {
    let mut unary: Vec<(ExprOp, u32)> = vec![(ExprOp::IntUnary, IntUnaryOp::Abs.as_wire()), (ExprOp::FloatToF32, 0)];
    for op in [
        FloatUnaryOp::Abs,
        FloatUnaryOp::Floor,
        FloatUnaryOp::Ceil,
        FloatUnaryOp::Round,
        FloatUnaryOp::Trunc,
    ] {
        unary.push((ExprOp::FloatUnary, op.as_wire()));
    }
    for (op, sel) in unary {
        // The operand names a register no earlier instruction wrote — the same
        // rejection whether it is the opcode's own index or one past the end.
        for a in [0u32, 9] {
            assert!(matches!(
                wire_err(from_regions(&one(op, sel, &[a]), &[], vec![])),
                ExprValidateErr::RegReadBeforeWrite { .. }
            ));
        }
    }
    for (op, sel) in [
        (ExprOp::IntMinMax2, 0),
        (ExprOp::IntMinMax2, 1),
        (ExprOp::FloatMinMax2, 0),
        (ExprOp::FloatMinMax2, 1),
    ] {
        // Both operand words are bounded, not just the first.
        assert!(matches!(
            wire_err(from_regions(
                &code(&[(ExprOp::LoadConst, 0, &[0]), (op, sel, &[0, 9])]),
                &[],
                vec![]
            )),
            ExprValidateErr::RegReadBeforeWrite { reg: 9 }
        ));
    }
}

/// The three casts manufacture NULL out of a non-NULL input, so a program
/// carrying one can never take the `no_nulls` fast path; the pure transforms
/// only propagate and must not disturb it.
#[test]
fn narrowing_casts_manufacture_null_but_pure_transforms_do_not() {
    let nonnull = schema_pk_ints(1, false);
    let with = |instr: LogicalInstr| {
        let instrs = vec![LogicalInstr::LoadColInt { col: 1 }, instr];
        scalar_prog(&nonnull, instrs, Reg(1), vec![]).prog().no_nulls
    };
    for instr in [
        LogicalInstr::IntUnary { op: IntUnaryOp::Abs, a: Reg(0) },
        LogicalInstr::FloatUnary {
            op: super::FloatUnaryOp::Round,
            a: Reg(0),
        },
        LogicalInstr::IntMinMax2 { a: Reg(0), b: Reg(0), is_max: true },
    ] {
        assert!(with(instr), "a propagating transform keeps no_nulls");
    }
    for instr in [
        LogicalInstr::FloatToF32 { a: Reg(0) },
        LogicalInstr::IntCast { a: Reg(0), fi: FixedInt::I8 },
        LogicalInstr::FloatToInt { a: Reg(0), fi: FixedInt::I8 },
    ] {
        assert!(!with(instr), "a narrowing cast manufactures NULL");
    }
}

/// A null test reads the batch's null bitmap and yields a definite boolean, so
/// it does not by itself force the nullable arm — even over a nullable column.
/// What does force it is *loading* that column, which is a separate opcode with
/// its own verdict. Both directions are pinned here: the classification is this
/// module's, and `tests/eval.rs` only consumes it.
#[test]
fn a_null_test_alone_keeps_no_nulls_but_a_load_of_the_column_does_not() {
    let schema = schema_pk_ints(1, true);
    let no_nulls =
        |instrs: Vec<LogicalInstr>, result: u16| scalar_prog(&schema, instrs, Reg(result), vec![]).prog().no_nulls;

    for instr in [is_null_op(1), is_not_null_op(1)] {
        assert!(
            no_nulls(vec![instr], 0),
            "a null test over a nullable column is definite"
        );
    }

    // The same nullable column, now also loaded: the load is what carries the
    // NULL into a register, so the program belongs on the nullable arm.
    assert!(
        !no_nulls(vec![is_null_op(1), LogicalInstr::LoadColInt { col: 1 }], 0),
        "loading the tested column must force the nullable arm"
    );
}

/// A PK column carries no null bit — the null bitmap is payload-indexed — so
/// loading one never forces the nullable arm. `TestSchema` validates nothing,
/// so it can state the nullable PK both production implementors reject, which
/// is what makes the guard's absence observable.
#[test]
fn a_nullable_pk_column_load_keeps_no_nulls() {
    let schema = TestSchema::new(&[(TypeCode::U64, true), (TypeCode::I64, false)], &[0]);
    let instrs = vec![LogicalInstr::LoadColInt { col: 0 }];
    assert!(scalar_prog(&schema, instrs, Reg(0), vec![]).prog().no_nulls);
}

/// A float column load inherits its column's null bit, like every other typed
/// column operand. Driven in both directions — a nullable F64 column must force
/// the nullable arm, a non-nullable one must leave `no_nulls` set — so a
/// classification that ignored the column's nullability outright fails here
/// whichever way it defaulted.
#[test]
fn a_float_column_load_carries_its_null_bit() {
    let no_nulls_over = |nullable: bool| {
        let schema = TestSchema::new(&[(TypeCode::U64, false), (TypeCode::F64, nullable)], &[0]);
        let instrs = vec![LogicalInstr::LoadColFloat { col: 1 }];
        scalar_prog(&schema, instrs, Reg(0), vec![]).prog().no_nulls
    };
    assert!(!no_nulls_over(true), "a nullable F64 column forces the nullable arm");
    assert!(no_nulls_over(false), "a non-nullable one does not");
}

/// `IntMinMax2` picks its compare domain from the U64 tracking of BOTH
/// operands, and re-taints its own dst — which is what makes the lowering's
/// fold-head rotation sufficient to fix the domain for a whole n-ary fold.
#[test]
fn min_max2_compare_domain_and_u64_propagation() {
    // pk(U64), col1 = U64, col2 = I64.
    let schema = TestSchema::with_pk_at(0, &[TypeCode::U64, TypeCode::U64, TypeCode::I64]);
    let signed_of = |a_col: u32, b_col: u32| {
        let instrs = vec![
            LogicalInstr::LoadColInt { col: a_col },
            LogicalInstr::LoadColInt { col: b_col },
            LogicalInstr::IntMinMax2 { a: Reg(0), b: Reg(1), is_max: true },
            // A second fold against the signed column reads dst's taint.
            LogicalInstr::LoadColInt { col: 2 },
            LogicalInstr::IntMinMax2 { a: Reg(2), b: Reg(3), is_max: true },
        ];
        let prog = scalar_prog(&schema, instrs, Reg(4), vec![]);
        prog.prog()
            .instrs
            .iter()
            .filter_map(|(_, i)| match i {
                Instr::IntMinMax2 { signed, .. } => Some(*signed),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    // Both signed: signed compare throughout.
    assert_eq!(signed_of(2, 2), vec![true, true]);
    // Either operand U64-tracked makes the fold unsigned, and the taint is
    // sticky, so the follow-on fold against a signed column stays unsigned.
    assert_eq!(signed_of(1, 2), vec![false, false]);
    assert_eq!(signed_of(2, 1), vec![false, false]);
}

/// An emitted `INT_CAST` re-seeds the U64 tracking from its TARGET, which is
/// what lets `CAST(x AS BIGINT UNSIGNED)` drive a downstream unsigned compare.
#[test]
fn int_cast_reseeds_u64_tracking_from_its_target() {
    // pk(U64), col1 = I64 (signed-tracked), col2 = U64.
    let schema = TestSchema::with_pk_at(0, &[TypeCode::U64, TypeCode::I64, TypeCode::U64]);
    let cmp_signed_after = |fi: FixedInt, src_col: u32| {
        let instrs = vec![
            LogicalInstr::LoadColInt { col: src_col },
            LogicalInstr::IntCast { a: Reg(0), fi },
            LogicalInstr::LoadConst { val: 0, unsigned: false },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(1), b: Reg(2) },
        ];
        let prog = scalar_prog(&schema, instrs, Reg(3), vec![]);
        prog.prog()
            .instrs
            .iter()
            .find_map(|(_, i)| match i {
                Instr::Cmp { order, .. } => Some(*order == IntOrder::Signed),
                _ => None,
            })
            .expect("the compare survives")
    };
    assert!(!cmp_signed_after(FixedInt::U64, 1), "U64 target taints the dst");
    assert!(
        cmp_signed_after(FixedInt::I64, 2),
        "a signed target clears a U64 source's taint"
    );
    assert!(cmp_signed_after(FixedInt::I32, 1));
}

/// `src_signed` is resolved from the OPERAND's tracking, not from the target —
/// it is what decides whether the range check reads the register as i64 or u64.
#[test]
fn int_cast_records_the_source_signedness() {
    let schema = TestSchema::with_pk_at(0, &[TypeCode::U64, TypeCode::I64, TypeCode::U64]);
    let src_signed_of = |src_col: u32| {
        let instrs = vec![
            LogicalInstr::LoadColInt { col: src_col },
            LogicalInstr::IntCast { a: Reg(0), fi: FixedInt::I8 },
        ];
        let prog = scalar_prog(&schema, instrs, Reg(1), vec![]);
        let signed = prog
            .prog()
            .instrs
            .iter()
            .find_map(|(_, i)| match i {
                Instr::IntCast { src_signed, .. } => Some(*src_signed),
                _ => None,
            })
            .expect("the cast survives");
        signed
    };
    assert!(src_signed_of(1), "an I64 column is a signed source");
    assert!(!src_signed_of(2), "a U64 column is an unsigned source");
}

// ---------------------------------------------------------------------------
// Register classes
// ---------------------------------------------------------------------------

/// Decode a code region and keep only the verdict. The class rules are
/// schema-free, so this is where a forged program meets them — before any schema
/// is in hand.
fn from_code(code: &[[u32; INSTR_WORDS]]) -> Result<(), ExprValidateErr> {
    from_regions(code, &[], vec![]).map(|_| ())
}

/// A mixed-class register operand is refused whichever way round it goes: a
/// string opcode reading the scalar file would resolve a lane that was never
/// written, and an integer opcode reading a string register would interpret
/// whatever i64 sits at that index.
#[test]
fn operand_class_is_enforced_in_both_directions() {
    let load_str = (ExprOp::LoadColStr, 0, &[1u32] as &[u32]);
    // LOAD_COL_INT into reg 0, then UPPER of it.
    assert_eq!(
        from_code(&code(&[(ExprOp::LoadColInt, 0, &[1]), (ExprOp::StrCase, 1, &[0])])),
        Err(ExprValidateErr::RegClassMismatch { reg: 0 })
    );
    // LOAD_COL_STR into reg 0, then integer ADD of it.
    assert_eq!(
        from_code(&code(&[
            load_str,
            (ExprOp::IntArith, IntArithOp::Add.as_wire(), &[0, 0])
        ])),
        Err(ExprValidateErr::RegClassMismatch { reg: 0 })
    );
    // The mixed-class opcodes police each half separately: SUBSTR's source must
    // be a string and its bounds must not be.
    assert_eq!(
        from_code(&code(&[
            (ExprOp::LoadConst, 0, &[1]),
            (ExprOp::StrSubstr, 0, &[0, 0, u32::MAX])
        ])),
        Err(ExprValidateErr::RegClassMismatch { reg: 0 })
    );
    assert_eq!(
        from_code(&code(&[load_str, load_str, (ExprOp::StrSubstr, 0, &[0, 1, u32::MAX])])),
        Err(ExprValidateErr::RegClassMismatch { reg: 1 }),
        "a string register cannot be a window bound"
    );
}

/// Reading a register *before* its writer is refused outright: a stale lane
/// holds whatever the previous morsel left, and for a string lane that resolves
/// against a refilled arena and hands back another row's bytes.
#[test]
fn string_operand_read_before_its_writer_is_refused() {
    // UPPER of reg 1 at instruction 0; reg 1's only writer is instruction 1.
    let upper_then_load = code(&[(ExprOp::StrCase, 1, &[1]), (ExprOp::LoadColStr, 0, &[1])]);
    assert_eq!(
        from_code(&upper_then_load),
        Err(ExprValidateErr::RegReadBeforeWrite { reg: 1 })
    );
    // The same two instructions in the other order are legal, so the rejection
    // above is about ordering and not about the instructions themselves.
    let ordered = code(&[(ExprOp::LoadColStr, 0, &[1]), (ExprOp::StrCase, 1, &[0])]);
    assert!(from_code(&ordered).is_ok());
}

/// A register names the instruction that writes it, so an instruction reading
/// its own register is a forward reference into a lane the morsel has not
/// filled. This is what used to be the anti-aliasing rule the string kernels'
/// `split_windows` depends on; it is now the same one comparison as
/// read-before-write.
#[test]
fn a_string_op_cannot_read_the_register_it_writes() {
    let self_read = |op: ExprOp, sel: u32, a: u32, b: u32| {
        from_code(&code(&[
            (ExprOp::LoadColStr, 0, &[1]),
            (ExprOp::LoadColStr, 0, &[2]),
            (op, sel, &[a, b]),
        ]))
    };
    for (op, sel) in [(ExprOp::StrCmp, CmpOp::Eq.as_wire()), (ExprOp::StrConcat, 0)] {
        assert_eq!(
            self_read(op, sel, 2, 1),
            Err(ExprValidateErr::RegReadBeforeWrite { reg: 2 }),
            "{op:?} must not read the register it writes"
        );
    }
    // STR_SELECT takes a *scalar* condition, so its program needs one more load
    // before the self-reference is the only fault.
    assert_eq!(
        from_code(&code(&[
            (ExprOp::LoadConst, 0, &[0]),
            (ExprOp::LoadColStr, 0, &[1]),
            (ExprOp::StrSelect, 0, &[0, 2, 1]),
        ])),
        Err(ExprValidateErr::RegReadBeforeWrite { reg: 2 })
    );
}

/// Single assignment is not a checked rule any more: a register *is* the index
/// of the one instruction that writes it, so two writers cannot be spelled and a
/// sink-only program has no register to collide over.
#[test]
fn a_sink_only_program_names_no_registers() {
    assert!(LogicalProgram::copy_cols(&[0, 1, 2]).instrs().is_empty());
}

/// A register sink's destination must hold what the source register's class
/// stores. A mismatch either way is caught before eval, where it would write an
/// 8-byte register image into a 16-byte cell (or the reverse).
#[test]
fn a_register_sink_must_match_its_destination_column() {
    // out: [U64 PK, STRING].
    let str_out = TestSchema::with_pk_at(0, &[TypeCode::U64, TypeCode::String]);
    let int_out = TestSchema::with_pk_at(0, &[TypeCode::U64, TypeCode::I64]);
    let in_str = TestSchema::with_pk_at(0, &[TypeCode::U64, TypeCode::String]);
    let store_r0: &[[u32; SINK_WORDS]] = &[[1, 0]]; // Sink::Reg(0)

    let scalar_src = from_regions(&one(ExprOp::LoadConst, 0, &[7]), store_r0, vec![]).unwrap();
    assert_eq!(
        scalar_src.validate(&in_str, Some(&str_out)),
        Err(ExprValidateErr::EmitClassMismatch { out: 0, type_code: TypeCode::String })
    );

    let str_src = from_regions(&one(ExprOp::LoadColStr, 0, &[1]), store_r0, vec![]).unwrap();
    assert_eq!(
        str_src.validate(&in_str, Some(&int_out)),
        Err(ExprValidateErr::EmitClassMismatch { out: 0, type_code: TypeCode::I64 })
    );
    // The matching pairing is accepted, and the class mask the check read holds
    // reg 0 to be a string for one program and a scalar for the other.
    assert_eq!(str_src.validate(&in_str, Some(&str_out)), Ok(()));
    assert_eq!(str_src.str_class, 1);
    assert_eq!(scalar_src.validate(&in_str, Some(&int_out)), Ok(()));
    assert_eq!(scalar_src.str_class, 0);
}

/// The result register's class splits the two resolvers that make the identical
/// `validate` call: a filter reads its verdict out of the scalar file, while a
/// SET right-hand side wants exactly a string back.
#[test]
fn a_string_result_register_resolves_as_a_scalar_but_not_as_a_filter() {
    let schema = schema_pk_strings(1, true);
    let prog = || from_regions(&one(ExprOp::LoadColStr, 0, &[1]), &[[1, 0]], vec![]).unwrap();
    assert_eq!(
        prog().resolve_filter(&schema).err(),
        Some(ExprValidateErr::RegClassMismatch { reg: 0 })
    );
    assert!(prog().resolve_scalar(&schema).is_ok());
}

#[test]
fn load_col_str_requires_a_german_string_column() {
    let schema = TestSchema::with_pk_at(0, &[TypeCode::U64, TypeCode::I64]);
    assert_eq!(
        from_regions(&one(ExprOp::LoadColStr, 0, &[1]), &[], vec![])
            .unwrap()
            .validate(&schema, None),
        Err(ExprValidateErr::ColKindMismatch {
            col: 1,
            type_code: TypeCode::I64,
            want: type_phrase(ColKind::StringPayload),
        })
    );
}

#[test]
fn trim_mode_and_cast_target_are_narrowed_at_decode() {
    let trim = |mode: u32| {
        from_regions(
            &code(&[(ExprOp::LoadColStr, 0, &[1]), (ExprOp::StrTrim, mode, &[0, 0])]),
            &[],
            vec![b" ".to_vec()],
        )
        .map(|_| ())
    };
    for mode in 0..3 {
        assert!(trim(mode).is_ok(), "mode {mode}");
    }
    assert_eq!(
        trim(3),
        Err(ExprValidateErr::BadSelector {
            op: ExprOp::StrTrim.as_wire(),
            selector: 3
        })
    );

    assert_eq!(
        from_code(&code(&[(ExprOp::LoadColStr, 0, &[1]), (ExprOp::StrToInt, 999, &[0])])),
        Err(ExprValidateErr::BadSelector {
            op: ExprOp::StrToInt.as_wire(),
            selector: 999
        })
    );
}

#[test]
fn like_rejects_a_forged_operand() {
    // LOAD_COL_STR into reg 0, then LIKE of it into reg 1.
    let load_like =
        |ci: u32, pat_idx: u32| code(&[(ExprOp::LoadColStr, 0, &[1]), (ExprOp::StrLike, ci, &[0, pat_idx])]);
    let pool = || vec![b"a\xFF".to_vec()];
    let decode = |c: Vec<[u32; INSTR_WORDS]>, pool: Vec<Vec<u8>>| from_regions(&c, &[], pool).map(|_| ());

    assert!(decode(load_like(0, 0), pool()).is_ok());
    assert!(decode(load_like(1, 0), pool()).is_ok());
    // An empty pool entry is the legal `LIKE ''`.
    assert!(decode(load_like(0, 0), vec![Vec::new()]).is_ok());

    assert_eq!(
        decode(load_like(0, 9), pool()),
        Err(ExprValidateErr::ConstIdxOutOfRange { const_idx: 9, n: 1 })
    );
    // The source must be a string register.
    assert_eq!(
        decode(
            code(&[(ExprOp::LoadColInt, 0, &[1]), (ExprOp::StrLike, 0, &[0, 0])]),
            pool()
        ),
        Err(ExprValidateErr::RegClassMismatch { reg: 0 })
    );
}

/// A matcher is compiled per instruction, not per pool index: the same pattern
/// under LIKE and under ILIKE are two different matchers.
#[test]
fn two_like_opcodes_over_one_pool_index_get_a_matcher_each() {
    let schema = schema_pk_strings(1, false);
    let like = |ci| LogicalInstr::StrLike { src: Reg(0), pat_idx: ConstIdx(0), ci };
    let instrs = vec![LogicalInstr::LoadColStr { col: 1 }, like(false), like(true)];
    let mut ev = scalar_prog(&schema, instrs, Reg(1), vec![b"abc".to_vec()]);
    assert_eq!(ev.prog().like_matchers.len(), 2);

    let rows: Vec<&[&[u8]]> = vec![&[b"ABC"]];
    let view = make_string_view(&schema, &rows);
    // Register 1 is the case-sensitive verdict (the program's result); the
    // ILIKE one is read off the same row through `reg_values`. Captured rather
    // than asserted inside the callback, which a non-firing morsel loop would
    // let pass vacuously.
    assert_eq!(row_values(&mut ev, &view)[0], Some(0));
    let mut ci_verdicts: Vec<i64> = Vec::new();
    ev.eval_morsels(&view, 0, 1, |_, out| ci_verdicts.push(out.reg_values(2)[0]));
    assert_eq!(ci_verdicts, [1]);
}

/// `UPPER` of a non-nullable column introduces no NULL, so the `no_nulls` fast
/// path survives it; the parses and SUBSTR do not.
#[test]
fn string_nullability_classification() {
    let schema = schema_pk_strings(1, false);
    // The tail's last instruction is the result register; a column load takes
    // index 0, so the result is `tail.len()`.
    let no_nulls = |tail: Vec<LogicalInstr>| {
        let result = Reg(tail.len() as u16);
        let mut instrs = vec![LogicalInstr::LoadColStr { col: 1 }];
        instrs.extend(tail);
        scalar_prog(&schema, instrs, result, vec![]).prog().no_nulls
    };
    assert!(no_nulls(vec![LogicalInstr::StrCase { a: Reg(0), upper: true }]));
    // The window and reorder producers are total; REPLACE and the pads share
    // CONCAT's overflow NULL, and SPLIT_PART has its zero field index.
    assert!(no_nulls(vec![LogicalInstr::StrReverse { a: Reg(0) }]));
    assert!(no_nulls(vec![
        LogicalInstr::LoadConst { val: 1, unsigned: false },
        LogicalInstr::StrSide { src: Reg(0), n_reg: Reg(1), left: true }
    ]));
    assert!(!no_nulls(vec![LogicalInstr::StrReplace {
        s: Reg(0),
        from: Reg(0),
        to: Reg(0)
    }]));
    assert!(!no_nulls(vec![
        LogicalInstr::LoadConst { val: 1, unsigned: false },
        LogicalInstr::StrPad {
            s: Reg(0),
            n_reg: Reg(1),
            fill: Reg(0),
            left: false
        }
    ]));
    assert!(!no_nulls(vec![
        LogicalInstr::LoadConst { val: 1, unsigned: false },
        LogicalInstr::StrSplitPart { s: Reg(0), delim: Reg(0), n_reg: Reg(1) }
    ]));
    assert!(!no_nulls(vec![LogicalInstr::StrToInt { a: Reg(0), fi: FixedInt::I64 }]));
    // SUBSTR's only NULL is a negative length, so the two forms classify
    // differently: with a FOR clause it can produce one, without it cannot. The
    // window itself is total either way.
    let substr = |len_reg: Option<Reg>| {
        let mut tail = vec![LogicalInstr::LoadConst { val: 1, unsigned: false }];
        if len_reg.is_some() {
            tail.push(LogicalInstr::LoadConst { val: 3, unsigned: false });
        }
        tail.push(LogicalInstr::StrSubstr { src: Reg(0), start_reg: Reg(1), len_reg });
        no_nulls(tail)
    };
    assert!(substr(None), "no FOR clause writes no fail flag");
    assert!(!substr(Some(Reg(2))), "a FOR clause can name a negative length");
}

// ---------------------------------------------------------------------------
// Encoder / decoder drift — the two tables over one opcode space
// ---------------------------------------------------------------------------

/// The number of [`LogicalInstr`] variants. A new variant stops the match below
/// compiling until it is listed and counted.
impl LogicalInstr {
    pub(crate) const VARIANT_COUNT: usize = 46;

    /// Exhaustive by construction — the compile error on a new variant is the
    /// whole point, so this must never gain a `_` arm.
    #[allow(dead_code)]
    fn assert_every_variant_is_listed(&self) {
        use LogicalInstr as L;
        match *self {
            L::LoadColInt { .. }
            | L::LoadColFloat { .. }
            | L::LoadConst { .. }
            | L::IntArith { .. }
            | L::FloatArith { .. }
            | L::Cmp { .. }
            | L::FCmp { .. }
            | L::IntToFloat { .. }
            | L::FloatUnary { .. }
            | L::IntUnary { .. }
            | L::Calendar { .. }
            | L::FloatToInt { .. }
            | L::IntCast { .. }
            | L::FloatToF32 { .. }
            | L::IntMinMax2 { .. }
            | L::FloatMinMax2 { .. }
            | L::Select { .. }
            | L::LoadNull
            | L::BoolBinary { .. }
            | L::BoolNot { .. }
            | L::IsNull { .. }
            | L::IsNullReg { .. }
            | L::StrColConst { .. }
            | L::StrColCol { .. }
            | L::IntInSet { .. }
            | L::LoadColStr { .. }
            | L::LoadConstStr { .. }
            | L::LoadNullStr
            | L::StrSelect { .. }
            | L::StrCmp { .. }
            | L::StrLen { .. }
            | L::StrCase { .. }
            | L::StrSubstr { .. }
            | L::StrTrim { .. }
            | L::StrLike { .. }
            | L::StrConcat { .. }
            | L::IntToStr { .. }
            | L::FloatToStr { .. }
            | L::StrToInt { .. }
            | L::StrToFloat { .. }
            | L::StrSide { .. }
            | L::StrPos { .. }
            | L::StrReverse { .. }
            | L::StrReplace { .. }
            | L::StrPad { .. }
            | L::StrSplitPart { .. } => {}
        }
    }
}

/// Every instruction the decoder accepts over each opcode's selector word and a
/// few operand shapes. Operands are distinct so a swapped pair cannot
/// round-trip; 0/1 reach a flag word, `u32::MAX` SUBSTR's absent length.
fn decodable_words() -> Vec<([u32; INSTR_WORDS], LogicalInstr)> {
    let shapes = [[11, 12, 13], [11, 0, 13], [11, 1, 13], [11, 12, u32::MAX]];
    ExprOp::ALL
        .iter()
        .flat_map(|op| (0..=64u32).flat_map(move |sel| shapes.map(|[a, b, c]| [op.as_wire(), sel, a, b, c])))
        .filter_map(|w| {
            Some((
                w,
                LogicalProgram::decode_instr(gnitz_wire::as_le_bytes(&w).try_into().unwrap()).ok()?,
            ))
        })
        .collect()
}

/// `decode_instr ∘ to_wire == id` over everything the decoder accepts, and the
/// encoder emits the opcode and selector it was decoded from.
#[test]
fn every_decodable_instruction_round_trips_through_the_wire_form() {
    for (w, x) in decodable_words() {
        let back = x.to_wire();
        assert_eq!(back[..2], w[..2], "{x:?}: opcode and selector");
        let again = LogicalProgram::decode_instr(gnitz_wire::as_le_bytes(&back).try_into().unwrap());
        assert_eq!(again, Ok(x), "{x:?}: decode of its own encoding");
    }
}

/// The sweep reaches every [`LogicalInstr`] variant, and every selector family
/// ends inside it.
#[test]
fn the_sweep_decodes_every_variant() {
    assert_eq!(ExprOp::ALL.len(), LogicalInstr::VARIANT_COUNT);
    let seen: std::collections::HashSet<_> = decodable_words()
        .iter()
        .map(|(_, x)| std::mem::discriminant(x))
        .collect();
    assert_eq!(
        seen.len(),
        LogicalInstr::VARIANT_COUNT,
        "variants the decoder accepts a word for"
    );
    assert!(
        decodable_words().iter().all(|(w, _)| w[1] < 64),
        "a selector family reaches the sweep's last selector: widen the sweep"
    );
}

/// `LoadConst`'s `i64` is the one operand spanning two words: the split and the
/// join are inverses over the whole range, sign bit of the low half included.
#[test]
fn load_const_round_trips() {
    for v in [
        0i64,
        1,
        -1,
        i64::MIN,
        i64::MAX,
        0x0000_0001_0000_0000,
        -0x0000_0001_0000_0000,
    ] {
        let (a1, a2) = super::encode_load_const(v);
        assert_eq!(super::decode_load_const(a1, a2), v, "load_const round-trip for {v}");
    }
}

/// A filter's output is exactly one register sink: none, two, or a column sink
/// is no verdict.
#[test]
fn a_map_blob_is_not_accepted_as_a_filter() {
    let schema = schema_pk_ints(1, true);
    let outputs = [vec![], vec![Sink::Reg(Reg(0)), Sink::Reg(Reg(0))], vec![Sink::Col(1)]];
    for sinks in outputs {
        let mut b = crate::ExprBuilder::new();
        b.emit(LogicalInstr::LoadColInt { col: 1 });
        let blob = b.build(sinks.clone()).expect("a well-formed program").to_blob_bytes();
        let prog = LogicalProgram::from_blob(&blob).expect("the framing is well-formed");
        assert_eq!(
            prog.resolve_filter(&schema).err(),
            Some(ExprValidateErr::OutputRoleMismatch),
            "sinks {sinks:?}"
        );
    }
}

/// Two IN lists over the same values, in any order, share one pool entry and one
/// decoded pool.
#[test]
fn two_in_sets_over_one_pool_index_share_one_decoded_pool() {
    let schema = schema_pk_ints(2, true);
    let mut b = crate::ExprBuilder::new();
    let set = b.add_const_int_set(vec![3, 1, 2, 1]);
    let a = b.emit(LogicalInstr::LoadColInt { col: 1 });
    let a_in = b.emit(LogicalInstr::IntInSet { value_reg: a, set_idx: set });
    let c = b.emit(LogicalInstr::LoadColInt { col: 2 });
    let c_in = b.emit(LogicalInstr::IntInSet { value_reg: c, set_idx: set });
    let or = b.emit(LogicalInstr::BoolBinary { a: a_in, b: c_in, is_or: true });
    let prog = b.build(vec![Sink::Reg(or)]).expect("a well-formed program");
    assert_eq!(prog.const_strings().len(), 1, "the two lists intern to one pool entry");

    let ev = prog.resolve_filter(&schema).expect("resolves");
    assert_eq!(ev.prog().int_sets.len(), 1, "and to one decoded pool");
    assert_eq!(ev.prog().int_sets[0], vec![1, 2, 3]);
}

/// The scratch's i64 lanes are sized by the highest *non*-string register, so a
/// program whose registers are all strings allocates none — and one that
/// interleaves the two classes still reaches every scalar lane it names.
#[test]
fn scalar_lanes_covers_every_scalar_register_and_no_more() {
    let str_schema = schema_pk_strings(2, true);
    let lanes = |p: LogicalProgram, schema: &TestSchema| {
        let ev = p.resolve_scalar(schema).expect("resolves");
        (ev.prog().scalar_lanes, ev.prog().str_lanes)
    };

    // All-string: no scalar lane at all.
    let all_str = LogicalProgram::new(
        vec![
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::StrCase { a: Reg(0), upper: true },
        ],
        vec![Sink::Reg(Reg(1))],
        Vec::new(),
    );
    assert_eq!(lanes(all_str, &str_schema), (0, 2));

    // A register-free projection names neither class.
    let ev = LogicalProgram::copy_cols(&[1])
        .resolve_map(&str_schema, &schema_pk_strings(1, true))
        .expect("resolves");
    assert_eq!((ev.prog().scalar_lanes, ev.prog().str_lanes), (0, 0));

    // Interleaved: r0 string, r1 scalar (its length), r2 string, r3 scalar.
    // `scalar_lanes` must reach r3, not stop at r1.
    let interleaved = LogicalProgram::new(
        vec![
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::StrLen { a: Reg(0), chars: false },
            LogicalInstr::LoadColStr { col: 2 },
            LogicalInstr::StrLen { a: Reg(2), chars: false },
        ],
        vec![Sink::Reg(Reg(3))],
        Vec::new(),
    );
    assert_eq!(lanes(interleaved, &str_schema), (4, 3));
}

/// A map reproduces its input only when it copies every payload column into its
/// own slot, computes nothing, and the two schemas locate every column alike.
#[test]
fn is_identity_map_requires_an_in_order_copy_over_one_layout() {
    let schema = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::I64, true), (TypeCode::I64, false)],
        &[0],
    );
    assert!(LogicalProgram::copy_cols(&[1, 2]).is_identity_map(&schema, &schema));
    assert!(
        !LogicalProgram::copy_cols(&[2, 1]).is_identity_map(&schema, &schema),
        "reordered copy"
    );
    let computed = LogicalProgram::new(
        vec![LogicalInstr::LoadColInt { col: 2 }],
        vec![Sink::Col(1), Sink::Reg(Reg(0))],
        vec![],
    );
    assert!(!computed.is_identity_map(&schema, &schema), "computed sink");
    let other_payload = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::I64, true), (TypeCode::I32, false)],
        &[0],
    );
    assert!(
        !LogicalProgram::copy_cols(&[1, 2]).is_identity_map(&schema, &other_payload),
        "different payload type"
    );
    let other_pk = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::I64, false), (TypeCode::I64, false)],
        &[1],
    );
    assert!(
        !LogicalProgram::copy_cols(&[1, 2]).is_identity_map(&schema, &other_pk),
        "different PK layout"
    );
    // A PK that does not lead: the payload columns are 0 and 2.
    let mid_pk = TestSchema::new(
        &[(TypeCode::I64, true), (TypeCode::U64, false), (TypeCode::I64, false)],
        &[1],
    );
    assert!(LogicalProgram::copy_cols(&[0, 2]).is_identity_map(&mid_pk, &mid_pk));
    assert!(
        !LogicalProgram::copy_cols(&[1, 2]).is_identity_map(&mid_pk, &mid_pk),
        "a PK column copied into a payload slot"
    );
    // A PK-only schema: nothing to copy, so the empty program is its identity.
    let pk_only = TestSchema::new(&[(TypeCode::U64, false)], &[0]);
    assert!(LogicalProgram::copy_cols(&[]).is_identity_map(&pk_only, &pk_only));
}

/// The three shapes [`NullPerm`] collapses a copy list to, and the window each
/// writes. A source that cannot carry a set bit contributes no pair.
#[test]
fn null_perm_collapses_a_copy_list_to_three_shapes() {
    let payload = |slot: u8| ColumnLocator::Payload { slot, size: 8, type_code: TypeCode::I64 };
    let pk = ColumnLocator::Pk {
        byte_off: 0,
        size: 8,
        type_code: TypeCode::U64,
    };
    let copy = |src, slot| crate::ColCopy { src, slot, width: 8 };
    // Payload slots 0 and 2 admit NULL; slot 1 is NOT NULL.
    let nullable = 0b101;

    // Only sources with no bit of their own: nothing to move.
    let zero = NullPerm::new(&[copy(pk, 0), copy(payload(1), 1)], nullable);
    assert!(matches!(zero, NullPerm::Zero));

    // Every bit stays in its slot — one AND against the kept slots.
    let mask = NullPerm::new(
        &[copy(payload(0), 0), copy(payload(1), 1), copy(payload(2), 2)],
        nullable,
    );
    assert!(matches!(mask, NullPerm::Mask(0b101)));

    // Slot 2 -> slot 0 moves a bit, so the whole list permutes.
    let perm = NullPerm::new(&[copy(payload(2), 0), copy(payload(0), 1)], nullable);
    assert!(matches!(&perm, NullPerm::Permute(p) if p == &[(2u8, 0u8), (0, 1)]));

    // Two source rows, both bits set; the destination starts non-zero, so a
    // window an arm left alone reads as a stale bit rather than as a zero.
    let mut src = [0u8; 16];
    gnitz_wire::write_u64_le(&mut src, 0, 0b101);
    gnitz_wire::write_u64_le(&mut src, 8, 0b100);
    for (perm, want) in [
        (zero, [0u64, 0]),
        (mask, [0b101, 0b100]),
        // Row 0: slot 2 -> 0 and slot 0 -> 1. Row 1: only slot 2 is set.
        (perm, [0b11, 0b1]),
    ] {
        let mut dst = [0xAAu8; 24];
        perm.write_rows(&src, 0, &mut dst, 1, 2);
        assert_eq!(
            gnitz_wire::read_u64_le(&dst, 0),
            u64::from_le_bytes([0xAA; 8]),
            "row 0 is outside the window"
        );
        assert_eq!(
            [gnitz_wire::read_u64_le(&dst, 8), gnitz_wire::read_u64_le(&dst, 16)],
            want
        );
    }
}
