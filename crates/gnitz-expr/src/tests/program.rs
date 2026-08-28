// 3.14 is a deliberate float-bit-pattern test fixture, not an approximation
// of PI meant to be replaced with std::f64::consts::PI.
#![allow(clippy::approx_constant)]

use gnitz_wire::{type_code, ExprOp, TrimMode, TypeCode};
use std::collections::BTreeSet;

// `tests/program.rs` is `#[path]`-attached to `program.rs`, so `super` is that
// module — one import line rather than three spellings of it.
use super::{analyze, ColKind, FloatUnaryOp, IntUnaryOp, ProgramFacts};
use crate::batch::{decode_f64, encode_f64};
use crate::test_support::{
    filter_prog, is_not_null_op, is_null_op, make_int_view, make_string_view, scalar_prog, schema_pk_ints,
    schema_pk_strings, TestSchema, TestView,
};
use crate::{CmpOp, Evaluator, ExprValidateErr, Instr, LogicalInstr, LogicalProgram, StrOp};

/// Run `ev` at m=1 over `(mb, row)` and report
/// `(result value or NULL, EMIT null mask, EMIT values)` — the map-side read,
/// where each EMIT'd register lands in an output payload slot and a NULL
/// register stores 0 with its output bit set.
fn eval_with_emit(ev: &Evaluator, mb: &TestView, row: usize) -> (Option<i64>, u64, Vec<i64>) {
    let mut emit_vals: Vec<i64> = Vec::new();
    let mut emit_null_mask: u64 = 0;
    ev.eval_morsels(mb, row, 1, |_, out| {
        for &(src, payload) in ev.scalar_emits() {
            let mut is_null = false;
            out.for_each_null_row(src as usize, |_| is_null = true);
            if is_null {
                emit_vals.push(0);
                emit_null_mask |= 1u64 << payload;
            } else {
                emit_vals.push(out.reg_values(src as usize)[0]);
            }
        }
    });
    (ev.eval_row(mb, row), emit_null_mask, emit_vals)
}

#[test]
fn int_add_and_negate() {
    let schema = schema_pk_ints(2, true);
    let mb = make_int_view(&schema, &[(1, 0, &[10, 3])]);

    // ADD: 10 + 3 = 13
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::IntAdd { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let val = prog.eval_row(&mb, 0).expect("not NULL");
    assert_eq!(val, 13);

    // NEG: -10
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::IntUnary {
            op: IntUnaryOp::Neg,
            dst: 1,
            a: 0,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 2, 1, vec![]);
    let val = prog.eval_row(&mb, 0).expect("not NULL");
    assert_eq!(val, -10);
}

#[test]
fn float_add() {
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, type_code::F64, type_code::F64]);
    // Store floats as i64 bits
    let a_bits = encode_f64(3.14);
    let b_bits = encode_f64(2.0);
    let mb = make_int_view(&schema, &[(1, 0, &[a_bits, b_bits])]);

    // FLOAT_ADD: 3.14 + 2.0
    let instrs = vec![
        LogicalInstr::LoadColFloat { dst: 0, col: 1 },
        LogicalInstr::LoadColFloat { dst: 1, col: 2 },
        LogicalInstr::FloatAdd { dst: 2, a: 0, b: 1 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let val = prog.eval_row(&mb, 0).expect("not NULL");
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
        dst: 0,
        val: ((1i64) << 32) | (2i64 & 0xFFFF_FFFF),
    }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
    let val = prog.eval_row(&mb, 0).expect("not NULL");
    assert_eq!(val, (1i64 << 32) | 2);

    // Test negative constant: -1 (the wire low/high split is reconstructed by
    // `from_wire`; the typed instruction carries the full i64 value directly).
    let instrs = vec![LogicalInstr::LoadConst { dst: 0, val: -1 }];
    let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
    let val = prog.eval_row(&mb, 0).expect("not NULL");
    assert_eq!(val, -1);
}

#[test]
fn int_to_float_widens_the_register() {
    let schema = schema_pk_ints(1, true);
    let mb = make_int_view(&schema, &[(1, 0, &[42])]);

    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::IntToFloat { dst: 1, a: 0 },
    ];
    let prog = scalar_prog(&schema, instrs, 2, 1, vec![]);
    let val = prog.eval_row(&mb, 0).expect("not NULL");
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
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, type_code::U64]);
    // u64::MAX is -1 as i64, so every signed reading below is a different number.
    let mb = make_int_view(&schema, &[(1, 0, &[u64::MAX as i64])]);
    let run = |instrs: Vec<LogicalInstr>, num_regs, result| {
        scalar_prog(&schema, instrs, num_regs, result, vec![])
            .eval_row(&mb, 0)
            .expect("not NULL")
    };
    let load = LogicalInstr::LoadColInt { dst: 0, col: 1 };
    let two = LogicalInstr::LoadConst { dst: 1, val: 2 };

    assert_eq!(
        run(vec![load, two, LogicalInstr::IntDiv { dst: 2, a: 0, b: 1 }], 3, 2),
        i64::MAX,
        "u64::MAX / 2 unsigned; signed -1/2 would be 0",
    );
    assert_eq!(
        run(vec![load, two, LogicalInstr::IntMod { dst: 2, a: 0, b: 1 }], 3, 2),
        1,
        "u64::MAX % 2 unsigned; signed -1 % 2 would be -1",
    );
    assert_eq!(
        decode_f64(run(vec![load, LogicalInstr::IntToFloat { dst: 1, a: 0 }], 2, 1)),
        u64::MAX as f64,
        "unsigned cast is ~1.8e19; signed would be -1.0",
    );
}

#[test]
fn emit_writes_each_named_output_slot() {
    let schema = schema_pk_ints(2, true);
    let mb = make_int_view(&schema, &[(1, 0, &[10, 20])]);

    // Compute col1 + col2, EMIT to payload col 0
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::IntAdd { dst: 2, a: 0, b: 1 },
        LogicalInstr::Emit { src: 2, out: 0 }, // emit r2 to payload col 0
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);

    let (val, mask, emit_vals) = eval_with_emit(&prog, &mb, 0);
    assert_eq!(val, Some(30)); // 10 + 20
    assert_eq!(mask, 0);
    assert_eq!(emit_vals[0], 30);
}

/// `r5 = (col1 > 1) AND (col2 > 1)` over `schema_pk_ints(2, _)`, using registers
/// 0-5. The classification tests below and the EMIT test share it.
fn conjunction_over_two_cols() -> Vec<LogicalInstr> {
    vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 1 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        },
        LogicalInstr::LoadColInt { dst: 3, col: 2 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 4,
            a: 3,
            b: 1,
        },
        LogicalInstr::BoolAnd { dst: 5, a: 2, b: 4 },
    ]
}

/// EMIT counts as a non-bool read of its source, so a boolean it ships is not
/// `bit_only` and its producer writes `regs`. Were the source missing from the
/// operand table, BOOL_AND would skip the unpack and EMIT would read a stale
/// lane.
#[test]
fn an_emitted_boolean_lands_in_regs() {
    // Nullable payload columns: under `no_nulls`, BOOL_AND writes `regs`
    // whatever `bit_only` says, which would make the value assertion vacuous.
    let schema = schema_pk_ints(2, true);
    let mb = make_int_view(&schema, &[(1, 0, &[10, 20])]);
    let mut instrs = conjunction_over_two_cols();
    instrs.push(LogicalInstr::Emit { src: 5, out: 0 });
    let prog = scalar_prog(&schema, instrs, 6, 5, vec![]);
    assert!(
        !prog.prog.no_nulls,
        "nullable columns keep the test off the no_nulls arm"
    );
    assert!(!prog.prog.is_bit_only(5), "a register read by EMIT is not bit_only");
    let (_, _, emit_vals) = eval_with_emit(&prog, &mb, 0);
    assert_eq!(emit_vals[0], 1, "EMIT must ship the AND value, not a stale lane");
}

/// A NULL result reaching EMIT sets the output slot's null bit and stores 0 —
/// the one place a NULL row's value half is defined, since the output column
/// must hold something.
#[test]
fn emit_of_a_null_result_sets_the_slot_bit_and_stores_zero() {
    let schema = schema_pk_ints(2, true);
    let mb = make_int_view(&schema, &[(1, 0, &[10, 3])]);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::IntDiv { dst: 2, a: 0, b: 1 },
        LogicalInstr::Emit { src: 2, out: 0 },
    ];
    let prog = scalar_prog(&schema, instrs, 3, 2, vec![]);
    let (_, mask, emit_vals) = eval_with_emit(&prog, &mb, 0);
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
    // One COPY_COL instruction: copy col 1 → payload 0 (source type derived in resolve)
    let instrs = vec![LogicalInstr::CopyCol { src_col: 1, out: 0 }];
    let prog = || LogicalProgram::new(instrs.clone(), 0, 0, vec![]);
    assert_eq!(
        prog().resolve_scalar(&schema).err(),
        Some(ExprValidateErr::ResultRegRequired),
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
    type Case = (usize, &'static [u8], u64, &'static [i64], &'static [(Option<u8>, i64)]);
    let cases: [Case; 2] = [
        (
            0,
            &[type_code::U64, type_code::I64, type_code::I64],
            42,
            &[10, 20],
            &[(None, 42), (Some(0), 10), (Some(1), 20)],
        ),
        (
            1,
            &[type_code::I64, type_code::U64, type_code::I64],
            99,
            &[5, 7],
            &[(Some(0), 5), (None, 99), (Some(1), 7)],
        ),
    ];

    for (pk_index, cols, pk_val, payloads, want) in cases {
        let schema = TestSchema::with_pk_at(pk_index, cols);
        let mb = make_int_view(&schema, &[(pk_val, 0, payloads)]);
        for (ci, &(want_slot, want_val)) in want.iter().enumerate() {
            let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: ci as u32 }];
            let prog = scalar_prog(&schema, instrs, 1, 0, vec![]);
            let got_slot = match prog.prog.instrs[0] {
                Instr::LoadPk { .. } => None,
                Instr::LoadPayloadInt { pi, .. } => Some(pi),
                ref other => panic!("pk_index={pk_index} col {ci} resolved to {other:?}"),
            };
            assert_eq!(got_slot, want_slot, "pk_index={pk_index} col {ci}: wrong slot");
            assert_eq!(
                prog.eval_row(&mb, 0),
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
        (StrOp::Eq, "EQ_CONST"),
        (StrOp::Lt, "LT_CONST"),
        (StrOp::Le, "LE_CONST"),
    ] {
        let instrs = vec![LogicalInstr::StrColConst {
            op: *op,
            dst: 0,
            col: 1,
            const_idx: 0,
        }];
        let prog = scalar_prog(&nullable_schema, instrs.clone(), 1, 0, vec![b"x".to_vec()]);
        assert!(!prog.prog.no_nulls, "{_name}: nullable col1 must yield no_nulls=false");
        let prog = scalar_prog(&nonnull_schema, instrs, 1, 0, vec![b"x".to_vec()]);
        assert!(
            prog.prog.no_nulls,
            "{_name}: non-nullable col1 must yield no_nulls=true"
        );
    }

    // STR_COL_*_COL — both operands matter
    for (op, _name) in &[(StrOp::Eq, "EQ_COL"), (StrOp::Lt, "LT_COL"), (StrOp::Le, "LE_COL")] {
        let instrs = vec![LogicalInstr::StrColCol {
            op: *op,
            dst: 0,
            col_a: 1,
            col_b: 2,
        }];
        let prog = scalar_prog(&nullable_schema, instrs.clone(), 1, 0, vec![]);
        assert!(
            !prog.prog.no_nulls,
            "{_name}: nullable operands must yield no_nulls=false"
        );
        let prog = scalar_prog(&nonnull_schema, instrs, 1, 0, vec![]);
        assert!(
            prog.prog.no_nulls,
            "{_name}: non-nullable operands must yield no_nulls=true"
        );
    }
}

// ---------------------------------------------------------------------------
// SELECT / LOAD_NULL (SQL CASE blend)
// ---------------------------------------------------------------------------

/// `CASE WHEN cond THEN 42 END` (implicit ELSE NULL) lowers to
/// `select(cond, 42, load_null())`: truthy → 42, else → NULL.
#[test]
fn load_null_else_branch_eval() {
    let schema = schema_pk_ints(1, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // cond
        LogicalInstr::LoadConst { dst: 1, val: 42 }, // a
        LogicalInstr::LoadNull { dst: 2 },           // else NULL
        LogicalInstr::Select {
            dst: 3,
            cond: 0,
            a: 1,
            b: 2,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 4, 3, vec![]);

    // cond truthy → 42.
    let batch = make_int_view(&schema, &[(1, 0, &[5])]);
    let v = prog.eval_row(&batch, 0).expect("not NULL");
    assert_eq!(v, 42);

    // cond false → else NULL.
    let batch = make_int_view(&schema, &[(1, 0, &[0])]);
    let n = prog.eval_row(&batch, 0).is_none();
    assert!(n, "false cond → else NULL");

    // cond NULL → else NULL.
    let batch = make_int_view(&schema, &[(1, 1, &[9])]);
    let n = prog.eval_row(&batch, 0).is_none();
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
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadNull { dst: 1 },
        LogicalInstr::Select {
            dst: 2,
            cond: 0,
            a: 0,
            b: 1,
        },
    ];
    let prog = scalar_prog(&nonnull_schema, instrs, 3, 2, vec![]);
    assert!(!prog.prog.no_nulls, "LoadNull must force the nullable path");
}

/// A Select over only NOT NULL branches (no LoadNull) stays strictly-non-nullable:
/// Select copies branch values and adds no NULL of its own.
#[test]
fn select_is_non_nullable_when_both_branches_are() {
    let nonnull_schema = schema_pk_ints(3, false);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::LoadColInt { dst: 2, col: 3 },
        LogicalInstr::Select {
            dst: 3,
            cond: 0,
            a: 1,
            b: 2,
        },
    ];
    let prog = scalar_prog(&nonnull_schema, instrs, 4, 3, vec![]);
    assert!(
        prog.prog.no_nulls,
        "Select over NOT NULL branches must stay strictly-non-nullable"
    );
}

/// U64-ness flows through Select (U64 if either branch is U64), so a downstream
/// ordered compare on the CASE result picks the unsigned variant.
#[test]
fn select_propagates_u64_tracking_to_its_reader() {
    // Schema: pk(u64), u64col(U64), i64col(I64).
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, type_code::U64, type_code::I64]);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 2 }, // cond (i64)
        LogicalInstr::LoadColInt { dst: 1, col: 1 }, // a (U64)
        LogicalInstr::LoadColInt { dst: 2, col: 2 }, // b (i64)
        LogicalInstr::Select {
            dst: 3,
            cond: 0,
            a: 1,
            b: 2,
        },
        LogicalInstr::LoadConst { dst: 4, val: 100 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 5,
            a: 3,
            b: 4,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 6, 5, vec![]);
    // Found by shape, not by position: which index resolution lands the compare
    // at is an internal detail, and three sibling tests already read it this way.
    let cmp = prog.prog.instrs.iter().find_map(|i| match i {
        Instr::Cmp { op, signed, .. } => Some((*op, *signed)),
        _ => None,
    });
    assert_eq!(
        cmp,
        Some((CmpOp::Gt, false)),
        "Select carrying a U64 branch must make the downstream compare unsigned"
    );
}

/// SELECT feeding BOOL_AND: `cond` is a bool_input, the select result feeds the
/// AND as a bool_input, and the select dst (a value register) is never bit_only.
#[test]
fn select_classification_of_cond_and_result() {
    let schema = schema_pk_ints(4, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // cond
        LogicalInstr::LoadColInt { dst: 1, col: 2 }, // a
        LogicalInstr::LoadColInt { dst: 2, col: 3 }, // b
        LogicalInstr::Select {
            dst: 3,
            cond: 0,
            a: 1,
            b: 2,
        },
        LogicalInstr::LoadColInt { dst: 4, col: 4 }, // other bool
        LogicalInstr::BoolAnd { dst: 5, a: 3, b: 4 },
    ];
    let ProgramFacts {
        bit_only, bool_input, ..
    } = analyze(&instrs, &schema, 5, true);
    // The same program must also be a legal filter.
    filter_prog(&schema, instrs, 6, 5, vec![]);
    assert_ne!(bool_input & (1 << 0), 0, "cond is read as a bool_input");
    assert_ne!(bool_input & (1 << 3), 0, "select result feeds BOOL_AND as bool_input");
    assert_eq!(bit_only & (1 << 3), 0, "select dst is a value register, never bit_only");
}

/// SELECT blends branch values, so its destination is a `WriteAs::Value`. As a
/// `WriteAs::Bool` it would be `bit_only` here — read by nobody, and
/// `is_filter = false` keeps `result_reg` out of `bool_input` — and `eval_row`
/// would return the packed truth bit instead of the branch value.
#[test]
fn a_select_result_reads_back_as_a_value() {
    // The nullable column load turns `no_nulls` off; `eval_row` consults
    // `bit_only` only on the nullable arm.
    let schema = schema_pk_ints(1, true);
    let mb = make_int_view(&schema, &[(1, 0, &[1])]);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // cond, truthy
        LogicalInstr::LoadConst { dst: 1, val: 5 },
        LogicalInstr::LoadConst { dst: 2, val: 7 },
        LogicalInstr::Select {
            dst: 3,
            cond: 0,
            a: 1,
            b: 2,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 4, 3, vec![]);
    assert!(!prog.prog.no_nulls, "the nullable load keeps this off the no_nulls arm");
    let val = prog.eval_row(&mb, 0).expect("not NULL");
    assert_eq!(val, 5, "SELECT returns the chosen branch value, not a truth bit");
}

/// `dst` aliasing any of `cond`/`a`/`b` violates the SELECT anti-alias rule that
/// makes `regs_split`'s raw split borrows sound.
#[test]
#[should_panic(expected = "RegisterAliasing")]
fn select_rejects_a_dst_aliasing_an_operand() {
    // `LogicalProgram::new` validates aliasing before any schema is consulted.
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::Select {
            dst: 0,
            cond: 0,
            a: 0,
            b: 0,
        }, // dst == cond
    ];
    let _ = LogicalProgram::new(instrs, 1, 0, vec![]);
}

/// Nested SELECT — `CASE WHEN c1 THEN v1 WHEN c2 THEN v2 ELSE v3 END` lowered as
/// `select(c1, v1, select(c2, v2, v3))` — must pick the first truthy WHEN.
#[test]
fn nested_selects_evaluate_inside_out() {
    // Schema: pk, c1, c2, v1, v2, v3.
    let schema = schema_pk_ints(5, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // c1
        LogicalInstr::LoadColInt { dst: 1, col: 2 }, // c2
        LogicalInstr::LoadColInt { dst: 2, col: 3 }, // v1
        LogicalInstr::LoadColInt { dst: 3, col: 4 }, // v2
        LogicalInstr::LoadColInt { dst: 4, col: 5 }, // v3
        LogicalInstr::Select {
            dst: 5,
            cond: 1,
            a: 3,
            b: 4,
        }, // inner = c2 ? v2 : v3
        LogicalInstr::Select {
            dst: 6,
            cond: 0,
            a: 2,
            b: 5,
        }, // outer = c1 ? v1 : inner
    ];
    let prog = scalar_prog(&schema, instrs, 7, 6, vec![]);
    // payload cols: c1, c2, v1=10, v2=20, v3=30.
    // c1 truthy → v1.
    let batch = make_int_view(&schema, &[(1, 0, &[1, 1, 10, 20, 30])]);
    let v = prog.eval_row(&batch, 0).expect("not NULL");
    assert_eq!(v, 10, "c1 truthy → v1");
    // c1 false, c2 truthy → v2.
    let batch = make_int_view(&schema, &[(1, 0, &[0, 1, 10, 20, 30])]);
    let v = prog.eval_row(&batch, 0).expect("not NULL");
    assert_eq!(v, 20, "c1 false, c2 truthy → v2");
    // both false → v3.
    let batch = make_int_view(&schema, &[(1, 0, &[0, 0, 10, 20, 30])]);
    let v = prog.eval_row(&batch, 0).expect("not NULL");
    assert_eq!(v, 30, "both false → else v3");
}

// ---------------------------------------------------------------------------
// Expr-program validation (ExprValidateErr): one crafted input per vector.
// Wire code is flat u32 quads `[opcode, w1, w2, w3]`; several opcodes read
// `w2`/`w3` as full u32 (col / const_idx / out), not the loop's truncated u16.
// ---------------------------------------------------------------------------

/// `from_wire`'s `Ok` value (`LogicalProgram`) is not `Debug`/`PartialEq`, so
/// extract the `Err` to compare the variant directly.
fn wire_err(r: Result<LogicalProgram, ExprValidateErr>) -> ExprValidateErr {
    match r {
        Ok(_) => panic!("expected Err, got a valid program"),
        Err(e) => e,
    }
}

#[test]
fn from_wire_rejects_an_unknown_opcode() {
    // 0 and u32::MAX are holes permanently. Deliberately NOT "one past the
    // current maximum": that couples the test to every opcode addition while
    // adding no coverage a new opcode's own decode test does not already give.
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[0, 0, 0, 0], 1, 0, vec![])),
        ExprValidateErr::UnknownOpcode(0)
    );
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[u32::MAX, 0, 0, 0], 1, 0, vec![])),
        ExprValidateErr::UnknownOpcode(u32::MAX)
    );
    // A valid opcode lowers (control): LOAD_COL_INT dst0 col0.
    assert!(LogicalProgram::from_wire(&[1, 0, 0, 0], 1, 0, vec![]).is_ok());
}

// The rendering the planner's `Unsupported` and the engine's `CREATE VIEW`
// rejection both print: one wording for both. The register cap and the column
// mismatch are the variants a working query can newly hit, so each gets a
// sentence rather than a struct dump — `ColKind`'s fields are private, so its
// `Debug` form would name them to a client who cannot act on either.
#[test]
fn validate_err_display_names_the_register_limit() {
    assert_eq!(
        ExprValidateErr::TooManyRegs(66).to_string(),
        format!(
            "expression needs 66 registers; the limit is {} — split the predicate",
            crate::MAX_REGS
        )
    );
    // Both axes of the requirement: its type, and — since a PK column is exactly
    // what a client is likely to have named — whether a PK column is admissible.
    let mismatch = |want| {
        ExprValidateErr::ColKindMismatch {
            col: 2,
            type_code: type_code::U64,
            want,
        }
        .to_string()
    };
    assert_eq!(
        mismatch(ColKind::FIXED_INT.describe()),
        "column 2 (type code 8) cannot be used here; this operator needs a fixed-width integer column"
    );
    assert_eq!(
        mismatch(ColKind::FLOAT.describe()),
        "column 2 (type code 8) cannot be used here; \
         this operator needs a floating-point column that is not part of the primary key"
    );
    // Everything else is an internal-shape violation with no user action:
    // rendered as its Debug form.
    assert_eq!(
        ExprValidateErr::ColOutOfRange { col: 7, num_columns: 3 }.to_string(),
        "ColOutOfRange { col: 7, num_columns: 3 }"
    );
}

#[test]
fn from_wire_rejects_a_bad_register_file() {
    // num_regs over the 64-register limit.
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[], 65, 0, vec![])),
        ExprValidateErr::TooManyRegs(65)
    );
    // result_reg out of range (>= num_regs).
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[], 3, 3, vec![])),
        ExprValidateErr::ResultRegOutOfRange {
            result_reg: 3,
            num_regs: 3,
        }
    );
}

#[test]
fn from_wire_rejects_an_out_of_range_register() {
    // IntAdd dst0 a5 b1 with num_regs=2: operand `a=5` is out of range.
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[4, 0, 5, 1], 2, 0, vec![])),
        ExprValidateErr::RegOutOfRange { reg: 5, num_regs: 2 }
    );
}

#[test]
fn from_wire_rejects_register_aliasing() {
    // IntAdd dst0 a0 b1: dst aliases a source — breaks `regs_split`'s split borrows.
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[4, 0, 0, 1], 2, 0, vec![])),
        ExprValidateErr::RegisterAliasing { dst: 0, reg: 0 }
    );
}

#[test]
fn from_wire_rejects_an_out_of_range_const_idx() {
    // STR_COL_EQ_CONST (40) dst0 col0 const_idx9, pool of length 1.
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[40, 0, 0, 9], 1, 0, vec![b"x".to_vec()])),
        ExprValidateErr::ConstIdxOutOfRange { const_idx: 9, n: 1 }
    );
}

#[test]
fn validate_rejects_an_out_of_range_column() {
    // LOAD_COL_INT (1) col=200 against a 3-column schema.
    let s3 = schema_pk_ints(2, true);
    let prog = LogicalProgram::from_wire(&[1, 0, 200, 0], 1, 0, vec![]).unwrap();
    assert_eq!(
        prog.validate(Some(&s3), None),
        Err(ExprValidateErr::ColOutOfRange {
            col: 200,
            num_columns: 3,
        })
    );
}

#[test]
fn validate_rejects_a_pk_column_for_a_payload_only_opcode() {
    // A PK at column 0 (non-nullable) plus a payload string column.
    let s_pk = schema_pk_strings(1, true);
    // Each payload-only opcode that routes a PK column to the pi=255 sentinel.
    let cases: &[(&[u32], Vec<Vec<u8>>)] = &[
        (&[2, 0, 0, 0], vec![]),               // LOAD_COL_FLOAT col0
        (&[30, 0, 0, 0], vec![]),              // IS_NULL col0
        (&[40, 0, 0, 0], vec![b"x".to_vec()]), // STR_COL_EQ_CONST col0
        (&[43, 0, 0, 1], vec![]),              // STR_COL_EQ_COL col_a=0
    ];
    for (code, consts) in cases {
        let prog = LogicalProgram::from_wire(code, 1, 0, consts.clone()).unwrap();
        assert_eq!(
            prog.validate(Some(&s_pk), None),
            Err(ExprValidateErr::ColNotPayload { col: 0 }),
            "opcode {} must reject a PK source column",
            code[0]
        );
    }
}

#[test]
fn validate_rejects_a_wide_column_register_load() {
    // LOAD_COL_INT on a 16-byte U128 column: a register holds 8 bytes, so the
    // load is rejected at validation rather than silently truncating.
    let schema = TestSchema::new(&[(type_code::U64, false), (type_code::U128, true)], &[0]);
    let prog = LogicalProgram::from_wire(&[1, 0, 1, 0], 1, 0, vec![]).unwrap();
    assert_eq!(
        prog.validate(Some(&schema), None),
        Err(ExprValidateErr::ColKindMismatch {
            col: 1,
            type_code: type_code::U128,
            want: ColKind::FIXED_INT.describe(),
        })
    );
}

/// `LoadColInt`'s kernel decodes little-endian integer bytes; a width test alone
/// let two shapes through that it cannot decode — a float column (whose bit
/// pattern would become an integer; both widths take different kernel arms) and
/// an unknown type code, which `wire_stride` maps to 8 by design.
#[test]
fn load_col_int_requires_a_fixed_int_column() {
    for tc in [type_code::F64, type_code::F32, 200, type_code::STRING] {
        let schema = TestSchema::with_pk_at(0, &[type_code::U64, tc]);
        let prog = LogicalProgram::from_wire(&[1, 0, 1, 0], 1, 0, vec![]).unwrap();
        assert_eq!(
            prog.validate(Some(&schema), None),
            Err(ExprValidateErr::ColKindMismatch {
                col: 1,
                type_code: tc,
                want: ColKind::FIXED_INT.describe(),
            }),
            "LOAD_COL_INT must reject type code {tc}"
        );
    }
    // Every fixed-width integer is loadable, PK column included.
    for tc in [
        type_code::U8,
        type_code::I8,
        type_code::U16,
        type_code::I16,
        type_code::U32,
        type_code::I32,
        type_code::U64,
        type_code::I64,
    ] {
        let schema = TestSchema::with_pk_at(0, &[type_code::U64, tc]);
        let prog = LogicalProgram::from_wire(&[1, 0, 1, 0], 1, 0, vec![]).unwrap();
        assert_eq!(prog.validate(Some(&schema), None), Ok(0), "type code {tc}");
    }
}

/// `LoadColFloat`'s kernel branches on width alone: a 1- or 2-byte integer
/// column makes it slice an 8-byte stride out of a narrower region (a panic),
/// and every other non-float width is silent garbage.
#[test]
fn load_col_float_requires_a_float_column() {
    let case = |tc: u8| {
        let schema = TestSchema::with_pk_at(0, &[type_code::U64, tc]);
        LogicalProgram::from_wire(&[2, 0, 1, 0], 1, 0, vec![])
            .unwrap()
            .validate(Some(&schema), None)
    };
    for tc in [type_code::U16, type_code::U64, type_code::STRING] {
        assert_eq!(
            case(tc),
            Err(ExprValidateErr::ColKindMismatch {
                col: 1,
                type_code: tc,
                want: ColKind::FLOAT.describe(),
            }),
            "LOAD_COL_FLOAT must reject type code {tc}"
        );
    }
    assert_eq!(case(type_code::F32), Ok(0));
    assert_eq!(case(type_code::F64), Ok(0));
}

/// The string compares read 16-byte German-string cells with `col_data(pi, 16)`,
/// which over-reads (and eventually runs off) a narrower column's region.
#[test]
fn str_opcodes_require_a_german_string_column() {
    let schema = |a: u8, b: u8| TestSchema::with_pk_at(0, &[type_code::U64, a, b]);
    // STR_COL_EQ_CONST (40) col=1.
    let vs_const = |a: u8| {
        LogicalProgram::from_wire(&[40, 0, 1, 0], 1, 0, vec![b"x".to_vec()])
            .unwrap()
            .validate(Some(&schema(a, type_code::STRING)), None)
    };
    assert_eq!(
        vs_const(type_code::U64),
        Err(ExprValidateErr::ColKindMismatch {
            col: 1,
            type_code: type_code::U64,
            want: ColKind::GERMAN_STRING.describe(),
        })
    );
    assert_eq!(vs_const(type_code::STRING), Ok(0));
    assert_eq!(vs_const(type_code::BLOB), Ok(0));

    // STR_COL_EQ_COL (43) col_a=1 col_b=2 — both operands are checked.
    let vs_col = |a: u8, b: u8| {
        LogicalProgram::from_wire(&[43, 0, 1, 2], 1, 0, vec![])
            .unwrap()
            .validate(Some(&schema(a, b)), None)
    };
    assert_eq!(
        vs_col(type_code::U64, type_code::STRING),
        Err(ExprValidateErr::ColKindMismatch {
            col: 1,
            type_code: type_code::U64,
            want: ColKind::GERMAN_STRING.describe(),
        })
    );
    assert_eq!(
        vs_col(type_code::STRING, type_code::U64),
        Err(ExprValidateErr::ColKindMismatch {
            col: 2,
            type_code: type_code::U64,
            want: ColKind::GERMAN_STRING.describe(),
        })
    );
    assert_eq!(vs_col(type_code::STRING, type_code::BLOB), Ok(0));
}

/// `IsNull`/`IsNotNull` read the NULL bitmap and nothing else, so any payload
/// column is legitimate — splitting them out of the shared payload-only arm must
/// not have narrowed them to a type class.
#[test]
fn null_tests_accept_any_payload_column() {
    for tc in [type_code::U128, type_code::STRING, type_code::F32] {
        let schema = TestSchema::with_pk_at(0, &[type_code::U64, tc]);
        for op in [30u32, 31] {
            let prog = LogicalProgram::from_wire(&[op, 0, 1, 0], 1, 0, vec![]).unwrap();
            assert_eq!(prog.validate(Some(&schema), None), Ok(0), "opcode {op} type {tc}");
        }
    }
}

/// `copy_column` byte-copies at equal width and otherwise widens a narrower
/// integer into a wider slot: no narrowing, no representation change. The
/// widening set is exactly the cross-width set-op coercion the client emits.
#[test]
fn copy_col_admits_only_a_widening_destination() {
    // in: [U64 PK, <src>]; out: [U64 PK, <dst>] — one payload slot each.
    let pair = |src: u8, dst: u8| {
        let in_schema = TestSchema::with_pk_at(0, &[type_code::U64, src]);
        let out_schema = TestSchema::with_pk_at(0, &[type_code::U64, dst]);
        // COPY_COL (34) src_col=1 out=0.
        LogicalProgram::from_wire(&[34, 0, 1, 0], 0, 0, vec![])
            .unwrap()
            .validate(Some(&in_schema), Some(&out_schema))
    };
    for (src, dst) in [
        (type_code::I64, type_code::I64),
        (type_code::STRING, type_code::STRING),
        (type_code::BLOB, type_code::BLOB),
        (type_code::U128, type_code::U128),
        (type_code::F64, type_code::F64),
        (type_code::U32, type_code::U64),
        (type_code::U32, type_code::I64),
        (type_code::U8, type_code::I16),
    ] {
        assert_eq!(pair(src, dst), Ok(0), "{src} -> {dst} must be accepted");
    }
    for (src, dst) in [
        (type_code::STRING, type_code::U64),
        (type_code::U64, type_code::STRING),
        (type_code::U128, type_code::U64),
        (type_code::U64, type_code::F64),
        (type_code::F32, type_code::F64),
        (type_code::U64, type_code::I64),
    ] {
        assert_eq!(
            pair(src, dst),
            Err(ExprValidateErr::CopyTypeMismatch {
                col: 1,
                src_tc: src,
                out: 0,
                out_tc: dst,
            }),
            "{src} -> {dst} must be rejected"
        );
    }
    // A PK source into a payload slot of the same type is a copy, not a promotion.
    let in_pk = TestSchema::with_pk_at(0, &[type_code::U64, type_code::I64]);
    let out_pk = TestSchema::with_pk_at(0, &[type_code::I64, type_code::U64]);
    let prog = LogicalProgram::from_wire(&[34, 0, 0, 0], 0, 0, vec![]).unwrap();
    assert_eq!(prog.validate(Some(&in_pk), Some(&out_pk)), Ok(0));
    // Both output-side checks are inert for a filter (`out_schema = None`).
    assert_eq!(prog.validate(Some(&in_pk), None), Ok(0));
}

/// EMIT stores a whole 8-byte register image: a 16-byte slot panics on the first
/// row and a narrower one truncates, so the destination stride is exactly 8.
#[test]
fn an_emit_slot_must_be_eight_bytes() {
    // out: [U64 PK, <slot>] — one payload slot, written by the single EMIT.
    let case = |tc: u8| {
        let schema = TestSchema::with_pk_at(0, &[type_code::U64, tc]);
        // LOAD_CONST (3) into reg 0 — EMIT's source must have a writer — then
        // EMIT (32) src=0 out=0.
        LogicalProgram::from_wire(&[3, 0, 0, 0, 32, 0, 0, 0], 1, 0, vec![])
            .unwrap()
            .validate(Some(&schema), Some(&schema))
    };
    for tc in [type_code::I64, type_code::U64, type_code::F64] {
        assert_eq!(case(tc), Ok(0), "type code {tc}");
    }
    for tc in [type_code::U128, type_code::F32] {
        assert_eq!(
            case(tc),
            Err(ExprValidateErr::EmitSlotNotEightBytes { out: 0, type_code: tc }),
            "type code {tc}"
        );
    }
    // A German-string slot is refused on class, before the stride rule: the
    // source register is scalar, and the two errors name different faults.
    for tc in [type_code::STRING, type_code::BLOB] {
        assert_eq!(
            case(tc),
            Err(ExprValidateErr::EmitClassMismatch { out: 0, type_code: tc }),
            "type code {tc}"
        );
    }
}

#[test]
fn output_indices_are_checked_only_for_a_map() {
    // in: [U64 PK, I64]; out: [U64 PK, I64] — one output payload slot.
    let in_schema = schema_pk_ints(1, false);
    let out_schema = schema_pk_ints(1, false);
    // COPY_COL (34) src_col=0 out=200: rejected as a MAP (out_schema Some).
    let prog = LogicalProgram::from_wire(&[34, 0, 0, 200], 0, 0, vec![]).unwrap();
    assert_eq!(
        prog.validate(Some(&in_schema), Some(&out_schema)),
        Err(ExprValidateErr::OutputIdxOutOfRange {
            out: 200,
            num_payload_cols: 1,
        })
    );
    // The same output opcode in a FILTER (out_schema None) is a harmless no-op.
    assert_eq!(prog.validate(Some(&in_schema), None), Ok(0));
}

/// Output coverage: with an `out_schema`, every declared payload slot must be
/// written. Counted by popcount, so a duplicate destination — which leaves
/// another slot unwritten while the instruction count still matches — is caught
/// by the same rule.
#[test]
fn validate_rejects_an_unwritten_output_slot() {
    // in/out: [U64 PK, I64, I64] — two output payload slots.
    let schema = schema_pk_ints(2, false);
    let map = |quads: &[u32]| LogicalProgram::from_wire(quads, 0, 0, vec![]).unwrap();

    // Both slots written — accepted.
    assert_eq!(
        map(&[34, 0, 1, 0, 34, 0, 2, 1]).validate(Some(&schema), Some(&schema)),
        Ok(0)
    );

    // Only slot 0 written.
    assert_eq!(
        map(&[34, 0, 1, 0]).validate(Some(&schema), Some(&schema)),
        Err(ExprValidateErr::OutputSlotUnwritten {
            written: 0b01,
            num_payload_cols: 2,
        })
    );

    // Two CopyCols, both onto slot 0: the count matches but slot 1 is unwritten.
    assert_eq!(
        map(&[34, 0, 1, 0, 34, 0, 2, 0]).validate(Some(&schema), Some(&schema)),
        Err(ExprValidateErr::OutputSlotUnwritten {
            written: 0b01,
            num_payload_cols: 2,
        })
    );

    // A predicate over the same program is unaffected — no output plan.
    assert_eq!(map(&[34, 0, 1, 0]).validate(Some(&schema), None), Ok(0));
}

#[test]
#[should_panic(expected = "RegisterAliasing")]
fn new_panics_on_an_aliased_register() {
    // A compiler-built (trusted) aliased-register program still panics from `new`.
    let _ = LogicalProgram::new(vec![LogicalInstr::IntAdd { dst: 0, a: 0, b: 1 }], 2, 0, vec![]);
}

// ---------------------------------------------------------------------------
// INT_IN_SET — set membership as one opcode (O(1) registers, O(log N) per row)
// ---------------------------------------------------------------------------

/// `r0 = col1; r1 = r0 IN set`, result_reg = 1 — the compiled shape of
/// `col1 IN (…)`. `col_tc` picks col1's type (I64 / U64 / …).
fn in_set_prog(col_tc: u8, set: &[i64]) -> (TestSchema, Evaluator) {
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, col_tc]);
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::IntInSet {
            dst: 1,
            value_reg: 0,
            set_idx: 0,
        },
    ];
    let prog = scalar_prog(&schema, instrs, 2, 1, vec![gnitz_wire::as_le_bytes(set).to_vec()]);
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
    type Case<'a> = (u8, &'a [i64], &'a [(i64, i64)]);
    let big: Vec<i64> = (0..1000).collect();
    let cases: [Case<'_>; 4] = [
        (
            type_code::I64,
            &[-5, -1, 0, 3],
            &[(-5, 1), (-1, 1), (0, 1), (3, 1), (2, 0), (100, 0)],
        ),
        (type_code::U64, &[-1], &[(u64::MAX as i64, 1), (7, 0)]),
        (type_code::I64, &[], &[(42, 0)]),
        (type_code::I64, &big, &[(0, 1), (777, 1), (999, 1), (1000, 0), (-1, 0)]),
    ];

    for (col_tc, pool, values) in cases {
        let (schema, prog) = in_set_prog(col_tc, pool);
        for &(v, want) in values {
            let mb = make_int_view(&schema, &[(1, 0, &[v])]);
            assert_eq!(
                prog.eval_row(&mb, 0),
                Some(want),
                "tc={col_tc} pool_len={} value={v}",
                pool.len()
            );
        }
        // col1 is payload slot 0, so bit 0 of the null word is its NULL.
        let mb = make_int_view(&schema, &[(1, 0b1, &[0])]);
        assert!(
            prog.eval_row(&mb, 0).is_none(),
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
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::IntInSet {
            dst: 1,
            value_reg: 0,
            set_idx: 0,
        },
        LogicalInstr::BoolNot { dst: 2, a: 1 },
    ];
    let prog = scalar_prog(
        &schema,
        instrs,
        3,
        2,
        vec![gnitz_wire::as_le_bytes(&[1i64, 2, 3]).to_vec()],
    );
    for (null_word, val, want) in [(0b1, 0, None), (0, 9, Some(1)), (0, 2, Some(0))] {
        let mb = make_int_view(&schema, &[(1, null_word, &[val])]);
        assert_eq!(
            prog.eval_row(&mb, 0),
            want,
            "NOT IN over {val} (null_word {null_word:#b})"
        );
    }
}

/// The wire pool order is not trusted: `resolve` sorts it, because the kernel
/// binary-searches it and the only sort otherwise happens in the client planner.
#[test]
fn an_unsorted_in_set_pool_is_sorted_at_resolve() {
    let (schema, prog) = in_set_prog(type_code::I64, &[42, -1, 7, 3]);
    assert_eq!(prog.prog.int_sets[0], vec![-1, 3, 7, 42]);
    // A binary search over the raw descending-ish order would miss 7 (it sits
    // past the first probe's `42 > 7` left turn).
    for v in [-1i64, 3, 7, 42] {
        let mb = make_int_view(&schema, &[(1, 0, &[v])]);
        assert_eq!(prog.eval_row(&mb, 0), Some(1), "value {v} must be found");
    }
    let mb = make_int_view(&schema, &[(1, 0, &[8])]);
    assert_eq!(prog.eval_row(&mb, 0), Some(0));
}

#[test]
fn validate_rejects_a_misaligned_in_set_pool() {
    // A pool whose length is not a multiple of 8 is a clean IntSetNotAligned,
    // not a silent chunks_exact tail-drop at resolve. `from_wire` runs the
    // structure-only validate, so it rejects here.
    // Quad: [INT_IN_SET=46, dst=1, value_reg=0, set_idx=0], pool of 5 bytes.
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[46, 1, 0, 0], 2, 1, vec![vec![0u8; 5]])),
        ExprValidateErr::IntSetNotAligned { set_idx: 0, len: 5 }
    );
}

#[test]
fn validate_rejects_an_out_of_range_set_idx() {
    // set_idx = 9 against a pool of length 1 → ConstIdxOutOfRange.
    assert_eq!(
        wire_err(LogicalProgram::from_wire(&[46, 1, 0, 9], 2, 1, vec![vec![0u8; 8]])),
        ExprValidateErr::ConstIdxOutOfRange { const_idx: 9, n: 1 }
    );
}

/// Classifier: every CMP and every AND in a pure conjunction is bit_only
/// (with `is_filter=true`); result_reg stays bit_only.
#[test]
fn classifier_pure_conjunction_filter() {
    let schema = schema_pk_ints(2, true);
    let instrs = conjunction_over_two_cols();
    let ProgramFacts {
        bit_only, bool_input, ..
    } = analyze(&instrs, &schema, 5, true);
    // The same program must also be a legal filter.
    filter_prog(&schema, instrs, 6, 5, vec![]);
    // Bool producers: r2 (CMP_GT), r4 (CMP_GT), r5 (BOOL_AND).
    // Non-bool readers consume r0/r1/r3 (CMPs read them as i64), so those
    // never qualify for bit_only. r2/r4 are read only by BOOL_AND, and r5
    // (result_reg) stays bit_only under is_filter=true.
    let expected_bit_only = (1u64 << 2) | (1u64 << 4) | (1u64 << 5);
    assert_eq!(
        bit_only, expected_bit_only,
        "expected r2/r4/r5 bit_only; got mask {bit_only:#010b}"
    );
    // r2 and r4 are read by BOOL_AND; r5 (result_reg) is force-marked a bool
    // input so the filter's nullable word merge always finds it packed.
    assert_eq!(bool_input, (1 << 2) | (1 << 4) | (1 << 5));
}

// ---------------------------------------------------------------------------
// Numeric scalar functions and numeric CAST: validation and the resolve-time
// analyses (U64 tracking, nullability classification).
// ---------------------------------------------------------------------------

/// The cast target rides the `a2` word, so a forged blob can put anything
/// there. It must be rejected before eval, where it would index a bounds table
/// that has no arm for it.
#[test]
fn validate_rejects_a_forged_cast_target() {
    for op in [ExprOp::IntCast.as_wire(), ExprOp::FloatToInt.as_wire()] {
        for tc in [
            0u32,
            type_code::STRING as u32,
            type_code::U128 as u32,
            type_code::F64 as u32,
            255,
            // A `>= 255` word must not be truncated into a valid code on the
            // way in: the full u32 has to reach `validate`, or these would
            // pass as I64.
            0x100u32 | type_code::I64 as u32,
            0x1_0000u32 | type_code::I64 as u32,
        ] {
            let code = [ExprOp::LoadColInt.as_wire(), 0, 1, 0, op, 1, 0, tc];
            assert_eq!(
                wire_err(LogicalProgram::from_wire(&code, 2, 1, vec![])),
                ExprValidateErr::BadCastTarget { tc },
                "op {op} tc {tc}"
            );
        }
        // Control: every fixed-int target is accepted.
        for tc in [
            type_code::I8,
            type_code::U8,
            type_code::I16,
            type_code::U16,
            type_code::I32,
            type_code::U32,
            type_code::I64,
            type_code::U64,
        ] {
            let code = [ExprOp::LoadColInt.as_wire(), 0, 1, 0, op, 1, 0, tc as u32];
            assert!(
                LogicalProgram::from_wire(&code, 2, 1, vec![]).is_ok(),
                "op {op} tc {tc} must be accepted"
            );
        }
    }
}

#[test]
fn validate_bounds_checks_every_register_operand() {
    let unary = [
        ExprOp::IntAbs.as_wire(),
        ExprOp::FloatAbs.as_wire(),
        ExprOp::FloatFloor.as_wire(),
        ExprOp::FloatCeil.as_wire(),
        ExprOp::FloatRound.as_wire(),
        ExprOp::FloatTrunc.as_wire(),
        ExprOp::FloatToF32.as_wire(),
    ];
    for op in unary {
        // dst out of range, then operand out of range.
        assert!(matches!(
            wire_err(LogicalProgram::from_wire(&[op, 9, 0, 0], 2, 0, vec![])),
            ExprValidateErr::RegOutOfRange { .. }
        ));
        assert!(matches!(
            wire_err(LogicalProgram::from_wire(&[op, 0, 9, 0], 2, 0, vec![])),
            ExprValidateErr::RegOutOfRange { .. }
        ));
    }
    for op in [
        ExprOp::IntMax2.as_wire(),
        ExprOp::IntMin2.as_wire(),
        ExprOp::FloatMax2.as_wire(),
        ExprOp::FloatMin2.as_wire(),
    ] {
        assert!(matches!(
            wire_err(LogicalProgram::from_wire(&[op, 0, 1, 9], 2, 0, vec![])),
            ExprValidateErr::RegOutOfRange { .. }
        ));
        // The binary ops are SSA anti-aliased: dst may not be an operand.
        assert!(
            LogicalProgram::from_wire(&[op, 0, 0, 1], 2, 0, vec![]).is_err(),
            "op {op}: dst == a must be rejected"
        );
    }
}

/// The three casts manufacture NULL out of a non-NULL input, so a program
/// carrying one can never take the `no_nulls` fast path; the pure transforms
/// only propagate and must not disturb it.
#[test]
fn narrowing_casts_manufacture_null_but_pure_transforms_do_not() {
    let nonnull = schema_pk_ints(1, false);
    let with = |instr: LogicalInstr| {
        let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 1 }, instr];
        scalar_prog(&nonnull, instrs, 2, 1, vec![]).prog.no_nulls
    };
    for instr in [
        LogicalInstr::IntUnary {
            op: IntUnaryOp::Abs,
            dst: 1,
            a: 0,
        },
        LogicalInstr::FloatUnary {
            op: super::FloatUnaryOp::Round,
            dst: 1,
            a: 0,
        },
        LogicalInstr::IntMinMax2 {
            dst: 1,
            a: 0,
            b: 0,
            is_max: true,
        },
    ] {
        assert!(with(instr), "a propagating transform keeps no_nulls");
    }
    for instr in [
        LogicalInstr::FloatToF32 { dst: 1, a: 0 },
        LogicalInstr::IntCast {
            dst: 1,
            a: 0,
            tc: type_code::I8 as u32,
        },
        LogicalInstr::FloatToInt {
            dst: 1,
            a: 0,
            tc: type_code::I8 as u32,
        },
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
    let no_nulls = |instrs: Vec<LogicalInstr>, num_regs: u32, result: u32| {
        scalar_prog(&schema, instrs, num_regs, result, vec![]).prog.no_nulls
    };

    for instr in [is_null_op(0, 1), is_not_null_op(0, 1)] {
        assert!(
            no_nulls(vec![instr], 1, 0),
            "a null test over a nullable column is definite"
        );
    }

    // The same nullable column, now also loaded: the load is what carries the
    // NULL into a register, so the program belongs on the nullable arm.
    assert!(
        !no_nulls(
            vec![is_null_op(0, 1), LogicalInstr::LoadColInt { dst: 1, col: 1 },],
            2,
            0
        ),
        "loading the tested column must force the nullable arm"
    );
}

/// A PK column carries no null bit — the null bitmap is payload-indexed — so
/// loading one never forces the nullable arm. `TestSchema` validates nothing,
/// so it can state the nullable PK both production implementors reject, which
/// is what makes the guard's absence observable.
#[test]
fn a_nullable_pk_column_load_keeps_no_nulls() {
    let schema = TestSchema::new(&[(type_code::U64, true), (type_code::I64, false)], &[0]);
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 0 }];
    assert!(scalar_prog(&schema, instrs, 1, 0, vec![]).prog.no_nulls);
}

/// A float column load inherits its column's null bit, like every other typed
/// column operand. Driven in both directions — a nullable F64 column must force
/// the nullable arm, a non-nullable one must leave `no_nulls` set — so a
/// classification that ignored the column's nullability outright fails here
/// whichever way it defaulted.
#[test]
fn a_float_column_load_carries_its_null_bit() {
    let no_nulls_over = |nullable: bool| {
        let schema = TestSchema::new(&[(type_code::U64, false), (type_code::F64, nullable)], &[0]);
        let instrs = vec![LogicalInstr::LoadColFloat { dst: 0, col: 1 }];
        scalar_prog(&schema, instrs, 1, 0, vec![]).prog.no_nulls
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
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, type_code::U64, type_code::I64]);
    let signed_of = |a_col: u32, b_col: u32| {
        let instrs = vec![
            LogicalInstr::LoadColInt { dst: 0, col: a_col },
            LogicalInstr::LoadColInt { dst: 1, col: b_col },
            LogicalInstr::IntMinMax2 {
                dst: 2,
                a: 0,
                b: 1,
                is_max: true,
            },
            // A second fold against the signed column reads dst's taint.
            LogicalInstr::LoadColInt { dst: 3, col: 2 },
            LogicalInstr::IntMinMax2 {
                dst: 4,
                a: 2,
                b: 3,
                is_max: true,
            },
        ];
        let prog = scalar_prog(&schema, instrs, 5, 4, vec![]);
        prog.prog
            .instrs
            .iter()
            .filter_map(|i| match i {
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
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, type_code::I64, type_code::U64]);
    let cmp_signed_after = |tc: u8, src_col: u32| {
        let instrs = vec![
            LogicalInstr::LoadColInt { dst: 0, col: src_col },
            LogicalInstr::IntCast {
                dst: 1,
                a: 0,
                tc: tc as u32,
            },
            LogicalInstr::LoadConst { dst: 2, val: 0 },
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 3,
                a: 1,
                b: 2,
            },
        ];
        let prog = scalar_prog(&schema, instrs, 4, 3, vec![]);
        prog.prog
            .instrs
            .iter()
            .find_map(|i| match i {
                Instr::Cmp { signed, .. } => Some(*signed),
                _ => None,
            })
            .expect("the compare survives")
    };
    assert!(!cmp_signed_after(type_code::U64, 1), "U64 target taints the dst");
    assert!(
        cmp_signed_after(type_code::I64, 2),
        "a signed target clears a U64 source's taint"
    );
    assert!(cmp_signed_after(type_code::I32, 1));
}

/// `src_signed` is resolved from the OPERAND's tracking, not from the target —
/// it is what decides whether the range check reads the register as i64 or u64.
#[test]
fn int_cast_records_the_source_signedness() {
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, type_code::I64, type_code::U64]);
    let src_signed_of = |src_col: u32| {
        let instrs = vec![
            LogicalInstr::LoadColInt { dst: 0, col: src_col },
            LogicalInstr::IntCast {
                dst: 1,
                a: 0,
                tc: type_code::I8 as u32,
            },
        ];
        let prog = scalar_prog(&schema, instrs, 2, 1, vec![]);
        prog.prog
            .instrs
            .iter()
            .find_map(|i| match i {
                Instr::IntCast { src_signed, .. } => Some(*src_signed),
                _ => None,
            })
            .expect("the cast survives")
    };
    assert!(src_signed_of(1), "an I64 column is a signed source");
    assert!(!src_signed_of(2), "a U64 column is an unsigned source");
}

// ---------------------------------------------------------------------------
// Register classes and single-assignment
// ---------------------------------------------------------------------------

/// Decode a wire program and keep only the verdict. The class rules are
/// schema-free, so this is where a forged program meets them — before any schema
/// is in hand.
fn from_wire(code: &[u32], num_regs: u32) -> Result<(), ExprValidateErr> {
    wire_verdict(code, num_regs, 0)
}

fn wire_verdict(code: &[u32], num_regs: u32, result_reg: u32) -> Result<(), ExprValidateErr> {
    LogicalProgram::from_wire(code, num_regs, result_reg, vec![]).map(|_| ())
}

/// A mixed-class register operand is refused whichever way round it goes: a
/// string opcode reading the scalar file would resolve a lane that was never
/// written, and an integer opcode reading a string register would interpret
/// whatever i64 sits at that index.
#[test]
fn operand_class_is_enforced_in_both_directions() {
    // LOAD_COL_INT into reg 0, then UPPER of it.
    assert_eq!(
        from_wire(
            &[
                ExprOp::LoadColInt.as_wire(),
                0,
                1,
                0,
                ExprOp::StrUpper.as_wire(),
                1,
                0,
                0
            ],
            2
        ),
        Err(ExprValidateErr::RegClassMismatch { reg: 0 })
    );
    // LOAD_COL_STR into reg 0, then integer ADD of it.
    assert_eq!(
        from_wire(
            &[ExprOp::LoadColStr.as_wire(), 0, 1, 0, ExprOp::IntAdd.as_wire(), 1, 0, 0],
            2
        ),
        Err(ExprValidateErr::RegClassMismatch { reg: 0 })
    );
    // The mixed-class opcodes police each half separately: SUBSTR's source must
    // be a string and its bounds must not be.
    assert_eq!(
        from_wire(
            &[
                ExprOp::LoadConst.as_wire(),
                0,
                1,
                0,
                ExprOp::StrSubstr.as_wire(),
                1,
                0,
                0
            ],
            2
        ),
        Err(ExprValidateErr::RegClassMismatch { reg: 0 })
    );
    assert_eq!(
        from_wire(
            &[
                ExprOp::LoadColStr.as_wire(),
                0,
                1,
                0, //
                ExprOp::LoadColStr.as_wire(),
                1,
                1,
                0, //
                ExprOp::StrSubstr.as_wire(),
                2,
                0,
                1,
            ],
            3
        ),
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
    let code = [
        ExprOp::StrUpper.as_wire(),
        0,
        1,
        0,
        ExprOp::LoadColStr.as_wire(),
        1,
        1,
        0,
    ];
    assert_eq!(from_wire(&code, 2), Err(ExprValidateErr::RegReadBeforeWrite { reg: 1 }));
    // The same two instructions in the other order are legal, so the rejection
    // above is about ordering and not about the instructions themselves.
    let ordered = [
        ExprOp::LoadColStr.as_wire(),
        0,
        1,
        0,
        ExprOp::StrUpper.as_wire(),
        1,
        0,
        0,
    ];
    assert!(from_wire(&ordered, 2).is_ok());
}

/// The multi-operand string opcodes join the existing SSA anti-aliasing arms:
/// their kernels split `str_views` at `dst` while reading a source window, and
/// `split_windows`' disjointness guard is a `debug_assert` release compiles out.
#[test]
fn string_ops_are_anti_aliased_against_their_destination() {
    let load_two = [
        ExprOp::LoadColStr.as_wire(),
        0,
        1,
        0,
        ExprOp::LoadColStr.as_wire(),
        1,
        2,
        0,
    ];
    let alias = |op: u32, dst: u32, a: u32, b: u32| {
        let mut code = load_two.to_vec();
        code.extend_from_slice(&[op, dst, a, b]);
        from_wire(&code, 3)
    };
    for (op, a, b) in [(ExprOp::StrCmpEq.as_wire(), 0, 1), (ExprOp::StrConcat.as_wire(), 0, 1)] {
        assert!(
            matches!(alias(op, 0, a, b), Err(ExprValidateErr::RegisterAliasing { .. })),
            "opcode {op} must not write one of its own sources"
        );
    }
    // STR_SELECT packs `a | b << 16`, and SUBSTR `start | len << 16`.
    let sel = |dst: u32, cond: u32, a: u32, b: u32| {
        let mut code = load_two.to_vec();
        code.extend_from_slice(&[
            ExprOp::StrSelect.as_wire(),
            dst,
            cond,
            gnitz_wire::pack_operand_pair(a, b),
        ]);
        from_wire(&code, 3)
    };
    assert!(matches!(sel(1, 2, 0, 1), Err(ExprValidateErr::RegisterAliasing { .. })));
    // `dst == a` where the register was never written: only the aliasing arm
    // catches this, since a never-written register also reads as class-clear.
    let mut code = vec![ExprOp::LoadColStr.as_wire(), 0, 1, 0];
    code.extend_from_slice(&[ExprOp::StrSubstr.as_wire(), 0, 0, 1]);
    assert!(matches!(
        from_wire(&code, 2),
        Err(ExprValidateErr::RegisterAliasing { .. })
    ));
}

/// Single-assignment stops being a client convention and becomes a checked rule.
/// Every planner-emitted program is already SSA (`ExprBuilder::push` is a monotonic
/// counter), so nothing legitimate is refused.
#[test]
fn a_register_may_have_only_one_writer() {
    // Same class.
    assert_eq!(
        from_wire(
            &[
                ExprOp::LoadConst.as_wire(),
                0,
                1,
                0,
                ExprOp::LoadConst.as_wire(),
                0,
                2,
                0
            ],
            1
        ),
        Err(ExprValidateErr::RegRewrite { reg: 0 })
    );
    // Across classes, which is what makes a register's class well-defined.
    assert_eq!(
        from_wire(
            &[
                ExprOp::LoadColStr.as_wire(),
                0,
                1,
                0,
                ExprOp::LoadConst.as_wire(),
                0,
                2,
                0
            ],
            1
        ),
        Err(ExprValidateErr::RegRewrite { reg: 0 })
    );
    // The aliasing check keeps precedence, so the more specific diagnosis wins.
    assert!(matches!(
        from_wire(
            &[ExprOp::LoadColInt.as_wire(), 0, 1, 0, ExprOp::CmpGt.as_wire(), 0, 0, 0],
            1
        ),
        Err(ExprValidateErr::RegisterAliasing { .. })
    ));
    // A register-free `CopyCol` program has no `dst` to collide.
    assert!(LogicalProgram::copy_cols(&[0, 1, 2]).payload_copy_srcs().is_some());
}

/// EMIT's destination must hold what its source register's class stores. A
/// mismatch either way is caught before eval, where it would write an 8-byte
/// register image into a 16-byte cell (or the reverse).
#[test]
fn emit_class_must_match_its_destination_column() {
    // out: [U64 PK, STRING].
    let str_out = TestSchema::with_pk_at(0, &[type_code::U64, type_code::STRING]);
    let int_out = TestSchema::with_pk_at(0, &[type_code::U64, type_code::I64]);
    let in_str = TestSchema::with_pk_at(0, &[type_code::U64, type_code::STRING]);

    let scalar_src = LogicalProgram::from_wire(
        &[ExprOp::LoadConst.as_wire(), 0, 7, 0, ExprOp::Emit.as_wire(), 0, 0, 0],
        1,
        0,
        vec![],
    )
    .unwrap();
    assert_eq!(
        scalar_src.validate(Some(&in_str), Some(&str_out)),
        Err(ExprValidateErr::EmitClassMismatch {
            out: 0,
            type_code: type_code::STRING
        })
    );

    let str_src = LogicalProgram::from_wire(
        &[ExprOp::LoadColStr.as_wire(), 0, 1, 0, ExprOp::Emit.as_wire(), 0, 0, 0],
        1,
        0,
        vec![],
    )
    .unwrap();
    assert_eq!(
        str_src.validate(Some(&in_str), Some(&int_out)),
        Err(ExprValidateErr::EmitClassMismatch {
            out: 0,
            type_code: type_code::I64
        })
    );
    // The matching pairing is accepted, and reports reg 0 as a string.
    assert_eq!(str_src.validate(Some(&in_str), Some(&str_out)), Ok(1));
    assert_eq!(scalar_src.validate(Some(&in_str), Some(&int_out)), Ok(0));
}

/// EMIT's source is exempt from the class check — its class picks the
/// destination-column rule — but not from the bound every other read gets.
#[test]
fn emit_bounds_checks_its_source_register() {
    assert_eq!(
        from_wire(&[ExprOp::Emit.as_wire(), 0, 5, 0], 2),
        Err(ExprValidateErr::RegOutOfRange { reg: 5, num_regs: 2 })
    );
}

/// The result register's class splits the two resolvers that make the identical
/// `validate` call: a filter reads its verdict out of the scalar file, while a
/// SET right-hand side wants exactly a string back.
#[test]
fn a_string_result_register_resolves_as_a_scalar_but_not_as_a_filter() {
    let schema = schema_pk_strings(1, true);
    let prog = || LogicalProgram::from_wire(&[ExprOp::LoadColStr.as_wire(), 0, 1, 0], 1, 0, vec![]).unwrap();
    assert_eq!(
        prog().resolve_filter(&schema).err(),
        Some(ExprValidateErr::RegClassMismatch { reg: 0 })
    );
    assert!(prog().resolve_scalar(&schema).is_ok());
}

#[test]
fn load_col_str_requires_a_german_string_column() {
    let schema = TestSchema::with_pk_at(0, &[type_code::U64, type_code::I64]);
    assert_eq!(
        LogicalProgram::from_wire(&[ExprOp::LoadColStr.as_wire(), 0, 1, 0], 1, 0, vec![])
            .unwrap()
            .validate(Some(&schema), None),
        Err(ExprValidateErr::ColKindMismatch {
            col: 1,
            type_code: type_code::I64,
            want: ColKind::GERMAN_STRING.describe(),
        })
    );
}

#[test]
fn trim_mode_and_cast_target_are_narrowed_at_decode() {
    let trim = |mode: u32| {
        LogicalProgram::from_wire(
            &[
                ExprOp::LoadColStr.as_wire(),
                0,
                1,
                0,
                ExprOp::StrTrim.as_wire(),
                1,
                gnitz_wire::pack_operand_pair(0, mode),
                0,
            ],
            2,
            1,
            vec![b" ".to_vec()],
        )
        .map(|_| ())
    };
    for mode in 0..3 {
        assert!(trim(mode).is_ok(), "mode {mode}");
    }
    assert_eq!(trim(3), Err(ExprValidateErr::BadTrimMode { mode: 3 }));

    assert_eq!(
        wire_verdict(
            &[
                ExprOp::LoadColStr.as_wire(),
                0,
                1,
                0,
                ExprOp::StrToInt.as_wire(),
                1,
                0,
                999
            ],
            2,
            1
        ),
        Err(ExprValidateErr::BadCastTarget { tc: 999 })
    );
}

#[test]
fn like_rejects_a_forged_escape_or_operand() {
    // LOAD_COL_STR into reg 0, then LIKE of it into reg 1, with the escape
    // packed above the source register.
    let load_like = |op, escape: u32, pat_idx| {
        [
            ExprOp::LoadColStr.as_wire(),
            0,
            1,
            0,
            op,
            1,
            gnitz_wire::pack_operand_pair(0, escape),
            pat_idx,
        ]
    };
    let pool = || vec![b"a%".to_vec()];
    let decode = |code: [u32; 8], pool: Vec<Vec<u8>>| LogicalProgram::from_wire(&code, 2, 1, pool).map(|_| ());

    assert!(decode(load_like(ExprOp::StrLike.as_wire(), b'\\' as u32, 0), pool()).is_ok());
    // Escape 0 disables escaping, and an empty pool entry is the legal `LIKE ''`.
    assert!(decode(load_like(ExprOp::StrIlike.as_wire(), 0, 0), pool()).is_ok());
    assert!(decode(load_like(ExprOp::StrLike.as_wire(), 0, 0), vec![Vec::new()]).is_ok());

    assert_eq!(
        decode(load_like(ExprOp::StrLike.as_wire(), b'\\' as u32, 9), pool()),
        Err(ExprValidateErr::ConstIdxOutOfRange { const_idx: 9, n: 1 })
    );
    // The escape is one byte, so the half above it must be clear.
    assert_eq!(
        decode(load_like(ExprOp::StrLike.as_wire(), 0x1_5C, 0), pool()),
        Err(ExprValidateErr::BadLikeEscape { escape: 0x1_5C })
    );
    // The source must be a string register …
    assert_eq!(
        decode(
            [
                ExprOp::LoadColInt.as_wire(),
                0,
                1,
                0,
                ExprOp::StrLike.as_wire(),
                1,
                0,
                0
            ],
            pool()
        ),
        Err(ExprValidateErr::RegClassMismatch { reg: 0 })
    );
    // … and the destination may not alias it.
    assert_eq!(
        LogicalProgram::from_wire(
            &[
                ExprOp::LoadColStr.as_wire(),
                0,
                1,
                0,
                ExprOp::StrLike.as_wire(),
                0,
                0,
                0
            ],
            1,
            0,
            pool()
        )
        .map(|_| ()),
        Err(ExprValidateErr::RegisterAliasing { dst: 0, reg: 0 })
    );
}

/// A matcher is compiled per instruction, not per pool index: the same pattern
/// under LIKE and under ILIKE are two different matchers.
#[test]
fn two_like_opcodes_over_one_pool_index_get_a_matcher_each() {
    let schema = schema_pk_strings(1, false);
    let like = |dst, ci| LogicalInstr::StrLike {
        dst,
        src: 0,
        escape: b'\\' as u32,
        pat_idx: 0,
        ci,
    };
    let instrs = vec![
        LogicalInstr::LoadColStr { dst: 0, col: 1 },
        like(1, false),
        like(2, true),
    ];
    let ev = scalar_prog(&schema, instrs, 3, 1, vec![b"abc".to_vec()]);
    assert_eq!(ev.prog.like_matchers.len(), 2);

    let rows: Vec<&[&[u8]]> = vec![&[b"ABC"]];
    let view = make_string_view(&schema, &rows);
    // Register 1 is the case-sensitive verdict (the program's result); the
    // ILIKE one is read off the same row through `reg_values`. Captured rather
    // than asserted inside the callback, which a non-firing morsel loop would
    // let pass vacuously.
    assert_eq!(ev.eval_row(&view, 0), Some(0));
    let mut ci_verdicts: Vec<i64> = Vec::new();
    ev.eval_morsels(&view, 0, 1, |_, out| ci_verdicts.push(out.reg_values(2)[0]));
    assert_eq!(ci_verdicts, [1]);
}

/// `UPPER` of a non-nullable column introduces no NULL, so the `no_nulls` fast
/// path survives it; the parses and SUBSTR do not.
#[test]
fn string_nullability_classification() {
    let schema = schema_pk_strings(1, false);
    let no_nulls = |tail: Vec<LogicalInstr>, num_regs: u32, result: u32| {
        let mut instrs = vec![LogicalInstr::LoadColStr { dst: 0, col: 1 }];
        instrs.extend(tail);
        scalar_prog(&schema, instrs, num_regs, result, vec![]).prog.no_nulls
    };
    assert!(no_nulls(
        vec![LogicalInstr::StrCase {
            dst: 1,
            a: 0,
            upper: true
        }],
        2,
        1
    ));
    assert!(!no_nulls(
        vec![LogicalInstr::StrToInt {
            dst: 1,
            a: 0,
            tc: type_code::I64 as u32,
        }],
        2,
        1
    ));
    // SUBSTR's only NULL is a negative length, so the two forms classify
    // differently: with a FOR clause it can produce one, without it cannot. The
    // window itself is total either way.
    let substr = |len_reg: Option<u16>| {
        let mut tail = vec![LogicalInstr::LoadConst { dst: 1, val: 1 }];
        if len_reg.is_some() {
            tail.push(LogicalInstr::LoadConst { dst: 2, val: 3 });
        }
        let dst = if len_reg.is_some() { 3 } else { 2 };
        tail.push(LogicalInstr::StrSubstr {
            dst,
            src: 0,
            start_reg: 1,
            len_reg,
        });
        no_nulls(tail, dst as u32 + 1, dst as u32)
    };
    assert!(substr(None), "no FOR clause writes no fail flag");
    assert!(!substr(Some(2)), "a FOR clause can name a negative length");
}

// ---------------------------------------------------------------------------
// Encoder / decoder drift — the two tables over one opcode space
// ---------------------------------------------------------------------------

/// The number of [`LogicalInstr`] variants, which is what makes [`every_variant`]
/// checkable for completeness: the fixture covers every variant iff the number of
/// *distinct* variants it holds equals this.
///
/// Adding a variant is two forced steps and one number: the match below stops
/// compiling, the author adds the name and bumps the count — and then the
/// completeness assert fails until `every_variant` gains an entry. The match
/// carries no per-variant data, so there is no index bookkeeping to keep dense;
/// `mem::discriminant` does the counting.
impl LogicalInstr {
    pub(crate) const VARIANT_COUNT: usize = 48;

    /// Exhaustive by construction — the compile error on a new variant is the
    /// whole point, so this must never gain a `_` arm.
    #[allow(dead_code)]
    fn assert_every_variant_is_listed(&self) {
        use LogicalInstr as L;
        match *self {
            L::LoadColInt { .. }
            | L::LoadColFloat { .. }
            | L::LoadConst { .. }
            | L::IntAdd { .. }
            | L::IntSub { .. }
            | L::IntMul { .. }
            | L::IntDiv { .. }
            | L::IntMod { .. }
            | L::FloatAdd { .. }
            | L::FloatSub { .. }
            | L::FloatMul { .. }
            | L::FloatDiv { .. }
            | L::Cmp { .. }
            | L::FCmp { .. }
            | L::IntToFloat { .. }
            | L::FloatUnary { .. }
            | L::IntUnary { .. }
            | L::FloatToInt { .. }
            | L::IntCast { .. }
            | L::FloatToF32 { .. }
            | L::IntMinMax2 { .. }
            | L::FloatMinMax2 { .. }
            | L::Select { .. }
            | L::LoadNull { .. }
            | L::BoolAnd { .. }
            | L::BoolOr { .. }
            | L::BoolNot { .. }
            | L::IsNull { .. }
            | L::StrColConst { .. }
            | L::StrColCol { .. }
            | L::IntInSet { .. }
            | L::LoadColStr { .. }
            | L::LoadConstStr { .. }
            | L::LoadNullStr { .. }
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
            | L::CopyCol { .. }
            | L::Emit { .. } => {}
        }
    }
}

impl LogicalProgram {
    pub(crate) fn instrs(&self) -> &[LogicalInstr] {
        &self.instrs
    }
}
/// One instance of every [`LogicalInstr`] variant, with a distinct value in
/// every field so a swapped pair cannot round-trip by coincidence.
///
/// The registers are deliberately small and the programs below are never
/// resolved: what is under test is the word layout alone, so the list need
/// not be a type-coherent or even a validatable program.
fn every_variant() -> Vec<LogicalInstr> {
    use LogicalInstr as L;
    let mut v = vec![
        L::LoadColInt { dst: 1, col: 2 },
        L::LoadColFloat { dst: 3, col: 4 },
        L::LoadConst {
            dst: 5,
            val: -1_234_567_890_123,
        },
        L::IntAdd { dst: 6, a: 7, b: 8 },
        L::IntSub { dst: 9, a: 10, b: 11 },
        L::IntMul { dst: 12, a: 13, b: 14 },
        L::IntDiv { dst: 15, a: 16, b: 17 },
        L::IntMod { dst: 18, a: 19, b: 20 },
        L::FloatAdd { dst: 21, a: 22, b: 23 },
        L::FloatSub { dst: 24, a: 25, b: 26 },
        L::FloatMul { dst: 27, a: 28, b: 29 },
        L::FloatDiv { dst: 30, a: 31, b: 32 },
        L::IntToFloat { dst: 33, a: 34 },
        L::FloatToF32 { dst: 35, a: 36 },
        L::FloatToInt {
            dst: 37,
            a: 38,
            tc: TypeCode::I16 as u32,
        },
        L::IntCast {
            dst: 39,
            a: 40,
            tc: TypeCode::I32 as u32,
        },
        L::Select {
            dst: 41,
            cond: 42,
            a: 43,
            b: 44,
        },
        L::LoadNull { dst: 45 },
        L::BoolAnd { dst: 46, a: 47, b: 48 },
        L::BoolOr { dst: 49, a: 50, b: 51 },
        L::BoolNot { dst: 52, a: 53 },
        L::IsNull {
            dst: 54,
            col: 55,
            invert: false,
        },
        L::IsNull {
            dst: 56,
            col: 57,
            invert: true,
        },
        L::IntInSet {
            dst: 58,
            value_reg: 59,
            set_idx: 60,
        },
        L::LoadColStr { dst: 61, col: 62 },
        L::LoadConstStr { dst: 63, const_idx: 64 },
        L::LoadNullStr { dst: 65 },
        L::StrSelect {
            dst: 66,
            cond: 67,
            a: 68,
            b: 69,
        },
        // The two packed-pair families sit in *different* operand words —
        // SELECT/SUBSTR in a2, TRIM/LIKE in a1 — which is the single
        // per-opcode fact the encoder and decoder can silently disagree on.
        L::StrSubstr {
            dst: 70,
            src: 71,
            start_reg: 72,
            len_reg: Some(73),
        },
        L::StrSubstr {
            dst: 74,
            src: 75,
            start_reg: 76,
            len_reg: None,
        },
        L::StrTrim {
            dst: 77,
            a: 78,
            mode: TrimMode::Leading.as_wire(),
            set_idx: 79,
        },
        L::StrLike {
            dst: 80,
            src: 81,
            escape: b'!' as u32,
            pat_idx: 82,
            ci: false,
        },
        L::StrLike {
            dst: 83,
            src: 84,
            escape: 0,
            pat_idx: 85,
            ci: true,
        },
        L::IntToStr { dst: 86, a: 87 },
        L::FloatToStr { dst: 88, a: 89 },
        L::StrToInt {
            dst: 90,
            a: 91,
            tc: TypeCode::I64 as u32,
        },
        L::StrToFloat { dst: 92, a: 93 },
        L::CopyCol { src_col: 94, out: 95 },
        L::Emit { src: 96, out: 97 },
    ];
    // The operator- and flag-parameterized families, every value of each.
    for op in [CmpOp::Eq, CmpOp::Ne, CmpOp::Gt, CmpOp::Ge, CmpOp::Lt, CmpOp::Le] {
        v.push(L::Cmp {
            op,
            dst: 100,
            a: 101,
            b: 102,
        });
        v.push(L::FCmp {
            op,
            dst: 103,
            a: 104,
            b: 105,
        });
    }
    for op in [StrOp::Eq, StrOp::Lt, StrOp::Le] {
        v.push(L::StrColConst {
            op,
            dst: 106,
            col: 107,
            const_idx: 108,
        });
        v.push(L::StrColCol {
            op,
            dst: 109,
            col_a: 110,
            col_b: 111,
        });
        v.push(L::StrCmp {
            op,
            dst: 112,
            a: 113,
            b: 114,
        });
    }
    for op in [IntUnaryOp::Neg, IntUnaryOp::Abs] {
        v.push(L::IntUnary { op, dst: 115, a: 116 });
    }
    for op in [
        FloatUnaryOp::Neg,
        FloatUnaryOp::Abs,
        FloatUnaryOp::Floor,
        FloatUnaryOp::Ceil,
        FloatUnaryOp::Round,
        FloatUnaryOp::Trunc,
    ] {
        v.push(L::FloatUnary { op, dst: 117, a: 118 });
    }
    for is_max in [true, false] {
        v.push(L::IntMinMax2 {
            dst: 119,
            a: 120,
            b: 121,
            is_max,
        });
        v.push(L::FloatMinMax2 {
            dst: 122,
            a: 123,
            b: 124,
            is_max,
        });
    }
    for chars in [false, true] {
        v.push(L::StrLen {
            dst: 125,
            a: 126,
            chars,
        });
    }
    for upper in [true, false] {
        v.push(L::StrCase {
            dst: 127,
            a: 128,
            upper,
        });
    }
    for skip_null in [false, true] {
        v.push(L::StrConcat {
            dst: 129,
            a: 130,
            b: 131,
            skip_null,
        });
    }
    v
}

/// `from_wire ∘ to_wire == id`. The encoder and the decoder are the only two
/// statements of the wire word layout, and this is what binds them: a swapped
/// operand pair, a flag encoded into the wrong opcode, or a cast target on
/// the wrong word all fail here.
#[test]
fn every_instruction_round_trips_through_the_wire_form() {
    let want = every_variant();
    let code: Vec<u32> = want.iter().copied().flat_map(LogicalInstr::to_wire).collect();
    // `from_wire` runs the structure-only validation, which this deliberately
    // ill-formed fixture cannot pass — so decode the quads directly.
    let got: Vec<LogicalInstr> = code
        .chunks_exact(4)
        .map(|q| LogicalProgram::decode_quad(q).expect("to_wire emits a decodable opcode"))
        .collect();
    assert_eq!(got, want);
}

/// Every opcode the decoder accepts must be reachable from the encoder, and
/// every [`LogicalInstr`] variant must appear in [`every_variant`] so the
/// round-trip above actually covers its operand layout.
///
/// `decode_quad` matches [`ExprOp`] exhaustively, so "accepted" is `ExprOp::ALL`
/// — an opcode with no decode arm no longer compiles, and one with no *encoder*
/// arm fails here.
#[test]
fn the_encoder_reaches_every_opcode_the_decoder_accepts() {
    let emitted: BTreeSet<u32> = every_variant().iter().map(|i| i.to_wire()[0]).collect();
    let accepted: BTreeSet<u32> = ExprOp::ALL.iter().map(|op| op.as_wire()).collect();
    assert_eq!(emitted, accepted, "encoded opcodes vs. opcodes the decoder accepts");

    // Two opcodes can share a variant (a flag folded into the opcode), so the
    // opcode sets agreeing does not imply every variant is covered.
    let all = every_variant();
    let seen: std::collections::HashSet<_> = all.iter().map(std::mem::discriminant).collect();
    assert_eq!(
        seen.len(),
        LogicalInstr::VARIANT_COUNT,
        "every_variant() covers {} of {} LogicalInstr variants",
        seen.len(),
        LogicalInstr::VARIANT_COUNT,
    );
}

#[test]
fn sequential_copy_projection() {
    // num_regs covers the largest register index in the synthetic programs
    // below so LogicalProgram::new's register-bounds assert passes; this test
    // exercises sequential_copy_base, not register limits.
    let make = |instrs: Vec<LogicalInstr>| LogicalProgram::new(instrs, 16, 0, vec![]);
    let copy = |src_col: u32, out: u32| LogicalInstr::CopyCol { src_col, out };
    // src 1,2 → dst 0,1: base = 1.
    assert_eq!(make(vec![copy(1, 0), copy(2, 1)]).sequential_copy_base(), Some(1));
    // sources not sequential (2, then 1)
    assert_eq!(make(vec![copy(2, 0), copy(1, 1)]).sequential_copy_base(), None);
    // a non-COPY_COL instruction breaks the block copy
    assert_eq!(
        make(vec![copy(1, 0), LogicalInstr::LoadColInt { dst: 9, col: 2 }]).sequential_copy_base(),
        None
    );
    assert_eq!(make(vec![]).sequential_copy_base(), None); // empty
                                                           // Sequential sources but destinations swapped (1, 0) — a permutation, not an identity.
    assert_eq!(make(vec![copy(1, 1), copy(2, 0)]).sequential_copy_base(), None);
    // Compound PK (k = 2): finalize copies columns 2, 3 → destinations 0, 1.
    assert_eq!(make(vec![copy(2, 0), copy(3, 1)]).sequential_copy_base(), Some(2));
}
