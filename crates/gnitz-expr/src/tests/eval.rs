//! The evaluator types' read-backs, and the 3VL / bit_only / AND-chain behaviour
//! visible through them.

use crate::{ConstIdx, Reg, Sink};
use gnitz_wire::{FixedInt, TypeCode};

use crate::batch::{encode_f64, MORSEL};
use crate::eval::Resolved;
use crate::test_support::{
    filter_prog, is_not_null_op, is_null_op, make_n_col_view, map_prog, passing_ranges, passing_rows, row_values, runs,
    scalar_prog, schema_pk_ints, TestSchema, TestView,
};
use crate::{CmpOp, IntArithOp, LogicalInstr, SchemaFacts};
use gnitz_wire::{payload_bytes, payload_u64, RowSource};

/// A map writes each computed slot from `dst_start` on — zeroing a NULL result's
/// cell — and moves each copied column's null bit into its slot. A BOOLEAN slot
/// stores 1 for any non-zero register value.
#[test]
fn a_map_writes_its_computed_slots_and_moves_the_copied_null_bits() {
    let in_schema = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::I64, true), (TypeCode::String, true)],
        &[0],
    );
    let out_schema = TestSchema::new(
        &[
            (TypeCode::U64, false),
            (TypeCode::I64, true),
            (TypeCode::I16, true),
            (TypeCode::String, true),
            (TypeCode::I64, true),
            (TypeCode::Bool, true),
        ],
        &[0],
    );
    let n = MORSEL + 7;
    let int = |row: usize| row as i64 - 100;
    let int_null = |row: usize| row.is_multiple_of(5);
    let text = |row: usize| format!("r{row}-{}", "x".repeat(row % 20));
    let text_null = |row: usize| row.is_multiple_of(7);
    let mut mb = make_n_col_view(&in_schema, n, |row, _| int(row), |row, col| col == 0 && int_null(row));
    for row in 0..n {
        mb.set_string(row, 1, text(row).as_bytes());
        if text_null(row) {
            mb.set_null(row, 1);
        }
    }

    let instrs = vec![
        LogicalInstr::LoadCol { col: 1 },
        LogicalInstr::LoadConst { val: 300, unsigned: false },
        LogicalInstr::IntArith {
            op: IntArithOp::Mul,
            a: Reg(0),
            b: Reg(1),
        },
        LogicalInstr::IntCast { a: Reg(2), fi: FixedInt::I16 },
        LogicalInstr::LoadColStr { col: 2 },
        LogicalInstr::StrCase { a: Reg(4), upper: true },
        LogicalInstr::LoadConst { val: 0, unsigned: false },
        LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(6) },
        is_not_null_op(2),
        LogicalInstr::BoolBinary { is_or: false, a: Reg(7), b: Reg(8) },
    ];
    let sinks = vec![
        Sink::Col(1),
        Sink::Reg(Reg(3)),
        Sink::Reg(Reg(5)),
        Sink::Reg(Reg(9)),
        Sink::Reg(Reg(2)),
    ];
    let mut ev = map_prog(&in_schema, &out_schema, instrs, sinks, vec![]);
    assert!(ev.emits_anything());
    assert_eq!(
        ev.copies(),
        &[crate::ColCopy {
            src: in_schema.locate(1),
            slot: 0,
            width: 8
        }]
    );

    let dst_start = 3;
    let mut out = TestView::for_schema(&out_schema, n + dst_start);
    ev.write_computed(&mb, 0, n, &mut out, dst_start);
    for row in 0..n {
        let dst = row + dst_start;
        let word = out.get_null_word(dst);
        let bit = |slot: usize| word >> slot & 1 != 0;
        assert_eq!(bit(0), int_null(row), "row {row}: the copied column's bit");

        let cast = (!int_null(row)).then(|| i16::try_from(int(row) * 300).ok()).flatten();
        let cell = out_schema.locate(2).decode_i64(&out, dst, FixedInt::I16);
        assert_eq!(
            (cell, bit(1)),
            (cast.map_or(0, i64::from), cast.is_none()),
            "row {row}: I16 slot"
        );

        match text_null(row) {
            true => assert_eq!(
                (out.get_col_ptr(dst, 2, 16), bit(2)),
                (&[0u8; 16][..], true),
                "row {row}: NULL string"
            ),
            false => assert_eq!(
                (payload_bytes(&out, dst, 2), bit(2)),
                (text(row).to_uppercase().as_bytes(), false),
                "row {row}"
            ),
        }

        let and = and3((!int_null(row)).then(|| int(row) > 0), Some(!text_null(row)));
        assert_eq!(
            (payload_u64(&out, dst, 3) as i64, bit(3)),
            (and.map_or(0, i64::from), and.is_none()),
            "row {row}: AND"
        );

        assert_eq!(
            (out.get_col_ptr(dst, 4, 1), bit(4)),
            (&[u8::from(!int_null(row) && int(row) != 0)][..], int_null(row)),
            "row {row}: BOOLEAN slot"
        );
    }

    // A pure projection emits nothing, so a caller may skip the kernel.
    let projection = map_prog(
        &in_schema,
        &schema_pk_ints(1, true),
        Vec::new(),
        vec![Sink::Col(1)],
        vec![],
    );
    assert!(!projection.emits_anything());
}

/// `ranges` reports maximal half-open runs, stitched across a morsel boundary,
/// on both arms.
#[test]
fn filter_emits_exact_maximal_ranges() {
    let n = MORSEL + 8;
    for nullable in [false, true] {
        let schema = schema_pk_ints(1, nullable);
        let instrs = vec![
            LogicalInstr::LoadCol { col: 1 },
            LogicalInstr::LoadConst { val: 0, unsigned: false },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
        ];
        let mut ev = filter_prog(&schema, instrs, vec![]);
        let mut runs = |pass: &dyn Fn(usize) -> bool| {
            let mb = make_n_col_view(&schema, n, |row, _| i64::from(pass(row)), |_, _| false);
            passing_ranges(&mut ev, &mb)
        };

        // Rows 0, 3, and MORSEL-1 are the only failures, so the last two runs
        // straddle and start at the morsel boundary.
        assert_eq!(
            runs(&|row| row != 0 && row != 3 && row != MORSEL - 1),
            vec![(1, 3), (4, MORSEL - 1), (MORSEL, n)]
        );
        // An all-pass batch is one range, not one per morsel; all-fail is none.
        assert_eq!(runs(&|_| true), vec![(0, n)]);
        assert_eq!(runs(&|_| false), vec![]);
    }
}

/// Whether a row's column is NULL.
type NullAt = fn(usize, usize) -> bool;

/// Reference 3VL AND: a definite FALSE on either side forces FALSE even when the
/// other is NULL, which is the rule a two-valued implementation gets wrong.
fn and3(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

/// The dual: a definite TRUE on either side forces TRUE.
fn or3(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

/// A three-deep AND chain, as a filter and as a scalar, against 3VL — with
/// NULLs scattered, everywhere, and absent on the `no_nulls` arm.
#[test]
fn three_and_chain_matches_the_three_valued_reference() {
    // The columns cycle mod 5, not mod 4: with `> 1` on three columns whose
    // values are three *consecutive* residues, mod 4 admits no passing row.
    let value = |row: usize, col: usize| ((row + col) as i64) % 5;
    let arrangements: [(&str, bool, NullAt); 3] = [
        ("spread", true, |row, col| row % [5, 7, 11][col] == 0),
        ("not_null", false, |_, _| false),
        ("all_null", true, |_, _| true),
    ];
    for (label, nullable, null_at) in arrangements {
        let schema = schema_pk_ints(3, nullable);
        let instrs = vec![
            LogicalInstr::LoadCol { col: 1 },                                // r0 = col0
            LogicalInstr::LoadConst { val: 1, unsigned: false },             // r1 = 1
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },       // r2 = col0 > 1
            LogicalInstr::LoadCol { col: 2 },                                // r3 = col1
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(3), b: Reg(1) },       // r4 = col1 > 1
            LogicalInstr::BoolBinary { is_or: false, a: Reg(2), b: Reg(4) }, // r5 = r2 AND r4
            LogicalInstr::LoadCol { col: 3 },                                // r6 = col2
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(6), b: Reg(1) },       // r7 = col2 > 1
            LogicalInstr::BoolBinary { is_or: false, a: Reg(5), b: Reg(7) }, // r8 = r5 AND r7
        ];
        let mut filter = filter_prog(&schema, instrs.clone(), vec![]);
        assert_eq!(filter.prog().no_nulls, !nullable, "{label}: wrong arm");
        let n = 77;
        let mb = make_n_col_view(&schema, n, value, null_at);
        let want: Vec<Option<bool>> = (0..n)
            .map(|row| {
                let clause = |col| (!null_at(row, col)).then(|| value(row, col) > 1);
                and3(and3(clause(0), clause(1)), clause(2))
            })
            .collect();
        assert_eq!(
            passing_rows(&mut filter, &mb),
            want.iter().map(|&w| w == Some(true)).collect::<Vec<_>>(),
            "{label}: filter"
        );
        assert_eq!(
            row_values(&mut scalar_prog(&schema, instrs, vec![]), &mb),
            want.iter().map(|w| w.map(i128::from)).collect::<Vec<_>>(),
            "{label}: scalar"
        );
    }
}

/// Every boolean combinator over the whole `{TRUE, FALSE, NULL}²` table, as a
/// filter and as a scalar.
#[test]
fn every_boolean_combinator_covers_the_whole_three_valued_table() {
    let schema = schema_pk_ints(2, true);
    const STATES: [Option<bool>; 3] = [Some(true), Some(false), None];
    // Truthy but not 1, and stored under a NULL too, where only the null bit
    // keeps it from reading as TRUE.
    const TRUTHY: i64 = -5;
    let cells: Vec<[Option<bool>; 2]> = STATES.iter().flat_map(|&a| STATES.map(|b| [a, b])).collect();
    let mb = make_n_col_view(
        &schema,
        cells.len(),
        |row, col| if cells[row][col] == Some(false) { 0 } else { TRUTHY },
        |row, col| cells[row][col].is_none(),
    );
    let verdicts = |want: &[Option<bool>]| want.iter().map(|&w| w == Some(true)).collect::<Vec<_>>();

    let (a, b) = (LogicalInstr::LoadCol { col: 1 }, LogicalInstr::LoadCol { col: 2 });
    // A bare load as a filter: no boolean producer wrote it, so the filter packs
    // its truthiness itself.
    let want: Vec<Option<bool>> = cells.iter().map(|c| c[0]).collect();
    assert_eq!(
        passing_rows(&mut filter_prog(&schema, vec![a], vec![]), &mb),
        verdicts(&want)
    );

    let binary = |is_or| LogicalInstr::BoolBinary { is_or, a: Reg(0), b: Reg(1) };
    type Reference = fn(Option<bool>, Option<bool>) -> Option<bool>;
    let combinators: [(&str, Vec<LogicalInstr>, Reference); 3] = [
        ("NOT a", vec![a, LogicalInstr::BoolNot { a: Reg(0) }], |a, _| {
            a.map(|a| !a)
        }),
        ("a AND b", vec![a, b, binary(false)], and3),
        ("a OR b", vec![a, b, binary(true)], or3),
    ];
    for (name, instrs, reference) in combinators {
        let want: Vec<Option<bool>> = cells.iter().map(|&[a, b]| reference(a, b)).collect();
        assert_eq!(
            passing_rows(&mut filter_prog(&schema, instrs.clone(), vec![]), &mb),
            verdicts(&want),
            "{name} as a filter"
        );
        assert_eq!(
            row_values(&mut scalar_prog(&schema, instrs, vec![]), &mb),
            want.iter().map(|w| w.map(i128::from)).collect::<Vec<_>>(),
            "{name} as a scalar"
        );
    }
}

/// NOT sets bits past the batch's last row; a failing last row still ends every
/// run inside the batch.
#[test]
fn not_leaves_no_run_past_the_last_row() {
    let schema = schema_pk_ints(1, true);
    let instrs = vec![
        LogicalInstr::LoadCol { col: 1 },
        LogicalInstr::LoadConst { val: 0, unsigned: false },
        LogicalInstr::Cmp { op: CmpOp::Ge, a: Reg(0), b: Reg(1) },
        LogicalInstr::BoolNot { a: Reg(2) },
    ];
    let mut ev = filter_prog(&schema, instrs, vec![]);
    assert!(!ev.prog().no_nulls);
    for n in [1, 63, 64, 65, 300] {
        // The last row, 0, fails NOT (v >= 0).
        let v = |row: usize| if row == n - 1 { 0 } else { (row % 3) as i64 - 1 };
        let mb = make_n_col_view(&schema, n, |row, _| v(row), |row, _| row % 5 == 1);
        let want: Vec<bool> = (0..n).map(|row| row % 5 != 1 && v(row) < 0).collect();
        assert_eq!(passing_ranges(&mut ev, &mb), runs(&want), "n={n}");
    }
}

/// No NULL, all NULL, and NULLs scattered through every word.
const NULL_ARRANGEMENTS: [(&str, NullAt); 3] = [
    ("none", |_, _| false),
    ("all", |_, _| true),
    ("spread", |row, col| (row + col) % 3 == 0),
];

/// A null-test predicate's verdict given whether each column is NULL.
type NullReference = fn(&dyn Fn(u32) -> bool) -> bool;

/// Programs whose only contact with a nullable column is `IS [NOT] NULL`
/// resolve `no_nulls` and select the reference rows.
#[test]
fn is_null_shapes_select_the_reference_rows() {
    let schema = schema_pk_ints(3, true);
    let and = |a, b| LogicalInstr::BoolBinary { is_or: false, a: Reg(a), b: Reg(b) };
    let shapes: Vec<(&str, Vec<LogicalInstr>, NullReference)> = vec![
        ("is_null", vec![is_null_op(1)], |n| n(1)),
        ("is_not_null", vec![is_not_null_op(1)], |n| !n(1)),
        // A PK column carries no null bit, so its null test is the constant.
        ("pk_is_null", vec![is_null_op(0)], |_| false),
        ("pk_is_not_null", vec![is_not_null_op(0)], |_| true),
        ("and", vec![is_null_op(1), is_not_null_op(2), and(0, 1)], |n| {
            n(1) && !n(2)
        }),
        (
            "or",
            vec![
                is_null_op(1),
                is_null_op(2),
                LogicalInstr::BoolBinary { is_or: true, a: Reg(0), b: Reg(1) },
            ],
            |n| n(1) || n(2),
        ),
        ("not", vec![is_null_op(1), LogicalInstr::BoolNot { a: Reg(0) }], |n| {
            !n(1)
        }),
        // Three conjuncts: the accumulator spine is two ANDs long and a null
        // test feeds another AND rather than the result register directly.
        (
            "and_chain",
            vec![is_null_op(1), is_null_op(2), and(0, 1), is_null_op(3), and(2, 3)],
            |n| n(1) && n(2) && n(3),
        ),
        // CASE WHEN col1 IS NULL THEN 1 ELSE 0 END.
        (
            "case_cond",
            vec![
                is_null_op(1),
                LogicalInstr::LoadConst { val: 1, unsigned: false },
                LogicalInstr::LoadConst { val: 0, unsigned: false },
                LogicalInstr::Select { cond: Reg(0), a: Reg(1), b: Reg(2) },
            ],
            |n| n(1),
        ),
    ];
    for (name, instrs, reference) in shapes {
        let mut ev = filter_prog(&schema, instrs, vec![]);
        assert!(ev.prog().no_nulls, "{name}: must resolve no_nulls");
        for (arrangement, null_pred) in NULL_ARRANGEMENTS {
            let n = 70;
            let mb = make_n_col_view(&schema, n, |row, col| ((row + col) % 5) as i64, null_pred);
            let want: Vec<bool> = (0..n)
                .map(|row| reference(&|col| col > 0 && null_pred(row, col as usize - 1)))
                .collect();
            assert_eq!(passing_rows(&mut ev, &mb), want, "{name}/{arrangement}");
        }
    }
}

/// A null test's verdict written to a register sink: nothing reads `dst` as a
/// packed bit, so it has to arrive in the `regs` lane on both arms, and it is
/// never NULL.
#[test]
fn is_null_into_a_register_sink_reads_back_per_row() {
    let in_schema = schema_pk_ints(1, true);
    let out_schema = schema_pk_ints(1, false);
    let n = MORSEL + 44;
    for invert in [false, true] {
        let mut ev = map_prog(
            &in_schema,
            &out_schema,
            vec![LogicalInstr::IsNull { col: 1, invert }],
            vec![Sink::Reg(Reg(0))],
            vec![],
        );
        assert!(ev.prog().no_nulls);
        for no_nulls in [true, false] {
            ev.set_no_nulls(no_nulls);
            for (arrangement, null_pred) in NULL_ARRANGEMENTS {
                let mb = make_n_col_view(&in_schema, n, |row, _| row as i64, null_pred);
                let mut out = TestView::for_schema(&out_schema, n);
                ev.write_computed(&mb, 0, n, &mut out, 0);
                for row in 0..n {
                    assert_eq!(
                        (payload_u64(&out, row, 0), out.get_null_word(row)),
                        (u64::from(null_pred(row, 0) ^ invert), 0),
                        "no_nulls={no_nulls}/{arrangement}/invert={invert}: row {row}"
                    );
                }
            }
        }
    }
}

/// Each register is NULL on exactly its nullable source's rows: a NOT NULL
/// column's null bit is never read, and each nullable one's is read at its slot.
#[test]
fn nullable_and_not_null_columns_side_by_side() {
    let schema = TestSchema::new(
        &[
            (TypeCode::U64, false),    // 0: pk
            (TypeCode::I64, true),     // 1: payload slot 0
            (TypeCode::I64, false),    // 2: payload slot 1
            (TypeCode::String, true),  // 3: payload slot 2
            (TypeCode::String, false), // 4: payload slot 3
            (TypeCode::F32, true),     // 5: payload slot 4
            (TypeCode::F32, false),    // 6: payload slot 5
        ],
        &[0],
    );
    // The rows each payload slot's bit is set on; the odd slots are NOT NULL, so
    // theirs are forged, on rows their nullable neighbour's are not.
    let slot_bit = |pi: usize, row: usize| match pi {
        0 => row.is_multiple_of(3),
        1 => row % 3 == 1,
        2 => row.is_multiple_of(5),
        3 => row % 5 == 2,
        4 => row.is_multiple_of(7),
        _ => row % 7 == 3,
    };
    let n = 300;
    let mut v = TestView::for_schema(&schema, n);
    for row in 0..n {
        let mut word = 0u64;
        for pi in 0..6 {
            gnitz_wire::null_word_set(&mut word, pi, slot_bit(pi, row));
        }
        v.set_null_word(row, word);
        v.set_int(row, 0, row as i64);
        v.set_int(row, 1, row as i64);
        v.set_string(row, 2, if row % 3 == 0 { b"alpha" } else { b"zeta" });
        v.set_string(row, 3, if row % 2 == 0 { b"beta" } else { b"omega" });
        v.set_payload(row, 4, &(row as f32).to_bits().to_le_bytes());
        v.set_payload(row, 5, &(row as f32).to_bits().to_le_bytes());
    }

    let instrs = vec![
        LogicalInstr::LoadCol { col: 1 },
        LogicalInstr::LoadCol { col: 2 },
        LogicalInstr::LoadCol { col: 5 },
        LogicalInstr::LoadCol { col: 6 },
        LogicalInstr::LoadColStr { col: 3 },
        LogicalInstr::LoadColStr { col: 4 },
        LogicalInstr::StrColConst {
            op: CmpOp::Lt,
            col: 4,
            const_idx: ConstIdx(0),
        },
        LogicalInstr::StrColCol { op: CmpOp::Lt, col_a: 3, col_b: 4 },
        LogicalInstr::StrColCol { op: CmpOp::Lt, col_a: 4, col_b: 4 },
    ];
    let mut ev = scalar_prog(&schema, instrs, vec![b"m".to_vec()]);
    assert!(!ev.prog().no_nulls);

    // The rows each register must report NULL on.
    let want_null = |reg: usize, row: usize| match reg {
        0 => slot_bit(0, row),     // nullable I64
        2 => slot_bit(4, row),     // nullable F32
        4 | 7 => slot_bit(2, row), // nullable STRING, and the compare it feeds
        _ => false,
    };
    let mut seen = vec![vec![false; n]; 9];
    ev.eval_morsels(&v, 0, n, |rel_start, out| {
        for (reg, rows) in seen.iter_mut().enumerate() {
            out.for_each_null_row(reg, |i| rows[rel_start + i] = true);
        }
    });
    for (reg, rows) in seen.iter().enumerate() {
        assert_eq!(
            *rows,
            (0..n).map(|row| want_null(reg, row)).collect::<Vec<_>>(),
            "reg {reg}"
        );
    }
}

/// An integer result reads back unsigned exactly when its register is tracked
/// U64.
#[test]
fn int_results_widen_by_the_result_registers_signedness() {
    let schema = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::U64, true), (TypeCode::I64, true)],
        &[0],
    );
    // Bit 63 set, so an unsigned and a signed reading differ.
    let big = (1u64 << 63) | 6;
    let mb = make_n_col_view(&schema, 1, |_, col| [big as i64, -1][col], |_, _| false);
    let u64_col = LogicalInstr::LoadCol { col: 1 };
    let i64_col = LogicalInstr::LoadCol { col: 2 };
    let k = |val| LogicalInstr::LoadConst { val, unsigned: false };
    let arith = |op| LogicalInstr::IntArith { op, a: Reg(0), b: Reg(1) };
    let max = |a, b| LogicalInstr::IntMinMax2 { a: Reg(a), b: Reg(b), is_max: true };
    for (label, instrs, want) in [
        ("u64 load", vec![u64_col], Some(i128::from(big))),
        (
            "u64 + 1",
            vec![u64_col, k(1), arith(IntArithOp::Add)],
            Some(i128::from(big + 1)),
        ),
        (
            "u64 / 2",
            vec![u64_col, k(2), arith(IntArithOp::Div)],
            Some(i128::from(big / 2)),
        ),
        ("u64 % 4", vec![u64_col, k(4), arith(IntArithOp::Mod)], Some(2)),
        (
            "u64 as float",
            vec![u64_col, LogicalInstr::IntToFloat { a: Reg(0) }],
            Some(i128::from(encode_f64(big as f64))),
        ),
        ("i64 load", vec![i64_col], Some(-1)),
        (
            "unsigned constant",
            vec![LogicalInstr::LoadConst { val: -1, unsigned: true }],
            Some(i128::from(u64::MAX)),
        ),
        ("signed constant", vec![k(-1)], Some(-1)),
        (
            "u64 > 1",
            vec![u64_col, k(1), LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) }],
            Some(1),
        ),
        // The i64 column is truthy, so the CASE picks the U64 branch, and the
        // compare reading it must be unsigned.
        (
            "CASE u64 ELSE i64 END > 100",
            vec![
                i64_col,
                u64_col,
                LogicalInstr::Select { cond: Reg(0), a: Reg(1), b: Reg(0) },
                k(100),
                LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(2), b: Reg(3) },
            ],
            Some(1),
        ),
        // Either operand tracked U64 makes a fold unsigned, and its result stays
        // tracked, so a follow-on fold against a signed value is unsigned too.
        (
            "MAX(MAX(u64, 1), 1)",
            vec![u64_col, k(1), max(0, 1), max(2, 1)],
            Some(i128::from(big)),
        ),
        (
            "MAX(MAX(1, u64), 1)",
            vec![k(1), u64_col, max(0, 1), max(2, 0)],
            Some(i128::from(big)),
        ),
        (
            "CAST(u64 AS BIGINT) of a value past i64::MAX",
            vec![u64_col, LogicalInstr::IntCast { a: Reg(0), fi: FixedInt::I64 }],
            None,
        ),
    ] {
        assert_eq!(
            row_values(&mut scalar_prog(&schema, instrs, vec![]), &mb),
            [want],
            "{label}"
        );
    }
}

/// A read's filter keeps the rows passing both its predicate and its bound, and
/// is the pass-through exactly when neither can drop a row.
#[test]
fn row_filter_keeps_the_intersection_of_its_predicate_and_its_bound() {
    use gnitz_wire::{key_image, Cut, KeyRange, PkColList, PkKeys, ReadBound};
    let schema = schema_pk_ints(1, true);
    let v_gt_3 = crate::LogicalProgram::new(
        vec![
            LogicalInstr::LoadCol { col: 1 },
            LogicalInstr::LoadConst { val: 3, unsigned: false },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
        ],
        vec![Sink::Reg(Reg(2))],
        vec![],
    )
    .to_blob_bytes();
    let null = |row: usize| row.is_multiple_of(5);
    let walk = |col: u32, start, end| ReadBound::Range(KeyRange::new(PkColList::from_slice(&[col]), &[], start, end));
    let p = |x| key_image(TypeCode::I64, FixedInt::I64.pack(x));
    let key = 5u64.to_be_bytes();
    type Keep<'a> = &'a dyn Fn(usize) -> bool;
    let preds: [(&str, &[u8], Keep<'_>); 2] = [
        ("none", &[], &|_| true),
        ("v > 3", &v_gt_3, &|row| row > 3 && !null(row)),
    ];
    let bounds: [(&str, ReadBound, Keep<'_>); 4] = [
        ("full scan", ReadBound::None, &|_| true),
        ("key set", ReadBound::PkSet(PkKeys::from_keys(8, [&key[..]])), &|_| true),
        ("pk in [3, 80]", walk(0, Cut::before(3), Cut::after(80)), &|row| {
            (2..80).contains(&row)
        }),
        ("v in [6, 12)", walk(1, Cut::before(p(6)), Cut::before(p(12))), &|row| {
            (6..12).contains(&row) && !null(row)
        }),
    ];
    for (pred_name, pred, pred_keeps) in preds {
        for (bound_name, bound, bound_keeps) in &bounds {
            let label = format!("{pred_name} × {bound_name}");
            let mut f = crate::RowFilter::for_read(pred, bound, &schema).unwrap();
            assert_eq!(
                f.keeps_every_row(),
                pred.is_empty() && !matches!(bound, ReadBound::Range(_)),
                "{label}"
            );
            for n in [100, 0, 10] {
                let mb = make_n_col_view(&schema, n, |row, _| row as i64, |row, _| null(row));
                assert_eq!(
                    passing_rows(&mut f, &mb),
                    (0..n)
                        .map(|row| pred_keeps(row) && bound_keeps(row))
                        .collect::<Vec<_>>(),
                    "{label}: n={n}"
                );
            }
        }
    }
}
