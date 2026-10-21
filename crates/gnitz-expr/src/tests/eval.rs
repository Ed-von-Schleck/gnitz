//! The evaluator types' read-backs, and the 3VL / bit_only / AND-chain behaviour
//! visible through them.

use crate::SchemaColumn;
use crate::{ConstIdx, Reg, Sink};
use gnitz_wire::{FixedInt, TypeCode};

use crate::batch::MORSEL;
use crate::eval::Resolved;
use crate::simd::encode_f64;
use crate::test_support::{
    filter_prog, i64s, is_not_null_op, is_null_op, make_n_col_view, map_prog, map_rows, passing_rows, row_values,
    scalar_prog, schema_pk, schema_pk_ints, verdicts, view, Cell, Col, TestView,
};
use crate::SchemaDescriptor;
use crate::{CmpOp, IntArithOp, LogicalInstr};

/// A map writes each computed slot — zeroing a NULL result's cell — and moves
/// each copied column's null bit into its slot. A BOOLEAN slot stores 1 for any
/// non-zero register value.
#[test]
fn a_map_writes_its_computed_slots_and_moves_the_copied_null_bits() {
    let n = MORSEL + 7;
    let int = |row: usize| row as i64 - 100;
    let int_null = |row: usize| row.is_multiple_of(5);
    let text = |row: usize| format!("r{row}-{}", "x".repeat(row % 20));
    let text_null = |row: usize| row.is_multiple_of(7);
    let (in_schema, mb) = view(&[
        i64s((0..n).map(|row| (!int_null(row)).then(|| int(row)))),
        Col {
            tc: TypeCode::String,
            cells: (0..n)
                .map(|row| (!text_null(row)).then(|| Cell::Str(text(row).into_bytes())))
                .collect(),
        },
    ]);
    let out_schema = schema_pk(
        &[
            TypeCode::I64,
            TypeCode::I16,
            TypeCode::String,
            TypeCode::I64,
            TypeCode::Bool,
        ],
        true,
    );

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

    for (row, (word, cells)) in map_rows(&mut ev, &out_schema, &mb).into_iter().enumerate() {
        let bit = |slot: usize| word >> slot & 1 != 0;
        assert_eq!(bit(0), int_null(row), "row {row}: the copied column's bit");

        let cast = (!int_null(row)).then(|| i16::try_from(int(row) * 300).ok()).flatten();
        assert_eq!(
            (&cells[1][..], bit(1)),
            (&cast.unwrap_or(0).to_le_bytes()[..], cast.is_none()),
            "row {row}: I16 slot"
        );

        let upper = (!text_null(row)).then(|| text(row).to_uppercase().into_bytes());
        assert_eq!(
            (&cells[2], bit(2)),
            (&upper.clone().unwrap_or_default(), upper.is_none()),
            "row {row}: string slot"
        );

        let and = and3((!int_null(row)).then(|| int(row) > 0), Some(!text_null(row)));
        assert_eq!(
            (&cells[3][..], bit(3)),
            (&and.map_or(0, i64::from).to_le_bytes()[..], and.is_none()),
            "row {row}: AND"
        );

        assert_eq!(
            (&cells[4][..], bit(4)),
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

/// A filter's runs are the maximal runs of its verdicts, stitched across a
/// morsel boundary, never one per morsel — on both arms, with a failing first
/// and last row, and under a NOT, which sets bits past the batch's last row.
#[test]
fn filter_runs_are_maximal_across_morsels() {
    let gt_0 = vec![
        LogicalInstr::LoadCol { col: 1 },
        LogicalInstr::LoadConst { val: 0, unsigned: false },
        LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
    ];
    let not_le_0 = vec![
        LogicalInstr::LoadCol { col: 1 },
        LogicalInstr::LoadConst { val: 0, unsigned: false },
        LogicalInstr::Cmp { op: CmpOp::Le, a: Reg(0), b: Reg(1) },
        LogicalInstr::BoolNot { a: Reg(2) },
    ];
    let n = MORSEL + 8;
    type Pass = fn(usize, usize) -> bool;
    // Rows 0, 3 and MORSEL-1 fail, so the last two runs straddle and start at
    // the morsel boundary.
    let mut cases: Vec<(usize, Pass)> = vec![
        (n, |row, _| row != 0 && row != 3 && row != MORSEL - 1),
        (n, |_, _| true),
        (n, |_, _| false),
    ];
    // The last row fails, either side of a word's end.
    cases.extend([1, 63, 64, 65, 300].map(|n| (n, (|row, n| row % 3 != 2 && row + 1 != n) as Pass)));
    for (n, pass) in cases {
        for nullable in [false, true] {
            let schema = schema_pk_ints(1, nullable);
            let null = |row: usize| nullable && row % 5 == 1;
            let mb = make_n_col_view(&schema, n, |row, _| i64::from(pass(row, n)), |row, _| null(row));
            let want: Vec<bool> = (0..n).map(|row| pass(row, n) && !null(row)).collect();
            for instrs in [&gt_0, &not_le_0] {
                let mut ev = filter_prog(&schema, instrs.clone(), vec![]);
                assert_eq!(passing_rows(&mut ev, &mb), want, "n={n} nullable={nullable} {instrs:?}");
            }
        }
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

/// A 3VL reference as the integer a boolean register reads back.
fn as_value(rows: impl IntoIterator<Item = Option<bool>>) -> Vec<Option<i128>> {
    rows.into_iter().map(|w| w.map(i128::from)).collect()
}

/// A three-deep AND chain against 3VL — with NULLs scattered, everywhere, and
/// absent, where no register tracks NULLs.
#[test]
fn three_and_chain_matches_the_three_valued_reference() {
    // The columns cycle mod 5, not mod 4: with `> 1` on three columns whose
    // values are three *consecutive* residues, mod 4 admits no passing row.
    let value = |row: usize, col: usize| ((row + col) as i64) % 5;
    for (label, null_at) in NULL_ARRANGEMENTS {
        let nullable = label != "none";
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
        assert_eq!(
            filter_prog(&schema, instrs.clone(), vec![]).prog().null_regs == 0,
            !nullable,
            "{label}: wrong arm"
        );
        let n = 77;
        let mb = make_n_col_view(&schema, n, value, null_at);
        let want = (0..n).map(|row| {
            let clause = |col| (!null_at(row, col)).then(|| value(row, col) > 1);
            and3(and3(clause(0), clause(1)), clause(2))
        });
        assert_eq!(verdicts(&schema, instrs, vec![], &mb), as_value(want), "{label}");
    }
}

/// Every boolean combinator over the whole `{TRUE, FALSE, NULL}²` table, and a
/// bare load, which no boolean producer wrote, so the filter packs its
/// truthiness itself.
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

    let (a, b) = (LogicalInstr::LoadCol { col: 1 }, LogicalInstr::LoadCol { col: 2 });
    let binary = |is_or| LogicalInstr::BoolBinary { is_or, a: Reg(0), b: Reg(1) };
    type Reference = fn(Option<bool>, Option<bool>) -> Option<bool>;
    let combinators: [(&str, Vec<LogicalInstr>, Reference); 4] = [
        ("a", vec![a], |a, _| a),
        ("NOT a", vec![a, LogicalInstr::BoolNot { a: Reg(0) }], |a, _| {
            a.map(|a| !a)
        }),
        ("a AND b", vec![a, b, binary(false)], and3),
        ("a OR b", vec![a, b, binary(true)], or3),
    ];
    for (name, instrs, reference) in combinators {
        let want = cells.iter().map(|&[a, b]| reference(a, b));
        let got = verdicts(&schema, instrs, vec![], &mb);
        // A bare load reads back its value, not its truth.
        let got: Vec<Option<i128>> = got.into_iter().map(|v| v.map(|x| i128::from(x != 0))).collect();
        assert_eq!(got, as_value(want), "{name}");
    }
}

/// A SELECT feeding `BoolBinary` is read as a boolean although no boolean
/// producer wrote it, and so is its condition, a bare load: both must be packed
/// for the AND, on the nullable arm as on the fast one.
#[test]
fn a_select_feeding_a_boolean_is_read_as_its_truthiness() {
    let schema = schema_pk_ints(4, true);
    let instrs = vec![
        LogicalInstr::LoadCol { col: 1 }, // cond
        LogicalInstr::LoadCol { col: 2 }, // a
        LogicalInstr::LoadCol { col: 3 }, // b
        LogicalInstr::Select { cond: Reg(0), a: Reg(1), b: Reg(2) },
        LogicalInstr::LoadCol { col: 4 },
        LogicalInstr::BoolBinary { is_or: false, a: Reg(3), b: Reg(4) },
    ];
    let value = |row: usize, col: usize| (row / [1, 2, 4, 8][col] % 3) as i64;
    let null = |row: usize, col: usize| row % [5, 7, 11, 13][col] == 1;
    let n = 90;
    let mb = make_n_col_view(&schema, n, value, null);
    let truth = |row, col| (!null(row, col)).then(|| value(row, col) != 0);
    let want = (0..n).map(|row| {
        let chosen = if truth(row, 0) == Some(true) { 1 } else { 2 };
        and3(truth(row, chosen), truth(row, 3))
    });
    assert_eq!(verdicts(&schema, instrs, vec![], &mb), as_value(want));
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
/// resolve no nullable register and select the reference rows.
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
        assert_eq!(ev.prog().null_regs, 0, "{name}: must resolve no nullable register");
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
        assert_eq!(ev.prog().null_regs, 0);
        for (arrangement, null_pred) in NULL_ARRANGEMENTS {
            let mb = make_n_col_view(&in_schema, n, |row, _| row as i64, null_pred);
            let want: Vec<(u64, Vec<Vec<u8>>)> = (0..n)
                .map(|row| (0, vec![u64::from(null_pred(row, 0) ^ invert).to_le_bytes().to_vec()]))
                .collect();
            assert_eq!(
                map_rows(&mut ev, &out_schema, &mb),
                want,
                "{arrangement}/invert={invert}"
            );
        }
    }
}

/// Each register is NULL on exactly its nullable source's rows: a NOT NULL
/// column's null bit is never read, and each nullable one's is read at its slot.
#[test]
fn nullable_and_not_null_columns_side_by_side() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),    // 0: pk
            SchemaColumn::new(TypeCode::I64, true),     // 1: payload slot 0
            SchemaColumn::new(TypeCode::I64, false),    // 2: payload slot 1
            SchemaColumn::new(TypeCode::String, true),  // 3: payload slot 2
            SchemaColumn::new(TypeCode::String, false), // 4: payload slot 3
            SchemaColumn::new(TypeCode::F32, true),     // 5: payload slot 4
            SchemaColumn::new(TypeCode::F32, false),    // 6: payload slot 5
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
    assert_ne!(ev.prog().null_regs, 0);

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
    let schema = schema_pk(&[TypeCode::U64, TypeCode::I64], true);
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

/// A read's filter keeps the rows passing both its predicate and its walk.
#[test]
fn row_filter_keeps_the_intersection_of_its_predicate_and_its_bound() {
    use gnitz_wire::{key_image, KeyRange, PkColList};
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
    let walk = |col: u32, first, last| Some(KeyRange::new(PkColList::from_slice(&[col]), &[], first, last));
    let p = |x| key_image(TypeCode::I64, FixedInt::I64.pack(x));
    type Keep<'a> = &'a dyn Fn(usize) -> bool;
    let preds: [(&str, &[u8], Keep<'_>); 2] = [
        ("none", &[], &|_| true),
        ("v > 3", &v_gt_3, &|row| row > 3 && !null(row)),
    ];
    let bounds: [(&str, Option<KeyRange>, Keep<'_>); 3] = [
        ("full scan", None, &|_| true),
        ("pk in [3, 80]", walk(0, 3, 80), &|row| (2..80).contains(&row)),
        ("v in [6, 11]", walk(1, p(6), p(11)), &|row| {
            (6..12).contains(&row) && !null(row)
        }),
    ];
    for (pred_name, pred, pred_keeps) in preds {
        for (bound_name, bound, bound_keeps) in &bounds {
            let label = format!("{pred_name} × {bound_name}");
            let mut f = crate::RowFilter::for_read(pred, bound.as_ref(), &schema).unwrap();
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
