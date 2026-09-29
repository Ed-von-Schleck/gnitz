//! The evaluator types' read-backs, and the 3VL / bit_only / AND-chain behaviour
//! visible through them.

use crate::{ConstIdx, Reg, Sink};
use gnitz_wire::{FixedInt, TypeCode};

use crate::batch::{encode_f64, MORSEL};
use crate::eval::Resolved;
use crate::test_support::{
    both_arms, filter_prog, is_not_null_op, is_null_op, make_int_view, make_n_col_view, map_prog, passing_ranges,
    passing_rows, row_strs, row_values, scalar_prog, schema_pk_ints, schema_pk_strings, FilterShape, TestOut,
    TestSchema, TestView,
};
use crate::{CmpOp, IntArithOp, LogicalInstr, SchemaFacts};

/// A map writes every computed slot at `dst_start` on — a narrowed integer, a
/// string, and a boolean — across a morsel boundary, and moves each copied
/// column's null bit into its output slot; the copied bytes themselves are the
/// caller's. A NULL result sets its slot's bit and zeroes the cell.
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
        LogicalInstr::LoadColInt { col: 1 },
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
    let sinks = vec![Sink::Col(1), Sink::Reg(Reg(3)), Sink::Reg(Reg(5)), Sink::Reg(Reg(9))];
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
    let mut out = TestOut::new(n + dst_start, &[8, 2, 16, 8]);
    ev.write_computed(&mb, 0, n, &mut out, dst_start);
    for row in 0..n {
        let dst = row + dst_start;
        let word = gnitz_wire::read_u64_le(&out.nulls, dst * 8);
        let bit = |slot: usize| word >> slot & 1 != 0;
        assert_eq!(bit(0), int_null(row), "row {row}: the copied column's bit");

        let cast = (!int_null(row)).then(|| i16::try_from(int(row) * 300).ok()).flatten();
        let cell = i16::from_le_bytes(out.cols[1][dst * 2..dst * 2 + 2].try_into().unwrap());
        assert_eq!(
            (cell, bit(1)),
            (cast.unwrap_or(0), cast.is_none()),
            "row {row}: I16 slot"
        );

        let cell = &out.cols[2][dst * 16..dst * 16 + 16];
        match text_null(row) {
            true => assert_eq!((cell, bit(2)), (&[0u8; 16][..], true), "row {row}: NULL string"),
            false => {
                let got = gnitz_wire::german_string_content(cell, &out.blob);
                assert_eq!((got, bit(2)), (text(row).to_uppercase().as_bytes(), false), "row {row}");
            }
        }

        let and = and3((!int_null(row)).then(|| int(row) > 0), Some(!text_null(row)));
        let cell = i64::from_le_bytes(out.cols[3][dst * 8..dst * 8 + 8].try_into().unwrap());
        assert_eq!(
            (cell, bit(3)),
            (and.map_or(0, i64::from), and.is_none()),
            "row {row}: AND"
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

/// A fused string compare against a nullable column keeps the program on the
/// nullable arm, so a NULL row never satisfies `=` — across a morsel boundary.
#[test]
fn a_fused_string_compare_never_passes_a_null_row() {
    let schema = schema_pk_strings(1, true);
    let n = MORSEL + 20;
    // Odd rows are live and alternate "foo" / "bar"; even rows are NULL "foo".
    let mut mb = TestView::for_schema(&schema, n);
    for row in 0..n {
        mb.set_string(row, 0, if row % 4 == 3 { b"bar" } else { b"foo" });
        if row % 2 == 0 {
            mb.set_null(row, 0);
        }
    }
    let instrs = vec![LogicalInstr::StrColConst {
        op: CmpOp::Eq,
        col: 1,
        const_idx: ConstIdx(0),
    }];
    let mut ev = filter_prog(&schema, instrs, Reg(0), vec![b"foo".to_vec()]);
    assert_eq!(
        passing_rows(&mut ev, &mb),
        (0..n).map(|row| row % 4 == 1).collect::<Vec<_>>()
    );
}

/// `filter` reports maximal *runs*, not per-row verdicts: every other test here
/// collapses them into a `Vec<bool>`, which cannot tell one run from two
/// adjacent ones. Assert the exact `(start, end)` list — half-open, `end`
/// exclusive — across a leading gap, an interior gap, and a run that reaches the
/// last row, plus one spanning a morsel boundary so the per-morsel bitmap words
/// are stitched rather than flushed at 256.
///
/// Run over both nullability arms: they pack `filter_bits` by different routes
/// (`no_nulls` reads `regs`, the nullable arm merges `bool_bits & !null_bits`),
/// and `n = MORSEL + 8` gives the second morsel an 8-row tail, so a route that
/// mishandled a partial word would split or drop the run reaching row `n`.
#[test]
fn filter_emits_exact_maximal_ranges() {
    let n = MORSEL + 8;
    for nullable in [false, true] {
        let schema = schema_pk_ints(1, nullable);
        let instrs = vec![
            LogicalInstr::LoadColInt { col: 1 },
            LogicalInstr::LoadConst { val: 0, unsigned: false },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
        ];
        let mut ev = filter_prog(&schema, instrs, Reg(2), vec![]);
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
        // A run ending exactly on a 64-bit word boundary, with the next word all
        // zero: the empty word itself has to close it, since a bitmap walk driven
        // off the set bits never visits it.
        assert_eq!(runs(&|row| row < 64), vec![(0, 64)]);
        // The same, two words on: the gap word is interior rather than trailing.
        assert_eq!(runs(&|row| !(64..192).contains(&row)), vec![(0, 64), (192, n)]);
    }
}

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

/// The left-deep chain `col0 > k AND col1 > 1 AND col2 > 1`, as a filter and as
/// a scalar, against a per-row 3VL reference at every morsel boundary in
/// 1..=MORSEL+1 — m < 64, m = 64 exactly, m crossing 64, the full MORSEL, and
/// the multi-morsel case. The scalar read is the one that tells NULL from FALSE;
/// a filter drops both.
///
/// The arrangements differ in how the non-survivors are excluded:
/// - `mixed` spreads failures and NULLs through every word.
/// - `clustered_leading` makes col0 monotonic and never NULL, so the leading
///   clause is definite-FALSE for all of morsel 0 and the survivors all land in
///   later morsels — the case a word-at-a-time kernel can treat differently from
///   a scattered one.
/// - `not_null` is the same data over a NOT NULL schema, which takes the
///   `no_nulls` arm.
/// - `all_null` fills every null word, and must still reject every row.
#[test]
fn three_and_chain_boundary_sweep() {
    // col1/col2 cycle mod 5, not mod 4: with `> 1` on three columns whose values
    // are three *consecutive* residues, mod 4 admits no passing row at all.
    type Arrangement = (
        &'static str,
        bool,
        i64,
        fn(usize, usize) -> i64,
        fn(usize, usize) -> bool,
    );
    let cycle: fn(usize, usize) -> i64 = |row, col| ((row + col) as i64) % 5;
    let spread: fn(usize, usize) -> bool = |row, col| match col {
        0 => row % 5 == 0,
        1 => row % 7 == 0,
        _ => row % 11 == 0,
    };
    let arrangements: [Arrangement; 4] = [
        ("mixed", true, 1, cycle, spread),
        (
            "clustered_leading",
            true,
            255,
            |row, col| if col == 0 { row as i64 } else { ((row + col) as i64) % 5 },
            |row, col| match col {
                0 => false,
                1 => row % 7 == 0,
                _ => row % 11 == 0,
            },
        ),
        ("not_null", false, 1, cycle, |_, _| false),
        ("all_null", true, 1, cycle, |_, _| true),
    ];

    for (label, nullable, k0, value, null_at) in arrangements {
        let schema = schema_pk_ints(3, nullable);
        let instrs = vec![
            LogicalInstr::LoadColInt { col: 1 },                             // r0 = col0
            LogicalInstr::LoadConst { val: k0, unsigned: false },            // r1 = k0
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },       // r2 = col0 > k0
            LogicalInstr::LoadColInt { col: 2 },                             // r3 = col1
            LogicalInstr::LoadConst { val: 1, unsigned: false },             // r4 = 1
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(3), b: Reg(4) },       // r5 = col1 > 1
            LogicalInstr::BoolBinary { is_or: false, a: Reg(2), b: Reg(5) }, // r6 = r2 AND r5
            LogicalInstr::LoadColInt { col: 3 },                             // r7 = col2
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(7), b: Reg(4) },       // r8 = col2 > 1
            LogicalInstr::BoolBinary { is_or: false, a: Reg(6), b: Reg(8) }, // r9 = r6 AND r8
        ];
        let mut filter = filter_prog(&schema, instrs.clone(), Reg(9), vec![]);
        let mut scalar = scalar_prog(&schema, instrs, Reg(9), vec![]);
        assert_eq!(filter.prog().no_nulls, !nullable, "{label}: wrong arm");

        for &n in &[1, 7, 63, 64, 65, 127, 128, 255, 256, 257, 300] {
            let mb = make_n_col_view(&schema, n, value, null_at);
            let want: Vec<Option<bool>> = (0..n)
                .map(|row| {
                    let clause = |col: usize| {
                        let k = if col == 0 { k0 } else { 1 };
                        (!null_at(row, col)).then(|| value(row, col) > k)
                    };
                    and3(and3(clause(0), clause(1)), clause(2))
                })
                .collect();
            assert_eq!(
                passing_rows(&mut filter, &mb),
                want.iter().map(|&w| w == Some(true)).collect::<Vec<_>>(),
                "{label}: filter at n={n}"
            );
            assert_eq!(
                row_values(&mut scalar, &mb),
                want.iter().map(|w| w.map(i128::from)).collect::<Vec<_>>(),
                "{label}: scalar at n={n}"
            );
        }
    }
}

/// The complete `{TRUE, FALSE, NULL}²` table for every boolean combinator, as a
/// filter and as a scalar, against the references above. Swept rather than
/// hand-listed: the asymmetric cells are the whole content of 3VL, and a
/// hand-written list of them silently omitted `T AND T` and `F OR F`.
///
/// TRUE is stored as `-5`, so truthiness is `!= 0` rather than `== 1`; a NULL
/// row stores that same truthy value, so only the null bit keeps it out of the
/// definite-true term. The bare load is a filter whose result register no bool
/// producer wrote, so the filter packs its truthiness itself.
#[test]
fn every_boolean_combinator_covers_the_whole_three_valued_table() {
    let schema = schema_pk_ints(2, true);
    const STATES: [Option<bool>; 3] = [Some(true), Some(false), None];
    let cells: Vec<(Option<bool>, Option<bool>)> = STATES.iter().flat_map(|&a| STATES.map(|b| (a, b))).collect();
    let mb = make_n_col_view(
        &schema,
        cells.len(),
        |row, col| {
            let (a, b) = cells[row];
            if [a, b][col] == Some(false) {
                0
            } else {
                -5
            }
        },
        |row, col| [cells[row].0, cells[row].1][col].is_none(),
    );

    type Combinator = (
        &'static str,
        LogicalInstr,
        fn(Option<bool>, Option<bool>) -> Option<bool>,
    );
    let combinators: [Combinator; 4] = [
        ("a", LogicalInstr::LoadConst { val: 0, unsigned: false }, |a, _| a),
        ("NOT a", LogicalInstr::BoolNot { a: Reg(0) }, |a, _| a.map(|a| !a)),
        (
            "a AND b",
            LogicalInstr::BoolBinary { is_or: false, a: Reg(0), b: Reg(1) },
            and3,
        ),
        (
            "a OR b",
            LogicalInstr::BoolBinary { is_or: true, a: Reg(0), b: Reg(1) },
            or3,
        ),
    ];
    for (name, op, reference) in combinators {
        let (instrs, result) = match name {
            "a" => (vec![LogicalInstr::LoadColInt { col: 1 }], Reg(0)),
            _ => (
                vec![
                    LogicalInstr::LoadColInt { col: 1 },
                    LogicalInstr::LoadColInt { col: 2 },
                    op,
                ],
                Reg(2),
            ),
        };
        let want: Vec<Option<bool>> = cells.iter().map(|&(a, b)| reference(a, b)).collect();
        let mut filter = filter_prog(&schema, instrs.clone(), result, vec![]);
        assert_eq!(
            passing_rows(&mut filter, &mb),
            want.iter().map(|&w| w == Some(true)).collect::<Vec<_>>(),
            "{name} as a filter"
        );
        if name != "a" {
            assert_eq!(
                row_values(&mut scalar_prog(&schema, instrs, result, vec![]), &mb),
                want.iter().map(|w| w.map(i128::from)).collect::<Vec<_>>(),
                "{name} as a scalar"
            );
        }
    }
}

/// The filter's nullable-arm tail mask. `BoolNot` is the op that dirties the
/// tail: it complements whole `bool_bits` words (`!va & !na`), so every bit
/// above `m % 64` of the last word comes out set, and the word merge into
/// `filter_bits` carries them through. The loaded nullable column is what keeps
/// the program off the `no_nulls` arm, where the verdict is packed out of `regs`
/// and no such word exists.
///
/// A phantom bit cannot pass a real row — it sits at row `n` or above, so the
/// run it opens is the degenerate `(n, n)`, which `passing_ranges` refuses. That
/// only bites when the row directly under the tail *fails*: otherwise the
/// phantom merges into a real run ending at `n`. Every `n` here is chosen so row
/// `n - 1` fails.
#[test]
fn bool_not_tail_mask() {
    let schema = schema_pk_ints(1, true);
    for &n in &[1, 65, 300] {
        // NOT (col1 >= 0), with col1 cycling -1, 0, 1 and NULL every 5th row.
        let mb = make_n_col_view(&schema, n, |row, _| (row % 3) as i64 - 1, |row, _| row % 5 == 0);
        let instrs = vec![
            LogicalInstr::LoadColInt { col: 1 },
            LogicalInstr::LoadConst { val: 0, unsigned: false },
            LogicalInstr::Cmp { op: CmpOp::Ge, a: Reg(0), b: Reg(1) },
            LogicalInstr::BoolNot { a: Reg(2) },
        ];
        let mut ev = filter_prog(&schema, instrs, Reg(3), vec![]);
        assert!(!ev.prog().no_nulls);
        // 3VL: NOT NULL is NULL, which the filter drops.
        let want: Vec<bool> = (0..n).map(|row| row % 5 != 0 && row % 3 == 0).collect();
        assert_eq!(passing_rows(&mut ev, &mb), want, "n={n}");
    }
}

/// Row counts straddling the 64-bit word and the 256-row morsel, for every
/// arm sweep below.
const ARM_SWEEP_ROWS: [usize; 7] = [63, 64, 65, 255, 256, 257, 300];

/// A named null arrangement: the `null_pred` a sweep hands `make_n_col_view`.
/// Spelled as an alias because the inline tuple trips `clippy::type_complexity`.
type NullArrangement = (&'static str, fn(usize, usize) -> bool);

/// The null arrangements the sweeps run every shape over: the two extremes plus
/// two mixed densities. `none` and `all` make every null test uniformly definite
/// for a whole morsel; `spread` scatters NULLs through every 64-bit word, while
/// `clustered` gives alternating words that are entirely NULL or entirely not —
/// the difference a word-at-a-time kernel could see and a per-row one cannot.
const ARM_SWEEP_NULLS: [NullArrangement; 4] = [
    ("none", |_, _| false),
    ("all", |_, _| true),
    ("spread", |row, col| (row + col) % 3 == 0),
    ("clustered", |row, _| (row / 64) % 2 == 0),
];

/// A null-test predicate's verdict given whether each column is NULL.
type NullReference = fn(&dyn Fn(u32) -> bool) -> bool;

/// Predicates whose only contact with a nullable column is a null test, so they
/// resolve onto the `no_nulls` arm, each with its reference verdict. Against
/// `schema_pk_ints(3, true)`.
fn null_test_shapes() -> Vec<(&'static str, FilterShape, NullReference)> {
    vec![
        ("is_null", (vec![is_null_op(1)], Reg(0)), |n| n(1)),
        ("is_not_null", (vec![is_not_null_op(1)], Reg(0)), |n| !n(1)),
        // A PK column carries no null bit, so its null test is the constant.
        ("pk_is_null", (vec![is_null_op(0)], Reg(0)), |_| false),
        ("pk_is_not_null", (vec![is_not_null_op(0)], Reg(0)), |_| true),
        (
            "and",
            (
                vec![
                    is_null_op(1),
                    is_not_null_op(2),
                    LogicalInstr::BoolBinary { is_or: false, a: Reg(0), b: Reg(1) },
                ],
                Reg(2),
            ),
            |n| n(1) && !n(2),
        ),
        (
            "or",
            (
                vec![
                    is_null_op(1),
                    is_null_op(2),
                    LogicalInstr::BoolBinary { is_or: true, a: Reg(0), b: Reg(1) },
                ],
                Reg(2),
            ),
            |n| n(1) || n(2),
        ),
        (
            "not",
            (vec![is_null_op(1), LogicalInstr::BoolNot { a: Reg(0) }], Reg(1)),
            |n| !n(1),
        ),
        // Three conjuncts: the accumulator spine is two ANDs long and a null
        // test feeds another AND rather than the result register directly.
        (
            "and_chain",
            (
                vec![
                    is_null_op(1),
                    is_null_op(2),
                    LogicalInstr::BoolBinary { is_or: false, a: Reg(0), b: Reg(1) },
                    is_null_op(3),
                    LogicalInstr::BoolBinary { is_or: false, a: Reg(2), b: Reg(3) },
                ],
                Reg(4),
            ),
            |n| n(1) && n(2) && n(3),
        ),
        // CASE WHEN col1 IS NULL THEN 1 ELSE 0 END — the null test as a SELECT
        // condition, which the nullable arm reads out of `bool_bits` and the
        // `no_nulls` arm out of `regs`.
        (
            "case_cond",
            (
                vec![
                    is_null_op(1),
                    LogicalInstr::LoadConst { val: 1, unsigned: false },
                    LogicalInstr::LoadConst { val: 0, unsigned: false },
                    LogicalInstr::Select { cond: Reg(0), a: Reg(1), b: Reg(2) },
                ],
                Reg(3),
            ),
            |n| n(1),
        ),
    ]
}

/// A program that resolves `no_nulls` because its only contact with a nullable
/// column is `IS [NOT] NULL` must select the reference rows on that arm and,
/// forced, on the nullable one. The two are different code — the fast arm
/// computes in `regs` and packs the verdict once, the nullable arm computes in
/// packed `bool_bits`/`null_bits`.
#[test]
fn is_null_shapes_select_the_reference_rows_on_both_arms() {
    let schema = schema_pk_ints(3, true);
    for (name, (instrs, result_reg), reference) in null_test_shapes() {
        // One evaluator per arm for the whole sweep, reused across row counts as
        // the engine reuses one.
        let (mut fast, mut nullable) = both_arms(name, || filter_prog(&schema, instrs.clone(), result_reg, vec![]));
        for &n in &ARM_SWEEP_ROWS {
            for (arrangement, null_pred) in ARM_SWEEP_NULLS {
                let mb = make_n_col_view(&schema, n, |row, col| ((row + col) % 5) as i64, null_pred);
                let want: Vec<bool> = (0..n)
                    .map(|row| reference(&|col| col > 0 && null_pred(row, col as usize - 1)))
                    .collect();
                assert_eq!(passing_rows(&mut fast, &mb), want, "{name}/{arrangement}: fast, n={n}");
                assert_eq!(
                    passing_rows(&mut nullable, &mb),
                    want,
                    "{name}/{arrangement}: nullable, n={n}"
                );
            }
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
    for invert in [false, true] {
        let (mut fast, mut nullable) = both_arms("map", || {
            map_prog(
                &in_schema,
                &out_schema,
                vec![LogicalInstr::IsNull { col: 1, invert }],
                vec![Sink::Reg(Reg(0))],
                vec![],
            )
        });
        for &n in &ARM_SWEEP_ROWS {
            for (arrangement, null_pred) in ARM_SWEEP_NULLS {
                let mb = make_n_col_view(&in_schema, n, |row, _| row as i64, null_pred);
                let want: Vec<i64> = (0..n).map(|row| i64::from(null_pred(row, 0) ^ invert)).collect();
                for (arm, ev) in [("fast", &mut fast), ("nullable", &mut nullable)] {
                    let mut out = TestOut::new(n, &[8]);
                    ev.write_computed(&mb, 0, n, &mut out, 0);
                    let got: Vec<i64> = out.cols[0]
                        .as_chunks::<8>()
                        .0
                        .iter()
                        .map(|c| i64::from_le_bytes(*c))
                        .collect();
                    assert_eq!(got, want, "{arm}/{arrangement}/invert={invert}: n={n}");
                    assert!(out.nulls.iter().all(|&b| b == 0), "{arm}: a null test is never NULL");
                }
            }
        }
    }
}

/// `nullable_slots` is indexed by payload slot, not column index, so an
/// all-`NOT NULL` schema — where the mask is empty — cannot catch an off-by-one
/// in how it is built. Load a nullable column beside a `NOT NULL` one, which is
/// also what keeps the program on the nullable arm without forcing it, and pin
/// each register's NULL rows absolutely.
///
/// Every `NOT NULL` slot carries a forged bit in the batch's bitmap, on a
/// different row set than its nullable neighbour, so a gather against a
/// misaligned mask — or a `NOT NULL` load that read the bitmap at all — reports
/// NULL on rows the right one never touches.
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
    // Which rows carry a set bit for each payload slot. The odd slots are
    // declared NOT NULL, so theirs are forged — and set on rows their nullable
    // neighbour's are not.
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
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadColInt { col: 2 },
        LogicalInstr::LoadColFloat { col: 5 },
        LogicalInstr::LoadColFloat { col: 6 },
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
    let mut ev = scalar_prog(&schema, instrs, Reg(0), vec![b"m".to_vec()]);
    assert!(!ev.prog().no_nulls);

    // Per register, the rows it must report NULL on. Every `NOT NULL` load and
    // compare reports none, forged bit or not.
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

/// `eval_all` returns the arm its program's result class fixes, one entry per
/// row across a morsel boundary — the NULL blanking offset by the morsel's
/// first row, and the string arm's spans into one shared arena.
#[test]
fn eval_all_reports_one_result_per_row_in_its_own_class() {
    let schema = schema_pk_ints(2, true);
    let n = MORSEL + 7;
    let mb = make_n_col_view(
        &schema,
        n,
        |row, col| ((row * 7 + col) % 5) as i64,
        |row, _| row.is_multiple_of(11),
    );
    // A divide, so a zero divisor makes NULLs the operands' own nullability
    // does not.
    let mut div = scalar_prog(
        &schema,
        vec![
            LogicalInstr::LoadColInt { col: 1 },
            LogicalInstr::LoadColInt { col: 2 },
            LogicalInstr::IntArith {
                op: IntArithOp::Div,
                a: Reg(0),
                b: Reg(1),
            },
        ],
        Reg(2),
        vec![],
    );
    let want: Vec<Option<i128>> = (0..n)
        .map(|row| {
            let (a, b) = ((row * 7 % 5) as i128, ((row * 7 + 1) % 5) as i128);
            (!row.is_multiple_of(11) && b != 0).then(|| a / b)
        })
        .collect();
    assert_eq!(row_values(&mut div, &mb), want);

    let str_schema = schema_pk_strings(1, true);
    // Alternating either side of the 12-byte inline boundary, so both cell forms
    // cross the morsel boundary.
    let text = |row: usize| format!("r{row}-{}", "x".repeat(row % 20));
    let mut sv = TestView::for_schema(&str_schema, n);
    for row in 0..n {
        sv.set_string(row, 0, text(row).as_bytes());
        if row.is_multiple_of(9) {
            sv.set_null(row, 0);
        }
    }
    let mut upper = scalar_prog(
        &str_schema,
        vec![
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::StrCase { a: Reg(0), upper: true },
        ],
        Reg(1),
        vec![],
    );
    let want: Vec<Option<Vec<u8>>> = (0..n)
        .map(|row| (!row.is_multiple_of(9)).then(|| text(row).to_uppercase().into_bytes()))
        .collect();
    assert_eq!(row_strs(&mut upper, &sv), want);
}

/// An integer result widens by the result register's resolve-time U64 tracking,
/// and every opcode with an unsigned form takes it when an operand is tracked
/// unsigned: a U64 load, an unsigned constant, and arithmetic, division, the
/// float lift, a CASE blend and a MIN/MAX fold over one read unsigned; a signed
/// load or constant, a comparison and a cast to a signed type do not.
#[test]
fn int_results_widen_by_the_result_registers_signedness() {
    let schema = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::U64, true), (TypeCode::I64, true)],
        &[0],
    );
    // Bit 63 set, so an unsigned and a signed reading differ.
    let big = (1u64 << 63) | 6;
    let mb = make_int_view(&schema, &[(1, 0, &[big as i64, -1])]);
    let u64_col = LogicalInstr::LoadColInt { col: 1 };
    let i64_col = LogicalInstr::LoadColInt { col: 2 };
    let k = |val| LogicalInstr::LoadConst { val, unsigned: false };
    let arith = |op| LogicalInstr::IntArith { op, a: Reg(0), b: Reg(1) };
    let max = |a, b| LogicalInstr::IntMinMax2 { a: Reg(a), b: Reg(b), is_max: true };
    for (label, instrs, want) in [
        ("u64 load", vec![u64_col], i128::from(big)),
        (
            "u64 + 1",
            vec![u64_col, k(1), arith(IntArithOp::Add)],
            i128::from(big + 1),
        ),
        (
            "u64 / 2",
            vec![u64_col, k(2), arith(IntArithOp::Div)],
            i128::from(big / 2),
        ),
        ("u64 % 4", vec![u64_col, k(4), arith(IntArithOp::Mod)], 2),
        (
            "u64 as float",
            vec![u64_col, LogicalInstr::IntToFloat { a: Reg(0) }],
            i128::from(encode_f64(big as f64)),
        ),
        ("i64 load", vec![i64_col], -1),
        (
            "unsigned constant",
            vec![LogicalInstr::LoadConst { val: -1, unsigned: true }],
            i128::from(u64::MAX),
        ),
        ("signed constant", vec![k(-1)], -1),
        (
            "u64 > 1",
            vec![u64_col, k(1), LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) }],
            1,
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
            1,
        ),
        // Either operand tracked U64 makes a fold unsigned, and its result stays
        // tracked, so a follow-on fold against a signed value is unsigned too.
        (
            "MAX(MAX(u64, 1), 1)",
            vec![u64_col, k(1), max(0, 1), max(2, 1)],
            i128::from(big),
        ),
        (
            "MAX(MAX(1, u64), 1)",
            vec![k(1), u64_col, max(0, 1), max(2, 0)],
            i128::from(big),
        ),
    ] {
        let result = Reg(instrs.len() as u16 - 1);
        assert_eq!(
            row_values(&mut scalar_prog(&schema, instrs, result, vec![]), &mb),
            [Some(want)],
            "{label}"
        );
    }
    let cast = vec![u64_col, LogicalInstr::IntCast { a: Reg(0), fi: FixedInt::I64 }];
    assert_eq!(
        row_values(&mut scalar_prog(&schema, cast, Reg(1), vec![]), &mb),
        [None],
        "CAST(u64 AS BIGINT) of a value past i64::MAX"
    );
}

// ---------------------------------------------------------------------------
// RowFilter: predicate × range walk
// ---------------------------------------------------------------------------

/// `pk U64, v I64 nullable` over `n` rows: `pk = row + 1`, `v = row`, NULL on
/// every fifth row.
fn row_filter_fixture(n: usize) -> (TestSchema, TestView) {
    let schema = schema_pk_ints(1, true);
    let v = make_n_col_view(&schema, n, |row, _| row as i64, |row, _| row.is_multiple_of(5));
    (schema, v)
}

/// `v > 3` as the wire blob a read ships.
fn v_gt_3_blob() -> Vec<u8> {
    crate::LogicalProgram::new(
        vec![
            LogicalInstr::LoadColInt { col: 1 },
            LogicalInstr::LoadConst { val: 3, unsigned: false },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
        ],
        vec![Sink::Reg(Reg(2))],
        vec![],
    )
    .to_blob_bytes()
}

/// `pk` in `[lo, hi]`, as the bound a PK range read carries.
fn pk_between(lo: i128, hi: i128) -> gnitz_wire::ReadBound {
    use gnitz_wire::{Cut, KeyRange, PkColList};
    gnitz_wire::ReadBound::Range(KeyRange::new(
        PkColList::from_slice(&[0]),
        &[],
        Cut::before(lo as u128),
        Cut::after(hi as u128),
    ))
}

/// Each arm of the predicate × walk product, over a two-word batch, an empty
/// one, and a narrower one read after it — the survivors of both are the
/// intersection of each alone.
#[test]
fn row_filter_intersects_its_predicate_and_its_walk() {
    let none = gnitz_wire::ReadBound::None;
    let walk = pk_between(3, 80);
    let passes_pred = |row: usize| row > 3 && !row.is_multiple_of(5);
    let in_walk = |row: usize| (2..80).contains(&row);
    for (arm, pred, bound, keep) in [
        ("none", Vec::new(), &none, &(|_| true) as &dyn Fn(usize) -> bool),
        ("predicate", v_gt_3_blob(), &none, &passes_pred),
        ("walk", Vec::new(), &walk, &in_walk),
        ("both", v_gt_3_blob(), &walk, &|row| passes_pred(row) && in_walk(row)),
    ] {
        let (schema, _) = row_filter_fixture(0);
        let mut f = crate::RowFilter::for_read(&pred, bound, &schema).unwrap();
        assert_eq!(f.keeps_every_row(), arm == "none", "{arm}");
        for n in [100, 0, 10] {
            let (_, mb) = row_filter_fixture(n);
            assert_eq!(
                passing_rows(&mut f, &mb),
                (0..n).map(keep).collect::<Vec<_>>(),
                "{arm}: n={n}"
            );
        }
    }
}

/// `for_read` walks a PK range over the PK columns, an index range over its own
/// columns, and nothing for a full scan or a key set.
#[test]
fn row_filter_for_read_walks_each_bound_variant() {
    use gnitz_wire::{key_image, Cut, KeyRange, PkColList, PkKeys, ReadBound};
    let n = 20;
    let (schema, mb) = row_filter_fixture(n);
    let p = |x| key_image(TypeCode::I64, FixedInt::I64.pack(x));
    let index = ReadBound::Range(KeyRange::new(
        PkColList::from_slice(&[1]),
        &[],
        Cut::before(p(6)),
        Cut::before(p(12)),
    ));
    let key = 5u64.to_be_bytes();
    let set = ReadBound::PkSet(PkKeys::from_keys(8, [&key[..]]));
    for (label, bound, keep) in [
        ("None", ReadBound::None, &(|_| true) as &dyn Fn(usize) -> bool),
        ("pk range", pk_between(3, 7), &|row| (2..7).contains(&row)),
        // The index holds no NULL, so the walk drops row 10's.
        ("index range", index, &|row| (6..12).contains(&row) && row != 10),
        ("PkSet", set, &|_| true),
    ] {
        let mut f = crate::RowFilter::for_read(&[], &bound, &schema).unwrap();
        assert_eq!(
            passing_rows(&mut f, &mb),
            (0..n).map(keep).collect::<Vec<_>>(),
            "{label}"
        );
    }
}
