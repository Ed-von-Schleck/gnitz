//! Driving a resolved program through [`crate::Evaluator`]: the filter over a
//! whole batch, the m=1 row read, the 3VL / bit_only / AND-chain behaviour
//! visible at that surface, and the map-side accessors an engine consumer reads
//! a resolved program through.

use crate::{ConstIdx, Reg, Sink};
use gnitz_wire::type_code;

use crate::batch::MORSEL;
use crate::test_support::{
    both_arms, filter_prog, is_not_null_op, is_null_op, make_int_view, make_n_col_view, map_prog, passing_ranges,
    passing_rows, push_payload_cols, row_value, scalar_prog, schema_pk_ints, schema_pk_strings, set_row_pk,
    FilterShape, TestSchema, TestView,
};
use crate::{CmpOp, Evaluator, ExprResults, IntArithOp, LogicalInstr};

/// True iff `ev`'s predicate passes for `row`.
fn passes(ev: &Evaluator, mb: &TestView, row: usize) -> bool {
    row_value(ev, mb, row).is_some_and(|val| val != 0)
}

/// The map-side surface an engine consumer reads a resolved program through:
/// which columns are copied verbatim, which are written out of the register
/// file, and which output slots admit NULL. Every one of these is read by
/// `gnitz-server`'s columnar map, so a wrong answer here is a wrong output
/// column there — but none of them is visible through the three drive methods
/// the rest of this file exercises.
#[test]
fn a_resolved_map_reports_its_copies_emits_and_nullable_slots() {
    // in:  pk U64, 0: I64 nullable, 1: STRING nullable
    // out: pk U64, 0: I64 (copied verbatim), 1: I64 (computed), 2: STRING (computed)
    let in_schema = TestSchema::new(
        &[
            (type_code::U64, false),
            (type_code::I64, true),
            (type_code::STRING, true),
        ],
        &[0],
    );
    let out_schema = TestSchema::new(
        &[
            (type_code::U64, false),
            (type_code::I64, true),
            (type_code::I64, false),
            (type_code::STRING, true),
        ],
        &[0],
    );
    let instrs = vec![LogicalInstr::LoadConst { val: 7 }, LogicalInstr::LoadColStr { col: 2 }];
    let sinks = vec![Sink::Col(1), Sink::Reg(Reg(0)), Sink::Reg(Reg(1))];
    let ev = map_prog(&in_schema, &out_schema, instrs, sinks, vec![]);

    assert!(ev.emits_anything(), "this map writes two slots out of the registers");
    // The scalar and string emits are two halves of one list, split by class.
    assert_eq!(ev.scalar_emits(), &[(0, 1)]);
    assert_eq!(ev.str_emits(), &[(1, 2)]);
    // One verbatim move: input column 1 into output slot 0, at the output width.
    let copies: Vec<(u32, u8)> = ev.copies().iter().map(|&(_, out, w)| (out, w)).collect();
    assert_eq!(copies, vec![(0, 8)]);
    // Bit N set iff *input* payload slot N admits NULL — the schema the program
    // reads through, not the one it writes. Both input payload columns are
    // nullable; the output's non-nullable slot 1 does not appear here.
    assert_eq!(ev.nullable_slots(), 0b11);
    assert!(!ev.result_is_str(), "a map's result register is meaningless");

    // A pure projection emits nothing, so driving its kernel cannot change the
    // output — the property `emits_anything` exists to let a caller skip it.
    let projection = map_prog(
        &in_schema,
        &TestSchema::new(&[(type_code::U64, false), (type_code::I64, true)], &[0]),
        Vec::new(),
        vec![Sink::Col(1)],
        vec![],
    );
    assert!(!projection.emits_anything());
    assert!(projection.scalar_emits().is_empty() && projection.str_emits().is_empty());
}

/// `[(0, n)]` is the agreed spelling of "no predicate", and `filter_ranges`
/// must clear the caller's buffer, which is reused across chunks.
#[test]
fn filter_ranges_collects_into_a_reused_buffer() {
    let schema = schema_pk_ints(1, true);
    let mb = make_n_col_view(&schema, 8, |row, _| i64::from(row < 3 || row == 7), |_, _| false);
    let ev = filter_prog(
        &schema,
        vec![
            LogicalInstr::LoadColInt { col: 1 },
            LogicalInstr::LoadConst { val: 0 },
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                a: Reg(0),
                b: Reg(1),
            },
        ],
        Reg(2),
        vec![],
    );

    let mut out = vec![(99, 99)];
    ev.filter_ranges(&mb, &mut out);
    assert_eq!(out, vec![(0, 3), (7, 8)], "the stale entry must be cleared");
    assert_eq!(out, passing_ranges(&ev, &mb), "both readers report one run list");

    // An all-pass predicate is the single range covering the batch.
    let all = make_n_col_view(&schema, 8, |_, _| 1, |_, _| false);
    ev.filter_ranges(&all, &mut out);
    assert_eq!(out, vec![(0, 8)]);
}

/// The batch filter and the m=1 row read are two drives of one program and must
/// agree row for row.
#[test]
fn filter_agrees_with_the_row_read() {
    let schema = schema_pk_ints(1, true);
    // Pass iff col[1] > 15.
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 }, // r0 = col[1] (payload[0])
        LogicalInstr::LoadConst { val: 15 }, // r1 = 15
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            a: Reg(0),
            b: Reg(1),
        }, // r2 = r0 > r1
    ];
    let ev = filter_prog(&schema, instrs, Reg(2), vec![]);

    let rows: &[(u64, u64, &[i64])] = &[(1, 0, &[5]), (2, 0, &[15]), (3, 0, &[25]), (4, 0, &[0])];
    let mb = make_int_view(&schema, rows);

    let passing = passing_rows(&ev, &mb);

    for (i, &(_, _, vals)) in rows.iter().enumerate() {
        assert_eq!(passing[i], passes(&ev, &mb, i), "row {i}: val={}", vals[0]);
    }
    assert_eq!(passing, vec![false, false, true, false]);
}

/// A fused string compare against a nullable column keeps the program on the
/// nullable arm, so a NULL row can never satisfy `=`. Driven over enough rows to
/// cross a morsel and read back both ways: the batch filter and the `m = 1` row
/// read must agree, and neither may pass a NULL row.
#[test]
fn a_fused_string_compare_never_passes_a_null_row() {
    let schema = schema_pk_strings(1, true);

    // 20 rows: alternating null/non-null, with the non-null rows alternating
    // between "foo" (matches the predicate) and "bar" (does not).
    let n = 20usize;
    let mut mb = TestView::new(n, schema.pk_stride());
    push_payload_cols(&mut mb, &schema);
    for row in 0..n {
        set_row_pk(&mut mb, &schema, row, row as u64 + 1);
        mb.set_null_word(row, u64::from(row % 2 == 0)); // bit 0 = col1
        mb.set_string(row, 0, if row % 4 == 1 { b"foo" } else { b"bar" });
    }

    // Predicate: col1 = 'foo'. result_reg = 0.
    let instrs = vec![LogicalInstr::StrColConst {
        op: CmpOp::Eq,
        col: 1,
        const_idx: ConstIdx(0),
    }];
    let kind = filter_prog(&schema, instrs, Reg(0), vec![b"foo".to_vec()]);

    // Drive the (multi-morsel) batch path through the filter and compare to
    // the m=1 read.
    let passing = passing_rows(&kind, &mb);
    for (row, &batch_pass) in passing.iter().enumerate() {
        let row_pass = passes(&kind, &mb, row);
        assert_eq!(batch_pass, row_pass, "row {row}: batch={batch_pass} per-row={row_pass}",);
        // Stronger invariant: a null column value can never satisfy `=`.
        let null_word = crate::RowSource::get_null_word(&mb, row);
        if null_word & 1 != 0 {
            assert!(!batch_pass, "row {row} is null but batch said pass");
        }
    }
}

/// `filter` reports maximal *runs*, not per-row verdicts: every other test here
/// collapses the callback into a `Vec<bool>`, which cannot tell one run from two
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
    for nullable in [false, true] {
        filter_range_case(schema_pk_ints(1, nullable));
    }
}

fn filter_range_case(schema: TestSchema) {
    let n = MORSEL + 8;
    // Pass iff col1 > 0. Rows 0, 3, and MORSEL-1 are the only failures, so the
    // runs are [1,3), [4,MORSEL-1) and [MORSEL, n) — the last two straddling and
    // starting at the morsel boundary.
    let mb = make_n_col_view(
        &schema,
        n,
        |row, _| i64::from(row != 0 && row != 3 && row != MORSEL - 1),
        |_, _| false,
    );
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadConst { val: 0 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            a: Reg(0),
            b: Reg(1),
        },
    ];
    let ev = filter_prog(&schema, instrs, Reg(2), vec![]);

    assert_eq!(passing_ranges(&ev, &mb), vec![(1, 3), (4, MORSEL - 1), (MORSEL, n)]);

    // An all-pass batch is one range covering everything, not one per morsel.
    let all = make_n_col_view(&schema, n, |_, _| 1, |_, _| false);
    assert_eq!(passing_ranges(&ev, &all), vec![(0, n)]);

    // An all-fail batch reports zero runs.
    let none = make_n_col_view(&schema, n, |_, _| 0, |_, _| false);
    assert_eq!(passing_ranges(&ev, &none), vec![]);

    // A run ending exactly on a 64-bit word boundary, with the next word all
    // zero: the run has to be closed at the boundary by the empty word itself,
    // since a bitmap walk driven off the set bits never visits it.
    let boundary = make_n_col_view(&schema, n, |row, _| i64::from(row < 64), |_, _| false);
    assert_eq!(passing_ranges(&ev, &boundary), vec![(0, 64)]);

    // The same, two words on: the gap word is interior rather than trailing.
    let gap = make_n_col_view(&schema, n, |row, _| i64::from(!(64..192).contains(&row)), |_, _| false);
    assert_eq!(passing_ranges(&ev, &gap), vec![(0, 64), (192, n)]);
}

/// Differential test: the left-deep chain `col0 > k AND col1 > 1 AND col2 > 1`
/// over nullable columns must agree with a per-row 3VL reference at every morsel
/// boundary in 1..=MORSEL+1 — covering m < 64, m = 64 exactly, m crossing 64,
/// the full MORSEL, and the multi-morsel case.
///
/// Two data arrangements, because a whole morsel failing the leading clause is
/// the case a word-at-a-time kernel can treat differently from a scattered one:
/// `mixed` spreads failures and NULLs through every word, while
/// `clustered_leading` makes col0 monotonic and never null, so the leading
/// clause is definite-FALSE — not NULL — for all of morsel 0 and the survivors
/// all land in later morsels.
#[test]
fn three_and_chain_boundary_sweep() {
    let schema = schema_pk_ints(3, true);
    // (label, col0's threshold, value, null_at). col1/col2 always cycle 0..5 and
    // are NULL every 7th/11th row; only the leading column's shape varies. The
    // cycle is mod 5, not mod 4: with `> 1` on three columns whose values are
    // three *consecutive* residues, mod 4 admits no passing row at all and the
    // sweep degenerates into asserting that everything fails.
    type Arrangement = (&'static str, i64, fn(usize, usize) -> i64, fn(usize, usize) -> bool);
    let arrangements: [Arrangement; 2] = [
        (
            "mixed",
            1,
            |row, col| ((row + col) as i64) % 5,
            |row, col| match col {
                0 => row % 5 == 0,
                1 => row % 7 == 0,
                _ => row % 11 == 0,
            },
        ),
        (
            "clustered_leading",
            255,
            |row, col| if col == 0 { row as i64 } else { ((row + col) as i64) % 5 },
            |row, col| match col {
                0 => false,
                1 => row % 7 == 0,
                _ => row % 11 == 0,
            },
        ),
    ];

    for (label, k0, value, null_at) in arrangements {
        let instrs = vec![
            LogicalInstr::LoadColInt { col: 1 }, // r0 = col0
            LogicalInstr::LoadConst { val: k0 }, // r1 = k0
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                a: Reg(0),
                b: Reg(1),
            }, // r2 = col0 > k0
            LogicalInstr::LoadColInt { col: 2 }, // r3 = col1
            LogicalInstr::LoadConst { val: 1 },  // r4 = 1
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                a: Reg(3),
                b: Reg(4),
            }, // r5 = col1 > 1
            LogicalInstr::BoolBinary {
                is_or: false,
                a: Reg(2),
                b: Reg(5),
            }, // r6 = r2 AND r5
            LogicalInstr::LoadColInt { col: 3 }, // r7 = col2
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                a: Reg(7),
                b: Reg(4),
            }, // r8 = col2 > 1
            LogicalInstr::BoolBinary {
                is_or: false,
                a: Reg(6),
                b: Reg(8),
            }, // r9 = r6 AND r8  (result_reg)
        ];

        for &n in &[1, 7, 63, 64, 65, 127, 128, 255, 256, 257, 300] {
            let mb = make_n_col_view(&schema, n, value, null_at);
            let ev = filter_prog(&schema, instrs.clone(), Reg(9), vec![]);

            for (row, &got) in passing_rows(&ev, &mb).iter().enumerate() {
                // NULL column → unknown clause; otherwise the compare's verdict.
                let clause = |col: usize| {
                    let k = if col == 0 { k0 } else { 1 };
                    (!null_at(row, col)).then(|| value(row, col) > k)
                };
                let (v01, n01) = ref_and(clause(0), clause(1));
                let (v_all, n_all) = ref_and((!n01).then_some(v01), clause(2));
                let expected = !n_all && v_all;
                assert_eq!(
                    got,
                    expected,
                    "{label}: n={n} row={row} cols={:?} got={got} expected={expected}",
                    [clause(0), clause(1), clause(2)],
                );
            }
        }
    }
}

/// `NOT` over `bool(col)` against every {TRUE, FALSE, NULL} input. Exercises
/// the bit_only filter path: result_reg is the NOT output, so the filter
/// reads `bool_bits[result_reg]` and must honor 3VL (NOT NULL = NULL = fail).
#[test]
fn bit_only_not_3vl_truth_table() {
    let schema = schema_pk_ints(1, true);

    // (TRUE, FALSE, NULL) — col1 value, null bit
    let cases: &[(i64, bool, Option<bool>)] = &[(1, false, Some(true)), (0, false, Some(false)), (0, true, None)];

    for &(val, null, src_truthy) in cases {
        let mb = make_int_view(&schema, &[(1, u64::from(null), &[val])]);

        // Filter: NOT(col1 != 0). result_reg = NOT result (bit_only eligible).
        let instrs = vec![
            LogicalInstr::LoadColInt { col: 1 }, // r0 = col1
            LogicalInstr::LoadConst { val: 0 },  // r1 = 0
            LogicalInstr::Cmp {
                op: CmpOp::Ne,
                a: Reg(0),
                b: Reg(1),
            }, // r2 = bool(col1)
            LogicalInstr::BoolNot { a: Reg(2) }, // r3 = NOT r2
        ];
        let kind = filter_prog(&schema, instrs, Reg(3), vec![]);
        let passed = !passing_ranges(&kind, &mb).is_empty();
        // NOT TRUE=FALSE, NOT FALSE=TRUE, NOT NULL=NULL (filter fails on NULL)
        let expected = matches!(src_truthy, Some(false));
        assert_eq!(
            passed, expected,
            "NOT 3VL: src={src_truthy:?} val={val} null={null} expected={expected} got={passed}"
        );
    }
}

/// A filter whose `result_reg` is produced by `Instr::LoadPayloadInt` (not a
/// bool producer): the classifier marks it a bool input, so the producer packs
/// truthiness (`regs != 0` — negative ints included) into `bool_bits` and the
/// nullable word merge yields the same rows the per-row scan did.
#[test]
fn classifier_filter_result_reg_non_bool_falls_back() {
    let schema = schema_pk_ints(1, true);
    // Predicate: WHERE col1 (treat int as truthy).
    let instrs = vec![LogicalInstr::LoadColInt { col: 1 }];
    let kind = filter_prog(&schema, instrs, Reg(0), vec![]);

    // Build 4 rows: non-null 1, non-null 0, null, non-null -5.
    let rows: &[(i64, bool)] = &[(1, false), (0, false), (0, true), (-5, false)];
    let mb = make_n_col_view(&schema, rows.len(), |row, _| rows[row].0, |row, _| rows[row].1);

    let passed = passing_rows(&kind, &mb);
    // val=1 → pass, val=0 → fail, null → fail, val=-5 → pass.
    assert_eq!(
        passed,
        vec![true, false, false, true],
        "non-bool result_reg: packed truthiness must match per-row semantics"
    );
}

/// An absolute verdict for the AND of two null tests, over 65 rows so the last
/// word is partial. The sweeps below only check that the two arms agree, and
/// `is_null_and_is_not_null_are_complementary` only pins the atoms; neither
/// composes to "AND of two null tests selects the right rows", which is what
/// this holds.
#[test]
fn is_not_null_and_over_partial_word() {
    let schema = schema_pk_ints(2, true);
    // 65 rows — straddles the 64-bit word boundary so tail handling matters.
    let n = 65;
    // null every other row in col1, every third in col2
    let mb = make_n_col_view(&schema, n, |_, _| 0, |row, col| row % (col + 2) == 0);

    // WHERE col1 IS NOT NULL AND col2 IS NOT NULL
    let instrs = vec![
        is_not_null_op(1),
        is_not_null_op(2),
        LogicalInstr::BoolBinary {
            is_or: false,
            a: Reg(0),
            b: Reg(1),
        },
    ];
    let kind = filter_prog(&schema, instrs, Reg(2), vec![]);

    for (row, &got) in passing_rows(&kind, &mb).iter().enumerate() {
        let nn1 = row % 2 != 0;
        let nn2 = row % 3 != 0;
        let expected = nn1 && nn2;
        assert_eq!(
            got, expected,
            "row {row}: nn1={nn1} nn2={nn2} expected={expected} got={got}",
        );
    }
}

/// The filter's nullable-arm tail mask. BOOL_NOT is the op that dirties the
/// tail: it complements whole `bool_bits` words (`!va & !na`), so every bit
/// above `m % 64` of the last word comes out set, and the word merge into
/// `filter_bits` carries them through. The loaded nullable column is what keeps
/// the program off the `no_nulls` arm, where the verdict is packed out of `regs`
/// and no such word exists.
///
/// A phantom bit cannot pass a real row — it sits at row `n` or above, so the
/// run it opens is the degenerate `(n, n)`. The assertion is therefore on the
/// runs, each non-empty and inside the batch, and it only bites when the row
/// directly under the tail *fails*: otherwise the phantom merges into a real run
/// ending at `n` and reads as correct either way. Every `n` here is chosen so
/// row `n - 1` fails.
#[test]
fn bool_not_tail_mask() {
    let schema = schema_pk_ints(1, true);
    for &n in &[1, 65, 300] {
        // NOT (col1 >= 0), with col1 cycling -1, 0, 1 and NULL every 5th row.
        let mb = make_n_col_view(&schema, n, |row, _| (row % 3) as i64 - 1, |row, _| row % 5 == 0);
        let instrs = vec![
            LogicalInstr::LoadColInt { col: 1 },
            LogicalInstr::LoadConst { val: 0 },
            LogicalInstr::Cmp {
                op: CmpOp::Ge,
                a: Reg(0),
                b: Reg(1),
            },
            LogicalInstr::BoolNot { a: Reg(2) },
        ];
        let ev = filter_prog(&schema, instrs, Reg(3), vec![]);
        assert!(
            !ev.prog.no_nulls,
            "the nullable column load must keep this on the nullable arm"
        );

        let mut passed = vec![false; n];
        for (s, e) in passing_ranges(&ev, &mb) {
            assert!(
                s < e && e <= n,
                "n={n}: run ({s}, {e}) is not a non-empty run of 0..{n}"
            );
            passed[s..e].fill(true);
        }
        for (row, &got) in passed.iter().enumerate() {
            // 3VL: NOT NULL is NULL, which the filter drops.
            let expected = row % 5 != 0 && (row % 3) as i64 - 1 < 0;
            assert_eq!(got, expected, "n={n} row={row} got={got} expected={expected}");
        }
    }
}

/// Row counts straddling the 64-bit word and the 256-row morsel, for every
/// arms-agree sweep below.
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

/// Predicates whose only contact with a nullable column is a null test, which is
/// exactly the set `analyze` moved onto the `no_nulls` arm.
/// Against `schema_pk_ints(3, true)`.
fn null_test_shapes() -> Vec<(&'static str, FilterShape)> {
    vec![
        ("is_null", (vec![is_null_op(1)], Reg(0))),
        ("is_not_null", (vec![is_not_null_op(1)], Reg(0))),
        (
            "and",
            (
                vec![
                    is_null_op(1),
                    is_not_null_op(2),
                    LogicalInstr::BoolBinary {
                        is_or: false,
                        a: Reg(0),
                        b: Reg(1),
                    },
                ],
                Reg(2),
            ),
        ),
        (
            "or",
            (
                vec![
                    is_null_op(1),
                    is_null_op(2),
                    LogicalInstr::BoolBinary {
                        is_or: true,
                        a: Reg(0),
                        b: Reg(1),
                    },
                ],
                Reg(2),
            ),
        ),
        (
            "not",
            (vec![is_null_op(1), LogicalInstr::BoolNot { a: Reg(0) }], Reg(1)),
        ),
        // Three conjuncts: the deepest chain in the set, so the accumulator
        // spine is two ANDs long and a null test feeds another AND rather than
        // the result register directly.
        (
            "and_chain",
            (
                vec![
                    is_null_op(1),
                    is_null_op(2),
                    LogicalInstr::BoolBinary {
                        is_or: false,
                        a: Reg(0),
                        b: Reg(1),
                    },
                    is_null_op(3),
                    LogicalInstr::BoolBinary {
                        is_or: false,
                        a: Reg(2),
                        b: Reg(3),
                    },
                ],
                Reg(4),
            ),
        ),
        // CASE WHEN col1 IS NULL THEN 1 ELSE 0 END — the null test as a SELECT
        // condition, which the nullable arm reads out of `bool_bits` and the
        // `no_nulls` arm out of `regs`.
        (
            "case_cond",
            (
                vec![
                    is_null_op(1),
                    LogicalInstr::LoadConst { val: 1 },
                    LogicalInstr::LoadConst { val: 0 },
                    LogicalInstr::Select {
                        cond: Reg(0),
                        a: Reg(1),
                        b: Reg(2),
                    },
                ],
                Reg(3),
            ),
        ),
    ]
}

/// The reclassification's proof obligation: a program that resolves `no_nulls`
/// only because `IS [NOT] NULL` no longer disqualifies it must select the same
/// rows as it would have on the nullable arm. The two are different code — the
/// fast arm computes in `regs` and packs the verdict once, the nullable arm
/// computes in packed `bool_bits`/`null_bits` — so agreement is the property,
/// not the shape of either.
///
/// After the change these programs cannot reach the nullable arm by
/// construction, so the B side is forced with `prog.no_nulls = false`. Adding a
/// nullable column load instead would test a different program: that load's own
/// nullability, not the null test, is what would hold it there.
#[test]
fn is_null_arms_agree() {
    let schema = schema_pk_ints(3, true);
    for (name, (instrs, result_reg)) in null_test_shapes() {
        // One evaluator per arm for the whole sweep: the register file is sized
        // once at construction and the filter bitmap never shrinks, so reusing
        // them across row counts is also how the engine drives an evaluator.
        let (fast, nullable) = both_arms(name, || filter_prog(&schema, instrs.clone(), result_reg, vec![]));

        for &n in &ARM_SWEEP_ROWS {
            for (arrangement, null_pred) in ARM_SWEEP_NULLS {
                let mb = make_n_col_view(&schema, n, |row, col| ((row + col) % 5) as i64, null_pred);
                assert_eq!(
                    passing_rows(&fast, &mb),
                    passing_rows(&nullable, &mb),
                    "{name}/{arrangement}: arms disagree at n={n}",
                );
            }
        }
    }
}

/// The same obligation on the map drive, where the result leaves through
/// `Emit`'s register rather than a filter bitmap: both the values and the set of
/// NULL rows must match across the arms.
#[test]
fn is_null_map_arms_agree() {
    let in_schema = schema_pk_ints(1, true);
    let out_schema = schema_pk_ints(1, false);
    let instrs = vec![is_null_op(1)];
    let (fast, nullable) = both_arms("map", || {
        map_prog(&in_schema, &out_schema, instrs.clone(), vec![Sink::Reg(Reg(0))], vec![])
    });

    // The emitted value per row. `eval_is_null` clears the result's null bit and
    // the fast arm has none to begin with, so a NULL row here is a stale bit on
    // either arm — asserted directly rather than compared, which would pass on
    // two identically-stale arms.
    let drive = |ev: &Evaluator, mb: &TestView, n: usize| {
        let mut vals = Vec::with_capacity(n);
        ev.eval_morsels(mb, 0, n, |_, out| {
            vals.extend_from_slice(out.reg_values(0));
            out.for_each_null_row(0, |i| panic!("row {i} of a null test must never be NULL"));
        });
        vals
    };

    for &n in &ARM_SWEEP_ROWS {
        for (arrangement, null_pred) in ARM_SWEEP_NULLS {
            let mb = make_n_col_view(&in_schema, n, |row, _| row as i64, null_pred);
            assert_eq!(
                drive(&fast, &mb, n),
                drive(&nullable, &mb, n),
                "{arrangement}: map arms disagree at n={n}",
            );
        }
    }
}

/// The all-`NOT NULL` schema the sweep runs over: `pk U64`, then one payload
/// column per load kernel under test — `I64`, `F32`, and two `STRING`s.
fn not_null_load_schema() -> TestSchema {
    TestSchema::new(
        &[
            (type_code::U64, false),
            (type_code::I64, false),
            (type_code::F32, false),
            (type_code::STRING, false),
            (type_code::STRING, false),
        ],
        &[0],
    )
}

/// `n` rows over [`not_null_load_schema`], every row's whole null word set to
/// `null_word`.
fn not_null_load_view(n: usize, null_word: u64) -> TestView {
    let schema = not_null_load_schema();
    let mut v = TestView::new(n, schema.pk_stride());
    push_payload_cols(&mut v, &schema);
    for row in 0..n {
        set_row_pk(&mut v, &schema, row, row as u64 + 1);
        v.set_null_word(row, null_word);
        v.set_payload(row, 0, &((row % 5) as i64 - 2).to_le_bytes());
        v.set_payload(row, 1, &(row as f32).to_bits().to_le_bytes());
        v.set_string(row, 2, if row % 3 == 0 { b"alpha" } else { b"zeta" });
        v.set_string(row, 3, if row % 2 == 0 { b"beta" } else { b"omega" });
    }
    v
}

/// One shape per column-reading instruction, all against
/// [`not_null_load_schema`], each paired with whether its loaded register holds
/// a string. Every shape writes the loaded value to register 0. The const pool's
/// entry 0 is the string constant the two string shapes compare against.
fn not_null_load_shapes() -> Vec<(&'static str, FilterShape, bool)> {
    vec![
        (
            "load_payload_int",
            (
                vec![
                    LogicalInstr::LoadColInt { col: 1 },
                    LogicalInstr::LoadConst { val: 0 },
                    LogicalInstr::Cmp {
                        op: CmpOp::Gt,
                        a: Reg(0),
                        b: Reg(1),
                    },
                ],
                Reg(2),
            ),
            false,
        ),
        (
            "load_payload_f32",
            (
                vec![
                    LogicalInstr::LoadColFloat { col: 2 },
                    LogicalInstr::LoadConst { val: 3 },
                    LogicalInstr::IntToFloat { a: Reg(1) },
                    LogicalInstr::FCmp {
                        op: CmpOp::Gt,
                        a: Reg(0),
                        b: Reg(2),
                    },
                ],
                Reg(3),
            ),
            false,
        ),
        (
            "load_col_str",
            (
                vec![
                    LogicalInstr::LoadColStr { col: 3 },
                    LogicalInstr::LoadConstStr { const_idx: ConstIdx(0) },
                    LogicalInstr::StrCmp {
                        op: CmpOp::Lt,
                        a: Reg(0),
                        b: Reg(1),
                    },
                ],
                Reg(2),
            ),
            true,
        ),
        (
            "str_col_const",
            (
                vec![LogicalInstr::StrColConst {
                    op: CmpOp::Lt,
                    col: 3,
                    const_idx: ConstIdx(0),
                }],
                Reg(0),
            ),
            false,
        ),
        (
            "str_col_col",
            (
                vec![LogicalInstr::StrColCol {
                    op: CmpOp::Lt,
                    col_a: 3,
                    col_b: 4,
                }],
                Reg(0),
            ),
            false,
        ),
    ]
}

/// A `NOT NULL` column drops out of `nullable_slots`, so a load from one clears
/// its destination's null words instead of gathering them: both arms must select
/// the same rows, emit the same values, and report no NULL at all. One shape per
/// column-reading kernel, since each names its own payload slots.
///
/// The batch's own null word is swept over a clean value and one with every
/// payload bit forged set. The forged word is the discriminating input: a load
/// that still read the bit would report every row NULL on the nullable arm while
/// the `no_nulls` arm — which never reads the bitmap — read the same rows as
/// live data.
#[test]
fn not_null_load_arms_agree_and_report_no_null() {
    let schema = not_null_load_schema();
    let consts = || vec![b"m".to_vec()];
    // Every shape loads into register 0 and, as a map, emits it.
    const LOAD_REG: u16 = 0;

    for (name, (instrs, result_reg), is_str) in not_null_load_shapes() {
        let out_tc = if is_str { type_code::STRING } else { type_code::I64 };
        let out_schema = TestSchema::new(&[(type_code::U64, false), (out_tc, false)], &[0]);

        let (fast_filter, nullable_filter) =
            both_arms(name, || filter_prog(&schema, instrs.clone(), result_reg, consts()));
        let (fast_map, nullable_map) = both_arms(name, || {
            map_prog(
                &schema,
                &out_schema,
                instrs.clone(),
                vec![Sink::Reg(Reg(LOAD_REG))],
                consts(),
            )
        });

        // The emitted value per row, plus the direct assertion that no row
        // reports NULL — comparing the arms alone would pass on two identically
        // stale ones.
        let drive_map = |ev: &Evaluator, mb: &TestView, n: usize| {
            let reg = LOAD_REG as usize;
            let mut vals: Vec<Vec<u8>> = Vec::with_capacity(n);
            ev.eval_morsels(mb, 0, n, |_, out| {
                if is_str {
                    vals.extend((0..out.rows()).map(|i| out.str_bytes(reg, i).to_vec()));
                } else {
                    vals.extend(out.reg_bytes(reg).chunks_exact(8).map(<[u8]>::to_vec));
                }
                out.for_each_null_row(reg, |i| {
                    panic!("{name}: row {i} of a NOT NULL load must never report NULL")
                });
            });
            vals
        };

        for &n in &ARM_SWEEP_ROWS {
            for (bitmap, null_word) in [("clean", 0u64), ("forged", 0b1111u64)] {
                let mb = not_null_load_view(n, null_word);
                assert_eq!(
                    passing_rows(&fast_filter, &mb),
                    passing_rows(&nullable_filter, &mb),
                    "{name}/{bitmap}: filter arms disagree at n={n}",
                );
                assert_eq!(
                    drive_map(&fast_map, &mb, n),
                    drive_map(&nullable_map, &mb, n),
                    "{name}/{bitmap}: map arms disagree at n={n}",
                );
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
/// misaligned mask reports NULL on rows the right one never touches.
#[test]
fn nullable_and_not_null_columns_side_by_side() {
    let schema = TestSchema::new(
        &[
            (type_code::U64, false),    // 0: pk
            (type_code::I64, true),     // 1: payload slot 0
            (type_code::I64, false),    // 2: payload slot 1
            (type_code::STRING, true),  // 3: payload slot 2
            (type_code::STRING, false), // 4: payload slot 3
            (type_code::F32, true),     // 5: payload slot 4
            (type_code::F32, false),    // 6: payload slot 5
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
    let mut v = TestView::new(n, schema.pk_stride());
    push_payload_cols(&mut v, &schema);
    for row in 0..n {
        set_row_pk(&mut v, &schema, row, row as u64 + 1);
        let mut word = 0u64;
        for pi in 0..6 {
            gnitz_wire::null_word_set(&mut word, pi, slot_bit(pi, row));
        }
        v.set_null_word(row, word);
        v.set_payload(row, 0, &(row as i64).to_le_bytes());
        v.set_payload(row, 1, &(row as i64).to_le_bytes());
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
        LogicalInstr::StrColCol {
            op: CmpOp::Lt,
            col_a: 3,
            col_b: 4,
        },
    ];
    let ev = scalar_prog(&schema, instrs, Reg(0), vec![b"m".to_vec()]);
    assert!(
        !ev.prog.no_nulls,
        "a nullable column load must keep the program on the nullable arm",
    );

    // Per register, the rows it must report NULL on. Every `NOT NULL` load and
    // compare reports none, forged bit or not.
    let want_null = |reg: usize, row: usize| match reg {
        0 => slot_bit(0, row),     // nullable I64
        2 => slot_bit(4, row),     // nullable F32
        4 | 7 => slot_bit(2, row), // nullable STRING, and the compare it feeds
        _ => false,
    };
    let mut seen = vec![vec![false; n]; 8];
    ev.eval_morsels(&v, 0, n, |rel_start, out| {
        for (reg, rows) in seen.iter_mut().enumerate() {
            out.for_each_null_row(reg, |i| rows[rel_start + i] = true);
        }
    });
    for (reg, rows) in seen.iter().enumerate() {
        for (row, &is_null) in rows.iter().enumerate() {
            assert_eq!(is_null, want_null(reg, row), "reg {reg}, row {row}");
        }
    }
}

/// A sign flip inside `eval_is_null` would survive every arms-agree assertion
/// above — both arms run that one kernel. Pin the polarity absolutely: `IS NULL`
/// selects exactly the NULL rows, and `IS NOT NULL` selects exactly the rest.
///
/// Polarity is row-local, so one multi-morsel `n` says everything a sweep would;
/// the boundary counts belong to the packing routes, which `is_null_arms_agree`
/// sweeps.
#[test]
fn is_null_and_is_not_null_are_complementary() {
    let schema = schema_pk_ints(1, true);
    let null_row = |row: usize| row % 7 < 3;
    let n = 300;
    let mb = make_n_col_view(&schema, n, |row, _| row as i64, |row, _| null_row(row));
    let run = |instr| passing_rows(&filter_prog(&schema, vec![instr], Reg(0), vec![]), &mb);
    let is_null = run(is_null_op(1));
    let is_not_null = run(is_not_null_op(1));
    for row in 0..n {
        assert_eq!(is_null[row], null_row(row), "row {row}: IS NULL");
        assert_ne!(is_null[row], is_not_null[row], "row {row}: not complementary");
    }
}

/// A NULL row keeps whatever bytes its column held, so it reaches the 3VL OR
/// carrying a *set* `bool_bits` bit, and the `!na` / `!nb` masks are the only
/// thing keeping it out of the definite-true term. Here col1 is NULL on row 0
/// but holds 10, which satisfies the compare: `NULL OR FALSE` is NULL, and the
/// filter drops it.
#[test]
fn or_does_not_take_a_null_row_stored_value_as_definite_true() {
    let schema = schema_pk_ints(2, true);
    let n = 1;
    let mb = make_n_col_view(&schema, n, |_, col| if col == 0 { 10 } else { 0 }, |_, col| col == 0);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadConst { val: 5 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            a: Reg(0),
            b: Reg(1),
        },
        LogicalInstr::LoadColInt { col: 2 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            a: Reg(3),
            b: Reg(1),
        },
        LogicalInstr::BoolBinary {
            is_or: true,
            a: Reg(2),
            b: Reg(4),
        },
    ];
    let ev = filter_prog(&schema, instrs, Reg(5), vec![]);
    assert_eq!(
        passing_rows(&ev, &mb),
        vec![false],
        "NULL OR FALSE is NULL, which the filter drops"
    );
}

/// A 3-conjunct chain read back per row, which reports the null bit
/// directly where `filter` cannot: it consumes `bool_bits & !null_bits`, so a
/// cleared bool already forces the verdict and NULL is indistinguishable from
/// FALSE there. A filter-resolved program really is driven this way — `passes`
/// above is exactly that.
///
/// Two rows, because the interesting cases are the two the chain can produce:
/// `TRUE AND NULL AND TRUE` is NULL, and a definite-FALSE chain is FALSE rather
/// than the previous drive's NULL carried forward in the scratch.
#[test]
fn and_chain_null_and_false_per_row() {
    let schema = schema_pk_ints(3, true);
    // Row 0: col1 = 1 (true), col2 NULL holding 5, col3 = 1 (true)
    //        → acc = TRUE AND NULL = NULL, terminal = NULL AND TRUE = NULL.
    // Row 1: col1 = 0, so acc is definite-FALSE and so is the terminal.
    let value = |row: usize, col: usize| match (row, col) {
        (0, 0) | (0, 2) => 1,
        (0, 1) => 5,
        _ => 0,
    };
    let mb = make_n_col_view(&schema, 2, value, |row, col| row == 0 && col == 1);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadConst { val: 0 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            a: Reg(0),
            b: Reg(1),
        },
        LogicalInstr::LoadColInt { col: 2 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            a: Reg(3),
            b: Reg(1),
        },
        LogicalInstr::BoolBinary {
            is_or: false,
            a: Reg(2),
            b: Reg(4),
        },
        LogicalInstr::LoadColInt { col: 3 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            a: Reg(6),
            b: Reg(1),
        },
        LogicalInstr::BoolBinary {
            is_or: false,
            a: Reg(5),
            b: Reg(7),
        },
    ];
    let ev = filter_prog(&schema, instrs, Reg(8), vec![]);

    assert_eq!(row_value(&ev, &mb, 0), None, "TRUE AND NULL AND TRUE is NULL");
    assert_eq!(
        row_value(&ev, &mb, 1),
        Some(0),
        "a definite-FALSE chain is FALSE, not the previous drive's NULL"
    );
}

/// `col0 = -1 AND col1 > 0 AND col2 > 0` must select the same 1-in-4 survivor
/// rows however the other three quarters are excluded, and on either arm.
///
/// - `null_flood` (nullable schema): col0 is NULL on 3 of every 4 rows, so the
///   leading clause is NULL rather than definite-FALSE, and the packed 3VL word
///   loop has to carry that through two more ANDs. An all-NULL morsel would not
///   be sharp — NULL and FALSE both read as "excluded" out of a filter — so the
///   survivors are what separate 3VL NULL from FALSE.
/// - `definite_false` (NOT NULL schema): the same rows fail on value instead,
///   which selects the `no_nulls` arm — `bin_op!` over `regs`, with no
///   `bool_bits`/`null_bits` allocated at all.
///
/// Same instruction stream and same expected rows for both, so the two arms are
/// held to one answer rather than to two hand-written ones.
#[test]
fn and_chain_survivors_agree_across_arms() {
    // col0 = -1 AND col1 > 0 AND col2 > 0  (const regs: -1 and 0)
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadConst { val: -1 },
        LogicalInstr::Cmp {
            op: CmpOp::Eq,
            a: Reg(0),
            b: Reg(1),
        },
        LogicalInstr::LoadColInt { col: 2 },
        LogicalInstr::LoadConst { val: 0 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            a: Reg(3),
            b: Reg(4),
        },
        LogicalInstr::BoolBinary {
            is_or: false,
            a: Reg(2),
            b: Reg(5),
        },
        LogicalInstr::LoadColInt { col: 3 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            a: Reg(7),
            b: Reg(4),
        },
        LogicalInstr::BoolBinary {
            is_or: false,
            a: Reg(6),
            b: Reg(8),
        },
    ];

    // A survivor is `row % 4 == 0` in both; col1 = col2 = 5 always passes.
    let survives = |row: usize| row.is_multiple_of(4);
    for (label, nullable) in [("null_flood", true), ("definite_false", false)] {
        let schema = schema_pk_ints(3, nullable);
        for &n in &[64, 256, 257] {
            // Nullable: col0 = -1 everywhere and the non-survivors are NULL.
            // NOT NULL: the non-survivors hold 0, which simply is not -1.
            let mb = make_n_col_view(
                &schema,
                n,
                |row, col| match col {
                    0 if nullable || survives(row) => -1,
                    0 => 0,
                    _ => 5,
                },
                |row, col| nullable && col == 0 && !survives(row),
            );
            let ev = filter_prog(&schema, instrs.clone(), Reg(9), vec![]);
            assert_eq!(
                ev.prog.no_nulls, !nullable,
                "{label}: wrong arm — the drive proves nothing"
            );
            assert_eq!(
                passing_rows(&ev, &mb),
                (0..n).map(survives).collect::<Vec<_>>(),
                "{label}: n={n}"
            );
        }
    }

    // Neither arrangement above ever fills a whole 64-bit null word: `null_flood`
    // nulls only col0. Every column NULL on every row is the input a
    // word-at-a-time 3VL loop can treat differently from a mixed one, and it must
    // still reject every row.
    let schema = schema_pk_ints(3, true);
    let ev = filter_prog(&schema, instrs, Reg(9), vec![]);
    for &n in &[64, 128, 257] {
        let mb = make_n_col_view(&schema, n, |_, _| 0, |_, _| true);
        assert!(
            passing_rows(&ev, &mb).iter().all(|&p| !p),
            "an all-NULL batch has no survivors at n={n}"
        );
    }
}

/// Reference 3VL: `(truthy, is_null)` for `a AND b`. A definite FALSE on either
/// side forces FALSE even when the other is NULL, which is the rule a two-valued
/// implementation gets wrong.
fn ref_and(a: Option<bool>, b: Option<bool>) -> (bool, bool) {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => (false, false),
        (Some(true), Some(true)) => (true, false),
        _ => (false, true), // NULL
    }
}

/// The dual: a definite TRUE on either side forces TRUE.
fn ref_or(a: Option<bool>, b: Option<bool>) -> (bool, bool) {
    match (a, b) {
        (Some(true), _) | (_, Some(true)) => (true, false),
        (Some(false), Some(false)) => (false, false),
        _ => (false, true), // NULL
    }
}

/// The complete `{TRUE, FALSE, NULL}²` table for both combinators, against the
/// references above. Swept rather than hand-listed: the asymmetric cells are the
/// whole content of 3VL, and a hand-written list of them silently omitted
/// `T AND T` and `F OR F`.
#[test]
fn bool_and_or_cover_the_whole_three_valued_table() {
    let schema = schema_pk_ints(2, true);
    const STATES: [Option<bool>; 3] = [Some(true), Some(false), None];
    let cells: Vec<(Option<bool>, Option<bool>)> = STATES.iter().flat_map(|&a| STATES.map(|b| (a, b))).collect();

    // A NULL row still carries bytes, so the NULL cells store a *truthy* value:
    // only the null bit may keep them out of the definite-true term.
    let bit = |v: Option<bool>| i64::from(v.unwrap_or(true));
    let vals: Vec<[i64; 2]> = cells.iter().map(|&(a, b)| [bit(a), bit(b)]).collect();
    let rows: Vec<(u64, u64, &[i64])> = cells
        .iter()
        .zip(&vals)
        .enumerate()
        .map(|(i, (&(a, b), v))| {
            let null_word = u64::from(a.is_none()) | (u64::from(b.is_none()) << 1);
            (i as u64 + 1, null_word, &v[..])
        })
        .collect();
    let mb = make_int_view(&schema, &rows);

    for (name, mk, reference) in [
        (
            "AND",
            (|a, b| LogicalInstr::BoolBinary { is_or: false, a, b }) as fn(Reg, Reg) -> LogicalInstr,
            ref_and as fn(Option<bool>, Option<bool>) -> (bool, bool),
        ),
        ("OR", |a, b| LogicalInstr::BoolBinary { is_or: true, a, b }, ref_or),
    ] {
        let instrs = vec![
            LogicalInstr::LoadColInt { col: 1 },
            LogicalInstr::LoadColInt { col: 2 },
            mk(Reg(0), Reg(1)),
        ];
        let ev = scalar_prog(&schema, instrs, Reg(2), vec![]);
        for (row, &(a, b)) in cells.iter().enumerate() {
            let (want_val, want_null) = reference(a, b);
            let want = (!want_null).then_some(i64::from(want_val));
            assert_eq!(row_value(&ev, &mb, row), want, "{name}: {a:?}, {b:?}");
        }
    }
}

/// `eval_all` returns the arm its program's result class fixes, and its string
/// arm addresses one shared arena — so the per-morsel span arithmetic has to
/// stay in step with the rows across a morsel boundary. The scalar arm reads
/// back through the same `result_value` the single-row form does, so only its
/// length and the class dispatch are at stake here.
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
    let div = scalar_prog(
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
    let ExprResults::Scalar(vals) = div.eval_all(&mb) else {
        panic!("a scalar program must evaluate to the scalar arm");
    };
    assert_eq!(vals, (0..n).map(|i| row_value(&div, &mb, i)).collect::<Vec<_>>());

    let str_schema = schema_pk_strings(1, true);
    let mut sv = TestView::new(n, str_schema.pk_stride());
    push_payload_cols(&mut sv, &str_schema);
    for row in 0..n {
        set_row_pk(&mut sv, &str_schema, row, row as u64 + 1);
        if row.is_multiple_of(9) {
            sv.set_null(row, 0);
        } else {
            // Alternating either side of the 12-byte inline boundary, so both
            // cell forms cross the morsel boundary.
            sv.set_string(row, 0, format!("r{row}-{}", "x".repeat(row % 20)).as_bytes());
        }
    }
    let upper = scalar_prog(
        &str_schema,
        vec![
            LogicalInstr::LoadColStr { col: 1 },
            LogicalInstr::StrCase { a: Reg(0), upper: true },
        ],
        Reg(1),
        vec![],
    );
    let ExprResults::Str { bytes, spans } = upper.eval_all(&sv) else {
        panic!("a string program must evaluate to the string arm");
    };
    assert_eq!(spans.len(), n);
    for (row, span) in spans.iter().enumerate() {
        let want = (!row.is_multiple_of(9)).then(|| format!("R{row}-{}", "X".repeat(row % 20)));
        assert_eq!(
            span.map(|(o, l)| String::from_utf8(bytes[o..o + l].to_vec()).unwrap()),
            want,
            "row {row}",
        );
    }
}
