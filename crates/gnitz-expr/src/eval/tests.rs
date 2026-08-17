//! Driving a resolved program through [`crate::Evaluator`]: the filter over a
//! whole batch, the m=1 row read, and the 3VL / bit_only / AND-chain behaviour
//! visible at that surface.
//!
//! `0 * MORSEL`, `1 * NULL_WORDS_PER_REG` etc. are deliberate layout-documenting
//! expressions making the register/word index explicit at each access site;
//! collapsing them obscures which register is in use.
#![allow(clippy::erasing_op, clippy::identity_op)]

use gnitz_wire::type_code;

use crate::batch::MORSEL;
use crate::test_support::{
    both_arms, filter_prog, is_not_null_op, is_null_op, make_int_row, make_int_view, make_n_col_view, map_prog,
    passing_ranges, passing_rows, push_payload_cols, scalar_prog, schema_pk_ints, set_row_pk, TestSchema, TestView,
};
use crate::{CmpOp, Evaluator, LogicalInstr, StrOp};

/// True iff `ev`'s predicate passes for `row`.
fn passes(ev: &Evaluator, mb: &TestView, row: usize) -> bool {
    let (val, is_null) = ev.eval_row(mb, row);
    !is_null && val != 0
}

/// The batch filter and the m=1 row read are two drives of one program and must
/// agree row for row.
#[test]
fn filter_agrees_with_eval_row() {
    let schema = schema_pk_ints(1, true);
    // Pass iff col[1] > 15.
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // r0 = col[1] (payload[0])
        LogicalInstr::LoadConst { dst: 1, val: 15 }, // r1 = 15
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        }, // r2 = r0 > r1
    ];
    let ev = filter_prog(&schema, instrs, 3, 2, vec![]);

    let rows: &[(u64, u64, &[i64])] = &[(1, 0, &[5]), (2, 0, &[15]), (3, 0, &[25]), (4, 0, &[0])];
    let mb = make_int_view(&schema, rows);

    let mut passing = vec![false; rows.len()];
    ev.filter(&mb, rows.len(), |start, end| passing[start..end].fill(true));

    for (i, &(_, _, vals)) in rows.iter().enumerate() {
        assert_eq!(passing[i], passes(&ev, &mb, i), "row {i}: val={}", vals[0]);
    }
    assert_eq!(passing, vec![false, false, true, false]);
}

/// Regression: `is_strictly_non_nullable` formerly ignored STR_COL_*_CONST,
/// so a `WHERE str_col = 'foo'` against a nullable string column would set
/// `no_nulls=true` on the batch path and let null rows leak through as
/// definite-true / definite-false results. Verify the batch path is
/// row-for-row correct on mixed null/non-null inputs.
#[test]
fn test_str_col_eq_const_nullable_column_matches_per_row() {
    // Schema: pk(U64) + nullable STRING.
    let schema = TestSchema::new(&[(type_code::U64, false), (type_code::STRING, true)], &[0]);

    // 20 rows: alternating null/non-null, with the non-null rows alternating
    // between "foo" (matches the predicate) and "bar" (does not).
    let n = 20usize;
    let mut mb = TestView::new(n, 8);
    mb.push_col(16);
    for row in 0..n {
        mb.set_pk_col(row, 0, &(row as u64 + 1).to_le_bytes(), type_code::U64);
        mb.set_null_word(row, u64::from(row % 2 == 0)); // bit 0 = col1
        mb.set_string(row, 0, if row % 4 == 1 { b"foo" } else { b"bar" });
    }

    // Predicate: col1 = 'foo'. result_reg = 0.
    let instrs = vec![LogicalInstr::StrColConst {
        op: StrOp::Eq,
        dst: 0,
        col: 1,
        const_idx: 0,
    }];
    let kind = filter_prog(&schema, instrs, 1, 0, vec![b"foo".to_vec()]);

    // Drive the (multi-morsel) batch path through the filter and compare to
    // the m=1 read.
    let mut passing = vec![false; n];
    kind.filter(&mb, n, |start, end| {
        passing[start..end].fill(true);
    });
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
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let ev = filter_prog(&schema, instrs, 3, 2, vec![]);

    assert_eq!(passing_ranges(&ev, &mb, n), vec![(1, 3), (4, MORSEL - 1), (MORSEL, n)]);

    // An all-pass batch is one range covering everything, not one per morsel.
    let all = make_n_col_view(&schema, n, |_, _| 1, |_, _| false);
    assert_eq!(passing_ranges(&ev, &all, n), vec![(0, n)]);

    // An all-fail batch calls back zero times.
    let none = make_n_col_view(&schema, n, |_, _| 0, |_, _| false);
    let mut count = 0;
    ev.filter(&none, n, |_, _| count += 1);
    assert_eq!(count, 0);

    // A run ending exactly on a 64-bit word boundary, with the next word all
    // zero: the run has to be closed at the boundary by the empty word itself,
    // since a bitmap walk driven off the set bits never visits it.
    let boundary = make_n_col_view(&schema, n, |row, _| i64::from(row < 64), |_, _| false);
    assert_eq!(passing_ranges(&ev, &boundary, n), vec![(0, 64)]);

    // The same, two words on: the gap word is interior rather than trailing.
    let gap = make_n_col_view(&schema, n, |row, _| i64::from(!(64..192).contains(&row)), |_, _| false);
    assert_eq!(passing_ranges(&ev, &gap, n), vec![(0, 64), (192, n)]);
}

#[test]
fn golden_load_payload_null_row() {
    let schema = schema_pk_ints(1, true);
    let mb = make_int_row(&schema, &[42], 1);

    // Predicate: col1 > 0. With col1 null, result must be null → predicate fails.
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        },
    ];
    let kind = filter_prog(&schema, instrs, 3, 2, vec![]);
    assert!(!passes(&kind, &mb, 0));
}

#[test]
fn golden_str_col_eq_const_null_row() {
    let schema = TestSchema::new(&[(type_code::U64, false), (type_code::STRING, true)], &[0]);
    let mut mb = TestView::new(1, 8);
    mb.push_col(16);
    mb.set_pk_col(0, 0, &1u64.to_le_bytes(), type_code::U64);
    mb.set_null_word(0, 1); // payload 0 (the string col) is NULL
    mb.set_string(0, 0, b"foo");

    let instrs = vec![LogicalInstr::StrColConst {
        op: StrOp::Eq,
        dst: 0,
        col: 1,
        const_idx: 0,
    }];
    let kind = filter_prog(&schema, instrs, 1, 0, vec![b"foo".to_vec()]);
    assert!(!passes(&kind, &mb, 0), "STR_COL_EQ_CONST on NULL row must not pass",);
}

#[test]
fn golden_is_null_and_is_not_null_single_row() {
    let schema = schema_pk_ints(1, true);

    // Null row: IS NULL → true, IS NOT NULL → false.
    let mb = make_int_row(&schema, &[0], 1);
    let instrs_is_null = vec![is_null_op(0, 1)];
    let kind = filter_prog(&schema, instrs_is_null, 1, 0, vec![]);
    assert!(passes(&kind, &mb, 0));
    let instrs_is_not_null = vec![is_not_null_op(0, 1)];
    let kind = filter_prog(&schema, instrs_is_not_null, 1, 0, vec![]);
    assert!(!passes(&kind, &mb, 0));

    // Non-null row: opposite.
    let mb = make_int_row(&schema, &[7], 0);
    let instrs_is_null = vec![is_null_op(0, 1)];
    let kind = filter_prog(&schema, instrs_is_null, 1, 0, vec![]);
    assert!(!passes(&kind, &mb, 0));
    let instrs_is_not_null = vec![is_not_null_op(0, 1)];
    let kind = filter_prog(&schema, instrs_is_not_null, 1, 0, vec![]);
    assert!(passes(&kind, &mb, 0));
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
            LogicalInstr::LoadColInt { dst: 0, col: 1 }, // r0 = col0
            LogicalInstr::LoadConst { dst: 1, val: k0 }, // r1 = k0
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 2,
                a: 0,
                b: 1,
            }, // r2 = col0 > k0
            LogicalInstr::LoadColInt { dst: 3, col: 2 }, // r3 = col1
            LogicalInstr::LoadConst { dst: 4, val: 1 },  // r4 = 1
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 5,
                a: 3,
                b: 4,
            }, // r5 = col1 > 1
            LogicalInstr::BoolAnd { dst: 6, a: 2, b: 5 }, // r6 = r2 AND r5
            LogicalInstr::LoadColInt { dst: 7, col: 3 }, // r7 = col2
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 8,
                a: 7,
                b: 4,
            }, // r8 = col2 > 1
            LogicalInstr::BoolAnd { dst: 9, a: 6, b: 8 }, // r9 = r6 AND r8  (result_reg)
        ];

        for &n in &[1, 7, 63, 64, 65, 127, 128, 255, 256, 257, 300] {
            let mb = make_n_col_view(&schema, n, value, null_at);
            let ev = filter_prog(&schema, instrs.clone(), 10, 9, vec![]);

            for (row, &got) in passing_rows(&ev, &mb, n).iter().enumerate() {
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
        let mb = make_int_row(&schema, &[val], u64::from(null));

        // Filter: NOT(col1 != 0). result_reg = NOT result (bit_only eligible).
        let instrs = vec![
            LogicalInstr::LoadColInt { dst: 0, col: 1 }, // r0 = col1
            LogicalInstr::LoadConst { dst: 1, val: 0 },  // r1 = 0
            LogicalInstr::Cmp {
                op: CmpOp::Ne,
                dst: 2,
                a: 0,
                b: 1,
            }, // r2 = bool(col1)
            LogicalInstr::BoolNot { dst: 3, a: 2 },      // r3 = NOT r2
        ];
        let kind = filter_prog(&schema, instrs, 4, 3, vec![]);
        let mut passed = false;
        kind.filter(&mb, 1, |_, _| {
            passed = true;
        });
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
    let instrs = vec![LogicalInstr::LoadColInt { dst: 0, col: 1 }];
    let kind = filter_prog(&schema, instrs, 1, 0, vec![]);

    // Build 4 rows: non-null 1, non-null 0, null, non-null -5.
    let rows: &[(i64, bool)] = &[(1, false), (0, false), (0, true), (-5, false)];
    let mb = make_n_col_view(&schema, rows.len(), |row, _| rows[row].0, |row, _| rows[row].1);

    let mut passed = vec![false; 4];
    kind.filter(&mb, 4, |s, e| {
        passed[s..e].fill(true);
    });
    // val=1 → pass, val=0 → fail, null → fail, val=-5 → pass.
    assert_eq!(
        passed,
        vec![true, false, false, true],
        "non-bool result_reg: packed truthiness must match per-row semantics"
    );
}

/// All-null word: 64 consecutive null rows on each side of an AND. Both
/// nullable arms (no `bit_only` and `bit_only`) must yield null in every
/// row, matching the historical 3VL behavior.
#[test]
fn bit_only_all_null_word_and() {
    let schema = schema_pk_ints(2, true);

    let n = 64;
    // both columns NULL in every row
    let mb = make_n_col_view(&schema, n, |_, _| 0, |_, _| true);

    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::Cmp {
            op: CmpOp::Ne,
            dst: 2,
            a: 0,
            b: 1,
        }, // r2 = bool(col1)
        LogicalInstr::LoadColInt { dst: 3, col: 2 },
        LogicalInstr::Cmp {
            op: CmpOp::Ne,
            dst: 4,
            a: 3,
            b: 1,
        }, // r4 = bool(col2)
        LogicalInstr::BoolAnd { dst: 5, a: 2, b: 4 },
    ];
    let kind = filter_prog(&schema, instrs, 6, 5, vec![]);

    let passed = passing_rows(&kind, &mb, n);
    assert!(passed.iter().all(|&p| !p), "all-null AND must reject every row");
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
        is_not_null_op(0, 1),
        is_not_null_op(1, 2),
        LogicalInstr::BoolAnd { dst: 2, a: 0, b: 1 },
    ];
    let kind = filter_prog(&schema, instrs, 3, 2, vec![]);

    for (row, &got) in passing_rows(&kind, &mb, n).iter().enumerate() {
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
            LogicalInstr::LoadColInt { dst: 0, col: 1 },
            LogicalInstr::LoadConst { dst: 1, val: 0 },
            LogicalInstr::Cmp {
                op: CmpOp::Ge,
                dst: 2,
                a: 0,
                b: 1,
            },
            LogicalInstr::BoolNot { dst: 3, a: 2 },
        ];
        let ev = filter_prog(&schema, instrs, 4, 3, vec![]);
        assert!(
            !ev.prog.no_nulls,
            "the nullable column load must keep this on the nullable arm"
        );

        let mut passed = vec![false; n];
        for (s, e) in passing_ranges(&ev, &mb, n) {
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

/// A predicate as `filter_prog` takes it: `(instrs, num_regs, result_reg)`.
type FilterShape = (Vec<LogicalInstr>, u32, u32);

/// A named null arrangement: the `null_pred` a sweep hands `make_n_col_view`.
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
/// exactly the set `is_strictly_non_nullable` moved onto the `no_nulls` arm.
/// Against `schema_pk_ints(3, true)`.
fn null_test_shapes() -> Vec<(&'static str, FilterShape)> {
    vec![
        ("is_null", (vec![is_null_op(0, 1)], 1, 0)),
        ("is_not_null", (vec![is_not_null_op(0, 1)], 1, 0)),
        (
            "and",
            (
                vec![
                    is_null_op(0, 1),
                    is_not_null_op(1, 2),
                    LogicalInstr::BoolAnd { dst: 2, a: 0, b: 1 },
                ],
                3,
                2,
            ),
        ),
        (
            "or",
            (
                vec![
                    is_null_op(0, 1),
                    is_null_op(1, 2),
                    LogicalInstr::BoolOr { dst: 2, a: 0, b: 1 },
                ],
                3,
                2,
            ),
        ),
        (
            "not",
            (vec![is_null_op(0, 1), LogicalInstr::BoolNot { dst: 1, a: 0 }], 2, 1),
        ),
        // Three conjuncts: the deepest chain in the set, so the accumulator
        // spine is two ANDs long and a null test feeds another AND rather than
        // the result register directly.
        (
            "and_chain",
            (
                vec![
                    is_null_op(0, 1),
                    is_null_op(1, 2),
                    LogicalInstr::BoolAnd { dst: 2, a: 0, b: 1 },
                    is_null_op(3, 3),
                    LogicalInstr::BoolAnd { dst: 4, a: 2, b: 3 },
                ],
                5,
                4,
            ),
        ),
        // CASE WHEN col1 IS NULL THEN 1 ELSE 0 END — the null test as a SELECT
        // condition, which the nullable arm reads out of `bool_bits` and the
        // `no_nulls` arm out of `regs`.
        (
            "case_cond",
            (
                vec![
                    is_null_op(0, 1),
                    LogicalInstr::LoadConst { dst: 1, val: 1 },
                    LogicalInstr::LoadConst { dst: 2, val: 0 },
                    LogicalInstr::Select {
                        dst: 3,
                        cond: 0,
                        a: 1,
                        b: 2,
                    },
                ],
                4,
                3,
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
    for (name, (instrs, num_regs, result_reg)) in null_test_shapes() {
        // One evaluator per arm for the whole sweep: `ensure_capacity` never
        // shrinks, so reusing them across row counts is also how the engine
        // drives an evaluator.
        let (fast, nullable) = both_arms(name, || {
            filter_prog(&schema, instrs.clone(), num_regs, result_reg, vec![])
        });

        for &n in &ARM_SWEEP_ROWS {
            for (arrangement, null_pred) in ARM_SWEEP_NULLS {
                let mb = make_n_col_view(&schema, n, |row, col| ((row + col) % 5) as i64, null_pred);
                assert_eq!(
                    passing_rows(&fast, &mb, n),
                    passing_rows(&nullable, &mb, n),
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
    let instrs = vec![is_null_op(0, 1), LogicalInstr::Emit { src: 0, out: 0 }];
    let (fast, nullable) = both_arms("map", || {
        map_prog(&in_schema, &out_schema, instrs.clone(), 1, 0, vec![])
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
                    LogicalInstr::LoadColInt { dst: 0, col: 1 },
                    LogicalInstr::LoadConst { dst: 1, val: 0 },
                    LogicalInstr::Cmp {
                        op: CmpOp::Gt,
                        dst: 2,
                        a: 0,
                        b: 1,
                    },
                ],
                3,
                2,
            ),
            false,
        ),
        (
            "load_payload_f32",
            (
                vec![
                    LogicalInstr::LoadColFloat { dst: 0, col: 2 },
                    LogicalInstr::LoadConst { dst: 1, val: 3 },
                    LogicalInstr::IntToFloat { dst: 2, a: 1 },
                    LogicalInstr::FCmp {
                        op: CmpOp::Gt,
                        dst: 3,
                        a: 0,
                        b: 2,
                    },
                ],
                4,
                3,
            ),
            false,
        ),
        (
            "load_col_str",
            (
                vec![
                    LogicalInstr::LoadColStr { dst: 0, col: 3 },
                    LogicalInstr::LoadConstStr { dst: 1, const_idx: 0 },
                    LogicalInstr::StrCmp {
                        op: StrOp::Lt,
                        dst: 2,
                        a: 0,
                        b: 1,
                    },
                ],
                3,
                2,
            ),
            true,
        ),
        (
            "str_col_const",
            (
                vec![LogicalInstr::StrColConst {
                    op: StrOp::Lt,
                    dst: 0,
                    col: 3,
                    const_idx: 0,
                }],
                1,
                0,
            ),
            false,
        ),
        (
            "str_col_col",
            (
                vec![LogicalInstr::StrColCol {
                    op: StrOp::Lt,
                    dst: 0,
                    col_a: 3,
                    col_b: 4,
                }],
                1,
                0,
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

    for (name, (instrs, num_regs, result_reg), is_str) in not_null_load_shapes() {
        let out_tc = if is_str { type_code::STRING } else { type_code::I64 };
        let out_schema = TestSchema::new(&[(type_code::U64, false), (out_tc, false)], &[0]);

        let (fast_filter, nullable_filter) = both_arms(name, || {
            filter_prog(&schema, instrs.clone(), num_regs, result_reg, consts())
        });
        let map_instrs: Vec<LogicalInstr> = instrs
            .iter()
            .copied()
            .chain([LogicalInstr::Emit { src: LOAD_REG, out: 0 }])
            .collect();
        let (fast_map, nullable_map) = both_arms(name, || {
            map_prog(
                &schema,
                &out_schema,
                map_instrs.clone(),
                num_regs,
                LOAD_REG as u32,
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
                    passing_rows(&fast_filter, &mb, n),
                    passing_rows(&nullable_filter, &mb, n),
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
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadColInt { dst: 1, col: 2 },
        LogicalInstr::LoadColFloat { dst: 2, col: 5 },
        LogicalInstr::LoadColFloat { dst: 3, col: 6 },
        LogicalInstr::LoadColStr { dst: 4, col: 3 },
        LogicalInstr::LoadColStr { dst: 5, col: 4 },
        LogicalInstr::StrColConst {
            op: StrOp::Lt,
            dst: 6,
            col: 4,
            const_idx: 0,
        },
        LogicalInstr::StrColCol {
            op: StrOp::Lt,
            dst: 7,
            col_a: 3,
            col_b: 4,
        },
    ];
    let ev = scalar_prog(&schema, instrs, 8, 0, vec![b"m".to_vec()]);
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
/// The batch-drive counterpart to `golden_is_null_and_is_not_null_single_row`,
/// which pins the same polarity through the `m = 1` `eval_row` drive. Polarity is
/// row-local, so one multi-morsel `n` says everything a sweep would; the boundary
/// counts belong to the packing routes, which `is_null_arms_agree` sweeps.
#[test]
fn is_null_and_is_not_null_are_complementary() {
    let schema = schema_pk_ints(1, true);
    let null_row = |row: usize| row % 7 < 3;
    let n = 300;
    let mb = make_n_col_view(&schema, n, |row, _| row as i64, |row, _| null_row(row));
    let run = |instr| passing_rows(&filter_prog(&schema, vec![instr], 1, 0, vec![]), &mb, n);
    let is_null = run(is_null_op(0, 1));
    let is_not_null = run(is_not_null_op(0, 1));
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
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 5 },
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
        LogicalInstr::BoolOr { dst: 5, a: 2, b: 4 },
    ];
    let ev = filter_prog(&schema, instrs, 6, 5, vec![]);
    assert_eq!(
        passing_rows(&ev, &mb, n),
        vec![false],
        "NULL OR FALSE is NULL, which the filter drops"
    );
}

/// A 3-conjunct chain read through `eval_row`, which reports the null bit
/// directly where `filter` cannot: it consumes `bool_bits & !null_bits`, so a
/// cleared bool already forces the verdict and NULL is indistinguishable from
/// FALSE there. A filter-resolved program really is driven this way — `passes`
/// above is exactly that.
///
/// Two rows, because the interesting cases are the two the chain can produce:
/// `TRUE AND NULL AND TRUE` is NULL, and a definite-FALSE chain is FALSE rather
/// than the previous drive's NULL carried forward in the scratch.
#[test]
fn and_chain_null_and_false_through_eval_row() {
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
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: 0 },
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
        LogicalInstr::LoadColInt { dst: 6, col: 3 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 7,
            a: 6,
            b: 1,
        },
        LogicalInstr::BoolAnd { dst: 8, a: 5, b: 7 },
    ];
    let ev = filter_prog(&schema, instrs, 9, 8, vec![]);

    assert_eq!(ev.eval_row(&mb, 0), (0, true), "TRUE AND NULL AND TRUE is NULL");
    assert_eq!(
        ev.eval_row(&mb, 1),
        (0, false),
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
        LogicalInstr::LoadColInt { dst: 0, col: 1 },
        LogicalInstr::LoadConst { dst: 1, val: -1 },
        LogicalInstr::Cmp {
            op: CmpOp::Eq,
            dst: 2,
            a: 0,
            b: 1,
        },
        LogicalInstr::LoadColInt { dst: 3, col: 2 },
        LogicalInstr::LoadConst { dst: 4, val: 0 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 5,
            a: 3,
            b: 4,
        },
        LogicalInstr::BoolAnd { dst: 6, a: 2, b: 5 },
        LogicalInstr::LoadColInt { dst: 7, col: 3 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 8,
            a: 7,
            b: 4,
        },
        LogicalInstr::BoolAnd { dst: 9, a: 6, b: 8 },
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
            let ev = filter_prog(&schema, instrs.clone(), 10, 9, vec![]);
            assert_eq!(
                ev.prog.no_nulls, !nullable,
                "{label}: wrong arm — the drive proves nothing"
            );
            assert_eq!(
                passing_rows(&ev, &mb, n),
                (0..n).map(survives).collect::<Vec<_>>(),
                "{label}: n={n}"
            );
        }
    }
}

/// Reference 3VL: returns (truthy, is_null) for `a AND b`.
fn ref_and(a: Option<bool>, b: Option<bool>) -> (bool, bool) {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => (false, false),
        (Some(true), Some(true)) => (true, false),
        _ => (false, true), // NULL
    }
}

/// Regression guard for the shared German-string comparator on the
/// `col <op> 'const'` filter loop — ~1M rows, non-nullable STRING. Both channels
/// for that shape run over the same view: the fused 16-byte-cell opcode, and the
/// `LOAD_COL_STR` + `LOAD_CONST_STR` + `STR_CMP` register compare. Asserting
/// their hit counts equal rules out the two channels doing different amounts of
/// work; the reported figure is still wall-clock on a box whose absolute timings
/// are noisy, so read the ratio and ignore the milliseconds.
///
/// The controlled pair is `digits-first` against `abcd-shared-prefix`: same
/// lengths, same content bytes, differing only in whether the 4-byte prefix
/// collides. Only the cell form can short-circuit on that prefix — a `StrView`
/// carries none — so the fused time moves between the two and the register time
/// does not, and the gap remaining at `abcd*` is the register lane's own cost of
/// materialising each row into a `MORSEL`-wide lane. Matching the lengths is
/// what makes that attributable: the fall-through compare is a plain byte
/// compare, so a shorter value would have moved both channels.
///
/// Both channels run in one process, so a `perf stat` over this test measures
/// their sum — separating retired instructions per kernel would need one process
/// each, which this test does not do.
///
/// `#[ignore]`; run release:
///   cargo test -p gnitz-expr --release str_const_filter_bench \
///       -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn str_const_filter_bench() {
    let schema = TestSchema::new(&[(type_code::U64, false), (type_code::STRING, false)], &[0]);
    let n = 1_000_000usize;
    // (domain, constant, value per row). `mixed` is the original fixture:
    // ~1/16 rows match and every 7th row is a long (heap-backed) string.
    type Domain = (&'static str, &'static str, fn(usize) -> String);
    let domains: [Domain; 4] = [
        ("mixed", "match_target", |row| {
            if row % 16 == 0 {
                "match_target".to_string()
            } else if row % 7 == 0 {
                format!("long_string_variant_number_{row}")
            } else {
                format!("k{}", row % 97)
            }
        }),
        ("long", "long_string_variant_number_42", |row| {
            format!("long_string_variant_number_{}", row % 97)
        }),
        // The controlled pair: `{i}abcd` and `abcd{i}` hold the same bytes at the
        // same lengths, so the prefix is the only thing that differs.
        ("digits-first", "42abcd", |row| format!("{}abcd", row % 97)),
        ("abcd-shared-prefix", "abcd42", |row| format!("abcd{}", row % 97)),
    ];
    const PASSES: usize = 30;

    for (domain, constant, value) in domains {
        let mut mb = TestView::new(n, 8);
        mb.push_col(16);
        for row in 0..n {
            mb.set_pk_col(row, 0, &(row as u64 + 1).to_le_bytes(), type_code::U64);
            mb.set_string(row, 0, value(row).as_bytes());
        }

        for (name, op) in [("eq", StrOp::Eq), ("lt", StrOp::Lt)] {
            let consts = vec![constant.as_bytes().to_vec()];
            let fused = filter_prog(
                &schema,
                vec![LogicalInstr::StrColConst {
                    op,
                    dst: 0,
                    col: 1,
                    const_idx: 0,
                }],
                1,
                0,
                consts.clone(),
            );
            let regs = filter_prog(
                &schema,
                vec![
                    LogicalInstr::LoadColStr { dst: 0, col: 1 },
                    LogicalInstr::LoadConstStr { dst: 1, const_idx: 0 },
                    LogicalInstr::StrCmp { op, dst: 2, a: 0, b: 1 },
                ],
                3,
                2,
                consts,
            );

            // Warm-up (which is also the equality check), then report the
            // FASTEST of many timed passes — the minimum is robust against
            // thermal throttling and scheduler noise, unlike a mean over the
            // whole run.
            let count = |f: &Evaluator| {
                let mut hits = 0usize;
                f.filter(&mb, n, |s, e| hits += e - s);
                hits
            };
            let hits = count(&fused);
            assert_eq!(hits, count(&regs), "{domain}/{name}: the channels disagree");

            let pass = |f: &Evaluator| {
                let t = std::time::Instant::now();
                let mut h = 0usize;
                f.filter(&mb, n, |s, e| h += e - s);
                std::hint::black_box(h);
                t.elapsed()
            };
            // Alternate which channel is timed first, so a drift over the run
            // cannot land wholly on whichever one always went second.
            let (mut bf, mut br) = (std::time::Duration::MAX, std::time::Duration::MAX);
            for i in 0..PASSES {
                if i % 2 == 0 {
                    bf = bf.min(pass(&fused));
                    br = br.min(pass(&regs));
                } else {
                    br = br.min(pass(&regs));
                    bf = bf.min(pass(&fused));
                }
            }
            println!(
                "str_const_filter {domain}/{name}: {n} rows, best of {PASSES} passes = \
                 fused {bf:?} ({:.1} M rows/s), registers {br:?} ({:.1} M rows/s), \
                 ratio {:.2}x (hits={hits})",
                n as f64 / bf.as_secs_f64() / 1e6,
                n as f64 / br.as_secs_f64() / 1e6,
                br.as_secs_f64() / bf.as_secs_f64()
            );
        }
    }
}

/// Retired-instruction harness for the filter kernels. Prints nothing useful on
/// its own: run it at two pass counts and difference them, so batch setup and
/// process start cancel out. Wall-clock on these machines is far noisier than
/// the effects being measured, so it is never reported.
///
///   for p in 1 501; do GNITZ_BENCH_PASSES=$p perf stat -e instructions:u \
///     cargo test -p gnitz-expr --release filter_kernel_bench -- --ignored --nocapture; done
/// The pass count the `#[ignore]`d benches loop over, from `GNITZ_BENCH_PASSES`.
/// Two runs at different counts, differenced, cancel everything that happens
/// once per process.
fn bench_passes() -> usize {
    std::env::var("GNITZ_BENCH_PASSES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}

#[test]
#[ignore]
fn filter_kernel_bench() {
    let passes = bench_passes();
    let n = 200_000usize;

    // `pk > n/2` — the PK-region load.
    let pk_schema = schema_pk_ints(1, false);
    let pk_view = make_n_col_view(&pk_schema, n, |_, _| 1, |_, _| false);
    let pk_filter = filter_prog(
        &pk_schema,
        vec![
            LogicalInstr::LoadColInt { dst: 0, col: 0 },
            LogicalInstr::LoadConst {
                dst: 1,
                val: (n / 2) as i64,
            },
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 2,
                a: 0,
                b: 1,
            },
        ],
        3,
        2,
        vec![],
    );

    // `-a > 0 AND b < 80` over nullable columns — unary, the 3VL AND, and the
    // per-column null-bit gather.
    let nn_schema = schema_pk_ints(2, true);
    let nn_view = make_n_col_view(
        &nn_schema,
        n,
        |row, col| ((row * 7 + col) % 100) as i64,
        |row, _| row % 32 == 0,
    );
    let nn_filter = filter_prog(
        &nn_schema,
        vec![
            LogicalInstr::LoadColInt { dst: 0, col: 1 },
            LogicalInstr::IntUnary {
                op: crate::program::IntUnaryOp::Neg,
                dst: 1,
                a: 0,
            },
            LogicalInstr::LoadConst { dst: 2, val: 0 },
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 3,
                a: 1,
                b: 2,
            },
            LogicalInstr::LoadColInt { dst: 4, col: 2 },
            LogicalInstr::LoadConst { dst: 5, val: 80 },
            LogicalInstr::Cmp {
                op: CmpOp::Lt,
                dst: 6,
                a: 4,
                b: 5,
            },
            LogicalInstr::BoolAnd { dst: 7, a: 3, b: 6 },
        ],
        8,
        7,
        vec![],
    );

    let mut hits = 0usize;
    for _ in 0..passes {
        pk_filter.filter(&pk_view, n, |s, e| hits += e - s);
        nn_filter.filter(&nn_view, n, |s, e| hits += e - s);
    }
    println!(
        "filter_kernel_bench passes={passes} n={n} hits={}",
        std::hint::black_box(hits)
    );
}

/// `col1 IS NULL AND col2 > k AND ... ` over `is_null_bench_schema`: `n_cmp`
/// compares of NOT NULL columns hung off one null test, so the whole predicate
/// still resolves `no_nulls`. `n_cmp` sets the chain depth, which is what scales
/// the per-conjunct cost the arms are being compared on.
fn is_null_chain(k: i64, n_cmp: u16) -> FilterShape {
    let mut instrs = vec![is_null_op(0, 1)];
    if n_cmp == 0 {
        // No compare, so no constant to load — an unread `LoadConst` would still
        // cost a register write per morsel and blunt the bare shape's figure.
        return (instrs, 1, 0);
    }
    instrs.push(LogicalInstr::LoadConst { dst: 1, val: k });
    let mut acc = 0u16;
    for i in 0..n_cmp {
        let base = 2 + i * 3;
        instrs.push(LogicalInstr::LoadColInt {
            dst: base,
            col: u32::from(i) + 2,
        });
        instrs.push(LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: base + 1,
            a: base,
            b: 1,
        });
        instrs.push(LogicalInstr::BoolAnd {
            dst: base + 2,
            a: acc,
            b: base + 1,
        });
        acc = base + 2;
    }
    let num_regs = u32::from(2 + n_cmp * 3);
    (instrs, num_regs, u32::from(acc))
}

/// One nullable column (the null test's) plus four NOT NULL ones (the
/// compares'). Mixing the two is what the bench is about: a compare over a
/// nullable column would hold the program on the nullable arm through its own
/// load, whatever the null test is classified as.
fn is_null_bench_schema() -> TestSchema {
    let mut cols = vec![(type_code::U64, false), (type_code::I64, true)];
    cols.extend(std::iter::repeat_n((type_code::I64, false), 4));
    TestSchema::new(&cols, &[0])
}

/// A/B for the arm an `IS [NOT] NULL` predicate lands on. Each shape is built
/// twice from one instruction stream — once as resolution classifies it
/// (`no_nulls`), once forced onto the nullable arm — and the two are asserted to
/// select the same rows before either is driven. Prints no measurement itself,
/// like [`filter_kernel_bench`]: `GNITZ_BENCH_SHAPE` and `GNITZ_BENCH_ARM` cut
/// the run down to one driven loop, and differencing two pass counts under
/// `perf` cancels fixture construction, the warm-up and process start.
///
///   for s in bare one_and chain_spread chain_clustered \
///            chain_nonselective chain_rare map; do
///     for arm in fast nullable; do for p in 1 201; do \
///       GNITZ_BENCH_SHAPE=$s GNITZ_BENCH_ARM=$arm GNITZ_BENCH_PASSES=$p \
///       perf stat -e instructions:u,cycles:u cargo test -p gnitz-expr --release \
///         is_null_arm_bench -- --ignored --nocapture --test-threads=1
///   done; done; done
///
/// Moving to the fast arm is cheaper on every shape here, at
/// `-C target-cpu=x86-64-v3` (what `crates/.cargo/config.toml` ships): −3.0 %
/// retired instructions on `map`, −5.9 % on `bare`, −11.1 % on `one_and`, and
/// −13.8 % to −14.5 % across the four 4-conjunct chains. What separates the
/// chain shapes from each other is only their NULL arrangement, and it barely
/// separates them at all — the arms run the same kernels over the same word
/// count. The compares read NOT NULL columns, which are outside
/// `nullable_slots`, so the nullable arm clears their null words rather than
/// gathering per row; what is left is the null bookkeeping the fast arm has
/// none of.
///
/// Take both events. `instructions:u` repeats here to under 0.001 %, `cycles:u`
/// to a few percent; the first is the reproducible one, the second is the one
/// that sees a stall. Neither is a constant — batch size, NULL rate and
/// clustering all move them.
#[test]
#[ignore]
fn is_null_arm_bench() {
    let passes = bench_passes();
    let arm = std::env::var("GNITZ_BENCH_ARM").unwrap_or_else(|_| "both".to_string());
    let (run_fast, run_nullable) = (arm != "nullable", arm != "fast");
    // Which shape to drive. Every shape is still built and checked; only the
    // driven loop is skipped, so a `perf stat` over the process attributes its
    // pass-count difference to the one named here.
    let only = std::env::var("GNITZ_BENCH_SHAPE").unwrap_or_else(|_| "all".to_string());
    // Both selectors are decoded by inequality, so a typo would silently drive
    // nothing (or both arms) and read out as a 0 % effect rather than an error.
    assert!(
        matches!(arm.as_str(), "both" | "fast" | "nullable"),
        "GNITZ_BENCH_ARM must be both/fast/nullable, got {arm:?}"
    );
    let driven = |name: &str, want: bool| want && (only == "all" || only == name);
    let n = 200_000usize;
    let schema = is_null_bench_schema();

    // Three views, shared by the shapes that want the same NULL arrangement.
    // They span how the NULLs are distributed rather than just how many there
    // are: `spread` puts 16 per morsel, `rare` 4, and `clustered` gives 7 of
    // every 8 morsels no NULL at all — the arrangement a morsel-granular
    // optimization would be most sensitive to.
    let value = |row: usize, col: usize| ((row * 7 + col * 13) % 100) as i64;
    let spread = make_n_col_view(&schema, n, value, |row, col| col == 0 && row.is_multiple_of(16));
    let clustered = make_n_col_view(&schema, n, value, |row, col| {
        col == 0 && (row / MORSEL).is_multiple_of(8)
    });
    let rare = make_n_col_view(&schema, n, value, |row, col| col == 0 && row.is_multiple_of(64));

    // `k = 50` passes about half the rows; `k = -1` passes every row, which is
    // the non-selective variant.
    let shapes: [(&str, &TestView, FilterShape); 6] = [
        ("bare", &spread, is_null_chain(50, 0)),
        ("one_and", &spread, is_null_chain(50, 1)),
        ("chain_spread", &spread, is_null_chain(50, 4)),
        ("chain_clustered", &clustered, is_null_chain(50, 4)),
        ("chain_nonselective", &spread, is_null_chain(-1, 4)),
        ("chain_rare", &rare, is_null_chain(50, 4)),
    ];

    let mut selected = 0usize;
    for (name, view, (instrs, num_regs, result_reg)) in &shapes {
        let (fast, nullable) = both_arms(name, || {
            filter_prog(&schema, instrs.clone(), *num_regs, *result_reg, vec![])
        });
        // Also the warm-up, and outside the driven region.
        let passed = passing_rows(&fast, view, n);
        assert_eq!(passed, passing_rows(&nullable, view, n), "{name}: the arms disagree");
        let hits = passed.iter().filter(|&&p| p).count();

        let run = |ev: &Evaluator| {
            let mut h = 0usize;
            for _ in 0..passes {
                ev.filter(*view, n, |s, e| h += e - s);
            }
            std::hint::black_box(h);
        };
        for (want, ev) in [(run_fast, &fast), (run_nullable, &nullable)] {
            if driven(name, want) {
                selected += 1;
                run(ev);
            }
        }
        println!("is_null_arm_bench {name}: passes={passes} n={n} hits={hits}");
    }

    // The map drive, which leaves through EMIT's register rather than a bitmap.
    let out_schema = schema_pk_ints(1, false);
    let map_instrs = vec![is_null_op(0, 1), LogicalInstr::Emit { src: 0, out: 0 }];
    let (fast, nullable) = both_arms("map", || {
        map_prog(&schema, &out_schema, map_instrs.clone(), 1, 0, vec![])
    });
    // The warm-up doubles as the agreement check, as it does per filter shape.
    let emitted = |ev: &Evaluator| {
        let mut vals = Vec::with_capacity(n);
        ev.eval_morsels(&spread, 0, n, |_, out| vals.extend_from_slice(out.reg_values(0)));
        vals
    };
    assert_eq!(emitted(&fast), emitted(&nullable), "map: the arms disagree");
    let run = |ev: &Evaluator| {
        let mut acc = 0i64;
        for _ in 0..passes {
            ev.eval_morsels(&spread, 0, n, |_, out| acc += out.reg_values(0).iter().sum::<i64>());
        }
        std::hint::black_box(acc);
    };
    for (want, ev) in [(run_fast, &fast), (run_nullable, &nullable)] {
        if driven("map", want) {
            selected += 1;
            run(ev);
        }
    }
    println!("is_null_arm_bench map: passes={passes} n={n}");
    // A misspelled shape name would otherwise drive nothing at all, and the two
    // pass counts would difference to a 0 % effect instead of failing.
    assert!(selected > 0, "GNITZ_BENCH_SHAPE matched no shape: {only:?}");
}
