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
    filter_prog, make_int_row, make_int_view, make_n_col_view, schema_pk_ints, TestSchema, TestView,
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
/// (`no_nulls` reads `regs`, the nullable arm merges `bool_bits & !null_bits`
/// and masks the `m % 64` tail), and `n = MORSEL + 8` gives the second morsel an
/// 8-row tail so phantom bits above it would show up as extra passing rows.
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

    let mut ranges = Vec::new();
    ev.filter(&mb, n, |start, end| ranges.push((start, end)));
    assert_eq!(ranges, vec![(1, 3), (4, MORSEL - 1), (MORSEL, n)]);

    // An all-pass batch is one range covering everything, not one per morsel.
    let all = make_n_col_view(&schema, n, |_, _| 1, |_, _| false);
    let mut ranges = Vec::new();
    ev.filter(&all, n, |start, end| ranges.push((start, end)));
    assert_eq!(ranges, vec![(0, n)]);

    // An all-fail batch calls back zero times.
    let none = make_n_col_view(&schema, n, |_, _| 0, |_, _| false);
    let mut count = 0;
    ev.filter(&none, n, |_, _| count += 1);
    assert_eq!(count, 0);

    // A run ending exactly on a 64-bit word boundary, with the next word all
    // zero: the run has to be closed at the boundary by the empty word itself,
    // since a bitmap walk driven off the set bits never visits it.
    let boundary = make_n_col_view(&schema, n, |row, _| i64::from(row < 64), |_, _| false);
    let mut ranges = Vec::new();
    ev.filter(&boundary, n, |start, end| ranges.push((start, end)));
    assert_eq!(ranges, vec![(0, 64)]);

    // The same, two words on: the gap word is interior rather than trailing.
    let gap = make_n_col_view(&schema, n, |row, _| i64::from(!(64..192).contains(&row)), |_, _| false);
    let mut ranges = Vec::new();
    ev.filter(&gap, n, |start, end| ranges.push((start, end)));
    assert_eq!(ranges, vec![(0, 64), (192, n)]);
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
    let instrs_is_null = vec![LogicalInstr::IsNull { dst: 0, col: 1 }];
    let kind = filter_prog(&schema, instrs_is_null, 1, 0, vec![]);
    assert!(passes(&kind, &mb, 0));
    let instrs_is_not_null = vec![LogicalInstr::IsNotNull { dst: 0, col: 1 }];
    let kind = filter_prog(&schema, instrs_is_not_null, 1, 0, vec![]);
    assert!(!passes(&kind, &mb, 0));

    // Non-null row: opposite.
    let mb = make_int_row(&schema, &[7], 0);
    let instrs_is_null = vec![LogicalInstr::IsNull { dst: 0, col: 1 }];
    let kind = filter_prog(&schema, instrs_is_null, 1, 0, vec![]);
    assert!(!passes(&kind, &mb, 0));
    let instrs_is_not_null = vec![LogicalInstr::IsNotNull { dst: 0, col: 1 }];
    let kind = filter_prog(&schema, instrs_is_not_null, 1, 0, vec![]);
    assert!(passes(&kind, &mb, 0));
}

/// Differential test: 3-clause AND over nullable columns. For every morsel
/// boundary in 1..=MORSEL+1, verify the filter agrees with a per-row 3VL
/// reference computed from the column values.
#[test]
fn bit_only_three_and_chain_boundary_sweep() {
    let schema = schema_pk_ints(3, true);

    // (col0 > 1) AND (col1 > 1) AND (col2 > 1)
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 }, // r0 = col0
        LogicalInstr::LoadConst { dst: 1, val: 1 },  // r1 = 1
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        }, // r2 = r0 > 1
        LogicalInstr::LoadColInt { dst: 3, col: 2 }, // r3 = col1
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 4,
            a: 3,
            b: 1,
        }, // r4 = r3 > 1
        LogicalInstr::BoolAnd { dst: 5, a: 2, b: 4 }, // r5 = r2 AND r4
        LogicalInstr::LoadColInt { dst: 6, col: 3 }, // r6 = col2
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 7,
            a: 6,
            b: 1,
        }, // r7 = r6 > 1
        LogicalInstr::BoolAnd { dst: 8, a: 5, b: 7 }, // r8 = r5 AND r7  (result_reg)
    ];

    // Test sizes that cover: m < 64, m = 64 exactly, m crossing 64, the full
    // MORSEL=256, and the multi-morsel case (MORSEL + 1).
    for &n in &[1, 7, 63, 64, 65, 127, 128, 255, 256, 257, 300] {
        let mb = make_n_col_view(
            &schema,
            n,
            // value cycles 0..4 to mix matching/non-matching rows
            |row, col| ((row + col) as i64) % 4,
            // null every 5th row in col0, every 7th in col1, every 11th in col2
            |row, col| match col {
                0 => row % 5 == 0,
                1 => row % 7 == 0,
                _ => row % 11 == 0,
            },
        );

        let kind = filter_prog(&schema, instrs.clone(), 9, 8, vec![]);

        let mut passed = vec![false; n];
        kind.filter(&mb, n, |s, e| {
            passed[s..e].fill(true);
        });

        for (row, &got) in passed.iter().enumerate() {
            let v0 = (row as i64) % 4;
            let v1 = ((row + 1) as i64) % 4;
            let v2 = ((row + 2) as i64) % 4;
            let n0 = row % 5 == 0;
            let n1 = row % 7 == 0;
            let n2 = row % 11 == 0;
            let b0 = if n0 { None } else { Some(v0 > 1) };
            let b1 = if n1 { None } else { Some(v1 > 1) };
            let b2 = if n2 { None } else { Some(v2 > 1) };
            let (v01, n01) = ref_and(b0, b1);
            let combined = if n01 { None } else { Some(v01) };
            let (v_all, n_all) = ref_and(combined, b2);
            let expected = !n_all && v_all;
            assert_eq!(
                got, expected,
                "n={n} row={row} v0={v0}(null={n0}) v1={v1}(null={n1}) v2={v2}(null={n2}) \
                 batch={got} expected={expected}",
            );
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

    let mut passed = vec![false; n];
    kind.filter(&mb, n, |s, e| {
        passed[s..e].fill(true);
    });
    assert!(passed.iter().all(|&p| !p), "all-null AND must reject every row");
}

/// IS_NOT_NULL feeds into BOOL_AND. Tests the tail-mask in the filter
/// fast-path: IS_NOT_NULL writes `bool_bits[dst] = !null_word`, leaving 1s
/// above bit `m % 64` of the last word. Without the tail mask those phantom
/// bits become false-positive passing rows.
#[test]
fn bit_only_is_not_null_tail_mask() {
    let schema = schema_pk_ints(2, true);
    // 65 rows — straddles the 64-bit word boundary so tail handling matters.
    let n = 65;
    // null every other row in col1, every third in col2
    let mb = make_n_col_view(&schema, n, |_, _| 0, |row, col| row % (col + 2) == 0);

    // WHERE col1 IS NOT NULL AND col2 IS NOT NULL
    let instrs = vec![
        LogicalInstr::IsNotNull { dst: 0, col: 1 },
        LogicalInstr::IsNotNull { dst: 1, col: 2 },
        LogicalInstr::BoolAnd { dst: 2, a: 0, b: 1 },
    ];
    let kind = filter_prog(&schema, instrs, 3, 2, vec![]);

    let mut passed = vec![false; n];
    kind.filter(&mb, n, |s, e| {
        passed[s..e].fill(true);
    });
    for (row, &got) in passed.iter().enumerate() {
        let nn1 = row % 2 != 0;
        let nn2 = row % 3 != 0;
        let expected = nn1 && nn2;
        assert_eq!(
            got, expected,
            "row {row}: nn1={nn1} nn2={nn2} expected={expected} got={got}",
        );
    }
}

/// The 3-clause chain `col0 > 255 AND col1 > 1 AND col2 > 1`, leading column
/// clustered (`col0 = row`, never null). Morsel 0 (rows 0..255) is entirely
/// definite-FALSE on the leading clause, so the dead-tail skip fires; later
/// morsels carry survivors, so it must not. Across the boundary sweep,
/// the filter must match a per-row 3VL reference at every n — covering the
/// firing path, the survivor path, and partial-morsel tail handling.
#[test]
fn and_chain_skip_fires_boundary_sweep() {
    let schema = schema_pk_ints(3, true);

    // col0 > 255 AND col1 > 1 AND col2 > 1  (const regs: 255 and 1)
    let instrs = vec![
        LogicalInstr::LoadColInt { dst: 0, col: 1 },  // r0 = col0
        LogicalInstr::LoadConst { dst: 1, val: 255 }, // r1 = 255
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 2,
            a: 0,
            b: 1,
        }, // r2 = col0 > 255
        LogicalInstr::LoadColInt { dst: 3, col: 2 },  // r3 = col1
        LogicalInstr::LoadConst { dst: 4, val: 1 },   // r4 = 1
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 5,
            a: 3,
            b: 4,
        }, // r5 = col1 > 1
        LogicalInstr::BoolAnd { dst: 6, a: 2, b: 5 }, // r6 = r2 AND r5  (trigger)
        LogicalInstr::LoadColInt { dst: 7, col: 3 },  // r7 = col2
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 8,
            a: 7,
            b: 4,
        }, // r8 = col2 > 1
        LogicalInstr::BoolAnd { dst: 9, a: 6, b: 8 }, // r9 = r6 AND r8  (result_reg)
    ];

    for &n in &[1, 7, 63, 64, 65, 127, 128, 255, 256, 257, 300] {
        let mb = make_n_col_view(
            &schema,
            n,
            // col0 = row (clustered); col1, col2 cycle 0..4
            |row, col| match col {
                0 => row as i64,
                1 => ((row + 1) as i64) % 4,
                _ => ((row + 2) as i64) % 4,
            },
            // col0 never null (leading clause is definite-FALSE, not NULL);
            // null col1 every 7th row, col2 every 11th
            |row, col| match col {
                0 => false,
                1 => row % 7 == 0,
                _ => row % 11 == 0,
            },
        );

        let kind = filter_prog(&schema, instrs.clone(), 10, 9, vec![]);

        let mut passed = vec![false; n];
        kind.filter(&mb, n, |s, e| {
            passed[s..e].fill(true);
        });

        for (row, &got) in passed.iter().enumerate() {
            let v0 = row as i64;
            let v1 = ((row + 1) as i64) % 4;
            let v2 = ((row + 2) as i64) % 4;
            let n1 = row % 7 == 0;
            let n2 = row % 11 == 0;
            let b0 = Some(v0 > 255); // col0 never null
            let b1 = if n1 { None } else { Some(v1 > 1) };
            let b2 = if n2 { None } else { Some(v2 > 1) };
            let (v01, n01) = ref_and(b0, b1);
            let combined = if n01 { None } else { Some(v01) };
            let (v_all, n_all) = ref_and(combined, b2);
            let expected = !n_all && v_all;
            assert_eq!(
                got, expected,
                "n={n} row={row} v0={v0} v1={v1}(null={n1}) v2={v2}(null={n2}) got={got} expected={expected}",
            );
        }
    }
}

/// NULL-flooded morsel must not misfire. `col0 = -1 AND col1 > 0 AND col2 > 0`
/// with col0 NULL on 3 of every 4 rows: the leading clause (hence the trigger
/// AND) is NULL — not definite-FALSE — on those rows, so `alive != 0` and the
/// skip must not fire. The 1-in-4 survivor rows (col0 = -1) must still pass; a
/// misfire would zero the terminal and wrongly drop them. (An all-NULL morsel
/// could not catch a misfire: NULL and FALSE both mean "excluded", so the
/// output would be identical either way — the survivors are what make it sharp.)
#[test]
fn and_chain_null_flood_does_not_misfire() {
    let schema = schema_pk_ints(3, true);

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
        LogicalInstr::BoolAnd { dst: 6, a: 2, b: 5 }, // trigger
        LogicalInstr::LoadColInt { dst: 7, col: 3 },
        LogicalInstr::Cmp {
            op: CmpOp::Gt,
            dst: 8,
            a: 7,
            b: 4,
        },
        LogicalInstr::BoolAnd { dst: 9, a: 6, b: 8 }, // result_reg
    ];

    for &n in &[64, 256, 257] {
        let mb = make_n_col_view(
            &schema,
            n,
            // col0 = -1 (meaningful only on survivor rows); col1 = col2 = 5
            |_row, col| if col == 0 { -1 } else { 5 },
            // col0 NULL on 3 of every 4 rows; col1/col2 never null
            |row, col| col == 0 && row % 4 != 0,
        );

        let kind = filter_prog(&schema, instrs.clone(), 10, 9, vec![]);

        let mut passed = vec![false; n];
        kind.filter(&mb, n, |s, e| {
            passed[s..e].fill(true);
        });

        for (row, &got) in passed.iter().enumerate() {
            // survivor rows (row % 4 == 0): col0 = -1 → all clauses TRUE → pass.
            // other rows: col0 NULL → chain NULL → fail.
            let expected = row % 4 == 0;
            assert_eq!(got, expected, "n={n} row={row} got={got} expected={expected}");
        }
    }
}

/// Non-nullable AND chain: the same predicate over NOT NULL columns selects the
/// `no_nulls` arm, where `bool_bits`/`null_bits` are never allocated. The skip
/// lives only on the nullable arm, so it must never run here — were it to, the
/// `alive` reduce would index the empty `bool_bits` and panic. Results must
/// still be correct.
#[test]
fn and_chain_non_nullable_skips_runtime_check() {
    let schema = schema_pk_ints(3, false);

    // col0 = -1 AND col1 > 0 AND col2 > 0
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

    for &n in &[64, 256, 257] {
        let mb = make_n_col_view(
            &schema,
            n,
            // survivor rows (row % 4 == 0): col0 = -1; others col0 = 0 (!= -1)
            |row, col| {
                if col != 0 {
                    5
                } else if row % 4 == 0 {
                    -1
                } else {
                    0
                }
            },
            // non-nullable: no nulls anywhere
            |_row, _col| false,
        );

        let kind = filter_prog(&schema, instrs.clone(), 10, 9, vec![]);

        let mut passed = vec![false; n];
        kind.filter(&mb, n, |s, e| {
            passed[s..e].fill(true);
        });

        for (row, &got) in passed.iter().enumerate() {
            let expected = row % 4 == 0; // col0 == -1
            assert_eq!(got, expected, "n={n} row={row} got={got} expected={expected}");
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
#[test]
#[ignore]
fn filter_kernel_bench() {
    let passes: usize = std::env::var("GNITZ_BENCH_PASSES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
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
