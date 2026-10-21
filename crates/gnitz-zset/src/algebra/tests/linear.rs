use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{
    assert_folds, make_batch, make_schema_u64_i64, pk_payload_schema, u64_pk_schema, weighted_rows, zset_of, zset_sum,
};

/// How a fixture batch holds rows given in (PK, payload) order.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Rows {
    /// In that order, certified consolidated.
    Claimed,
    /// In that order, without the claim.
    InOrder,
    /// Reversed.
    Scrambled,
}

/// `(pk, weight, payload)` rows over `schema`, `None` a NULL payload, held as
/// `how` says.
fn opt_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, Option<i64>)], how: Rows) -> Batch {
    let mut rows = rows.to_vec();
    if how == Rows::Scrambled {
        rows.reverse();
    }
    let mut b = BatchBuilder::new(schema);
    for (pk, w, v) in rows {
        b.begin_row(pk as u128, w);
        b.put_opt_int(v.map(|v| v as u128));
        b.end_row();
    }
    let mut b = b.finish();
    if how == Rows::Claimed {
        b.certify_consolidated();
    }
    b
}

/// Union is Z-Set `+` under the union's own schema, whichever way it gets
/// there — owned, lent or flipped: two inputs in consolidated order merge into a
/// consolidated fold, with the claim or without it, anything else concatenates
/// and stays unconsolidated, and an empty operand on either side keeps the
/// other's claim. A NULL and a zero hold the same bytes, so only the
/// nullability the union merges in, from either side, keeps them two elements;
/// equal elements at opposite weights cancel.
#[test]
fn union_is_the_zset_sum_under_every_input_layout() {
    let (nonnull, nullable) = (
        make_schema_u64_i64(),
        u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, true)]),
    );
    let out_schema = union_nullability_merge(&nonnull, &nullable).unwrap();
    assert_eq!(union_nullability_merge(&nullable, &nonnull), Ok(out_schema));
    assert!(out_schema.columns()[1].nullable);
    assert_eq!(union_nullability_merge(&nonnull, &nonnull), Ok(nonnull));
    let a_rows = [(1, 1, Some(0)), (2, 1, Some(10)), (3, 2, Some(30)), (5, 1, Some(50))];
    let b_rows = [(1, -1, None), (2, -1, Some(10)), (3, 1, Some(31)), (4, 1, Some(40))];
    use Rows::*;
    for (a_how, b_how, b_rows) in [
        (Claimed, Claimed, &b_rows[..]),
        (Claimed, InOrder, &b_rows),
        (InOrder, InOrder, &b_rows),
        (Claimed, Scrambled, &b_rows),
        (Scrambled, Claimed, &b_rows),
        (Scrambled, Scrambled, &b_rows),
        (Claimed, Claimed, &[]),
        (InOrder, Claimed, &[]),
        (Scrambled, Claimed, &[]),
    ] {
        let (a, b) = (opt_batch(&nonnull, &a_rows, a_how), opt_batch(&nullable, b_rows, b_how));
        let consolidated = match b_rows.is_empty() {
            true => a_how == Claimed,
            false => a_how != Scrambled && b_how != Scrambled,
        };
        for (form, out) in [
            (
                "owned",
                op_union(Cow::Owned(a.clone()), Cow::Owned(b.clone()), &out_schema),
            ),
            ("lent", op_union(Cow::Borrowed(&a), Cow::Borrowed(&b), &out_schema)),
            (
                "flipped",
                op_union(Cow::Borrowed(&b), Cow::Owned(a.clone()), &out_schema),
            ),
        ] {
            let what = format!("a {a_how:?}, b {b_how:?}, b rows {}, {form}", b_rows.len());
            assert_eq!(out.schema(), &out_schema, "{what}");
            assert_eq!(out.is_consolidated(), consolidated, "{what}");
            let inputs = [a.clone(), b.clone()];
            match consolidated {
                true => assert_folds(&inputs, &out, &what),
                false => assert_eq!(zset_of(&out, &out_schema), zset_sum(&inputs, &out_schema), "{what}"),
            }
        }
    }
}

/// A filter keeps exactly the matching rows at their weights, in input order,
/// and a consolidated input stays consolidated; when every row passes it hands
/// an owned input through rather than paying a whole-batch copy for a no-op.
#[test]
fn filter_keeps_exactly_the_matching_rows() {
    let schema = make_schema_u64_i64();
    let gt = |k: i64| {
        crate::test_support::cmp_all(&[(gnitz_expr::CmpOp::Gt, 1, k)])
            .resolve_filter(&schema)
            .unwrap()
    };
    let rows = [(1, 3, 5), (2, -2, 15), (3, 1, 10), (4, 2, 20), (5, 1, 0)];
    let input = make_batch(&schema, &rows);

    let out = op_filter(Cow::Borrowed(&input), &mut gt(10));
    let want: Vec<_> = rows.iter().copied().filter(|&(.., v)| v > 10).collect();
    assert_eq!(weighted_rows(&out), weighted_rows(&make_batch(&schema, &want)));
    assert!(out.is_consolidated());
    // Every row passes: an owned input is the output, not a copy of it.
    let owned = input.clone();
    let regions = owned.pk().as_ptr();
    assert_eq!(op_filter(Cow::Owned(owned), &mut gt(-1)).pk().as_ptr(), regions);
}

// ── Derived output schemas ──────────────────────────────────────────────

/// A mismatched pair is the whole layout contract, and in release there is
/// nothing else: adopting `a`'s schema would let `op_union` read `b`'s bytes
/// through it.
#[test]
fn union_of_mismatched_input_layouts_is_rejected() {
    let a = pk_payload_schema(&[TypeCode::U128]);
    assert_eq!(
        union_nullability_merge(&a, &make_schema_u64_i64())
            .expect_err("mismatched layouts")
            .to_string(),
        "union: inputs do not share a physical layout"
    );
}

/// The schema `NULL_EXTEND` compiles to and the rows its dispatch writes are two
/// derivations of one layout, off one `nulls_first`: the fill columns must read
/// NULL at the slots the schema declares nullable, on either side.
#[test]
fn the_null_extend_schema_and_the_widened_rows_agree_on_both_sides() {
    let in_schema = make_schema_u64_i64();
    let input = make_batch(&in_schema, &[(1, 1, 42)]);
    for nulls_first in [false, true] {
        let out_schema = null_extend_output_schema(&in_schema, &[TypeCode::I64, TypeCode::String], nulls_first)
            .expect("two fill columns extend cleanly");
        let out = input.widened_with_nulls(&out_schema, nulls_first);

        let declared: Vec<bool> = out_schema.payload_columns().map(|(_, c)| c.nullable).collect();
        let written: Vec<bool> = (0..out_schema.num_payload_cols())
            .map(|pi| gnitz_wire::null_word_get(out.get_null_word(0), pi))
            .collect();
        assert_eq!(written, declared, "nulls_first={nulls_first}");
    }
}

/// The null-extend output is the input schema plus one nullable column per
/// fill slot, so the bound is on the *merged* width — a list-length bound alone
/// would miss a near-max-width input taking a short extension over the limit.
#[test]
fn a_null_extend_overflowing_the_merged_schema_is_rejected() {
    let guard = format!(
        "null-extend: merged schema column count {} exceeds MAX_COLUMNS ({})",
        gnitz_wire::MAX_COLUMNS + 1,
        gnitz_wire::MAX_COLUMNS
    );
    let extend = |s: &SchemaDescriptor, n: usize| null_extend_output_schema(s, &vec![TypeCode::I64; n], false);
    let narrow = make_schema_u64_i64();
    let out = extend(&narrow, 1).expect("a short type_codes list extends cleanly");
    assert_eq!(out.num_columns(), narrow.num_columns() + 1);
    assert!(out.columns()[out.num_columns() - 1].nullable);
    // One column short of the limit, extended by two: the merged width
    // overflows, which a bound on the list length misses.
    let wide = u64_pk_schema(&vec![
        SchemaColumn::new(TypeCode::I64, false);
        gnitz_wire::MAX_COLUMNS - 2
    ]);
    assert!(extend(&wide, 1).is_ok());
    assert_eq!(extend(&wide, 2).expect_err("overflow").to_string(), guard);
}
