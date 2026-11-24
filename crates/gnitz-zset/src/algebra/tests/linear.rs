use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{
    assert_folds, certified, make_batch, make_batch_bytes, make_int_batch, make_schema_pk_u64_payload_string,
    make_schema_u64_i64, pk_only_schema, pk_payload_schema, u64_pk_schema, weighted_rows, zset_of, zset_sum,
};
use gnitz_wire::TypeCode;

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
    let rows: Vec<(u64, i64, &[Option<i64>])> = rows
        .iter()
        .map(|(pk, w, v)| (*pk, *w, std::slice::from_ref(v)))
        .collect();
    let mut b = make_int_batch(schema, &rows);
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
/// equal elements at opposite weights cancel. Inputs of two physical layouts
/// are refused.
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
    assert_eq!(
        union_nullability_merge(&pk_payload_schema(&[TypeCode::U128]), &nonnull),
        Err("union: inputs do not share a physical layout".to_string())
    );
    let a_rows = [(1, 1, Some(0)), (2, 1, Some(10)), (3, 2, Some(30)), (5, 1, Some(50))];
    let b_rows = [(1, -1, None), (2, -1, Some(10)), (3, 1, Some(31)), (4, 1, Some(40))];
    use Rows::*;
    let hows = [Claimed, InOrder, Scrambled];
    let b_cases = hows.map(|h| (h, &b_rows[..])).into_iter().chain([(Claimed, &[][..])]);
    for (b_how, b_rows) in b_cases {
        for a_how in hows {
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
}

/// A filter keeps exactly the matching rows at their weights, in input order,
/// and a consolidated input stays consolidated; when every row passes, an owned
/// input is the output.
#[test]
fn filter_keeps_exactly_the_matching_rows() {
    let schema = make_schema_u64_i64();
    let gt = |k: i64| {
        crate::test_support::cmp_all(&[(gnitz_expr::CmpOp::Gt, 1, k)])
            .resolve_filter(&schema)
            .unwrap()
    };
    let rows = [(1, 3, 5), (2, -2, 15), (3, 1, 10), (4, 2, 20), (5, 1, 0)];
    let input = certified(make_batch(&schema, &rows));

    let out = op_filter(Cow::Borrowed(&input), &mut gt(10));
    let want: Vec<_> = rows.iter().copied().filter(|&(.., v)| v > 10).collect();
    assert_eq!(
        weighted_rows(&out),
        weighted_rows(&certified(make_batch(&schema, &want)))
    );
    assert!(out.is_consolidated());
    let owned = input.clone();
    let regions = owned.pk().as_ptr();
    assert_eq!(op_filter(Cow::Owned(owned), &mut gt(-1)).pk().as_ptr(), regions);
}

// ── Derived output schemas ──────────────────────────────────────────────

/// The schema `NULL_EXTEND` compiles to and the rows its dispatch writes agree
/// at both NULL placements: the fill columns read NULL exactly where the schema
/// declares a nullable slot, the input's PK and payload land where the placement
/// leaves them, the blob heap comes along with a long string, and the consolidated
/// claim carries over.
#[test]
fn null_extend_writes_the_rows_its_schema_declares_on_both_sides() {
    let long: &[u8] = b"a-fairly-long-string-value";
    let in_schema = make_schema_pk_u64_payload_string();
    let input = certified(make_batch_bytes(&in_schema, &[(1, 1, long), (2, 1, b"short")]));
    for nulls_first in [false, true] {
        let out_schema = null_extend_output_schema(&in_schema, &[TypeCode::I64, TypeCode::String], nulls_first)
            .expect("two fill columns extend cleanly");
        let out = null_extend(&input, &out_schema, nulls_first);
        let str_slot = if nulls_first { 2 } else { 0 };
        assert_eq!(out.len(), 2, "nulls_first={nulls_first}");
        assert!(out.is_consolidated(), "nulls_first={nulls_first}");
        let declared: Vec<bool> = out_schema.payload_columns().map(|(_, c)| c.nullable).collect();
        for (row, (pk, s)) in [(1, long), (2, &b"short"[..])].into_iter().enumerate() {
            let written: Vec<bool> = (0..out_schema.num_payload_cols())
                .map(|pi| gnitz_wire::null_word_get(out.get_null_word(row), pi))
                .collect();
            assert_eq!(written, declared, "nulls_first={nulls_first} row {row}");
            assert_eq!(gnitz_wire::widen_pk_be(out.get_pk_bytes(row)), pk);
            assert_eq!(
                gnitz_wire::payload_bytes(&out, row, str_slot),
                s,
                "nulls_first={nulls_first}"
            );
        }
    }
}

/// The null-extend output is the input schema plus one nullable column per
/// fill slot, so the bound is on the merged width: a near-max-width input
/// taking a short extension over the limit is refused.
#[test]
fn a_null_extend_overflowing_the_merged_schema_is_rejected() {
    let guard = format!(
        "null-extend: merged schema column count {} exceeds MAX_COLUMNS ({})",
        gnitz_wire::MAX_COLUMNS + 1,
        gnitz_wire::MAX_COLUMNS
    );
    let extend = |s: &SchemaDescriptor, n: usize| null_extend_output_schema(s, &vec![TypeCode::I64; n], false);
    // Two columns short of the limit: one more fits, two overflow.
    let wide = u64_pk_schema(&vec![
        SchemaColumn::new(TypeCode::I64, false);
        gnitz_wire::MAX_COLUMNS - 2
    ]);
    assert!(extend(&wide, 1).is_ok());
    assert_eq!(extend(&wide, 2).expect_err("overflow").to_string(), guard);
}

/// A null-extend whose shift spans the whole null word: a PK-only input under
/// 64 fill columns ahead of it, and a 64-column input under none. Every fill
/// bit reads NULL and every input bit keeps its value.
#[test]
fn null_extend_at_the_full_null_word() {
    let pk_only = pk_only_schema(&[TypeCode::U64]);
    let mut input = Batch::with_capacity(&pk_only, 3);
    for k in 1..=3u64 {
        input.push_key_row(&k.to_be_bytes(), 1);
    }
    let input = certified(input);
    let wide = null_extend_output_schema(&pk_only, &[TypeCode::I64; 64], false).unwrap();
    for nulls_first in [false, true] {
        let out = null_extend(&input, &wide, nulls_first);
        assert_eq!(out.len(), 3);
        for row in 0..3 {
            assert_eq!(out.get_null_word(row), u64::MAX, "nulls_first={nulls_first}");
        }
        let same = null_extend(&out, &wide, nulls_first);
        for row in 0..3 {
            assert_eq!(same.get_null_word(row), u64::MAX, "nulls_first={nulls_first}");
        }
    }
}
