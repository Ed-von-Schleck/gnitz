use crate::error::GnitzSqlError;
use crate::exec::batch::filter_batch;
use crate::ir::BoundExpr;
use crate::lower::compile_filter_evaluator;
use gnitz_core::{Schema, ZSetBatch};

/// Indices of the rows of `batch` that pass every residual predicate, in
/// increasing order. Serves the UPDATE/DELETE resolution, which re-imposes the
/// WHERE on the transaction's own buffered rows and needs the passing indices.
///
/// The conjuncts go through the shared `and_fold` and are compiled once, then
/// driven over the whole batch by the shared evaluator — the same program a
/// `CREATE VIEW` WHERE compiles to, so a DML residual and a view filter cannot
/// disagree.
///
/// **Folding is sound.** Per-conjunct short-circuiting excluded a row whose
/// conjunct was NULL; the chain agrees, because `BoolAnd`'s nullable arm yields
/// definite-false when either side is definite-false and NULL otherwise, and
/// `Evaluator::filter` keeps `bool_bits & !null_bits` — so both (NULL, TRUE) and
/// (NULL, FALSE) exclude.
///
/// `schema` is the schema the *rows* carry, which is what the conjuncts must be
/// compiled against: resolution bakes in payload slots, PK byte offsets, type
/// codes and the nullability verdict.
pub(crate) fn matching_indices(
    preds: &[&BoundExpr],
    batch: &ZSetBatch,
    schema: &Schema,
) -> Result<Vec<usize>, GnitzSqlError> {
    let n = batch.pks.len();
    // Zero rows → zero matches, before any compilation. Nothing to test means the
    // predicate need not be expressible: a transaction that buffered only
    // tombstones must not raise `Unsupported` for a WHERE the VM cannot compile.
    if n == 0 {
        return Ok(Vec::new());
    }
    // No residual → every row, before any compilation. This exempts a bound that
    // applies the whole WHERE by itself — a `pk IN (…)` gather, a full-PK point,
    // no `WHERE` at all — from building a view and materializing German cells for
    // a predicate that does not exist.
    //
    // `None` from the compile is the statically-true verdict: the binder
    // const-folds a null test on a non-nullable column, so
    // `WHERE nonnull_col IS NOT NULL` arrives as `LitInt(1)` and must keep every
    // row. The mirror needs nothing — `LitInt(0)` compiles to a real
    // `LoadConst 0` and drops every row.
    let Some(pred) = crate::ir::and_fold(preds.iter().map(|p| (*p).clone())) else {
        return Ok((0..n).collect());
    };
    let Some(ev) = compile_filter_evaluator(&pred, schema)? else {
        return Ok((0..n).collect());
    };
    let mut matched = Vec::with_capacity(n);
    // Ranges arrive in increasing order with `end` EXCLUSIVE, so `matched` keeps
    // the sorted-index contract its callers rely on.
    filter_batch(&ev, batch, schema, |start, end| matched.extend(start..end));
    Ok(matched)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::BinOp;
    use crate::test_support::{batch_2col, compound_schema_u64_u64, pk_schema, two_col, uuid_schema_payload};
    use gnitz_core::{BatchAppender, TypeCode};

    /// Which rows `pred` selects — the real path (fold → compile → adapter →
    /// vectorized kernel), which is what every test here exists to pin.
    fn matches(pred: &BoundExpr, batch: &ZSetBatch, schema: &Schema) -> Vec<usize> {
        matching_indices(&[pred], batch, schema).expect("residual must compile")
    }

    fn binop(l: BoundExpr, op: BinOp, r: BoundExpr) -> BoundExpr {
        BoundExpr::BinOp(Box::new(l), op, Box::new(r))
    }

    fn eq(col: usize, v: i64) -> BoundExpr {
        binop(BoundExpr::ColRef(col), BinOp::Eq, BoundExpr::LitInt(v))
    }

    /// One row: PK `pk`, a single zero payload cell. The shape every PK-addressing
    /// test wants — the payload exists only because the schema declares it.
    fn pk_row(schema: &Schema, pk: u128) -> ZSetBatch {
        let mut batch = ZSetBatch::new(schema);
        BatchAppender::new(&mut batch, schema).add_row(pk, 1).i64_val(0);
        batch
    }

    // Column *widening* (every width, both signednesses, both PK and payload) is
    // the evaluator's own contract and is pinned in `gnitz-expr`. What these
    // tests own is the client adapter in front of it: the OPK PK region and the
    // German string cells `ViewBuffers` materializes, the AND fold, and the two
    // "keep every row" exits.

    // ------------------------------------------------------------------
    // NULL propagation (3VL): UNKNOWN excludes the row
    // ------------------------------------------------------------------

    #[test]
    fn null_col_excludes_the_row_bare_and_compared() {
        // `WHERE val` and `WHERE val = 0` must both miss a NULL row.
        let schema = two_col(TypeCode::I64);
        let batch = batch_2col(vec![0u8; 8], TypeCode::I64, 0b1);
        assert!(matches(&BoundExpr::ColRef(1), &batch, &schema).is_empty());
        assert!(matches(&eq(1, 0), &batch, &schema).is_empty());
    }

    /// The AND fold's 3VL over more than one conjunct: a NULL conjunct excludes
    /// the row whether the other side is TRUE or FALSE, exactly as the per-conjunct
    /// short-circuit it replaced did.
    #[test]
    fn and_fold_null_conjunct_excludes_either_way() {
        let schema = two_col(TypeCode::I64);
        let batch = batch_2col(vec![0u8; 8], TypeCode::I64, 0b1); // val is NULL
        let pk_true = eq(0, 1); // pk = 1 → TRUE
        let pk_false = eq(0, 2); // pk = 2 → FALSE
        let val_null = eq(1, 0); // NULL = 0 → UNKNOWN
        for other in [pk_true, pk_false] {
            assert!(
                matching_indices(&[&other, &val_null], &batch, &schema)
                    .unwrap()
                    .is_empty(),
                "a NULL conjunct must exclude the row"
            );
        }
    }

    // ------------------------------------------------------------------
    // The PK region the adapter builds
    // ------------------------------------------------------------------

    /// A signed PK round-trips through the adapter's OPK encode and the kernel's
    /// decode — the two halves are written in different crates, so agreeing on
    /// the sign flip is a property of the pair, not of either.
    #[test]
    fn signed_pk_reads_back_through_the_opk_region() {
        let schema = pk_schema(TypeCode::I64);
        let batch = pk_row(&schema, ((-1i64) as u64) as u128);
        assert_eq!(matches(&eq(0, -1), &batch, &schema), vec![0]);
    }

    #[test]
    fn compound_pk_colref_reads_byte_region() {
        let schema = compound_schema_u64_u64();
        let mut batch = ZSetBatch::new(&schema);
        let mut pk_bytes = [0u8; 16];
        pk_bytes[..8].copy_from_slice(&7u64.to_le_bytes());
        pk_bytes[8..16].copy_from_slice(&9u64.to_le_bytes());
        // Not `BatchAppender::add_row`: its scalar `u128` PK would sign-extend
        // across the second column of a compound key.
        batch.pks.push_bytes(&pk_bytes);
        batch.weights.push(1);
        batch.nulls.push(0);
        BatchAppender::new(&mut batch, &schema).i64_val(42);
        // Both PK columns and the payload, as one three-conjunct AND chain.
        let (a, b, v) = (eq(0, 7), eq(1, 9), eq(2, 42));
        assert_eq!(matching_indices(&[&a, &b, &v], &batch, &schema).unwrap(), vec![0]);
        // The second PK column is addressed at its own OPK byte offset, not the
        // first one's.
        let wrong = eq(1, 7);
        assert!(matching_indices(&[&wrong], &batch, &schema).unwrap().is_empty());
    }

    // ------------------------------------------------------------------
    // 128-bit columns: rejected, and the message names the type
    // ------------------------------------------------------------------

    #[test]
    fn wide_pk_is_rejected() {
        let schema = pk_schema(TypeCode::U128);
        let batch = pk_row(&schema, u128::MAX);
        let err = matching_indices(&[&eq(0, 1)], &batch, &schema).expect_err("U128 PK must be rejected");
        assert!(err.to_string().contains("U128"), "error must name the type: {err}");
    }

    #[test]
    fn uuid_payload_is_rejected() {
        let schema = uuid_schema_payload();
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema).add_row(1, 1).u128_val(0);
        let err =
            matching_indices(&[&BoundExpr::ColRef(1)], &batch, &schema).expect_err("UUID column must be rejected");
        assert!(err.to_string().contains("UUID"), "error should mention UUID: {err}");
    }

    // ------------------------------------------------------------------
    // Newly servable: float columns, string columns
    // ------------------------------------------------------------------

    /// F32 and F64 both filter. The widths matter to the adapter, not just the
    /// kernel: an F32 column is a 4-byte region the view hands out and the load
    /// widens on read.
    #[test]
    fn float_payload_residual_filters() {
        for (tc, bytes) in [
            (TypeCode::F32, 1.0f32.to_le_bytes().to_vec()),
            (TypeCode::F64, 1.0f64.to_le_bytes().to_vec()),
        ] {
            let schema = two_col(tc);
            let batch = batch_2col(bytes, tc, 0);
            let gt = binop(BoundExpr::ColRef(1), BinOp::Gt, BoundExpr::LitFloat(0.5));
            assert_eq!(matches(&gt, &batch, &schema), vec![0], "{tc:?} > 0.5");
            let lt = binop(BoundExpr::ColRef(1), BinOp::Lt, BoundExpr::LitFloat(0.5));
            assert!(matches(&lt, &batch, &schema).is_empty(), "{tc:?} < 0.5");
        }
    }

    /// A string residual reads the German cells `ViewBuffers` materializes — the
    /// one part of the adapter that has no counterpart in the batch it is built
    /// from.
    #[test]
    fn string_column_residual_filters() {
        let schema = two_col(TypeCode::String);
        let mut batch = ZSetBatch::new(&schema);
        let mut app = BatchAppender::new(&mut batch, &schema);
        for (i, s) in ["alpha", "beta"].iter().enumerate() {
            app.add_row(i as u128 + 1, 1).str_val(s);
        }
        let pred = binop(BoundExpr::ColRef(1), BinOp::Eq, BoundExpr::LitStr("beta".to_string()));
        assert_eq!(matches(&pred, &batch, &schema), vec![1]);
    }

    // ------------------------------------------------------------------
    // The two "keep every row" exits
    // ------------------------------------------------------------------

    /// No conjuncts at all, and a predicate the binder already const-folded to
    /// true (`WHERE nonnull IS NOT NULL` arrives as `LitInt(1)`), both keep every
    /// row without compiling anything. The mirror `LitInt(0)` compiles for real
    /// and drops every row.
    #[test]
    fn the_keep_every_row_exits() {
        let schema = two_col(TypeCode::I64);
        let batch = batch_2col(7i64.to_le_bytes().to_vec(), TypeCode::I64, 0);
        assert_eq!(matching_indices(&[], &batch, &schema).unwrap(), vec![0]);
        assert_eq!(matches(&BoundExpr::LitInt(1), &batch, &schema), vec![0]);
        assert!(matches(&BoundExpr::LitInt(0), &batch, &schema).is_empty());
    }
}
