use crate::error::GnitzSqlError;
use crate::expr_lower::compile_conjuncts_evaluator;
use crate::ir::BoundExpr;
use gnitz_core::{Schema, ZSetBatch};

/// Indices of the rows of `batch` that pass every residual predicate, in
/// increasing order. Serves the UPDATE/DELETE resolution, which re-imposes the
/// WHERE on the transaction's own buffered rows and needs the passing indices.
///
/// The conjuncts are AND-compiled once, then
/// driven over the whole batch by the shared evaluator — the same program a
/// `CREATE VIEW` WHERE compiles to, so a DML residual and a view filter cannot
/// disagree.
///
/// **Folding is sound.** Per-conjunct short-circuiting excluded a row whose
/// conjunct was NULL; the chain agrees, because `BoolAnd`'s nullable arm yields
/// definite-false when either side is definite-false and NULL otherwise, and
/// `Evaluator::filter_ranges` keeps `bool_bits & !null_bits` — so both (NULL, TRUE) and
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
    let Some(ev) = compile_conjuncts_evaluator(preds.iter().copied(), schema)? else {
        return Ok((0..n).collect());
    };
    // One view over the whole batch: `ViewBuffers::view` rebuilds a region list
    // per call, so a per-row view would be a malloc plus a PK-region rebuild per
    // row. The row count comes from the view, so the drive cannot run past the
    // batch's end.
    let mut bufs = gnitz_core::ViewBuffers::default();
    let view = bufs.view(batch, schema);
    let mut ranges = Vec::new();
    ev.filter_ranges(&view, &mut ranges);
    // Ranges arrive in increasing order with `end` EXCLUSIVE, so `matched` keeps
    // the sorted-index contract its callers rely on.
    let mut matched = Vec::with_capacity(n);
    for (start, end) in ranges {
        matched.extend(start..end);
    }
    Ok(matched)
}

#[cfg(test)]
#[path = "tests/residual.rs"]
mod tests;
