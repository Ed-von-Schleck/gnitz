use crate::error::GnitzSqlError;
use crate::expr_lower::compile_conjuncts_evaluator;
use crate::ir::BoundExpr;
use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::RangeMembership;

/// Indices of the rows of `batch` inside `walk` that pass every residual predicate, in
/// increasing order: the transaction's own buffered rows an UPDATE/DELETE matches.
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
    walk: Option<&RangeMembership>,
) -> Result<Vec<usize>, GnitzSqlError> {
    let n = batch.pks.len();
    // Zero rows: nothing to test.
    if n == 0 {
        return Ok(Vec::new());
    }
    // `None` is the statically-true verdict: the binder const-folds a null test on a
    // non-nullable column, so `WHERE nonnull_col IS NOT NULL` arrives as `LitInt(1)`.
    let ev = compile_conjuncts_evaluator(preds.iter().copied(), schema)?;
    let mut ranges = Vec::new();
    let mut words = Vec::new();
    match (ev, walk) {
        (None, None) => return Ok((0..n).collect()),
        (None, Some(w)) => w.filter_ranges(batch, &mut words, &mut ranges),
        (Some(ev), None) => ev.filter_ranges(batch, &mut ranges),
        (Some(ev), Some(w)) => {
            w.filter_words(batch, &mut words);
            ev.filter_ranges_within(batch, &words, &mut ranges);
        }
    }
    Ok(ranges.into_iter().flat_map(|(s, e)| s..e).collect())
}

#[cfg(test)]
#[path = "tests/residual.rs"]
mod tests;
