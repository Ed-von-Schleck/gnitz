//! The ad-hoc SELECT finish: ORDER BY / OFFSET / LIMIT applied as one client-side
//! pass over a fetched result (base tables and views).
//!
//! The batch arrives as a client `ZSetBatch`: entries carrying an integer
//! **weight**. Entries are *not* unique by `(PK, payload)` — a reply train is
//! concatenated and never folded — and the finish does not need them to be, only
//! `weight >= 1`. It sorts by wire keys over the result's own schema, hidden
//! appended keys included; under a cut the order is made total by the identity
//! tiebreak the worker's top-k uses. LIMIT and OFFSET count summed weight, not
//! entries. The invariants are pinned by the unit tests below.

use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::{cmp_order_keys, order_locators};

/// The client-side cut a sink applies to its own result. `limit: None` is
/// unbounded.
#[derive(Clone, Copy)]
pub(crate) struct Window {
    pub(crate) offset: usize,
    pub(crate) limit: Option<usize>,
}

impl Window {
    /// Whether this window skips or bounds anything.
    pub(crate) fn cuts(&self) -> bool {
        self.offset > 0 || self.limit.is_some()
    }

    /// The logical row the cut ends before; `None` when unbounded.
    pub(crate) fn end(&self) -> Option<usize> {
        self.limit.map(|l| self.offset.saturating_add(l))
    }
}

/// Walk entries, in the order given, over the logical window `[offset, end)`
/// (`end = u64::MAX` when unbounded), returning each surviving entry's row index
/// and its surviving weight. Entry *i* occupies `[Cᵢ, Cᵢ + wᵢ)` (cumulative weight
/// before it); its surviving weight is `max(0, min(Cᵢ+wᵢ, end) − max(Cᵢ, offset))`.
/// This one overlap formula handles OFFSET mid-entry, LIMIT mid-entry, and both
/// cuts inside the same entry. Requires every weight ≥ 1.
fn paginate(rows: impl IntoIterator<Item = (usize, i64)>, offset: u64, end: u64) -> Vec<(usize, i64)> {
    let mut cum: u64 = 0;
    let mut out = Vec::new();
    for (r, w) in rows {
        let lo_i = cum;
        let hi_i = cum.saturating_add(w as u64);
        let surviving = hi_i.min(end).saturating_sub(lo_i.max(offset));
        if surviving > 0 {
            out.push((r, surviving as i64));
        }
        cum = hi_i;
        if cum >= end {
            break;
        }
    }
    out
}

/// Sort `batch` by `order` — wire keys over `schema`, hidden appended keys included —
/// and cut it to `window`, counting logical rows by weight. Hands `batch` back
/// untouched when there is nothing to order or cut.
pub(crate) fn order_and_window(
    schema: &Schema,
    batch: ZSetBatch,
    order: &[gnitz_wire::OrderKey],
    window: Window,
) -> ZSetBatch {
    let end = window.end();
    let cut = window.cuts();
    if order.is_empty() && !cut {
        return batch;
    }
    // Bag positivity, the precondition of both steps below: the cut keeps
    // `offset + limit` entries because each covers at least one logical row, and
    // `paginate` sums weights into a running position. Checked over the whole
    // batch, before the cut can discard the violator unseen.
    debug_assert!(
        batch.weights.iter().all(|&w| w > 0),
        "ordering sink: non-positive weight violates the bag invariant"
    );
    let tiebroken = order_locators(order, schema).expect("planned order keys index the result schema");
    // A cut appends the identity tiebreak, so it picks the same rows the worker's top-k
    // kept, on every worker count. Without one the written keys alone order: ties are
    // SQL's to leave open, and a total order would cost n·log n comparisons where equal
    // keys partition in n·log k.
    let keys = if cut { &tiebroken[..] } else { &tiebroken[..order.len()] };
    let mut perm: Vec<usize> = (0..batch.len()).collect();
    if !keys.is_empty() {
        let cmp = |&a: &usize, &b: &usize| cmp_order_keys(keys, &batch, a, &batch, b);
        // Ties are left only between identical rows here, so a partial selection is
        // exact however it splits a tie group.
        if let Some(k) = end.filter(|&k| k < perm.len()) {
            if k > 0 {
                perm.select_nth_unstable_by(k - 1, cmp);
            }
            perm.truncate(k);
        }
        // Stable, so ties without a cut keep reply order.
        perm.sort_by(cmp);
    }
    let survivors = paginate(
        perm.iter().map(|&r| (r, batch.weights[r])),
        window.offset as u64,
        end.map_or(u64::MAX, |e| e as u64),
    );
    batch.gather(&survivors)
}

#[cfg(test)]
#[path = "tests/order.rs"]
mod tests;
