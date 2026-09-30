//! The ad-hoc SELECT finish: ORDER BY / OFFSET / LIMIT applied as one client-side
//! pass over a fetched result (base tables and views).

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

/// Sort `batch` by `order` — wire keys over `schema`, hidden appended keys included —
/// and cut it to `window`, counting logical rows by weight.
pub(crate) fn order_and_window(
    schema: &Schema,
    batch: ZSetBatch,
    order: &[gnitz_wire::OrderKey],
    window: Window,
) -> ZSetBatch {
    let cut = window.cuts();
    if order.is_empty() && !cut {
        return batch;
    }
    debug_assert!(
        batch.weights.iter().all(|&w| w > 0),
        "ordering sink: non-positive weight violates the bag invariant"
    );
    let end = window.end().unwrap_or(usize::MAX);
    let tiebroken = order_locators(order, schema);
    // A cut breaks ties by identity, so it keeps the rows the worker's top-k kept. Uncut ties
    // stay open: equal keys sort in n·log k, a total order in n·log n.
    let keys = if cut { &tiebroken[..] } else { &tiebroken[..order.len()] };
    let mut perm: Vec<usize> = (0..batch.len()).collect();
    if !keys.is_empty() {
        let cmp = |&a: &usize, &b: &usize| cmp_order_keys(keys, &batch, a, &batch, b);
        // Every entry holds at least one logical row, so the first `end` entries cover the window.
        if end > 0 && end < perm.len() {
            perm.select_nth_unstable_by(end - 1, cmp);
            perm.truncate(end);
        }
        perm.sort_by(cmp);
    }
    // Entry `r` holds logical rows `[at, at + w)`; it keeps their overlap with the window.
    let mut at = 0usize;
    let mut survivors = Vec::new();
    for r in perm {
        let next = at.saturating_add(batch.weights[r] as usize);
        let kept = next.min(end).saturating_sub(at.max(window.offset));
        if kept > 0 {
            survivors.push((r, kept as i64));
        }
        at = next;
        if at >= end {
            break;
        }
    }
    batch.gather(&survivors)
}

#[cfg(test)]
#[path = "tests/order.rs"]
mod tests;
