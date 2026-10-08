//! The ad-hoc SELECT finish: ORDER BY / OFFSET / LIMIT applied as one client-side
//! pass over a fetched result (base tables and views).

use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::{order_locators, RowRanking};

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
    let rows: Vec<u32> = if order.is_empty() {
        // Every entry holds at least one logical row, so the first `end` entries cover the window.
        (0..batch.len().min(end) as u32).collect()
    } else {
        // A cut breaks ties by identity, so which tied rows it keeps is a function of the
        // rows alone. Uncut ties stay in input order.
        let keys = order_locators(order, schema.layout(), cut);
        let mut ranking = RowRanking::new(&keys, &batch);
        ranking.keep_smallest(end);
        ranking.sorted()
    };
    if !cut {
        return batch.gather(&rows);
    }
    // Entry `r` holds logical rows `[at, at + w)`; the window keeps `span` of the entries, whole
    // but for what it clips off the first and the last.
    let mut at = 0usize;
    let mut span = 0..0;
    let (mut head, mut tail) = (0i64, 0i64);
    for (i, &r) in rows.iter().enumerate() {
        let next = at.saturating_add(batch.weights[r as usize] as usize);
        let kept = next.min(end).saturating_sub(at.max(window.offset)) as i64;
        if kept > 0 {
            if span.is_empty() {
                (span.start, head) = (i, kept);
            }
            (span.end, tail) = (i + 1, kept);
        }
        at = next;
        if at >= end {
            break;
        }
    }
    let mut out = batch.gather(&rows[span]);
    if let Some(w) = out.weights.first_mut() {
        *w = head;
    }
    if let Some(w) = out.weights.last_mut() {
        *w = tail;
    }
    out
}

#[cfg(test)]
#[path = "tests/order.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/order.rs"]
mod bench;
