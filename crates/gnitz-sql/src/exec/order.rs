//! The ad-hoc SELECT finish: ORDER BY / OFFSET / LIMIT applied as one client-side
//! pass over a fetched result (base tables and views).

use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::{order_locators, RowRanking, WeightSlots};

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
    let mut slots = WeightSlots {
        skip: window.offset as u64,
        keep: window.limit.map_or(u64::MAX, |l| l as u64),
    };
    let (mut kept, mut takes) = (Vec::new(), Vec::new());
    for &r in &rows {
        let take = slots.take(batch.weights[r as usize]);
        if take > 0 {
            kept.push(r);
            takes.push(take);
        }
        if slots.keep == 0 {
            break;
        }
    }
    let mut out = batch.gather(&kept);
    out.weights = takes;
    out
}

#[cfg(test)]
#[path = "tests/order.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/order.rs"]
mod bench;
