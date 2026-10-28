//! The ad-hoc SELECT finish: ORDER BY / OFFSET / LIMIT applied as one client-side
//! pass over the result the client holds.

use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::{order_locators, RowRanking, WeightSlots};

/// The OFFSET / LIMIT slice of a result, cut exactly by the client after any worker's
/// superset cut. `limit: None` is unbounded.
#[derive(Clone, Copy)]
pub(crate) struct Slice {
    pub(crate) offset: usize,
    pub(crate) limit: Option<usize>,
}

impl Slice {
    /// Whether this slice skips or bounds anything.
    pub(crate) fn cuts(&self) -> bool {
        self.offset > 0 || self.limit.is_some()
    }

    /// The logical row the cut ends before; `None` when unbounded.
    pub(crate) fn end(&self) -> Option<usize> {
        self.limit.map(|l| self.offset.saturating_add(l))
    }
}

/// Sort `batch` by `order` — wire keys over `schema`, hidden appended keys included —
/// and cut it to `slice`, counting logical rows by weight.
pub(crate) fn order_and_slice(
    schema: &Schema,
    batch: ZSetBatch,
    order: &[gnitz_wire::OrderKey],
    slice: Slice,
) -> ZSetBatch {
    let cut = slice.cuts();
    if order.is_empty() && !cut {
        return batch;
    }
    debug_assert!(
        batch.weights.iter().all(|&w| w > 0),
        "client cut: non-positive weight violates the bag invariant"
    );
    let end = slice.end().unwrap_or(usize::MAX);
    let rows: Vec<u32> = if order.is_empty() {
        // Every entry holds at least one logical row, so the first `end` entries cover the slice.
        (0..batch.len().min(end) as u32).collect()
    } else {
        // A cut breaks ties by identity, so which tied rows it keeps is a function of the
        // rows alone. Uncut ties stay in input order.
        let keys =
            order_locators(order, schema.layout(), cut).expect("the planner orders by columns of its own schema");
        let mut ranking = RowRanking::new(&keys, &batch);
        ranking.keep_smallest(end);
        ranking.sorted()
    };
    if !cut {
        return batch.gather(&rows);
    }
    let mut slots = WeightSlots {
        skip: slice.offset as u64,
        keep: slice.limit.map_or(u64::MAX, |l| l as u64),
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
