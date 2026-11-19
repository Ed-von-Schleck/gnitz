//! The ad-hoc SELECT finish: ORDER BY / OFFSET / LIMIT applied as one client-side
//! pass over the result the client holds.

use std::sync::Arc;

use gnitz_core::{Schema, ZSetBatch};
use gnitz_expr::{OrderLocator, RowRanking, order_locators};
use gnitz_wire::{OrderKey, WeightSlots};
use gnitz_zset::repr::{Pick, materialize_carrying};

use crate::error::GnitzSqlError;

/// ORDER BY / OFFSET / LIMIT over a finished result, resolved against its schema at plan
/// time. The schema is the result's, carried for the reply; [`ResultCut::apply`] reads
/// only the keys and the slice.
pub(crate) struct ResultCut {
    schema: Arc<Schema>,
    /// The written keys, hidden appended ones included; under a cut, extended to a total
    /// order. Empty when unordered.
    keys: Vec<OrderLocator>,
    /// The OFFSET / LIMIT window, cut exactly here after any worker's superset cut.
    slice: WeightSlots,
}

impl ResultCut {
    /// `order`, wire keys over `schema`, then `slice`.
    pub(crate) fn new(schema: Arc<Schema>, order: &[OrderKey], slice: WeightSlots) -> Result<Self, GnitzSqlError> {
        // A cut breaks ties by identity, so which tied rows it keeps is a function of the
        // rows alone. Uncut ties stay in input order.
        let keys = match order.is_empty() {
            true => Vec::new(),
            false => {
                order_locators(order, schema.layout(), slice != WeightSlots::ALL).map_err(GnitzSqlError::Internal)?
            }
        };
        Ok(ResultCut { schema, keys, slice })
    }

    /// The schema of the result this cut applies to.
    pub(crate) fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub(crate) fn is_ordered(&self) -> bool {
        !self.keys.is_empty()
    }

    /// Whether the slice skips or bounds anything.
    pub(crate) fn cuts(&self) -> bool {
        self.slice != WeightSlots::ALL
    }

    /// Whether [`Self::apply`] returns its input as it is.
    pub(crate) fn is_identity(&self) -> bool {
        !self.is_ordered() && !self.cuts()
    }

    /// Whether the result is empty whatever the input: `LIMIT 0`.
    pub(crate) fn keeps_nothing(&self) -> bool {
        self.slice.keep == 0
    }

    /// `batch` sorted and cut to the slice, counting logical rows by weight.
    pub(crate) fn apply(&self, batch: ZSetBatch) -> ZSetBatch {
        if self.is_identity() {
            return batch;
        }
        let view = batch.view();
        debug_assert!(
            view.weights().all(|w| w > 0),
            "client cut: non-positive weight violates the bag invariant"
        );
        let as_usize = |n: u64| usize::try_from(n).unwrap_or(usize::MAX);
        let end = self.slice.end().map_or(usize::MAX, as_usize);
        let rows: Vec<u32> = if self.keys.is_empty() {
            // Every entry holds at least one logical row, so the first `end` entries cover the slice.
            (0..view.len().min(end) as u32).collect()
        } else {
            let mut ranking = RowRanking::new(&self.keys, &view);
            ranking.keep_smallest(end);
            ranking.sorted()
        };
        if !self.cuts() {
            return batch.gather(&rows);
        }
        let mut slots = self.slice;
        // Each kept row at the logical rows it takes; every kept row takes at least one of
        // the `keep` slots.
        let mut picks: Vec<Pick> = Vec::with_capacity(rows.len().min(as_usize(slots.keep)));
        for &r in &rows {
            let take = slots.take(view.get_weight(r as usize));
            if take > 0 {
                picks.push((0, r, take));
            }
            if slots.keep == 0 {
                break;
            }
        }
        ZSetBatch::of(batch.schema(), materialize_carrying(&[view], view.schema(), &picks))
    }
}

#[cfg(test)]
#[path = "tests/order.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/order.rs"]
mod bench;
