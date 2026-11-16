//! A page of a pinned store: the rows of one relation as a snapshot pinned it,
//! after a key, in key order — what a paged read of a snapshot asks each worker
//! for.

use gnitz_wire::snapshot::PageRequest;
use gnitz_zset::algebra::Slot;
use gnitz_zset::repr::{Batch, from_runs, from_runs_in_band};
use gnitz_zset::schema::key_above;

use super::{check_layout, check_stride};
use crate::relation::Snapshot;

impl Snapshot {
    /// The first live rows of relation `id` as this snapshot pinned it whose key
    /// sorts strictly above the cursor `page` names for the pinned store's
    /// share — every row with none — in key order, in the layout `page.layout`
    /// names: that share of `page.limit`, exactly, unless the pinned store holds
    /// fewer, so a page shorter than the share is the store's last.
    pub fn page(&self, id: u64, page: &PageRequest) -> Result<Batch, String> {
        let store = self
            .stores
            .get(&id)
            .ok_or_else(|| format!("the snapshot holds no relation {id}"))?;
        let Slot { rank, of } = store.share;
        let after = page.after_at(rank as usize, of as usize)?;
        let rows = PageRequest::rows_per_store(page.limit, of as usize);
        let schema = store.schema;
        check_layout(page.layout, &schema)?;
        let limit = usize::try_from(rows).unwrap_or(usize::MAX);
        let runs = store.runs.iter().cloned();
        let mut cursor = match after {
            None => from_runs(runs, schema),
            Some(after) => {
                check_stride("snapshot page", id, after.len(), schema.pk_stride())?;
                // No key sorts above the greatest one.
                let Some(start) = key_above(after, after.len()) else {
                    return Ok(Batch::empty_with_schema(&schema));
                };
                from_runs_in_band(runs, schema, start.pk_bytes(), None)
            }
        };
        // A drain passes over cancelled groups without counting them, and a pin
        // holds no view's skeleton rows, so one drain falls short of `limit`
        // only where the store ends.
        Ok(cursor
            .drain_chunk(limit)
            .unwrap_or_else(|| Batch::empty_with_schema(&schema)))
    }
}

#[cfg(test)]
#[path = "tests/pinned.rs"]
mod tests;
