//! A page of a pinned store: the rows of one relation as a snapshot pinned it,
//! after a key, in key order — what a paged read of a snapshot asks each worker
//! for.

use gnitz_wire::snapshot::PageRequest;
use gnitz_zset::algebra::Slot;
use gnitz_zset::repr::{from_runs, from_runs_in_band, Batch};
use gnitz_zset::schema::key_above;

use super::check_layout;
use crate::relation::RelationRegistry;

impl RelationRegistry {
    /// The first live rows of relation `id` as `snapshot` pinned it whose key
    /// sorts strictly above the cursor `page` names for `store` — every row with
    /// none — in key order, in the layout `page.layout` names: `store`'s share of
    /// `page.limit`, exactly, unless the pinned store holds fewer, so a page
    /// shorter than the share is the store's last.
    pub fn pinned_page(&self, snapshot: u64, id: u64, page: &PageRequest, store: Slot) -> Result<Batch, String> {
        let after = page.after_at(store.rank as usize, store.of as usize)?;
        let share = PageRequest::rows_per_store(page.limit, store.of as usize);
        let store = self.pinned_store(snapshot, id)?;
        let schema = store.schema;
        check_layout(page.layout, &schema)?;
        let limit = usize::try_from(share).unwrap_or(usize::MAX);
        let (runs, cap) = (store.runs.iter().cloned(), store.runs.len());
        let mut cursor = match after {
            None => from_runs(runs, schema, cap),
            Some(after) => {
                if after.len() != schema.pk_stride() {
                    return Err(format!(
                        "pinned page of {id}: a key is not {} bytes",
                        schema.pk_stride()
                    ));
                }
                // No key sorts above the greatest one.
                let Some(start) = key_above(after, after.len()) else {
                    return Ok(Batch::empty_with_schema(&schema));
                };
                from_runs_in_band(runs, schema, cap, start.pk_bytes(), None)
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
