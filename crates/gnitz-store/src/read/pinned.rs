//! A page of a pinned store: the rows of one relation as a snapshot pinned it,
//! after a key, in key order — what a paged read of a snapshot asks each worker
//! for.

use gnitz_wire::snapshot::PageRequest;
use gnitz_zset::repr::{from_runs, from_runs_in_band, Batch};
use gnitz_zset::schema::key::{key_range_between_cuts, KeyCut};

use super::scan_spec::check_layout;
use crate::relation::RelationRegistry;

impl RelationRegistry {
    /// The first `page.limit` live rows of relation `id` as `snapshot` pinned it
    /// whose key sorts strictly above `page.after` — every row with `after`
    /// `None` — in key order, in the layout `page.layout` names. Exactly `limit`
    /// rows unless the pinned store holds fewer, so a page shorter than `limit`
    /// is the store's last.
    pub fn pinned_page(&self, snapshot: u64, id: u64, page: &PageRequest) -> Result<Batch, String> {
        let store = self.pinned_store(snapshot, id)?;
        let schema = store.schema;
        check_layout(page.layout, &schema)?;
        let limit = usize::try_from(page.limit).map_err(|_| format!("a page of {} rows", page.limit))?;
        let (runs, cap) = (store.runs.iter().cloned(), store.runs.len());
        let mut cursor = match &page.after {
            None => from_runs(runs, schema, cap),
            Some(after) => {
                if after.len() != schema.pk_stride() {
                    return Err(format!(
                        "pinned page of {id}: a key is not {} bytes",
                        schema.pk_stride()
                    ));
                }
                // No key sorts above the greatest one.
                let Some((start, _)) = key_range_between_cuts(KeyCut::above(after), KeyCut::above(&[]), after.len())
                else {
                    return Ok(Batch::empty_with_schema(&schema));
                };
                from_runs_in_band(runs, schema, cap, start.pk_bytes(), None)
            }
        };
        let mut rows = Batch::empty_with_schema(&schema);
        // A drain counts the groups it visits, cancelled ones included, so one
        // short of `limit` says nothing about what is left.
        while rows.len() < limit {
            let Some(chunk) = cursor.drain_chunk(limit - rows.len()) else {
                break;
            };
            rows.append_above(chunk);
        }
        Ok(rows)
    }
}

#[cfg(test)]
#[path = "tests/pinned.rs"]
mod tests;
