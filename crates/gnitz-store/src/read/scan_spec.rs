//! The worker half of an ad-hoc `ReadSpec` read, over this worker's slice: open
//! the bound, filter, map, then forward rows or fold them. A capacity-bounded
//! view's rows hydrate chunk by chunk as the sink drains them.

use gnitz_expr::SchemaFacts;
use gnitz_wire::{PkKeys, ReadSpec};

use std::rc::Rc;

use super::SkeletonHydrator;
use crate::relation::{Cut, RelationKind, RelationRegistry};
use gnitz_expr::RowFilter;
use gnitz_zset::algebra::SinkPlan;
use gnitz_zset::repr::{Batch, SkeletonKeys};
use gnitz_zset::schema::SchemaDescriptor;

impl RelationRegistry {
    /// Execute `spec` on this worker's slice, replying in the layout whose
    /// [`SchemaFacts::layout_digest`] is `reply_layout`.
    pub fn scan_spec(
        &self,
        target_id: u64,
        spec: ReadSpec,
        reply_layout: u64,
        mut hydrator: Option<&mut dyn SkeletonHydrator>,
    ) -> Result<Rc<Batch>, String> {
        let entry = self.relation_or_err(target_id)?;
        if entry.kind() == RelationKind::Stream {
            return Err(format!(
                "relation {target_id} is a stream: a stream holds no rows and cannot be read"
            ));
        }
        let src_schema = entry.schema();
        // Nothing to hydrate either: the relation whole, off the store's cached
        // snapshot.
        if spec.is_whole() && !entry.table().has_skeleton_rows() {
            check_layout(reply_layout, &src_schema)?;
            return Ok(entry.full_scan());
        }
        let ReadSpec { bound, predicate, sink } = spec;
        let (mut source, walk) = self.open_bound(target_id, bound, Cut::Now)?;
        // A bad predicate and a bad walk are both a corrupt request: the client
        // pre-compiled the identical program at plan time.
        let mut filter =
            RowFilter::for_read(&predicate, walk.as_ref(), &src_schema).map_err(|e| format!("scan_spec: {e}"))?;
        let mut sink = SinkPlan::from_wire(&src_schema, &sink, self.config.adhoc_group_cap)?;
        check_layout(reply_layout, sink.output_schema())?;
        // Each skeleton row a chunk meets is replaced by that key's rows,
        // recomputed through `hydrator`.
        let chunk_rows = self.config.scan_chunk_rows;
        let mut ranges = Vec::new();
        for drain_rows in drain_ramp(sink.first_drain(chunk_rows), chunk_rows) {
            let mut skeletons = SkeletonKeys::default();
            // `drain_rows` bounds the merge groups visited; a skeleton row counts
            // once however many rows it hydrates to.
            let Some(mut chunk) = source.drain_live_chunk(drain_rows, &mut skeletons) else {
                break;
            };
            if !skeletons.keys.is_empty() {
                chunk = hydrate(self, target_id, hydrator.as_deref_mut(), chunk, skeletons)?;
            }
            filter.ranges(&chunk.as_mem_batch(), &mut ranges);
            if sink.push(&chunk, &mut ranges)? {
                break;
            }
        }
        Ok(Rc::new(sink.finish()))
    }
}

/// `live` merged with the rows of `id` recomputed at `skeletons`' keys.
fn hydrate(
    registry: &RelationRegistry,
    id: u64,
    hydrator: Option<&mut (dyn SkeletonHydrator + '_)>,
    live: Batch,
    mut skeletons: SkeletonKeys,
) -> Result<Batch, String> {
    let Some(hydrator) = hydrator else {
        return Err(format!(
            "relation {id} holds skeleton rows but this process maintains no circuit"
        ));
    };
    let keys = PkKeys::from_sorted(live.schema().pk_stride(), std::mem::take(&mut skeletons.keys));
    #[cfg(debug_assertions)]
    let asked = keys.clone();
    let hydrated = hydrator
        .hydrate_keys(registry, id, keys)
        .map_err(|e| format!("hydrate: view {id}: {e}"))?;
    #[cfg(debug_assertions)]
    assert_hydration_matches(&hydrated, &asked, &skeletons.coarse);
    // Both consolidated and PK-disjoint.
    let schema = *live.schema();
    Ok(match live.is_empty() {
        true => hydrated,
        false => hydrated.merged_consolidated(&live, &schema),
    })
}

/// The reply guard's refusal: a keeper built in any other layout would ship its
/// regions under the client's strides.
pub(super) fn check_layout(reply_layout: u64, produced: &SchemaDescriptor) -> Result<(), String> {
    if reply_layout != produced.layout_digest() {
        return Err("reply schema does not match the output layout".to_string());
    }
    Ok(())
}

/// Tripwire: the replay's per-PK weight sum must equal the coarse weight the
/// skeleton row carried, by linearity of the PK projection. `keys` and `out` are
/// both ascending, so one co-walk checks every key and catches a PK no key named.
#[cfg(debug_assertions)]
fn assert_hydration_matches(out: &Batch, keys: &PkKeys, coarse: &[i64]) {
    use gnitz_zset::repr::pk_group_end;
    use gnitz_zset::schema::key::compare_pk_bytes;

    assert_eq!(keys.len(), coarse.len());
    let mut expected = keys.iter().zip(coarse).peekable();
    let mut i = 0;
    while i < out.len() {
        let pk = out.get_pk_bytes(i);
        if let Some((key, _)) = expected.next_if(|(key, _)| compare_pk_bytes(key, pk).is_lt()) {
            panic!("hydration produced no rows for skeleton key {key:?}");
        }
        let Some((_, &weight)) = expected.next().filter(|(key, _)| *key == pk) else {
            panic!("hydration produced rows for a PK no skeleton row named");
        };
        let j = pk_group_end(out, i);
        let sum = out.as_mem_batch().sum_weights(i, j);
        assert_eq!(sum, weight, "hydration weight mismatch for key {pk:?}");
        i = j;
    }
    assert!(
        expected.next().is_none(),
        "hydration produced no rows for a trailing skeleton key"
    );
}

/// Drain sizes doubling from `first`, each cut at the next multiple of
/// `chunk_rows`, so the drain crosses every boundary a flat `chunk_rows` drain does.
fn drain_ramp(first: usize, chunk_rows: usize) -> impl Iterator<Item = usize> {
    let (mut step, mut drained) = (first, 0usize);
    std::iter::from_fn(move || {
        let rows = step.min(chunk_rows - drained % chunk_rows);
        drained = drained.saturating_add(rows);
        step = step.saturating_mul(2);
        Some(rows)
    })
}

#[cfg(test)]
#[path = "tests/scan_spec.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/scan_spec.rs"]
mod bench;
