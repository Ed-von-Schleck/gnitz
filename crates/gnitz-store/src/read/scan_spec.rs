//! The worker half of an ad-hoc `ReadSpec` read, over this worker's slice: open
//! the bound, filter, map, then forward rows or fold them. A capacity-bounded
//! view's rows hydrate chunk by chunk as the sink drains them.

use gnitz_expr::SchemaFacts;
use gnitz_wire::{PkKeys, ReadBound, ReadSpec, SinkKind, WireFault, WireStatus};

use std::rc::Rc;

use super::SkeletonHydrator;
use crate::relation::RelationRegistry;
use crate::relation::{delta_round, delta_round_prefix};
use gnitz_expr::RowFilter;
use gnitz_zset::algebra::SinkPlan;
use gnitz_zset::repr::{Batch, SkeletonKeys, SourceCursor};
use gnitz_zset::schema::key::{key_range_between_cuts, KeyCut};
use gnitz_zset::schema::SchemaDescriptor;

impl RelationRegistry {
    /// Execute `spec` on this worker's slice, replying in the layout whose
    /// [`SchemaFacts::layout_digest`] is `reply_layout`.
    pub fn scan_spec(
        &self,
        target_id: u64,
        spec: ReadSpec,
        reply_layout: u64,
        hydrator: Option<&mut dyn SkeletonHydrator>,
    ) -> Result<Rc<Batch>, String> {
        let ReadSpec { bound, predicate, sink } = spec;
        let entry = self.relation_or_err(target_id)?;
        let src_schema = entry.schema();
        // Nothing bounded, filtered, mapped or cut, and nothing to hydrate: the
        // relation whole, off the store's cached snapshot.
        if let (ReadBound::None, true, None, SinkKind::Rows { cut: None }) =
            (&bound, predicate.is_empty(), &sink.map, &sink.kind)
        {
            if !entry.table().has_skeleton_rows() {
                check_layout(reply_layout, &src_schema)?;
                return Ok(entry.full_scan());
            }
        }
        let (source, unapplied) = self.open_bound(target_id, bound)?;
        // A bad predicate and a bad walk are both a corrupt request: the client
        // pre-compiled the identical program at plan time.
        let filter = RowFilter::for_read(&predicate, &unapplied, &src_schema).map_err(|e| format!("scan_spec: {e}"))?;
        let mut sink = SinkPlan::from_wire(&src_schema, &sink, self.config.adhoc_group_cap)?;
        check_layout(reply_layout, sink.output_schema())?;
        let mut rows = Survivors {
            registry: self,
            id: target_id,
            source,
            hydrator,
            filter,
            ranges: Vec::new(),
        };
        let chunk_rows = self.config.scan_chunk_rows;
        for drain_rows in drain_ramp(sink.first_drain(chunk_rows), chunk_rows) {
            let Some((chunk, ranges)) = rows.next(drain_rows)? else {
                break;
            };
            if sink.push(&chunk, ranges)? {
                break;
            }
        }
        Ok(Rc::new(sink.finish()))
    }

    /// Every delta `id`'s feed recorded in rounds `(after_tick, cut_tick]`, in the
    /// view's schema whatever `after_tick`; `0` walks the view's own output store.
    /// A cursor below the retained floor is refused as [`WireStatus::DeltaExpired`];
    /// every other refusal is `Error`.
    pub fn delta_read(
        &self,
        id: u64,
        after_tick: u64,
        cut_tick: u64,
        reply_layout: u64,
    ) -> Result<Rc<Batch>, WireFault> {
        let entry = self.relation_or_err(id)?;
        if !entry.kind().has_delta_feed() {
            return Err(format!(
                "delta_read: relation {id} carries no delta feed; \
                 create the view WITH (delta = '<size>') to subscribe to it"
            )
            .into());
        }
        let view = entry.schema();
        check_layout(reply_layout, &view)?;
        if after_tick == 0 {
            return Ok(entry.full_scan());
        }
        let feed = entry
            .delta()
            .ok_or_else(|| format!("delta_read: this process holds no delta store for relation {id}"))?;
        let dropped_through = delta_round(feed.dropped_max().pk_bytes());
        // A cursor at the floor has lost nothing.
        if after_tick < dropped_through {
            return Err(WireFault {
                status: WireStatus::DeltaExpired,
                text: format!(
                    "delta cursor {after_tick} of relation {id} is below the \
                     retained floor {dropped_through}; re-read at 0"
                ),
            });
        }
        let band = key_range_between_cuts(
            KeyCut::above(&delta_round_prefix(after_tick)),
            KeyCut::above(&delta_round_prefix(cut_tick)),
            feed.schema().pk_stride(),
        );
        let rows = feed.range_cursor(band).materialize();
        Ok(Rc::new(rows.without_key_prefix(&view)))
    }
}

/// The reply guard's refusal: a keeper built in any other layout would ship its
/// regions under the client's strides.
fn check_layout(reply_layout: u64, produced: &SchemaDescriptor) -> Result<(), String> {
    if reply_layout != produced.layout_digest() {
        return Err("reply schema does not match the output layout".to_string());
    }
    Ok(())
}

/// A source chunk and its surviving row ranges.
type SurvivorChunk<'r> = (Batch, &'r mut Vec<(usize, usize)>);

/// The rows surviving the bound and the predicate, one source chunk at a time —
/// the one input every sink reads. Each skeleton row a chunk meets is replaced by
/// that key's rows, recomputed through `hydrator`.
struct Survivors<'a, 'h> {
    registry: &'a RelationRegistry,
    id: u64,
    source: SourceCursor,
    hydrator: Option<&'h mut dyn SkeletonHydrator>,
    /// The predicate, and the part of the bound the source did not apply.
    filter: RowFilter,
    /// The current chunk's surviving row ranges; scratch reused across chunks.
    ranges: Vec<(usize, usize)>,
}

impl Survivors<'_, '_> {
    /// The next source chunk and its surviving row ranges; `None` once the source is
    /// exhausted. `max_rows` bounds the merge groups visited; a skeleton row counts
    /// once however many rows it hydrates to.
    fn next(&mut self, max_rows: usize) -> Result<Option<SurvivorChunk<'_>>, String> {
        let mut skeletons = SkeletonKeys::default();
        let Some(mut chunk) = self.source.drain_live_chunk(max_rows, &mut skeletons) else {
            return Ok(None);
        };
        if !skeletons.keys.is_empty() {
            chunk = self.hydrate(chunk, skeletons)?;
        }
        self.filter.ranges(&chunk.as_mem_batch(), &mut self.ranges);
        Ok(Some((chunk, &mut self.ranges)))
    }

    /// `live` merged with the rows recomputed at `skeletons`' keys.
    fn hydrate(&mut self, live: Batch, mut skeletons: SkeletonKeys) -> Result<Batch, String> {
        let Some(hydrator) = self.hydrator.as_deref_mut() else {
            return Err(format!(
                "relation {} holds skeleton rows but this process maintains no circuit",
                self.id
            ));
        };
        let keys = PkKeys::from_sorted(live.schema().pk_stride(), std::mem::take(&mut skeletons.keys));
        #[cfg(debug_assertions)]
        let asked = keys.clone();
        let hydrated = hydrator
            .hydrate_keys(self.registry, self.id, keys)
            .map_err(|e| format!("hydrate: view {}: {e}", self.id))?;
        #[cfg(debug_assertions)]
        assert_hydration_matches(&hydrated, &asked, &skeletons.coarse);
        // Both consolidated and PK-disjoint.
        let schema = *live.schema();
        Ok(match live.is_empty() {
            true => hydrated,
            false => hydrated.merged_consolidated(&live, &schema),
        })
    }
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
        drained += rows;
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
