//! The worker half of a delta read: the rounds of a fed view's feed past a
//! cursor, under the `ReadSpec` the subscriber reads the view by.

use gnitz_expr::RowFilter;
use gnitz_wire::{PkKeys, ReadBound, ReadSpec, SinkKind, WireFault, WireStatus};

use std::rc::Rc;

use super::scan_spec::check_layout;
use crate::relation::{delta_round, delta_round_prefix, Cut, RelationRegistry};
use gnitz_zset::algebra::SinkPlan;
use gnitz_zset::repr::{Batch, SourceCursor};
use gnitz_zset::schema::key::{key_range_between_cuts, KeyCut};

impl RelationRegistry {
    /// `spec` applied to every delta `id`'s feed recorded in rounds
    /// `(after_tick, cut_tick]`, which are the deltas of `spec` applied to the
    /// view: a filter and a map are linear. `after_tick = 0` reads the view's
    /// own output store instead. A cursor below the retained floor is refused as
    /// [`WireStatus::DeltaExpired`]; every other refusal is `Error`.
    pub fn delta_read(
        &self,
        id: u64,
        after_tick: u64,
        cut_tick: u64,
        spec: ReadSpec,
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
        if !matches!(spec.sink.kind, SinkKind::Rows { cut: None }) {
            return Err("delta_read: a delta read forwards rows, uncut and unfolded"
                .to_string()
                .into());
        }
        if after_tick == 0 {
            return Ok(self.scan_spec(id, spec, reply_layout, None)?);
        }
        let view = entry.schema();
        let whole = spec.is_whole();
        // Before the floor: a layout no cursor could have been handed is a bad
        // request, not an expired one.
        let reply = match whole {
            true => view,
            false => *SinkPlan::from_wire(&view, &spec.sink, self.config.adhoc_group_cap)?.output_schema(),
        };
        check_layout(reply_layout, &reply)?;
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
        if whole {
            let rows = feed.range_cursor(band, Cut::Now).materialize();
            return Ok(Rc::new(rows.without_key_prefix(&view)));
        }

        let ReadSpec { bound, predicate, sink } = spec;
        // Few enough keys are gathered round by round; the rest are picked out
        // of a walk of the band.
        let probes = match &bound {
            ReadBound::PkSet(keys) if keys.stride() != view.pk_stride() => {
                return Err(format!(
                    "delta_read: PkSet key stride {} != pk_stride {} (relation {id})",
                    keys.stride(),
                    view.pk_stride()
                )
                .into());
            }
            ReadBound::PkSet(keys) => {
                let rounds = cut_tick.saturating_sub(after_tick);
                (rounds.saturating_mul(keys.len() as u64) <= DELTA_GATHER_MAX_PROBES)
                    .then(|| stamped_keys(after_tick + 1..=cut_tick, keys))
            }
            _ => None,
        };
        let (mut source, unapplied) = match probes {
            Some(probes) => (
                SourceCursor::PkSet(Box::new(feed.gather(probes, Cut::Now))),
                ReadBound::None,
            ),
            None => (SourceCursor::Full(Box::new(feed.range_cursor(band, Cut::Now))), bound),
        };
        // The feed numbers the view's columns as the view does, so the spec's
        // programs run on its chunks as they are and only survivors are copied.
        let stamped = *feed.schema();
        let mut filter =
            RowFilter::for_read(&predicate, &unapplied, &stamped).map_err(|e| format!("delta_read: {e}"))?;
        let mut plan = SinkPlan::from_wire(&stamped, &sink, self.config.adhoc_group_cap)?;
        let mut ranges = Vec::new();
        while let Some(chunk) = source.drain_chunk(self.config.scan_chunk_rows) {
            filter.ranges(&chunk.as_mem_batch(), &mut ranges);
            // The one bound the filter does not apply.
            if let ReadBound::PkSet(keys) = &unapplied {
                keep_keyed(&chunk, keys, &mut ranges);
            }
            plan.push(&chunk, &mut ranges)?;
        }
        let rows = plan.finish().without_key_prefix(&reply);
        // A map can send two rows to one, and a retraction and an insert that
        // differ only in a column it drops to nothing.
        Ok(Rc::new(match sink.map {
            Some(_) => rows.into_consolidated(),
            None => rows,
        }))
    }
}

/// The most `(round, key)` probes a keyed delta read gathers before it walks the
/// band instead. A probe costs the same whatever the band holds and a walk costs
/// per band row, so the cap is what bounds a read whose cursor lags many rounds.
const DELTA_GATHER_MAX_PROBES: u64 = 4096;

/// `keys` under each round of `rounds`, as keys of the delta store.
fn stamped_keys(rounds: std::ops::RangeInclusive<u64>, keys: &PkKeys) -> PkKeys {
    let stride = keys.stride() + delta_round_prefix(0).len();
    let mut bytes = Vec::with_capacity(keys.len() * stride * rounds.clone().count());
    for round in rounds {
        for key in keys.iter() {
            bytes.extend_from_slice(&delta_round_prefix(round));
            bytes.extend_from_slice(key);
        }
    }
    PkKeys::from_sorted(stride, bytes)
}

/// Cut `ranges`, row ranges of the delta-store chunk `chunk`, down to the rows
/// whose view key is one of `keys`.
fn keep_keyed(chunk: &Batch, keys: &PkKeys, ranges: &mut Vec<(usize, usize)>) {
    let stamp = chunk.schema().pk_stride() - keys.stride();
    let mut kept = Vec::with_capacity(ranges.len());
    for &(start, end) in ranges.iter() {
        let mut run = None;
        for row in start..end {
            match (keys.contains(&chunk.get_pk_bytes(row)[stamp..]), run) {
                (true, None) => run = Some(row),
                (false, Some(first)) => {
                    kept.push((first, row));
                    run = None;
                }
                _ => {}
            }
        }
        if let Some(first) = run {
            kept.push((first, end));
        }
    }
    *ranges = kept;
}

#[cfg(test)]
#[path = "tests/delta_read.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/delta_read.rs"]
mod bench;
