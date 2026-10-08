//! The worker half of a delta read: the rounds of a fed view's feed past a
//! cursor, under the `ReadSpec` the subscriber reads the view by.

use gnitz_expr::RowFilter;
use gnitz_wire::{ReadBound, ReadSpec, SinkKind, WireFault, WireStatus};

use std::rc::Rc;

use super::scan_spec::check_layout;
use crate::relation::RelationRegistry;
use gnitz_zset::algebra::SinkPlan;
use gnitz_zset::repr::Batch;

impl RelationRegistry {
    /// `spec` applied to every delta `id`'s feed recorded in the rounds after
    /// `after_tick`; `after_tick = 0` reads the view's own output store
    /// instead. A cursor below the retained floor is refused as
    /// [`WireStatus::DeltaExpired`]; every other refusal is `Error`.
    pub fn delta_read(
        &self,
        id: u64,
        after_tick: u64,
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
        let feed = entry
            .feed()
            .ok_or_else(|| format!("delta_read: this process holds no delta feed for relation {id}"))?;
        let whole = spec.is_whole();
        let ReadSpec { bound, predicate, sink } = spec;
        let mut plan = SinkPlan::from_wire(&view, &sink, self.config.adhoc_group_cap)?;
        // Before the floor: a layout no cursor could have been handed is a bad
        // request, not an expired one.
        check_layout(reply_layout, plan.output_schema())?;
        let dropped_through = feed.dropped_through();
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
        let rounds = feed.rounds(after_tick);
        if whole {
            let mut held = rounds.clone();
            return Ok(match (held.next(), held.next()) {
                // One round is the reply as it was captured.
                (Some(only), None) => Rc::clone(only),
                _ => Rc::new(Batch::concat(&view, rounds.map(|round| round.as_mem_batch()))),
            });
        }
        let walk = match &bound {
            ReadBound::Range(r) => Some(r),
            ReadBound::None | ReadBound::PkSet(_) => None,
        };
        let mut filter = RowFilter::for_read(&predicate, walk, &view).map_err(|e| format!("delta_read: {e}"))?;
        let chunk_rows = self.config.scan_chunk_rows;
        let mut ranges = Vec::new();
        let mut read = |rows: &Batch| {
            filter.ranges(&rows.as_mem_batch(), &mut ranges);
            plan.push(rows, &mut ranges).map(drop)
        };
        if let ReadBound::PkSet(keys) = &bound {
            if keys.stride() != view.pk_stride() {
                return Err(format!(
                    "delta_read: PkSet key stride {} != pk_stride {} (relation {id})",
                    keys.stride(),
                    view.pk_stride()
                )
                .into());
            }
            // The keyed rows of every round, read a chunk at a time.
            let mut keyed = Vec::new();
            let mut chunk = Batch::empty_with_schema(&view);
            for round in rounds {
                round.key_ranges(keys.iter(), &mut keyed);
                chunk.append_ranges(&round.as_mem_batch(), &keyed);
                if chunk.len() >= chunk_rows {
                    read(&chunk)?;
                    chunk.clear();
                }
            }
            if !chunk.is_empty() {
                read(&chunk)?;
            }
        } else {
            // The spec runs on each round where it lies; a run of small rounds
            // is read as one chunk.
            let rounds: Vec<&Rc<Batch>> = rounds.collect();
            let small = |round: &Rc<Batch>| round.len() < CHUNKED_BELOW_ROWS;
            let mut i = 0;
            while i < rounds.len() {
                let (mut j, mut rows) = (i + 1, rounds[i].len());
                while small(rounds[i]) && j < rounds.len() && small(rounds[j]) && rows + rounds[j].len() <= chunk_rows {
                    rows += rounds[j].len();
                    j += 1;
                }
                match &rounds[i..j] {
                    [round] => read(round)?,
                    chunk => read(&Batch::concat(&view, chunk.iter().map(|round| round.as_mem_batch())))?,
                }
                i = j;
            }
        }
        let rows = plan.finish();
        // A map can send two rows to one, and a retraction and an insert that
        // differ only in a column it drops to nothing.
        Ok(Rc::new(match sink.map {
            Some(_) => rows.into_consolidated(),
            None => rows,
        }))
    }
}

/// The rows below which a round is read in a chunk with the small rounds beside
/// it: under them, copying it costs less than a filter and a sink set up for it
/// alone.
const CHUNKED_BELOW_ROWS: usize = 128;

#[cfg(test)]
#[path = "tests/delta_read.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/delta_read.rs"]
mod bench;
