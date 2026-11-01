//! The worker half of a delta read: the rounds of a fed view's feed past a
//! cursor, under the `ReadSpec` the subscriber reads the view by.

use gnitz_expr::RowFilter;
use gnitz_wire::{ReadBound, ReadSpec, SinkKind, WireFault, WireStatus};

use std::rc::Rc;

use super::{check_layout, check_stride, ReadRows};
use crate::relation::RelationRegistry;
use gnitz_zset::algebra::SinkPlan;
use gnitz_zset::repr::Batch;

impl RelationRegistry {
    /// `spec` applied to every delta `id`'s feed recorded in the rounds after
    /// `after_tick`; `after_tick = 0` reads the view's own output store
    /// instead. A cursor below the floor `Feed::rounds` applies is refused as
    /// [`WireStatus::DeltaExpired`]; every other refusal is `Error`.
    pub fn delta_read(
        &self,
        id: u64,
        after_tick: u64,
        spec: ReadSpec,
        reply_layout: u64,
    ) -> Result<ReadRows, WireFault> {
        let entry = self.relation_or_err(id)?;
        let feed = entry
            .feed()
            .ok_or_else(|| format!("delta_read: this process holds no delta feed for relation {id}"))?;
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
        let ReadSpec { bound, predicate, sink } = spec;
        let mut plan =
            SinkPlan::from_wire(&view, &sink, self.config.adhoc_group_cap).map_err(|e| format!("delta_read: {e}"))?;
        // Before the floor: a layout no cursor could have been handed is a bad
        // request, not an expired one.
        check_layout(reply_layout, plan.output_schema())?;
        let Some(mut rounds) = feed.rounds(after_tick) else {
            return Err(WireFault {
                status: WireStatus::DeltaExpired,
                text: format!(
                    "delta cursor {after_tick} of relation {id} is below the \
                     retained floor {}; re-read at 0",
                    feed.dropped_through()
                ),
            });
        };
        if whole {
            return Ok(match rounds.len() {
                // One round is the reply as it was captured.
                1 => ReadRows::Shared(Rc::clone(rounds.next().unwrap())),
                _ => ReadRows::Own(Batch::concat(&view, rounds.map(|round| round.as_mem_batch()))),
            });
        }
        let mut filter =
            RowFilter::for_read(&predicate, bound.range(), &view).map_err(|e| format!("delta_read: {e}"))?;
        let chunk_rows = self.config.scan_chunk_rows;
        let mut read = |rows: &Batch| plan.push(rows, &mut filter).map(drop);
        if let ReadBound::PkSet(keys) = &bound {
            check_stride("delta_read", id, keys, view.pk_stride())?;
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
        Ok(ReadRows::Own(match sink.map {
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
