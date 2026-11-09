//! The worker half of an ad-hoc `ReadSpec` read, over this worker's slice: open
//! the bound, filter, map, then forward rows or fold them. A capacity-bounded
//! view's rows hydrate chunk by chunk as the sink drains them.
use gnitz_wire::{PkKeys, ReadSink, ReadSpec, SinkKind};

use std::borrow::Cow;

use super::{check_layout, ReadRows};
use crate::relation::RelationRegistry;
use gnitz_expr::{LogicalProgram, RowFilter};
use gnitz_zset::algebra::SinkPlan;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;

impl RelationRegistry {
    /// Execute `spec` on this worker's slice, replying in the layout whose
    /// [`SchemaDescriptor::layout_digest`](gnitz_zset::schema::SchemaDescriptor::layout_digest)
    /// is `reply_layout`.
    ///
    /// `hydrate` recomputes a capacity-bounded view's rows at its skeleton keys,
    /// from the view's own maintained state: every row of the view at `keys` — at
    /// the view's `pk_stride`, each held by the view's store as a skeleton row —
    /// consolidated, each key's weights summing to its skeleton row's weight. A
    /// host that maintains no circuit passes `None`.
    pub fn scan_spec(
        &self,
        target_id: u64,
        spec: ReadSpec,
        reply_layout: u64,
        mut hydrate: Option<&mut dyn FnMut(PkKeys) -> Result<Batch, String>>,
    ) -> Result<ReadRows, String> {
        let entry = self.relation_or_err(target_id)?;
        let table = entry.table_or_err()?;
        let src_schema = entry.schema();
        let whole = spec.is_whole();
        let ReadSpec { bound, predicate, sink: wire_sink } = spec;
        // A bad sink, predicate or walk is a corrupt request: the client
        // pre-compiled the identical program at plan time.
        let mut sink = SinkPlan::from_wire(&src_schema, &wire_sink, self.config.adhoc_group_cap)
            .map_err(|e| format!("scan_spec: {e}"))?;
        check_layout(reply_layout, sink.output_schema())?;
        // Nothing to hydrate either: the relation whole, off the store's cached
        // snapshot.
        if whole && !table.has_skeleton_rows() {
            return Ok(ReadRows::Shared(entry.full_scan()));
        }
        let (mut source, walk) = entry.open_read(bound, payload_read(&src_schema, &predicate, &wire_sink))?;
        let mut filter =
            RowFilter::for_read(&predicate, walk.as_ref(), &src_schema).map_err(|e| format!("scan_spec: {e}"))?;
        // Chunks bound a read's peak memory only where the sink drops, cuts or
        // maps the rows, or where they hydrate; a read that keeps every row as it
        // is drains its source once.
        let chunk_rows = if sink.forwards_every_row() && filter.keeps_all() && !table.has_skeleton_rows() {
            usize::MAX
        } else {
            self.config.scan_chunk_rows
        };
        let mut hydrate_chunk = |keys| match hydrate.as_deref_mut() {
            Some(h) => h(keys).map_err(|e| format!("hydrate: view {target_id}: {e}")),
            None => Err(format!(
                "relation {target_id} holds skeleton rows but this process maintains no circuit"
            )),
        };
        // The drain leaves out the rows `filter` drops that it can, but for a
        // sink that folds them in place, which the copy would only slow. A chunk
        // whose every row the drain's `filter` decided reaches the sink unfiltered.
        let mut keep_all = RowFilter::default();
        let folds = sink.folds();
        for drain_rows in drain_ramp(sink.first_drain(), chunk_rows) {
            let drain_filter = if folds { &mut keep_all } else { &mut filter };
            let Some((chunk, decided)) = source.drain_hydrated(drain_rows, drain_filter, &mut hydrate_chunk)? else {
                break;
            };
            let push_filter = if decided && !folds { &mut keep_all } else { &mut filter };
            if sink.push(Cow::Owned(chunk), push_filter)? {
                break;
            }
        }
        Ok(ReadRows::Own(sink.finish()))
    }
}

/// The payload columns of `schema`, as payload bits, that `predicate` or
/// `sink` reads. A sink that takes the rows unmapped reads what it folds, or
/// every column of the rows it forwards; a mapped one reads what its map does.
fn payload_read(schema: &SchemaDescriptor, predicate: &[u8], sink: &ReadSink) -> u64 {
    let sink_read = match (&sink.map, &sink.kind) {
        (Some(map), _) => LogicalProgram::payload_read(&map.program, schema),
        (None, SinkKind::Fold(fold)) => schema.payload_bits(fold.cols()),
        (None, SinkKind::Rows { .. }) => return schema.payload_slots(),
    };
    match predicate.is_empty() {
        true => sink_read,
        false => sink_read | LogicalProgram::payload_read(predicate, schema),
    }
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
