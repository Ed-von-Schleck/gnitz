//! Applying a delta to a copy.

use gnitz_core::{MirrorError, RawBlock, Shape};
use gnitz_foundation::fault::Seam;
use gnitz_store::relation::Relation;
use gnitz_store::schema::make_delta_schema;
use gnitz_store::storage::Batch;

use crate::handle::Mirror;

/// `GNITZ_INJECT_MIRROR_INGEST_PANIC`: panic once inside the region
/// [`Mirror::touching`] guards. Debug-only, and the one seam that injects a
/// *panic* rather than an `Err`, because nothing but a panic reaches that guard's
/// arm.
///
/// It fires on a **poll's** apply and not a bootstrap's: only that leaves the
/// state worth testing, a copy still valid and answerable beside a poisoned
/// store. A bootstrap's would leave no cursor, so the read it produced would be
/// delegated rather than refused.
static PANIC_SEAM: Seam = Seam::new("GNITZ_INJECT_MIRROR_INGEST_PANIC");

impl Mirror {
    /// Decode each block under the schema `shape` names — the copy's own, or the
    /// delta-store shape derived from it — and ingest it. Returns the bytes
    /// applied.
    pub(crate) fn ingest_blocks(
        &mut self,
        table_id: u64,
        blocks: Vec<RawBlock>,
        shape: Shape,
    ) -> Result<usize, MirrorError> {
        if shape == Shape::Stamped && PANIC_SEAM.take_once() {
            panic!("injected mirror ingest panic");
        }
        let view_desc = self
            .registry
            .relation(table_id as i64)
            .map(Relation::schema)
            .ok_or_else(|| MirrorError::Engine(format!("relation {table_id} is not mirrored")))?;
        let in_desc = match shape {
            Shape::Plain => view_desc,
            // The server derived this same shape, with this same builder, to
            // open the feed this copy is fed by: it refuses `WITH (delta)` on a
            // shape that overruns, live and on replay.
            Shape::Stamped => {
                make_delta_schema(&view_desc).expect("the server derived this same shape to open the feed")
            }
        };
        let mut applied = 0usize;
        for raw in blocks {
            let block = raw.block();
            applied += block.len();
            let batch = Batch::decode_foreign_wal_block(block, &in_desc)
                .map_err(|e| self.poison(format!("decoding a delta for {table_id} failed: {e}")))?;
            let batch = match shape {
                Shape::Plain => batch,
                Shape::Stamped => batch.stripped_of_pk_prefix(&view_desc),
            };
            if batch.is_empty() {
                continue;
            }
            self.registry
                .ingest(table_id as i64, batch)
                .map_err(|e| self.poison(format!("applying a delta to {table_id} failed: {e}")))?;
        }
        Ok(applied)
    }
}
