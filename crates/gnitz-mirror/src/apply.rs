//! Applying a delta to a copy.

use gnitz_core::{MirrorError, RawBlock, Shape};
use gnitz_store::foundation::fault::Seam;
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
    /// Decode each block under the schema `shape` names — both come from the
    /// registration — and ingest it.
    ///
    /// Two engine entries here have a near twin that is silently wrong for a
    /// socket frame, so each is named for what it keeps: the **public** decode
    /// keeps the long-string extent check the ring-internal one drops, and the
    /// `Layout::Raw` it returns keeps the ingest's sort-and-fold, where a
    /// sender's `Consolidated` claim would fold weights onto the wrong element
    /// past a sortedness check that is debug-only.
    pub(crate) fn ingest_blocks(
        &mut self,
        table_id: u64,
        blocks: Vec<RawBlock>,
        shape: Shape,
    ) -> Result<(), MirrorError> {
        if shape == Shape::Stamped && PANIC_SEAM.take_once() {
            panic!("injected mirror ingest panic");
        }
        let shapes = self.shapes(table_id)?;
        let (view_desc, delta_desc) = (shapes.view_desc, shapes.delta_desc);
        let in_desc = match shape {
            Shape::Plain => view_desc,
            Shape::Stamped => delta_desc,
        };
        let mut applied = 0usize;
        for raw in blocks {
            let block = raw.block();
            applied += block.len();
            let (batch, _) = Batch::decode_from_wal_block(block, &in_desc, false)
                .map_err(|e| self.poison(format!("decoding a delta for {table_id} failed: {e}")))?;
            let batch = match shape {
                Shape::Plain => batch,
                Shape::Stamped => batch.stripped_of_pk_prefix(&in_desc, &view_desc),
            };
            if batch.is_empty() {
                continue;
            }
            self.registry
                .ingest_returning_effective(table_id as i64, batch, false)
                .map(drop)
                .map_err(|e| self.poison(format!("applying a delta to {table_id} failed: {e}")))?;
        }
        self.note_applied(applied);
        Ok(())
    }
}
