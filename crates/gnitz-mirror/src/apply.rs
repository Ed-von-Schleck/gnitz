//! Applying a delta to a copy.

use gnitz_core::{MirrorError, RawBlock};
use gnitz_store::schema::{make_delta_schema, SchemaDescriptor};
use gnitz_store::storage::Batch;

use crate::handle::Mirror;

impl Mirror {
    /// Decode `blocks` and apply them: round-stamped rows when `stamped`, the
    /// view's own otherwise. Returns the bytes applied.
    ///
    /// A failure erases this copy, whose half-applied rows sit under a cursor that
    /// never advanced.
    pub(crate) fn ingest_blocks(
        &mut self,
        table_id: u64,
        blocks: Vec<RawBlock>,
        stamped: bool,
        view_desc: SchemaDescriptor,
    ) -> Result<usize, MirrorError> {
        let in_desc = match stamped {
            false => view_desc,
            true => make_delta_schema(&view_desc).expect("the server derived this same shape to open the feed"),
        };
        let mut applied = 0usize;
        for raw in blocks {
            let block = raw.block();
            applied += block.len();
            let batch = match Batch::decode_foreign_wal_block(block, &in_desc) {
                Ok(b) => b,
                Err(e) => return Err(self.erase_copy(table_id, format!("decoding a delta for {table_id}: {e}"))),
            };
            let batch = match stamped {
                false => batch,
                true => batch.stripped_of_pk_prefix(&view_desc),
            };
            if batch.is_empty() {
                continue;
            }
            if let Err(e) = self.registry.ingest(table_id as i64, batch) {
                return Err(self.erase_copy(table_id, format!("applying a delta to {table_id}: {e}")));
            }
        }
        Ok(applied)
    }
}
