//! The sorted key spans of one relation's column list, read off the index on
//! those columns, or off the relation's own PK where those columns lead it.

use crate::relation::RelationRegistry;
use gnitz_trace::runs::ReadCursor;
use gnitz_zset::repr::Batch;

/// The sorted `cols` spans of this process's rows of one relation, a chunk at a time.
pub struct KeySpans {
    chunk_rows: usize,
    /// Rows whose keys lead with the spans, in order: the entries of the index
    /// on exactly `cols`, or the relation's rows where `cols` leads its PK.
    index: ReadCursor,
    /// The current chunk, a span per row's PK region; empty once none are left.
    chunk: Batch,
}

impl RelationRegistry {
    /// Off the index on `cols`; `Err` when the relation has none.
    pub fn key_spans(&self, id: u64, cols: &[u32]) -> Result<KeySpans, String> {
        let relation = self.relation_or_err(id)?;
        let index = relation.index_or_err(cols)?;
        let mut spans = KeySpans {
            chunk_rows: self.config.scan_chunk_rows,
            index: index.span_cursor(relation),
            chunk: Batch::empty_with_schema(&index.key_spec().span_schema()),
        };
        spans.advance();
        Ok(spans)
    }
}

impl KeySpans {
    pub fn chunk(&self) -> &Batch {
        &self.chunk
    }

    /// Move to the next chunk.
    pub fn advance(&mut self) {
        match self.index.drain_chunk(self.chunk_rows) {
            Some(entries) => self.chunk = entries.keyed_by_prefix(self.chunk.schema()),
            None => self.chunk.clear(),
        }
    }
}

#[cfg(test)]
#[path = "tests/key_spans.rs"]
mod tests;
