//! The sorted key spans of one relation's column list: off the index on those
//! columns where one exists, else from a spill sort of the relation's rows.

use crate::relation::{relation_dir, RelationRegistry};
use gnitz_zset::algebra::append_spans;
use gnitz_zset::repr::{Batch, KeyProducer, ReadCursor, SpillSort};
use gnitz_zset::schema::KeySpec;

/// The sorted `cols` spans of this process's rows of one relation, a chunk at a time.
pub struct KeySpans {
    chunk_rows: usize,
    source: Source,
    /// The current chunk, a span per row's PK region; empty once none are left.
    chunk: Batch,
}

enum Source {
    /// The index on exactly `cols`: its entries are the spans, in order.
    Index(Box<ReadCursor>),
    Sorted(KeyProducer),
}

impl RelationRegistry {
    /// Off the index on `cols` where one exists, else from a spill sort of the
    /// relation's rows, which runs here.
    pub fn key_spans(&self, id: u64, cols: &[u32]) -> Result<KeySpans, String> {
        let relation = self.relation_or_err(id)?;
        let spec = KeySpec::new(cols, &relation.schema())?;
        let chunk_rows = self.config.scan_chunk_rows;
        let source = match relation.index_on(cols) {
            Some(index) => Source::Index(Box::new(index.cursor())),
            None => {
                let dir = relation_dir(&self.base_dir, id);
                let mut sort = SpillSort::new(&dir, spec.key_size(), self.config.key_spans_spill_bytes);
                let mut rows = relation.cursor();
                let mut spans = Vec::new();
                // A drained chunk is consolidated, every row at weight 1.
                while let Some(chunk) = rows.drain_chunk(chunk_rows) {
                    spans.clear();
                    append_spans(&mut spans, sort.slot(), &chunk.as_mem_batch(), &spec, |_| true);
                    sort.push(&spans)?;
                }
                Source::Sorted(sort.finish()?)
            }
        };
        let mut spans = KeySpans {
            chunk_rows,
            source,
            chunk: Batch::empty_with_schema(&spec.span_schema()),
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
        match &mut self.source {
            Source::Index(cursor) => match cursor.drain_chunk(self.chunk_rows) {
                Some(entries) => self.chunk = entries.keyed_by_prefix(self.chunk.schema()),
                None => self.chunk.clear(),
            },
            Source::Sorted(spans) => spans.fill(&mut self.chunk, self.chunk_rows),
        }
    }
}

#[cfg(test)]
#[path = "tests/key_spans.rs"]
mod tests;
