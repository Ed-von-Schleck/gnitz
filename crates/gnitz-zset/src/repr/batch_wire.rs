//! Wire serialization for `Batch`: [`WireRows`], the row selection that sizes
//! and encodes itself, and WAL-block decoding with the validation it runs.

use std::ops::Range;

use super::batch::Batch;
use super::batch_pool::PooledBuf;
use super::merge::MemBatch;
use super::scatter::{copy_ranges, scatter_copy};
use super::string_heap::{blob_span_key, prorated_blob_cap, walk_heap_spans, BlobCache};
use super::writer::DirectWriter;
use crate::schema::SchemaDescriptor;
use gnitz_wire::wal;
use gnitz_wire::{Regions, TypeCode};

/// One or more rows of a batch as one WAL block: which rows, and the heap
/// bytes the block holds for them. Its size and its bytes are read off the
/// same value.
#[derive(Clone, Copy)]
pub struct WireRows<'a> {
    batch: &'a Batch,
    pick: Pick<'a>,
    heap: usize,
}

#[derive(Clone, Copy)]
enum Pick<'a> {
    /// Every row, over the batch's heap as it stands.
    Whole,
    /// Rows `[start, start + rows)`, their strings relocated.
    Range { start: usize, rows: usize },
    /// The listed rows, their strings relocated: under span dedup when `dedup`,
    /// else each cell on its own.
    Listed { rows: &'a [u32], dedup: bool },
}

impl WireRows<'_> {
    /// Rows the block holds; never zero.
    pub fn rows(&self) -> usize {
        match self.pick {
            Pick::Whole => self.batch.count,
            Pick::Range { rows, .. } => rows,
            Pick::Listed { rows, .. } => rows.len(),
        }
    }

    /// Bytes [`Self::encode`] writes.
    pub fn byte_size(&self) -> usize {
        wal::WAL_HEADER_SIZE + self.rows() * self.batch.schema().row_width() + self.heap
    }

    /// The block at the front of `out`. Returns bytes written.
    pub fn encode(&self, out: &mut [u8]) -> usize {
        let b = self.batch;
        match self.pick {
            Pick::Whole => {
                b.debug_verify_dead_heap();
                wal::write_block(&b.wire_regions(), b.dead_heap, out)
            }
            Pick::Range { start, rows } => b
                .encode_relocating(rows, self.heap, out, true, |writer| {
                    // An empty heap is carried: its cells name no byte of it.
                    let heap_at = b.blob.is_empty().then_some(0);
                    copy_ranges(&b.as_mem_batch(), heap_at, &[(start, start + rows)], writer);
                })
                .expect("a sized range fits the bytes its size reserved"),
            Pick::Listed { rows, dedup } => b
                .encode_listed(rows, self.heap, out, dedup)
                .expect("a sized list fits the bytes its size reserved"),
        }
    }
}

impl Batch {
    // ── Wire serialization (used by the server's SAL and frame codecs) ─────

    /// Every row, over the heap as it stands and under its dead-byte bound;
    /// `None` for an empty batch.
    pub fn wire_whole(&self) -> Option<WireRows<'_>> {
        self.wire_rows_within(0, usize::MAX)
    }

    /// The longest run of rows from `start` whose block fits `budget`: at least
    /// one row, however wide, and `None` once no row remains. The whole batch,
    /// when it fits, goes over its heap as it stands.
    pub fn wire_rows_within(&self, start: usize, budget: usize) -> Option<WireRows<'_>> {
        if start == self.count {
            return None;
        }
        let whole = WireRows {
            batch: self,
            pick: Pick::Whole,
            heap: self.blob.len(),
        };
        if start == 0 && whole.byte_size() <= budget {
            return Some(whole);
        }
        let (rows, heap) = match self.heap_referencing_slots() {
            0 => (self.rows_by_width(start, budget), 0),
            slots => self.rows_with_heap(start..self.count, slots, budget),
        };
        let out = WireRows {
            batch: self,
            pick: Pick::Range { start, rows },
            heap,
        };
        debug_assert!(
            rows <= 1 || out.byte_size() <= budget,
            "a run of {rows} rows encodes past {budget}"
        );
        Some(out)
    }

    /// The rows `indices` lists, in that order; `None` for an empty list. A
    /// list of every row in batch order is the batch itself, over its heap as
    /// it stands. Any other sends each row with its own copy of its long
    /// strings, unless that would send more heap than the batch holds: listed
    /// cells then share spans, and the block keeps each span once.
    pub fn wire_listed<'a>(&'a self, indices: &'a [u32]) -> Option<WireRows<'a>> {
        if indices.is_empty() {
            return None;
        }
        if indices.len() == self.count && indices.iter().zip(0..).all(|(&row, at)| row == at) {
            return self.wire_whole();
        }
        let slots = self.heap_referencing_slots();
        let rows = || indices.iter().map(|&row| row as usize);
        let copied: usize = rows()
            .flat_map(|row| self.long_spans(row, slots))
            .map(|span| span.len())
            .sum();
        let dedup = copied > self.blob.len();
        let heap = match dedup {
            true => self.rows_with_heap(rows(), slots, usize::MAX).1,
            false => copied,
        };
        Some(WireRows {
            batch: self,
            pick: Pick::Listed { rows: indices, dedup },
            heap,
        })
    }

    /// Payload slots whose cells can reference this batch's heap.
    fn heap_referencing_slots(&self) -> u64 {
        if self.blob.is_empty() {
            0
        } else {
            self.schema().string_payload_slots()
        }
    }

    /// The heap spans the long cells of row `row` name, over the string slots
    /// `slots`: the spans a relocation of the row copies.
    fn long_spans(&self, row: usize, slots: u64) -> impl Iterator<Item = Range<usize>> + '_ {
        gnitz_wire::BitIter(slots)
            .filter_map(move |pi| gnitz_wire::german_string_heap(self.get_col_ptr(row, pi, 16), self.blob.len()))
    }

    /// Rows from `start` that fit `budget` by fixed width alone.
    fn rows_by_width(&self, start: usize, budget: usize) -> usize {
        let slope = self.schema().row_width();
        (self.count - start).min((budget.saturating_sub(wal::WAL_HEADER_SIZE) / slope).max(1))
    }

    /// How many of `rows`, from the front, fit `budget` once the heap bytes they
    /// relocate under span dedup are charged, and those heap bytes. `slots`
    /// names the German-string payload slots.
    fn rows_with_heap(&self, rows: impl ExactSizeIterator<Item = usize>, slots: u64, budget: usize) -> (usize, usize) {
        let slope = self.schema().row_width();
        let base = wal::WAL_HEADER_SIZE;
        let mut seen = BlobCache::new(self.string_cells(rows.len()));
        let mut heap = 0usize;
        let mut fit = 0usize;
        for row in rows {
            let row_heap = heap + self.row_heap_cost(row, slots, &mut seen);
            // The first row goes in whatever it costs; a frame carries whole rows.
            if fit > 0 && base + (fit + 1) * slope + row_heap > budget {
                break;
            }
            heap = row_heap;
            fit += 1;
        }
        (fit, heap)
    }

    /// Heap bytes row `row` adds to a chunk whose spans are already in `seen`.
    fn row_heap_cost(&self, row: usize, slots: u64, seen: &mut BlobCache) -> usize {
        let mut cost = 0;
        for span in self.long_spans(row, slots) {
            let len = span.len();
            if seen
                .map()
                .insert(blob_span_key(&self.blob, span.start, len), 0)
                .is_none()
            {
                cost += len;
            }
        }
        cost
    }

    /// Every fixed region followed by the blob heap, in canonical order.
    pub fn wire_regions(&self) -> Regions<'_> {
        let mut out = Regions::new();
        for r in 0..self.schema().num_regions() {
            out.push(self.region_at(r));
        }
        out.push(&self.blob);
        out
    }

    /// The rows `indices` selects, in order, as one WAL block at the front of
    /// `out`, their strings relocated into the block's own heap, for which
    /// `heap_bytes` are reserved: under span dedup when `dedup`, else each cell
    /// on its own. Returns bytes written; `None` when the block does not fit
    /// `out`.
    fn encode_listed(&self, indices: &[u32], heap_bytes: usize, out: &mut [u8], dedup: bool) -> Option<usize> {
        let count = indices.len();
        self.encode_relocating(count, heap_bytes, out, dedup, |writer| {
            scatter_copy(&self.as_mem_batch(), indices, writer);
        })
    }

    /// A WAL block of `count` rows at the front of `out`, which `fill` writes
    /// through a writer over its fixed bytes and a heap reserved at
    /// `heap_bytes`, relocating under span dedup when `dedup`. `None` when the
    /// block does not fit `out`.
    fn encode_relocating(
        &self,
        count: usize,
        heap_bytes: usize,
        out: &mut [u8],
        dedup: bool,
        fill: impl FnOnce(&mut DirectWriter<'_>),
    ) -> Option<usize> {
        let fixed = count * self.schema().row_width();
        let heap_at = wal::WAL_HEADER_SIZE + fixed;
        if heap_at > out.len() {
            return None;
        }
        let mut heap = PooledBuf::with_capacity(heap_bytes);
        let mut cache = BlobCache::new(self.string_cells(count));
        let block = &mut out[wal::WAL_HEADER_SIZE..heap_at];
        let cache = dedup.then_some(&mut cache);
        fill(&mut DirectWriter::over(
            block,
            count,
            0,
            count,
            self.schema(),
            &mut heap,
            cache,
        ));
        let total = heap_at + heap.len();
        out.get_mut(heap_at..total)?.copy_from_slice(&heap);
        // Relocated cell by cell, so every heap byte is referenced.
        Some(wal::write_head(out, count, fixed, heap.len(), 0))
    }

    /// The longest front run of the rows `indices` selects, ascending, that fits
    /// `out` as one block, written there: its row count and bytes. `None` when
    /// not even the first row fits.
    pub fn encode_scattered_prefix(&self, indices: &[u32], out: &mut [u8]) -> Option<(usize, usize)> {
        let width = self.schema().row_width();
        let fixed_fit = out.len().saturating_sub(wal::WAL_HEADER_SIZE) / width;
        let rows = &indices[..indices.len().min(fixed_fit)];
        if rows.is_empty() {
            return None;
        }
        // Every row, ascending, is the batch itself: framed whole, heap as it is.
        if indices.len() == self.count {
            if let Some(whole) = self.wire_whole().filter(|w| w.byte_size() <= out.len()) {
                return Some((self.count, whole.encode(out)));
            }
        }
        let slots = self.heap_referencing_slots();
        // A heap-free block fits by width alone, and the sizing pass is skipped
        // when the source's whole heap fits too.
        if slots == 0 || wal::WAL_HEADER_SIZE + rows.len() * width + self.blob.len() <= out.len() {
            let heap = prorated_blob_cap(self.blob.len(), self.count, rows.len());
            if let Some(len) = self.encode_listed(rows, heap, out, true) {
                return Some((rows.len(), len));
            }
        }
        let (n, heap) = self.rows_with_heap(rows.iter().map(|&i| i as usize), slots, out.len());
        self.encode_listed(&rows[..n], heap, out, true).map(|len| (n, len))
    }

    /// Decode a WAL block the engine wrote into an owned unconsolidated batch. String
    /// cells are copied verbatim with their heap.
    pub fn decode_from_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        Ok(Batch::from_mem_batch(&MemBatch::of_wal_block(data, schema)?))
    }

    /// [`Self::decode_from_wal_block`] for a block a peer wrote, validated
    /// first. The header's dead-heap bound is not trusted: the heap is measured.
    pub fn decode_foreign_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        let mut mb = MemBatch::of_wal_block(data, schema)?;
        mb.dead_heap = validate_string_cells(&mb)?;
        if gnitz_wire::first_not_null_violation(schema.not_null_payload_slots(), mb.null_bmp()).is_some() {
            return Err("a null bit on a NOT NULL column");
        }
        if first_valued_null_cell(&mb).is_some() {
            return Err("a non-zero cell under a NULL");
        }
        Ok(Batch::from_mem_batch(&mb))
    }
}

/// The first `(row, payload slot)` of `mb` whose NULL cell is not zeroed: a key
/// reader packs or routes a cell whatever its null bit says, so a second
/// encoding would key a row and its retraction apart.
pub(super) fn first_valued_null_cell(mb: &MemBatch<'_>) -> Option<(usize, usize)> {
    let schema = mb.schema;
    let col = |pi| {
        let width = schema.columns[schema.payload_col_idx(pi)].size() as usize;
        (mb.col_data(pi, width), width)
    };
    gnitz_wire::first_valued_null(schema.nullable_payload_slots(), mb.null_bmp(), col)
}

/// Every German-string cell of `mb` is in canonical form against its own heap,
/// and every STRING cell's content is UTF-8. Answers the heap's exact count of
/// bytes no cell references.
fn validate_string_cells(mb: &MemBatch<'_>) -> Result<usize, &'static str> {
    // Non-ASCII STRING contents, each closed by an ASCII byte so no character
    // runs from one into the next.
    let mut text = Vec::new();
    let dead = walk_heap_spans(mb, |cell, tc| {
        if !gnitz_wire::german_string_cell_ok(cell, mb.blob) {
            return false;
        }
        if tc == TypeCode::String && !gnitz_wire::german_string_short_ascii(cell) {
            let c = gnitz_wire::german_string_content(cell, mb.blob);
            if !c.is_ascii() {
                text.extend_from_slice(c);
                text.push(0);
            }
        }
        true
    })
    .ok_or("data WAL German string is not in canonical form")?;
    simdutf8::basic::from_utf8(&text).map_err(|_| "a STRING cell is not UTF-8")?;
    Ok(dead)
}

impl<'a> MemBatch<'a> {
    /// The rows of the WAL block `data` under `schema`: its fixed bytes are an
    /// arena of exactly its row count.
    ///
    /// Its string cells and dead-heap bound are taken as the engine wrote
    /// them; a block a peer wrote goes through
    /// [`Batch::decode_foreign_wal_block`].
    pub fn of_wal_block(data: &'a [u8], schema: &'a SchemaDescriptor) -> Result<Self, &'static str> {
        let (count, fixed, blob, dead_heap) = wal::parse_block(data, schema.row_width())?;
        Ok(MemBatch {
            data: fixed,
            schema,
            cap: count,
            blob,
            count,
            dead_heap,
        })
    }
}

#[cfg(test)]
#[path = "tests/batch_wire.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/batch_wire.rs"]
mod bench;
