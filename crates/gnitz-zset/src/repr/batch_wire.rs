//! Wire serialization for `Batch`: [`WireRows`], the row selection that sizes
//! and encodes itself, and WAL-block decoding with the validation it runs.

use super::batch::{row_width, strides_from_schema, Batch, MAX_BATCH_REGIONS, REG_PAYLOAD_START, REG_PK};
use super::batch_pool::PooledBuf;
use super::merge::{DirectWriter, MemBatch};
use super::string_heap::{blob_span_key, copy_string_cells, prorated_blob_cap, walk_heap_spans, BlobCache};
use crate::schema::{SchemaDescriptor, SchemaFacts};
use gnitz_wire::wal;
use gnitz_wire::{Regions, TypeCode};

/// Each fixed region's offset in a `rows`-row block.
fn wire_offsets(strides: &[u8], nr: usize, rows: usize, offsets: &mut [usize; MAX_BATCH_REGIONS]) {
    let mut off = wal::WAL_HEADER_SIZE;
    for r in 0..nr {
        offsets[r] = off;
        off += rows * strides[r] as usize;
    }
}

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
    /// The listed rows of a batch whose cells reference no heap.
    Listed(&'a [u32]),
}

impl WireRows<'_> {
    /// Rows the block holds; never zero.
    pub fn rows(&self) -> usize {
        match self.pick {
            Pick::Whole => self.batch.count,
            Pick::Range { rows, .. } => rows,
            Pick::Listed(indices) => indices.len(),
        }
    }

    /// Bytes [`Self::encode`] writes.
    pub fn byte_size(&self) -> usize {
        wal::WAL_HEADER_SIZE + self.rows() * row_width(self.batch.strides()) + self.heap
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
                .encode_relocating(rows, out, |data, offsets, heap| {
                    b.copy_range(start, rows, data, offsets, heap)
                })
                .expect("a sized range fits the bytes its size reserved"),
            Pick::Listed(indices) => b
                .encode_listed(indices, out)
                .expect("a heap-free list fits the bytes its size reserved"),
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

    /// The rows `indices` lists, in that order; `None` for an empty list. No cell
    /// of `self` references a heap, so the block holds none: a relocated heap is
    /// sized only by the pass [`Self::encode_scattered_prefix`] runs.
    pub fn wire_listed<'a>(&'a self, indices: &'a [u32]) -> Option<WireRows<'a>> {
        debug_assert_eq!(self.heap_referencing_slots(), 0, "wire_listed: the batch has a heap");
        (!indices.is_empty()).then_some(WireRows {
            batch: self,
            pick: Pick::Listed(indices),
            heap: 0,
        })
    }

    /// Payload slots whose cells can reference this batch's heap.
    pub fn heap_referencing_slots(&self) -> u64 {
        if self.blob.is_empty() {
            0
        } else {
            self.schema().string_payload_slots()
        }
    }

    /// Rows from `start` that fit `budget` by fixed width alone.
    fn rows_by_width(&self, start: usize, budget: usize) -> usize {
        let slope = row_width(self.strides());
        (self.count - start).min((budget.saturating_sub(wal::WAL_HEADER_SIZE) / slope).max(1))
    }

    /// How many of `rows`, from the front, fit `budget` once the heap bytes they
    /// relocate are charged, and those heap bytes. `slots` names the
    /// German-string payload slots.
    fn rows_with_heap(&self, rows: impl ExactSizeIterator<Item = usize>, slots: u64, budget: usize) -> (usize, usize) {
        let slope = row_width(self.strides());
        let base = wal::WAL_HEADER_SIZE;
        let mut seen = BlobCache::new(rows.len());
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
        for pi in gnitz_wire::BitIter(slots) {
            let cell = self.get_col_ptr(row, pi, 16);
            let Some(span) = gnitz_wire::german_string_heap(cell, self.blob.len()) else {
                continue;
            };
            let len = span.end - span.start;
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
        for r in 0..self.strides().len() {
            out.push(self.region_at(r));
        }
        out.push(&self.blob);
        out
    }

    /// Rows `[start, start + rows)` into a block's regions at `offsets`, their
    /// string cells relocated into `heap`.
    fn copy_range(&self, start: usize, rows: usize, data: &mut [u8], offsets: &[usize], heap: &mut Vec<u8>) {
        let slots = self.heap_referencing_slots();
        let mut cache = BlobCache::new(rows);
        for (r, &stride) in self.strides().iter().enumerate() {
            let stride = stride as usize;
            let src = &self.region_at(r)[start * stride..][..rows * stride];
            let dst = &mut data[offsets[r]..][..rows * stride];
            match r.checked_sub(REG_PAYLOAD_START) {
                Some(pi) if (slots >> pi) & 1 != 0 => copy_string_cells(dst, src, &self.blob, heap, None, &mut cache),
                _ => dst.copy_from_slice(src),
            }
        }
    }

    /// The rows `indices` selects, in order, as one WAL block at the front of
    /// `out`, their strings relocated into the block's own heap. Returns bytes
    /// written; `None` when the block does not fit `out`.
    fn encode_listed(&self, indices: &[u32], out: &mut [u8]) -> Option<usize> {
        let count = indices.len();
        self.encode_relocating(count, out, |data, offsets, heap| {
            let mut writer = DirectWriter::over_regions(data, offsets, self.strides(), count, self.schema(), heap);
            super::scatter::scatter_copy(&self.as_mem_batch(), indices, &mut writer);
        })
    }

    /// A WAL block of `count` rows at the front of `out`, whose regions `fill`
    /// writes at `offsets` and whose heap it relocates into. `None` when the
    /// block does not fit `out`.
    fn encode_relocating(
        &self,
        count: usize,
        out: &mut [u8],
        fill: impl FnOnce(&mut [u8], &[usize], &mut Vec<u8>),
    ) -> Option<usize> {
        let strides = self.strides();
        let fixed = count * row_width(strides);
        let heap_at = wal::WAL_HEADER_SIZE + fixed;
        if heap_at > out.len() {
            return None;
        }
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        wire_offsets(strides, strides.len(), count, &mut offsets);
        let mut heap = PooledBuf::with_capacity(prorated_blob_cap(self.blob.len(), self.count, count));
        fill(&mut out[..heap_at], &offsets[..strides.len()], &mut heap.0);
        let total = heap_at + heap.0.len();
        out.get_mut(heap_at..total)?.copy_from_slice(&heap.0);
        // Relocated cell by cell, so every heap byte is referenced.
        Some(wal::write_head(out, count, fixed, heap.0.len(), 0))
    }

    /// The longest front run of the rows `indices` selects, ascending, that fits
    /// `out` as one block, written there: its row count and bytes. `None` when
    /// not even the first row fits.
    pub fn encode_scattered_prefix(&self, indices: &[u32], out: &mut [u8]) -> Option<(usize, usize)> {
        let width = row_width(self.strides());
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
            if let Some(len) = self.encode_listed(rows, out) {
                return Some((rows.len(), len));
            }
        }
        let n = self
            .rows_with_heap(rows.iter().map(|&i| i as usize), slots, out.len())
            .0;
        self.encode_listed(&rows[..n], out).map(|len| (n, len))
    }

    /// Decode a WAL block the engine wrote into an owned unconsolidated batch. String
    /// cells are copied verbatim with their heap.
    pub fn decode_from_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        Ok(Batch::from_mem_batch(&WalBlock::parse(data, schema)?.view(), schema))
    }

    /// [`Self::decode_from_wal_block`] for a block a peer wrote, validated
    /// first. The header's dead-heap bound is not trusted: the heap is measured.
    pub fn decode_foreign_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        let block = WalBlock::parse(data, schema)?;
        let mut mb = block.view();
        mb.dead_heap = validate_string_cells(&mb, schema)?;
        if gnitz_wire::first_not_null_violation(schema.not_null_payload_slots(), mb.null_bmp()).is_some() {
            return Err("a null bit on a NOT NULL column");
        }
        if first_valued_null_cell(&mb, schema).is_some() {
            return Err("a non-zero cell under a NULL");
        }
        Ok(Batch::from_mem_batch(&mb, schema))
    }
}

/// The first `(row, payload slot)` of `mb` whose NULL cell is not zeroed: a key
/// reader packs or routes a cell whatever its null bit says, so a second
/// encoding would key a row and its retraction apart.
pub(super) fn first_valued_null_cell(mb: &MemBatch<'_>, schema: &SchemaDescriptor) -> Option<(usize, usize)> {
    let col = |pi| {
        let width = schema.columns[schema.payload_col_idx(pi)].size() as usize;
        (mb.col_data(pi, width), width)
    };
    gnitz_wire::first_valued_null(schema.nullable_payload_slots(), mb.null_bmp(), col)
}

/// Every German-string cell of `mb` is in canonical form against its own heap,
/// and every STRING cell's content is UTF-8. Answers the heap's exact count of
/// bytes no cell references.
fn validate_string_cells(mb: &MemBatch<'_>, schema: &SchemaDescriptor) -> Result<usize, &'static str> {
    // Non-ASCII STRING contents, each closed by an ASCII byte so no character
    // runs from one into the next.
    let mut text = Vec::new();
    let dead = walk_heap_spans(mb, schema, |cell, tc| {
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

/// A WAL block validated under `schema`, borrowing `data`, with the region
/// offsets its [`MemBatch`] view reads through.
///
/// Its string cells and dead-heap bound are taken as the engine wrote them; a
/// block a peer wrote goes through [`Batch::decode_foreign_wal_block`].
pub struct WalBlock<'a> {
    offsets: [usize; MAX_BATCH_REGIONS],
    data: &'a [u8],
    blob: &'a [u8],
    count: usize,
    dead_heap: usize,
    pk_stride: u8,
}

impl<'a> WalBlock<'a> {
    pub fn parse(data: &'a [u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        let strides = strides_from_schema(schema);
        let nr = REG_PAYLOAD_START + schema.num_payload_cols();
        let (count, _, blob, dead_heap) = wal::parse_block(data, row_width(&strides[..nr]))?;
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        wire_offsets(&strides, nr, count, &mut offsets);
        Ok(WalBlock {
            offsets,
            data,
            blob,
            count,
            dead_heap,
            pk_stride: strides[REG_PK],
        })
    }

    /// The block's rows, as [`Batch::as_mem_batch`] views a batch's.
    #[inline]
    pub fn view(&self) -> MemBatch<'_> {
        MemBatch {
            data: self.data,
            offsets: &self.offsets,
            pk_stride: self.pk_stride,
            blob: self.blob,
            count: self.count,
            dead_heap: self.dead_heap,
        }
    }
}

#[cfg(test)]
#[path = "tests/batch_wire.rs"]
mod tests;
