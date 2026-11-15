//! Wire serialization for `Batch` and `MemBatch`: [`WireRows`], the row
//! selection that sizes and encodes itself, and WAL-block decoding with the
//! validation it runs.

use super::batch::Batch;
use super::batch::MemBatch;
use super::merge;
use super::pool::PooledBuf;
use super::scatter::{copy_ranges, scatter_copy};
use super::string_heap::{blob_span_key, walk_heap_spans, BlobCache};
use super::writer::DirectWriter;
use crate::schema::SchemaDescriptor;
use gnitz_expr::ColumnLocator;
use gnitz_wire::wal;
use gnitz_wire::{Regions, RowSource, TypeCode};

/// One or more rows of a batch as one WAL block: which rows, and the heap
/// bytes the block holds for them. Its size and its bytes are read off the
/// same value.
#[derive(Clone, Copy)]
pub struct WireRows<'a> {
    batch: MemBatch<'a>,
    selection: Selection<'a>,
    heap: usize,
}

/// Which rows of the batch a block holds.
#[derive(Clone, Copy)]
enum Selection<'a> {
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
        match self.selection {
            Selection::Whole => self.batch.len(),
            Selection::Range { rows, .. } => rows,
            Selection::Listed { rows, .. } => rows.len(),
        }
    }

    /// Bytes [`Self::encode`] writes.
    pub fn byte_size(&self) -> usize {
        wal::WAL_HEADER_SIZE + self.rows() * self.batch.schema().row_width() + self.heap
    }

    /// The block at the front of `out`. Returns bytes written.
    pub fn encode(&self, out: &mut [u8]) -> usize {
        let b = self.batch;
        match self.selection {
            Selection::Whole => {
                b.debug_verify_dead_heap();
                wal::write_block(&b.wire_regions(), b.dead_heap, out)
            }
            Selection::Range { start, rows } => b
                .encode_relocating(rows, self.heap, out, true, |writer| {
                    // An empty heap is carried: its cells name no byte of it.
                    let heap_at = b.blob().is_empty().then_some(0);
                    copy_ranges(&b, heap_at, &[(start, start + rows)], writer);
                })
                .expect("a sized range fits the bytes its size reserved"),
            Selection::Listed { rows, dedup } => b
                .encode_listed(rows, self.heap, out, dedup)
                .expect("a sized list fits the bytes its size reserved"),
        }
    }
}

impl<'a> MemBatch<'a> {
    /// Every row, over the heap as it stands and under its dead-byte bound;
    /// `None` for an empty batch.
    pub fn wire_whole(&self) -> Option<WireRows<'a>> {
        (!self.is_empty()).then(|| WireRows {
            batch: *self,
            selection: Selection::Whole,
            heap: self.blob().len(),
        })
    }

    /// The longest run of rows from `start` whose block fits `budget`: at least
    /// one row, however wide, and `None` once no row remains. The whole batch,
    /// when it fits, goes over its heap as it stands.
    pub fn wire_rows_within(&self, start: usize, budget: usize) -> Option<WireRows<'a>> {
        if start == self.len() {
            return None;
        }
        if let Some(whole) = self.wire_whole().filter(|w| start == 0 && w.byte_size() <= budget) {
            return Some(whole);
        }
        let (rows, heap) = match self.heap_referencing_slots() {
            0 => (
                (self.len() - start)
                    .min((budget.saturating_sub(wal::WAL_HEADER_SIZE) / self.schema().row_width()).max(1)),
                0,
            ),
            slots => self.rows_with_heap(start..self.len(), slots, budget),
        };
        let out = WireRows {
            batch: *self,
            selection: Selection::Range { start, rows },
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
    pub fn wire_listed<'b>(&self, indices: &'b [u32]) -> Option<WireRows<'b>>
    where
        'a: 'b,
    {
        if indices.is_empty() {
            return None;
        }
        if indices.len() == self.len() && indices.iter().zip(0..).all(|(&row, at)| row == at) {
            return self.wire_whole();
        }
        let slots = self.heap_referencing_slots();
        let rows = || indices.iter().map(|&row| row as usize);
        let blob_len = self.blob().len();
        let copied: usize = gnitz_wire::BitIter(slots)
            .map(|pi| {
                let cells = self.col_data(pi).as_chunks::<16>().0;
                rows()
                    .filter_map(|row| gnitz_wire::german_string_heap(&cells[row], blob_len))
                    .map(|span| span.len())
                    .sum::<usize>()
            })
            .sum();
        let dedup = copied > blob_len;
        let heap = match dedup {
            true => self.rows_with_heap(rows(), slots, usize::MAX).1,
            false => copied,
        };
        Some(WireRows {
            batch: *self,
            selection: Selection::Listed { rows: indices, dedup },
            heap,
        })
    }

    /// Payload slots whose cells can reference this batch's heap.
    fn heap_referencing_slots(&self) -> u64 {
        if self.blob().is_empty() {
            0
        } else {
            self.schema().string_payload_slots()
        }
    }

    /// How many of `rows`, from the front, fit `budget` once the heap bytes they
    /// relocate under span dedup are charged, and those heap bytes. `slots`
    /// names the German-string payload slots.
    fn rows_with_heap(&self, rows: impl ExactSizeIterator<Item = usize>, slots: u64, budget: usize) -> (usize, usize) {
        let slope = self.schema().row_width();
        let base = wal::WAL_HEADER_SIZE;
        let mut seen = BlobCache::for_rows(self.schema, rows.len());
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

    /// Heap bytes row `row` adds to a chunk whose spans are already in `seen`:
    /// the spans its long cells in the string slots `slots` name, each new one
    /// charged once.
    #[inline]
    fn row_heap_cost(&self, row: usize, slots: u64, seen: &mut BlobCache) -> usize {
        let blob = self.blob();
        let mut cost = 0;
        for pi in gnitz_wire::BitIter(slots) {
            if let Some(span) = gnitz_wire::german_string_heap(self.german_cell(row, pi), blob.len()) {
                cost += charge_span(&blob[span], seen);
            }
        }
        cost
    }

    /// Every fixed region followed by the blob heap, in canonical order.
    pub fn wire_regions(&self) -> Regions<'a> {
        let mut out = Regions::new();
        for r in 0..self.schema().num_fixed_regions() {
            out.push(self.region(r));
        }
        out.push(self.blob());
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
            scatter_copy(self, indices, writer);
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
        let mut cache = BlobCache::for_rows(self.schema, count);
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
        if indices.len() == self.len() {
            if let Some(whole) = self.wire_whole().filter(|w| w.byte_size() <= out.len()) {
                return Some((self.len(), whole.encode(out)));
            }
        }
        let slots = self.heap_referencing_slots();
        // A heap-free block fits by width alone, and the sizing pass is skipped
        // when the source's whole heap fits too.
        if slots == 0 || wal::WAL_HEADER_SIZE + rows.len() * width + self.blob().len() <= out.len() {
            // Dedup copies each distinct span once, so the whole source heap
            // bounds the block's unless cells name overlapping spans, which a
            // certified foreign block may; a block that then overruns `out` is
            // refused here and sized exactly below.
            let heap = if slots == 0 { 0 } else { self.blob().len() };
            if let Some(len) = self.encode_listed(rows, heap, out, true) {
                return Some((rows.len(), len));
            }
        }
        let (n, heap) = self.rows_with_heap(rows.iter().map(|&i| i as usize), slots, out.len());
        self.encode_listed(&rows[..n], heap, out, true).map(|len| (n, len))
    }
}

impl Batch {
    /// [`MemBatch::wire_whole`].
    pub fn wire_whole(&self) -> Option<WireRows<'_>> {
        self.as_mem_batch().wire_whole()
    }

    /// [`MemBatch::wire_rows_within`].
    pub fn wire_rows_within(&self, start: usize, budget: usize) -> Option<WireRows<'_>> {
        self.as_mem_batch().wire_rows_within(start, budget)
    }

    /// [`MemBatch::wire_regions`].
    pub fn wire_regions(&self) -> Regions<'_> {
        self.as_mem_batch().wire_regions()
    }

    /// A WAL block a peer wrote, validated — its string cells canonical and
    /// STRING ones UTF-8, its null bits, its BOOLEAN cells 0 or 1, its heap
    /// measured rather than its header's dead-byte bound trusted — and copied
    /// into an owned unconsolidated batch.
    pub fn decode_foreign_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        Ok(MemBatch::validate_foreign(data, schema)?.to_batch())
    }

    /// The sum of `blocks`, WAL blocks a peer wrote, as one batch, every block
    /// validated as [`Self::decode_foreign_wal_block`] validates one before any is
    /// read. Several blocks that each stand consolidated are merged into one
    /// consolidated batch; a lone block is copied as it stands and any other
    /// mix concatenated, both unconsolidated.
    pub fn decode_foreign_wal_blocks(blocks: &[&[u8]], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        match blocks {
            [] => Ok(Batch::empty_with_schema(schema)),
            [one] => Batch::decode_foreign_wal_block(one, schema),
            _ => {
                let views = (blocks.iter())
                    .map(|b| MemBatch::validate_foreign(b, schema))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(match views.iter().all(merge::view_in_consolidated_order) {
                    true => merge::merge_consolidated(&views, schema),
                    false => Batch::concat(schema, views.iter().copied()),
                })
            }
        }
    }
}

/// The first breach of `mb`'s null rule, and its `(row, payload slot)`: a null
/// bit on a NOT NULL column, or a NULL cell that is not zeroed — a key reader
/// packs or routes a cell whatever its null bit says, so a second encoding
/// would key a row and its retraction apart.
pub(super) fn null_breach(mb: &MemBatch<'_>) -> Option<(&'static str, (usize, usize))> {
    let schema = mb.schema;
    if let Some(at) = gnitz_wire::first_not_null_violation(schema.nullable_payload_slots(), mb.null_bmp()) {
        return Some(("a null bit on a NOT NULL column", at));
    }
    let col = |pi| {
        (
            mb.col_data(pi),
            schema.region_stride(gnitz_wire::REG_PAYLOAD_START + pi),
        )
    };
    gnitz_wire::first_valued_null(schema.nullable_payload_slots(), mb.null_bmp(), col)
        .map(|at| ("a non-zero cell under a NULL", at))
}

/// Every BOOLEAN cell of `mb`, key or payload, is 0 or 1: a third byte would
/// be a second encoding of true, which keys and compares apart from the first.
fn boolean_cells_ok(mb: &MemBatch<'_>) -> bool {
    // Branch-free over a column: a conforming batch is the one worth being fast on.
    fn le1<'a>(cells: impl Iterator<Item = &'a u8>) -> bool {
        cells.fold(0, |a, &b| a | b) <= 1
    }
    (0..mb.schema.num_columns()).all(|ci| match mb.schema.locate(ci) {
        ColumnLocator::Payload { slot, type_code: TypeCode::Bool, .. } => le1(mb.col_data(slot as usize).iter()),
        ColumnLocator::Pk { byte_off, type_code: TypeCode::Bool, .. } => {
            le1(mb.pk().iter().skip(byte_off as usize).step_by(mb.pk_stride()))
        }
        _ => true,
    })
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
    /// them; a block a peer wrote goes through [`Batch::decode_foreign_wal_block`],
    /// [`Batch::decode_foreign_wal_blocks`] or [`Self::certify_foreign`] first.
    pub fn of_wal_block(data: &'a [u8], schema: &'a SchemaDescriptor) -> Result<Self, &'static str> {
        let block = wal::parse_block(data, schema.row_width())?;
        Ok(MemBatch {
            data: block.fixed,
            schema,
            cap: block.rows,
            blob: block.heap,
            count: block.rows,
            dead_heap: block.heap_dead,
        })
    }

    /// [`Self::of_wal_block`] for a block a peer wrote, validated first: its
    /// string cells, null bits and BOOLEAN cells. The header's dead-heap bound
    /// is not trusted: the heap is measured.
    fn validate_foreign(data: &'a [u8], schema: &'a SchemaDescriptor) -> Result<Self, &'static str> {
        let mut mb = MemBatch::of_wal_block(data, schema)?;
        mb.dead_heap = validate_string_cells(&mb)?;
        if let Some((breach, _)) = null_breach(&mb) {
            return Err(breach);
        }
        if !boolean_cells_ok(&mb) {
            return Err("a BOOLEAN cell is neither 0 nor 1");
        }
        Ok(mb)
    }

    /// Validate the block `data` a peer wrote as [`Batch::decode_foreign_wal_block`]
    /// validates one, then write the heap's measured dead-byte bound into its
    /// header: from then on [`Self::of_wal_block`] reads `data` as a block the
    /// engine wrote.
    pub fn certify_foreign(data: &mut [u8], schema: &SchemaDescriptor) -> Result<(), &'static str> {
        let dead = MemBatch::validate_foreign(data, schema)?.dead_heap;
        gnitz_wire::wal::set_heap_dead(data, dead);
        Ok(())
    }
}

/// The bytes of `content`, a span of the heap a chunk's rows read, when `seen`
/// does not hold it yet, which it then does; else zero. Out of line, so a row
/// with no long cell sizes without the map insert's register pressure.
#[inline(never)]
fn charge_span(content: &[u8], seen: &mut BlobCache) -> usize {
    match seen.map().insert(blob_span_key(content), 0) {
        None => content.len(),
        Some(_) => 0,
    }
}

#[cfg(test)]
#[path = "tests/batch_wire.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/batch_wire.rs"]
mod bench;
