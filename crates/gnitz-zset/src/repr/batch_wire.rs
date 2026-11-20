//! Wire serialization for `Batch` and `MemBatch`: [`WireRows`], the row
//! selection that sizes and encodes itself, and WAL-block decoding with the
//! validation it runs.

use super::batch::Batch;
use super::batch::MemBatch;
use super::merge;
use super::pool::PooledBuf;
use super::scatter::{copy_ranges, scatter_copy};
use super::string_heap::{BlobCache, blob_span_key, walk_heap_spans};
use super::writer::DirectWriter;
use crate::schema::SchemaDescriptor;
use gnitz_expr::ColumnLocator;
use gnitz_wire::wal;
use gnitz_wire::{RowSource, TypeCode};
use std::ops::Range;

/// One or more rows of a batch as one WAL block: which rows, and the heap
/// bytes the block holds for them. Its size and its bytes are read off the
/// same value.
#[derive(Clone, Copy)]
pub struct WireRows<'a> {
    batch: MemBatch<'a>,
    selection: Selection<'a>,
    heap: usize,
}

/// Which rows of the batch a block holds. A selection other than the whole
/// batch relocates its strings into the block's own heap: under span dedup when
/// `dedup`, else each cell on its own, as [`MemBatch::fit_plain`] decides.
#[derive(Clone, Copy)]
enum Selection<'a> {
    /// Every row, over the batch's heap as it stands.
    Whole,
    /// Rows `[start, start + rows)`.
    Range { start: usize, rows: usize, dedup: bool },
    /// The listed rows.
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
                wal::write_block(&b.regions(), b.dead_heap, out)
            }
            Selection::Range { start, rows, dedup } => b
                .encode_relocating(rows, self.heap, out, dedup, |writer| {
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

/// The rows a block is sized over, in order: a run of the batch's rows, or a
/// list of them.
enum Rows<'r> {
    Run(Range<usize>),
    Listed(&'r [u32]),
}

impl Rows<'_> {
    fn len(&self) -> usize {
        match self {
            Rows::Run(r) => r.len(),
            Rows::Listed(ids) => ids.len(),
        }
    }

    /// The `i`-th row.
    #[inline(always)]
    fn at(&self, i: usize) -> usize {
        match self {
            Rows::Run(r) => r.start + i,
            Rows::Listed(ids) => ids[i] as usize,
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
        if start == 0
            && let Some(whole) = self.wire_whole()
            && whole.byte_size() <= budget
        {
            return Some(whole);
        }
        // Relocation is decided over the rows that fit by width alone.
        let by_width = (budget.saturating_sub(wal::WAL_HEADER_SIZE) / self.schema().row_width()).max(1);
        let (rows, heap, dedup) = self.relocation(Rows::Run(start..self.len().min(start + by_width)), budget);
        let out = WireRows {
            batch: *self,
            selection: Selection::Range { start, rows, dedup },
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
    /// it stands; any other relocates as [`Self::fit_plain`] decides.
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
        let (_, heap, dedup) = self.relocation(Rows::Listed(indices), usize::MAX);
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
    /// relocate are charged, those heap bytes, and whether they relocate under
    /// span dedup, as [`Self::fit_plain`] decides.
    fn relocation(&self, rows: Rows<'_>, budget: usize) -> (usize, usize, bool) {
        let slots = self.heap_referencing_slots();
        let (n, heap, dedup) = self.fit_plain(&rows, slots, budget);
        if !dedup {
            return (n, heap, false);
        }
        let (n, heap) = self.deduped_fit(&rows, slots, budget);
        (n, heap, true)
    }

    /// How many of `rows`, from the front, fit `budget` once the heap bytes they
    /// relocate under span dedup are charged — each span once — and those heap
    /// bytes. `slots` names the German-string payload slots.
    fn deduped_fit(&self, rows: &Rows<'_>, slots: u64, budget: usize) -> (usize, usize) {
        let mut seen = BlobCache::for_rows(self.schema, rows.len());
        let costs = (0..rows.len()).map(|i| self.row_heap_cost(rows.at(i), slots, &mut seen));
        self.fit_rows(costs, budget)
    }

    /// How many rows, from the front, fit `budget`, each taking its fixed width
    /// and the heap bytes `costs` charges it, and those heap bytes.
    fn fit_rows(&self, costs: impl Iterator<Item = usize>, budget: usize) -> (usize, usize) {
        let slope = self.schema().row_width();
        let (mut fit, mut heap) = (0usize, 0usize);
        for cost in costs {
            // The first row goes in whatever it costs; a frame carries whole rows.
            if fit > 0 && wal::WAL_HEADER_SIZE + (fit + 1) * slope + heap + cost > budget {
                break;
            }
            heap += cost;
            fit += 1;
        }
        (fit, heap)
    }

    /// How many of `rows`, from the front, fit `budget` once the heap bytes
    /// their long cells in the German-string slots `slots` take relocated each
    /// on its own are charged, and those heap bytes — summed a column at a time
    /// over a chunk of rows, up to the chunk whose row passes `budget` — and
    /// whether the block relocates under span dedup instead: where the cells of
    /// the rows read take more heap than the extent of the heap they name, some
    /// name a byte twice, and the block keeps each span once.
    fn fit_plain(&self, rows: &Rows<'_>, slots: u64, budget: usize) -> (usize, usize, bool) {
        const CHUNK: usize = 256;
        let (blob_len, slope) = (self.blob().len(), self.schema().row_width());
        let (mut fit, mut heap) = (0usize, 0usize);
        // The plain heap of the rows read, and the extent of the heap they name.
        let (mut plain, mut lo, mut hi) = (0usize, usize::MAX, 0usize);
        let mut costs = [0usize; CHUNK];
        for s in (0..rows.len()).step_by(CHUNK) {
            let e = (s + CHUNK).min(rows.len());
            let costs = &mut costs[..e - s];
            costs.fill(0);
            for pi in gnitz_wire::BitIter(slots) {
                let cells = self.col_data(pi).as_chunks::<16>().0;
                let mut charge = |cost: &mut usize, cell: &[u8; 16]| {
                    if let Some(span) = gnitz_wire::german_string_heap(cell, blob_len) {
                        *cost += span.len();
                        (lo, hi) = (lo.min(span.start), hi.max(span.end));
                    }
                };
                match rows {
                    Rows::Run(r) => costs
                        .iter_mut()
                        .zip(&cells[r.start + s..r.start + e])
                        .for_each(|(c, cell)| charge(c, cell)),
                    Rows::Listed(ids) => costs
                        .iter_mut()
                        .zip(&ids[s..e])
                        .for_each(|(c, &row)| charge(c, &cells[row as usize])),
                }
            }
            for &cost in costs.iter() {
                plain += cost;
                // The first row goes in whatever it costs; a frame carries whole rows.
                if fit > 0 && wal::WAL_HEADER_SIZE + (fit + 1) * slope + heap + cost > budget {
                    return (fit, heap, plain > hi.saturating_sub(lo));
                }
                heap += cost;
                fit += 1;
            }
        }
        (fit, heap, plain > hi.saturating_sub(lo))
    }

    /// Heap bytes row `row` adds to a chunk: the spans its long cells in the
    /// string slots `slots` name, each charged once, `seen` holding the chunk's
    /// spans so far.
    #[inline]
    fn row_heap_cost(&self, row: usize, slots: u64, seen: &mut BlobCache) -> usize {
        let blob = self.blob();
        gnitz_wire::BitIter(slots)
            .filter_map(|pi| gnitz_wire::german_string_heap(self.german_cell(row, pi), blob.len()))
            .map(|span| charge_span(&blob[span], seen))
            .sum()
    }

    /// The rows `indices` selects, in order, as one WAL block at the front of
    /// `out`, their strings relocated into the block's own heap, for which
    /// `heap_bytes` are reserved: under span dedup when `dedup`, else each cell
    /// on its own. Returns bytes written; `None` when the block does not fit
    /// `out`.
    fn encode_listed(&self, indices: &[u32], heap_bytes: usize, out: &mut [u8], dedup: bool) -> Option<usize> {
        let count = indices.len();
        self.encode_relocating(count, heap_bytes, out, dedup, |writer| {
            scatter_copy(self, indices, None, writer);
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
    /// not even the first row fits. Strings relocate as [`Self::fit_plain`]
    /// decides over the rows that fit by width.
    pub fn encode_scattered_prefix(&self, indices: &[u32], out: &mut [u8]) -> Option<(usize, usize)> {
        debug_assert!(
            indices.is_sorted_by(|a, b| a < b),
            "encode_scattered_prefix: a list that is not strictly ascending"
        );
        let width = self.schema().row_width();
        let fixed_fit = out.len().saturating_sub(wal::WAL_HEADER_SIZE) / width;
        let rows = &indices[..indices.len().min(fixed_fit)];
        if rows.is_empty() {
            return None;
        }
        // Every row, ascending, is the batch itself: framed whole, heap as it is.
        if indices.len() == self.len()
            && let Some(whole) = self.wire_whole().filter(|w| w.byte_size() <= out.len())
        {
            return Some((self.len(), whole.encode(out)));
        }
        let slots = self.heap_referencing_slots();
        let listed = Rows::Listed(rows);
        let (_, copied, dedup) = self.fit_plain(&listed, slots, usize::MAX);
        // Dedup copies each distinct span once, so the whole source heap bounds
        // the block's unless cells name overlapping spans, which a certified
        // foreign block may; a block that then overruns `out` is refused here
        // and sized exactly below.
        let heap = if dedup { self.blob().len() } else { copied };
        if wal::WAL_HEADER_SIZE + rows.len() * width + heap <= out.len()
            && let Some(len) = self.encode_listed(rows, heap, out, dedup)
        {
            return Some((rows.len(), len));
        }
        let (n, heap) = match dedup {
            false => {
                let (n, heap, _) = self.fit_plain(&listed, slots, out.len());
                (n, heap)
            }
            true => self.deduped_fit(&listed, slots, out.len()),
        };
        self.encode_listed(&rows[..n], heap, out, dedup).map(|len| (n, len))
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

    /// The sum of `blocks`, WAL blocks a peer wrote, as one batch, every block
    /// validated as [`MemBatch::of_foreign_wal_block`] validates one before any
    /// is read. Several blocks that each stand consolidated are merged into one
    /// consolidated batch; a lone block is copied as it stands and any other
    /// mix concatenated, both unconsolidated.
    pub fn decode_foreign_wal_blocks(blocks: &[&[u8]], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        match blocks {
            [] => Ok(Batch::empty_with_schema(schema)),
            [one] => Ok(MemBatch::of_foreign_wal_block(one, schema)?.to_batch()),
            _ => {
                let views = (blocks.iter())
                    .map(|b| MemBatch::of_foreign_wal_block(b, schema))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(match views.iter().all(merge::view_in_consolidated_order) {
                    true => merge::merge_consolidated(&views, schema),
                    false => Batch::concat(schema, views.iter().copied()),
                })
            }
        }
    }
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
    simdutf8::basic::from_utf8(&text).map_err(|_| "a TEXT cell is not UTF-8")?;
    Ok(dead)
}

/// The first `(row, payload slot)` whose little-endian `u64` null word in
/// `null_bmp` sets a bit outside the `nullable` slots, past the last payload
/// slot included.
fn first_not_null_violation(nullable: u64, null_bmp: &[u8]) -> Option<(usize, usize)> {
    let not_null = !nullable;
    let words = null_bmp.as_chunks::<8>().0;
    // A conforming batch takes only this branch-free pass.
    if not_null == 0 || words.iter().fold(0u64, |a, w| a | u64::from_le_bytes(*w)) & not_null == 0 {
        return None;
    }
    words.iter().enumerate().find_map(|(row, w)| {
        let bad = u64::from_le_bytes(*w) & not_null;
        (bad != 0).then(|| (row, bad.trailing_zeros() as usize))
    })
}

/// The first `(row, payload slot)` whose null bit in `null_bmp`, among the
/// `nullable` slots, sits over a non-zero cell: NULL encodes as a zeroed cell.
/// `col(slot)` is that slot's region and its cell width.
fn first_valued_null<'a>(
    nullable: u64,
    null_bmp: &[u8],
    col: impl Fn(usize) -> (&'a [u8], usize),
) -> Option<(usize, usize)> {
    let words = null_bmp.as_chunks::<8>().0;
    // A batch holding no NULL takes only this branch-free pass.
    let present = words.iter().fold(0u64, |a, w| a | u64::from_le_bytes(*w)) & nullable;
    let mut first: Option<(usize, usize)> = None;
    // One pass per slot that holds a NULL anywhere.
    for slot in gnitz_wire::BitIter(present) {
        let (cells, width) = col(slot);
        let row = gnitz_wire::by_cell_width!(width, |T| valued_null_row::<{ size_of::<T>() }>(words, cells, slot));
        if let Some(row) = row.filter(|&r| first.is_none_or(|(f, _)| r < f)) {
            first = Some((row, slot));
        }
    }
    first
}

/// The first row whose null word sets bit `slot` over a non-zero `W`-byte cell.
fn valued_null_row<const W: usize>(words: &[[u8; 8]], cells: &[u8], slot: usize) -> Option<usize> {
    let cells = cells.as_chunks::<W>().0;
    let valued = |(w, c): (&[u8; 8], &[u8; W])| (u64::from_le_bytes(*w) >> slot) & 1 != 0 && *c != [0u8; W];
    // A conforming column takes only this pass, which has no early exit to branch on.
    if !words.iter().zip(cells).fold(false, |any, wc| any | valued(wc)) {
        return None;
    }
    words.iter().zip(cells).position(valued)
}

impl<'a> MemBatch<'a> {
    /// The first breach of this batch's null rule, and its `(row, payload slot)`: a null
    /// bit on a NOT NULL column, or a NULL cell that is not zeroed — a key reader
    /// packs or routes a cell whatever its null bit says, so a second encoding
    /// would key a row and its retraction apart.
    pub fn null_breach(&self) -> Option<(&'static str, (usize, usize))> {
        let schema = self.schema;
        if let Some(at) = first_not_null_violation(schema.nullable_payload_slots(), self.null_bmp()) {
            return Some(("a null bit on a NOT NULL column", at));
        }
        let col = |pi| {
            (
                self.col_data(pi),
                schema.region_stride(gnitz_wire::REG_PAYLOAD_START + pi),
            )
        };
        first_valued_null(schema.nullable_payload_slots(), self.null_bmp(), col)
            .map(|at| ("a non-zero cell under a NULL", at))
    }

    /// The rows of the WAL block `data` under `schema`: its fixed bytes are an
    /// arena of exactly its row count.
    ///
    /// Its string cells and dead-heap bound are taken as the engine wrote
    /// them; a block a peer wrote is read through [`Self::of_foreign_wal_block`],
    /// [`Batch::decode_foreign_wal_blocks`] or a [`CertifiedBlock`].
    pub fn of_wal_block(data: &'a [u8], schema: &'a SchemaDescriptor) -> Result<Self, &'static str> {
        let block = wal::parse_block(data, schema.row_width())?;
        Ok(MemBatch::of_block(&block, schema))
    }

    /// The rows of a WAL block whose header [`wal::parse_block`] read, under `schema`,
    /// taken as [`Self::of_wal_block`] takes them.
    #[inline]
    fn of_block(block: &wal::ParsedBlock<'a>, schema: &'a SchemaDescriptor) -> Self {
        debug_assert_eq!(block.fixed.len(), block.rows * schema.row_width());
        MemBatch {
            data: block.fixed,
            schema,
            cap: block.rows,
            blob: block.heap,
            count: block.rows,
            dead_heap: block.heap_dead,
        }
    }

    /// [`Self::of_wal_block`] for a block a peer wrote, validated first: its
    /// string cells, null bits and BOOLEAN cells. The header's dead-heap bound
    /// is not trusted: the heap is measured.
    pub fn of_foreign_wal_block(data: &'a [u8], schema: &'a SchemaDescriptor) -> Result<Self, &'static str> {
        let mut mb = MemBatch::of_wal_block(data, schema)?;
        mb.dead_heap = validate_string_cells(&mb)?;
        if let Some((breach, _)) = mb.null_breach() {
            return Err(breach);
        }
        if !boolean_cells_ok(&mb) {
            return Err("a BOOLEAN cell is neither 0 nor 1");
        }
        Ok(mb)
    }
}

/// A WAL block a peer wrote, validated once where it lies: at `range` of the
/// bytes it was certified in, `rows` rows, and its heap's measured count of
/// unreferenced bytes. Its rows are read from those bytes without parsing the
/// header again.
#[derive(Clone, Debug)]
pub struct CertifiedBlock {
    range: Range<usize>,
    rows: usize,
    heap_dead: usize,
}

impl CertifiedBlock {
    /// Validate the block at `range` of `data` under `schema` as
    /// [`MemBatch::of_foreign_wal_block`] validates one. Inline: compiled out of
    /// line, with the validation inlined into it, the cell loop measured slower
    /// per row.
    #[inline]
    pub fn certify(data: &[u8], range: Range<usize>, schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        let mb = MemBatch::of_foreign_wal_block(&data[range.clone()], schema)?;
        let (rows, heap_dead) = (mb.count, mb.dead_heap);
        Ok(CertifiedBlock { range, rows, heap_dead })
    }

    /// The block's rows, read from `data` under `schema`: the bytes and the
    /// schema it was certified under.
    #[inline]
    pub fn view<'a>(&self, data: &'a [u8], schema: &'a SchemaDescriptor) -> MemBatch<'a> {
        let fixed = self.range.start + wal::WAL_HEADER_SIZE;
        let heap = fixed + self.rows * schema.row_width();
        let block = wal::ParsedBlock {
            rows: self.rows,
            fixed: &data[fixed..heap],
            heap: &data[heap..self.range.end],
            heap_dead: self.heap_dead,
        };
        MemBatch::of_block(&block, schema)
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
