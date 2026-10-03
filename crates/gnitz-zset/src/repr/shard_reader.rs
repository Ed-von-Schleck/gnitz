//! Memory-mapped columnar shard reader.
//!
//! Opening verifies the header and directory; [`MappedShard::verify_body`]
//! verifies the body.

use std::cell::OnceCell;
use std::ops::Range;
use std::rc::Rc;

use super::batch::{write_to_batch, Batch, FIXED_REGION_BYTES, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::batch_pool::PooledBuf;
use super::encoding::*;
use super::layout::*;
use super::merge::{ColPtr, ColumnarSource, UnifiedSource};
use super::mmap::Mmap;
use super::scatter::DecodedColumns;
use super::shard_filter;
use super::string_heap::{carried_dead, long_bytes_outside, prorated_blob_cap, relocate_german_string_vec};
use crate::repr::error::StorageError;
use crate::schema::SchemaDescriptor;
use gnitz_expr::RowSource;
use gnitz_wire::read_u64_le;

use StorageError::Corrupt;

/// A payload-column region.
pub(crate) enum PayloadRegion {
    /// Readable in place: a Raw (stride = width) or Constant (stride 0) region
    /// of the mapping, or `ZERO_CELL` at stride 0 for a column the file predates
    /// (its null bit comes from `null_pad_mask`).
    Mapped(ColPtr),
    Packed(PackedRegion),
    /// An [`Encoding::Dict`] region of the mapping: a per-row read answers out
    /// of the dictionary in place, a bulk read decodes its own window.
    Dict(DictImage<'static>),
}

/// What every row of a column the file predates reads.
static ZERO_CELL: [u8; 16] = [0; 16];

/// A payload region read through a decode. A per-row read decodes the block it
/// lands in and keeps it; a bulk read decodes its own window and keeps nothing.
pub(crate) struct PackedRegion {
    image: PackedImage,
    /// The column's cell width.
    width: usize,
    blocks: DecodedBlocks,
}

/// A [`PackedRegion`]'s image, a region of the mapping.
enum PackedImage {
    For(ForImage<'static>),
    Seq(SeqImage<'static>),
    Sparse(SparseImage<'static>),
}

/// The blocks of a column that per-row reads have decoded, one per
/// `DECODE_BLOCK_ROWS` rows.
pub(crate) struct DecodedBlocks(Box<[OnceCell<Box<[u8]>>]>);

impl DecodedBlocks {
    fn new(rows: usize) -> Self {
        DecodedBlocks((0..rows.div_ceil(DECODE_BLOCK_ROWS)).map(|_| OnceCell::new()).collect())
    }

    /// The leading `size` bytes of row `row`'s `width`-byte cell of a column of
    /// `rows`, from the block holding it — which `decode` fills from its first
    /// row on if no per-row read has yet.
    #[inline]
    fn cell(
        &self,
        row: usize,
        size: usize,
        (rows, width): (usize, usize),
        decode: impl FnOnce(usize, &mut [u8]),
    ) -> &[u8] {
        let block = &self.0[row / DECODE_BLOCK_ROWS];
        // The decode stays out of the read of a block already held.
        let cells = match block.get() {
            Some(cells) => cells,
            None => block.get_or_init(|| {
                let first = row - row % DECODE_BLOCK_ROWS;
                let mut out = vec![0u8; DECODE_BLOCK_ROWS.min(rows - first) * width].into_boxed_slice();
                decode(first, &mut out);
                out
            }),
        };
        &cells[(row % DECODE_BLOCK_ROWS) * width..][..size]
    }
}

/// A region of one 8-byte word per row: the weights, or the null words. A
/// packed one is a region of the mapping, read per row in place.
pub(crate) enum WordRegion {
    Mapped(ColPtr),
    TwoValue(TwoValueImage<'static>),
    For(ForImage<'static>),
}

impl WordRegion {
    /// The region `span` holds for `count` rows.
    fn bind(data: &'static [u8], span: &Span, count: usize) -> Result<Self, StorageError> {
        let image = span.bytes(data);
        match span.encoding {
            Encoding::TwoValue => TwoValueImage::parse(image, count).map(WordRegion::TwoValue),
            Encoding::For => ForImage::parse(image, count, FIXED_REGION_BYTES - 1).map(WordRegion::For),
            _ => return direct_region(data, span, count, FIXED_REGION_BYTES).map(WordRegion::Mapped),
        }
        .ok_or(Corrupt("region size"))
    }

    /// Row `row`'s word, `row` a row of the shard.
    #[inline(always)]
    fn word(&self, row: usize) -> u64 {
        match self {
            // SAFETY: the region holds a word for every row of the shard.
            WordRegion::Mapped(cp) => read_u64_le(unsafe { cp.row(row, FIXED_REGION_BYTES) }, 0),
            WordRegion::TwoValue(two) => two.at(row),
            WordRegion::For(frame) => frame.at(row),
        }
    }

    /// Rows `start..` as they stand in a batch, `dst.len() / 8` rows of the
    /// shard.
    fn decode(&self, start: usize, dst: &mut [u8]) {
        match self {
            // SAFETY: the region holds a word for every row of the shard.
            WordRegion::Mapped(cp) => unsafe { cp.copy_rows(start, FIXED_REGION_BYTES, dst) },
            WordRegion::TwoValue(two) => two.decode(start, dst),
            WordRegion::For(frame) => frame.decode(start, FIXED_REGION_BYTES, dst),
        }
    }
}

pub struct MappedShard {
    /// The mapping every pointer and `'static` slice below points into, shared
    /// by every handle [`rebind`](Self::rebind) derives from this one.
    mmap: Rc<Mmap>,
    header: ShardHeader,
    schema: SchemaDescriptor,
    pk: ColPtr,
    weight: WordRegion,
    null_bmp: WordRegion,
    /// Non-PK column regions indexed by payload position, always one per payload
    /// column of `schema`, a column the file predates included.
    col_regions: Vec<PayloadRegion>,
    /// The null bits of `schema`'s payload columns past the file's own, OR'd
    /// into every null word read: a column the file predates reads NULL.
    null_pad_mask: u64,
    blob: &'static [u8],
    /// The PK filter region, or `None` when the file carries none.
    shard_filter: Option<&'static [u8]>,
}

/// A shard's header and directory, read under no schema.
pub struct ShardDirectory {
    pub rows: usize,
    pub skeleton: bool,
    /// The digest the writer recorded over the body: two shards of one length
    /// that agree on it hold the same regions.
    pub body_checksum: u64,
    /// Each directory entry's role, encoding name and stored bytes.
    pub regions: Vec<(String, &'static str, usize)>,
}

impl ShardDirectory {
    /// The directory of the shard at `path`, checked as [`MappedShard::open`]
    /// checks it.
    pub fn read(path: &str) -> Result<Self, StorageError> {
        let (mmap, header) = MappedShard::map(path)?;
        let spans = region_spans(mmap.as_slice(), header.file_npc)?;
        let regions = spans
            .iter()
            .enumerate()
            .map(|(i, span)| {
                let role = match i {
                    REG_PK => "pk".to_string(),
                    REG_WEIGHT => "weight".to_string(),
                    REG_NULL_BMP => "null".to_string(),
                    _ if i == spans.len() - 2 => "blob".to_string(),
                    _ if i == spans.len() - 1 => "filter".to_string(),
                    _ => format!("p{}", i - REG_PAYLOAD_START),
                };
                (role, span.encoding.name(), span.size)
            })
            .collect();
        Ok(ShardDirectory {
            rows: header.row_count,
            skeleton: header.skeleton,
            body_checksum: header.body_checksum,
            regions,
        })
    }
}

/// A Raw (`count` elements) or Constant (one element, stride 0) region of
/// `width`-byte elements.
fn direct_region(data: &[u8], span: &Span, count: usize, width: usize) -> Result<ColPtr, StorageError> {
    let (stride, size) = match span.encoding {
        Encoding::Raw => (width, count * width),
        Encoding::Constant => (0, width),
        _ => return Err(Corrupt("encoding")),
    };
    if span.size != size {
        return Err(Corrupt("region size"));
    }
    Ok(ColPtr { base: span.bytes(data).as_ptr(), stride })
}

impl MappedShard {
    /// Whether this file is a bounded view's skeleton shard.
    #[inline(always)]
    pub fn is_skeleton(&self) -> bool {
        self.header.skeleton
    }

    /// Row `row`'s signed Z-set weight.
    #[inline(always)]
    pub fn get_weight(&self, row: usize) -> i64 {
        ColumnarSource::get_weight(self, row)
    }

    /// How many rows carry a negative weight.
    pub fn retraction_rows(&self) -> usize {
        self.header.retractions
    }

    pub fn open(path: &str, schema: &SchemaDescriptor) -> Result<Self, StorageError> {
        let (mmap, header) = Self::map(path)?;
        mmap.advise_hugepage();
        Self::bind(Rc::new(mmap), header, schema)
    }

    /// The file at `path` mapped, and its header, once its descriptive prefix
    /// matches the digest stamped over it.
    fn map(path: &str) -> Result<(Mmap, ShardHeader), StorageError> {
        let file = std::fs::File::open(path)?;
        if file.metadata()?.len() == 0 {
            return Err(Corrupt("empty file"));
        }
        let mmap = Mmap::from_file(&file)?;
        let data = mmap.as_slice();
        let header = ShardHeader::read(data)?;
        let prefix = data
            .get(..desc_len(header.file_npc))
            .ok_or(Corrupt("shorter than its directory"))?;
        if desc_digest(path, prefix) != read_u64_le(prefix, OFF_DESC_CHECKSUM) {
            return Err(Corrupt("descriptor digest"));
        }
        Ok((mmap, header))
    }

    /// This shard's mapping bound to `schema`, with no file I/O.
    pub fn rebind(&self, schema: &SchemaDescriptor) -> Result<Self, StorageError> {
        Self::bind(Rc::clone(&self.mmap), self.header, schema)
    }

    /// A handle on `mmap` under `schema`. Trusts the descriptive prefix
    /// [`open`](Self::open) checked.
    fn bind(mmap: Rc<Mmap>, header: ShardHeader, schema: &SchemaDescriptor) -> Result<Self, StorageError> {
        // SAFETY: `mmap` moves into the handle built here, which keeps the
        // mapping alive, and every read of a slice of it borrows that handle.
        let data: &'static [u8] = unsafe { &*(mmap.as_slice() as *const [u8]) };
        let (file_npc, count) = (header.file_npc, header.row_count);
        let spans = region_spans(data, file_npc)?;

        let pk = direct_region(data, &spans[REG_PK], count, schema.pk_stride())?;
        let weight = WordRegion::bind(data, &spans[REG_WEIGHT], count)?;
        let null_bmp = WordRegion::bind(data, &spans[REG_NULL_BMP], count)?;

        let col_regions = schema
            .payload_columns()
            .map(|(pi, col)| {
                let width = col.size() as usize;
                if pi >= file_npc {
                    debug_assert!(width <= ZERO_CELL.len());
                    return Ok(PayloadRegion::Mapped(ColPtr { base: ZERO_CELL.as_ptr(), stride: 0 }));
                }
                let span = &spans[REG_PAYLOAD_START + pi];
                let image = span.bytes(data);
                let string = col.type_code.is_german_string();
                let image = match span.encoding {
                    Encoding::Raw | Encoding::Constant => {
                        return direct_region(data, span, count, width).map(PayloadRegion::Mapped);
                    }
                    Encoding::Dict => {
                        return DictImage::parse(image, count)
                            .map(PayloadRegion::Dict)
                            .ok_or(Corrupt("region size"));
                    }
                    Encoding::For => {
                        let fi = col.fixed_int().ok_or(Corrupt("encoding"))?;
                        ForImage::parse(image, count, fi.width() - 1).map(PackedImage::For)
                    }
                    Encoding::Seq if string => SeqImage::parse(image, count).map(PackedImage::Seq),
                    Encoding::Sparse if col.nullable && !string => {
                        SparseImage::parse(image, count, width).map(PackedImage::Sparse)
                    }
                    _ => return Err(Corrupt("encoding")),
                };
                Ok(PayloadRegion::Packed(PackedRegion {
                    image: image.ok_or(Corrupt("region size"))?,
                    width,
                    blocks: DecodedBlocks::new(count),
                }))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        let null_pad_mask = gnitz_wire::low_bits_mask(schema.num_payload_cols()) & !gnitz_wire::low_bits_mask(file_npc);

        let [.., blob, filter] = spans.as_slice() else {
            unreachable!("region_spans yields every region plus the filter")
        };
        if blob.encoding != Encoding::Raw || filter.encoding != Encoding::Raw {
            return Err(Corrupt("encoding"));
        }
        let shard_filter = match filter.size {
            0 => None,
            _ => {
                let region = filter.bytes(data);
                if !shard_filter::is_valid(region) {
                    return Err(Corrupt("filter descriptor"));
                }
                Some(region)
            }
        };

        Ok(MappedShard {
            header,
            schema: *schema,
            pk,
            weight,
            null_bmp,
            col_regions,
            null_pad_mask,
            blob: blob.bytes(data),
            shard_filter,
            mmap,
        })
    }

    /// Whether the body still hashes to the checksum the writer recorded.
    pub fn verify_body(&self) -> Result<(), StorageError> {
        self.mmap.advise_sequential();
        let body = &self.mmap.as_slice()[desc_len(self.header.file_npc)..];
        (gnitz_wire::checksum(body) == self.header.body_checksum)
            .then_some(())
            .ok_or(Corrupt("body checksum"))
    }

    /// The schema this handle reads under.
    pub(crate) fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }

    /// Bytes this shard occupies on disk.
    #[inline]
    pub fn file_len(&self) -> u64 {
        self.mmap.as_slice().len() as u64
    }

    /// Rows `first..` of payload column `pi` as they stand in a batch:
    /// `out.len() / width` rows of the shard, at its `width`-byte cells.
    fn decode_payload(&self, pi: usize, width: usize, first: usize, out: &mut [u8]) {
        debug_assert!(first + out.len() / width <= self.header.row_count);
        match &self.col_regions[pi] {
            // SAFETY: the region holds a cell for every row of the shard.
            PayloadRegion::Mapped(cp) => unsafe { cp.copy_rows(first, width, out) },
            PayloadRegion::Dict(dict) => dict.decode(first, width, out),
            PayloadRegion::Packed(p) => self.decode_packed(p, pi, first, out),
        }
    }

    /// Rows `first..` of `p`, payload column `pi`, as they stand in a batch:
    /// `out.len() / p.width` rows.
    fn decode_packed(&self, p: &PackedRegion, pi: usize, first: usize, out: &mut [u8]) {
        match &p.image {
            PackedImage::For(frame) => frame.decode(first, p.width, out),
            PackedImage::Seq(seq) => seq.decode(self.blob, first, out),
            PackedImage::Sparse(sparse) => {
                // From the block's first row on: the decode counts the values before `first`.
                let from = first - first % DECODE_BLOCK_ROWS;
                let rows = first - from + out.len() / p.width;
                // SAFETY: the decode writes every word of `nulls`.
                let mut nulls = unsafe { PooledBuf::uninit(rows * FIXED_REGION_BYTES) };
                self.null_bmp.decode(from, &mut nulls);
                sparse.decode(first, p.width, out, &nulls, pi)
            }
        }
    }

    /// The leading `size` bytes of row `row`'s cell of `p`, payload column `pi`.
    #[inline]
    fn packed_cell<'a>(&'a self, p: &'a PackedRegion, pi: usize, row: usize, size: usize) -> &'a [u8] {
        let decode = |first, out: &mut [u8]| self.decode_packed(p, pi, first, out);
        p.blocks.cell(row, size, (self.header.row_count, p.width), decode)
    }

    /// How many blocks of payload column `pi` per-row reads have decoded.
    #[cfg(test)]
    fn decoded_blocks(&self, pi: usize) -> usize {
        match &self.col_regions[pi] {
            PayloadRegion::Packed(p) => p.blocks.0.iter().filter(|b| b.get().is_some()).count(),
            PayloadRegion::Mapped(_) | PayloadRegion::Dict(_) => 0,
        }
    }

    /// Whether this shard carries a PK filter.
    pub fn has_shard_filter(&self) -> bool {
        self.shard_filter.is_some()
    }

    /// A shard carrying no filter admits every key.
    pub fn shard_filter_may_contain(&self, probe_key: u64) -> bool {
        self.shard_filter
            .is_none_or(|region| shard_filter::may_contain(region, probe_key))
    }

    /// [`seek_lower_bound`](super::seek::seek_lower_bound) over this shard's PKs.
    pub fn find_lower_bound_bytes(&self, key: &[u8]) -> usize {
        unsafe { super::seek::seek_lower_bound(self.header.row_count, self.schema.pk_stride(), self.pk, key) }
    }

    /// [`seek_advance_to`](super::seek::seek_advance_to) over this shard's PKs.
    pub(crate) fn advance_to(&self, key: &[u8], hint: usize) -> usize {
        unsafe { super::seek::seek_advance_to(self.header.row_count, self.schema.pk_stride(), self.pk, key, hint) }
    }

    /// Copy a contiguous slice of rows into a batch under the schema this handle
    /// is bound to, the blob arm picked by [`carried_dead`]. A shard holds no dead
    /// bytes of its own; a carried heap charges the long bytes of the rows outside
    /// the slice.
    #[inline]
    pub fn slice_to_owned_batch(&self, start: usize, row_count: usize) -> Batch {
        let n = self.header.row_count;
        let carried = carried_dead(self.blob().len(), 0, n, row_count, || {
            long_bytes_outside(self, self.schema.string_payload_slots(), &[(start, start + row_count)])
        });
        self.slice_to_owned_batch_with(start, row_count, carried)
    }

    /// [`slice_to_owned_batch`](Self::slice_to_owned_batch) with the blob arm
    /// passed in rather than derived, so both arms can be forced on one slice:
    /// `carried` is the dead-byte bound of a carried heap, `None` relocating.
    fn slice_to_owned_batch_with(&self, start: usize, row_count: usize, carried: Option<usize>) -> Batch {
        assert!(
            !self.header.skeleton,
            "a skeleton shard has no payload to slice; hydrate its keys"
        );
        assert!(start + row_count <= self.header.row_count, "slice out of range");

        let schema = &self.schema;
        let blob = self.blob;
        let relocate = carried.is_none();
        let blob_cap = if relocate {
            prorated_blob_cap(blob.len(), self.header.row_count, row_count)
        } else {
            blob.len()
        };
        let mut batch = write_to_batch(schema, row_count, blob_cap, |w| {
            let (pk, weight, null_bmp) = w.fixed_mut();
            // SAFETY: the assert above keeps the slice inside the shard.
            unsafe { self.pk.copy_rows(start, schema.pk_stride(), pk) };
            self.weight.decode(start, weight);
            self.null_bmp.decode(start, null_bmp);
            if self.null_pad_mask != 0 {
                for word in null_bmp.as_chunks_mut::<8>().0 {
                    *word = (u64::from_le_bytes(*word) | self.null_pad_mask).to_le_bytes();
                }
            }
            for (pi, col) in schema.payload_columns() {
                if relocate && col.type_code.is_german_string() {
                    // The cells as the shard holds them, then each onto this batch's heap.
                    let (cells, heap, mut cache) = w.string_col_mut(pi);
                    self.decode_payload(pi, 16, start, cells);
                    for cell in cells.as_chunks_mut::<16>().0 {
                        *cell = relocate_german_string_vec(&*cell, blob, heap, cache.as_deref_mut());
                    }
                } else {
                    self.decode_payload(pi, col.size() as usize, start, w.col_mut(pi));
                }
            }
            if !relocate {
                let base = w.adopt_heap(blob);
                debug_assert_eq!(base, 0, "a fresh writer's heap is empty");
            }
            w.count = row_count;
        });
        batch.charge_dead(carried.unwrap_or(0));
        batch.certify_consolidated();
        batch
    }
}

impl RowSource for MappedShard {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        debug_assert!(row < self.header.row_count);
        unsafe { self.pk.row(row, self.schema.pk_stride()) }
    }

    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        debug_assert!(row < self.header.row_count);
        self.null_bmp.word(row) | self.null_pad_mask
    }

    #[inline(always)]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        debug_assert!(row < self.header.row_count);
        match &self.col_regions[payload_col] {
            PayloadRegion::Mapped(cp) => unsafe { cp.row(row, col_size) },
            PayloadRegion::Packed(p) => self.packed_cell(p, payload_col, row, col_size),
            PayloadRegion::Dict(d) => &d.cell(row)[..col_size],
        }
    }

    #[inline(always)]
    fn blob(&self) -> &[u8] {
        self.blob
    }

    #[inline(always)]
    fn row_count(&self) -> usize {
        self.header.row_count
    }
}

impl ColumnarSource for MappedShard {
    fn to_unified(
        &self,
        schema: &SchemaDescriptor,
        cols: &mut Vec<ColPtr>,
        window: Range<usize>,
        decoded: &mut DecodedColumns,
    ) -> UnifiedSource<'_> {
        debug_assert!(window.end <= self.header.row_count);
        let cols_off = cols.len();
        cols.extend(
            self.col_regions[..schema.num_payload_cols()]
                .iter()
                .zip(self.schema.payload_columns())
                .map(|(region, (pi, col))| match region {
                    PayloadRegion::Mapped(cp) => *cp,
                    _ => {
                        let w = col.size() as usize;
                        // SAFETY: the decode writes every cell of `column`.
                        let mut column = unsafe { PooledBuf::uninit(window.len() * w) };
                        self.decode_payload(pi, w, window.start, &mut column);
                        // Rebased so that row `window.start` reads the column's first cell.
                        ColPtr {
                            base: decoded.hold(column).wrapping_sub(window.start * w),
                            stride: w,
                        }
                    }
                }),
        );
        let null_bmp = match &self.null_bmp {
            WordRegion::Mapped(cp) => *cp,
            packed => {
                // SAFETY: the decode writes every word of `words`.
                let mut words = unsafe { PooledBuf::uninit(window.len() * FIXED_REGION_BYTES) };
                packed.decode(window.start, &mut words);
                ColPtr {
                    base: decoded.hold(words).wrapping_sub(window.start * FIXED_REGION_BYTES),
                    stride: FIXED_REGION_BYTES,
                }
            }
        };
        UnifiedSource {
            pk: self.pk,
            null_bmp,
            null_pad_mask: self.null_pad_mask,
            cols_off,
            blob: self.blob(),
            heap_at: None,
        }
    }

    #[inline(always)]
    fn get_weight(&self, row: usize) -> i64 {
        debug_assert!(row < self.header.row_count);
        self.weight.word(row) as i64
    }

    #[inline(always)]
    fn is_skeleton(&self) -> bool {
        MappedShard::is_skeleton(self)
    }
}

#[cfg(test)]
#[path = "tests/shard_reader.rs"]
mod tests;
