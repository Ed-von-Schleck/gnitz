//! Memory-mapped columnar shard reader.
//!
//! Opening verifies the header, the directory, and that every region's size and
//! encoding fits the schema, the heap's symbol table and the PK filter included;
//! [`MappedShard::verify_body`] verifies the body.

use std::cell::OnceCell;
use std::ops::Range;
use std::rc::Rc;

use super::codebook::Codebook;
use super::encoding::*;
use super::error::StorageError;
use super::layout::*;
use super::mmap::Mmap;
use super::pk_filter;
use gnitz_wire::{
    FIXED_REGION_BYTES, FixedInt, Heap, PkBuf, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT, RowSource,
    read_u64_le,
};
use gnitz_zset::repr::Batch;
use gnitz_zset::repr::DirectWriter;
use gnitz_zset::repr::PooledBuf;
use gnitz_zset::repr::{ColPtr, seek_lower_bound, seek_pk_rows};
use gnitz_zset::repr::{ColumnarSource, DecodedColumns};
use gnitz_zset::repr::{StringDest, carried_dead, long_bytes_outside, prorated_blob_cap, row_long_bytes};
use gnitz_zset::schema::SchemaDescriptor;
use xorf::{BinaryFuse8Ref, Filter};

use StorageError::Corrupt;

/// A payload-column region.
enum PayloadRegion {
    /// Readable in place: a Raw (stride = width) or Constant (stride 0) region
    /// of the mapping, or zero cells at stride 0 for a column the file predates
    /// (its null bit is set by [`WordRegion::fit_nulls`]).
    Mapped(ColPtr),
    Packed(PackedRegion),
    /// An [`Encoding::Dict`] region of the mapping: a per-row read answers out
    /// of the dictionary in place, a bulk read decodes its own window.
    Dict(DictImage<'static>),
}

/// A payload region read through a decode. A per-row read decodes the block it
/// lands in and keeps it; a bulk read decodes its own window and keeps nothing.
struct PackedRegion {
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

/// The blocks of a column or of a packed key that per-row reads have decoded,
/// one per `DECODE_BLOCK_ROWS` rows.
struct DecodedBlocks(Box<[OnceCell<Box<[u8]>>]>);

impl DecodedBlocks {
    fn new(rows: usize) -> Self {
        DecodedBlocks((0..rows.div_ceil(DECODE_BLOCK_ROWS)).map(|_| OnceCell::new()).collect())
    }

    /// How many blocks per-row reads have decoded.
    #[cfg(test)]
    fn held(&self) -> usize {
        self.0.iter().filter(|b| b.get().is_some()).count()
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

/// The PK region.
enum KeyRegion {
    /// Readable in place: a Raw (stride = `pk_stride`) or Constant (stride 0)
    /// region of the mapping.
    Mapped(ColPtr),
    Packed(Box<PackedKey>),
}

/// An [`Encoding::Key`] region of the mapping. A seek searches the image in
/// place; a per-row read decodes the block it lands in and keeps it, and a bulk
/// read decodes its own window and keeps nothing.
struct PackedKey {
    image: KeyImage<'static>,
    blocks: DecodedBlocks,
}

impl KeyRegion {
    /// The region `span` holds for `count` rows of `schema`'s PK.
    fn bind(data: &'static [u8], span: &Span, count: usize, schema: &SchemaDescriptor) -> Result<Self, StorageError> {
        if span.encoding != Encoding::Key {
            return direct_region(data, span, count, schema.pk_stride()).map(KeyRegion::Mapped);
        }
        let image = KeyImage::parse(span.bytes(data), count, schema).ok_or(Corrupt("region size"))?;
        Ok(KeyRegion::Packed(Box::new(PackedKey {
            image,
            blocks: DecodedBlocks::new(count),
        })))
    }

    /// Rows `start..` as they stand in a batch, `dst.len() / stride` rows of
    /// the shard.
    fn decode(&self, start: usize, stride: usize, dst: &mut [u8]) {
        match self {
            // SAFETY: the region holds a key for every row of the shard.
            KeyRegion::Mapped(cp) => unsafe { cp.copy_rows(start, stride, dst) },
            KeyRegion::Packed(key) => key.image.decode(start, dst),
        }
    }
}

/// A region of one 8-byte word per row: the weights, or the null words. A
/// packed one is a region of the mapping, read per row in place.
enum WordRegion {
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

    /// The words as a column readable in place, where they are not packed.
    fn in_place(&self) -> Option<ColPtr> {
        match self {
            WordRegion::Mapped(cp) => Some(*cp),
            WordRegion::TwoValue(_) | WordRegion::For(_) => None,
        }
    }

    /// These null words of a file of `file_npc` payload columns, `count` rows,
    /// fitted to `schema`: without the bits of columns `schema` lacks, and a
    /// column the file predates NULL. Fitted in the image where it holds the
    /// words by value, else into the words returned, which this then reads.
    fn fit_nulls(&mut self, file_npc: usize, schema: &SchemaDescriptor, count: usize) -> Option<Box<[u8]>> {
        let npc = schema.num_payload_cols();
        let (keep, pad) = (
            schema.payload_slots(),
            schema.payload_slots() & !gnitz_wire::low_bits_mask(file_npc),
        );
        match self {
            _ if file_npc == npc => None,
            WordRegion::TwoValue(two) => {
                two.map_words(|w| (w & keep) | pad);
                None
            }
            // Every file word is below bit `file_npc`, where the pad starts.
            WordRegion::For(frame) if file_npc < npc => {
                frame.set_high_bits(pad);
                None
            }
            _ => {
                // A Constant region stays one word read at stride 0.
                let stride = self.in_place().map_or(FIXED_REGION_BYTES, |cp| cp.stride);
                let rows = if stride == 0 { count.min(1) } else { count };
                let mut words = vec![0u8; rows * FIXED_REGION_BYTES].into_boxed_slice();
                self.decode(0, &mut words);
                for word in words.as_chunks_mut::<8>().0 {
                    *word = ((u64::from_le_bytes(*word) & keep) | pad).to_le_bytes();
                }
                *self = WordRegion::Mapped(ColPtr { base: words.as_ptr(), stride });
                Some(words)
            }
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

/// A shard file mapped read-only and bound to the schema it reads under;
/// every region it reads was checked when it was bound.
pub struct MappedShard {
    /// The mapping the regions below read, shared by every handle
    /// [`rebind`](Self::rebind) derives from this one. Two regions read
    /// elsewhere: a column the file predates reads zero cells at stride 0,
    /// and a `null_bmp` fitted to `schema` may read `_fitted_nulls`.
    mmap: Rc<Mmap>,
    header: ShardHeader,
    schema: SchemaDescriptor,
    pk: KeyRegion,
    weight: WordRegion,
    null_bmp: WordRegion,
    /// Non-PK column regions indexed by payload position, always one per payload
    /// column of `schema`, a column the file predates included.
    col_regions: Vec<PayloadRegion>,
    heap: ShardHeap<'static>,
    /// The validated PK filter, or `None` when the file carries none.
    pk_filter: Option<BinaryFuse8Ref<'static>>,
    /// The words `null_bmp` points into where fitting them to `schema` took a
    /// copy (see [`WordRegion::fit_nulls`]).
    _fitted_nulls: Option<Box<[u8]>>,
}

/// The heap region: the verbatim heap, or the parsed image of one coded under
/// a symbol table.
enum ShardHeap<'a> {
    Plain(&'a [u8]),
    Coded(SymbolsImage<'a>),
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
    /// The header and directory of the shard at `path`, refused wherever
    /// [`MappedShard::open`] would refuse its header, directory, heap or PK
    /// filter; no other region is checked.
    pub fn read(path: &str) -> Result<Self, StorageError> {
        let (mmap, header) = MappedShard::map(path)?;
        let spans = region_spans(mmap.as_slice(), header.file_npc)?;
        frame(mmap.as_slice(), &spans)?;
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

/// The heap and the PK filter of the shard `data`, whose regions `spans`
/// places: the regions no schema binds.
fn frame<'a>(data: &'a [u8], spans: &[Span]) -> Result<(ShardHeap<'a>, Option<BinaryFuse8Ref<'a>>), StorageError> {
    let [.., blob, filter] = spans else {
        unreachable!("region_spans yields every region plus the filter")
    };
    let heap = match blob.encoding {
        Encoding::Raw => ShardHeap::Plain(blob.bytes(data)),
        Encoding::Symbols => ShardHeap::Coded(SymbolsImage::parse(blob.bytes(data)).ok_or(Corrupt("region size"))?),
        _ => return Err(Corrupt("encoding")),
    };
    if filter.encoding != Encoding::Raw {
        return Err(Corrupt("encoding"));
    }
    let pk_filter = match filter.size {
        0 => None,
        _ => Some(pk_filter::parse(filter.bytes(data)).ok_or(Corrupt("filter descriptor"))?),
    };
    Ok((heap, pk_filter))
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

    /// Maps the shard at `path`, advised for huge pages, and binds it to
    /// `schema`, refusing a file whose header, directory or regions do not
    /// check out; the body is not hashed (see [`verify_body`](Self::verify_body)).
    pub fn open(path: &str, schema: &SchemaDescriptor) -> Result<Self, StorageError> {
        let (mmap, header) = Self::map(path)?;
        mmap.advise(libc::MADV_HUGEPAGE);
        Self::bind(Rc::new(mmap), header, schema)
    }

    /// The file at `path` mapped, and its header, once its descriptive prefix
    /// matches the digest stamped over it.
    fn map(path: &str) -> Result<(Mmap, ShardHeader), StorageError> {
        let file = std::fs::File::open(path)?;
        let mmap = Mmap::of_file(&file)?.ok_or(Corrupt("empty file"))?;
        let header = ShardHeader::read_prefix(path, mmap.as_slice())?;
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

        let pk = KeyRegion::bind(data, &spans[REG_PK], count, schema)?;
        let weight = WordRegion::bind(data, &spans[REG_WEIGHT], count)?;
        let mut null_bmp = WordRegion::bind(data, &spans[REG_NULL_BMP], count)?;
        let fitted_nulls = null_bmp.fit_nulls(file_npc, schema, count);

        let col_regions = schema
            .payload_columns()
            .map(|(pi, col)| {
                let width = col.size();
                if pi >= file_npc {
                    return Ok(PayloadRegion::Mapped(ColPtr::zeroes()));
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
                        let fi = FixedInt::from_type_code(col.type_code).ok_or(Corrupt("encoding"))?;
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
        let (heap, pk_filter) = frame(data, &spans)?;

        Ok(MappedShard {
            header,
            schema: *schema,
            pk,
            weight,
            null_bmp,
            _fitted_nulls: fitted_nulls,
            col_regions,
            heap,
            pk_filter,
            mmap,
        })
    }

    /// Whether the body still hashes to the checksum the writer recorded.
    pub fn verify_body(&self) -> Result<(), StorageError> {
        let body = &self.mmap.as_slice()[desc_len(self.header.file_npc)..];
        (gnitz_wire::checksum(body) == self.header.body_checksum)
            .then_some(())
            .ok_or(Corrupt("body checksum"))
    }

    /// Advise the mapping, which every handle rebound from this one shares,
    /// MADV_SEQUENTIAL.
    pub(super) fn advise_sequential(&self) {
        self.mmap.advise(libc::MADV_SEQUENTIAL);
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
            PackedImage::Seq(seq) => seq.decode(self.heap(), first, out),
            PackedImage::Sparse(sparse) => sparse.decode(first, p.width, out, pi, |rows| {
                // SAFETY: the decode writes every word of `nulls`.
                let mut nulls = unsafe { PooledBuf::uninit(rows.len() * FIXED_REGION_BYTES) };
                self.null_bmp.decode(rows.start, &mut nulls);
                nulls
            }),
        }
    }

    /// The leading `size` bytes of row `row`'s cell of `p`, payload column `pi`.
    #[inline]
    fn packed_cell<'a>(&'a self, p: &'a PackedRegion, pi: usize, row: usize, size: usize) -> &'a [u8] {
        let decode = |first, out: &mut [u8]| self.decode_packed(p, pi, first, out);
        p.blocks.cell(row, size, (self.header.row_count, p.width), decode)
    }

    /// Row `row`'s key of `key`, `stride` bytes.
    #[inline]
    fn packed_key<'a>(&'a self, key: &'a PackedKey, row: usize, stride: usize) -> &'a [u8] {
        let decode = |first, out: &mut [u8]| key.image.decode(first, out);
        key.blocks.cell(row, stride, (self.header.row_count, stride), decode)
    }

    /// How many blocks of a packed key per-row reads have decoded.
    #[cfg(test)]
    fn decoded_key_blocks(&self) -> usize {
        match &self.pk {
            KeyRegion::Packed(key) => key.blocks.held(),
            KeyRegion::Mapped(_) => 0,
        }
    }

    /// How many blocks of payload column `pi` per-row reads have decoded.
    #[cfg(test)]
    fn decoded_blocks(&self, pi: usize) -> usize {
        match &self.col_regions[pi] {
            PayloadRegion::Packed(p) => p.blocks.held(),
            PayloadRegion::Mapped(_) | PayloadRegion::Dict(_) => 0,
        }
    }

    /// The heap region, where it is coded.
    pub(crate) fn coded_heap(&self) -> Option<&SymbolsImage<'static>> {
        match &self.heap {
            ShardHeap::Coded(image) => Some(image),
            ShardHeap::Plain(_) => None,
        }
    }

    /// The codebook of the heap's table; `None` for a plain heap or a body that
    /// fails its checksum.
    pub fn codebook(&self) -> Option<Codebook> {
        let image = self.coded_heap()?;
        self.verify_body().ok()?;
        Some(Codebook::rebuilt(image.table.symbols(), image.trained_sixteenths))
    }

    /// Whether this shard carries a PK filter.
    pub fn has_pk_filter(&self) -> bool {
        self.pk_filter.is_some()
    }

    /// A shard carrying no filter admits every key.
    pub fn pk_filter_may_contain(&self, probe_key: u64) -> bool {
        self.pk_filter.as_ref().is_none_or(|filter| filter.contains(&probe_key))
    }

    /// Row `row`'s key by value, decoded alone where the key is packed.
    pub fn pk_at(&self, row: usize) -> PkBuf {
        assert!(row < self.header.row_count);
        let mut pk = PkBuf::zeroed(self.schema.pk_stride());
        self.pk.decode(row, self.schema.pk_stride(), pk.pk_bytes_mut());
        pk
    }

    /// The keys of `rows`, appended to `dst`.
    pub(crate) fn append_pks(&self, rows: Range<usize>, dst: &mut Vec<u8>) {
        assert!(rows.end <= self.header.row_count);
        let (stride, at) = (self.schema.pk_stride(), dst.len());
        dst.resize(at + rows.len() * stride, 0);
        self.pk.decode(rows.start, stride, &mut dst[at..]);
    }

    /// The rows whose PK is the OPK `key`, exactly `pk_stride` bytes: none, at
    /// its lower bound, where the shard holds no such row.
    pub fn pk_rows(&self, key: &[u8]) -> Range<usize> {
        let (count, stride) = (self.header.row_count, self.schema.pk_stride());
        match &self.pk {
            // SAFETY: the region holds a key for every row of the shard.
            KeyRegion::Mapped(cp) => unsafe { seek_pk_rows(count, stride, *cp, key, None) },
            KeyRegion::Packed(packed) => packed.image.rows_of(key),
        }
    }

    /// The first row whose PK is at or after the OPK `key`, galloping from
    /// `hint` where there is one.
    #[inline]
    pub(crate) fn seek(&self, key: &[u8], hint: Option<usize>) -> usize {
        let (count, stride) = (self.header.row_count, self.schema.pk_stride());
        match &self.pk {
            // SAFETY: the region holds a key for every row of the shard.
            KeyRegion::Mapped(cp) => unsafe { seek_lower_bound(count, stride, *cp, key, hint) },
            KeyRegion::Packed(packed) => packed.image.seek(key, hint),
        }
    }

    /// The heap a batch copying the German-string slots `strings` of `rows`
    /// carries whole, with the bytes of it charged dead; `None` relocates each
    /// value. A plain heap is carried into a batch with no codebook where
    /// [`carried_dead`] prefers it; a shard holds no dead bytes of its own, so it
    /// charges the long bytes of the rows outside `rows`, and those of the
    /// string slots outside `strings` in `rows`. A coded heap is carried
    /// into a batch coded under `codebook`, for every row and when its codes are
    /// of that same table, at no dead byte.
    pub(crate) fn carry_for(
        &self,
        rows: Range<usize>,
        strings: u64,
        codebook: Option<&Codebook>,
    ) -> Option<(&[u8], usize)> {
        if strings == 0 {
            return None;
        }
        let n = self.header.row_count;
        match (self.heap(), codebook) {
            (Heap::Plain(blob), None) => {
                // The string slots the batch leaves out are dead in the rows it
                // keeps too.
                let all = self.schema.string_payload_slots();
                let dropped = match all & !strings {
                    0 => 0,
                    d => rows.clone().map(|r| row_long_bytes(self, d, r)).sum(),
                };
                carried_dead(blob.len(), dropped, n, rows.len(), || {
                    long_bytes_outside(self, all, &[(rows.start, rows.end)])
                })
                .map(|dead| (blob, dead))
            }
            (Heap::Coded(c), Some(book)) if rows.len() == n && c.table.digest() == book.table().digest() => {
                Some((c.codes, 0))
            }
            _ => None,
        }
    }

    /// Copy a contiguous slice of rows into a batch under the schema this handle
    /// is bound to, its heap carried where [`Self::carry_for`] carries it. A
    /// coded heap is never carried: its values are decoded into the batch's own.
    /// The payload columns `skip` names, as payload bits, are written as zeroes
    /// that are not NULL, for a reader that never reads them.
    #[inline]
    pub fn slice_to_owned_batch(&self, start: usize, row_count: usize, skip: u64) -> Batch {
        let strings = self.schema.string_payload_slots() & !skip;
        let carried = self.carry_for(start..start + row_count, strings, None);
        self.slice_to_owned_batch_with(start, row_count, carried, skip)
    }

    /// [`slice_to_owned_batch`](Self::slice_to_owned_batch) with the blob arm
    /// passed in rather than derived, so both arms can be forced on one slice:
    /// `carried` is the plain heap carried whole and its dead bytes, `None`
    /// relocating.
    fn slice_to_owned_batch_with(
        &self,
        start: usize,
        row_count: usize,
        carried: Option<(&[u8], usize)>,
        skip: u64,
    ) -> Batch {
        assert!(
            !self.header.skeleton,
            "a skeleton shard has no payload to slice; hydrate its keys"
        );
        assert!(start + row_count <= self.header.row_count, "slice out of range");

        let schema = &self.schema;
        let blob_cap = match (carried, schema.string_payload_slots() & !skip) {
            (Some((heap, _)), _) => heap.len(),
            (None, 0) => 0,
            (None, _) => prorated_blob_cap(self.heap().plain_len(), self.header.row_count, row_count),
        };
        // A carried heap lands first in the fresh one, so its cells stand as
        // the shard holds them.
        let dest = match carried {
            Some(_) => StringDest::Carried(0),
            None => StringDest::Placed(None),
        };
        let carried = carried.map(|(heap, dead)| (heap, 0, dead));
        let mut batch = Batch::write_new_carrying(schema, row_count, blob_cap, carried.as_slice(), |w| {
            self.decode_rows_into(start..start + row_count, w, dest, skip);
        });
        batch.certify_consolidated();
        batch
    }

    /// Decode `rows` into `w`, a writer of exactly that many rows under this
    /// shard's schema, or under its PK-only schema for a skeleton shard, whose
    /// every null word is zero. Each long value's cell is rebased onto the
    /// writer's heap as `dest` says; the payload columns `skip` names are
    /// written as zeroes that are not NULL.
    pub(crate) fn decode_rows_into(
        &self,
        rows: Range<usize>,
        w: &mut DirectWriter<'_>,
        dest: StringDest<'_>,
        skip: u64,
    ) {
        assert!(rows.end <= self.header.row_count, "rows out of range");
        debug_assert_eq!(rows.len(), w.rows(), "a writer of another row count");
        let (schema, start) = (&self.schema, rows.start);
        debug_assert!(
            match self.header.skeleton {
                true => w.schema.num_payload_cols() == 0 && w.schema.pk_stride() == schema.pk_stride(),
                false => w.schema.same_region_types(schema),
            },
            "a writer of another layout"
        );
        let (pk, weight, null_bmp) = w.fixed_mut();
        self.decode_region(REG_PK, start, pk);
        self.decode_region(REG_WEIGHT, start, weight);
        if self.header.skeleton {
            null_bmp.fill(0);
            return;
        }
        self.decode_region(REG_NULL_BMP, start, null_bmp);
        if skip != 0 {
            for word in null_bmp.as_chunks_mut::<8>().0 {
                *word = (u64::from_le_bytes(*word) & !skip).to_le_bytes();
            }
        }
        for (pi, col) in schema.payload_columns() {
            if skip >> pi & 1 == 1 {
                // Zero cells, an empty string's included.
                w.col_mut(pi).fill(0);
                continue;
            }
            self.decode_region(REG_PAYLOAD_START + pi, start, w.col_mut(pi));
            if col.type_code.is_german_string() {
                w.rebase_string_col(pi, self.heap(), dest);
            }
        }
    }

    /// Rows `first..` of region `r` as they stand in a batch, as many as `out`
    /// holds cells of the region's stride.
    fn decode_region(&self, r: usize, first: usize, out: &mut [u8]) {
        match r {
            REG_PK => self.pk.decode(first, self.schema.pk_stride(), out),
            REG_WEIGHT => self.weight.decode(first, out),
            REG_NULL_BMP => self.null_bmp.decode(first, out),
            _ => self.decode_payload(r - REG_PAYLOAD_START, self.schema.region_stride(r), first, out),
        }
    }
}

impl RowSource for MappedShard {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        debug_assert!(row < self.header.row_count);
        let stride = self.schema.pk_stride();
        match &self.pk {
            // SAFETY: the region holds a key for every row of the shard.
            KeyRegion::Mapped(cp) => unsafe { cp.row(row, stride) },
            KeyRegion::Packed(key) => self.packed_key(key, row, stride),
        }
    }

    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        debug_assert!(row < self.header.row_count);
        self.null_bmp.word(row)
    }

    #[inline(always)]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        debug_assert!(row < self.header.row_count);
        match &self.col_regions[payload_col] {
            // SAFETY: the region holds a cell for every row of the shard.
            PayloadRegion::Mapped(cp) => unsafe { cp.row(row, col_size) },
            PayloadRegion::Packed(p) => self.packed_cell(p, payload_col, row, col_size),
            PayloadRegion::Dict(d) => &d.cell(row)[..col_size],
        }
    }

    #[inline(always)]
    fn heap(&self) -> Heap<'_> {
        match &self.heap {
            ShardHeap::Plain(blob) => Heap::Plain(blob),
            ShardHeap::Coded(image) => image.heap(),
        }
    }

    #[inline(always)]
    fn row_count(&self) -> usize {
        self.header.row_count
    }
}

impl MappedShard {
    /// This shard at `rows`, their keys decoded once.
    pub(crate) fn window(&self, rows: Range<usize>) -> ShardWindow<'_> {
        debug_assert!(rows.end <= self.header.row_count);
        let mut decoded = DecodedColumns::default();
        let pk = self.region(REG_PK, &rows, &mut decoded);
        ShardWindow { shard: self, rows, pk, _decoded: decoded }
    }
}

impl ColumnarSource for MappedShard {
    #[inline(always)]
    fn get_weight(&self, row: usize) -> i64 {
        debug_assert!(row < self.header.row_count);
        self.weight.word(row) as i64
    }

    #[inline(always)]
    fn is_skeleton(&self) -> bool {
        MappedShard::is_skeleton(self)
    }

    fn in_place(&self, r: usize) -> Option<ColPtr> {
        match r {
            REG_PK => match &self.pk {
                KeyRegion::Mapped(cp) => Some(*cp),
                KeyRegion::Packed(_) => None,
            },
            REG_WEIGHT => self.weight.in_place(),
            REG_NULL_BMP => self.null_bmp.in_place(),
            _ => match &self.col_regions[r - REG_PAYLOAD_START] {
                PayloadRegion::Mapped(cp) => Some(*cp),
                _ => None,
            },
        }
    }

    fn region(&self, r: usize, window: &Range<usize>, decoded: &mut DecodedColumns) -> ColPtr {
        debug_assert!(window.end <= self.header.row_count);
        self.in_place(r).unwrap_or_else(|| {
            decoded.column(window, self.schema.region_stride(r), |out| {
                self.decode_region(r, window.start, out)
            })
        })
    }
}

/// A shard at the rows of one window, their keys decoded once for the merge
/// that compares them and the scatter that copies them.
pub(crate) struct ShardWindow<'a> {
    shard: &'a MappedShard,
    rows: Range<usize>,
    /// The keys, readable at `rows`.
    pk: ColPtr,
    /// What `pk` reads, where the keys are decoded.
    _decoded: DecodedColumns,
}

impl RowSource for ShardWindow<'_> {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        debug_assert!(self.rows.contains(&row), "a key outside the window");
        // SAFETY: `pk` holds a key for every row of `rows`.
        unsafe { self.pk.row(row, self.shard.schema.pk_stride()) }
    }

    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        self.shard.get_null_word(row)
    }

    #[inline(always)]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        self.shard.get_col_ptr(row, payload_col, col_size)
    }

    #[inline(always)]
    fn heap(&self) -> Heap<'_> {
        self.shard.heap()
    }

    #[inline(always)]
    fn row_count(&self) -> usize {
        self.shard.header.row_count
    }
}

impl ColumnarSource for ShardWindow<'_> {
    #[inline(always)]
    fn get_weight(&self, row: usize) -> i64 {
        self.shard.get_weight(row)
    }

    #[inline(always)]
    fn is_skeleton(&self) -> bool {
        self.shard.is_skeleton()
    }

    fn in_place(&self, r: usize) -> Option<ColPtr> {
        match r {
            REG_PK => Some(self.pk),
            _ => self.shard.in_place(r),
        }
    }

    fn region(&self, r: usize, window: &Range<usize>, decoded: &mut DecodedColumns) -> ColPtr {
        debug_assert!(self.rows.start <= window.start && window.end <= self.rows.end);
        match r {
            REG_PK => self.pk,
            _ => self.shard.region(r, window, decoded),
        }
    }
}

#[cfg(test)]
#[path = "tests/reader.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/reader.rs"]
mod bench;
