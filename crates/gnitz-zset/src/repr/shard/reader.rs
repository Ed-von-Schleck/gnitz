//! Memory-mapped columnar shard reader.
//!
//! Opening verifies the header, the directory, and that every region's size and
//! encoding fits the schema, the heap's symbol table and the PK filter included;
//! [`MappedShard::verify_body`] verifies the body.

use std::cell::OnceCell;
use std::ops::Range;
use std::rc::Rc;

use super::encoding::*;
use super::error::StorageError;
use super::layout::*;
use super::mmap::Mmap;
use super::pk_filter;
use crate::repr::batch::{write_to_batch, Batch};
use crate::repr::scatter::{ColTable, ColumnarSource, DecodedColumns, UnifiedSource};
use crate::repr::seek::{seek_lower_bound, seek_pk_rows, ColPtr};
use crate::repr::string_heap::{carried_dead, long_bytes_outside, prorated_blob_cap};
use crate::repr::writer::DirectWriter;
use crate::repr::PooledBuf;
use crate::schema::SchemaDescriptor;
use gnitz_wire::{
    read_u64_le, FixedInt, Heap, PkBuf, RowSource, FIXED_REGION_BYTES, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK,
    REG_WEIGHT,
};
use xorf::{BinaryFuse8Ref, Filter};

use StorageError::Corrupt;

/// A payload-column region.
enum PayloadRegion {
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

    /// The keys as a column readable at `rows`, decoded into `decoded` where
    /// they are packed.
    fn window(&self, rows: &Range<usize>, stride: usize, decoded: &mut DecodedColumns) -> ColPtr {
        match self {
            KeyRegion::Mapped(cp) => *cp,
            KeyRegion::Packed(key) => decoded.column(rows, stride, |keys| key.image.decode(rows.start, keys)),
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
    /// elsewhere: a column the file predates reads `ZERO_CELL` at stride 0,
    /// and a narrowed `null_bmp` reads `_narrowed_nulls`.
    mmap: Rc<Mmap>,
    header: ShardHeader,
    schema: SchemaDescriptor,
    pk: KeyRegion,
    weight: WordRegion,
    null_bmp: WordRegion,
    /// Non-PK column regions indexed by payload position, always one per payload
    /// column of `schema`, a column the file predates included.
    col_regions: Vec<PayloadRegion>,
    /// The null bits of `schema`'s payload columns past the file's own, OR'd
    /// into every null word read: a column the file predates reads NULL.
    null_pad_mask: u64,
    heap: ShardHeap<'static>,
    /// The validated PK filter, or `None` when the file carries none.
    pk_filter: Option<BinaryFuse8Ref<'static>>,
    /// The words `null_bmp` points into when the file is wider than `schema`:
    /// the file's null words without the bits of the columns `schema` lacks.
    _narrowed_nulls: Option<Box<[u8]>>,
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
        let narrowed_nulls = (file_npc > schema.num_payload_cols()).then(|| {
            let keep = gnitz_wire::low_bits_mask(schema.num_payload_cols());
            let mut words = vec![0u8; count * FIXED_REGION_BYTES].into_boxed_slice();
            null_bmp.decode(0, &mut words);
            for word in words.as_chunks_mut::<8>().0 {
                *word = (u64::from_le_bytes(*word) & keep).to_le_bytes();
            }
            null_bmp = WordRegion::Mapped(ColPtr {
                base: words.as_ptr(),
                stride: FIXED_REGION_BYTES,
            });
            words
        });

        let col_regions = schema
            .payload_columns()
            .map(|(pi, col)| {
                let width = col.size();
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
        let null_pad_mask = gnitz_wire::low_bits_mask(schema.num_payload_cols()) & !gnitz_wire::low_bits_mask(file_npc);

        let (heap, pk_filter) = frame(data, &spans)?;

        Ok(MappedShard {
            header,
            schema: *schema,
            pk,
            weight,
            null_bmp,
            _narrowed_nulls: narrowed_nulls,
            col_regions,
            null_pad_mask,
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

    /// Copy a contiguous slice of rows into a batch under the schema this handle
    /// is bound to, the blob arm picked by [`carried_dead`]. A shard holds no dead
    /// bytes of its own; a carried heap charges the long bytes of the rows outside
    /// the slice. A coded heap is never carried: its values are decoded into the
    /// batch's own.
    #[inline]
    pub fn slice_to_owned_batch(&self, start: usize, row_count: usize) -> Batch {
        let n = self.header.row_count;
        let carried = self.heap().plain().and_then(|blob| {
            carried_dead(blob.len(), 0, n, row_count, || {
                long_bytes_outside(self, self.schema.string_payload_slots(), &[(start, start + row_count)])
            })
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
        let heap = self.heap();
        let relocate = carried.is_none();
        let blob_cap = if relocate {
            prorated_blob_cap(heap.plain_len(), self.header.row_count, row_count)
        } else {
            heap.plain_len()
        };
        let mut batch = write_to_batch(schema, row_count, blob_cap, |w| {
            // A carried heap lands first in the fresh one, so its cells stand as
            // the shard holds them.
            self.decode_rows_into(start..start + row_count, w, !relocate);
        });
        batch.charge_dead(carried.unwrap_or(0));
        batch.certify_consolidated();
        batch
    }

    /// Decode `rows` into `w`, a writer of exactly that many rows under this
    /// shard's schema. `carry` appends the shard's plain heap whole to the
    /// writer's and shifts the copied cells onto it; otherwise each long value
    /// is relocated, a coded one decoded.
    pub(crate) fn decode_rows_into(&self, rows: Range<usize>, w: &mut DirectWriter<'_>, carry: bool) {
        assert!(
            !self.header.skeleton,
            "a skeleton shard has no payload to decode; hydrate its keys"
        );
        assert!(rows.end <= self.header.row_count, "rows out of range");
        debug_assert_eq!(rows.len(), w.rows(), "a writer of another row count");
        debug_assert!(w.schema.same_region_types(&self.schema), "a writer of another layout");
        let (schema, heap, start) = (&self.schema, self.heap(), rows.start);
        let (pk, weight, null_bmp) = w.fixed_mut();
        self.pk.decode(start, schema.pk_stride(), pk);
        self.weight.decode(start, weight);
        self.null_bmp.decode(start, null_bmp);
        if self.null_pad_mask != 0 {
            for word in null_bmp.as_chunks_mut::<8>().0 {
                *word = (u64::from_le_bytes(*word) | self.null_pad_mask).to_le_bytes();
            }
        }
        let heap_at = carry.then(|| w.adopt_heap(heap.plain().expect("only a plain heap is carried")));
        for (pi, col) in schema.payload_columns() {
            self.decode_payload(pi, col.size(), start, w.col_mut(pi));
            if col.type_code.is_german_string() {
                w.rebase_string_col(pi, heap, heap_at);
            }
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
        self.null_bmp.word(row) | self.null_pad_mask
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
        let pk = self.pk.window(&rows, self.schema.pk_stride(), &mut decoded);
        ShardWindow { shard: self, rows, pk, _decoded: decoded }
    }

    /// [`ColumnarSource::to_unified`] over `pk`, a column of this shard's keys
    /// readable at `window`.
    fn unified(
        &self,
        pk: ColPtr,
        schema: &SchemaDescriptor,
        cols: &mut ColTable,
        window: Range<usize>,
        decoded: &mut DecodedColumns,
    ) -> UnifiedSource<'_> {
        debug_assert!(window.end <= self.header.row_count);
        let cols_off = cols.len();
        cols.extend(
            self.schema
                .payload_columns()
                .take(schema.num_payload_cols())
                .map(|(pi, col)| {
                    self.direct_col(pi).unwrap_or_else(|| {
                        decoded.column(&window, col.size(), |column| {
                            self.decode_payload(pi, col.size(), window.start, column)
                        })
                    })
                }),
        );
        let null_bmp = match &self.null_bmp {
            WordRegion::Mapped(cp) => *cp,
            packed => decoded.column(&window, FIXED_REGION_BYTES, |words| packed.decode(window.start, words)),
        };
        UnifiedSource {
            pk,
            null_bmp,
            null_pad_mask: self.null_pad_mask,
            cols_off,
            heap: self.heap(),
            heap_at: None,
        }
    }
}

impl ColumnarSource for MappedShard {
    fn to_unified(
        &self,
        schema: &SchemaDescriptor,
        cols: &mut ColTable,
        window: Range<usize>,
        decoded: &mut DecodedColumns,
    ) -> UnifiedSource<'_> {
        let pk = self.pk.window(&window, schema.pk_stride(), decoded);
        self.unified(pk, schema, cols, window, decoded)
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

    fn direct_col(&self, pi: usize) -> Option<ColPtr> {
        match &self.col_regions[pi] {
            PayloadRegion::Mapped(cp) => Some(*cp),
            _ => None,
        }
    }

    fn direct_fixed(&self) -> Option<[ColPtr; 2]> {
        match &self.pk {
            KeyRegion::Mapped(pk) => Some([*pk, self.direct_nulls()?]),
            KeyRegion::Packed(_) => None,
        }
    }
}

impl MappedShard {
    /// The null words as a [`ColPtr`] readable in place, where they are stored
    /// as the row accessor reads them: unpacked, and no column padded NULL.
    fn direct_nulls(&self) -> Option<ColPtr> {
        match &self.null_bmp {
            WordRegion::Mapped(cp) if self.null_pad_mask == 0 => Some(*cp),
            _ => None,
        }
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
    fn to_unified(
        &self,
        schema: &SchemaDescriptor,
        cols: &mut ColTable,
        window: Range<usize>,
        decoded: &mut DecodedColumns,
    ) -> UnifiedSource<'_> {
        debug_assert!(self.rows.start <= window.start && window.end <= self.rows.end);
        self.shard.unified(self.pk, schema, cols, window, decoded)
    }

    #[inline(always)]
    fn get_weight(&self, row: usize) -> i64 {
        self.shard.get_weight(row)
    }

    #[inline(always)]
    fn is_skeleton(&self) -> bool {
        self.shard.is_skeleton()
    }

    fn direct_col(&self, pi: usize) -> Option<ColPtr> {
        self.shard.direct_col(pi)
    }

    fn direct_fixed(&self) -> Option<[ColPtr; 2]> {
        Some([self.pk, self.shard.direct_nulls()?])
    }
}

#[cfg(test)]
#[path = "tests/reader.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/reader.rs"]
mod bench;
