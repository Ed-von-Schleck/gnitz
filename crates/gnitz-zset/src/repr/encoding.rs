//! The encodings a shard region may carry: for each, the image the writer
//! builds and the parsed form the reader decodes it through.
//!
//! A parse checks an image's size against the row count it is read for. The
//! bytes inside an image are body bytes no open verifies, so no decode trusts
//! them: a code, a length or a rank that points outside its image reads the
//! last entry, the empty string or zero.

use gnitz_wire::{
    read_u32_le, read_u64_le, write_u32_le, write_u64_le, FixedInt, GERMAN_INLINE_OFF, SHORT_STRING_THRESHOLD,
};

gnitz_wire::wire_enum! {
    /// A directory entry's encoding byte.
    pub(crate) enum Encoding: u8 {
        Raw = 0,
        /// One element, read at stride 0.
        Constant = 1,
        /// A weight or null region of exactly two distinct words.
        TwoValue = 2,
        /// Frame-of-reference: a payload column of a 2-, 4- or 8-byte integer type,
        /// or a weight or null region of three or more distinct words.
        For = 3,
        /// Dictionary: a payload column as its distinct cells and one code per row.
        Dict = 4,
        /// Sequential: a string column of values that do not repeat, as one length
        /// per row.
        Seq = 5,
        /// Sparse: a nullable payload column, not a string's, as its non-NULL cells.
        Sparse = 6,
    }
}

impl Encoding {
    /// The encoding's name in a disk-usage report.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Encoding::Raw => "raw",
            Encoding::Constant => "constant",
            Encoding::TwoValue => "two-value",
            Encoding::For => "for",
            Encoding::Dict => "dict",
            Encoding::Seq => "seq",
            Encoding::Sparse => "sparse",
        }
    }
}

// A TwoValue image: `value_a`, `value_b` (u64 LE), then one bit per row.
const TWO_VALUE_A_AT: usize = 0;
const TWO_VALUE_B_AT: usize = 8;
const TWO_VALUE_BITS_AT: usize = 16;

/// A word region's TwoValue image, bit *i* set ⇔ row *i* holds `value_b`; or
/// `None` at a third distinct word.
pub(crate) fn two_value_encode(src: &[u8]) -> Option<Vec<u8>> {
    let n = src.len() / 8;
    let first = read_u64_le(src, 0);
    // Rows before the first second value hold `first`: their bits start 0.
    let mut second: Option<(u64, Vec<u8>)> = None;
    for i in 1..n {
        let v = read_u64_le(src, i * 8);
        if v == first {
            continue;
        }
        let (b, image) = second.get_or_insert_with(|| {
            let mut image = vec![0u8; TWO_VALUE_BITS_AT + n.div_ceil(8)];
            write_u64_le(&mut image, TWO_VALUE_A_AT, first);
            write_u64_le(&mut image, TWO_VALUE_B_AT, v);
            (v, image)
        });
        if v != *b {
            return None;
        }
        image[TWO_VALUE_BITS_AT + i / 8] |= 1 << (i % 8);
    }
    second.map(|(_, image)| image)
}

/// A TwoValue image taken apart.
#[derive(Clone, Copy)]
pub(crate) struct TwoValueImage<'a> {
    /// `value_a` and `value_b`.
    words: [u64; 2],
    bits: &'a [u8],
}

impl<'a> TwoValueImage<'a> {
    /// `image` as the two words of `count` rows, or `None` unless it is that size.
    pub(crate) fn parse(image: &'a [u8], count: usize) -> Option<Self> {
        (image.len() == TWO_VALUE_BITS_AT + count.div_ceil(8)).then(|| TwoValueImage {
            words: [read_u64_le(image, TWO_VALUE_A_AT), read_u64_le(image, TWO_VALUE_B_AT)],
            bits: &image[TWO_VALUE_BITS_AT..],
        })
    }

    /// Row `row`'s word.
    #[inline(always)]
    pub(crate) fn at(&self, row: usize) -> u64 {
        self.words[(self.bits[row / 8] >> (row % 8)) as usize & 1]
    }

    /// Decode rows `first_row..` to their words, `out.len() / 8` rows.
    pub(crate) fn decode(&self, first_row: usize, out: &mut [u8]) {
        let cells = out.as_chunks_mut::<8>().0;
        // The rows up to a byte of bits, the bytes at eight rows each, the rows left.
        let (head, rest) = cells.split_at_mut(cells.len().min(first_row.wrapping_neg() % 8));
        for (i, cell) in head.iter_mut().enumerate() {
            *cell = self.at(first_row + i).to_le_bytes();
        }
        let whole = first_row + head.len();
        let (groups, tail) = rest.as_chunks_mut::<8>();
        let ([a, b], bytes) = (self.words, &self.bits[whole / 8..][..groups.len()]);
        for (group, &byte) in groups.iter_mut().zip(bytes) {
            for (k, cell) in group.iter_mut().enumerate() {
                *cell = (a ^ ((a ^ b) & ((byte >> k) as u64 & 1).wrapping_neg())).to_le_bytes();
            }
        }
        let whole = whole + groups.len() * 8;
        for (i, cell) in tail.iter_mut().enumerate() {
            *cell = self.at(whole + i).to_le_bytes();
        }
    }
}

// A FoR image: the frame reference (u64 LE), then each row's `value − reference`
// in `bw` bytes, then zero slack so the last row also loads as one u64.
const FOR_REFERENCE_AT: usize = 0;
const FOR_CELLS_AT: usize = 8;
const FOR_SLACK: usize = size_of::<u64>() - 1;

const fn for_cell_at(row: usize, bw: usize) -> usize {
    FOR_CELLS_AT + row * bw
}

pub(crate) const fn for_image_len(count: usize, bw: usize) -> usize {
    for_cell_at(count, bw) + FOR_SLACK
}

/// The FoR image of a fixed-int region, framed on its minimum; or `None` unless
/// it is smaller than the region.
pub(crate) fn for_encode(src: &[u8], fi: FixedInt) -> Option<Vec<u8>> {
    gnitz_wire::for_each_fixed_int!(fi, |FI| {
        const W: usize = FI.width();
        let cells = src.as_chunks::<W>().0;
        let widen = |cell: &[u8; W]| FI.decode_le_i64(cell) as u64;
        // XOR with the sign bit maps i64 order onto u64 order, so one unsigned
        // min/max serves both signednesses.
        let bias = if FI.is_signed() { 1u64 << 63 } else { 0 };
        let (mut min, mut max) = (u64::MAX, 0u64);
        for cell in cells {
            let b = widen(cell) ^ bias;
            min = min.min(b);
            max = max.max(b);
        }
        let reference = min ^ bias;
        let bw = for_bw((max ^ bias).wrapping_sub(reference));
        (bw > 0 && for_image_len(cells.len(), bw) < src.len())
            .then(|| for_image(cells.len(), cells.iter().map(widen), reference, bw))
    })
}

/// The fewest bytes that hold every offset up to `max_offset`.
const fn for_bw(max_offset: u64) -> usize {
    ((u64::BITS - max_offset.leading_zeros()) as usize).div_ceil(8)
}

/// The FoR image of `count` `values`, framed on `reference` at `bw` bytes each.
fn for_image(count: usize, values: impl Iterator<Item = u64>, reference: u64, bw: usize) -> Vec<u8> {
    let mut image = vec![0u8; for_image_len(count, bw)];
    write_u64_le(&mut image, FOR_REFERENCE_AT, reference);
    // A whole u64 per row: the next row overwrites the excess, the slack takes the last row's.
    for (row, value) in values.enumerate() {
        write_u64_le(&mut image, for_cell_at(row, bw), value.wrapping_sub(reference));
    }
    image
}

/// A FoR image taken apart.
#[derive(Clone, Copy)]
pub(crate) struct ForImage<'a> {
    image: &'a [u8],
    reference: u64,
    /// The low `bw` bytes of a loaded u64.
    mask: u64,
    bw: usize,
}

impl<'a> ForImage<'a> {
    /// `image` as the frame of `count` rows, or `None` unless some offset width
    /// in `1..=max_bw` gives exactly that size.
    pub(crate) fn parse(image: &'a [u8], count: usize, max_bw: usize) -> Option<Self> {
        let bw = image.len().checked_sub(FOR_CELLS_AT + FOR_SLACK)?.checked_div(count)?;
        ((1..=max_bw).contains(&bw) && image.len() == for_image_len(count, bw)).then(|| ForImage {
            image,
            reference: read_u64_le(image, FOR_REFERENCE_AT),
            mask: gnitz_wire::low_bits_mask(8 * bw),
            bw,
        })
    }

    /// Row `row`'s value, widened to 64 bits.
    #[inline(always)]
    pub(crate) fn at(&self, row: usize) -> u64 {
        let packed = u64::from_le_bytes(*self.image[for_cell_at(row, self.bw)..].first_chunk().unwrap());
        (packed & self.mask).wrapping_add(self.reference)
    }

    /// Decode rows `first_row..` back to their raw little-endian `width`-byte
    /// cells, `out.len() / width` rows.
    pub(crate) fn decode(&self, first_row: usize, width: usize, out: &mut [u8]) {
        match width {
            2 => self.decode_cells::<2>(first_row, out),
            4 => self.decode_cells::<4>(first_row, out),
            8 => self.decode_cells::<8>(first_row, out),
            _ => unreachable!("a framed cell is 2, 4 or 8 bytes"),
        }
    }

    fn decode_cells<const W: usize>(&self, first_row: usize, out: &mut [u8]) {
        let ForImage { image, reference, mask, bw } = *self;
        let cells = out.as_chunks_mut::<W>().0;
        let Some(last) = cells.len().checked_sub(1) else {
            return;
        };
        assert!(for_cell_at(first_row + last, bw) + size_of::<u64>() <= image.len());
        for (i, cell) in cells.iter_mut().enumerate() {
            // SAFETY: row `first_row + i` is at most `first_row + last`, whose load the assert bounds.
            let packed = unsafe {
                image
                    .as_ptr()
                    .add(for_cell_at(first_row + i, bw))
                    .cast::<u64>()
                    .read_unaligned()
            };
            let v = (u64::from_le(packed) & mask).wrapping_add(reference);
            *cell = *v.to_le_bytes().first_chunk::<W>().unwrap();
        }
    }
}

// A Dict image: the entry count (u64 LE), the 16-byte entries — a narrower cell
// zero-padded — then each row's code in the fewest bits that tell the entries
// apart, row 0 in the lowest, then zero slack so the last code also loads as one
// u32.
const DICT_ENTRIES_AT: usize = 8;
const DICT_SLACK: usize = size_of::<u32>() - 1;

/// The most entries a dictionary holds: what a 16-bit code addresses, which at
/// any bit offset in its byte still lies inside one u32.
pub(crate) const DICT_MAX_ENTRIES: usize = 1 << 16;

/// Bits per code of a dictionary of `entries`.
const fn dict_code_bits(entries: usize) -> usize {
    match entries {
        0..=2 => 1,
        n => (usize::BITS - (n - 1).leading_zeros()) as usize,
    }
}

pub(crate) const fn dict_image_len(count: usize, entries: usize) -> usize {
    DICT_ENTRIES_AT + entries * 16 + (count * dict_code_bits(entries)).div_ceil(8) + DICT_SLACK
}

/// The Dict image of a column whose row `i` holds `entries[ids[i]]`.
pub(crate) fn dict_encode(entries: &[[u8; 16]], ids: &[u32]) -> Vec<u8> {
    assert!((1..=DICT_MAX_ENTRIES).contains(&entries.len()));
    let mut image = Vec::with_capacity(dict_image_len(ids.len(), entries.len()));
    image.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    image.extend_from_slice(entries.as_flattened());
    let (codes_at, bits) = (image.len(), dict_code_bits(entries.len()));
    image.resize(dict_image_len(ids.len(), entries.len()), 0);
    for (row, &id) in ids.iter().enumerate() {
        let at = row * bits;
        let word = codes_at + at / 8;
        let code = read_u32_le(&image, word) | id << (at % 8);
        write_u32_le(&mut image, word, code);
    }
    image
}

/// A Dict image taken apart: its entries, and its codes at their width.
#[derive(Clone, Copy)]
pub(crate) struct DictImage<'a> {
    entries: &'a [[u8; 16]],
    codes: &'a [u8],
    bits: usize,
}

impl<'a> DictImage<'a> {
    /// `image` as the dictionary of `count` rows, or `None` unless its entry
    /// count gives exactly that size.
    pub(crate) fn parse(image: &'a [u8], count: usize) -> Option<Self> {
        let n = usize::try_from(read_u64_le(image.get(..DICT_ENTRIES_AT)?, 0)).ok()?;
        if !(1..=DICT_MAX_ENTRIES).contains(&n) || image.len() != dict_image_len(count, n) {
            return None;
        }
        let (entries, codes) = image[DICT_ENTRIES_AT..].split_at(n * 16);
        Some(DictImage {
            entries: entries.as_chunks().0,
            codes,
            bits: dict_code_bits(n),
        })
    }

    /// Row `row`'s cell. A code past the last entry reads the last.
    #[inline(always)]
    pub(crate) fn cell(&self, row: usize) -> &'a [u8; 16] {
        let at = row * self.bits;
        let word = u32::from_le_bytes(*self.codes[at / 8..].first_chunk().unwrap());
        let code = (word >> (at % 8)) as usize & ((1 << self.bits) - 1);
        // SAFETY: `parse` admits no empty dictionary, and the index is clamped into it.
        unsafe { self.entries.get_unchecked(code.min(self.entries.len() - 1)) }
    }

    /// Decode rows `first_row..` to their `width`-byte cells, `out.len() / width`
    /// rows.
    pub(crate) fn decode(&self, first_row: usize, width: usize, out: &mut [u8]) {
        match width {
            1 => self.decode_cells::<1>(first_row, out),
            2 => self.decode_cells::<2>(first_row, out),
            4 => self.decode_cells::<4>(first_row, out),
            8 => self.decode_cells::<8>(first_row, out),
            16 => self.decode_cells::<16>(first_row, out),
            _ => unreachable!("a fixed-width cell is 1, 2, 4, 8 or 16 bytes"),
        }
    }

    fn decode_cells<const W: usize>(&self, first_row: usize, out: &mut [u8]) {
        let cells = out.as_chunks_mut::<W>().0;
        let Some(last) = cells.len().checked_sub(1) else {
            return;
        };
        assert!((first_row + last) * self.bits / 8 + size_of::<u32>() <= self.codes.len());
        let (mask, top) = ((1u32 << self.bits) - 1, self.entries.len() - 1);
        for (i, cell) in cells.iter_mut().enumerate() {
            let at = (first_row + i) * self.bits;
            // SAFETY: row `first_row + i` is at most `first_row + last`, whose load the assert bounds.
            let word = unsafe { self.codes.as_ptr().add(at / 8).cast::<u32>().read_unaligned() };
            let code = ((u32::from_le(word) >> (at % 8)) & mask) as usize;
            // SAFETY: `parse` admits no empty dictionary, and the index is clamped into it.
            *cell = *unsafe { self.entries.get_unchecked(code.min(top)) }
                .first_chunk()
                .unwrap();
        }
    }
}

/// Rows per block of a packed payload column: what a per-row read decodes and
/// keeps, sized so that a block of an I64 column is one page, and the spacing of
/// the entry points a Seq or Sparse image holds, so that a block decodes from
/// one.
pub(crate) const DECODE_BLOCK_ROWS: usize = 512;

// A Seq image: a string column as its lengths alone, its content lying in row
// order — a long value in the shard's heap, a short one in the image's own pool.
// The pool's size (u64 LE); per block, where its long content starts in the heap
// and its short content in the pool (u64 LE each); the FoR image of the lengths;
// the pool, then zero slack so that its last value loads as the twelve bytes a
// short cell holds.
const SEQ_BLOCKS_AT: usize = 8;
const SEQ_BLOCK_ENTRY: usize = 16;
const SEQ_POOL_SLACK: usize = SHORT_STRING_THRESHOLD;
/// A length is a u32, framed at any width up to its own.
const SEQ_MAX_BW: usize = size_of::<u32>();

/// Where a Seq image of `count` rows holds its lengths.
const fn seq_lens_at(count: usize) -> usize {
    SEQ_BLOCKS_AT + count.div_ceil(DECODE_BLOCK_ROWS) * SEQ_BLOCK_ENTRY
}

/// Bytes per length of a column whose lengths span `min..=max`: one even where
/// they are all the same, so that the lengths are a FoR image like any other.
const fn seq_bw(min: usize, max: usize) -> usize {
    match for_bw((max - min) as u64) {
        0 => 1,
        bw => bw,
    }
}

/// The size of the Seq image of `count` rows whose lengths span `min..=max`
/// and whose short values take `pool` bytes.
pub(crate) const fn seq_image_len(count: usize, (min, max): (usize, usize), pool: usize) -> usize {
    seq_lens_at(count) + for_image_len(count, seq_bw(min, max)) + pool + SEQ_POOL_SLACK
}

/// The Seq image of a column of `count` rows whose contents `content` yields in
/// row order, their lengths spanning `min..=max`. The long ones are appended to
/// `heap`.
pub(crate) fn seq_encode<'a>(
    count: usize,
    (min, max): (usize, usize),
    content: impl Iterator<Item = &'a [u8]> + Clone,
    heap: &mut Vec<u8>,
) -> Vec<u8> {
    let mut image = vec![0u8; seq_lens_at(count)];
    let lens = content.clone().map(|c| c.len() as u64);
    image.extend_from_slice(&for_image(count, lens, min as u64, seq_bw(min, max)));
    let pool_at = image.len();
    for (row, c) in content.enumerate() {
        if row % DECODE_BLOCK_ROWS == 0 {
            let entry = SEQ_BLOCKS_AT + row / DECODE_BLOCK_ROWS * SEQ_BLOCK_ENTRY;
            let pooled = image.len() - pool_at;
            write_u64_le(&mut image, entry, heap.len() as u64);
            write_u64_le(&mut image, entry + 8, pooled as u64);
        }
        if c.len() > SHORT_STRING_THRESHOLD {
            heap.extend_from_slice(c);
        } else {
            image.extend_from_slice(c);
        }
    }
    let pooled = image.len() - pool_at;
    write_u64_le(&mut image, 0, pooled as u64);
    image.resize(image.len() + SEQ_POOL_SLACK, 0);
    image
}

/// A Seq image taken apart.
#[derive(Clone, Copy)]
pub(crate) struct SeqImage<'a> {
    blocks: &'a [[u8; SEQ_BLOCK_ENTRY]],
    lens: ForImage<'a>,
    /// The pool and its slack.
    pool: &'a [u8],
}

impl<'a> SeqImage<'a> {
    /// `image` as the Seq image of `count` rows, or `None` unless its pool size
    /// and some length width give exactly that size.
    pub(crate) fn parse(image: &'a [u8], count: usize) -> Option<Self> {
        let pool = usize::try_from(read_u64_le(image.get(..SEQ_BLOCKS_AT)?, 0)).ok()?;
        let (blocks, rest) = image[SEQ_BLOCKS_AT..].split_at_checked(seq_lens_at(count) - SEQ_BLOCKS_AT)?;
        let (lens, pool) = rest.split_at_checked(rest.len().checked_sub(pool.checked_add(SEQ_POOL_SLACK)?)?)?;
        Some(SeqImage {
            blocks: blocks.as_chunks().0,
            lens: ForImage::parse(lens, count, SEQ_MAX_BW)?,
            pool,
        })
    }

    /// Decode rows `first_row..` to their cells over `heap`, `out.len() / 16`
    /// rows. A value the lengths place past the heap or the pool reads as the
    /// empty string.
    pub(crate) fn decode(&self, heap: &[u8], first_row: usize, out: &mut [u8]) {
        let cells = out.as_chunks_mut::<16>().0;
        if cells.is_empty() {
            return;
        }
        let SeqImage { blocks, lens, pool } = *self;
        let block = first_row / DECODE_BLOCK_ROWS;
        let start = |at| usize::try_from(read_u64_le(&blocks[block], at)).unwrap_or(usize::MAX);
        let (mut heap_at, mut pool_at) = (start(0), start(8));
        let len = |row| lens.at(row) as u32 as usize;
        // The value of `len` bytes at `*at`, which it moves past.
        let step = |at: &mut usize, len: usize| std::mem::replace(at, at.saturating_add(len));
        for row in block * DECODE_BLOCK_ROWS..first_row {
            match len(row) {
                long if long > SHORT_STRING_THRESHOLD => step(&mut heap_at, long),
                short => step(&mut pool_at, short),
            };
        }
        for (i, cell) in cells.iter_mut().enumerate() {
            let len = len(first_row + i);
            let mut image = [0u8; 16];
            if len > SHORT_STRING_THRESHOLD {
                let at = step(&mut heap_at, len);
                let value = heap.get(at..).filter(|rest| rest.len() >= len);
                if let Some(prefix) = value.and_then(|v| v.first_chunk::<4>()) {
                    image[..4].copy_from_slice(&(len as u32).to_le_bytes());
                    image[4..8].copy_from_slice(prefix);
                    image[8..].copy_from_slice(&(at as u64).to_le_bytes());
                }
            } else if let Some(value) = pool
                .get(step(&mut pool_at, len)..)
                .and_then(|v| v.first_chunk::<SEQ_POOL_SLACK>())
            {
                // Twelve bytes whatever the length: the mask drops the next values'.
                image[GERMAN_INLINE_OFF..].copy_from_slice(value);
                let keep = u128::MAX >> (128 - 8 * (GERMAN_INLINE_OFF + len));
                image = (u128::from_le_bytes(image) & keep | len as u128).to_le_bytes();
            }
            *cell = image;
        }
    }
}

// A Sparse image: a nullable column as its non-NULL cells alone. The non-NULL
// row count (u64 LE); per block, the non-NULL rows before it (u32 LE); the
// values in row order — the FoR image of them where that is the smaller, else
// the cells themselves.
const SPARSE_RANKS_AT: usize = 8;
const SPARSE_RANK_ENTRY: usize = size_of::<u32>();

/// The Sparse image of a region of `width`-byte cells whose row `r` is NULL
/// iff `is_null(r)`, its values framed where `fi` admits a frame smaller than
/// they are.
pub(crate) fn sparse_encode(
    src: &[u8],
    width: usize,
    fi: Option<FixedInt>,
    is_null: impl Fn(usize) -> bool,
) -> Vec<u8> {
    let n = src.len() / width;
    let mut image = vec![0u8; SPARSE_RANKS_AT + n.div_ceil(DECODE_BLOCK_ROWS) * SPARSE_RANK_ENTRY];
    let mut values = Vec::new();
    for (row, cell) in src.chunks_exact(width).enumerate() {
        if row % DECODE_BLOCK_ROWS == 0 {
            let entry = SPARSE_RANKS_AT + row / DECODE_BLOCK_ROWS * SPARSE_RANK_ENTRY;
            write_u32_le(&mut image, entry, (values.len() / width) as u32);
        }
        if !is_null(row) {
            values.extend_from_slice(cell);
        }
    }
    write_u64_le(&mut image, 0, (values.len() / width) as u64);
    let framed = fi.and_then(|fi| for_encode(&values, fi));
    image.extend_from_slice(framed.as_ref().unwrap_or(&values));
    image
}

/// A Sparse image taken apart.
#[derive(Clone, Copy)]
pub(crate) struct SparseImage<'a> {
    ranks: &'a [[u8; SPARSE_RANK_ENTRY]],
    values: SparseValues<'a>,
    /// How many values there are.
    held: usize,
}

#[derive(Clone, Copy)]
enum SparseValues<'a> {
    Cells(&'a [u8]),
    Framed(ForImage<'a>),
}

impl<'a> SparseImage<'a> {
    /// `image` as the Sparse image of `count` rows of `width`-byte cells, or
    /// `None` unless its value count gives exactly that size, as cells or under
    /// some frame.
    pub(crate) fn parse(image: &'a [u8], count: usize, width: usize) -> Option<Self> {
        let held = usize::try_from(read_u64_le(image.get(..SPARSE_RANKS_AT)?, 0)).ok()?;
        let ranks = count.div_ceil(DECODE_BLOCK_ROWS) * SPARSE_RANK_ENTRY;
        let (ranks, values) = image[SPARSE_RANKS_AT..].split_at_checked(ranks)?;
        // `sparse_encode` frames only values a frame shrinks, so cells of a frame's size are cells.
        let values = if held.checked_mul(width)? == values.len() {
            SparseValues::Cells(values)
        } else {
            // A frame widens its offsets to a u64, which a wider cell is not read from.
            let max_bw = if width <= size_of::<u64>() { width - 1 } else { 0 };
            SparseValues::Framed(ForImage::parse(values, held, max_bw)?)
        };
        (held <= count).then_some(SparseImage { ranks: ranks.as_chunks().0, values, held })
    }

    /// Decode rows `first_row..` to their `width`-byte cells, `out.len() /
    /// width` rows: a NULL row's is zero. `nulls` is the null words from the
    /// first row of `first_row`'s block on, of which bit `pi` is this column's.
    /// A row they place past the last value reads zero too.
    pub(crate) fn decode(&self, first_row: usize, width: usize, out: &mut [u8], nulls: &[u8], pi: usize) {
        match width {
            1 => self.decode_cells::<1>(first_row, out, nulls, pi),
            2 => self.decode_cells::<2>(first_row, out, nulls, pi),
            4 => self.decode_cells::<4>(first_row, out, nulls, pi),
            8 => self.decode_cells::<8>(first_row, out, nulls, pi),
            16 => self.decode_cells::<16>(first_row, out, nulls, pi),
            _ => unreachable!("a fixed-width cell is 1, 2, 4, 8 or 16 bytes"),
        }
    }

    fn decode_cells<const W: usize>(&self, first_row: usize, out: &mut [u8], nulls: &[u8], pi: usize) {
        // No rows may lie past the last block.
        if out.is_empty() {
            return;
        }
        let (before, nulls) = nulls.as_chunks::<8>().0.split_at(first_row % DECODE_BLOCK_ROWS);
        let held = |word: &[u8; 8]| (u64::from_le_bytes(*word) >> pi & 1 == 0) as usize;
        let at = u32::from_le_bytes(self.ranks[first_row / DECODE_BLOCK_ROWS]) as usize;
        let at = at + before.iter().map(held).sum::<usize>();
        let cells = out.as_chunks_mut::<W>().0.iter_mut().zip(nulls.iter().map(held));
        match self.values {
            // `parse` admits a frame only under cells a u64 holds.
            SparseValues::Framed(frame) => {
                self.fill(cells, at, |at| *frame.at(at).to_le_bytes().first_chunk().unwrap())
            }
            SparseValues::Cells(values) => self.fill(cells, at, |at| *values[at * W..].first_chunk().unwrap()),
        }
    }

    /// Write each of `cells` — a cell, and whether its row holds a value — from
    /// value `at` on. A cell past the last value is zero whatever its row holds.
    #[inline(always)]
    fn fill<'c, const W: usize>(
        &self,
        cells: impl Iterator<Item = (&'c mut [u8; W], usize)>,
        mut at: usize,
        value: impl Fn(usize) -> [u8; W],
    ) {
        for (cell, held) in cells {
            let held = held & (at < self.held) as usize;
            *cell = if held != 0 { value(at) } else { [0; W] };
            at += held;
        }
    }
}

#[cfg(test)]
#[path = "tests/encoding.rs"]
mod tests;
