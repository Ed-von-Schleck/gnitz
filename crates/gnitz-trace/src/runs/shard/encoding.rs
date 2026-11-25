//! The encodings a shard region may carry: for each, the image the writer
//! builds and the parsed form the reader decodes it through.
//!
//! A parse checks an image's size against the row count it is read for. The
//! bytes inside an image are body bytes no open verifies, so no decode trusts
//! them: a code, a length or a rank that points outside its image reads the
//! last entry, the empty string or zero.

use std::ops::Range;

use super::codebook::{Codebook, PAYING_SIXTEENTHS};
use gnitz_expr::{Level, null_bits};
use gnitz_wire::{
    CodedHeap, FixedInt, GERMAN_INLINE_OFF, Heap, HeapValue, MAX_PK_COLUMNS, MAX_SYMBOL_LEN, MAX_SYMBOLS,
    SHORT_STRING_THRESHOLD, ScalarKind, SymbolTable, read_u32_le, read_u64_le, write_u32_le, write_u64_le,
};
use gnitz_zset::repr::append_value;
use gnitz_zset::repr::{gallop_by, lower_bound_by};
use gnitz_zset::schema::SchemaDescriptor;

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
        /// Sequential: a string column as one length per row, then each row's value
        /// in row order.
        Seq = 5,
        /// Sparse: a nullable payload column, not a string's, as its non-NULL cells.
        Sparse = 6,
        /// Key: a PK region as a narrower key of the same order.
        Key = 7,
        /// Symbols: the heap as its values coded under a symbol table (FSST).
        Symbols = 8,
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
            Encoding::Key => "key",
            Encoding::Symbols => "symbols",
        }
    }
}

// A TwoValue image: `value_a`, `value_b` (u64 LE), then one bit per row.
const TWO_VALUE_A_AT: usize = 0;
const TWO_VALUE_B_AT: usize = 8;
const TWO_VALUE_BITS_AT: usize = 16;

/// A word region's TwoValue image, bit *i* set ⇔ row *i* holds `value_b`; or
/// `None` unless the region holds exactly two distinct words.
pub(crate) fn two_value_encode(src: &[u8]) -> Option<Vec<u8>> {
    let words = src.as_chunks::<8>().0;
    let a = words[0];
    let b = *words.iter().find(|w| **w != a)?;
    let (a, b) = (u64::from_le_bytes(a), u64::from_le_bytes(b));
    let mut image = vec![0u8; TWO_VALUE_BITS_AT + words.len().div_ceil(8)];
    write_u64_le(&mut image, TWO_VALUE_A_AT, a);
    write_u64_le(&mut image, TWO_VALUE_B_AT, b);
    // A word of bits per 64 rows, beside one of the rows holding neither word.
    for (rows, bits) in words.chunks(64).zip(image[TWO_VALUE_BITS_AT..].chunks_mut(8)) {
        let (mut is_b, mut other) = (0u64, 0u64);
        for (j, w) in rows.iter().enumerate() {
            let v = u64::from_le_bytes(*w);
            is_b |= ((v == b) as u64) << j;
            other |= ((v != a && v != b) as u64) << j;
        }
        if other != 0 {
            return None;
        }
        bits.copy_from_slice(&is_b.to_le_bytes()[..bits.len()]);
    }
    Some(image)
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

    /// Both words mapped through `f`, so every row's word is.
    pub(crate) fn map_words(&mut self, f: impl Fn(u64) -> u64) {
        self.words = self.words.map(f);
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

const fn for_image_len(count: usize, bw: usize) -> usize {
    for_cell_at(count, bw) + FOR_SLACK
}

/// The FoR image of a fixed-int region, framed on its minimum; or `None` unless
/// it is smaller than the region.
pub(crate) fn for_encode(src: &[u8], fi: FixedInt) -> Option<Vec<u8>> {
    gnitz_wire::for_each_fixed_int!(fi, |FI| {
        const W: usize = FI.width();
        let cells = src.as_chunks::<W>().0;
        let widen = |cell: &[u8; W]| FI.decode_le_i64(cell) as u64;
        // The order image maps i64 order onto u64 order, so one unsigned min/max
        // serves both signednesses.
        const KIND: ScalarKind = ScalarKind::Int(FI);
        let (mut min, mut max) = (u64::MAX, 0u64);
        for cell in cells {
            let b = KIND.order_image(widen(cell));
            min = min.min(b);
            max = max.max(b);
        }
        let reference = KIND.order_inverse(min);
        let bw = for_bw(KIND.order_inverse(max).wrapping_sub(reference));
        (bw > 0 && for_image_len(cells.len(), bw) < src.len())
            .then(|| for_image(cells.len(), cells.iter().map(widen), reference, bw))
    })
}

/// The fewest bytes that hold every offset up to `max_offset`.
const fn for_bw(max_offset: u64) -> usize {
    (max_offset.bit_width() as usize).div_ceil(8)
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

    /// Every value with the bits `high` set, each of them above every bit any
    /// value holds: the reference takes them, since a value is its offset added
    /// to the reference and the sum carries into none of them.
    pub(crate) fn set_high_bits(&mut self, high: u64) {
        self.reference |= high;
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
        match self.bw {
            1 => self.decode_framed::<W, 1>(first_row, out),
            2 => self.decode_framed::<W, 2>(first_row, out),
            3 => self.decode_framed::<W, 3>(first_row, out),
            4 => self.decode_framed::<W, 4>(first_row, out),
            5 => self.decode_framed::<W, 5>(first_row, out),
            6 => self.decode_framed::<W, 6>(first_row, out),
            7 => self.decode_framed::<W, 7>(first_row, out),
            _ => unreachable!("a framed offset is 1 to 7 bytes"),
        }
    }

    /// [`decode_cells`](Self::decode_cells) at the constant offset width `BW`.
    #[inline(always)]
    fn decode_framed<const W: usize, const BW: usize>(&self, first_row: usize, out: &mut [u8]) {
        let cells = out.as_chunks_mut::<W>().0;
        let Some(last) = cells.len().checked_sub(1) else {
            return;
        };
        // The rows' offsets, the last one a whole u64.
        let offsets = &self.image[for_cell_at(first_row, BW)..for_cell_at(first_row + last, BW) + size_of::<u64>()];
        for (i, cell) in cells.iter_mut().enumerate() {
            let at = &offsets[i * BW..];
            // An offset the width of an integer loads as one; any other, as the low bytes of a u64.
            let packed = if BW.is_power_of_two() {
                let mut exact = [0u8; 8];
                exact[..BW].copy_from_slice(at.first_chunk::<BW>().unwrap());
                u64::from_le_bytes(exact)
            } else {
                u64::from_le_bytes(*at.first_chunk().unwrap()) & gnitz_wire::low_bits_mask(8 * BW)
            };
            *cell = *packed
                .wrapping_add(self.reference)
                .to_le_bytes()
                .first_chunk::<W>()
                .unwrap();
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
        n => (n - 1).bit_width() as usize,
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
        gnitz_wire::by_cell_width!(width, |T| self.decode_cells::<{ size_of::<T>() }>(first_row, out))
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

/// The most fields a key reads as.
const KEY_MAX_FIELDS: usize = 2 * MAX_PK_COLUMNS;

// A Key image: each field's minimum (u64 LE); each field's bit width (u8), the
// widths zero-padded to eight bytes; a packed row per row; `KEY_SLACK` zeroes.
// A packed row is each field's `value − minimum` at its bit width, the first
// field leading, big-endian and zero-padded to whole bytes: it orders as its row.
const KEY_ROW_MAX: usize = size_of::<u128>();
/// The bytes behind the packed rows that let the last one be read and written
/// as a whole [`KeyWord`].
const KEY_SLACK: usize = KEY_ROW_MAX - 1;

const fn key_rows_at(fields: usize) -> usize {
    fields * 8 + fields.next_multiple_of(8)
}

const fn key_image_len(count: usize, fields: usize, stride: usize) -> usize {
    key_rows_at(fields) + count * stride + KEY_SLACK
}

/// One field of a key: a big-endian unsigned integer of a raw row.
#[derive(Clone, Copy, Default)]
struct KeyField {
    /// Where the field starts in a raw row, and its bytes there.
    at: usize,
    width: usize,
    min: u64,
    /// The bits of `value − min` in a packed row.
    mask: u64,
    /// How far they lie above the low end of the row's [`KeyWord`].
    shift: u32,
}

impl KeyField {
    /// This field of the raw row at `row`.
    ///
    /// # Safety
    /// The field's bytes are readable at `row`.
    #[inline(always)]
    unsafe fn load(&self, row: *const u8) -> u64 {
        // SAFETY: the caller's contract above; each read is the field's width.
        unsafe {
            let at = row.add(self.at);
            match self.width {
                8 => u64::from_be(at.cast::<u64>().read_unaligned()),
                4 => u32::from_be(at.cast::<u32>().read_unaligned()) as u64,
                2 => u16::from_be(at.cast::<u16>().read_unaligned()) as u64,
                _ => *at as u64,
            }
        }
    }

    /// [`load`](Self::load) as one eight-byte read, whatever the field's width.
    ///
    /// # Safety
    /// Eight bytes are readable at the field's start in `row`.
    #[inline(always)]
    unsafe fn load_wide(&self, row: *const u8) -> u64 {
        // SAFETY: the caller's contract above.
        let word = unsafe { row.add(self.at).cast::<u64>().read_unaligned() };
        u64::from_be(word) >> (u64::BITS as usize - 8 * self.width)
    }

    /// `value` left-aligned in a u64, so that its big-endian bytes lead with
    /// the field's.
    #[inline(always)]
    fn leading(&self, value: u64) -> u64 {
        value << (u64::BITS as usize - 8 * self.width)
    }
}

/// How many rows of `stride` bytes at the front of `len` bytes an eight-byte
/// read or write starting anywhere in them stays inside `len`.
fn wide_rows(len: usize, stride: usize) -> usize {
    len.saturating_sub(size_of::<u64>() - 1) / stride
}

/// A packed row loaded left-aligned as an unsigned integer, the bytes of the
/// rows after it below its own: a `u64` where the row fits one, which keeps a
/// search's compare and a decode's shifts in one register.
trait KeyWord: Copy + Ord + Default {
    const BITS: u32;
    /// The row at `at`.
    ///
    /// # Safety
    /// `size_of::<Self>()` bytes are readable at `at`.
    unsafe fn load(at: *const u8) -> Self;
    /// `self` to `at`, a row with zeroes below it.
    ///
    /// # Safety
    /// `size_of::<Self>()` bytes are writable at `at`.
    unsafe fn store(self, at: *mut u8);
    fn field(self, field: &KeyField) -> u64;
    fn with(self, field: &KeyField, offset: u64) -> Self;
    /// The word's leading `stride` bytes, a row's own, over zeroes.
    fn row(self, stride: usize) -> Self;
    /// `self` counted up by one at bit `bit`, where the word holds that.
    fn above(self, bit: u32) -> Option<Self>;
}

macro_rules! impl_key_word {
    ($($word:ty)*) => {$(
        impl KeyWord for $word {
            const BITS: u32 = <$word>::BITS;
            #[inline(always)]
            unsafe fn load(at: *const u8) -> $word {
                // SAFETY: the trait method's contract.
                <$word>::from_be(unsafe { at.cast::<$word>().read_unaligned() })
            }
            #[inline(always)]
            unsafe fn store(self, at: *mut u8) {
                // SAFETY: the trait method's contract.
                unsafe { at.cast::<$word>().write_unaligned(self.to_be()) }
            }
            #[inline(always)]
            fn field(self, field: &KeyField) -> u64 {
                (self >> field.shift) as u64 & field.mask
            }
            #[inline(always)]
            fn with(self, field: &KeyField, offset: u64) -> $word {
                self | (offset as $word) << field.shift
            }
            #[inline(always)]
            fn row(self, stride: usize) -> $word {
                self & (<$word>::MAX << (<$word>::BITS as usize - 8 * stride))
            }
            #[inline(always)]
            fn above(self, bit: u32) -> Option<$word> {
                self.checked_add((1 as $word).checked_shl(bit)?)
            }
        }
    )*};
}
impl_key_word!(u64 u128);

/// Run `$body` with `$w` the [`KeyWord`] a packed row of `$stride` bytes loads as.
macro_rules! key_word_dispatch {
    ($stride:expr, |$w:ident| $body:expr) => {
        if $stride <= size_of::<u64>() {
            type $w = u64;
            $body
        } else {
            type $w = u128;
            $body
        }
    };
}

/// The fields a PK row reads as, in order: a column of up to eight bytes is
/// one, a 16-byte column its two halves.
#[derive(Clone, Copy)]
struct KeyLayout {
    fields: [KeyField; KEY_MAX_FIELDS],
    n: usize,
    /// Bytes per raw row.
    raw_stride: usize,
}

impl KeyLayout {
    /// The fields of `schema`'s PK region, with no place in a packed row yet.
    fn of(schema: &SchemaDescriptor) -> Self {
        let mut layout = KeyLayout {
            fields: [KeyField::default(); KEY_MAX_FIELDS],
            n: 0,
            raw_stride: 0,
        };
        for col in schema.pk_columns() {
            let halves: &[usize] = if col.size() == 16 { &[8, 8] } else { &[col.size()] };
            for &width in halves {
                assert!(matches!(width, 1 | 2 | 4 | 8), "a PK column is 1, 2, 4, 8 or 16 bytes");
                let field = &mut layout.fields[layout.n];
                (field.at, field.width) = (layout.raw_stride, width);
                layout.n += 1;
                layout.raw_stride += width;
            }
        }
        layout
    }

    fn fields(&self) -> &[KeyField] {
        &self.fields[..self.n]
    }

    /// The bytes of a packed row and these fields placed in it, at `bits` and
    /// `min` per field; `None` unless each width fits its field and the row is
    /// 1 to [`KEY_ROW_MAX`] bytes.
    fn packed(mut self, bits: impl Fn(usize) -> u32, min: impl Fn(usize) -> u64) -> Option<(usize, Self)> {
        let total: u32 = (0..self.n).map(&bits).sum();
        let stride = (total as usize).div_ceil(8);
        if !(1..=KEY_ROW_MAX).contains(&stride) {
            return None;
        }
        let word = key_word_dispatch!(stride, |W| W::BITS);
        let mut shift = word;
        for (f, field) in self.fields[..self.n].iter_mut().enumerate() {
            if bits(f) > 8 * field.width as u32 {
                return None;
            }
            shift -= bits(f);
            (field.min, field.mask) = (min(f), gnitz_wire::low_bits_mask(bits(f) as usize));
            // A leading field of no bits lies at the word's width, which no
            // shift reaches; it reads zero from anywhere.
            field.shift = shift.min(word - 1);
        }
        Some((stride, self))
    }
}

// The arms of `key_fields_dispatch!` are the field counts a key can have.
const _: () = assert!(KEY_MAX_FIELDS == 10);

/// Run `$body` with `$set` the fields of `$layout` by value, an array of
/// their count: the constant count unrolls a row loop over them, and the
/// fields stay in registers across the loop's stores.
macro_rules! key_fields_dispatch {
    ($layout:expr, |$set:ident| $body:expr) => {
        key_fields_dispatch!(@arms $layout, $set, $body, 1 2 3 4 5 6 7 8 9 10)
    };
    (@arms $layout:expr, $set:ident, $body:expr, $($c:literal)*) => {
        match $layout.n {
            $($c => {
                let $set: [KeyField; $c] = *$layout.fields.first_chunk().unwrap();
                $body
            })*
            _ => unreachable!("a key reads as 1 to {KEY_MAX_FIELDS} fields"),
        }
    };
}

/// The Key image of `src`, the PK region of `schema` in key order; or `None`
/// unless a packed row fits and the image is smaller than the region.
pub(crate) fn key_encode(src: &[u8], schema: &SchemaDescriptor) -> Option<Vec<u8>> {
    let raw = KeyLayout::of(schema);
    debug_assert!(src.chunks_exact(raw.raw_stride).is_sorted());
    let (fields, rows) = (raw.n, src.len() / raw.raw_stride);
    let spans = key_fields_dispatch!(raw, |set| key_spans(set, raw.raw_stride, src));
    let bits = |f: usize| (spans[f].1 - spans[f].0).bit_width();
    let (stride, layout) = raw.packed(bits, |f| spans[f].0)?;
    let len = key_image_len(rows, fields, stride);
    if len >= src.len() {
        return None;
    }
    let mut image = vec![0u8; len];
    for f in 0..fields {
        write_u64_le(&mut image, f * 8, spans[f].0);
        image[fields * 8 + f] = bits(f) as u8;
    }
    let packed = &mut image[key_rows_at(fields)..];
    key_word_dispatch!(stride, |W| key_fields_dispatch!(layout, |set| {
        key_pack::<W, _>(set, stride, layout.raw_stride, src, packed)
    }));
    Some(image)
}

/// Each of `fields`' least and greatest value over the rows of `raw_stride`
/// bytes of `src`.
#[inline(never)]
fn key_spans<const N: usize>(fields: [KeyField; N], raw_stride: usize, src: &[u8]) -> [(u64, u64); KEY_MAX_FIELDS] {
    let mut spans = [(u64::MAX, 0u64); KEY_MAX_FIELDS];
    // The rows are in key order, so the leading field's span is its first and last row's.
    let (first, last) = (&src[..raw_stride], &src[src.len() - raw_stride..]);
    // SAFETY: a field lies inside its row.
    spans[0] = unsafe { (fields[0].load(first.as_ptr()), fields[0].load(last.as_ptr())) };
    let (rest, fields) = (&mut spans[1..], &fields[1..]);
    let (n, wide) = (src.len() / raw_stride, wide_rows(src.len(), raw_stride));
    let src = src.as_ptr();
    // SAFETY: `src` holds `n` rows, a field inside each and the first `wide` wide.
    unsafe {
        for i in 0..wide {
            for (span, field) in rest.iter_mut().zip(fields) {
                let v = field.load_wide(src.add(i * raw_stride));
                *span = (span.0.min(v), span.1.max(v));
            }
        }
        for i in wide..n {
            for (span, field) in rest.iter_mut().zip(fields) {
                let v = field.load(src.add(i * raw_stride));
                *span = (span.0.min(v), span.1.max(v));
            }
        }
    }
    spans
}

/// The rows of `raw_stride` bytes of `src` packed to `dst` as rows of `stride`
/// bytes.
#[inline(never)]
fn key_pack<W: KeyWord, const N: usize>(
    fields: [KeyField; N],
    stride: usize,
    raw_stride: usize,
    src: &[u8],
    dst: &mut [u8],
) {
    let (n, wide) = (src.len() / raw_stride, wide_rows(src.len(), raw_stride));
    assert!(n * stride + KEY_SLACK <= dst.len());
    let (src, dst) = (src.as_ptr(), dst.as_mut_ptr());
    // SAFETY: `src` holds `n` rows, a field inside each and the first `wide`
    // wide; the assert leaves each packed row a whole word, whose excess the
    // next row overwrites.
    unsafe {
        for i in 0..wide {
            let row = src.add(i * raw_stride);
            let word = fields.iter().fold(W::default(), |word, field| {
                word.with(field, field.load_wide(row) - field.min)
            });
            word.store(dst.add(i * stride));
        }
        for i in wide..n {
            let row = src.add(i * raw_stride);
            let word = fields.iter().fold(W::default(), |word, field| {
                word.with(field, field.load(row) - field.min)
            });
            word.store(dst.add(i * stride));
        }
    }
}

/// A Key image taken apart.
#[derive(Clone, Copy)]
pub(crate) struct KeyImage<'a> {
    /// The packed rows and their slack.
    rows: &'a [u8],
    count: usize,
    /// Bytes per packed row.
    stride: usize,
    layout: KeyLayout,
}

impl<'a> KeyImage<'a> {
    /// `image` as the key of `count` rows of `schema`'s PK, or `None` unless
    /// its bit widths fit their fields and give exactly that size.
    pub(crate) fn parse(image: &'a [u8], count: usize, schema: &SchemaDescriptor) -> Option<Self> {
        let raw = KeyLayout::of(schema);
        let n = raw.n;
        let (header, rows) = image.split_at_checked(key_rows_at(n))?;
        let (stride, layout) = raw.packed(|f| header[n * 8 + f] as u32, |f| read_u64_le(header, f * 8))?;
        (rows.len() == count.checked_mul(stride)?.checked_add(KEY_SLACK)?).then_some(KeyImage {
            rows,
            count,
            stride,
            layout,
        })
    }

    /// Decode rows `first_row..` back to their raw rows, `out.len() / raw
    /// stride` rows. A sum past its field's width wraps.
    pub(crate) fn decode(&self, first_row: usize, out: &mut [u8]) {
        let (layout, src) = (&self.layout, &self.rows[first_row * self.stride..]);
        key_word_dispatch!(self.stride, |W| key_fields_dispatch!(layout, |set| {
            key_unpack::<W, _>(set, self.stride, layout.raw_stride, src, out)
        }))
    }

    /// Row `row`'s word.
    #[inline(always)]
    fn word<W: KeyWord>(&self, row: usize) -> W {
        debug_assert!(row < self.count);
        // SAFETY: `parse` found `count` packed rows and the slack behind them.
        unsafe { W::load(self.rows.as_ptr().add(row * self.stride)) }
    }

    /// The packed key that the rows at or after the raw key `key` are at or
    /// after, and whether it is `key` itself, which alone a row can equal;
    /// `None` where every row sorts before `key`.
    #[inline(always)]
    fn probe<W: KeyWord>(&self, key: &[u8]) -> Option<(W, bool)> {
        assert_eq!(
            key.len(),
            self.layout.raw_stride,
            "seek probe width must equal pk_stride"
        );
        let mut packed = W::default();
        // The bit that counts the fields so far up by one.
        let mut carry = W::BITS;
        for field in self.layout.fields() {
            // SAFETY: the fields lie inside a raw row, which the assert makes `key`.
            let v = unsafe { field.load(key.as_ptr()) };
            if v < field.min {
                // Below every row that shares the fields so far: their first.
                return Some((packed, false));
            }
            let offset = v - field.min;
            if offset > field.mask {
                // Above every row that shares them: the first row past those.
                return Some((packed.above(carry)?, false));
            }
            packed = packed.with(field, offset);
            carry -= field.mask.count_ones();
        }
        Some((packed, true))
    }

    /// The first row that sorts at or after the raw key `key`, galloping from
    /// `hint` where there is one.
    #[inline]
    pub(crate) fn seek(&self, key: &[u8], hint: Option<usize>) -> usize {
        key_word_dispatch!(self.stride, |W| {
            let Some((probe, _)) = self.probe::<W>(key) else {
                return self.count;
            };
            // A row's word runs into the rows after it, where the probe is zero.
            gallop_by(self.count, hint, |row: usize| self.word::<W>(row) < probe)
        })
    }

    /// The rows whose key is the raw key `key`: none, at its lower bound, where
    /// no row holds it.
    #[inline]
    pub(crate) fn rows_of(&self, key: &[u8]) -> Range<usize> {
        key_word_dispatch!(self.stride, |W| {
            let Some((probe, exact)) = self.probe::<W>(key) else {
                return self.count..self.count;
            };
            let lo = lower_bound_by(0, self.count, |row| self.word::<W>(row) < probe);
            let mut hi = lo;
            while exact && hi < self.count && self.word::<W>(hi).row(self.stride) == probe {
                hi += 1;
            }
            lo..hi
        })
    }
}

/// The packed rows of `stride` bytes of `src` decoded to `out`, `out.len() /
/// raw_stride` of them.
#[inline(never)]
fn key_unpack<W: KeyWord, const N: usize>(
    fields: [KeyField; N],
    stride: usize,
    raw_stride: usize,
    src: &[u8],
    out: &mut [u8],
) {
    assert!(out.len() / raw_stride * stride + KEY_SLACK <= src.len());
    // SAFETY: the assert leaves each packed row a whole word to load.
    let word = |i: usize| unsafe { W::load(src.as_ptr().add(i * stride)) };
    // A whole u64 per field while the row's last one stays inside `out`: the
    // field or the row after it overwrites the excess.
    let whole = wide_rows(out.len(), raw_stride);
    let dst = out.as_mut_ptr();
    for i in 0..whole {
        let word = word(i);
        for field in &fields {
            let v = word.field(field).wrapping_add(field.min);
            // SAFETY: the field's eight bytes end inside `out`, by `whole`.
            unsafe {
                dst.add(i * raw_stride + field.at)
                    .cast::<u64>()
                    .write_unaligned(field.leading(v).to_be())
            };
        }
    }
    for (i, row) in out.chunks_exact_mut(raw_stride).enumerate().skip(whole) {
        let word = word(i);
        for field in &fields {
            let v = word.field(field).wrapping_add(field.min);
            row[field.at..field.at + field.width].copy_from_slice(&field.leading(v).to_be_bytes()[..field.width]);
        }
    }
}

/// Rows per block of a packed payload column or key: what a per-row read
/// decodes and keeps, sized so that a block of an I64 column is one page, and
/// the spacing of the entry points a Seq or Sparse image holds, so that a block
/// decodes from one.
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

/// What a string cell holds over `src`, as two equal values hold it alike: its
/// length, and its bytes as the heap stores them — the value, or its codes under
/// the heap's one table. A long value `src` does not hold reads as the empty
/// string.
#[inline]
pub(crate) fn stored_value<'a>(cell: &'a [u8; 16], src: Heap<'a>) -> (usize, &'a [u8]) {
    match gnitz_wire::german_string_inline(cell) {
        Some(inline) => (inline.len(), inline),
        None => gnitz_wire::german_string_value(cell, src).map_or((0, &[]), |v| (v.len(), v.stored())),
    }
}

/// The Seq image of `cells`, a column of German-string cells over `src` whose
/// lengths span `min..=max`. The long values are appended to `heap`.
pub(crate) fn seq_encode(
    cells: &[[u8; 16]],
    (min, max): (usize, usize),
    src: Heap<'_>,
    heap: &mut HeapWriter<'_>,
) -> Vec<u8> {
    let count = cells.len();
    let mut image = vec![0u8; seq_lens_at(count)];
    let lens = cells.iter().map(|c| stored_value(c, src).0 as u64);
    image.extend_from_slice(&for_image(count, lens, min as u64, seq_bw(min, max)));
    let pool_at = image.len();
    for (row, cell) in cells.iter().enumerate() {
        if row % DECODE_BLOCK_ROWS == 0 {
            let entry = SEQ_BLOCKS_AT + row / DECODE_BLOCK_ROWS * SEQ_BLOCK_ENTRY;
            let pooled = image.len() - pool_at;
            write_u64_le(&mut image, entry, heap.next_at() as u64);
            write_u64_le(&mut image, entry + 8, pooled as u64);
        }
        match gnitz_wire::german_string_inline(cell) {
            Some(inline) => image.extend_from_slice(inline),
            None => {
                if let Some(value) = gnitz_wire::german_string_value(cell, src) {
                    heap.push(value);
                }
            }
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
    pub(crate) fn decode(&self, heap: Heap<'_>, first_row: usize, out: &mut [u8]) {
        match heap {
            // A plain heap holds each value's bytes back to back.
            Heap::Plain(blob) => self.decode_over(first_row, out, |at, len| {
                let value = blob.get(at..).filter(|rest| rest.len() >= len);
                (
                    value.and_then(|v| v.first_chunk::<4>()).copied(),
                    at.saturating_add(len),
                )
            }),
            Heap::Coded(_) => self.decode_over(first_row, out, |at, len| {
                let value = heap.value(at as u64, len);
                let prefix = value.map(|v| v.lead()[..4].try_into().unwrap());
                (prefix, value.map_or(usize::MAX, HeapValue::end))
            }),
        }
    }

    /// [`decode`](Self::decode) over a heap `long` reads: the prefix of the
    /// value of `len` bytes stored from `at`, if the heap holds it, and where
    /// the next one starts.
    #[inline(always)]
    fn decode_over(&self, first_row: usize, out: &mut [u8], long: impl Fn(usize, usize) -> (Option<[u8; 4]>, usize)) {
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
                n if n > SHORT_STRING_THRESHOLD => heap_at = long(heap_at, n).1,
                short => pool_at = pool_at.saturating_add(short),
            }
        }
        for (i, cell) in cells.iter_mut().enumerate() {
            let len = len(first_row + i);
            let mut image = [0u8; 16];
            if len > SHORT_STRING_THRESHOLD {
                let (prefix, next) = long(heap_at, len);
                if let Some(prefix) = prefix {
                    image[..4].copy_from_slice(&(len as u32).to_le_bytes());
                    image[4..8].copy_from_slice(&prefix);
                    image[8..].copy_from_slice(&(heap_at as u64).to_le_bytes());
                }
                heap_at = next;
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

// A Symbols image: a heap as its values coded under a symbol table. The bytes
// the values decode to (u64 LE); how the table coded its training values, in
// sixteenths of their bytes (u64 LE); the symbol count (u64 LE); each symbol,
// eight bytes; each symbol's length, a byte; the values, each where a cell names
// it, as its code count (LEB128) and its codes.
const SYMBOLS_PLAIN_LEN_AT: usize = 0;
const SYMBOLS_TRAINED_AT: usize = 8;
pub(crate) const SYMBOLS_COUNT_AT: usize = 16;
pub(crate) const SYMBOLS_AT: usize = 24;

/// The heap a shard's string columns are packed into, as the writer fills it:
/// each long value verbatim, or coded under a codebook.
pub(crate) struct HeapWriter<'a> {
    /// The heap image's bytes: the values, or the coded image's head and codes.
    bytes: Vec<u8>,
    /// Where the codes start in `bytes`, and the codebook they are coded under.
    coded: Option<(usize, &'a Codebook)>,
    /// The bytes the values appended take decoded.
    plain_len: usize,
}

impl<'a> HeapWriter<'a> {
    /// A heap holding each value verbatim.
    pub(crate) fn plain() -> Self {
        HeapWriter {
            bytes: Vec::new(),
            coded: None,
            plain_len: 0,
        }
    }

    /// A heap holding each value coded under `book`.
    pub(crate) fn coded(book: &'a Codebook) -> Self {
        let table = book.table();
        let mut bytes = vec![0u8; SYMBOLS_AT];
        write_u64_le(&mut bytes, SYMBOLS_TRAINED_AT, book.trained_sixteenths() as u64);
        write_u64_le(&mut bytes, SYMBOLS_COUNT_AT, table.symbols().count() as u64);
        for (symbol, _) in table.symbols() {
            bytes.extend_from_slice(symbol);
        }
        bytes.extend(table.symbols().map(|(_, len)| len));
        HeapWriter {
            coded: Some((bytes.len(), book)),
            bytes,
            plain_len: 0,
        }
    }

    /// Where the next value appended starts, as its cell names it.
    pub(crate) fn next_at(&self) -> usize {
        self.bytes.len() - self.coded.map_or(0, |(codes_at, _)| codes_at)
    }

    /// Append `value`; where it starts, as its cell names it. See
    /// [`append_value`] for a coded heap.
    pub(crate) fn push(&mut self, value: HeapValue<'_>) -> usize {
        let at = self.next_at();
        self.plain_len += value.len();
        append_value(value, &mut self.bytes, self.coded.map(|(_, book)| book));
        at
    }

    /// `cell`, a cell over the heap `src`, with its long value appended.
    #[inline]
    pub(crate) fn place(&mut self, cell: &[u8; 16], src: Heap<'_>) -> [u8; 16] {
        gnitz_wire::relocate_german_value_with(cell, src, |value| self.push(value))
    }

    /// Whether the heap is coded and its image, table included, holds more than
    /// `PAYING_SIXTEENTHS` of the bytes its values take.
    pub(crate) fn coding_lost(&self) -> bool {
        self.coded.is_some() && 16 * self.bytes.len() > PAYING_SIXTEENTHS * self.plain_len
    }

    /// The heap's region: its encoding and image.
    pub(crate) fn finish(mut self) -> (Encoding, Vec<u8>) {
        match self.coded {
            None => (Encoding::Raw, self.bytes),
            Some(_) => {
                write_u64_le(&mut self.bytes, SYMBOLS_PLAIN_LEN_AT, self.plain_len as u64);
                (Encoding::Symbols, self.bytes)
            }
        }
    }
}

/// A Symbols image taken apart.
pub(crate) struct SymbolsImage<'a> {
    pub(crate) table: Box<SymbolTable>,
    /// How the table coded its training values, in sixteenths of their bytes.
    pub(crate) trained_sixteenths: usize,
    codes: &'a [u8],
    plain_len: usize,
}

impl<'a> SymbolsImage<'a> {
    /// `image` as a Symbols image, or `None` unless its head and table fit it
    /// and the table is one [`SymbolTable::new`] takes.
    pub(crate) fn parse(image: &'a [u8]) -> Option<Self> {
        let head = image.get(..SYMBOLS_AT)?;
        let word = |at| usize::try_from(read_u64_le(head, at)).ok();
        let n = word(SYMBOLS_COUNT_AT).filter(|&n| n <= MAX_SYMBOLS)?;
        let (symbols, rest) = image[SYMBOLS_AT..].split_at_checked(n * MAX_SYMBOL_LEN)?;
        let (lens, codes) = rest.split_at_checked(n)?;
        Some(SymbolsImage {
            table: SymbolTable::new(symbols.as_chunks().0, lens)?,
            trained_sixteenths: word(SYMBOLS_TRAINED_AT)?,
            codes,
            plain_len: word(SYMBOLS_PLAIN_LEN_AT)?,
        })
    }

    /// The heap the image holds.
    pub(crate) fn heap(&self) -> Heap<'_> {
        Heap::Coded(CodedHeap {
            table: &self.table,
            codes: self.codes,
            plain_len: self.plain_len,
        })
    }
}

// A Sparse image: a nullable column as its non-NULL cells alone. The non-NULL
// row count (u64 LE); per block, the non-NULL rows before it (u32 LE); the
// values in row order — the FoR image of them where that is the smaller, else
// the cells themselves.
const SPARSE_RANKS_AT: usize = 8;
const SPARSE_RANK_ENTRY: usize = size_of::<u32>();

/// The Sparse image of a region of `width`-byte cells whose row `r` holds a
/// value iff bit `r` of `held` is set, as [`held_bits`] gives them, its values
/// framed where `fi` admits a frame smaller than they are.
pub(crate) fn sparse_encode(src: &[u8], width: usize, fi: Option<FixedInt>, held: &[u64]) -> Vec<u8> {
    let n = src.len() / width;
    debug_assert_eq!(held.len(), n.div_ceil(64), "one bit per row");
    let mut image = vec![0u8; SPARSE_RANKS_AT + n.div_ceil(DECODE_BLOCK_ROWS) * SPARSE_RANK_ENTRY];
    let mut count = 0usize;
    for (block, words) in held.chunks(DECODE_BLOCK_ROWS / 64).enumerate() {
        write_u32_le(&mut image, SPARSE_RANKS_AT + block * SPARSE_RANK_ENTRY, count as u32);
        count += words.iter().map(|w| w.count_ones() as usize).sum::<usize>();
    }
    write_u64_le(&mut image, 0, count as u64);
    let mut values = vec![0u8; count * width];
    gnitz_wire::by_cell_width!(width, |T| gather_held::<{ size_of::<T>() }>(src, held, &mut values));
    let framed = fi.and_then(|fi| for_encode(&values, fi));
    image.extend_from_slice(framed.as_ref().unwrap_or(&values));
    image
}

/// Copy the cells of `src` whose bit of `held` is set into `values`, in row order.
fn gather_held<const W: usize>(src: &[u8], held: &[u64], values: &mut [u8]) {
    let cells = src.as_chunks::<W>().0;
    let out = values.as_chunks_mut::<W>().0;
    let mut at = 0;
    for (w, &word) in held.iter().enumerate() {
        for j in gnitz_wire::BitIter(word) {
            out[at] = cells[w * 64 + j];
            at += 1;
        }
    }
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
    /// width` rows: a NULL row's is zero. `null_words(rows)` answers the null
    /// words of `rows`, of which bit `pi` is this column's. A row they place
    /// past the last value reads zero too.
    pub(crate) fn decode(
        &self,
        first_row: usize,
        width: usize,
        out: &mut [u8],
        pi: usize,
        null_words: impl FnOnce(Range<usize>) -> Vec<u8>,
    ) {
        // From the block's first row on: its rank counts the values before it.
        let from = first_row - first_row % DECODE_BLOCK_ROWS;
        let nulls = null_words(from..first_row + out.len() / width);
        debug_assert_eq!(nulls.len(), (first_row + out.len() / width - from) * size_of::<u64>());
        gnitz_wire::by_cell_width!(width, |T| self
            .decode_cells::<{ size_of::<T>() }>(first_row, out, &nulls, pi))
    }

    fn decode_cells<const W: usize>(&self, first_row: usize, out: &mut [u8], nulls: &[u8], pi: usize) {
        // No rows may lie past the last block.
        if out.is_empty() {
            return;
        }
        // A NULL row's cell, and one past the last value.
        out.fill(0);
        let level = Level::new();
        let (before, nulls) = nulls.split_at(first_row % DECODE_BLOCK_ROWS * size_of::<u64>());
        let mut bits = [0u64; HELD_CHUNK_ROWS / 64];
        let at = u32::from_le_bytes(self.ranks[first_row / DECODE_BLOCK_ROWS]) as usize;
        let at = at
            + held_bits(level, before, pi, &mut bits)
                .iter()
                .map(|w| w.count_ones() as usize)
                .sum::<usize>();
        let cells = out.as_chunks_mut::<W>().0;
        match self.values {
            // `parse` admits a frame only under cells a u64 holds.
            SparseValues::Framed(frame) => self.scatter(level, nulls, pi, cells, at, |at| {
                *frame.at(at).to_le_bytes().first_chunk().unwrap()
            }),
            SparseValues::Cells(values) => self.scatter(level, nulls, pi, cells, at, |at| {
                *values[at * W..].first_chunk().unwrap()
            }),
        }
    }

    /// Write value `at` on into each of `cells` whose row, by its word of
    /// `nulls`, holds one, until the values run out.
    #[inline(always)]
    fn scatter<const W: usize>(
        &self,
        level: Level,
        nulls: &[u8],
        pi: usize,
        cells: &mut [[u8; W]],
        mut at: usize,
        value: impl Fn(usize) -> [u8; W],
    ) {
        let mut bits = [0u64; HELD_CHUNK_ROWS / 64];
        let chunks = nulls
            .chunks(HELD_CHUNK_ROWS * size_of::<u64>())
            .zip(cells.chunks_mut(HELD_CHUNK_ROWS));
        for (rows, cells) in chunks {
            for (w, &word) in held_bits(level, rows, pi, &mut bits).iter().enumerate() {
                for j in gnitz_wire::BitIter(word) {
                    if at >= self.held {
                        return;
                    }
                    cells[w * 64 + j] = value(at);
                    at += 1;
                }
            }
        }
    }
}

/// Rows a Sparse decode gathers the null bits of at once.
const HELD_CHUNK_ROWS: usize = 64 * 64;
const _: () = assert!(DECODE_BLOCK_ROWS <= HELD_CHUNK_ROWS);

/// One bit per row of `rows` (8-byte null words), set where bit `pi` of the
/// row's word is clear, written to the leading words of `bits` and returned.
pub(super) fn held_bits<'b>(level: Level, rows: &[u8], pi: usize, bits: &'b mut [u64]) -> &'b [u64] {
    let n = rows.len() / size_of::<u64>();
    let bits = &mut bits[..n.div_ceil(64)];
    null_bits(level, rows, 1 << pi, bits);
    bits.iter_mut().for_each(|w| *w = !*w);
    if let (Some(last), tail @ 1..) = (bits.last_mut(), n % 64) {
        *last &= gnitz_wire::low_bits_mask(tail);
    }
    bits
}

#[cfg(test)]
#[path = "tests/encoding.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/encoding.rs"]
mod bench;
