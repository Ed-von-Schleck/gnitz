//! Shared shard file format constants, and the digest over the bytes that
//! decide how the rest of a shard is read.

use crate::repr::error::StorageError;
use gnitz_wire::{read_i64_le, read_signed_exact, read_u64_le, read_unsigned_exact, write_u64_le, FixedInt};

use StorageError::Corrupt;

pub(crate) const SHARD_MAGIC: u64 = 0x31305F5A54494E47;
/// Bumped by hand for any change to the bytes a writer produces;
/// `shard_bytes_are_pinned` fails until it is.
pub(crate) const SHARD_EPOCH: u64 = 22;

/// Compared for equality at open. A shard sizes its regions from the live
/// schema, so a system-table shape change must refuse the file, not reinterpret it.
pub(crate) const SHARD_VERSION: u64 = SHARD_EPOCH ^ gnitz_wire::SYS_SCHEMA_DIGEST;
pub(crate) const DIR_ENTRY_SIZE: usize = 16;
pub(crate) const ALIGNMENT: usize = 64;

pub(crate) const OFF_MAGIC: usize = 0;
pub(crate) const OFF_VERSION: usize = 8;
/// The row count (u64 LE), at least 1: every writer skips an empty output.
pub(crate) const OFF_ROW_COUNT: usize = 16;
pub(crate) const OFF_DESC_CHECKSUM: usize = 24;
/// The writer's payload-column count (u64 LE), which fixes the file's region count.
pub(crate) const OFF_FILE_NPC: usize = 32;
/// Flag bits (u64 LE); see [`SHARD_FLAG_SKELETON`].
pub(crate) const OFF_FLAGS: usize = 40;
/// XXH3-64 over every byte after the descriptive prefix, alignment padding
/// included.
pub(crate) const OFF_BODY_CHECKSUM: usize = 48;
pub(crate) const HEADER_SIZE: usize = OFF_BODY_CHECKSUM + 8;

/// [`OFF_FLAGS`] bit: a capacity-bounded view's skeleton shard, one (PK, coarse
/// weight) row per key.
pub(crate) const SHARD_FLAG_SKELETON: u64 = 1;

/// The header fields a reader acts on; [`read`](Self::read) checks magic and
/// version.
#[derive(Clone, Copy)]
pub(crate) struct ShardHeader {
    pub row_count: usize,
    pub file_npc: usize,
    pub skeleton: bool,
    pub body_checksum: u64,
}

impl ShardHeader {
    pub(crate) fn read(data: &[u8]) -> Result<Self, StorageError> {
        if data.len() < HEADER_SIZE {
            return Err(Corrupt("shorter than the header"));
        }
        if read_u64_le(data, OFF_MAGIC) != SHARD_MAGIC {
            return Err(Corrupt("magic"));
        }
        if read_u64_le(data, OFF_VERSION) != SHARD_VERSION {
            return Err(Corrupt("version"));
        }
        let file_npc = usize::try_from(read_u64_le(data, OFF_FILE_NPC))
            .ok()
            .filter(|&n| n <= gnitz_wire::MAX_COLUMNS)
            .ok_or(Corrupt("payload arity"))?;
        let row_count = match read_u64_le(data, OFF_ROW_COUNT) {
            0 => return Err(Corrupt("no rows")),
            n => n as usize,
        };
        Ok(ShardHeader {
            row_count,
            file_npc,
            skeleton: read_u64_le(data, OFF_FLAGS) & SHARD_FLAG_SKELETON != 0,
            body_checksum: read_u64_le(data, OFF_BODY_CHECKSUM),
        })
    }

    /// Every field but the descriptor digest, which is stamped over the result.
    pub(crate) fn write(&self, header: &mut [u8]) {
        write_u64_le(header, OFF_MAGIC, SHARD_MAGIC);
        write_u64_le(header, OFF_VERSION, SHARD_VERSION);
        write_u64_le(header, OFF_ROW_COUNT, self.row_count as u64);
        write_u64_le(header, OFF_FILE_NPC, self.file_npc as u64);
        write_u64_le(header, OFF_FLAGS, if self.skeleton { SHARD_FLAG_SKELETON } else { 0 });
        write_u64_le(header, OFF_BODY_CHECKSUM, self.body_checksum);
    }
}

/// Byte offset of directory entry `i`. The directory follows the header
/// immediately, so an entry's position is implied by its index — the file
/// stores no directory offset to disagree with this.
pub(crate) const fn dir_entry_off(i: usize) -> usize {
    HEADER_SIZE + i * DIR_ENTRY_SIZE
}

/// A directory entry as stored: no offset, which [`region_spans`] derives.
pub(crate) struct DirEntry {
    pub size: usize,
    pub encoding: u8,
}

impl DirEntry {
    pub(crate) fn read(image: &[u8], i: usize) -> Self {
        let d = dir_entry_off(i);
        DirEntry {
            size: read_u64_le(image, d) as usize,
            encoding: image[d + 8],
        }
    }

    pub(crate) fn write(&self, image: &mut [u8], i: usize) {
        let d = dir_entry_off(i);
        write_u64_le(image, d, self.size as u64);
        image[d + 8] = self.encoding;
    }
}

/// Where the next region of a file starts, once the one before it ended at `end`.
pub(crate) const fn region_start(end: usize) -> usize {
    end.next_multiple_of(ALIGNMENT)
}

/// A directory entry placed in its file.
pub(crate) struct Span {
    pub off: usize,
    pub size: usize,
    pub encoding: Encoding,
}

impl Span {
    pub(crate) fn bytes<'a>(&self, image: &'a [u8]) -> &'a [u8] {
        &image[self.off..self.off + self.size]
    }
}

/// Every directory entry of `image` placed in it: the batch regions
/// `[pk, weight, null, payload…, blob]`, then the PK filter.
pub(crate) fn region_spans(image: &[u8], file_npc: usize) -> Result<Vec<Span>, StorageError> {
    let file_size = image.len();
    let mut end = desc_len(file_npc);
    let spans = (0..=gnitz_wire::num_regions(file_npc))
        .map(|i| {
            let DirEntry { size, encoding } = DirEntry::read(image, i);
            let encoding = Encoding::from_byte(encoding).ok_or(Corrupt("encoding"))?;
            let off = region_start(end);
            if off > file_size || size > file_size - off {
                return Err(Corrupt("region past the end"));
            }
            end = off + size;
            Ok(Span { off, size, encoding })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if end != file_size {
        return Err(Corrupt("directory does not span the file"));
    }
    Ok(spans)
}

// A TwoValue image: `value_a`, `value_b` (i64 LE), then one bit per row.
const TWO_VALUE_A_AT: usize = 0;
const TWO_VALUE_B_AT: usize = 8;
const TWO_VALUE_BITS_AT: usize = 16;

pub(crate) const fn two_value_image_len(count: usize) -> usize {
    TWO_VALUE_BITS_AT + count.div_ceil(8)
}

#[inline]
fn two_value_bit(bitvec: &[u8], row: usize) -> bool {
    (bitvec[row / 8] >> (row % 8)) & 1 != 0
}

#[inline]
fn two_value_set_bit(bitvec: &mut [u8], row: usize) {
    bitvec[row / 8] |= 1 << (row % 8);
}

/// A weight region's TwoValue image, bit *i* set ⇔ row *i* holds `value_b`; or
/// `None` at a third distinct weight.
pub(crate) fn two_value_encode(src: &[u8]) -> Option<Vec<u8>> {
    let n = src.len() / 8;
    let first = read_i64_le(src, 0);
    // Rows before the first second value hold `first`: their bits start 0.
    let mut second: Option<(i64, Vec<u8>)> = None;
    for i in 1..n {
        let v = read_i64_le(src, i * 8);
        if v == first {
            continue;
        }
        match &mut second {
            None => {
                let mut image = vec![0u8; two_value_image_len(n)];
                write_u64_le(&mut image, TWO_VALUE_A_AT, first as u64);
                write_u64_le(&mut image, TWO_VALUE_B_AT, v as u64);
                two_value_set_bit(&mut image[TWO_VALUE_BITS_AT..], i);
                second = Some((v, image));
            }
            Some((b, image)) => {
                if v != *b {
                    return None;
                }
                two_value_set_bit(&mut image[TWO_VALUE_BITS_AT..], i);
            }
        }
    }
    second.map(|(_, image)| image)
}

/// A TwoValue image's `(value_a, value_b, bit vector)`.
pub(crate) fn two_value_decode(image: &[u8]) -> (i64, i64, &[u8]) {
    (
        read_i64_le(image, TWO_VALUE_A_AT),
        read_i64_le(image, TWO_VALUE_B_AT),
        &image[TWO_VALUE_BITS_AT..],
    )
}

/// Row `row`'s weight, from a [`two_value_decode`]d image.
#[inline(always)]
pub(crate) fn two_value_at(a: i64, b: i64, bits: &[u8], row: usize) -> i64 {
    if two_value_bit(bits, row) {
        b
    } else {
        a
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

/// The offset width an image of `size` bytes holds for `count ≥ 1` rows, or
/// `None` unless some `bw` in `1..elem_width` gives exactly that size.
pub(crate) fn for_image_bw(size: usize, count: usize, elem_width: usize) -> Option<usize> {
    let bw = size.checked_sub(FOR_CELLS_AT + FOR_SLACK)? / count;
    ((1..elem_width).contains(&bw) && size == for_image_len(count, bw)).then_some(bw)
}

/// The FoR image of a fixed-int region, framed on its minimum; or `None` when it
/// would not shrink the region's aligned footprint.
pub(crate) fn for_encode(src: &[u8], fi: FixedInt) -> Option<Vec<u8>> {
    // A 1-byte cell has no narrower offset width.
    if fi.width() == 1 {
        return None;
    }
    gnitz_wire::for_each_fixed_int!(fi, |FI| { for_encode_cells::<{ FI.width() }, { FI.is_signed() }>(src) })
}

fn for_encode_cells<const W: usize, const SIGNED: bool>(src: &[u8]) -> Option<Vec<u8>> {
    let cells = src.as_chunks::<W>().0;
    let widen = |cell: &[u8; W]| {
        if SIGNED {
            read_signed_exact(cell) as u64
        } else {
            read_unsigned_exact(cell)
        }
    };
    // XOR with the sign bit maps i64 order onto u64 order, so one unsigned
    // min/max serves both signednesses.
    let bias = if SIGNED { 1u64 << 63 } else { 0 };
    let (mut min, mut max) = (u64::MAX, 0u64);
    for cell in cells {
        let b = widen(cell) ^ bias;
        min = min.min(b);
        max = max.max(b);
    }
    let reference = min ^ bias;
    let max_offset = (max ^ bias).wrapping_sub(reference);
    let bw = ((u64::BITS - max_offset.leading_zeros()) as usize).div_ceil(8);
    let n = cells.len();
    // A raw-byte win that vanishes after alignment saves no disk and still costs
    // a decode. Implies `bw < W`.
    if bw == 0 || region_start(for_image_len(n, bw)) >= region_start(src.len()) {
        return None;
    }
    let mut image = vec![0u8; for_image_len(n, bw)];
    write_u64_le(&mut image, FOR_REFERENCE_AT, reference);
    // A whole u64 per row: the next row overwrites the excess, the slack takes the last row's.
    for (row, cell) in cells.iter().enumerate() {
        write_u64_le(&mut image, for_cell_at(row, bw), widen(cell).wrapping_sub(reference));
    }
    Some(image)
}

/// Decode rows `first_row..` of a FoR image back to their raw little-endian
/// form, `out.len() / elem_width` rows.
pub(crate) fn for_decode(image: &[u8], bw: usize, elem_width: usize, first_row: usize, out: &mut [u8]) {
    match elem_width {
        2 => for_decode_cells::<2>(image, bw, first_row, out),
        4 => for_decode_cells::<4>(image, bw, first_row, out),
        8 => for_decode_cells::<8>(image, bw, first_row, out),
        _ => unreachable!("open admits FoR only on 2/4/8-byte integer columns"),
    }
}

fn for_decode_cells<const W: usize>(image: &[u8], bw: usize, first_row: usize, out: &mut [u8]) {
    debug_assert!((1..W).contains(&bw), "open-time checks bound bw to 1..elem_width");
    let reference = read_u64_le(image, FOR_REFERENCE_AT);
    let mask = gnitz_wire::low_bits_mask(8 * bw);
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

// A Dict image: the entry count (u64 LE), the 16-byte entries, then each row's
// code — one byte while every code fits one, else two (LE).
const DICT_ENTRIES_AT: usize = 8;

/// The most entries a dictionary holds: what a two-byte code addresses.
pub(crate) const DICT_MAX_ENTRIES: usize = 1 << 16;

const fn dict_code_width(entries: usize) -> usize {
    if entries <= 1 << 8 {
        1
    } else {
        2
    }
}

pub(crate) const fn dict_image_len(count: usize, entries: usize) -> usize {
    DICT_ENTRIES_AT + entries * 16 + count * dict_code_width(entries)
}

/// The Dict image of a column whose row `i` holds `entries[ids[i]]`.
pub(crate) fn dict_encode(entries: &[[u8; 16]], ids: &[u32]) -> Vec<u8> {
    assert!((1..=DICT_MAX_ENTRIES).contains(&entries.len()));
    let mut image = Vec::with_capacity(dict_image_len(ids.len(), entries.len()));
    image.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    image.extend_from_slice(entries.as_flattened());
    if dict_code_width(entries.len()) == 1 {
        image.extend(ids.iter().map(|&id| id as u8));
    } else {
        image.extend(ids.iter().flat_map(|&id| (id as u16).to_le_bytes()));
    }
    image
}

/// A Dict image taken apart: its entries, and its codes at their width.
#[derive(Clone, Copy)]
pub(crate) struct DictImage<'a> {
    entries: &'a [[u8; 16]],
    codes: &'a [u8],
    wide: bool,
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
            wide: dict_code_width(n) == 2,
        })
    }

    /// The entry `code` names. The codes are body bytes no open verifies, so a
    /// code past the last entry reads the last.
    #[inline(always)]
    fn entry(&self, code: usize) -> &'a [u8; 16] {
        // SAFETY: `parse` admits no empty dictionary, and the index is clamped into it.
        unsafe { self.entries.get_unchecked(code.min(self.entries.len() - 1)) }
    }

    /// Row `row`'s cell.
    #[inline(always)]
    pub(crate) fn cell(&self, row: usize) -> &'a [u8; 16] {
        if self.wide {
            self.entry(u16::from_le_bytes(self.codes.as_chunks::<2>().0[row]) as usize)
        } else {
            self.entry(self.codes[row] as usize)
        }
    }

    /// Decode rows `first_row..` to their cells, `out.len() / 16` rows.
    pub(crate) fn decode(&self, first_row: usize, out: &mut [u8]) {
        let out = out.as_chunks_mut::<16>().0;
        if self.wide {
            let codes = &self.codes.as_chunks::<2>().0[first_row..first_row + out.len()];
            for (cell, code) in out.iter_mut().zip(codes) {
                *cell = *self.entry(u16::from_le_bytes(*code) as usize);
            }
        } else {
            let codes = &self.codes[first_row..first_row + out.len()];
            for (cell, &code) in out.iter_mut().zip(codes) {
                *cell = *self.entry(code as usize);
            }
        }
    }
}

/// Header plus directory, by the file's own payload arity.
pub(crate) const fn desc_len(file_npc: usize) -> usize {
    dir_entry_off(gnitz_wire::num_regions(file_npc) + 1)
}

/// XXH3-64 over a shard's descriptive prefix (header + directory), its own eight
/// bytes excluded, seeded with the shard's basename so a prefix written under
/// another name fails to validate.
pub(crate) fn desc_digest(path: &str, prefix: &[u8]) -> u64 {
    let basename = path.rsplit('/').next().unwrap_or(path);
    gnitz_wire::digest_with_hole(basename.as_bytes(), prefix, OFF_DESC_CHECKSUM)
}

/// A directory entry's encoding byte.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub(crate) enum Encoding {
    Raw = 0,
    /// One element, read at stride 0.
    Constant = 1,
    /// A weight region of exactly two distinct weights.
    TwoValue = 2,
    /// Frame-of-reference: a payload column of a 2-, 4- or 8-byte integer type.
    For = 3,
    /// Dictionary: a German-string payload column as its distinct cells and one
    /// code per row.
    Dict = 4,
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
        }
    }

    pub(crate) fn from_byte(b: u8) -> Option<Self> {
        Some(match b {
            0 => Encoding::Raw,
            1 => Encoding::Constant,
            2 => Encoding::TwoValue,
            3 => Encoding::For,
            4 => Encoding::Dict,
            _ => return None,
        })
    }
}

#[cfg(test)]
#[path = "tests/layout.rs"]
mod tests;
