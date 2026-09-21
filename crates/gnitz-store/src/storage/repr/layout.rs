//! Shared shard file format constants, and the digest over the bytes that
//! decide how the rest of a shard is read.

use std::ffi::CStr;

use super::super::error::StorageError;

pub(crate) const SHARD_MAGIC: u64 = 0x31305F5A54494E47;
/// Bumped by hand for a header/region layout change.
pub(crate) const SHARD_EPOCH: u64 = 20;

/// Shard file format version, written into the header and compared for equality
/// at open. A shard records only its payload-column count (`OFF_FILE_NPC`) and
/// sizes every region from the live `SchemaDescriptor`, so a system-family shape
/// change reinterprets an existing file rather than failing to parse it — hence
/// the digest. Compared, never parsed, so any mixing function does.
pub(crate) const SHARD_VERSION: u64 = SHARD_EPOCH ^ gnitz_wire::SYS_SCHEMA_DIGEST;
pub(crate) const HEADER_SIZE: usize = 56;
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
/// XXH3-64 over every region image, in directory order.
pub(crate) const OFF_BODY_CHECKSUM: usize = 48;

/// [`OFF_FLAGS`] bit: a capacity-bounded view's skeleton shard, one (PK, coarse
/// weight) row per key.
pub(crate) const SHARD_FLAG_SKELETON: u64 = 1;

/// The shard filter's region is `[descriptor: DMA_LEN][fingerprints]`, split at
/// a constant rather than at a framed length — so the dependency's descriptor
/// width is part of the on-disk format, and a change to it would reinterpret
/// every fingerprint byte of every existing shard. It has to fail the build
/// instead: bump `SHARD_EPOCH` and paste the new width here.
const _: () = assert!(
    xorf::Descriptor::DMA_LEN == 20,
    "BinaryFuse8 descriptor width changed: bump SHARD_EPOCH",
);

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
            size: gnitz_wire::read_u64_le(image, d) as usize,
            encoding: image[d + 8],
        }
    }

    pub(crate) fn write(&self, image: &mut [u8], i: usize) {
        let d = dir_entry_off(i);
        gnitz_wire::write_u64_le(image, d, self.size as u64);
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
    pub encoding: u8,
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
    if end > file_size {
        return Err(StorageError::Corrupt("shorter than its directory"));
    }
    let spans = (0..=gnitz_wire::region::num_regions(file_npc))
        .map(|i| {
            let DirEntry { size, encoding } = DirEntry::read(image, i);
            let off = region_start(end);
            if off > file_size || size > file_size - off {
                return Err(StorageError::Corrupt("region past the end"));
            }
            end = off + size;
            Ok(Span { off, size, encoding })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if end != file_size {
        return Err(StorageError::Corrupt("directory does not span the file"));
    }
    Ok(spans)
}

/// A `TwoValue` region's image: `value_a` LE ‖ `value_b` LE ‖ a `count`-bit
/// vector, bit *i* set ⇔ row *i* holds `value_b`. Encoder, open-time size check
/// and both read paths state the geometry only through these three.
pub(crate) const TWO_VALUE_HEADER: usize = 16;

pub(crate) const fn two_value_image_len(count: usize) -> usize {
    TWO_VALUE_HEADER + count.div_ceil(8)
}

/// True when `row` holds `value_b`. `bitvec` starts at the image's
/// [`TWO_VALUE_HEADER`] offset.
#[inline]
pub(crate) fn two_value_bit(bitvec: &[u8], row: usize) -> bool {
    (bitvec[row / 8] >> (row % 8)) & 1 != 0
}

#[inline]
pub(crate) fn two_value_set_bit(bitvec: &mut [u8], row: usize) {
    bitvec[row / 8] |= 1 << (row % 8);
}

/// A FoR region's image: an 8-byte frame reference ‖ each row's offset from it in
/// its low `bw` bytes, tightly packed. Encoder, open-time check and decoder state
/// the geometry only through these three.
pub(crate) const FOR_HEADER: usize = 8;

pub(crate) const fn for_image_len(count: usize, bw: usize) -> usize {
    FOR_HEADER + count * bw
}

/// The offset width an image of `size` bytes holds for `count ≥ 1` rows, or
/// `None` unless some `bw` in `1..elem_width` gives exactly that size.
pub(crate) fn for_image_bw(size: usize, count: usize, elem_width: usize) -> Option<usize> {
    let bw = size.checked_sub(FOR_HEADER)? / count;
    ((1..elem_width).contains(&bw) && size == for_image_len(count, bw)).then_some(bw)
}

/// Header plus directory, by the file's own payload arity.
pub(crate) const fn desc_len(file_npc: usize) -> usize {
    dir_entry_off(gnitz_wire::region::num_regions(file_npc) + 1)
}

/// XXH3-64 over a shard's descriptive prefix (header + directory), its own eight
/// bytes excluded, seeded with the shard's basename.
///
/// The seed binds a prefix to the name it was written under, so a prefix that
/// arrives from elsewhere — a rename, or a misdirected write carrying a
/// same-shaped neighbour's first sector — fails to validate. It separates names,
/// not directories: the naming grammar has no directory component, so a spill
/// name repeats across sibling partition directories and seeds identically.
pub(crate) fn desc_digest(path: &CStr, prefix: &[u8]) -> u64 {
    gnitz_wire::digest_with_hole(shard_basename(path.to_bytes()), prefix, OFF_DESC_CHECKSUM)
}

/// A shard's manifest identity: the last component of its path. Every writer and
/// the reader hold a full path; the manifest identity and the digest seed are
/// its last component, so the name the manifest records is the name the digest
/// is seeded with.
pub(crate) fn shard_basename(path: &[u8]) -> &[u8] {
    match path.iter().rposition(|&c| c == b'/') {
        Some(i) => &path[i + 1..],
        None => path,
    }
}

pub(crate) const ENCODING_RAW: u8 = 0x00;
pub(crate) const ENCODING_CONSTANT: u8 = 0x01;
pub(crate) const ENCODING_TWO_VALUE: u8 = 0x02;
/// Frame-of-reference + byte-width truncation for an integer payload region:
/// an 8-byte frame reference (the region min's bit pattern) followed by each
/// row's `value − ref` truncated to the fewest whole bytes (`bw`) that hold the
/// region's offset range. Legal only on payload column directory entries, only
/// where the writer packs integers (`ShardWriteOpts::pack_ints`), only on a
/// ≤8-byte integer column.
pub(crate) const ENCODING_FOR: u8 = 0x03;
