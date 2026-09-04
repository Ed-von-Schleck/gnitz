//! Shared shard file format constants, and the digest over the bytes that
//! decide how the rest of a shard is read.

use crate::foundation::xxh;

pub(crate) const SHARD_MAGIC: u64 = 0x31305F5A54494E47;
/// Shard file format version. A shard records only its payload-column count
/// (`OFF_FILE_NPC`) — every other aspect of a region is derived from the live
/// `SchemaDescriptor` — so a column-shape change to a system family needs a bump
/// here too, not just a header/region layout change. The reader's exact region
/// sizes come from that schema, so they reject most shards written under a
/// different one; a Constant-encoded region is one element wide under either
/// schema, and only this word rejects that.
pub(crate) const SHARD_VERSION: u64 = 18;

/// Pin the system-family column shapes to the version word above, the way
/// `gnitz_wire::wal` pins them to `WAL_FORMAT_VERSION`. Nothing else notices a
/// shape change. If this fails, bump `SHARD_VERSION` and paste the reported
/// digest here.
const _: () = assert!(
    gnitz_wire::SYS_SCHEMA_DIGEST == 2556830436675219726 && SHARD_VERSION == 18,
    "system-family column shapes changed: bump SHARD_VERSION"
);
pub(crate) const HEADER_SIZE: usize = 64;
pub(crate) const DIR_ENTRY_SIZE: usize = 32;
pub(crate) const ALIGNMENT: usize = 64;

pub(crate) const OFF_MAGIC: usize = 0;
pub(crate) const OFF_VERSION: usize = 8;
pub(crate) const OFF_ROW_COUNT: usize = 16;
pub(crate) const OFF_DESC_CHECKSUM: usize = 24;
/// The writer's `schema.num_payload_cols()` (u64 LE) — the file's own arity,
/// which fixes its region count and blob-region index. A reader whose schema is
/// wider (post-`ALTER TABLE … ADD COLUMN`) walks the directory by this count and
/// pads the columns past it to NULL.
pub(crate) const OFF_FILE_NPC: usize = 32;

/// High bit of [`OFF_FILE_NPC`]: the file's payload columns are absent *by
/// design* — it is a capacity-bounded view's skeleton shard, whose rows are one
/// (PK, coarse weight) pair each. The low bits stay the writer's payload-column
/// count (0 for a skeleton), which the reader bounds against
/// `MAX_PAYLOAD_REGIONS`; this bit is masked off before that check.
///
/// A bare `file_npc == 0` would not say this: an all-PK base table widened by
/// `ALTER TABLE … ADD COLUMN` produces the identical region shape with genuine
/// NULL semantics, and the read path's coarsening comparators must not treat
/// that table's cross-tier retract/insert pairs as one key. Bits 8..63 of the
/// word are otherwise unused and already inside [`desc_digest`]'s span, so the
/// bit is as unforgeable as any other descriptive byte.
pub(crate) const SHARD_FLAG_SKELETON: u64 = 1 << 63;
pub(crate) const OFF_SHARD_FILTER_OFFSET: usize = 40;
pub(crate) const OFF_SHARD_FILTER_SIZE: usize = 48;
pub(crate) const OFF_SHARD_FILTER_CHECKSUM: usize = 56;

/// The shard filter's region is `[descriptor: DMA_LEN][fingerprints]`, split at
/// a constant rather than at a framed length — so the dependency's descriptor
/// width is part of the on-disk format, and a change to it would reinterpret
/// every fingerprint byte of every existing shard. It has to fail the build
/// instead: bump `SHARD_VERSION` and paste the new width here.
const _: () = assert!(
    xorf::Descriptor::DMA_LEN == 20,
    "BinaryFuse8 descriptor width changed: bump SHARD_VERSION",
);

/// Byte offset of directory entry `i`. The directory follows the header
/// immediately, so an entry's position is implied by its index — the file
/// stores no directory offset to disagree with this.
pub(crate) const fn dir_entry_off(i: usize) -> usize {
    HEADER_SIZE + i * DIR_ENTRY_SIZE
}

/// One region's directory entry: `offset` ‖ `size` ‖ `checksum` (u64 LE each),
/// the encoding byte at +24, the rest reserved (and covered by [`desc_digest`]).
/// The writer, the open-time validation and the format tests all go through this
/// pair, so neither side can drift on the field order.
pub(crate) struct DirEntry {
    pub offset: usize,
    pub size: usize,
    pub checksum: u64,
    pub encoding: u8,
}

impl DirEntry {
    pub(crate) fn read(image: &[u8], i: usize) -> Self {
        let d = dir_entry_off(i);
        DirEntry {
            offset: gnitz_wire::read_u64_le(image, d) as usize,
            size: gnitz_wire::read_u64_le(image, d + 8) as usize,
            checksum: gnitz_wire::read_u64_le(image, d + 16),
            encoding: image[d + 24],
        }
    }

    pub(crate) fn write(&self, image: &mut [u8], i: usize) {
        let d = dir_entry_off(i);
        gnitz_wire::write_u64_le(image, d, self.offset as u64);
        gnitz_wire::write_u64_le(image, d + 8, self.size as u64);
        gnitz_wire::write_u64_le(image, d + 16, self.checksum);
        image[d + 24] = self.encoding;
    }
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

/// Length of the descriptive prefix — header plus one directory entry per
/// region — which is exactly the span [`desc_digest`] covers.
pub(crate) const fn desc_len(num_regions: usize) -> usize {
    dir_entry_off(num_regions)
}

/// XXH3-64 over a shard's descriptive prefix (header + directory), its own eight
/// bytes excluded, seeded with the shard's basename.
///
/// The seed binds a prefix to the name it was written under, so a prefix that
/// arrives from elsewhere — a rename, or a misdirected write carrying a
/// same-shaped neighbour's first sector — fails to validate. It separates names,
/// not directories: `spill_shard_name` is `shard_{table_id}_{lsn}.db` and every
/// child of a partitioned table shares the `table_id`, so the same name in two
/// sibling partition directories seeds identically.
pub(crate) fn desc_digest(basename: &[u8], data: &[u8], num_regions: usize) -> u64 {
    xxh::digest_with_hole(basename, &data[..desc_len(num_regions)], OFF_DESC_CHECKSUM)
}

/// A shard's manifest identity: the last component of its path. The L0 spill
/// writer names a shard by a dirfd-relative basename, compaction by an
/// `AT_FDCWD` full path, and the reader always holds a full path — this is the
/// one reduction all of them use, so the name the manifest records is the name
/// the digest is seeded with.
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
/// on compaction outputs.
pub(crate) const ENCODING_FOR: u8 = 0x03;
