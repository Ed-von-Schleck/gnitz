//! How a shard file is framed: its header, the directory of its regions and
//! where each lies, and the digest over the bytes that decide how the rest is
//! read. What a region's bytes mean is `encoding`'s.

use super::encoding::Encoding;
use crate::repr::error::StorageError;
use gnitz_wire::{read_u64_le, write_u64_le};

use StorageError::Corrupt;

pub(crate) const SHARD_MAGIC: u64 = 0x31305F5A54494E47;
/// Bumped by hand for any change to the bytes a writer produces;
/// `shard_bytes_are_pinned` fails until it is.
pub(crate) const SHARD_EPOCH: u64 = 27;

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
/// How many rows carry a negative weight (u64 LE).
pub(crate) const OFF_RETRACTIONS: usize = 56;
pub(crate) const HEADER_SIZE: usize = OFF_RETRACTIONS + 8;

/// [`OFF_FLAGS`] bit: a capacity-bounded view's skeleton shard, one (PK, coarse
/// weight) row per key.
pub(crate) const SHARD_FLAG_SKELETON: u64 = 1;

/// The header fields a reader acts on; [`read`](Self::read) checks magic and
/// version.
#[derive(Clone, Copy)]
pub(crate) struct ShardHeader {
    pub row_count: usize,
    pub retractions: usize,
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
            retractions: read_u64_le(data, OFF_RETRACTIONS) as usize,
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
        write_u64_le(header, OFF_RETRACTIONS, self.retractions as u64);
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
            let encoding = Encoding::from_wire(encoding).ok_or(Corrupt("encoding"))?;
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

/// Every region of the well-formed shard `image` placed in it.
#[cfg(test)]
pub(crate) fn spans_of(image: &[u8]) -> Vec<Span> {
    region_spans(image, ShardHeader::read(image).unwrap().file_npc).unwrap()
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
