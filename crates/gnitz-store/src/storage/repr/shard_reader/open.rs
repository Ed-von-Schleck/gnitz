//! Cold open-time path for [`MappedShard`]: header + directory validation,
//! region decoding, optional per-region checksum verification, and the PK
//! membership-filter load. Runs once per shard open, never per row.
//!
//! Two digests are unconditional, over the two spans this path reads in full
//! regardless of `validate_checksums`: the descriptive prefix (header +
//! directory), which decides how every payload byte is interpreted, and the
//! filter region, whose descriptor drives the fingerprint indexing a probe
//! performs — a corrupt one panics rather than answering wrongly, so the digest
//! is what keeps it unreachable. The payload regions stay behind
//! `validate_checksums` — they are demand-paged, so hashing them here would
//! fault in a whole shard for a point lookup. Only compaction passes it; the
//! boot/reload path opens with it off.

use std::ffi::CStr;

use super::super::batch::{
    FIXED_REGION_BYTES, MAX_PAYLOAD_REGIONS, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT,
};
use super::super::error::StorageError;
use super::super::layout::*;
use super::super::merge::ColPtr;
use super::super::shard_filter;
use super::{MappedShard, PackedRegion, PayloadRegion, WeightRegion, ZERO_CELL};
use crate::schema::SchemaColumn;
use gnitz_foundation::posix_io::{Advice, Mmap};
use gnitz_wire::{read_i64_le, read_u64_le};

impl MappedShard {
    pub(crate) fn open(
        path: &CStr,
        schema: &crate::schema::SchemaDescriptor,
        validate_checksums: bool,
    ) -> Result<Self, StorageError> {
        // `Mmap`'s Drop unmaps the file on any early `?` return below — no
        // manual cleanup needed in the validation path.
        // `validate_checksums` means "xxh3 every region now" — the only open
        // that reads the file front to back. The rest binary-search payload
        // regions left demand-paged, where readahead is I/O never used.
        let advice = if validate_checksums {
            Advice::Sequential
        } else {
            Advice::Default
        };
        let mmap = Mmap::open_ro(path, advice).map_err(|e| match e.raw_os_error() {
            Some(n) => StorageError::Io(n),
            // `open_ro` reports a zero-length file with no errno; that is the
            // same "shorter than the header" verdict as the check below.
            None => StorageError::Truncated,
        })?;
        let data = mmap.as_slice();
        let file_size = data.len();
        if file_size < HEADER_SIZE {
            return Err(StorageError::Truncated);
        }

        if read_u64_le(data, OFF_MAGIC) != SHARD_MAGIC {
            return Err(StorageError::InvalidMagic);
        }
        let version = read_u64_le(data, OFF_VERSION);
        if version != SHARD_VERSION {
            return Err(StorageError::InvalidVersion);
        }

        let pk_stride = schema.pk_stride();

        // The file's own payload-column count, bounded before any arithmetic on
        // it: `desc_len` is `HEADER_SIZE + n · DIR_ENTRY_SIZE` and overflows on a
        // forged u64. The skeleton flag rides the same word's high bit and is
        // masked off before that bound, so a forged high bit cannot smuggle a
        // count past it.
        let npc_word = read_u64_le(data, OFF_FILE_NPC);
        let skeleton = npc_word & SHARD_FLAG_SKELETON != 0;
        let file_npc = (npc_word & !SHARD_FLAG_SKELETON) as usize;
        if file_npc > MAX_PAYLOAD_REGIONS {
            return Err(StorageError::InvalidShard);
        }
        // The file's blob-region index, and with it the directory length the
        // digest covers.
        let file_nr = REG_PAYLOAD_START + file_npc;
        let num_regions = file_nr + 1;

        // Ahead of every structural check, so no forged byte reaches unchecked
        // arithmetic. The span's length comes from the file's own arity, so a
        // forged `file_npc` moves the digest span *and* sits inside it: it fails
        // the digest exactly as any other forged descriptive byte does.
        if desc_len(num_regions) > file_size {
            return Err(StorageError::Truncated);
        }
        if desc_digest(shard_basename(path.to_bytes()), data, num_regions) != read_u64_le(data, OFF_DESC_CHECKSUM) {
            return Err(StorageError::ChecksumMismatch);
        }

        let count = read_u64_le(data, OFF_ROW_COUNT) as usize;

        // Parse directory entries. Each entry's encoding byte is validated per
        // role by the region builders below — each role accepts exactly its legal
        // encoding set, so an unknown or misplaced byte is rejected at its decode
        // site. The reserved bytes [25,32) get no check of their own: the digest
        // above already covers them.
        let mut entries: Vec<DirEntry> = Vec::with_capacity(num_regions);
        for i in 0..num_regions {
            let e = DirEntry::read(data, i);
            // Keeps the region slice inside the mapping — a file truncated below
            // an intact prefix would slice past its end.
            if e.offset.saturating_add(e.size) > file_size {
                return Err(StorageError::InvalidShard);
            }
            entries.push(e);
        }

        // Validate checksums
        if validate_checksums {
            for e in &entries {
                if e.size > 0 && gnitz_wire::checksum(&data[e.offset..e.offset + e.size]) != e.checksum {
                    return Err(StorageError::ChecksumMismatch);
                }
            }
        }

        // Resolve a fixed-width region to its `(base, stride)` pointer, with the
        // size check that lets every accessor read `stride`-wide elements without
        // re-validating: Raw must carry `count` of them, Constant exactly one
        // (`stride == 0` then makes every row read that one). Only the weight
        // region may be TwoValue, so a forged TwoValue here is rejected at this
        // decode site instead of being asserted-against at every accessor.
        let direct_region = |e: &DirEntry, elem_width: usize| -> Result<ColPtr, StorageError> {
            let (stride, needed) = match e.encoding {
                ENCODING_RAW => (elem_width, count * elem_width),
                ENCODING_CONSTANT => (0, elem_width),
                _ => return Err(StorageError::InvalidShard),
            };
            // The writer emits every region at exactly its encoding's size, so an
            // inexact size here is corruption rather than a legal variant.
            if e.size != needed {
                return Err(StorageError::InvalidShard);
            }
            // SAFETY: `offset + size <= file_size` was checked for every entry.
            Ok(ColPtr {
                base: unsafe { data.as_ptr().add(e.offset) },
                stride,
            })
        };
        // Payload columns are the sole `ENCODING_FOR`-eligible role, and only for
        // the column types the writer packs: the codec widens whole integer
        // cells, so a forged FoR byte on a STRING or float column is rejected.
        let build_payload_region = |e: &DirEntry, col: &SchemaColumn| -> Result<PayloadRegion, StorageError> {
            if e.encoding != ENCODING_FOR {
                return direct_region(e, col.size() as usize).map(PayloadRegion::Mapped);
            }
            let fi = col.fixed_int().ok_or(StorageError::InvalidShard)?;
            let bw = for_image_bw(e.size, count, fi.width()).ok_or(StorageError::InvalidShard)?;
            Ok(PayloadRegion::Packed(PackedRegion {
                offset: e.offset,
                bw,
                elem_width: fi.width(),
                decoded: std::cell::OnceCell::new(),
            }))
        };
        let build_weight_region = |e: &DirEntry| -> Result<WeightRegion, StorageError> {
            if e.encoding == ENCODING_TWO_VALUE {
                if e.size != two_value_image_len(count) {
                    return Err(StorageError::InvalidShard);
                }
                return Ok(WeightRegion::TwoValue {
                    value_a: read_i64_le(data, e.offset),
                    value_b: read_i64_le(data, e.offset + 8),
                    bitvec_off: e.offset + TWO_VALUE_HEADER,
                });
            }
            direct_region(e, FIXED_REGION_BYTES).map(WeightRegion::Mapped)
        };

        let pk = direct_region(&entries[REG_PK], pk_stride)?;
        let weight = build_weight_region(&entries[REG_WEIGHT])?;
        let null_bmp = direct_region(&entries[REG_NULL_BMP], FIXED_REGION_BYTES)?;

        // `min` in both directions, and neither side is optional. A column the
        // *file* does not carry has no directory entry; a column the *schema*
        // does not describe cannot be built at all, since `build_payload_region`
        // needs the schema's column to validate a region. The wider-file direction is reachable from a correct crash —
        // a checkpoint publishes base manifests before making the catalog
        // durable — so it is narrowed, not rejected; rejecting it would make the
        // database unbootable. Reusing the current schema's columns for the
        // file's own columns is sound because no ALTER changes an existing
        // column's `type_code` or payload position.
        let schema_npc = schema.num_payload_cols();
        let mapped = file_npc.min(schema_npc);
        // One region per payload column of the *reader's* schema, so every
        // reader indexes it directly. Indices `[mapped, schema_npc)` name columns
        // this file predates and read `ZERO_CELL`.
        let mut col_regions = Vec::with_capacity(schema_npc);
        for (pi, col) in schema.payload_columns() {
            if pi >= mapped {
                col_regions.push(PayloadRegion::Mapped(ColPtr { base: ZERO_CELL.as_ptr(), stride: 0 }));
                continue;
            }
            col_regions.push(build_payload_region(&entries[REG_PAYLOAD_START + pi], col)?);
        }
        // Old rows wrote `0` in the null bit of a column the file predates, which reads as
        // "non-null"; this forces them to `1`. The naive `(1 << schema_npc) - 1`
        // is UB at 64.
        let null_pad_mask = gnitz_wire::all_payload_null_mask(schema_npc) & !gnitz_wire::all_payload_null_mask(mapped);

        // The blob region is always Raw; reject any other (forged) encoding.
        if entries[file_nr].encoding != ENCODING_RAW {
            return Err(StorageError::InvalidShard);
        }
        let blob_off = entries[file_nr].offset;
        let blob_len = entries[file_nr].size;

        // A zero offset is the filterless marker and the only one: anything
        // else must parse or the open fails, the way every other structural
        // defect here does. The checksum is unconditional — `read` touches
        // every one of these bytes anyway, and a corrupt descriptor panics a
        // probe rather than answering wrongly.
        let off = read_u64_le(data, OFF_SHARD_FILTER_OFFSET) as usize;
        let sz = read_u64_le(data, OFF_SHARD_FILTER_SIZE) as usize;
        let shard_filter = if off == 0 {
            None
        } else {
            // Both operands come from the header, so their sum can wrap past the
            // bound below and then panic on the slice.
            let end = off.checked_add(sz).ok_or(StorageError::InvalidShard)?;
            if end > file_size {
                return Err(StorageError::InvalidShard);
            }
            let bytes = &data[off..end];
            if gnitz_wire::checksum(bytes) != read_u64_le(data, OFF_SHARD_FILTER_CHECKSUM) {
                return Err(StorageError::ChecksumMismatch);
            }
            Some(shard_filter::ShardFilter::parse(bytes, off).ok_or(StorageError::InvalidShard)?)
        };

        Ok(MappedShard {
            mmap,
            count,
            pk,
            weight,
            null_bmp,
            col_regions,
            null_pad_mask,
            blob_off,
            blob_len,
            shard_filter,
            pk_stride,
            skeleton,
        })
    }
}
