//! Cold open-time path for [`MappedShard`]: header + directory validation,
//! region decoding, optional per-region checksum verification, and the XOR8
//! membership-filter load. Runs once per shard open, never per row.

use std::ffi::CStr;

use super::super::batch::{
    strides_from_schema, FIXED_REGION_BYTES, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT,
};
use super::super::error::StorageError;
use super::super::layout::*;
use super::super::xor8;
use super::{MappedShard, Mmap, PackedRegion, PayloadRegion, RegionView, WeightRegion};
use crate::foundation::xxh;
use gnitz_wire::{read_i64_le, read_u64_le};

impl MappedShard {
    pub fn open(
        path: &CStr,
        schema: &crate::schema::SchemaDescriptor,
        validate_checksums: bool,
    ) -> Result<Self, StorageError> {
        // `Mmap`'s Drop unmaps the file on any early `?` return below — no
        // manual cleanup needed in the validation path.
        let mmap = Mmap::open_ro(path).map_err(|_| StorageError::Io)?;
        let file_size = mmap.len();
        if file_size < HEADER_SIZE {
            return Err(StorageError::Truncated);
        }
        let data = mmap.as_slice();

        if read_u64_le(data, OFF_MAGIC) != SHARD_MAGIC {
            return Err(StorageError::InvalidMagic);
        }
        let version = read_u64_le(data, OFF_VERSION);
        if version != SHARD_VERSION {
            return Err(StorageError::InvalidVersion);
        }

        let count = read_u64_le(data, OFF_ROW_COUNT) as usize;
        let dir_off = read_u64_le(data, OFF_DIR_OFFSET) as usize;

        let pk_stride = schema.pk_stride();
        // Writer↔reader region-layout contract, shared with
        // `write_shard_streaming`: `strides` holds each fixed-width region's
        // per-element width, `nr` is the trailing blob region's index.
        let (strides, nr) = strides_from_schema(schema);
        let nr = nr as usize;
        let num_regions = nr + 1;

        // Parse directory entries
        struct DirEntry {
            offset: usize,
            size: usize,
            checksum: u64,
            encoding: u8,
        }

        let mut entries: Vec<DirEntry> = Vec::with_capacity(num_regions);
        for i in 0..num_regions {
            let entry_off = dir_off + i * DIR_ENTRY_SIZE;
            if entry_off + DIR_ENTRY_SIZE > file_size {
                return Err(StorageError::InvalidShard);
            }
            let r_off = read_u64_le(data, entry_off) as usize;
            let r_sz = read_u64_le(data, entry_off + 8) as usize;
            let r_cs = read_u64_le(data, entry_off + 16);

            let encoding = data[entry_off + 24];
            // Validate reserved bytes [25..32] are all zero
            for b in &data[entry_off + 25..entry_off + 32] {
                if *b != 0 {
                    return Err(StorageError::InvalidShard);
                }
            }
            // The encoding byte is validated per role by the region builders
            // below — each role accepts exactly its legal encoding set, so an
            // unknown or misplaced byte is rejected at its decode site.

            if r_off.saturating_add(r_sz) > file_size {
                return Err(StorageError::InvalidShard);
            }
            entries.push(DirEntry {
                offset: r_off,
                size: r_sz,
                checksum: r_cs,
                encoding,
            });
        }

        // Validate checksums
        if validate_checksums {
            for e in &entries {
                if e.size > 0 && xxh::checksum(&data[e.offset..e.offset + e.size]) != e.checksum {
                    return Err(StorageError::ChecksumMismatch);
                }
            }
        }

        // Normalize a fixed-width region to its `(offset, stride)` view, with the
        // size check that lets every accessor read `stride`-wide elements without
        // re-validating: Raw must carry `count` of them, Constant exactly one
        // (`stride == 0` then makes every row read that one). Only the weight
        // region may be TwoValue, so a forged TwoValue here is rejected at this
        // decode site instead of being asserted-against at every accessor.
        let direct_region = |e: &DirEntry, elem_width: usize| -> Result<RegionView, StorageError> {
            let (stride, needed) = match e.encoding {
                ENCODING_RAW => (elem_width, count * elem_width),
                ENCODING_CONSTANT => (0, elem_width.min(count * elem_width)),
                _ => return Err(StorageError::InvalidShard),
            };
            if e.size < needed {
                return Err(StorageError::InvalidShard);
            }
            Ok(RegionView {
                offset: e.offset,
                stride,
            })
        };
        // Payload columns are the sole `ENCODING_FOR`-eligible role, and only for
        // fixed-int types — the FoR codec widens whole integer cells, so a forged
        // FoR byte on a STRING or float column would drive `decode_for_region`
        // past its `bw < 8` contract. `elem_width` is the region's per-element
        // width (which also bounds `bw` for a FoR region).
        let build_payload_region =
            |e: &DirEntry, elem_width: usize, type_code: u8| -> Result<PayloadRegion, StorageError> {
                if e.encoding != ENCODING_FOR {
                    return direct_region(e, elem_width).map(PayloadRegion::Direct);
                }
                if !gnitz_wire::is_fixed_int(type_code) {
                    return Err(StorageError::InvalidShard);
                }
                // Decoder panic / OOB surface — these three checks make
                // `decode_for_region` pure arithmetic over in-bounds slices:
                // (1) count > 0 guards the divisor (the writer never emits an
                // empty FoR region — n == 0 short-circuits to Raw of size 0);
                // (2) size >= 8 guards the `size − 8` subtraction against a
                // truncated entry; (3) an exact `size == 8 + count·bw` with
                // `1 <= bw < elem_width` rejects trailing / short bytes.
                if count == 0 || e.size < 8 {
                    return Err(StorageError::InvalidShard);
                }
                let bw = (e.size - 8) / count;
                if bw < 1 || bw >= elem_width || e.size != 8 + count * bw {
                    return Err(StorageError::InvalidShard);
                }
                Ok(PayloadRegion::Packed(PackedRegion {
                    offset: e.offset,
                    size: e.size,
                    elem_width,
                    decoded: std::cell::OnceCell::new(),
                }))
            };
        let build_weight_region = |e: &DirEntry| -> Result<WeightRegion, StorageError> {
            if e.encoding == ENCODING_TWO_VALUE {
                if e.size < 16 + count.div_ceil(8) {
                    return Err(StorageError::InvalidShard);
                }
                return Ok(WeightRegion::TwoValue {
                    value_a: read_i64_le(data, e.offset),
                    value_b: read_i64_le(data, e.offset + 8),
                    bitvec_off: e.offset + 16,
                });
            }
            direct_region(e, FIXED_REGION_BYTES).map(WeightRegion::Direct)
        };

        let pk = direct_region(&entries[REG_PK], pk_stride as usize)?;
        let weight = build_weight_region(&entries[REG_WEIGHT])?;
        let null_bmp = direct_region(&entries[REG_NULL_BMP], FIXED_REGION_BYTES)?;

        let mut col_regions = Vec::with_capacity(nr - REG_PAYLOAD_START);
        for (pi, col) in schema.payload_columns() {
            let reg_idx = REG_PAYLOAD_START + pi;
            col_regions.push(build_payload_region(
                &entries[reg_idx],
                strides[reg_idx] as usize,
                col.type_code,
            )?);
        }

        // The blob region is always Raw; reject any other (forged) encoding.
        if entries[nr].encoding != ENCODING_RAW {
            return Err(StorageError::InvalidShard);
        }
        let blob_off = entries[nr].offset;
        let blob_len = entries[nr].size;

        let xor8_off = read_u64_le(data, OFF_XOR8_OFFSET) as usize;
        let xor8_sz = read_u64_le(data, OFF_XOR8_SIZE) as usize;
        let xor8_filter = if xor8_off > 0 && xor8_sz >= 16 && xor8_off + xor8_sz <= file_size {
            xor8::deserialize(&data[xor8_off..xor8_off + xor8_sz])
        } else {
            None
        };

        Ok(MappedShard {
            mmap,
            count,
            pk,
            weight,
            null_bmp,
            col_regions,
            blob_off,
            blob_len,
            xor8_filter,
            pk_stride,
        })
    }
}
