//! Cold open-time path for [`MappedShard`]: header + directory validation,
//! region decoding, optional per-region checksum verification, and the XOR8
//! membership-filter load. Runs once per shard open, never per row.

use std::ffi::CStr;

use super::super::error::StorageError;
use super::super::layout::*;
use super::super::xor8;
use super::{MappedShard, Mmap, RegionView};
use crate::foundation::codec::{read_i64_le, read_u64_le};
use crate::foundation::xxh;

impl MappedShard {
    pub fn open(
        path: &CStr,
        schema: &crate::schema::SchemaDescriptor,
        validate_checksums: bool,
    ) -> Result<Self, StorageError> {
        // `Mmap`'s Drop unmaps the file on any early `?` return below — no
        // manual cleanup needed in the validation path.
        let mmap = Mmap::open(path)?;
        let file_size = mmap.len;
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

        let num_cols = schema.num_columns();
        let pk_stride = schema.pk_stride();
        // num_payload_cols is non-PK columns; matches what `Batch::regions`
        // writes (PK + weight + null_bmp + per-payload + blob). The historical
        // `num_cols - 1` form only held for single-PK schemas.
        let num_non_pk = schema.num_payload_cols();
        let num_regions = 3 + num_non_pk + 1;

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

            let enc = data[entry_off + 24];
            // Validate reserved bytes [25..32] are all zero
            for b in &data[entry_off + 25..entry_off + 32] {
                if *b != 0 {
                    return Err(StorageError::InvalidShard);
                }
            }
            // Validate known encoding
            if enc != ENCODING_RAW && enc != ENCODING_CONSTANT && enc != ENCODING_TWO_VALUE {
                return Err(StorageError::InvalidShard);
            }
            let encoding = enc;

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

        // Build RegionViews
        let build_region_view = |e: &DirEntry| -> Result<RegionView, StorageError> {
            match e.encoding {
                ENCODING_CONSTANT => {
                    let mut value = [0u8; 16];
                    if e.size > 0 {
                        let copy_len = e.size.min(16);
                        value[..copy_len].copy_from_slice(&data[e.offset..e.offset + copy_len]);
                    }
                    Ok(RegionView::Constant {
                        value,
                        offset: e.offset,
                    })
                }
                ENCODING_TWO_VALUE => {
                    let expected_bitvec = count.div_ceil(8);
                    if e.size < 16 + expected_bitvec {
                        return Err(StorageError::InvalidShard);
                    }
                    let value_a = read_i64_le(data, e.offset);
                    let value_b = read_i64_le(data, e.offset + 8);
                    let bitvec_off = e.offset + 16;
                    Ok(RegionView::TwoValue {
                        value_a,
                        value_b,
                        bitvec_off,
                    })
                }
                _ => Ok(RegionView::Raw {
                    offset: e.offset,
                    size: e.size,
                }),
            }
        };

        let pk = build_region_view(&entries[0])?;
        let weight = build_region_view(&entries[1])?;
        let null_bmp = build_region_view(&entries[2])?;

        // Reject TwoValue for pk/null_bmp: encoder never emits this.
        if matches!(pk, RegionView::TwoValue { .. }) || matches!(null_bmp, RegionView::TwoValue { .. }) {
            return Err(StorageError::InvalidShard);
        }
        // Wide PK must be Raw: a Constant region holds only a 16-byte `value`,
        // so `get_pk_bytes` would slice `&value[..stride]` out of bounds. The
        // writer never emits Constant for a wide PK (wide strides stay Raw by
        // construction), so this is defense-in-depth against a corrupt or
        // forged file.
        if pk_stride as usize > crate::schema::NARROW_PK_MAX_BYTES && !matches!(pk, RegionView::Raw { .. }) {
            return Err(StorageError::InvalidShard);
        }
        // Validate Raw region sizes before the has_ghosts scan (which reads
        // count*8 bytes from the weight region and would panic on undersize).
        if let RegionView::Raw { size, .. } = &weight {
            if *size < count * 8 {
                return Err(StorageError::InvalidShard);
            }
        }
        if let RegionView::Raw { size, .. } = &pk {
            if *size < count * pk_stride as usize {
                return Err(StorageError::InvalidShard);
            }
        }
        if let RegionView::Raw { size, .. } = &null_bmp {
            if *size < count * 8 {
                return Err(StorageError::InvalidShard);
            }
        }

        // has_ghosts: true only if at least one row actually has weight == 0.
        // For Raw regions we scan the weight data once at open rather than
        // assuming any Raw-encoded shard has ghosts. The drain-then-scatter
        // path writes Raw weights that are all non-zero (drain filters w=0
        // before scatter), so the conservative `Raw => true` caused skip_ghosts
        // to do a full linear scan on every advance for ghost-free shards.
        let has_ghosts = match &weight {
            RegionView::Raw { offset, .. } => (0..count).any(|i| read_i64_le(data, offset + i * 8) == 0),
            RegionView::Constant { value, .. } => i64::from_le_bytes(value[..8].try_into().unwrap()) == 0,
            RegionView::TwoValue { value_a, value_b, .. } => *value_a == 0 || *value_b == 0,
        };

        let mut col_to_payload = Vec::with_capacity(num_cols);
        let mut col_regions = Vec::with_capacity(num_non_pk);
        let mut reg_idx = 3;
        for ci in 0..num_cols {
            if schema.is_pk_col(ci) {
                col_to_payload.push(usize::MAX);
            } else {
                col_to_payload.push(col_regions.len());
                col_regions.push(build_region_view(&entries[reg_idx])?);
                reg_idx += 1;
            }
        }
        // Reject TwoValue for payload columns (encoder never emits this) and
        // validate Raw sizes so to_unified can be infallible.
        for (pi, _ci, col) in schema.payload_columns() {
            match &col_regions[pi] {
                RegionView::TwoValue { .. } => return Err(StorageError::InvalidShard),
                RegionView::Raw { size, .. } if *size < count * col.size() as usize => {
                    return Err(StorageError::InvalidShard);
                }
                _ => {}
            }
        }

        let blob_off = entries[reg_idx].offset;
        let blob_len = entries[reg_idx].size;

        let xor8_off = read_u64_le(data, OFF_XOR8_OFFSET) as usize;
        let xor8_sz = read_u64_le(data, OFF_XOR8_SIZE) as usize;
        let xor8_filter = if xor8_off > 0 && xor8_sz >= 16 && xor8_off + xor8_sz <= file_size {
            xor8::deserialize(&data[xor8_off..xor8_off + xor8_sz])
        } else {
            None
        };

        // Read the flags byte written at OFF_FLAGS (byte 56). Old v7 shards that
        // predate this field have 0 there (written by vec![0u8; total_size]), so
        // they are conservatively treated as ZSet (not PkUnique).
        let is_pk_unique = file_size > OFF_FLAGS && (data[OFF_FLAGS] & SHARD_FLAG_PK_UNIQUE != 0);

        Ok(MappedShard {
            mmap,
            count,
            pk,
            weight,
            null_bmp,
            col_regions,
            blob_off,
            blob_len,
            col_to_payload,
            xor8_filter,
            has_ghosts,
            pk_stride,
            is_pk_unique,
        })
    }
}
