//! Cold open-time path for [`MappedShard`]: header + directory validation,
//! region decoding, optional per-region checksum verification, and the XOR8
//! membership-filter load. Runs once per shard open, never per row.

use std::ffi::CStr;

use super::super::super::repr::batch::{strides_from_schema, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::super::error::StorageError;
use super::super::layout::*;
use super::super::xor8;
use super::{MappedShard, Mmap, PackedRegion, PayloadRegion, ScalarRegion, WeightRegion};
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

        // Validate Raw region sizes up front: the has_ghosts scan and to_unified
        // read `count × stride` bytes and must not overrun.
        for (i, e) in entries.iter().take(nr).enumerate() {
            if e.encoding == ENCODING_RAW && e.size < count * strides[i] as usize {
                return Err(StorageError::InvalidShard);
            }
        }

        // Decode each region. Only the weight region may use the TwoValue
        // encoding, so a forged TwoValue on a scalar region (pk / null_bmp /
        // payload) is rejected at its decode site (`build_scalar_region`)
        // instead of being asserted-against at every per-row accessor.
        let read_const_value = |e: &DirEntry| -> [u8; 16] {
            let mut value = [0u8; 16];
            if e.size > 0 {
                let copy_len = e.size.min(16);
                value[..copy_len].copy_from_slice(&data[e.offset..e.offset + copy_len]);
            }
            value
        };
        let build_scalar_region = |e: &DirEntry| -> Result<ScalarRegion, StorageError> {
            match e.encoding {
                ENCODING_RAW => Ok(ScalarRegion::Raw {
                    offset: e.offset,
                    size: e.size,
                }),
                ENCODING_CONSTANT => Ok(ScalarRegion::Constant {
                    value: read_const_value(e),
                    offset: e.offset,
                }),
                _ => Err(StorageError::InvalidShard),
            }
        };
        // Payload columns are the sole `ENCODING_FOR`-eligible role; every other
        // encoding decodes exactly as a pk / null scalar region. `stride` is the
        // region's per-element width (validates `bw < stride` for a FoR region).
        let build_payload_region = |e: &DirEntry, stride: usize| -> Result<PayloadRegion, StorageError> {
            if e.encoding != ENCODING_FOR {
                return build_scalar_region(e).map(PayloadRegion::Scalar);
            }
            // Decoder panic / OOB surface — these three checks make
            // `decode_for_region` pure arithmetic over in-bounds slices:
            // (1) count > 0 guards the divisor (the writer never emits an
            // empty FoR region — n == 0 short-circuits to Raw of size 0);
            // (2) size >= 8 guards the `size − 8` subtraction against a
            // truncated entry; (3) an exact `size == 8 + count·bw` with
            // `1 <= bw < stride` rejects trailing / short bytes.
            if count == 0 || e.size < 8 {
                return Err(StorageError::InvalidShard);
            }
            let bw = (e.size - 8) / count;
            if bw < 1 || bw >= stride || e.size != 8 + count * bw {
                return Err(StorageError::InvalidShard);
            }
            Ok(PayloadRegion::Packed(PackedRegion {
                offset: e.offset,
                size: e.size,
                elem_width: stride,
                decoded: std::cell::OnceCell::new(),
            }))
        };
        let build_weight_region = |e: &DirEntry| -> Result<WeightRegion, StorageError> {
            match e.encoding {
                ENCODING_CONSTANT => Ok(WeightRegion::Constant {
                    value: read_const_value(e),
                }),
                ENCODING_TWO_VALUE => {
                    let expected_bitvec = count.div_ceil(8);
                    if e.size < 16 + expected_bitvec {
                        return Err(StorageError::InvalidShard);
                    }
                    Ok(WeightRegion::TwoValue {
                        value_a: read_i64_le(data, e.offset),
                        value_b: read_i64_le(data, e.offset + 8),
                        bitvec_off: e.offset + 16,
                    })
                }
                ENCODING_RAW => Ok(WeightRegion::Raw { offset: e.offset }),
                // The weight region's legal set is Raw/Constant/TwoValue; anything
                // else (e.g. a forged ENCODING_FOR) is rejected.
                _ => Err(StorageError::InvalidShard),
            }
        };

        let pk = build_scalar_region(&entries[REG_PK])?;
        let weight = build_weight_region(&entries[REG_WEIGHT])?;
        let null_bmp = build_scalar_region(&entries[REG_NULL_BMP])?;

        // Wide PK must be Raw: a Constant region holds only a 16-byte `value`,
        // so `get_pk_bytes` would slice `&value[..stride]` out of bounds. The
        // writer never emits Constant for a wide PK (wide strides stay Raw by
        // construction), so this is defense-in-depth against a corrupt or
        // forged file.
        if pk_stride as usize > crate::schema::NARROW_PK_MAX_BYTES && !matches!(pk, ScalarRegion::Raw { .. }) {
            return Err(StorageError::InvalidShard);
        }

        // has_ghosts: true only if at least one row actually has weight == 0.
        // For Raw regions we scan the weight data once at open rather than
        // assuming any Raw-encoded shard has ghosts. The drain-then-scatter
        // path writes Raw weights that are all non-zero (drain filters w=0
        // before scatter), so the conservative `Raw => true` caused skip_ghosts
        // to do a full linear scan on every advance for ghost-free shards.
        let has_ghosts = match &weight {
            WeightRegion::Raw { offset, .. } => (0..count).any(|i| read_i64_le(data, offset + i * 8) == 0),
            WeightRegion::Constant { value } => i64::from_le_bytes(value[..8].try_into().unwrap()) == 0,
            WeightRegion::TwoValue { value_a, value_b, .. } => *value_a == 0 || *value_b == 0,
        };

        let mut col_to_payload = Vec::with_capacity(num_cols);
        let mut col_regions = Vec::with_capacity(nr - REG_PAYLOAD_START);
        let mut reg_idx = REG_PAYLOAD_START;
        for ci in 0..num_cols {
            if schema.is_pk_col(ci) {
                col_to_payload.push(usize::MAX);
            } else {
                col_to_payload.push(col_regions.len());
                col_regions.push(build_payload_region(&entries[reg_idx], strides[reg_idx] as usize)?);
                reg_idx += 1;
            }
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

        // Read the flags byte written at OFF_FLAGS (byte 56). The `file_size`
        // guard is a defensive backstop; a well-formed shard always carries it.
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
