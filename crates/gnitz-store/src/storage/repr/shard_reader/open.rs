//! Cold open-time path for [`MappedShard`]: header + directory validation,
//! region decoding and the PK membership-filter load. Runs once per handle,
//! never per row.
//!
//! Opening verifies the header and directory; the body is verified only by
//! [`MappedShard::verify_body`].

use std::ffi::CStr;
use std::io::ErrorKind;
use std::rc::Rc;

use super::super::batch::{
    strides_from_schema, FIXED_REGION_BYTES, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT,
};
use super::super::error::StorageError;
use super::super::layout::*;
use super::super::merge::ColPtr;
use super::super::shard_filter::ShardFilter;
use super::{MappedShard, PackedRegion, PayloadRegion, WeightRegion, ZERO_CELL};
use crate::schema::SchemaDescriptor;
use gnitz_foundation::posix_io::Mmap;
use gnitz_wire::num_regions;
use gnitz_wire::{read_i64_le, read_u64_le};

use StorageError::Corrupt;

/// The payload arity of a mapping [`MappedShard::open`] accepted.
fn file_npc(data: &[u8]) -> usize {
    read_u64_le(data, OFF_FILE_NPC) as usize
}

/// A Raw (`count` elements) or Constant (one element, stride 0) region of
/// `width`-byte elements.
fn direct_region(data: &[u8], span: &Span, count: usize, width: usize) -> Result<ColPtr, StorageError> {
    let (stride, size) = match span.encoding {
        ENCODING_RAW => (width, count * width),
        ENCODING_CONSTANT => (0, width),
        _ => return Err(Corrupt("encoding")),
    };
    if span.size != size {
        return Err(Corrupt("region size"));
    }
    Ok(ColPtr { base: span.bytes(data).as_ptr(), stride })
}

impl MappedShard {
    pub(crate) fn open(path: &CStr, schema: &SchemaDescriptor) -> Result<Self, StorageError> {
        let mmap = Mmap::open_ro(path).map_err(|e| match e.kind() {
            ErrorKind::UnexpectedEof => Corrupt("empty file"),
            _ => e.into(),
        })?;
        let data = mmap.as_slice();
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
        let prefix = data
            .get(..desc_len(file_npc))
            .ok_or(Corrupt("shorter than its directory"))?;
        if desc_digest(path, prefix) != read_u64_le(prefix, OFF_DESC_CHECKSUM) {
            return Err(Corrupt("descriptor digest"));
        }
        Self::bind(Rc::new(mmap), schema)
    }

    /// This shard's mapping bound to `schema`, with no file I/O.
    pub(crate) fn rebind(&self, schema: &SchemaDescriptor) -> Result<Self, StorageError> {
        Self::bind(Rc::clone(&self.mmap), schema)
    }

    /// A handle on `mmap` under `schema`. Trusts the descriptive prefix
    /// [`open`](Self::open) checked.
    fn bind(mmap: Rc<Mmap>, schema: &SchemaDescriptor) -> Result<Self, StorageError> {
        let data = mmap.as_slice();
        let file_npc = file_npc(data);
        let count = read_u64_le(data, OFF_ROW_COUNT) as usize;
        if count == 0 {
            return Err(Corrupt("no rows"));
        }
        let spans = region_spans(data, file_npc)?;
        let (strides, _) = strides_from_schema(schema);

        let pk = direct_region(data, &spans[REG_PK], count, strides[REG_PK] as usize)?;
        let w = &spans[REG_WEIGHT];
        let weight = match w.encoding {
            ENCODING_TWO_VALUE => {
                if w.size != two_value_image_len(count) {
                    return Err(Corrupt("region size"));
                }
                let image = w.bytes(data);
                WeightRegion::TwoValue {
                    value_a: read_i64_le(image, 0),
                    value_b: read_i64_le(image, 8),
                    bitvec: image[TWO_VALUE_HEADER..].as_ptr(),
                }
            }
            _ => WeightRegion::Mapped(direct_region(data, w, count, FIXED_REGION_BYTES)?),
        };
        let null_bmp = direct_region(data, &spans[REG_NULL_BMP], count, FIXED_REGION_BYTES)?;

        let col_regions = schema
            .payload_columns()
            .map(|(pi, col)| {
                if pi >= file_npc {
                    return Ok(PayloadRegion::Mapped(ColPtr { base: ZERO_CELL.as_ptr(), stride: 0 }));
                }
                let span = &spans[REG_PAYLOAD_START + pi];
                if span.encoding != ENCODING_FOR {
                    return direct_region(data, span, count, strides[REG_PAYLOAD_START + pi] as usize)
                        .map(PayloadRegion::Mapped);
                }
                let fi = col.fixed_int().ok_or(Corrupt("encoding"))?;
                let bw = for_image_bw(span.size, count, fi.width()).ok_or(Corrupt("FoR width"))?;
                Ok(PayloadRegion::Packed(PackedRegion {
                    offset: span.off,
                    bw,
                    elem_width: fi.width(),
                    decoded: std::cell::OnceCell::new(),
                }))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        let schema_npc = schema.num_payload_cols();
        let null_pad_mask =
            gnitz_wire::low_bits_mask(schema_npc) & !gnitz_wire::low_bits_mask(file_npc.min(schema_npc));

        let blob = &spans[num_regions(file_npc) - 1];
        let filter = &spans[num_regions(file_npc)];
        if blob.encoding != ENCODING_RAW || filter.encoding != ENCODING_RAW {
            return Err(Corrupt("encoding"));
        }
        let shard_filter = match filter.size {
            0 => None,
            _ => {
                let region = filter.bytes(data);
                let parsed = ShardFilter::parse(region).ok_or(Corrupt("filter descriptor"))?;
                Some((parsed, region.as_ptr()))
            }
        };

        Ok(MappedShard {
            count,
            pk,
            weight,
            null_bmp,
            col_regions,
            null_pad_mask,
            blob: blob.bytes(data).as_ptr(),
            blob_len: blob.size,
            shard_filter,
            pk_stride: schema.pk_stride(),
            skeleton: read_u64_le(data, OFF_FLAGS) & SHARD_FLAG_SKELETON != 0,
            mmap,
        })
    }

    /// Whether the body still hashes to the checksum the writer recorded.
    pub(crate) fn verify_body(&self) -> Result<(), StorageError> {
        self.mmap.advise_sequential();
        let data = self.data();
        let mut body = gnitz_wire::RowHasher::default();
        for span in region_spans(data, file_npc(data))? {
            body.update(span.bytes(data));
        }
        (body.digest() == read_u64_le(data, OFF_BODY_CHECKSUM))
            .then_some(())
            .ok_or(Corrupt("body checksum"))
    }
}
