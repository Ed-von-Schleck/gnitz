//! Cold open-time path for [`MappedShard`]: header + directory validation,
//! region decoding and the PK membership-filter load. Runs once per handle,
//! never per row.
//!
//! Opening verifies the header and directory; the body is verified only by
//! [`MappedShard::verify_body`].

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
use gnitz_wire::{read_i64_le, read_u64_le};

use StorageError::Corrupt;

/// A Raw (`count` elements) or Constant (one element, stride 0) region of
/// `width`-byte elements.
fn direct_region(data: &[u8], span: &Span, count: usize, width: usize) -> Result<ColPtr, StorageError> {
    let (stride, size) = match span.encoding {
        Encoding::Raw => (width, count * width),
        Encoding::Constant => (0, width),
        Encoding::TwoValue | Encoding::For => return Err(Corrupt("encoding")),
    };
    if span.size != size {
        return Err(Corrupt("region size"));
    }
    Ok(ColPtr { base: span.bytes(data).as_ptr(), stride })
}

impl MappedShard {
    pub(crate) fn open(path: &str, schema: &SchemaDescriptor) -> Result<Self, StorageError> {
        let mmap = Mmap::open_ro(std::path::Path::new(path)).map_err(|e| match e.kind() {
            ErrorKind::UnexpectedEof => Corrupt("empty file"),
            _ => e.into(),
        })?;
        let data = mmap.as_slice();
        let header = ShardHeader::read(data)?;
        let prefix = data
            .get(..desc_len(header.file_npc))
            .ok_or(Corrupt("shorter than its directory"))?;
        if desc_digest(path, prefix) != read_u64_le(prefix, OFF_DESC_CHECKSUM) {
            return Err(Corrupt("descriptor digest"));
        }
        Self::bind(Rc::new(mmap), &header, schema)
    }

    /// This shard's mapping bound to `schema`, with no file I/O.
    pub(crate) fn rebind(&self, schema: &SchemaDescriptor) -> Result<Self, StorageError> {
        let header = ShardHeader::read(self.data())?;
        Self::bind(Rc::clone(&self.mmap), &header, schema)
    }

    /// A handle on `mmap` under `schema`. Trusts the descriptive prefix
    /// [`open`](Self::open) checked.
    fn bind(mmap: Rc<Mmap>, header: &ShardHeader, schema: &SchemaDescriptor) -> Result<Self, StorageError> {
        let data = mmap.as_slice();
        let (file_npc, count) = (header.file_npc, header.row_count);
        let spans = region_spans(data, file_npc)?;
        let (strides, _) = strides_from_schema(schema);

        let pk = direct_region(data, &spans[REG_PK], count, strides[REG_PK] as usize)?;
        let w = &spans[REG_WEIGHT];
        let weight = match w.encoding {
            Encoding::TwoValue => {
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
                if span.encoding != Encoding::For {
                    return direct_region(data, span, count, strides[REG_PAYLOAD_START + pi] as usize)
                        .map(PayloadRegion::Mapped);
                }
                let fi = col.fixed_int().ok_or(Corrupt("encoding"))?;
                let bw = for_image_bw(span.size, count, fi.width()).ok_or(Corrupt("FoR width"))?;
                Ok(PayloadRegion::Packed(PackedRegion {
                    image: span.off..span.off + span.size,
                    bw,
                    elem_width: fi.width(),
                    decoded: std::cell::OnceCell::new(),
                }))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        let schema_npc = schema.num_payload_cols();
        let null_pad_mask =
            gnitz_wire::low_bits_mask(schema_npc) & !gnitz_wire::low_bits_mask(file_npc.min(schema_npc));

        let [.., blob, filter] = spans.as_slice() else {
            unreachable!("region_spans yields every region plus the filter")
        };
        if blob.encoding != Encoding::Raw || filter.encoding != Encoding::Raw {
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
            skeleton: header.skeleton,
            mmap,
        })
    }

    /// Whether the body still hashes to the checksum the writer recorded.
    pub(crate) fn verify_body(&self) -> Result<(), StorageError> {
        self.mmap.advise_sequential();
        let data = self.data();
        let h = ShardHeader::read(data)?;
        (gnitz_wire::checksum(&data[desc_len(h.file_npc)..]) == h.body_checksum)
            .then_some(())
            .ok_or(Corrupt("body checksum"))
    }
}
