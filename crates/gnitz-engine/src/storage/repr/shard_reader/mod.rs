//! Memory-mapped columnar shard reader.
//!
//! Used by compaction (`compact`) and query-time reads (`read_cursor`).
//! Split into the cold open/validation path ([`open`]) and the hot per-row
//! accessors ([`access`]); the type definitions and the `mmap` RAII handle
//! live here, so both sub-modules read the (otherwise private) fields directly.
//!
//! `open` normalizes every fixed-width region to a [`RegionView`], so the
//! accessors see one addressing form rather than a per-encoding match.

use std::cell::OnceCell;

use super::shard_file::DecodedRegion;
#[cfg(test)]
use gnitz_wire::as_le_bytes;
#[cfg(test)]
use gnitz_wire::{read_i64_le, read_u64_le, write_u64_le};

mod access;
mod open;

pub(super) use crate::foundation::posix_io::{Advice, Mmap};

// ---------------------------------------------------------------------------
// Region views — every fixed-width region as one (offset, stride) pair
// ---------------------------------------------------------------------------

/// A fixed-width region addressed as `(offset, stride)` into the mmap.
///
/// `stride == 0` *is* the constant encoding: the region holds a single element
/// at `offset` and `row_off` returns it for every row, so no accessor branches
/// on the encoding. The element's width is not carried here — the caller
/// already knows it (`pk_stride`, `FIXED_REGION_BYTES`, the column size), and
/// `open` validates the region against it.
#[derive(Clone, Copy)]
pub(crate) struct RegionView {
    offset: usize,
    stride: usize,
}

impl RegionView {
    /// Byte offset of `row`'s element.
    #[inline(always)]
    fn row_off(&self, row: usize) -> usize {
        self.offset + row * self.stride
    }

    /// Whether the region stores one element per row, rather than a single
    /// element shared by all of them.
    #[cfg(test)]
    fn is_per_row(&self) -> bool {
        self.stride != 0
    }
}

/// A payload-column region — the only role that may carry `ENCODING_FOR`.
pub(crate) enum PayloadRegion {
    Direct(RegionView),
    Packed(PackedRegion),
    /// A column the file predates (written before an `ALTER TABLE … ADD
    /// COLUMN`), so it has no directory entry. Reads as `ZERO_CELL`; its null
    /// bit is always set by `null_pad_mask`, so the bytes are never a value.
    Absent,
}

/// The cell an [`Absent`](PayloadRegion::Absent) column reads. The readers need
/// a valid, correctly-sized `'static` address, which no in-file offset can
/// supply (a shard holds no guaranteed-zero 16-byte span). 16 bytes covers every
/// payload cell — `gnitz_wire::wire_stride` tops out there (U128/UUID/I128 and
/// the German-string struct).
static ZERO_CELL: [u8; 16] = [0; 16];

/// FoR + byte-width-truncated integer payload region (`ENCODING_FOR`).
/// `decoded` lazily holds the full `count × elem_width` little-endian image,
/// populated at most once per shard open (`packed_bytes`); its content address
/// is stable for the shard's lifetime, which the raw-pointer accessors rely on.
pub(crate) struct PackedRegion {
    offset: usize,
    size: usize,
    elem_width: usize,
    decoded: OnceCell<DecodedRegion>,
}

/// The weight region — the only role that may use the two-value encoding, so
/// that variant is unrepresentable elsewhere rather than rejected per accessor.
pub(crate) enum WeightRegion {
    Direct(RegionView),
    /// Exactly two distinct weights, selected per row by `bitvec_off`.
    TwoValue {
        value_a: i64,
        value_b: i64,
        bitvec_off: usize,
    },
}

pub(crate) struct MappedShard {
    /// Owning RAII handle for the mmap.  Dropped last, so `as_slice()` /
    /// raw pointers derived from it remain valid for the entire lifetime
    /// of the `MappedShard`.
    mmap: Mmap,
    pub(crate) count: usize,
    pub(crate) pk: RegionView,
    pub(crate) weight: WeightRegion,
    pub(crate) null_bmp: RegionView,
    /// Non-PK column regions indexed by payload position, always one per payload
    /// column of the reader's schema. Columns the file predates are
    /// [`PayloadRegion::Absent`].
    pub(crate) col_regions: Vec<PayloadRegion>,
    /// Null-word bits for this shard's `Absent` columns. OR'd into every null
    /// word the three readers hand out, so a column the file has no bytes for
    /// reads NULL rather than as a non-null zero. `0` for a full-width shard.
    pub(crate) null_pad_mask: u64,
    pub(crate) blob_off: usize,
    pub(crate) blob_len: usize,
    /// PK membership filter over this file's own filter region, or `None` when
    /// the file carries none.
    shard_filter: Option<super::shard_filter::ShardFilter>,
    /// Encoded OPK width per row: the sum of the PK columns' widths.
    pub(crate) pk_stride: u8,
    /// `SHARD_FLAG_SKELETON`: this file's rows are (PK, coarse weight) pairs
    /// with no payload — a capacity-bounded view's dehydrated shard. Every
    /// payload column is `Absent`, and the read path folds a PK group holding one
    /// of these rows to a single coarse row rather than to (PK, payload) groups.
    skeleton: bool,
}

impl MappedShard {
    /// Whether this file is a bounded view's skeleton shard.
    #[inline(always)]
    pub(crate) fn is_skeleton(&self) -> bool {
        self.skeleton
    }

    /// Bytes this shard occupies on disk — the mapped file's length, which is the
    /// quantity a capacity-bounded store sums to decide whether it is over budget.
    #[inline]
    pub(crate) fn file_len(&self) -> u64 {
        self.mmap.len() as u64
    }
}

// MappedShard does not implement Drop — the owned `mmap: Mmap` field handles
// `munmap` automatically when the shard is dropped.

#[cfg(test)]
#[path = "../tests/shard_reader.rs"]
mod tests;
