//! Memory-mapped columnar shard reader.
//!
//! Used by compaction (`compact`) and query-time reads (`read_cursor`).
//! Split into the cold open/validation path ([`open`]) and the hot per-row
//! accessors ([`access`]); the type definitions and the FoR decoder live here,
//! so both sub-modules read the (otherwise private) fields directly.

use std::cell::OnceCell;
use std::ops::Range;
use std::rc::Rc;

mod access;
mod open;

use super::layout::for_cell_at;
use super::merge::ColPtr;
use gnitz_foundation::posix_io::Mmap;
use gnitz_wire::read_u64_le;

/// A payload-column region — the only role that may carry [`Encoding::For`].
pub(crate) enum PayloadRegion {
    /// Readable in place: a Raw (stride = width) or Constant (stride 0) region
    /// of the mapping, or `ZERO_CELL` at stride 0 for a column the file predates
    /// (its null bit comes from `null_pad_mask`).
    Mapped(ColPtr),
    Packed(PackedRegion),
}

/// The cell a column the file predates reads. The readers need a valid,
/// correctly-sized `'static` address, which no in-file offset can supply (a
/// shard holds no guaranteed-zero 16-byte span). 16 bytes covers every payload
/// cell — `gnitz_wire::wire_stride` tops out there (U128/UUID/I128 and the
/// German-string struct).
static ZERO_CELL: [u8; 16] = [0; 16];

/// An [`Encoding::For`] payload region at `image` in the mapping, and its
/// decoded image once a read has needed it.
pub(crate) struct PackedRegion {
    image: Range<usize>,
    bw: usize,
    elem_width: usize,
    decoded: OnceCell<Box<[u8]>>,
}

/// The weight region — the only role that may use the two-value encoding, so
/// that variant is unrepresentable elsewhere rather than rejected per accessor.
pub(crate) enum WeightRegion {
    Mapped(ColPtr),
    /// Exactly two distinct weights, selected per row by the `count`-bit
    /// vector at `bitvec`.
    TwoValue {
        value_a: i64,
        value_b: i64,
        bitvec: *const u8,
    },
}

pub(crate) struct MappedShard {
    /// The mapping every pointer below points into, shared by every handle
    /// [`rebind`](Self::rebind) derives from this one.
    mmap: Rc<Mmap>,
    pub(crate) count: usize,
    pk: ColPtr,
    weight: WeightRegion,
    null_bmp: ColPtr,
    /// Non-PK column regions indexed by payload position, always one per payload
    /// column of the reader's schema, a column the file predates included.
    col_regions: Vec<PayloadRegion>,
    /// Null-word bits for the payload columns this file predates. OR'd into
    /// every null word the readers hand out, so a column the file has no bytes
    /// for reads NULL rather than as a non-null zero. `0` for a full-width shard.
    null_pad_mask: u64,
    blob: *const u8,
    blob_len: usize,
    /// The PK filter and its region, or `None` when the file carries none.
    shard_filter: Option<(super::shard_filter::ShardFilter, *const u8)>,
    /// Encoded OPK width per row: the sum of the PK columns' widths.
    pk_stride: usize,
    /// `SHARD_FLAG_SKELETON`: the read path folds a PK group holding one of
    /// these rows to a single coarse row.
    skeleton: bool,
}

impl MappedShard {
    /// Bytes this shard occupies on disk — the mapped file's length, which is the
    /// quantity a capacity-bounded store sums to decide whether it is over budget.
    #[inline]
    pub(crate) fn file_len(&self) -> u64 {
        self.mmap.as_slice().len() as u64
    }
}

/// Decode rows `first_row..` of a FoR image back to their raw little-endian
/// form, `out.len() / elem_width` rows.
pub(crate) fn decode_for_region(image: &[u8], bw: usize, elem_width: usize, first_row: usize, out: &mut [u8]) {
    match elem_width {
        2 => decode_for_cells::<2>(image, bw, first_row, out),
        4 => decode_for_cells::<4>(image, bw, first_row, out),
        8 => decode_for_cells::<8>(image, bw, first_row, out),
        _ => unreachable!("open admits FoR only on 2/4/8-byte integer columns"),
    }
}

fn decode_for_cells<const W: usize>(image: &[u8], bw: usize, first_row: usize, out: &mut [u8]) {
    debug_assert!((1..W).contains(&bw), "open-time checks bound bw to 1..elem_width");
    let reference = read_u64_le(image, 0);
    let mask = gnitz_wire::low_bits_mask(8 * bw);
    let cells = out.as_chunks_mut::<W>().0;
    for (i, cell) in cells.iter_mut().enumerate() {
        let v = (read_u64_le(image, for_cell_at(first_row + i, bw)) & mask).wrapping_add(reference);
        *cell = *v.to_le_bytes().first_chunk::<W>().unwrap();
    }
}

#[cfg(test)]
#[path = "../tests/shard_reader.rs"]
mod tests;
