//! Memory-mapped columnar shard reader.
//!
//! Used by compaction (`compact`) and query-time reads (`read_cursor`).
//! Split into the cold open/validation path ([`open`]) and the hot per-row
//! accessors ([`access`]); the type definitions, the `mmap` RAII handle and the
//! FoR decoder live here, so both sub-modules read the (otherwise private)
//! fields directly.
//!
//! `open` resolves every fixed-width region to a [`ColPtr`], so the accessors
//! address every encoding the same way.

use std::cell::OnceCell;

mod access;
mod open;

use super::layout::FOR_HEADER;
use super::merge::ColPtr;
use gnitz_foundation::posix_io::Mmap;
use gnitz_wire::read_u64_le;

/// A payload-column region — the only role that may carry `ENCODING_FOR`.
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

/// FoR + byte-width-truncated integer payload region (`ENCODING_FOR`).
/// `decoded` lazily holds the full `count × elem_width` little-endian image,
/// populated at most once per shard open (`packed_bytes`); its content address
/// is stable for the shard's lifetime, which the raw-pointer accessors rely on.
pub(crate) struct PackedRegion {
    offset: usize,
    bw: usize,
    elem_width: usize,
    decoded: OnceCell<Box<[u8]>>,
}

/// The weight region — the only role that may use the two-value encoding, so
/// that variant is unrepresentable elsewhere rather than rejected per accessor.
pub(crate) enum WeightRegion {
    Mapped(ColPtr),
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
    pk: ColPtr,
    weight: WeightRegion,
    null_bmp: ColPtr,
    /// Non-PK column regions indexed by payload position, always one per payload
    /// column of the reader's schema, a column the file predates included.
    pub(crate) col_regions: Vec<PayloadRegion>,
    /// Null-word bits for the payload columns this file predates. OR'd into
    /// every null word the readers hand out, so a column the file has no bytes
    /// for reads NULL rather than as a non-null zero. `0` for a full-width shard.
    pub(crate) null_pad_mask: u64,
    pub(crate) blob_off: usize,
    pub(crate) blob_len: usize,
    /// PK membership filter over this file's own filter region, or `None` when
    /// the file carries none.
    shard_filter: Option<super::shard_filter::ShardFilter>,
    /// Encoded OPK width per row: the sum of the PK columns' widths.
    pub(crate) pk_stride: usize,
    /// `SHARD_FLAG_SKELETON`: this file's rows are (PK, coarse weight) pairs
    /// with no payload — a capacity-bounded view's dehydrated shard. Every
    /// payload column is one the file predates, and the read path folds a PK
    /// group holding one of these rows to a single coarse row rather than to
    /// (PK, payload) groups.
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

/// Decode a FoR image back to its raw little-endian form, `out.len() / elem_width` rows.
pub(crate) fn decode_for_region(image: &[u8], bw: usize, elem_width: usize, out: &mut [u8]) {
    match elem_width {
        2 => decode_for_cells::<2>(image, bw, out),
        4 => decode_for_cells::<4>(image, bw, out),
        8 => decode_for_cells::<8>(image, bw, out),
        _ => unreachable!("open admits FoR only on 2/4/8-byte integer columns"),
    }
}

fn decode_for_cells<const W: usize>(image: &[u8], bw: usize, out: &mut [u8]) {
    debug_assert!((1..W).contains(&bw), "open-time checks bound bw to 1..elem_width");
    let reference = read_u64_le(image, 0);
    let mask = (1u64 << (8 * bw)) - 1;
    let cells = out.as_chunks_mut::<W>().0;
    // Rows whose offset can be read as one masked 8-byte load without passing the image end;
    // the few after them are gathered byte by byte.
    let loadable = match image.len().checked_sub(FOR_HEADER + 8) {
        Some(room) => (room / bw + 1).min(cells.len()),
        None => 0,
    };
    let (head, tail) = cells.split_at_mut(loadable);
    for (i, cell) in head.iter_mut().enumerate() {
        let v = (read_u64_le(image, FOR_HEADER + i * bw) & mask).wrapping_add(reference);
        *cell = *v.to_le_bytes().first_chunk::<W>().unwrap();
    }
    for (i, cell) in tail.iter_mut().enumerate() {
        let base = FOR_HEADER + (loadable + i) * bw;
        let off = image[base..base + bw]
            .iter()
            .rev()
            .fold(0u64, |acc, &b| (acc << 8) | b as u64);
        *cell = *off.wrapping_add(reference).to_le_bytes().first_chunk::<W>().unwrap();
    }
}

#[cfg(test)]
#[path = "../tests/shard_reader.rs"]
mod tests;
