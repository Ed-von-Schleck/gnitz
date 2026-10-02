//! Order-preserving primary-key (OPK) byte encoding.
//!
//! A PK region at rest holds **order-preserving big-endian** bytes: for every
//! pair of encoded keys `memcmp(a, b)` equals the typed lexicographic
//! comparison of the PK columns. Client and engine encode and decode through
//! this module, which is why it lives in `gnitz-wire`, the crate both depend on.
//!
//! All PK columns are fixed-width integer scalars (floats/strings/blobs are
//! rejected at DDL), so the transform is a fixed-width bijection: unsigned types
//! map to big-endian, signed types map to big-endian with the sign bit flipped.

use crate::TypeCode;

/// Write the low `dst.len()` bytes of `v` big-endian into `dst`, the top bit of that
/// width flipped when `flip`. With `v` a native value and `flip = tc.is_signed_int()`
/// that is the column's OPK bytes; with `flip = false` it stores a key image as it is.
#[inline(always)]
pub fn store_opk(dst: &mut [u8], v: u128, flip: bool) {
    macro_rules! store {
        ($ty:ty) => {{
            let d: &mut [u8; std::mem::size_of::<$ty>()] = dst.try_into().unwrap();
            *d = ((v as $ty) ^ ((flip as $ty) << (<$ty>::BITS - 1))).to_be_bytes();
        }};
    }
    match dst.len() {
        16 => store!(u128),
        8 => store!(u64),
        4 => store!(u32),
        2 => store!(u16),
        1 => store!(u8),
        _ => unreachable!("PK column width is 1/2/4/8/16"),
    }
}

/// [`store_opk`] appended to `buf` as one `width`-byte column.
#[inline(always)]
pub fn push_opk(buf: &mut Vec<u8>, width: usize, v: u128, flip: bool) {
    macro_rules! push {
        ($w:literal) => {{
            let mut cell = [0u8; $w];
            store_opk(&mut cell, v, flip);
            buf.extend_from_slice(&cell);
        }};
    }
    match width {
        16 => push!(16),
        8 => push!(8),
        4 => push!(4),
        2 => push!(2),
        1 => push!(1),
        _ => unreachable!("PK column width is 1/2/4/8/16"),
    }
}

/// The mask of a `width`-byte column's images: its low `8·width` bits.
#[inline(always)]
pub const fn image_mask(width: usize) -> u128 {
    u128::MAX >> (128 - 8 * width)
}

/// A native value's image in its column's key order: masked to the type's width, sign bit
/// flipped for a signed type — the OPK bytes read as a big-endian integer. Its own
/// inverse on a masked value.
#[inline(always)]
pub fn key_image(tc: TypeCode, native: u128) -> u128 {
    (native & image_mask(tc.wire_stride())) ^ opk_bias(tc)
}

/// The image of zero in a column of type `tc`: its sign bit if signed, else 0.
#[inline(always)]
pub fn opk_bias(tc: TypeCode) -> u128 {
    (tc.is_signed_int() as u128) << (tc.wire_stride() * 8 - 1)
}

/// Decode one OPK cell into its native little-endian bytes: `src` and `dst` are both
/// the column's width, `signed` the column's signedness.
///
/// Each arm stores through a `&mut [u8; W]`: a runtime-length `copy_from_slice` per
/// arm tail-merges into one shared `memcpy` call in a caller's row loop.
#[inline(always)]
pub fn decode_pk_cell(src: &[u8], signed: bool, dst: &mut [u8]) {
    debug_assert_eq!(dst.len(), src.len());
    macro_rules! decode {
        ($ty:ty) => {{
            const W: usize = std::mem::size_of::<$ty>();
            let v = <$ty>::from_be_bytes(src.try_into().unwrap()) ^ ((signed as $ty) << (<$ty>::BITS - 1));
            let d: &mut [u8; W] = dst.try_into().unwrap();
            *d = v.to_le_bytes();
        }};
    }
    match src.len() {
        16 => decode!(u128),
        8 => decode!(u64),
        4 => decode!(u32),
        2 => decode!(u16),
        1 => decode!(u8),
        other => unreachable!("PK column size must be 1/2/4/8/16, got {other}"),
    }
}

/// `f` over the `W`-byte cell at byte `off` of each `stride`-byte row of `src`, paired
/// with `dst`'s items in order.
#[inline(always)]
pub fn zip_cells<const W: usize, D>(
    src: &[u8],
    stride: usize,
    off: usize,
    dst: impl Iterator<Item = D>,
    mut f: impl FnMut(&[u8; W], D),
) {
    assert!(off + W <= stride, "a cell lies inside its row");
    if stride == W {
        for (cell, d) in src.as_chunks::<W>().0.iter().zip(dst) {
            f(cell, d);
        }
    } else {
        for (row, d) in src.chunks_exact(stride).zip(dst) {
            f(row[off..off + W].try_into().unwrap(), d);
        }
    }
}

/// Decode the `width`-byte PK column at byte `off` of each `stride`-byte row of `pk`
/// into `dst`'s native little-endian cells, `width` bytes each.
pub fn decode_pk_cells(pk: &[u8], stride: usize, off: usize, width: usize, signed: bool, dst: &mut [u8]) {
    macro_rules! decode_rows {
        ($w:literal) => {
            zip_cells::<$w, _>(pk, stride, off, dst.as_chunks_mut::<$w>().0.iter_mut(), |s, d| {
                decode_pk_cell(s, signed, d)
            })
        };
    }
    match width {
        1 => decode_rows!(1),
        2 => decode_rows!(2),
        4 => decode_rows!(4),
        8 => decode_rows!(8),
        16 => decode_rows!(16),
        other => unreachable!("PK column size must be 1/2/4/8/16, got {other}"),
    }
}

/// Widest PK region [`widen_pk_be`] packs into a `u128`; a wider key is ordered
/// and hashed as raw bytes.
pub const NARROW_PK_MAX_BYTES: usize = 16;

/// A narrow OPK region's image: its bytes read as a big-endian integer, right-aligned.
/// Width-specialized so only strides 3/5/6/7 pay a `memcpy`.
#[inline(always)]
pub fn widen_pk_be(pk_bytes: &[u8]) -> u128 {
    let stride = pk_bytes.len();
    debug_assert!(
        stride <= NARROW_PK_MAX_BYTES,
        "widen_pk_be: wide PK region (stride {stride})"
    );
    match stride {
        16 => u128::from_be_bytes(pk_bytes[..16].try_into().unwrap()),
        8 => u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128,
        4 => u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128,
        2 => u16::from_be_bytes(pk_bytes[..2].try_into().unwrap()) as u128,
        1 => pk_bytes[0] as u128,
        // 9..=15: two overlapping loads; the tail load's low `m` bytes are
        // `pk_bytes[8..stride]`.
        9..=15 => {
            let m = stride - 8;
            let hi = u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128;
            let tail = u64::from_be_bytes(pk_bytes[stride - 8..stride].try_into().unwrap());
            (hi << (8 * m)) | ((tail & ((1u64 << (8 * m)) - 1)) as u128)
        }
        // 3/5/6/7: too narrow for the overlapping-load trick (`stride - 8` underflows).
        _ => {
            let mut buf = [0u8; 16];
            buf[16 - stride..].copy_from_slice(&pk_bytes[..stride]);
            u128::from_be_bytes(buf)
        }
    }
}

/// Decode one OPK cell to `i64`, widened as [`crate::types::FixedInt`] defines.
///
/// Spelled with byte-array literals, which at `-O0` spares a per-row caller an
/// out-of-line call and a 16-byte stack image. The assert carries a static message:
/// an `#[inline(always)]` body duplicates a formatted `Arguments` block into every
/// call site.
#[inline(always)]
pub fn decode_opk_i64(opk: &[u8], fi: crate::FixedInt) -> i64 {
    use crate::FixedInt as F;
    debug_assert!(opk.len() == fi.width(), "decode_opk_i64: slice width != FixedInt width");
    match fi {
        F::U8 => opk[0] as i64,
        F::I8 => (opk[0] ^ 0x80) as i8 as i64,
        F::U16 => u16::from_be_bytes([opk[0], opk[1]]) as i64,
        F::I16 => (u16::from_be_bytes([opk[0], opk[1]]) ^ (1 << 15)) as i16 as i64,
        F::U32 => u32::from_be_bytes([opk[0], opk[1], opk[2], opk[3]]) as i64,
        F::I32 => (u32::from_be_bytes([opk[0], opk[1], opk[2], opk[3]]) ^ (1 << 31)) as i32 as i64,
        F::U64 => u64::from_be_bytes([opk[0], opk[1], opk[2], opk[3], opk[4], opk[5], opk[6], opk[7]]) as i64,
        F::I64 => {
            (u64::from_be_bytes([opk[0], opk[1], opk[2], opk[3], opk[4], opk[5], opk[6], opk[7]]) ^ (1 << 63)) as i64
        }
    }
}

// ---------------------------------------------------------------------------
// Width-tagged PK byte buffer
// ---------------------------------------------------------------------------

/// An OPK key of up to `MAX_PK_BYTES` bytes, held by the engine's key encoders and
/// the client's row-identity maps alike. `bytes[..len]` is the key and the tail past
/// it is zero: every constructor starts from a zeroed array and no method shortens a
/// key, which is what [`Self::widened`] reads.
#[derive(Clone, Copy)]
pub struct PkBuf {
    bytes: [u8; crate::MAX_PK_BYTES],
    len: u8,
}

/// Prints only `bytes[..len]`, so assertion diffs over keys stay readable.
impl std::fmt::Debug for PkBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PkBuf({:02x?})", self.pk_bytes())
    }
}

// Eq/Hash/Ord all read `bytes[..len]` and nothing else — no width prefix, which
// the `Borrow<[u8]>` impl below requires and which keeps a `HashSet<PkBuf>`
// touching `pk_stride` bytes per key. The order is byte-lexicographic, the same
// `memcmp` order OPK regions are merged and seeked in.
impl PartialEq for PkBuf {
    fn eq(&self, other: &Self) -> bool {
        self.pk_bytes() == other.pk_bytes()
    }
}
impl Eq for PkBuf {}

impl std::hash::Hash for PkBuf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pk_bytes().hash(state);
    }
}

/// Lets a raw `&[u8]` probe a `HashSet<PkBuf>` / `HashMap<PkBuf, _>` with no key
/// minted.
impl std::borrow::Borrow<[u8]> for PkBuf {
    fn borrow(&self) -> &[u8] {
        self.pk_bytes()
    }
}

impl PartialOrd for PkBuf {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PkBuf {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.pk_bytes().cmp(other.pk_bytes())
    }
}

impl PkBuf {
    /// All-zero key of the given width — the empty-shard / placeholder form.
    #[inline(always)] // per-row across the crate boundary; dev builds inline nothing else
    pub fn zeroed(len: usize) -> Self {
        debug_assert!(len <= crate::MAX_PK_BYTES);
        PkBuf {
            bytes: [0u8; crate::MAX_PK_BYTES],
            len: len as u8,
        }
    }

    /// All-`0xFF` key of the given width: an OPK region compares as unsigned
    /// bytes, so no key of that width sorts above it. The upper bound an
    /// open-ended range takes.
    #[inline(always)]
    pub fn max(len: usize) -> Self {
        let mut k = PkBuf::zeroed(len);
        k.bytes[..len].fill(0xFF);
        k
    }

    /// A key of exactly `slice`'s bytes and width.
    #[inline(always)]
    pub fn from_bytes(slice: &[u8]) -> Self {
        // Hard, not debug-only: `slice` is an externally-controlled length on the
        // client's decode paths, and the cast below would truncate a length >= 256.
        assert!(
            slice.len() <= crate::MAX_PK_BYTES,
            "PkBuf::from_bytes: length {} exceeds MAX_PK_BYTES {}",
            slice.len(),
            crate::MAX_PK_BYTES,
        );
        let mut bytes = [0u8; crate::MAX_PK_BYTES];
        bytes[..slice.len()].copy_from_slice(slice);
        PkBuf { bytes, len: slice.len() as u8 }
    }

    /// Extend the key by one `width`-byte column, written as [`store_opk`] writes it.
    #[inline(always)]
    pub fn push(&mut self, width: usize, v: u128, flip: bool) {
        let at = self.len as usize;
        store_opk(&mut self.bytes[at..at + width], v, flip);
        self.len = (at + width) as u8;
    }

    /// The key's OPK bytes.
    #[inline(always)]
    pub fn pk_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    /// The key's bytes, writable in place at their width.
    #[inline(always)]
    pub fn pk_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[..self.len as usize]
    }

    /// The key zero-padded to `width` bytes: an index leading-key span widened to a
    /// full PK stride.
    #[inline]
    pub fn widened(mut self, width: usize) -> Self {
        assert!(
            self.len as usize <= width && width <= crate::MAX_PK_BYTES,
            "PkBuf::widened: width outside len..=MAX_PK_BYTES"
        );
        self.len = width as u8;
        self
    }
}

#[cfg(test)]
#[path = "tests/pk.rs"]
mod tests;
