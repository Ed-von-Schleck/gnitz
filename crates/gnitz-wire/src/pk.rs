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

/// Order-preserving big-endian encoding of one PK column.
///
/// `src` and `dst` are both exactly the column's width (1/2/4/8/16). Native
/// little-endian input is byte-reversed to big-endian; signed types additionally
/// flip the sign bit so the signed range maps monotonically onto the unsigned
/// range. The result's unsigned lexicographic order equals the numeric order of
/// the source value.
///
/// The sign flip rides in the integer, not in `dst`: the sign bit *is* the top
/// bit of the big-endian image, so XOR-ing it before the store keeps the whole
/// transform one load, one `bswap` and one store — a trailing `dst[0] ^= 0x80`
/// would be a read-modify-write of bytes just written. [`decode_pk_column`] is
/// the exact mirror, `#[inline(always)]` for the reason stated there.
#[inline(always)]
pub fn encode_pk_column(src: &[u8], tc: TypeCode, dst: &mut [u8]) {
    debug_assert_eq!(dst.len(), src.len());
    let flip = tc.is_signed_int();
    match dst.len() {
        16 => {
            let v = u128::from_le_bytes(src.try_into().unwrap()) ^ ((flip as u128) << 127);
            dst.copy_from_slice(&v.to_be_bytes());
        }
        8 => {
            let v = u64::from_le_bytes(src.try_into().unwrap()) ^ ((flip as u64) << 63);
            dst.copy_from_slice(&v.to_be_bytes());
        }
        4 => {
            let v = u32::from_le_bytes(src.try_into().unwrap()) ^ ((flip as u32) << 31);
            dst.copy_from_slice(&v.to_be_bytes());
        }
        2 => {
            let v = u16::from_le_bytes(src.try_into().unwrap()) ^ ((flip as u16) << 15);
            dst.copy_from_slice(&v.to_be_bytes());
        }
        1 => dst[0] = src[0] ^ ((flip as u8) << 7),
        other => unreachable!("PK column size must be 1/2/4/8/16, got {other}"),
    }
}

/// OPK-encode a PK tuple from its native little-endian image. `cols` are the PK
/// columns' `(width, type)` in PK-list order, the order the key compares in.
#[inline(always)]
pub fn encode_pk_tuple(cols: impl IntoIterator<Item = (usize, TypeCode)>, src: &[u8]) -> PkBuf {
    let mut out = PkBuf::zeroed(0);
    let mut off = 0;
    for (cs, tc) in cols {
        out.append(cs, |dst| encode_pk_column(&src[off..off + cs], tc, dst));
        off += cs;
    }
    debug_assert_eq!(off, src.len(), "pk tuple width != sum of column widths");
    out
}

/// OPK-encode one image per `(src_tc, target_tc, image)` column, each truncated to
/// `src_tc`'s width, promoted to `target_tc` and packed in order.
pub fn encode_pk_images(cols: impl IntoIterator<Item = (TypeCode, TypeCode, u128)>) -> PkBuf {
    let mut out = PkBuf::zeroed(0);
    for (src_tc, target_tc, image) in cols {
        let src_w = src_tc.wire_stride();
        out.append(target_tc.wire_stride(), |dst| {
            store_opk_image(image & image_mask(src_w), src_tc, src_w, target_tc, dst)
        });
    }
    out
}

/// The mask of a `width`-byte column's images: its low `8·width` bits.
#[inline(always)]
pub const fn image_mask(width: usize) -> u128 {
    u128::MAX >> (128 - 8 * width)
}

/// A native value's image in its column's key order: masked to the type's width, sign bit
/// flipped for a signed type — the OPK bytes read as a big-endian integer.
#[inline(always)]
pub fn key_image(tc: TypeCode, native: u128) -> u128 {
    let w = tc.wire_stride();
    (native & image_mask(w)) ^ opk_bias(tc, w)
}

/// The image of zero in a `width`-byte column of type `tc`: `2^(width·8−1)` if
/// signed, else 0. A value's image — its OPK bytes as a big-endian integer — is
/// `value + opk_bias`.
#[inline(always)]
pub fn opk_bias(tc: TypeCode, width: usize) -> u128 {
    (tc.is_signed_int() as u128) << (width * 8 - 1)
}

/// Write into `dst` the OPK bytes at `target_tc` of the value whose image at `src_tc`
/// (width `src_w`) is `image`. `target_tc` must hold every `src_tc` value.
#[inline(always)]
pub fn store_opk_image(image: u128, src_tc: TypeCode, src_w: usize, target_tc: TypeCode, dst: &mut [u8]) {
    // Skipping the re-bias at identity measured −19% instructions on `reindex_pack_bench`.
    let v = if src_tc == target_tc {
        image
    } else {
        image
            .wrapping_sub(opk_bias(src_tc, src_w))
            .wrapping_add(opk_bias(target_tc, dst.len()))
    };
    debug_assert!(
        dst.len() == 16 || v >> (dst.len() * 8) == 0,
        "store_opk_image: target narrower than the value"
    );
    macro_rules! store {
        ($ty:ty) => {{
            let d: &mut [u8; std::mem::size_of::<$ty>()] = dst.try_into().unwrap();
            *d = (v as $ty).to_be_bytes();
        }};
    }
    match dst.len() {
        16 => store!(u128),
        8 => store!(u64),
        4 => store!(u32),
        2 => store!(u16),
        1 => dst[0] = v as u8,
        _ => unreachable!("store_opk_image: PK column width is 1/2/4/8/16"),
    }
}

/// Symmetric inverse of [`encode_pk_column`]: decode an OPK column back to
/// native little-endian bytes. `src` and `dst` are both the column's width. The
/// big-endian image is read, its sign bit un-flipped for signed types, and the
/// native little-endian value stored — the mirror of the encoder, arm for arm,
/// and likewise never a read-modify-write of `dst`.
///
/// Each arm stores through a `&mut [u8; W]`, never `copy_from_slice`: LLVM
/// tail-merges four runtime-length copies into one shared `memcpy` call, leaving
/// a caller's row loop an indirect branch and a `call memcpy`. In the type the
/// width propagates and the store is one instruction.
#[inline(always)]
pub fn decode_pk_column(src: &[u8], tc: TypeCode, dst: &mut [u8]) {
    decode_pk_cell(src, tc.is_signed_int(), dst)
}

/// [`decode_pk_column`] with the column's signedness decided by the caller, so a
/// row loop tests the type once.
#[inline(always)]
pub fn decode_pk_cell(src: &[u8], flip: bool, dst: &mut [u8]) {
    debug_assert_eq!(dst.len(), src.len());
    macro_rules! decode {
        ($ty:ty, $sign_bit:expr) => {{
            const W: usize = std::mem::size_of::<$ty>();
            let v = <$ty>::from_be_bytes(src.try_into().unwrap()) ^ ((flip as $ty) << $sign_bit);
            let d: &mut [u8; W] = dst.try_into().unwrap();
            *d = v.to_le_bytes();
        }};
    }
    match src.len() {
        16 => decode!(u128, 127),
        8 => decode!(u64, 63),
        4 => decode!(u32, 31),
        2 => decode!(u16, 15),
        1 => dst[0] = src[0] ^ ((flip as u8) << 7),
        other => unreachable!("PK column size must be 1/2/4/8/16, got {other}"),
    }
}

/// Sign- or zero-extend a native-LE integer of type `src_tc` into the wider slot
/// `dst`, as `copy_column` widens a payload cell.
#[inline]
pub fn widen_native_le(src: &[u8], src_tc: TypeCode, dst: &mut [u8]) {
    let src_width = src.len();
    debug_assert!(dst.len() >= src_width);
    // Native LE: the sign bit is the high bit of the most-significant (last)
    // byte; the extension bytes are appended at the high LE indices.
    let is_neg = src_tc.is_signed_int() && src_width > 0 && (src[src_width - 1] & 0x80) != 0;
    dst[..src_width].copy_from_slice(src);
    dst[src_width..].fill(if is_neg { 0xFF } else { 0x00 });
}

/// A narrow OPK region's image: its bytes read as a big-endian integer. Right-aligned,
/// unlike the left-aligned sort key `pack_pk_be`; width-specialized so only strides
/// 3/5/6/7 pay a `memcpy`.
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

/// Decode one OPK PK column straight to `i64` — the exact inverse of
/// [`encode_pk_column`], fused with the widening [`crate::types::FixedInt`] defines.
///
/// Spelled with byte-array literals rather than composed from `decode_pk_column`
/// and [`widen_pk_be`], which at `-O0` costs a per-row caller an out-of-line call
/// and a 16-byte stack image. The assert carries a static message: an
/// `#[inline(always)]` body duplicates a formatted `Arguments` block into every
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

/// Widest PK region [`widen_pk_be`] packs into a `u128`; a wider key is ordered
/// and hashed as raw bytes.
pub const NARROW_PK_MAX_BYTES: usize = 16;

// ---------------------------------------------------------------------------
// Width-tagged PK byte buffer
// ---------------------------------------------------------------------------

/// Width-tagged PK byte buffer — the one OPK byte container, held by the engine's
/// key encoders and the client's row-identity maps alike. Only `bytes[..len]` is
/// meaningful and the tail past it is always zero, which is what lets
/// [`Self::padded`] widen a key without touching it; every writer below derives
/// `len` from the span it hands out.
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

    /// Extend the key by `n` bytes, written by `f` into exactly that span.
    #[inline(always)]
    fn append(&mut self, n: usize, f: impl FnOnce(&mut [u8])) {
        let at = self.len as usize;
        f(&mut self.bytes[at..at + n]);
        self.len = (at + n) as u8;
    }

    /// Re-tag the key as `width` bytes (zeroing anything a wider key left past it)
    /// and hand them to `f`, returning what `f` returns.
    #[inline(always)]
    pub fn write<R>(&mut self, width: usize, f: impl FnOnce(&mut [u8]) -> R) -> R {
        self.set_len(width);
        f(&mut self.bytes[..width])
    }

    #[inline]
    fn debug_assert_zero_tail(&self) {
        debug_assert!(
            self.bytes[self.len as usize..].iter().all(|&b| b == 0),
            "PkBuf tail past len must be zero; widening would carry stale bytes",
        );
    }

    /// The key's OPK bytes — the single PK accessor.
    #[inline(always)]
    pub fn pk_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    /// Bytes per key — what [`Self::padded`] and [`Self::widened`] widen from.
    #[inline(always)]
    pub fn width(&self) -> usize {
        self.len as usize
    }

    /// The key zero-padded to `width` bytes. Widens an index leading-key span to
    /// a full PK stride.
    #[inline]
    pub fn padded(&self, width: usize) -> &[u8] {
        debug_assert!(self.len as usize <= width && width <= crate::MAX_PK_BYTES);
        self.debug_assert_zero_tail();
        &self.bytes[..width]
    }

    /// Owning [`Self::padded`]: a `len` bump, no copy.
    #[inline]
    pub fn widened(mut self, width: usize) -> Self {
        debug_assert!(self.len as usize <= width && width <= crate::MAX_PK_BYTES);
        self.debug_assert_zero_tail();
        self.len = width as u8;
        self
    }

    /// Re-tag as `len` bytes, re-zeroing whatever a wider previous key left past
    /// it. The one writer of `len`, and private: the public writers above derive
    /// their width from the span they hand out.
    #[inline(always)]
    fn set_len(&mut self, len: usize) {
        debug_assert!(len <= crate::MAX_PK_BYTES);
        if (self.len as usize) > len {
            self.bytes[len..self.len as usize].fill(0);
        }
        self.len = len as u8;
    }
}

#[cfg(test)]
#[path = "tests/pk.rs"]
mod tests;
