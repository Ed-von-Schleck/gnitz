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
//!
//! The owned key forms live here too: [`PkBuf`], one key, and [`PkKeys`], a
//! strictly ascending list of keys of one stride.

use crate::codec::{Reader, Wire, Writer};
use crate::{TypeCode, radix_sort_by_key, zip_cells};
use smallvec::SmallVec;

/// `$body` once per fixed cell width — 1/2/4/8/16 bytes, the widths of a PK
/// column and of every non-string payload cell — with `$t` the unsigned integer
/// of `$width` bytes.
#[macro_export]
macro_rules! by_cell_width {
    ($width:expr, |$t:ident| $body:expr) => {
        match $width {
            16 => {
                type $t = u128;
                $body
            }
            8 => {
                type $t = u64;
                $body
            }
            4 => {
                type $t = u32;
                $body
            }
            2 => {
                type $t = u16;
                $body
            }
            1 => {
                type $t = u8;
                $body
            }
            other => unreachable!("a fixed cell is 1/2/4/8/16 bytes, got {other}"),
        }
    };
}

/// Write the low `dst.len()` bytes of the key image `image` big-endian into `dst`: the
/// column's OPK bytes. A native value's image is [`key_image`].
#[inline(always)]
pub fn store_opk(dst: &mut [u8], image: u128) {
    by_cell_width!(dst.len(), |T| {
        let d: &mut [u8; std::mem::size_of::<T>()] = dst.try_into().unwrap();
        *d = (image as T).to_be_bytes();
    })
}

/// [`store_opk`] appended to `buf` as one `width`-byte column.
#[inline(always)]
pub fn push_opk(buf: &mut Vec<u8>, width: usize, image: u128) {
    by_cell_width!(width, |T| {
        buf.extend_from_slice(&(image as T).to_be_bytes());
    })
}

/// The mask of a `width`-byte column's images: its low `8·width` bits.
#[inline(always)]
pub const fn image_mask(width: usize) -> u128 {
    u128::MAX >> (128 - 8 * width)
}

/// Whether a native value — passed `v as u128`, sign-extended if it is signed —
/// fits a `width`-byte column: the bits outside its image are all clear (an
/// unsigned value) or all set (a sign-extended negative), so dropping them
/// loses nothing.
#[inline(always)]
pub const fn native_fits(v: u128, width: usize) -> bool {
    let outside = !image_mask(width);
    let dropped = v & outside;
    dropped == 0 || dropped == outside
}

/// A native value's image in its column's key order: masked to the type's width, sign bit
/// flipped for a signed type — the OPK bytes read as a big-endian integer. Its own
/// inverse on a masked value, which is how [`decode_opk_u128`] reads a cell back.
#[inline(always)]
pub fn key_image(tc: TypeCode, native: u128) -> u128 {
    (native & image_mask(tc.wire_stride())) ^ opk_bias(tc)
}

/// Decode one OPK cell of a column of type `tc` to its native value, zero-extended
/// from the type's width.
#[inline(always)]
pub fn decode_opk_u128(opk: &[u8], tc: TypeCode) -> u128 {
    key_image(tc, widen_pk_be(opk))
}

/// The image of zero in a column of type `tc`: its sign bit if signed, else 0.
/// Read from a table, so a client encoding a native value pays a load rather
/// than a variable-width `u128` shift.
#[inline(always)]
pub fn opk_bias(tc: TypeCode) -> u128 {
    OPK_BIAS[tc.as_wire() as usize]
}

/// [`opk_bias`] by wire type code; a code past the table's end fails the build.
const OPK_BIAS: [u128; 32] = {
    let mut t = [0u128; 32];
    let mut i = 0;
    while i < TypeCode::ALL.len() {
        let tc = TypeCode::ALL[i];
        t[tc.as_wire() as usize] = (tc.is_signed_int() as u128) << (tc.wire_stride() * 8 - 1);
        i += 1;
    }
    t
};

/// [`key_image`] of a `W`-byte native little-endian integer cell, `signed` its
/// column's signedness: zero-extended, the sign bit of its width flipped.
#[inline(always)]
pub fn native_key_image<const W: usize>(cell: &[u8; W], signed: bool) -> u128 {
    let mut le = [0u8; 16];
    le[..W].copy_from_slice(cell);
    u128::from_le_bytes(le) ^ ((signed as u128) << (8 * W - 1))
}

/// Decode one OPK cell into its native little-endian bytes: `src` and `dst` are both
/// the column's width, `signed` the column's signedness.
///
/// Each arm stores through a `&mut [u8; W]`: a runtime-length `copy_from_slice` per
/// arm tail-merges into one shared `memcpy` call in a caller's row loop.
#[inline(always)]
pub fn decode_pk_cell(src: &[u8], signed: bool, dst: &mut [u8]) {
    debug_assert_eq!(dst.len(), src.len());
    by_cell_width!(src.len(), |T| {
        let v = T::from_be_bytes(src.try_into().unwrap()) ^ ((signed as T) << (T::BITS - 1));
        let d: &mut [u8; std::mem::size_of::<T>()] = dst.try_into().unwrap();
        *d = v.to_le_bytes();
    })
}

/// Decode the PK column of type `tc` at byte `off` of each `stride`-byte row of `pk`
/// into `dst`'s native little-endian cells, `tc.wire_stride()` bytes each.
pub fn decode_pk_cells(pk: &[u8], stride: usize, off: usize, tc: TypeCode, dst: &mut [u8]) {
    let (width, signed) = (tc.wire_stride(), tc.is_signed_int());
    debug_assert_eq!(dst.len() % width, 0, "decode_pk_cells: whole cells");
    by_cell_width!(width, |T| {
        const W: usize = std::mem::size_of::<T>();
        zip_cells::<W, _>(pk, stride, off, dst.as_chunks_mut::<W>().0.iter_mut(), |s, d| {
            decode_pk_cell(s, signed, d)
        })
    })
}

/// Encode `src`'s native little-endian cells of type `tc`, `tc.wire_stride()` bytes
/// each, as the OPK column at byte `off` of each `stride`-byte row of `pk`: the inverse
/// of [`decode_pk_cells`].
pub fn encode_pk_cells(src: &[u8], tc: TypeCode, pk: &mut [u8], stride: usize, off: usize) {
    let width = tc.wire_stride();
    debug_assert_eq!(src.len() % width, 0, "encode_pk_cells: whole cells");
    by_cell_width!(width, |T| {
        const W: usize = std::mem::size_of::<T>();
        assert!(off + W <= stride, "a cell lies inside its row");
        let bias = opk_bias(tc) as T;
        let cells = src.as_chunks::<W>().0;
        if stride == W {
            for (cell, dst) in cells.iter().zip(pk.as_chunks_mut::<W>().0) {
                *dst = (T::from_le_bytes(*cell) ^ bias).to_be_bytes();
            }
        } else {
            for (cell, row) in cells.iter().zip(pk.chunks_exact_mut(stride)) {
                let d: &mut [u8; W] = (&mut row[off..off + W]).try_into().unwrap();
                *d = (T::from_le_bytes(*cell) ^ bias).to_be_bytes();
            }
        }
    })
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

/// OPK keys of one stride, strictly ascending — the order a forward gather
/// sweeps. Any number of keys: the request frame is the only bound on a list.
/// Fields private: every constructor sorts and dedups, or validates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkKeys {
    stride: u8,
    /// Inline for a list of at most `MAX_PK_BYTES`, such as one key.
    bytes: PkBytes,
}

type PkBytes = SmallVec<[u8; MAX_PK_BYTES]>;

impl PkKeys {
    /// Sort and dedup `keys`, each exactly `stride` bytes.
    pub fn from_keys<'a>(stride: usize, keys: impl IntoIterator<Item = &'a [u8]>) -> Self {
        assert!((1..=MAX_PK_BYTES).contains(&stride), "PkKeys: stride {stride}");
        let keys = keys
            .into_iter()
            .inspect(|k| assert_eq!(k.len(), stride, "PkKeys: key width"));
        match stride {
            // The width a PK most often has, read without `from_opk`'s width test.
            8 => Self::sorted(8, keys.map(|k| u64::from_be_bytes(k.try_into().unwrap()))),
            _ => crate::pk_width_dispatch!(stride, |K| Self::sorted(stride, keys.map(K::from_opk))),
        }
    }

    /// Sort and dedup key images, each stored as its low `stride` bytes
    /// big-endian. An image orders as those bytes do.
    pub fn from_images(stride: usize, images: impl IntoIterator<Item = u128>) -> Self {
        assert!((1..=NARROW_PK_MAX_BYTES).contains(&stride), "PkKeys: stride {stride}");
        let images = images.into_iter();
        // Left-aligned, as the sort key of those bytes is.
        match stride <= 8 {
            true => Self::sorted(stride, images.map(|v| (v as u64) << (64 - 8 * stride))),
            false => Self::sorted(stride, images.map(|v| v << (128 - 8 * stride))),
        }
    }

    /// `keys` sorted, then deduped while written: each a store of its whole
    /// sort key, whose zero spill past `stride` the next key overwrites. A list
    /// of at most two keys, and its bytes up to `MAX_PK_BYTES`, end up inline.
    fn sorted<'a, K: PkSortKey<'a>>(stride: usize, keys: impl Iterator<Item = K>) -> Self {
        let mut keys: SmallVec<[K; 2]> = keys.collect();
        K::sort_by_key(&mut keys, |&k| k);
        let width = K::put_width(stride);
        let mut bytes = PkBytes::from_elem(0, keys.len() * stride + width - stride);
        let mut at = 0;
        if let Some((&first, rest)) = keys.split_first() {
            first.put(&mut bytes[..width]);
            at = stride;
            let mut prev = first;
            for &k in rest {
                k.put(&mut bytes[at..at + width]);
                at += stride * (k != prev) as usize;
                prev = k;
            }
        }
        bytes.truncate(at);
        // Repeats or the spill may leave a list that fits inline on the heap.
        bytes.shrink_to_fit();
        Self::new(stride, bytes)
    }

    /// Keys of `stride` already concatenated in strictly ascending order.
    /// A list past the inline capacity keeps `bytes` as its storage.
    pub fn from_sorted(stride: usize, bytes: Vec<u8>) -> Self {
        assert!((1..=MAX_PK_BYTES).contains(&stride), "PkKeys: stride {stride}");
        assert_eq!(bytes.len() % stride, 0, "PkKeys: key width");
        assert!(
            strictly_ascending(&bytes, stride),
            "PkKeys: keys are not strictly ascending"
        );
        Self::heap(stride, bytes)
    }

    /// Keys of `stride` concatenated in ascending order, adjacent repeats
    /// dropped. A list of at most `MAX_PK_BYTES` never touches the heap.
    pub fn from_ascending(stride: usize, bytes: &[u8]) -> Self {
        assert!((1..=MAX_PK_BYTES).contains(&stride), "PkKeys: stride {stride}");
        assert_eq!(bytes.len() % stride, 0, "PkKeys: key width");
        let mut out = PkBytes::with_capacity(bytes.len());
        for key in bytes.chunks_exact(stride) {
            if out.len() < stride || out[out.len() - stride..] != *key {
                out.extend_from_slice(key);
            }
        }
        debug_assert!(strictly_ascending(&out, stride), "PkKeys: keys do not ascend");
        Self::new(stride, out)
    }

    /// [`Self::from_sorted`] for keys off the wire: `Err` where they do not
    /// strictly ascend. `stride` is in range and `bytes` a whole number of keys.
    pub(crate) fn checked(stride: usize, bytes: &[u8]) -> Result<Self, String> {
        debug_assert_eq!(bytes.len() % stride, 0, "PkKeys: key width");
        if !strictly_ascending(bytes, stride) {
            return Err("keys are not strictly ascending".to_string());
        }
        Ok(Self::new(stride, SmallVec::from_slice(bytes)))
    }

    fn new(stride: usize, bytes: PkBytes) -> Self {
        PkKeys { stride: stride as u8, bytes }
    }

    /// `bytes` as the list's storage: a list within the inline capacity is
    /// copied inline, a longer one keeps its buffer.
    fn heap(stride: usize, bytes: Vec<u8>) -> Self {
        Self::new(stride, SmallVec::from_vec(bytes))
    }

    pub fn stride(&self) -> usize {
        self.stride as usize
    }

    pub fn len(&self) -> usize {
        self.bytes.len() / self.stride()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn iter(&self) -> std::slice::ChunksExact<'_, u8> {
        self.bytes.chunks_exact(self.stride())
    }

    /// Keep only the keys `keep` admits. A subsequence of an ascending list
    /// ascends, so nothing is re-checked.
    pub fn retain(&mut self, mut keep: impl FnMut(&[u8]) -> bool) {
        let stride = self.stride();
        let bytes = &mut self.bytes;
        let mut kept = 0;
        for at in (0..bytes.len()).step_by(stride) {
            if keep(&bytes[at..at + stride]) {
                bytes.copy_within(at..at + stride, kept);
                kept += stride;
            }
        }
        bytes.truncate(kept);
    }

    /// The first and last key, or `None` for an empty list.
    pub fn bounds(&self) -> Option<(&[u8], &[u8])> {
        let first = self.iter().next()?;
        Some((first, &self.bytes[self.bytes.len() - self.stride()..]))
    }
}

/// Whether `bytes`, read as keys of `stride`, strictly ascend.
pub(crate) fn strictly_ascending(bytes: &[u8], stride: usize) -> bool {
    bytes.chunks_exact(stride).is_sorted_by(|a, b| a < b)
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
    // The last byte is read first, so its bounds check covers the others.
    macro_rules! be {
        ($ty:ty, $($i:literal),+; $last:literal) => {{
            let last = opk[$last];
            <$ty>::from_be_bytes([$(opk[$i]),+, last])
        }};
    }
    match fi {
        F::U8 => opk[0] as i64,
        F::I8 => (opk[0] ^ 0x80) as i8 as i64,
        F::U16 => be!(u16, 0; 1) as i64,
        F::I16 => (be!(u16, 0; 1) ^ (1 << 15)) as i16 as i64,
        F::U32 => be!(u32, 0, 1, 2; 3) as i64,
        F::I32 => (be!(u32, 0, 1, 2; 3) ^ (1 << 31)) as i32 as i64,
        F::U64 => be!(u64, 0, 1, 2, 3, 4, 5, 6; 7) as i64,
        F::I64 => (be!(u64, 0, 1, 2, 3, 4, 5, 6; 7) ^ (1 << 63)) as i64,
    }
}

// ---------------------------------------------------------------------------
// Width-tagged PK byte buffer
// ---------------------------------------------------------------------------

/// Maximum byte width of a PK region per row. Product of `MAX_PK_COLUMNS`
/// and the per-column ceiling (16 == max wire stride of any type valid as a
/// PK column — U128, UUID, I128; STRING and BLOB are rejected by schema
/// validation). Auto-tracks growth of `MAX_PK_COLUMNS`.
pub const MAX_PK_BYTES: usize = crate::MAX_PK_COLUMNS * 16;

// PkBuf::len, PkKeys::stride and a PkSet's stride byte hold a key width as a u8.
const _: () = assert!(MAX_PK_BYTES <= u8::MAX as usize);

/// An OPK key of up to `MAX_PK_BYTES` bytes, held by the engine's key encoders and
/// the client's row-identity maps alike. `bytes[..len]` is the key and the tail past
/// it is zero: every constructor starts from a zeroed array and no method shortens a
/// key, which is what [`Self::widened`] reads.
#[derive(Clone, Copy)]
pub struct PkBuf {
    bytes: [u8; MAX_PK_BYTES],
    len: u8,
}

/// Prints only `bytes[..len]`, so assertion diffs over keys stay readable.
impl std::fmt::Debug for PkBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PkBuf({:02x?})", self.pk_bytes())
    }
}

// Eq/Hash/Ord read `bytes[..len]` and nothing else, so they agree with `<[u8]>`'s,
// as the `Borrow<[u8]>` impl below requires.
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

/// The key as a `u32`-length-prefixed section; one wider than `MAX_PK_BYTES` is
/// refused.
impl Wire for PkBuf {
    fn write(&self, w: &mut Writer) {
        w.bytes32(self.pk_bytes());
    }
    fn read(r: &mut Reader) -> Result<Self, String> {
        let key = r.bytes32()?;
        if key.len() > MAX_PK_BYTES {
            return Err(format!("a {}-byte key exceeds {MAX_PK_BYTES}", key.len()));
        }
        Ok(PkBuf::from_bytes(key))
    }
}

impl PkBuf {
    /// All-zero key of the given width — the empty-shard / placeholder form.
    #[inline(always)] // per-row across the crate boundary; dev builds inline nothing else
    pub fn zeroed(len: usize) -> Self {
        debug_assert!(len <= MAX_PK_BYTES);
        PkBuf {
            bytes: [0u8; MAX_PK_BYTES],
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
            slice.len() <= MAX_PK_BYTES,
            "PkBuf::from_bytes: length {} exceeds MAX_PK_BYTES {}",
            slice.len(),
            MAX_PK_BYTES,
        );
        let mut bytes = [0u8; MAX_PK_BYTES];
        bytes[..slice.len()].copy_from_slice(slice);
        PkBuf { bytes, len: slice.len() as u8 }
    }

    /// Extend the key by one `width`-byte column holding `image`, written as
    /// [`store_opk`] writes it.
    #[inline(always)]
    pub fn push(&mut self, width: usize, image: u128) {
        let at = self.len as usize;
        store_opk(&mut self.bytes[at..at + width], image);
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
            self.len as usize <= width && width <= MAX_PK_BYTES,
            "PkBuf::widened: width outside len..=MAX_PK_BYTES"
        );
        self.len = width as u8;
        self
    }
}

// ---------------------------------------------------------------------------
// Narrow-region PK key packing
// ---------------------------------------------------------------------------

/// BE sort-key packer over an OPK region. Left-aligns the bytes at the MSB end
/// of a `u128` and reads big-endian, so `pack_pk_be(a).cmp(&pack_pk_be(b))`
/// equals the lexicographic byte order of the OPK regions. Narrow (`len ≤ 16`) =
/// the exact key; wide (`len > 16`) = the order-preserving leading-16 prefix
/// (authoritative whenever two prefixes differ; a prefix collision needs a
/// slice-order tiebreak).
///
/// The `{2, 4, 8, ≥16}` arms load the dominant scalar and `U128`/wide-prefix
/// widths straight into a register, value-equal to the pad-and-copy (a narrow
/// value occupies the high bits, the low bits zero; `≥16` reads the
/// order-preserving leading 16 bytes). The remaining arms compose the same
/// fixed-width loads: none copies through a runtime length, which would be an
/// out-of-line `memcpy` per key. Kept apart from [`widen_pk_be`]'s
/// right-aligned image, which retires more instructions per compare at 8, 12
/// and 16 bytes.
///
/// NOT a value accessor — for a U64 OPK value 1 (`[0,…,0,1]` at `[..8]`) this
/// packs as `1·2^64`, not 1.
#[inline(always)]
pub fn pack_pk_be(pk_bytes: &[u8]) -> u128 {
    match pk_bytes.len() {
        8 => (u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128) << 64,
        len if len >= 16 => u128::from_be_bytes(pk_bytes[..16].try_into().unwrap()),
        // Two overlapping loads: the shift drops the tail load's bytes below 8.
        len @ 9..=15 => {
            let hi = u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128;
            let tail = u64::from_be_bytes(pk_bytes[len - 8..len].try_into().unwrap());
            (hi << 64) | (tail << (8 * (16 - len))) as u128
        }
        4 => (u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128) << 96,
        2 => (u16::from_be_bytes(pk_bytes[..2].try_into().unwrap()) as u128) << 112,
        1 => (pk_bytes[0] as u128) << 120,
        3 => {
            let hi = u16::from_be_bytes(pk_bytes[..2].try_into().unwrap()) as u128;
            (hi << 112) | ((pk_bytes[2] as u128) << 104)
        }
        5 => {
            let hi = u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128;
            (hi << 96) | ((pk_bytes[4] as u128) << 88)
        }
        6 => {
            let hi = u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128;
            let lo = u16::from_be_bytes(pk_bytes[4..6].try_into().unwrap()) as u128;
            (hi << 96) | (lo << 80)
        }
        7 => {
            let hi = u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128;
            let mid = u16::from_be_bytes(pk_bytes[4..6].try_into().unwrap()) as u128;
            (hi << 96) | (mid << 80) | ((pk_bytes[6] as u128) << 72)
        }
        // Width 0: an empty OPK region packs to zero.
        _ => 0,
    }
}

/// A whole-PK sort key in the slice order of the bytes it was built from, so a
/// key sort needs no PK-byte tiebreak. Keys compare only
/// against keys built from the same number of bytes.
pub trait PkSortKey<'a>: Ord + Copy + Default {
    fn from_opk(opk: &'a [u8]) -> Self;
    /// The bytes [`Self::put`] writes for a key built from `stride` bytes.
    fn put_width(stride: usize) -> usize;
    /// The key's bytes big-endian into `out`, [`Self::put_width`] long: the
    /// `stride` bytes it was built from, then zeros.
    fn put(self, out: &mut [u8]);
    /// Sort `v` by `key`, unstably: a radix sort where the key is an integer.
    fn sort_by_key<T: Copy + Default>(v: &mut [T], key: impl Fn(&T) -> Self);
}

/// Run `$body` with `$k` bound to the [`PkSortKey`] matching `$stride` — the one
/// statement of the width taxonomy the impls below define.
#[macro_export]
macro_rules! pk_width_dispatch {
    ($stride:expr, |$k:ident| $body:expr $(,)?) => {
        match $stride {
            0..=8 => {
                type $k<'k> = u64;
                $body
            }
            9..=16 => {
                type $k<'k> = u128;
                $body
            }
            17..=32 => {
                type $k<'k> = [u128; 2];
                $body
            }
            _ => {
                type $k<'k> = &'k [u8];
                $body
            }
        }
    };
}

impl PkSortKey<'_> for u64 {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> u64 {
        if opk.len() >= 8 {
            u64::from_be_bytes(opk[..8].try_into().unwrap())
        } else {
            // `pack_pk_be` left-aligns a narrow key with fixed-width register loads.
            (pack_pk_be(opk) >> 64) as u64
        }
    }
    #[inline(always)]
    fn put_width(_: usize) -> usize {
        size_of::<Self>()
    }
    #[inline(always)]
    fn put(self, out: &mut [u8]) {
        out.copy_from_slice(&self.to_be_bytes());
    }
    #[inline]
    fn sort_by_key<T: Copy + Default>(v: &mut [T], key: impl Fn(&T) -> Self) {
        radix_sort_by_key(v, key)
    }
}

impl PkSortKey<'_> for u128 {
    /// The leading 8 bytes, then the trailing 8 shifted past the bytes they
    /// share with them below 16 bytes: the key left-aligned, with no length
    /// dispatch, so a search's comparison closure stays small enough to inline.
    /// A byte stands in the key once, so a radix sort passes over it once.
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> u128 {
        let len = opk.len();
        let hi = u64::from_be_bytes(opk[..8].try_into().unwrap()) as u128;
        let tail = u64::from_be_bytes(opk[len - 8..].try_into().unwrap()) << (8 * (16 - len));
        (hi << 64) | tail as u128
    }
    #[inline(always)]
    fn put_width(_: usize) -> usize {
        size_of::<Self>()
    }
    #[inline(always)]
    fn put(self, out: &mut [u8]) {
        out.copy_from_slice(&self.to_be_bytes());
    }
    #[inline]
    fn sort_by_key<T: Copy + Default>(v: &mut [T], key: impl Fn(&T) -> Self) {
        radix_sort_by_key(v, key)
    }
}

impl<'a> PkSortKey<'a> for &'a [u8] {
    #[inline(always)]
    fn from_opk(opk: &'a [u8]) -> Self {
        opk
    }
    #[inline(always)]
    fn put_width(stride: usize) -> usize {
        stride
    }
    #[inline(always)]
    fn put(self, out: &mut [u8]) {
        out.copy_from_slice(self);
    }
    #[inline]
    fn sort_by_key<T: Copy + Default>(v: &mut [T], key: impl Fn(&T) -> Self) {
        v.sort_unstable_by_key(key)
    }
}

impl PkSortKey<'_> for [u128; 2] {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> [u128; 2] {
        // The leading 16 bytes, then the trailing 1..=16 left-aligned: array `Ord`
        // is lexicographic, as byte order is.
        let hi = u128::from_be_bytes(opk[..16].try_into().unwrap());
        if opk.len() == 32 {
            [hi, u128::from_be_bytes(opk[16..32].try_into().unwrap())]
        } else {
            [hi, pack_pk_be(&opk[16..])]
        }
    }
    #[inline(always)]
    fn put_width(_: usize) -> usize {
        32
    }
    #[inline(always)]
    fn put(self, out: &mut [u8]) {
        let (high, low) = out.split_at_mut(16);
        self[0].put(high);
        self[1].put(low);
    }
    #[inline]
    fn sort_by_key<T: Copy + Default>(v: &mut [T], key: impl Fn(&T) -> Self) {
        radix_sort_by_key(v, key)
    }
}

#[cfg(test)]
#[path = "tests/pk.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/pk.rs"]
mod bench;
