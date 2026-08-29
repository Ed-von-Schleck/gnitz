//! Order-preserving primary-key (OPK) primitives.
//!
//! These pure layout/key operations sit *below* both `schema` and `storage`:
//! they encode a PK region — a whole one, a seek key reassembled from its wire
//! pair, or an index's leading-column span — to its order-preserving big-endian
//! image, compare two such images with a raw `memcmp`, pack a narrow region
//! into a sort key, carry a width-tagged PK byte buffer, and derive the
//! half-open key range a `RangeDescriptor`'s cut pair denotes — and compose the
//! two multi-column OPK keys: a secondary index's leading span
//! ([`IndexKeySpec`]) and a reindex's synthetic PK ([`ReindexPacker`]). None of
//! them reaches up into storage — the dependency runs `storage → schema::key`,
//! the legitimate downward direction. This module is the one import path: every
//! caller, storage included, names `crate::schema::key::X`, and every
//! native→OPK encoder lives here so the write, seek and route sides cannot
//! spell the encoding differently.

use std::cmp::Ordering;

use gnitz_expr::RowSource;
use gnitz_wire::{Cut, RangeDescriptor, ScalarKind, NARROW_PK_MAX_BYTES};

use crate::foundation::xxh::{self, RowHasher};
use crate::schema::{
    type_code, ColumnLocator, DerivedSchema, SchemaColumn, SchemaDescriptor, TypeCode, MAX_PK_BYTES, MAX_PK_COLUMNS,
};

// ---------------------------------------------------------------------------
// Column-aware PK byte-region comparator
// ---------------------------------------------------------------------------

/// Raw byte comparator for PK regions.
///
/// After the OPK-at-rest flip every PK region at rest holds order-preserving
/// big-endian bytes, so unsigned lexicographic byte comparison is numerically
/// identical to the typed comparison of the PK columns for any width. `a.cmp(b)`
/// compiles to an optimal `memcmp`. `a` and `b` are the OPK bytes produced by
/// `Batch::get_pk_bytes` / `MappedShard::get_pk_bytes`.
#[inline(always)]
pub fn compare_pk_bytes(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}

/// Typed lexicographic OPK ordering of two **equal-length** PK regions — the
/// comparator the N-way merge and the read-cursor loser tree read through their
/// sources. Returns the same `Ordering` as `compare_pk_bytes` at every width,
/// settling the common case on the leading-16 `pack_pk_be` image. For `len ≤ 16`
/// that image is the *whole* PK and is injective, so a `pack_pk_be` tie is
/// already a byte-equal PK — the byte
/// compare is skipped (it would be a guaranteed-`Equal` `memcmp`). Only `len > 16`
/// can tie on the 16-byte prefix while differing later, so the full-byte
/// `compare_pk_bytes` tiebreak runs only there. No stride / width-class dispatch.
#[inline(always)]
pub(crate) fn compare_pk_ordering(a: &[u8], b: &[u8]) -> Ordering {
    debug_assert_eq!(a.len(), b.len(), "compare_pk_ordering on unequal PK widths");
    match pack_pk_be(a).cmp(&pack_pk_be(b)) {
        Ordering::Equal if a.len() > NARROW_PK_MAX_BYTES => compare_pk_bytes(a, b),
        ord => ord,
    }
}

/// OPK byte-equality of two **equal-length** PK regions (a full PK, or a shared
/// equi-prefix sliced to the same width on both sides) — the equality sibling of
/// [`compare_pk_ordering`]. `pk_bytes_eq(a, b) == (a == b)` at every width: for
/// `len ≤ 16` the executed path is the register `pack_pk_be` compare with no
/// `bcmp`/`memcmp` call (OPK encoding is a bijection, so the left-aligned `u128`
/// images are equal iff the keys are byte-equal); for `len > 16` it confirms a
/// leading-16 prefix tie with the full `compare_pk_bytes`. Operates under the same
/// equal-width contract as `compare_pk_ordering`. Use at every merge/group fold or
/// probe that tests "same PK".
#[inline(always)]
pub(crate) fn pk_bytes_eq(a: &[u8], b: &[u8]) -> bool {
    compare_pk_ordering(a, b) == Ordering::Equal
}

/// `min <= key <= max` over OPK bytes. The exact-match probes gate on this
/// before searching, so a key outside a block's bounds costs two `memcmp`s
/// instead of a `log n` walk over cold cache lines. The cursor's lower-bound
/// seeks do not: they need a landing position on a miss, not a verdict.
#[inline]
pub(crate) fn pk_in_range(min: &[u8], max: &[u8], key: &[u8]) -> bool {
    compare_pk_bytes(min, key) != Ordering::Greater && compare_pk_bytes(key, max) != Ordering::Greater
}

// ---------------------------------------------------------------------------
// Order-preserving PK encoder
// ---------------------------------------------------------------------------

/// OPK-encode a PK from its **native LE** bytes, returning it as a `pk_stride`-
/// wide [`PkBuf`]. `native_le` must hold at least `pk_stride` bytes (in pk-list
/// column order); any trailing bytes are ignored. This is the single native→OPK
/// encoder for **every** PK width — a caller holding a narrow value passes
/// `&value.to_le_bytes()`; the seek path passes the reassembled wire image via
/// [`seek_opk_bytes`].
///
/// The schema-typed face of [`gnitz_wire::encode_pk_tuple`], fed
/// `schema.pk_columns()` — the *same* iterator `compare_pk_bytes` walks — so a
/// non-identity `pk_indices` (e.g. `[1, 0]`) encodes in pk-list order, matching
/// the comparator.
///
/// The encoding is **injective**: `encode(a) == encode(b)` iff `a == b`
/// byte-for-byte, because each column's transform is a bijection on its byte
/// range. Consolidation grouping relies on this — an OPK equality test is
/// exactly a PK-byte equality test.
#[inline]
pub(crate) fn opk_key(schema: &SchemaDescriptor, native_le: &[u8]) -> PkBuf {
    let stride = schema.pk_stride() as usize;
    debug_assert!(
        native_le.len() >= stride,
        "opk_key: native_le ({}) shorter than pk_stride ({stride})",
        native_le.len(),
    );
    let mut out = PkBuf::zeroed(stride);
    gnitz_wire::encode_pk_tuple(
        schema.pk_columns().map(|(_, col)| (col.size() as usize, col.type_code)),
        &native_le[..stride],
        &mut out.bytes[..stride],
    );
    out
}

/// Reassemble the native seek image from the wire pair `(low, extra)` — the
/// inverse of `PkTuple::split_wire` — and OPK-encode it via [`opk_key`]. The
/// shared seek-key encoder for the master partition router (`fan_out_seek`) and
/// the worker SEEK handler (`seek_family`) at every PK width. The seek frame
/// carries the key as native LE column bytes (it bypasses the client's
/// `build_pk_region`, so the bytes are not yet OPK): the low
/// [`NARROW_PK_MAX_BYTES`] ride in `low`, a wide PK's remaining suffix in
/// `extra` (empty for narrow PKs).
///
/// Errors if `extra` is shorter than that suffix.
pub fn seek_opk_bytes(schema: &SchemaDescriptor, low: u128, extra: &[u8]) -> Result<PkBuf, String> {
    let stride = schema.pk_stride() as usize;
    if stride > MAX_PK_BYTES {
        return Err(format!("PK stride {stride} exceeds MAX_PK_BYTES {MAX_PK_BYTES}"));
    }
    let needed = stride.saturating_sub(NARROW_PK_MAX_BYTES);
    if extra.len() < needed {
        return Err(format!(
            "PK stride {stride} requires {needed} extra bytes, got {}",
            extra.len()
        ));
    }
    // Native image = `low`'s 16 LE bytes, then the wide suffix. The upper bound
    // is `16 + needed` (not `stride`): for a narrow PK `needed == 0` makes it the
    // empty copy `le[16..16]` rather than the inverted range `le[16..stride]`.
    let mut le = [0u8; MAX_PK_BYTES];
    le[..NARROW_PK_MAX_BYTES].copy_from_slice(&low.to_le_bytes());
    le[NARROW_PK_MAX_BYTES..NARROW_PK_MAX_BYTES + needed].copy_from_slice(&extra[..needed]);
    Ok(opk_key(schema, &le[..stride]))
}

/// OPK-encode native key values into one leading-key span: column `i` reads
/// `natives[i]`'s low source-width bytes and encodes at its target column's
/// (possibly promoted) width, packed tightly in span order. The returned
/// [`PkBuf`] is exactly the span — bytes past it stay zero, so it is also the
/// minimum full key of its group and may be widened with [`PkBuf::padded`].
///
/// The one native→OPK leading-span encoder, shared by the index seek path
/// (`IndexKeySpec::seek_prefix`, whose sources promote to wider index columns)
/// and the base-table PK range path (whose source and target types are equal,
/// making the promotion the identity arm of `encode_pk_column_promoted`). Both
/// must agree byte-for-byte with the write side's `IndexKeySpec::write_span`,
/// which is why they encode through the same call rather than each spelling it.
pub(crate) fn encode_leading_opk(cols: impl IntoIterator<Item = (u8, SchemaColumn)>, natives: &[u128]) -> PkBuf {
    let mut out = PkBuf::zeroed(0);
    let mut off = 0usize;
    for ((src_tc, target), native) in cols.into_iter().zip(natives) {
        let w = target.size() as usize;
        gnitz_wire::encode_pk_column_promoted(
            &native.to_le_bytes()[..gnitz_wire::wire_stride(src_tc)],
            src_tc,
            target.type_code,
            &mut out.bytes[off..off + w],
        );
        off += w;
    }
    out.set_len(off);
    out
}

/// OPK-encode a native index-key value into an index's leading key column, for a
/// prefix seek or a check-batch composite PK — the scalar sibling of
/// `IndexKeySpec::seek_prefix` for callers whose source column lives in another
/// table's schema (FK probes). `src_type` is the *source* column type (the value
/// in `native` is zero-extended): a signed source sign-extends from its native
/// width before OPK-encoding at the promoted `idx_key_type`, byte-identical to
/// the write-side `IndexKeySpec::write_span`.
///
/// The returned `PkBuf` is exactly the leading column; a caller matching against
/// a wider composite (the source-PK suffix is zero) widens it with
/// [`PkBuf::padded`].
#[inline]
pub fn index_opk_prefix(native: u128, src_type: u8, idx_key_type: u8) -> PkBuf {
    encode_leading_opk([(src_type, SchemaColumn::new(idx_key_type, 0))], &[native])
}

// ---------------------------------------------------------------------------
// Narrow-region PK key packing
// ---------------------------------------------------------------------------

/// BE sort-key packer over an OPK region. Left-aligns the bytes at the MSB end
/// of a `u128` and reads big-endian, so `pack_pk_be(a).cmp(&pack_pk_be(b))`
/// equals the lexicographic byte order of the OPK regions — exactly
/// `compare_pk_bytes`. Narrow (`len ≤ 16`) = the exact key; wide (`len > 16`) =
/// the order-preserving leading-16 prefix (authoritative whenever two prefixes
/// differ; a prefix collision needs a `compare_pk_bytes` tiebreak).
///
/// The `{2, 4, 8, ≥16}` arms load the dominant scalar and `U128`/wide-prefix
/// widths straight into a register, value-equal to the pad-and-copy (a narrow
/// value occupies the high bits, the low bits zero; `≥16` reads the
/// order-preserving leading 16 bytes) — pinned by
/// `pack_pk_be_specialization_matches_naive`. Widths 9..=15 get two overlapping
/// loads for the same reason; only 1/3/5/6/7 reach the pad-and-copy arm.
///
/// NOT a value accessor — for a U64 OPK value 1 (`[0,…,0,1]` at `[..8]`) this
/// packs as `1·2^64`, not 1. Opposite alignment from `gnitz_wire::widen_pk_be`
/// (right-aligned value recovery); never conflate them.
#[inline(always)]
pub(crate) fn pack_pk_be(pk_bytes: &[u8]) -> u128 {
    match pk_bytes.len() {
        8 => (u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128) << 64,
        len if len >= 16 => u128::from_be_bytes(pk_bytes[..16].try_into().unwrap()),
        4 => (u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128) << 96,
        2 => (u16::from_be_bytes(pk_bytes[..2].try_into().unwrap()) as u128) << 112,
        // 9..=15: two overlapping big-endian loads instead of a runtime-length
        // `copy_from_slice`, which lowers to an out-of-line `memcpy` per key. With
        // `m = len - 8`, the low `m` bytes of the tail load are exactly
        // `pk_bytes[8..len]`, since `len - m == 8`. Measured ~3x on this band —
        // the merge and AVI paths sit here (`GROUP BY <INT>` is 13,
        // `PRIMARY KEY (BIGINT, INT)` is 12).
        len @ 9..=15 => {
            let m = len - 8;
            let hi = u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128;
            let tail = u64::from_be_bytes(pk_bytes[len - 8..len].try_into().unwrap());
            let low = (tail & ((1u64 << (8 * m)) - 1)) as u128;
            (hi << 64) | (low << (64 - 8 * m))
        }
        // 1/3/5/6/7: too narrow for the overlapping-load trick (`len - 8` underflows).
        len => {
            let mut buf = [0u8; 16];
            buf[..len].copy_from_slice(pk_bytes);
            u128::from_be_bytes(buf)
        }
    }
}

/// The `stride` OPK bytes of a narrow PK value that is **already in OPK/route
/// space** — the widened image `widen_pk_be` produces, sign-flipped for a signed
/// key. Right-aligning it big-endian reproduces the key's OPK region at any
/// width. A *native* value must be encoded through [`opk_key`] instead, which
/// applies the per-column sign flip this does not.
///
/// The one home for "right-align a `u128` into an OPK of width `stride`" — every
/// synthetic-key writer (the batch PK setters, the reduce group-key emitters)
/// builds one, so the width checks below cannot be skipped by hand-rolling
/// `&pk.to_be_bytes()[16 - stride..]`, which silently truncates a value that
/// overflows the stride. Zero-cost: a stack value, no allocation.
pub(crate) struct NarrowPkOpk {
    be: [u8; 16],
    stride: usize,
}

impl NarrowPkOpk {
    #[inline(always)]
    pub(crate) fn new(pk: u128, stride: usize) -> Self {
        // Static message: `#[inline(always)]` puts this in every per-row caller,
        // and an `Arguments` value costs a stack slot even on a cold panic path.
        assert!(
            stride <= NARROW_PK_MAX_BYTES,
            "NarrowPkOpk::new: stride exceeds NARROW_PK_MAX_BYTES; use the raw-OPK-bytes setter"
        );
        debug_assert!(
            stride == 16 || (pk >> (stride * 8)) == 0,
            "narrow PK {pk} does not fit {stride} bytes",
        );
        NarrowPkOpk {
            be: pk.to_be_bytes(),
            stride,
        }
    }

    /// The `stride` order-preserving bytes — a full PK region for one row.
    #[inline(always)]
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.be[16 - self.stride..]
    }
}

/// A fixed-width order-preserving sort key built by left-aligning a row's
/// `pk_stride` OPK bytes big-endian. The OPK bytes are order-preserving, and every
/// row in one batch shares a single `pk_stride` (hence identical zero padding past
/// `opk.len()`), so for two equal-length OPK slices that fit in `size_of::<K>()`
/// bytes `K::from_opk(a).cmp(&K::from_opk(b)) == compare_pk_bytes(a, b)`. The key
/// is the *whole* PK image (not a 16-byte prefix), so a key sort is exact with no
/// PK-byte tiebreak. Used by the reduce sort to width-match the key to `pk_stride`
/// (`u64`/`u128`/`[u128; 2]` for strides ≤8/≤16/≤32; wider PKs byte-walk directly).
pub(crate) trait PkSortKey: Ord + Copy {
    fn from_opk(opk: &[u8]) -> Self;
}

/// Run `$body` with `$k` bound to the [`PkSortKey`] whose width matches `$stride`;
/// strides too wide to pack into a register key take `$wide`.
///
/// The one statement of the width taxonomy the impls below define. A stride→width
/// `match` spelled at a call site instead goes stale silently when an impl is
/// added: the new width keeps falling to `$wide` and merely gets slower, with
/// nothing to fail.
macro_rules! pk_width_dispatch {
    ($stride:expr, |$k:ident| $body:expr, $wide:expr $(,)?) => {
        match $stride {
            0..=8 => {
                type $k = u64;
                $body
            }
            9..=16 => {
                type $k = u128;
                $body
            }
            17..=32 => {
                type $k = [u128; 2];
                $body
            }
            _ => $wide,
        }
    };
}
pub(crate) use pk_width_dispatch;

impl PkSortKey for u64 {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> u64 {
        // Dispatched only for strides ≤ 8; the `== 8` arm loads the dominant
        // U64/I64 key straight into a register (no memcpy). The narrower strides
        // left-align at the MSB end so a raw `u64` compare is the OPK byte order.
        if opk.len() == 8 {
            u64::from_be_bytes(opk.try_into().unwrap())
        } else {
            let mut x = [0u8; 8];
            x[..opk.len()].copy_from_slice(opk);
            u64::from_be_bytes(x)
        }
    }
}

impl PkSortKey for u128 {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> u128 {
        // Identical left-align to `u64`, one width up; `pack_pk_be` is the canonical
        // left-align-to-`u128`, reused here. Dispatched only for strides ≤ 16.
        pack_pk_be(opk)
    }
}

impl PkSortKey for [u128; 2] {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> [u128; 2] {
        // Dispatched only for 17..=32-byte strides: hi = the full leading 16 bytes
        // (always a register load), lo = the trailing 1..=16 left-aligned. Array
        // `Ord` is lexicographic, so the low limb settles a leading-16-byte tie a
        // bare `u128` prefix would tie on.
        let hi = u128::from_be_bytes(opk[..16].try_into().unwrap());
        if opk.len() == 32 {
            [hi, u128::from_be_bytes(opk[16..32].try_into().unwrap())]
        } else {
            let mut lo = [0u8; 16];
            lo[..opk.len() - 16].copy_from_slice(&opk[16..]);
            [hi, u128::from_be_bytes(lo)]
        }
    }
}

// ---------------------------------------------------------------------------
// OPK span fingerprint
// ---------------------------------------------------------------------------

/// The 64-bit fingerprint of an OPK byte span — a row's whole PK region, or an
/// index's leading-key span. Every approximate-membership structure over OPK
/// keys derives its key here, so a probe key always equals the key inserted.
///
/// A narrow span right-aligns its big-endian bytes into a `u128`; a wider one
/// collapses to the xxh3 checksum of the whole span. Either way the full span
/// is hashed to 64 bits, keeping its entropy — this is deliberately not
/// `worker_for_pk_bytes`, which reduces the same two images to a worker index.
#[inline]
pub fn probe_key(opk: &[u8]) -> u64 {
    let fingerprint = if opk.len() > NARROW_PK_MAX_BYTES {
        xxh::checksum(opk) as u128
    } else {
        gnitz_wire::widen_pk_be(opk, opk.len())
    };
    xxh::checksum(&fingerprint.to_le_bytes())
}

// ---------------------------------------------------------------------------
// Width-tagged PK byte buffer
// ---------------------------------------------------------------------------

/// Width-tagged PK byte buffer. Plain value type — no generics, no
/// trait bounds. `len` mirrors the owning table's `pk_stride`, so a
/// manifest round-trip preserves the exact key width. Only
/// `bytes[..len]` is meaningful; the tail is always zero by
/// construction, which lets the single-PK fast path widen `bytes[..len]`
/// to a `u128` with no ambiguity.
#[derive(Clone, Copy)]
pub struct PkBuf {
    pub(crate) bytes: [u8; MAX_PK_BYTES],
    pub(crate) len: u8,
}

// Prints only the meaningful `bytes[..len]` span (the 80-byte tail is always
// zero by construction), so test assertion diffs over `PkBuf` keys are readable.
impl std::fmt::Debug for PkBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PkBuf({:02x?})", &self.bytes[..self.len as usize])
    }
}

// Manual Eq/Hash compare and hash only bytes[..len], so a HashSet<PkBuf>
// touches pk_stride bytes per key rather than the full 80-byte array.
impl PartialEq for PkBuf {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.bytes[..self.len as usize] == other.bytes[..other.len as usize]
    }
}
impl Eq for PkBuf {}

impl std::hash::Hash for PkBuf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bytes[..self.len as usize].hash(state);
    }
}

// Enables zero-allocation heterogeneous lookup: a raw &[u8] slice can be
// passed to HashSet<PkBuf>::contains / HashMap<PkBuf, _>::get without
// constructing a PkBuf. The Hash impl above hashes bytes[..len], matching
// <[u8] as Hash>, as the Borrow contract requires.
impl std::borrow::Borrow<[u8]> for PkBuf {
    fn borrow(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

// Byte-lexicographic (`memcmp`) order over `bytes[..len]` — identical to
// `compare_pk_bytes`, the canonical PK comparator, and consistent with the
// `Eq`/`Hash` impls above (which also read only `bytes[..len]`). This is the
// order `seek_first_positive_with_prefix` / `walk_to_positive_with_prefix`
// walk the index in, and the valid merge order for the unique pre-flight
// k-way merge whose keys are OPK leading-key spans of any width.
impl PartialOrd for PkBuf {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PkBuf {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_pk_bytes(self.pk_bytes(), other.pk_bytes())
    }
}

impl PkBuf {
    /// All-zero key of the given width — the zero-row / placeholder /
    /// empty-shard form, and the mutable scratch every reused key buffer starts
    /// from. The fields are crate-visible: in-crate callers write the meaningful
    /// span in place, outside go through `set_from` / `pk_bytes` / `padded`.
    pub fn zeroed(len: usize) -> Self {
        debug_assert!(len <= MAX_PK_BYTES);
        PkBuf {
            bytes: [0u8; MAX_PK_BYTES],
            len: len as u8,
        }
    }

    /// All-`0xFF` key of the given width — the top of the key space, and the
    /// mirror of [`Self::zeroed`]: an OPK region is compared as unsigned bytes,
    /// so no key of that width sorts above it. The upper bound an open-ended
    /// range takes, where a short key would sort *below* every full key sharing
    /// its prefix.
    pub fn max(len: usize) -> Self {
        debug_assert!(len <= MAX_PK_BYTES);
        let mut k = PkBuf {
            bytes: [0u8; MAX_PK_BYTES],
            len: len as u8,
        };
        k.bytes[..len].fill(0xFF);
        k
    }

    /// `len = slice.len()`, `bytes[..len]` copied from `slice`, tail
    /// zero. The row constructor: `MappedShard::get_pk_bytes(row)`
    /// returns exactly `pk_stride` bytes, and manifest `parse` passes
    /// its on-disk `len`/payload slice.
    pub fn from_bytes(slice: &[u8]) -> Self {
        debug_assert!(slice.len() <= MAX_PK_BYTES);
        let mut bytes = [0u8; MAX_PK_BYTES];
        bytes[..slice.len()].copy_from_slice(slice);
        PkBuf {
            bytes,
            len: slice.len() as u8,
        }
    }

    /// In-place [`Self::from_bytes`]: overwrite this key with `src`. Reuses the
    /// buffer instead of re-zeroing and re-copying the whole `MAX_PK_BYTES`
    /// array, and keeps the zero tail (`set_len` clears whatever a wider
    /// previous key left behind).
    #[inline]
    pub fn set_from(&mut self, src: &[u8]) {
        debug_assert!(src.len() <= MAX_PK_BYTES);
        self.bytes[..src.len()].copy_from_slice(src);
        self.set_len(src.len());
    }

    /// `&self.bytes[..len]` — the OPK bytes of this bound. After the
    /// OPK-at-rest flip all PK comparison and range logic operates on these
    /// raw order-preserving bytes (`compare_pk_bytes` / `pack_pk_be`), so this
    /// is the single PK accessor.
    #[inline]
    pub fn pk_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    /// The key zero-padded to `width` bytes — sound because the tail past
    /// `len` is always zero by construction. Used where a narrower key (e.g.
    /// an index leading-key span) must be widened to a full PK stride whose
    /// suffix is zero.
    #[inline]
    pub fn padded(&self, width: usize) -> &[u8] {
        debug_assert!(self.len as usize <= width && width <= MAX_PK_BYTES);
        &self.bytes[..width]
    }

    /// Owning form of [`Self::padded`]: the same key re-tagged as `width` bytes.
    /// The tail past `len` is already zero, so widening is a `len` bump with no
    /// copy — the alternative, `PkBuf::from_bytes(k.padded(width))`, re-zeroes
    /// and re-copies the whole `MAX_PK_BYTES` array to reach the same value.
    #[inline]
    pub fn widened(mut self, width: usize) -> Self {
        debug_assert!(self.len as usize <= width && width <= MAX_PK_BYTES);
        self.len = width as u8;
        self
    }

    /// Re-tag the meaningful span as `len` bytes, restoring the zero tail when
    /// this key is narrower than what the buffer previously held. The one
    /// writer of `len`, so "the tail past `len` is zero" — the invariant
    /// [`Self::padded`] and [`Self::widened`] rest on — holds by construction
    /// rather than by every caller remembering to re-zero.
    #[inline]
    pub(crate) fn set_len(&mut self, len: usize) {
        debug_assert!(len <= MAX_PK_BYTES);
        if (self.len as usize) > len {
            self.bytes[len..self.len as usize].fill(0);
        }
        self.len = len as u8;
    }
}

/// Precomputed read/encode plan for one index's OPK leading-key span: per
/// indexed column, the owner-side read coordinate (PK-or-payload, resolved via
/// `locate` — a PK source column is sliced from the packed OPK PK region, a
/// payload column read from its dense slot) and the promoted index column it
/// is encoded at. Built once per circuit so the row paths do no catalog
/// reborrow, schema indexing, or allocation; `Copy`, so descriptors carrying
/// it stay allocation-free.
///
/// The span is the single definition of "what key do this row's indexed
/// columns map to", shared by every uniqueness-enforcement site (in-batch
/// validator, backfill dedup via `batch_project_index`, broadcast-skip filter,
/// insert-time check, pre-flight) — byte-equal ⟺ index-value equal at any
/// width, and byte-lexicographic order is the seek/merge order.
#[derive(Clone, Copy)]
pub struct IndexKeySpec {
    n: u8,
    /// Span width in bytes — the sum of the promoted column widths, precomputed
    /// so the per-row `key_bytes` path does no re-summation.
    key_size: u8,
    locators: [ColumnLocator; gnitz_wire::PK_LIST_MAX_COLS],
    idx_cols: [SchemaColumn; gnitz_wire::PK_LIST_MAX_COLS],
}

impl IndexKeySpec {
    /// `cols` is the circuit's source column list (owner-schema indices);
    /// `idx_schema` supplies the promoted leading columns the span encodes at.
    pub fn new(cols: &[u32], owner: &SchemaDescriptor, idx_schema: &SchemaDescriptor) -> Self {
        debug_assert!(!cols.is_empty() && cols.len() <= gnitz_wire::PK_LIST_MAX_COLS);
        let mut locators = [ColumnLocator::Pk {
            byte_off: 0,
            size: 0,
            type_code: 0,
        }; gnitz_wire::PK_LIST_MAX_COLS];
        let mut idx_cols = [SchemaColumn::EMPTY; gnitz_wire::PK_LIST_MAX_COLS];
        for (i, &c) in cols.iter().enumerate() {
            locators[i] = owner.locate(c as usize);
            idx_cols[i] = idx_schema.columns[i];
            // Spec-invariant, so checked once per circuit rather than per column
            // per row: `write_span` hands each source's bytes to the OPK encoder
            // *at `idx_cols[i].type_code`*, so the index column must be exactly
            // the source's promotion. `index_key_type` errors on STRING/BLOB, so
            // this also subsumes "a German-string source needs a content hash,
            // not a raw cell encode" — its 16-byte struct (a heap offset for a
            // long string) would otherwise encode as if it were an integer. Every
            // production caller builds `idx_schema` through `make_index_schema`,
            // which derives it from this very function.
            debug_assert_eq!(
                gnitz_wire::index_key_type(locators[i].type_code()).ok(),
                Some(idx_cols[i].type_code),
                "IndexKeySpec: index column {i} is not the source column's promotion",
            );
        }
        IndexKeySpec {
            n: cols.len() as u8,
            key_size: idx_schema.leading_key_size(cols.len()) as u8,
            locators,
            idx_cols,
        }
    }

    /// Span width (`idx_key_size`); see `SchemaDescriptor::leading_key_size`.
    #[inline]
    pub fn key_size(&self) -> usize {
        self.key_size as usize
    }

    /// Write one row's OPK leading-key span into `dst[..key_size()]`. Returns
    /// `false` (skip — the row is not indexed, `dst` partially written) when ANY
    /// indexed column is NULL: SQL NULL-distinctness, a row with a NULL in any
    /// indexed column never collides. The per-column encode is byte-identical
    /// to the seek-side [`Self::seek_prefix`], so the in-memory key, the
    /// projected index entry, and the seek prefix agree by construction.
    ///
    /// Each column encodes through `encode_pk_column_promoted`, sign-extending a
    /// signed source from its native width before OPK-encoding at the promoted
    /// index column: the span is order-preserving for every type (a signed source
    /// promotes to a signed `I64`/`I128` index column whose sign-flip puts
    /// negatives below non-negatives), and equality-correct (equal logical values
    /// pack byte-identically regardless of source/target width). A column whose
    /// source already matches the index type (`U128`/`UUID`, base unsigned ≤8B)
    /// reduces to `encode_pk_column`.
    ///
    /// The encoding is the locator's own `encode_opk_promoted`, which is also
    /// what the sibling [`ReindexPacker::pack_into`] calls: the two must emit
    /// byte-identical keys for one logical value, and sharing the method is what
    /// makes that hold by construction rather than by two sites agreeing.
    pub(crate) fn write_span(&self, mb: &impl RowSource, row: usize, dst: &mut [u8]) -> bool {
        debug_assert!(dst.len() >= self.key_size(), "write_span: dst shorter than the span");
        let mut off = 0;
        let n = self.n as usize;
        for (loc, col) in self.locators[..n].iter().zip(&self.idx_cols[..n]) {
            // PK columns are never null, so this is the payload-only NULL gate.
            if loc.is_null(mb, row) {
                return false;
            }
            let target_w = col.size() as usize; // promoted index column width
            loc.encode_opk_promoted(mb, row, col.type_code, &mut dst[off..off + target_w]);
            off += target_w;
        }
        true
    }

    /// [`Self::write_span`] plus the source-PK OPK suffix: one row's full index
    /// entry key `[span ‖ src_pk]` in `dst[..key_size() + pk_stride]`. The single
    /// definition of "this row's index entry", shared by the write-side
    /// projection (`batch_project_index`) and the in-batch uniqueness validator,
    /// so the two agree byte-for-byte by construction. Returns `false` (row not
    /// indexed — NULL in an indexed column; `dst` partially written) exactly as
    /// `write_span` does. Full-arity specs only: a prefix spec would place the
    /// suffix over the uncovered columns' bytes.
    pub(crate) fn write_entry(&self, mb: &impl RowSource, row: usize, dst: &mut [u8]) -> bool {
        if !self.write_span(mb, row, dst) {
            return false;
        }
        let pk = mb.get_pk_bytes(row);
        dst[self.key_size()..self.key_size() + pk.len()].copy_from_slice(pk);
        true
    }

    /// Split a stored index entry back into `(span, source PK)` — the read-side
    /// inverse of [`Self::write_entry`], which put the PK at `key_size()`. The
    /// split is exact by layout (an index schema is the promoted indexed columns
    /// followed by the source PK columns, so its stride is `key_size() +
    /// src_pk_stride`), so nothing is decoded.
    pub fn split_entry<'a>(&self, entry: &'a [u8]) -> (&'a [u8], &'a [u8]) {
        debug_assert!(entry.len() > self.key_size(), "index entry shorter than its span");
        entry.split_at(self.key_size())
    }

    /// `write_span` into a caller-reused `PkBuf` — no intermediate stack
    /// buffer, no `from_bytes` re-copy (this runs in the backfill scan and on
    /// every insert). Maintains `PkBuf`'s "tail past `len` is zero" invariant
    /// (zeroing only when this key is narrower than the previous one in the
    /// reused scratch — free in the common same-circuit loop), so callers may
    /// slice `out.bytes[..stride]` as the span zero-padded to any wider stride.
    /// A NULL-skipped row returns `false` with `out` unchanged in meaning.
    pub fn key_bytes(&self, mb: &impl RowSource, row: usize, out: &mut PkBuf) -> bool {
        if !self.write_span(mb, row, &mut out.bytes) {
            return false;
        }
        out.set_len(self.key_size());
        true
    }

    /// Seek-side counterpart of [`Self::write_span`]: OPK-encode native key
    /// values (zero-extended, as `pk_native_key`/`payload_native_key` produce
    /// them) into the leading-key span, returned as a [`PkBuf`] of exactly
    /// the encoded span's width. Encodes through the shared
    /// [`encode_leading_opk`], the same per-column call `write_span` makes,
    /// so the seek prefix matches the projected entries by construction. Bytes
    /// past the span stay zero (the source-PK suffix is not part of it).
    ///
    /// A leading-prefix seek passes fewer values than the spec has columns and
    /// gets the corresponding prefix of the span — the spec's leading `k`
    /// columns are the same read/encode plan whether or not the trailing ones
    /// are supplied, so no sub-spec has to be derived to seek by a prefix.
    pub(crate) fn seek_prefix(&self, natives: &[u128]) -> PkBuf {
        let k = natives.len();
        debug_assert!(
            k >= 1 && k <= self.n as usize,
            "seek_prefix: one native value per leading spec column"
        );
        let cols = self.locators[..k]
            .iter()
            .zip(&self.idx_cols[..k])
            .map(|(loc, col)| (loc.type_code(), *col));
        encode_leading_opk(cols, natives)
    }
}

// ---------------------------------------------------------------------------
// Byte successor / predecessor and cut → key-range derivation
// ---------------------------------------------------------------------------

/// Fixed-width byte-string successor: `p + 1` with carry, in place. Returns
/// `false` when `p` is all-`0xFF` (or empty) — no successor exists at this width
/// (carry-out), which a caller reads as `+∞` (scan to the table end, or a
/// provably-empty start).
fn increment_key_in_place(p: &mut [u8]) -> bool {
    for b in p.iter_mut().rev() {
        *b = b.wrapping_add(1);
        if *b != 0 {
            return true;
        }
    }
    false
}

/// Fixed-width byte-string predecessor: `p - 1` with borrow, in place — the
/// mirror of [`increment_key_in_place`]. An all-zero `p` borrows out and wraps to
/// all-`0xFF`; the sole caller never passes one.
fn decrement_key_in_place(p: &mut [u8]) {
    for b in p.iter_mut().rev() {
        *b = b.wrapping_sub(1);
        if *b != 0xFF {
            return;
        }
    }
}

/// A cut in the OPK key space, named by an OPK group prefix: either that
/// group's own minimum key or the first key above every member of it. Zero-
/// padding the prefix to the key width is what makes it the group's minimum.
#[derive(Clone, Copy)]
pub(crate) struct KeyCut<'a> {
    group: &'a [u8],
    above: bool,
}

impl<'a> KeyCut<'a> {
    /// The group's own minimum key — below every member of it.
    pub(crate) fn min_of(group: &'a [u8]) -> Self {
        KeyCut { group, above: false }
    }

    /// The first key above every member of the group. A saturated group — and
    /// the zero-width group, which is the whole key space — has none.
    pub(crate) fn above(group: &'a [u8]) -> Self {
        KeyCut { group, above: true }
    }

    /// This cut as a `stride`-wide key; `None` when it lies above the whole key
    /// space. The successor's carry ripples into the equality prefix, landing
    /// exactly on the first key of the next equality group.
    fn key(&self, stride: usize) -> Option<PkBuf> {
        let mut k = PkBuf::from_bytes(self.group);
        let exists = !self.above || increment_key_in_place(&mut k.bytes[..self.group.len()]);
        exists.then(|| k.widened(stride))
    }
}

/// Half-open `[start, end)` OPK key range between two cuts over a `stride`-byte
/// key space.
///
/// `None` = provably empty: `start` lies above the whole key space, or
/// `start >= end`. `end == None` inside `Some` means the range runs to the table
/// end.
pub(crate) fn key_range_between_cuts(start: KeyCut, end: KeyCut, stride: usize) -> Option<(PkBuf, Option<PkBuf>)> {
    let start = start.key(stride)?;
    let end = end.key(stride);
    if end.as_ref().is_some_and(|e| start.pk_bytes() >= e.pk_bytes()) {
        return None;
    }
    Some((start, end))
}

/// Map `range`'s cut pair to its half-open `[start, end)` OPK key range via
/// [`key_range_between_cuts`]: `Before(v)` is [`KeyCut::min_of`] the group
/// `encode(v)` names, `After(v)` is [`KeyCut::above`] it. `encode` returns that
/// group as a `PkBuf` whose `len` is the prefix width.
///
/// SQL bound semantics (inclusivity, unboundedness, out-of-range saturation)
/// are resolved to cuts in the planner; none reach this layer.
pub(crate) fn range_keys_from_cuts(
    range: &RangeDescriptor,
    stride: usize,
    mut encode: impl FnMut(u128) -> PkBuf,
) -> Option<(PkBuf, Option<PkBuf>)> {
    fn cut(c: Cut, group: &PkBuf) -> KeyCut<'_> {
        match c {
            Cut::Before(_) => KeyCut::min_of(group.pk_bytes()),
            Cut::After(_) => KeyCut::above(group.pk_bytes()),
        }
    }
    let (s, e) = (encode(range.start.value()), encode(range.end.value()));
    key_range_between_cuts(cut(range.start, &s), cut(range.end, &e), stride)
}

/// [`range_keys_from_cuts`] for a range whose leading `range.eq_vals()` columns
/// are equality-pinned and whose next column is cut-bounded — the shape both the
/// base-PK and the secondary-index walks take. `encode_leading` OPK-encodes the
/// first `n_eq + 1` column values into their group prefix; `arity` is the key's
/// column count and `stride` its full byte width.
///
/// `Ok(None)` = provably empty. `Err` = the descriptor pins every column with no
/// range column left within `arity` — a trust-boundary rejection the `pub` seek
/// paths surface and a backfill bound merely degrades on. Guarding here also
/// keeps `prefix_len < stride` strict, so the pad always extends the group key.
/// `what` names the key space in that message.
pub(crate) fn eq_prefix_range_keys(
    range: &RangeDescriptor,
    arity: usize,
    stride: usize,
    what: &str,
    encode_leading: impl Fn(&[u128]) -> PkBuf,
) -> Result<Option<(PkBuf, Option<PkBuf>)>, String> {
    let eq_natives = range.eq_vals();
    let n_eq = eq_natives.len();
    // Written `n_eq >= arity`, never a `+ 1` that could overflow on an
    // adversarial length.
    if n_eq >= arity {
        return Err(format!("{what}: n_eq {n_eq} has no range column within arity {arity}"));
    }
    let mut natives = [0u128; gnitz_wire::PK_LIST_MAX_COLS];
    natives[..n_eq].copy_from_slice(eq_natives);
    Ok(range_keys_from_cuts(range, stride, |v| {
        natives[n_eq] = v;
        encode_leading(&natives[..=n_eq])
    }))
}

/// True when every key in a half-open `[start, end)` range from
/// [`range_keys_from_cuts`] shares its leading `prefix` bytes. Since OPK order IS
/// byte order, it is enough that the range's first and last keys agree there.
///
/// The last key is `end - 1`, undoing the `After` successor (and any carry ripple)
/// the cut derivation applied — `Some(end)` only ever comes back with
/// `start < end`, so the decrement cannot borrow out. `end == None` means the end
/// cut carried out and the range runs to the table end, whose last key is
/// all-`0xFF`.
pub(crate) fn range_shares_prefix(start: &PkBuf, end: Option<&PkBuf>, prefix: usize) -> bool {
    let last = match end {
        Some(e) => {
            let mut l = *e;
            decrement_key_in_place(&mut l.bytes[..l.len as usize]);
            l
        }
        None => PkBuf::max(start.len as usize),
    };
    start.pk_bytes()[..prefix] == last.pk_bytes()[..prefix]
}

// ---------------------------------------------------------------------------
// Row-content key material — the hashed key bytes, for the slots no scalar OPK
// encode can produce: a string column's content and the group key's fold slot.
// ---------------------------------------------------------------------------

/// Feed a German-string column's content into `hasher` as a length-prefixed
/// byte run: a 4-byte LE length, then the content (following the heap pointer
/// for long strings). The length prefix keeps "ab"+"c" from aliasing "a"+"bc"
/// across adjacent columns.
///
/// Shared by the group-key fold below and the set-op row-identity hash
/// (`reindex_hash_row`) so a string column contributes the same bytes to both.
/// The two *digests* still differ by construction and are meant to: the group
/// fold streams each column's canonical `route_key` under a `1`/`0` null marker,
/// the row hash streams raw native cell bytes under the inverted marker and a
/// leading branch discriminator. Only this per-column body is shared.
#[inline]
pub(crate) fn hash_german_string_content(hasher: &mut RowHasher, struct_bytes: &[u8], blob: &[u8]) {
    let content = gnitz_wire::german_string_content(struct_bytes, blob);
    hasher.update(&(content.len() as u32).to_le_bytes());
    hasher.update(content);
}

/// Hash one group column into the fold-path digest. The single per-column body
/// [`hash_fold`] folds with — a divergence would silently merge or split groups
/// (a wrong output PK, and a wrong AVI bucket).
///
/// Reads the null bit unconditionally, like the sibling `compare_by_group_cols`:
/// a NOT NULL column never carries one, so masking it off would cost a per-row
/// AND to change nothing.
#[inline]
fn hash_group_col<R: RowSource>(hasher: &mut RowHasher, src: &R, row: usize, null_word: u64, loc: ColumnLocator) {
    // A PK column is never null, so this is the payload-only NULL gate.
    if loc.is_null_word(null_word) {
        hasher.update(&[0u8]); // null marker
        return;
    }
    hasher.update(&[1u8]); // non-null marker
    match loc {
        // Length-prefixed content, matching reindex_hash_row. BLOB comes here too:
        // it shares the 16-byte struct, so hashing that would key on a heap pointer.
        ColumnLocator::Payload { slot, size, type_code } if gnitz_wire::is_german_string(type_code) => {
            hash_german_string_content(hasher, src.get_col_ptr(row, slot as usize, size as usize), src.blob());
        }
        // Canonical (sign-flipped/widened) value, so a payload FK hashes like the
        // same value stored as a PK column. U128/UUID included: their
        // `payload_route_key` arm is `u128::from_le_bytes(cell)`.
        _ => hasher.update(&loc.route_key(src, row).to_le_bytes()),
    }
}

/// The 128-bit XXH3 fold of `locs` over one row. The one body behind both folds
/// — `ops::group_key::GroupKeyCols::key_row`'s non-canonical branch and the packed
/// group key's overflow slot — so the two cannot drift apart.
#[inline]
pub(crate) fn hash_fold<R: RowSource>(locs: &[ColumnLocator], src: &R, row: usize, null_word: u64) -> u128 {
    let mut hasher = RowHasher::new();
    for &loc in locs {
        hash_group_col(&mut hasher, src, row, null_word, loc);
    }
    hasher.digest128()
}

// ---------------------------------------------------------------------------
// ReindexPacker — the synthetic-key composer
// ---------------------------------------------------------------------------

/// Synthetic-PK / routing key for a German-string column's content. Both the
/// reindex Map (setting a row's `_join_pk`) and the exchange scatter (routing the
/// raw delta) reach it through the packer's `String` arm, so a string join key
/// scatters to the worker that owns its own `_join_pk` partition. Empty content —
/// including a NULL string, a zeroed German-string struct — hashes to 0.
#[inline]
pub(crate) fn german_string_promote_key(struct_bytes: &[u8], blob: &[u8]) -> u128 {
    let content = gnitz_wire::german_string_content(struct_bytes, blob);
    if content.is_empty() {
        return 0; // NULL / empty-string sentinel
    }
    // A true 128-bit content hash. A 64-bit hash widened to 128 bits would carry
    // only 2^64 of entropy — a ~2^32-row birthday bound past which two distinct
    // strings collide to one `_join_pk` and the join's OPK byte-compare silently
    // equijoins them.
    xxh::checksum_128(content)
}

// ---------------------------------------------------------------------------
// Packed group key
// ---------------------------------------------------------------------------

/// The PK slot type one group column packs into.
///
/// A `≤8`-byte integer (and the signed 128-bit one) keeps its own width and sign,
/// so its slot is the plain OPK image of the value. A float packs to `U64` — the
/// `ieee_order_bits` image, which is order-preserving where the raw bits are not
/// (±0.0 differ, NaN has no canonical pattern) and which agrees with the
/// `total_cmp` order the group comparator uses. Everything else — `U128`/`UUID`
/// verbatim, `STRING`/`BLOB` by content hash — packs to a 16-byte `U128`.
const fn group_key_slot_type(tc: u8) -> u8 {
    if gnitz_wire::is_fixed_int(tc) || tc == type_code::I128 {
        tc
    } else if tc == type_code::F32 || tc == type_code::F64 {
        type_code::U64
    } else {
        type_code::U128
    }
}

/// The layout of a packed group key: which slots its PK region carries, and how
/// many of the group columns got a slot of their own.
///
/// Every grouped reduce has one, at any arity and over any column type — that
/// universality is the point. Columns past the budget do not make the key
/// unbuildable; they fold into the trailing hash slot.
struct GroupKeyLayout {
    /// A leading `U8` presence bitmap: bit *i* is set iff packed column *i* is
    /// NULL. Present iff some group column is nullable. One byte total, not one
    /// per column — without it a NULL group and a `0` group collide on one
    /// output PK.
    has_bitmap: bool,
    /// How many leading group columns carry their own slot. The rest fold.
    n_packed: usize,
    /// Whether a trailing 16-byte hash slot folds the group columns past
    /// `n_packed`. Equivalently `n_packed < group_cols.len()`.
    has_fold: bool,
    /// The PK slot type codes in order: the bitmap (if any), then one per packed
    /// column, then the fold slot (if any).
    slots: Vec<u8>,
}

impl GroupKeyLayout {
    /// Total PK stride of the packed key.
    fn stride(&self) -> usize {
        self.slots.iter().map(|&t| gnitz_wire::wire_stride(t)).sum()
    }
}

/// Resolve the packed group-key layout for `cols`, each given as its
/// `(type_code, nullable)`, inside the PK budget the caller's `reserve` for its
/// own trailing suffix columns leaves free.
///
/// Greedy: pack leading columns while the budget still leaves room for the fold
/// slot the remaining columns would need. Total — every group set gets a layout,
/// which is what lets the reduce drop its eligibility gate.
fn group_key_layout(cols: &[(u8, bool)], reserve_cols: usize, reserve_bytes: usize) -> GroupKeyLayout {
    let max_cols = MAX_PK_COLUMNS - reserve_cols;
    let max_bytes = MAX_PK_BYTES - reserve_bytes;
    // Both bounds are functions of the reservation alone, so they are checked
    // here rather than left to each caller to assert for itself.
    assert!(
        max_cols >= 2 && max_bytes >= 17,
        "group-key reservation must leave room for a bitmap byte and a 16-byte fold slot",
    );
    assert!(max_cols <= 9, "the one bitmap byte addresses at most 8 packed columns");
    let has_bitmap = cols.iter().any(|&(_, nullable)| nullable);
    let mut slots: Vec<u8> = Vec::with_capacity(cols.len() + 2);
    let mut bytes = 0usize;
    if has_bitmap {
        slots.push(type_code::U8);
        bytes += 1;
    }
    let mut n_packed = 0usize;
    for (i, &(tc, _)) in cols.iter().enumerate() {
        let slot = group_key_slot_type(tc);
        let w = gnitz_wire::wire_stride(slot);
        // Room this column needs, plus the fold slot the columns behind it would
        // still require. Reserving it here is what keeps the greedy walk from
        // packing a column it would have to give back.
        let tail_cols = usize::from(i + 1 < cols.len());
        let tail_bytes = tail_cols * 16;
        if slots.len() + 1 + tail_cols > max_cols || bytes + w + tail_bytes > max_bytes {
            break;
        }
        slots.push(slot);
        bytes += w;
        n_packed += 1;
    }
    let has_fold = n_packed < cols.len();
    if has_fold {
        slots.push(type_code::U128);
    }
    GroupKeyLayout {
        has_bitmap,
        n_packed,
        has_fold,
        slots,
    }
}

#[derive(Clone, Copy)]
enum PromoteKind {
    /// Any scalar source column, at either width and either sign: the locator
    /// says which region holds the bytes, and the encode differs only by that.
    ///
    /// A float here packs raw IEEE bits — equality-correct, not order-preserving;
    /// [`Self::Float`] is the group key's spelling. `reject_float_key` blocks a
    /// float from every join / set-op / GROUP BY key, so neither is reachable.
    Col(ColumnLocator),
    /// STRING/BLOB payload: sign-agnostic XXH3 content-hash key. The only source
    /// that is not a scalar cell the OPK encoders can consume.
    String(ColumnLocator),
    /// Group-key presence bitmap (one leading `U8` slot): bit *i* is set iff
    /// packed column *i* is NULL. Written by `pack_into` after the slot loop,
    /// from the NULL tests that loop already performs.
    Bitmap,
    /// Group-key float slot: the column's `order_bits` image, big-endian in a
    /// `U64` slot — order-preserving where the raw bits are not, matching the
    /// `total_cmp` order the group comparator uses. Carries the column's own
    /// [`ScalarKind`] so the per-row pack re-derives nothing.
    Float(ColumnLocator, ScalarKind),
    /// Group-key overflow fold (one trailing `U128` slot): the 128-bit hash of
    /// every group column past the packed prefix — [`ReindexPacker::folded`].
    Fold,
}

/// Per-column classifier for "read a source column, project it to OPK PK
/// bytes". Runs once per column at construction; the resulting `PromoteKind` is
/// stored on the `ColPromoter`, and the per-row work is the read + OPK encode
/// `ReindexPacker::pack_into` performs.
fn classify_promote(loc: ColumnLocator) -> PromoteKind {
    match loc {
        // BLOB shares the 16-byte German-string struct layout with STRING, so it
        // takes the same hash path rather than a raw cell encode. Neither can be
        // a PK column, so only the payload arm needs the test.
        ColumnLocator::Payload { type_code, .. } if gnitz_wire::is_german_string(type_code) => PromoteKind::String(loc),
        _ => PromoteKind::Col(loc),
    }
}

/// One key column of a `ReindexPacker`: the output PK column it packs into —
/// resolved once at construction, and its `size()` is the slot width, so the
/// packed bytes and [`ReindexPacker::output_schema`] read one value rather than
/// two — plus the `PromoteKind` that says where the source bytes come from.
#[derive(Clone, Copy)]
struct ColPromoter {
    out_col: SchemaColumn,
    /// Group-key mode: the **source** column is nullable, so a NULL zeroes the
    /// slot and the presence bitmap carries the NULL-ness instead. A stale or
    /// arbitrary cell under a NULL would otherwise split one group in two.
    /// Never set on a join key — those are NULL-gated upstream.
    nullable: bool,
    kind: PromoteKind,
}

impl ColPromoter {
    /// Unused slots of the fixed `cols` array, matching what `IndexKeySpec::new`
    /// fills its own with: the schema layer's designated padding column and a
    /// zeroed PK locator.
    const PLACEHOLDER: ColPromoter = ColPromoter {
        out_col: SchemaColumn::EMPTY,
        nullable: false,
        kind: PromoteKind::Col(ColumnLocator::Pk {
            byte_off: 0,
            size: 0,
            type_code: 0,
        }),
    };

    /// A slot packing into a `out_tc` output PK column. The one place the output
    /// column is spelled, because it is never nullable while `nullable` — the
    /// *source* column's — routinely is.
    fn new(out_tc: u8, nullable: bool, kind: PromoteKind) -> Self {
        ColPromoter {
            out_col: SchemaColumn::new(out_tc, 0),
            nullable,
            kind,
        }
    }
}

/// Packs a reindex column list into a contiguous OPK PK region. The same packer
/// drives both the reindex map (which writes the synthetic `_join_pk` at
/// emission — `expr::MapPlan`'s `PkSource::Pack`) and the exchange scatter
/// (which routes the raw delta by the same key), so the reindexed trace side and
/// the delta scatter side co-partition byte-for-byte at every key arity and
/// width.
pub(crate) struct ReindexPacker {
    cols: [ColPromoter; MAX_PK_COLUMNS], // first `num_cols` valid
    num_cols: usize,
    pub(crate) out_stride: usize,
    /// Group columns past the packed prefix, hashed into the trailing fold slot.
    /// Empty for a join key and for a group key with no fold.
    folded: Vec<ColumnLocator>,
}

impl ReindexPacker {
    /// Build per-column promoters from the reindex column list (key order),
    /// tightly packed with no inter-column padding. The **only** derivation of
    /// that layout: [`Self::output_schema`] reads these same promoters, so the
    /// two cannot disagree per slot while still agreeing on the total stride —
    /// which would silently stop equal keys co-partitioning.
    ///
    /// `None` on arity over `MAX_PK_COLUMNS`, an out-of-range column, or a stride
    /// over `MAX_PK_BYTES` — a forged circuit is rejected, never panicked on.
    pub(crate) fn new(schema: &SchemaDescriptor, reindex_cols: &[u32], target_tcs: &[u8]) -> Option<Self> {
        if reindex_cols.len() > MAX_PK_COLUMNS {
            return None;
        }
        let mut cols = [ColPromoter::PLACEHOLDER; MAX_PK_COLUMNS];
        let mut stride = 0usize;
        for (i, &c) in reindex_cols.iter().enumerate() {
            // `locate`'s own out-of-range guard is a release-active panic, so the
            // rejection has to happen here — bound to the call, not merely ahead
            // of it.
            let loc = ((c as usize) < schema.num_columns()).then(|| schema.locate(c as usize))?;
            let kind = classify_promote(loc);
            // Carried promotion target (`0` = self-derive); the slot type and width
            // follow `resolve_reindex_type` so the scatter packer and the trace-side
            // reindex Map derive identical widths.
            let carried = target_tcs.get(i).copied().unwrap_or(0);
            let cp = ColPromoter::new(gnitz_wire::resolve_reindex_type(loc.type_code(), carried), false, kind);
            // A payload slot right-aligns its source, so one narrower than the
            // source would truncate it. `resolve_reindex_type` never derives that;
            // debug-only because it is a type-system invariant, not input.
            debug_assert!(
                cp.out_col.size() as usize >= gnitz_wire::wire_stride(loc.type_code())
                    || !matches!(kind, PromoteKind::Col(ColumnLocator::Payload { .. }))
            );
            stride += cp.out_col.size() as usize;
            cols[i] = cp;
        }
        if stride > MAX_PK_BYTES {
            return None;
        }
        Some(ReindexPacker {
            cols,
            num_cols: reindex_cols.len(),
            out_stride: stride,
            folded: Vec::new(),
        })
    }

    /// The output PK columns this packer's bytes fill, in key order. The
    /// promoters are the only derivation of that layout, so a schema built from
    /// these describes what `pack_into` writes by construction —
    /// [`Self::output_schema`] and `AviBake::new` both take theirs from here.
    pub(crate) fn key_columns(&self) -> impl Iterator<Item = SchemaColumn> + '_ {
        self.cols[..self.num_cols].iter().map(|cp| cp.out_col)
    }

    /// The reindex Map's output schema: the packer's own [`Self::key_columns`]
    /// — so this schema's stride and `out_stride` are the same sum — then
    /// `in_schema.columns[payload_cols[i]]`. `payload_cols` is what the reindex
    /// program copies, so a join side skipping a dead column stops persisting it.
    ///
    /// `None` iff the result exceeds `MAX_COLUMNS`; the PK-side bounds are `new`'s.
    pub(crate) fn output_schema(&self, in_schema: &SchemaDescriptor, payload_cols: &[u32]) -> Option<SchemaDescriptor> {
        let mut b = DerivedSchema::new();
        for c in self.key_columns() {
            b.push_pk(c)?;
        }
        for &c in payload_cols {
            b.push(in_schema.columns[c as usize])?;
        }
        Some(b.finish())
    }

    /// Build the packer for a **group** key over `group_cols`, leaving the PK
    /// budget `reserve` needs for the suffix columns the caller appends behind
    /// the key. Every slot type comes from [`group_key_layout`], read here and
    /// nowhere else: every schema over this key goes through
    /// [`Self::key_columns`], so a group key's slots and its bytes cannot
    /// disagree.
    ///
    /// Unlike a join key this is total — columns past the budget fold into one
    /// trailing hash slot — which is what lets the reduce index every group set
    /// instead of rescanning the trace per epoch.
    pub(crate) fn new_group_key(schema: &SchemaDescriptor, group_cols: &[u32], reserve: &[SchemaColumn]) -> Self {
        let descs: Vec<(u8, bool)> = group_cols
            .iter()
            .map(|&c| {
                let col = &schema.columns[c as usize];
                (col.type_code, col.nullable != 0)
            })
            .collect();
        // The reservation is the suffix columns themselves, so their count and
        // their width are one fact rather than two that can drift.
        let layout = group_key_layout(&descs, reserve.len(), reserve.iter().map(|c| c.size() as usize).sum());
        let folded: Vec<ColumnLocator> = group_cols[layout.n_packed..]
            .iter()
            .map(|&c| schema.locate(c as usize))
            .collect();

        let mut cols = [ColPromoter::PLACEHOLDER; MAX_PK_COLUMNS];
        let mut slot = 0usize;
        if layout.has_bitmap {
            cols[slot] = ColPromoter::new(layout.slots[slot], false, PromoteKind::Bitmap);
            slot += 1;
        }
        for (i, &(tc, nullable)) in descs[..layout.n_packed].iter().enumerate() {
            let loc = schema.locate(group_cols[i] as usize);
            let kind = match ScalarKind::from_type_code(TypeCode::from_validated_u8(tc)) {
                Some(sk) if sk.is_float() => PromoteKind::Float(loc, sk),
                _ => classify_promote(loc),
            };
            cols[slot] = ColPromoter::new(layout.slots[slot], nullable, kind);
            slot += 1;
        }
        if layout.has_fold {
            cols[slot] = ColPromoter::new(layout.slots[slot], false, PromoteKind::Fold);
            slot += 1;
        }
        ReindexPacker {
            cols,
            num_cols: slot,
            out_stride: layout.stride(),
            folded,
        }
    }

    /// Pack the full reindex key (`out_stride` OPK bytes) for `row` into `dst`.
    ///
    /// One pass: the running slot offset, and — for a group key — the presence
    /// bitmap, whose bits are the NULL tests the packed slots already perform.
    #[inline]
    pub(crate) fn pack_into<R: RowSource>(&self, dst: &mut [u8], batch: &R, row: usize) {
        let null_word = batch.get_null_word(row);
        let mut off = 0usize;
        let mut null_bits = 0u8;
        for (i, cp) in self.cols[..self.num_cols].iter().enumerate() {
            let w = cp.out_col.size() as usize;
            let slot = &mut dst[off..off + w];
            off += w;
            match cp.kind {
                // A NULL packed column: zeroed slot, and its bit in the bitmap.
                // `nullable` implies the bitmap exists and is slot 0, so this
                // slot's packed index is `i - 1`.
                PromoteKind::Col(loc) | PromoteKind::String(loc) | PromoteKind::Float(loc, _)
                    if cp.nullable && loc.is_null_word(null_word) =>
                {
                    null_bits |= 1 << (i - 1);
                    slot.fill(0);
                }
                PromoteKind::Col(loc) => loc.encode_opk_promoted(batch, row, cp.out_col.type_code, slot),
                PromoteKind::String(loc) => {
                    let h = german_string_promote_key(loc.bytes(batch, row), batch.blob());
                    slot.copy_from_slice(&h.to_be_bytes());
                }
                PromoteKind::Bitmap => {}
                PromoteKind::Float(loc, sk) => {
                    slot.copy_from_slice(&loc.order_bits(batch, row, sk).to_be_bytes());
                }
                PromoteKind::Fold => {
                    slot.copy_from_slice(&hash_fold(&self.folded, batch, row, null_word).to_be_bytes());
                }
            }
        }
        // Unconditional per row, so every slot of `dst` is fully overwritten —
        // which is what lets a caller reuse one destination across rows with no
        // inter-row clear.
        if matches!(self.cols[0].kind, PromoteKind::Bitmap) {
            dst[0] = null_bits;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::test_support::pk_only_schema;
    use gnitz_wire::{read_signed_exact, read_unsigned_exact};

    /// Independent typed reference comparator over native-LE PK bytes. This is
    /// the per-column column-walk that `compare_pk_bytes` used *before* the OPK
    /// flip; kept here as the test oracle for "OPK byte order == typed order".
    fn typed_cmp_pk_le(schema: &SchemaDescriptor, a: &[u8], b: &[u8]) -> Ordering {
        let mut off = 0usize;
        for (_ord, col) in schema.pk_columns() {
            let cs = col.size() as usize;
            let ord = match col.type_code {
                type_code::U128 | type_code::UUID => {
                    let va = u128::from_le_bytes(a[off..off + 16].try_into().unwrap());
                    let vb = u128::from_le_bytes(b[off..off + 16].try_into().unwrap());
                    va.cmp(&vb)
                }
                type_code::I128 => {
                    let va = i128::from_le_bytes(a[off..off + 16].try_into().unwrap());
                    let vb = i128::from_le_bytes(b[off..off + 16].try_into().unwrap());
                    va.cmp(&vb)
                }
                type_code::U64 | type_code::U32 | type_code::U16 | type_code::U8 => {
                    read_unsigned_exact(&a[off..off + cs]).cmp(&read_unsigned_exact(&b[off..off + cs]))
                }
                _ => read_signed_exact(&a[off..off + cs]).cmp(&read_signed_exact(&b[off..off + cs])),
            };
            if ord != Ordering::Equal {
                return ord;
            }
            off += cs;
        }
        Ordering::Equal
    }

    /// Compare two native-LE PK tuples the way storage now does: encode each to
    /// OPK, then `compare_pk_bytes` (a raw memcmp). Mirrors the read path.
    fn cmp_pk_le(schema: &SchemaDescriptor, a: &[u8], b: &[u8]) -> Ordering {
        compare_pk_bytes(opk_key(schema, a).pk_bytes(), opk_key(schema, b).pk_bytes())
    }

    /// The load-bearing OPK property: a raw memcmp of the order-preserving keys
    /// equals the typed lexicographic comparison of the PK columns.
    fn assert_opk_equivalence(schema: &SchemaDescriptor, a: &[u8], b: &[u8]) {
        assert_eq!(
            cmp_pk_le(schema, a, b),
            typed_cmp_pk_le(schema, a, b),
            "OPK order disagrees with typed comparison for a={a:?} b={b:?}",
        );
    }

    /// Sweep one single-column PK schema pairwise over `vals`: the OPK memcmp
    /// order at rest must equal `T`'s native typed order — an oracle fully
    /// independent of the in-file `typed_cmp_pk_le` — and must also agree with
    /// `typed_cmp_pk_le` over the same bytes. The `i == j` diagonal is the
    /// equal-buffer case.
    fn assert_single_col_order<T: Ord + Copy + std::fmt::Debug>(tc: u8, vals: &[T], le: impl Fn(T) -> Vec<u8>) {
        let s = pk_only_schema(&[tc]);
        for &a in vals {
            for &b in vals {
                let (ab, bb) = (le(a), le(b));
                assert_eq!(cmp_pk_le(&s, &ab, &bb), a.cmp(&b), "type_code {tc}: {a:?} vs {b:?}");
                assert_opk_equivalence(&s, &ab, &bb);
            }
        }
    }

    /// Boundary sweeps for every PK-eligible scalar type. The value sets pin
    /// sign-extension (`-1 < 1`), zero-extension (`0xFFFE > 1`), LE-vs-lex byte
    /// order (`1 < 256`), and the 2^63 / 2^64 width boundaries that separate a
    /// U64 image from an I64 one.
    #[test]
    fn opk_order_matches_native_order_per_type() {
        assert_single_col_order(type_code::U8, &[0u8, 1, 0x7F, 0x80, 0xFF], |v| vec![v]);
        assert_single_col_order(type_code::I8, &[i8::MIN, -1, 0, 1, i8::MAX], |v| vec![v as u8]);
        let le16 = |v: u16| v.to_le_bytes().to_vec();
        assert_single_col_order(type_code::U16, &[0u16, 1, 0x0100, 0x8000, 0xFFFE, u16::MAX], le16);
        assert_single_col_order(type_code::I16, &[i16::MIN, -1, 0, 1, i16::MAX], |v: i16| {
            v.to_le_bytes().to_vec()
        });
        let le32 = |v: u32| v.to_le_bytes().to_vec();
        assert_single_col_order(
            type_code::U32,
            &[0u32, 1, 256, 0x8000_0000, 0xFFFF_FFFE, u32::MAX],
            le32,
        );
        assert_single_col_order(type_code::I32, &[i32::MIN, -1, 0, 1, i32::MAX], |v: i32| {
            v.to_le_bytes().to_vec()
        });
        assert_single_col_order(type_code::U64, &[0u64, 1, 2, 256, 1 << 63, u64::MAX], |v: u64| {
            v.to_le_bytes().to_vec()
        });
        assert_single_col_order(type_code::I64, &[i64::MIN, -1, 0, 1, i64::MAX], |v: i64| {
            v.to_le_bytes().to_vec()
        });
        let le128 = |v: u128| v.to_le_bytes().to_vec();
        let wide = [0u128, 1, u64::MAX as u128, u64::MAX as u128 + 1, 1 << 127, u128::MAX];
        assert_single_col_order(type_code::U128, &wide, le128);
        assert_single_col_order(type_code::UUID, &wide, le128);
        assert_single_col_order(
            type_code::I128,
            &[i128::MIN, -1, 0, 1, 1i128 << 63, 1i128 << 64, i128::MAX],
            |v: i128| v.to_le_bytes().to_vec(),
        );
    }

    #[test]
    fn compare_pk_bytes_compound_u64_u64() {
        let s = pk_only_schema(&[type_code::U64, type_code::U64]);
        let mk = |a: u64, b: u64| {
            let mut v = Vec::with_capacity(16);
            v.extend_from_slice(&a.to_le_bytes());
            v.extend_from_slice(&b.to_le_bytes());
            v
        };
        let r0 = mk(1, 5);
        let r1 = mk(1, 9);
        let r2 = mk(2, 1);
        // Same first column, second column tiebreaks ascending.
        assert_eq!(cmp_pk_le(&s, &r0, &r1), Ordering::Less);
        // First column dominates (would be Greater under a u128 LE compare,
        // which would treat the second column as the high-order bits).
        assert_eq!(cmp_pk_le(&s, &r1, &r2), Ordering::Less);
        assert_opk_equivalence(&s, &r0, &r1);
        assert_opk_equivalence(&s, &r1, &r2);
        assert_opk_equivalence(&s, &r0, &r2);
        // Equal compound buffers compare Equal at every column.
        assert_eq!(cmp_pk_le(&s, &r0, &r0), Ordering::Equal);
        assert_opk_equivalence(&s, &r0, &r0);
    }

    #[test]
    fn compare_pk_bytes_compound_mixed() {
        let s = pk_only_schema(&[type_code::U64, type_code::I32]);
        let mk = |a: u64, b: i32| {
            let mut v = Vec::with_capacity(12);
            v.extend_from_slice(&a.to_le_bytes());
            v.extend_from_slice(&b.to_le_bytes());
            v
        };
        let neg = mk(1, -5);
        let zero = mk(1, 0);
        // Per-column dispatch picks read_signed_exact for col 1 even though col 0
        // is unsigned: -5 < 0.
        assert_eq!(cmp_pk_le(&s, &neg, &zero), Ordering::Less);
        assert_opk_equivalence(&s, &neg, &zero);
    }

    #[test]
    fn compare_pk_bytes_pk_indices_order_not_schema_order() {
        // Schema [U64, U64] with pk_indices = [1, 0]: column 1 is the first
        // PK column. The byte layout follows pk_indices() order, so the
        // first 8 bytes correspond to column 1.
        let s = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[1, 0],
        );
        // (col1=1, col0=5) vs (col1=2, col0=0): col1 dominates.
        let mut a = Vec::with_capacity(16);
        a.extend_from_slice(&1u64.to_le_bytes()); // col1
        a.extend_from_slice(&5u64.to_le_bytes()); // col0
        let mut b = Vec::with_capacity(16);
        b.extend_from_slice(&2u64.to_le_bytes()); // col1
        b.extend_from_slice(&0u64.to_le_bytes()); // col0
        assert_eq!(cmp_pk_le(&s, &a, &b), Ordering::Less);
        // Encoder iterates pk-list order [1,0], same as the comparator.
        assert_opk_equivalence(&s, &a, &b);
    }

    /// Every specialized arm of `pack_pk_be` — the `{2,4,8,≥16}` register loads
    /// and the `9..=15` overlapping pair — must be byte-value-identical to the
    /// generic pad-and-copy at every width. A changed value would silently
    /// corrupt every `pack_pk_be` consumer (the cached sort keys, the route/guard
    /// keys, the bloom probes). Sweeps `1..=16` so no arm boundary is untested.
    #[test]
    fn pack_pk_be_specialization_matches_naive() {
        fn naive(pk: &[u8]) -> u128 {
            let take = pk.len().min(16);
            let mut buf = [0u8; 16];
            buf[..take].copy_from_slice(&pk[..take]);
            u128::from_be_bytes(buf)
        }
        for width in (1usize..=16).chain([24, 80]) {
            for seed in 0u32..256 {
                let bytes: Vec<u8> = (0..width)
                    .map(|i| seed.wrapping_mul(31).wrapping_add(i as u32) as u8)
                    .collect();
                assert_eq!(pack_pk_be(&bytes), naive(&bytes), "width {width} seed {seed}");
            }
        }
    }

    #[test]
    fn pk_bytes_eq_matches_byte_equality_direct() {
        // Wide (> 16): fully-equal → true; equal leading-16 prefix, differing
        // suffix → false (the tiebreak arm, otherwise only covered two layers up).
        let mut a = [0u8; 24];
        let mut b = [0u8; 24];
        for i in 0..24 {
            a[i] = i as u8;
            b[i] = i as u8;
        }
        assert!(pk_bytes_eq(&a, &b));
        b[16] ^= 1;
        assert!(!pk_bytes_eq(&a, &b));
        assert_eq!(pk_bytes_eq(&a, &b), a[..] == b[..]);
        // Narrow (≤ 16): the register arm, equal and unequal.
        let x = [1u8; 8];
        let mut y = [1u8; 8];
        assert!(pk_bytes_eq(&x, &y));
        y[7] = 2;
        assert!(!pk_bytes_eq(&x, &y));
    }

    // -----------------------------------------------------------------------
    // OPK ↔ compare_pk_bytes property test
    // -----------------------------------------------------------------------

    mod opk_proptest {
        use super::*;
        use crate::test_support::arb_pk_type;
        use proptest::prelude::*;

        /// `(column type codes, pk_indices permutation, a_bytes, b_bytes)`.
        /// The permutation exercises non-identity `pk_indices` (e.g. `[1, 0]`),
        /// and 1..=4 columns spans both narrow (≤16) and wide (>16) strides.
        fn arb_pk_case() -> impl Strategy<Value = (Vec<u8>, Vec<u32>, Vec<u8>, Vec<u8>)> {
            prop::collection::vec(arb_pk_type(), 1..=4).prop_flat_map(|types| {
                let stride: usize = types.iter().map(|&t| gnitz_wire::wire_stride(t)).sum();
                let n = types.len();
                (
                    Just(types),
                    Just((0..n as u32).collect::<Vec<u32>>()).prop_shuffle(),
                    prop::collection::vec(any::<u8>(), stride),
                    prop::collection::vec(any::<u8>(), stride),
                )
            })
        }

        proptest! {
            /// The order-preserving key agrees with `compare_pk_bytes` for every
            /// PK-eligible type, every 1..=4-column compound arrangement, and any
            /// `pk_indices` permutation — over random PK byte tuples.
            #[test]
            fn opk_matches_compare_pk_bytes((types, perm, a, b) in arb_pk_case()) {
                let cols: Vec<SchemaColumn> =
                    types.iter().map(|&tc| SchemaColumn::new(tc, 0)).collect();
                let s = SchemaDescriptor::new(&cols, &perm);
                assert_opk_equivalence(&s, &a, &b);
            }
        }

        proptest! {
            /// `compare_pk_ordering` agrees with the authoritative byte comparator
            /// at every PK width — narrow (register `pack_pk_be`) and wide
            /// (>16-byte prefix + `compare_pk_bytes` tiebreak) alike — so
            /// `== Ordering::Equal` is exactly byte equality, the property the
            /// N-way merge fold and the single-batch drain rely on for grouping.
            #[test]
            fn compare_pk_ordering_matches_byte_compare((types, perm, a, b) in arb_pk_case()) {
                let cols: Vec<SchemaColumn> =
                    types.iter().map(|&tc| SchemaColumn::new(tc, 0)).collect();
                let s = SchemaDescriptor::new(&cols, &perm);
                let (oa, ob) = (opk_key(&s, &a), opk_key(&s, &b));
                let (oa, ob) = (oa.pk_bytes(), ob.pk_bytes());
                prop_assert_eq!(compare_pk_ordering(oa, ob), compare_pk_bytes(oa, ob));
                prop_assert_eq!(
                    compare_pk_ordering(oa, ob) == std::cmp::Ordering::Equal,
                    oa == ob
                );
            }
        }

        proptest! {
            /// `pk_bytes_eq` is exactly byte-equality of the OPK regions at every
            /// PK width — the register-arm narrow case and the prefix-tiebreak wide
            /// case alike — the property every "same PK" merge/group fold relies on.
            #[test]
            fn pk_bytes_eq_matches_byte_equality((types, perm, a, b) in arb_pk_case()) {
                let cols: Vec<SchemaColumn> =
                    types.iter().map(|&tc| SchemaColumn::new(tc, 0)).collect();
                let s = SchemaDescriptor::new(&cols, &perm);
                let (oa, ob) = (opk_key(&s, &a), opk_key(&s, &b));
                let (oa, ob) = (oa.pk_bytes(), ob.pk_bytes());
                prop_assert_eq!(pk_bytes_eq(oa, ob), oa == ob);
                prop_assert!(pk_bytes_eq(oa, oa));
            }
        }
    }

    // ── seek_opk_bytes: the width-universal seek-key encoder ─────────────────

    #[test]
    fn seek_opk_bytes_narrow_matches_opk_key() {
        // For every narrow stride (≤ 16) the wire pair degenerates to `(low, &[])`,
        // so `seek_opk_bytes` must be byte-identical to a direct `opk_key` of the
        // native value — both buffer and stride.
        let cases = [
            pk_only_schema(&[type_code::U8]),  // stride 1
            pk_only_schema(&[type_code::U32]), // stride 4
            pk_only_schema(&[type_code::U64]), // stride 8
            pk_only_schema(&[type_code::I64]), // stride 8, signed → OPK flips the sign bit
            // Compound (U32, U32) with a *permuted* PK list [1, 0]: stride 8,
            // exercises the multi-column pk-list walk in the encoder.
            SchemaDescriptor::new(
                &[
                    SchemaColumn::new(type_code::U32, 0),
                    SchemaColumn::new(type_code::U32, 0),
                ],
                &[1, 0],
            ),
        ];
        // Values spanning zero, small, mixed, and a sign-bit-set word (negative
        // for the I64 case) so the sign-flip and byte order are exercised. Both
        // encoders truncate to `stride`, so an over-wide value is a valid probe.
        for s in cases {
            for v in [0u128, 1, 0x0123_4567_89AB_CDEF, 0x8000_0000_0000_0000, u64::MAX as u128] {
                let want = opk_key(&s, &v.to_le_bytes());
                let got = seek_opk_bytes(&s, v, &[]).expect("narrow seek encodes");
                assert_eq!(got, want, "narrow seek must match opk_key for {s:?} v={v:#x}");
            }
        }
    }

    #[test]
    fn seek_opk_bytes_wide_reproduces_hand_built_opk() {
        // (U64, U64, U64) = stride 24, wide. The wire pair carries the first 16
        // native bytes in `low` and the trailing U64 in `extra`, exactly as
        // `PkTuple::split_wire` packs them. All-unsigned ⇒ OPK is each column's
        // big-endian image, so the expected key is built by hand.
        let s = pk_only_schema(&[type_code::U64; 3]);
        assert_eq!(s.pk_stride(), 24);
        let (a, b, c): (u64, u64, u64) = (0x1122_3344_5566_7788, 0x99AA_BBCC_DDEE_FF00, 0x0102_0304_0506_0708);
        // Native LE image = [a_LE, b_LE, c_LE]; split_wire's `low` is the first 16
        // bytes (a in the low half, b in the high half), `extra` is c's 8 bytes.
        let low = (a as u128) | ((b as u128) << 64);
        let opk = seek_opk_bytes(&s, low, &c.to_le_bytes()).expect("wide seek encodes");
        assert_eq!(opk.len, 24);
        let want: Vec<u8> = a
            .to_be_bytes()
            .into_iter()
            .chain(b.to_be_bytes())
            .chain(c.to_be_bytes())
            .collect();
        assert_eq!(opk.pk_bytes(), want.as_slice());
    }

    #[test]
    fn seek_opk_bytes_missing_extra_errs_not_panics() {
        // A wide stride needs `stride - 16` extra bytes; too few must return Err,
        // never panic — the runtime guard the two dispatch sites rely on.
        let s = pk_only_schema(&[type_code::U64; 3]);
        assert!(seek_opk_bytes(&s, 0, &[]).is_err(), "stride 24 with no extra must Err");
        assert!(seek_opk_bytes(&s, 0, &[0u8; 7]).is_err(), "7 < 8 extra bytes must Err");
        assert!(
            seek_opk_bytes(&s, 0, &[0u8; 8]).is_ok(),
            "exactly 8 extra bytes is enough"
        );
    }

    #[test]
    fn seek_opk_bytes_four_u128_ceiling() {
        // The widest SQL-reachable PK is 4 columns (PK_LIST_MAX_COLS); 4×U128 =
        // stride 64 exercises the `le[16..16 + needed]` copy at its ceiling
        // (`needed == 48`). All-unsigned ⇒ OPK is each column's BE image.
        // Column 0 rides in `low`; columns 1..4 (48 bytes) in `extra`.
        let s = pk_only_schema(&[type_code::U128; 4]);
        assert_eq!(s.pk_stride(), 64);
        let vals: [u128; 4] = [
            0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10,
            0x1112_1314_1516_1718_191A_1B1C_1D1E_1F20,
            0x2122_2324_2526_2728_292A_2B2C_2D2E_2F30,
            0x3132_3334_3536_3738_393A_3B3C_3D3E_3F40,
        ];
        let extra: Vec<u8> = vals[1..].iter().flat_map(|v| v.to_le_bytes()).collect();
        let opk = seek_opk_bytes(&s, vals[0], &extra).expect("4×U128 encodes");
        assert_eq!(opk.len, 64);
        let want: Vec<u8> = vals.iter().flat_map(|v| v.to_be_bytes()).collect();
        assert_eq!(opk.pk_bytes(), want.as_slice());
    }

    #[test]
    fn pkbuf_eq_hash_compare_only_len_window() {
        use std::collections::HashSet;
        // Same meaningful bytes, different tail → equal and same hash.
        let mut a = PkBuf::from_bytes(&7u64.to_le_bytes());
        let b = PkBuf::from_bytes(&7u64.to_le_bytes());
        a.bytes[8] = 0xAB; // tail garbage past len; eq/hash must ignore it
        assert!(a == b);
        let mut set: HashSet<PkBuf> = HashSet::new();
        set.insert(b);
        assert!(set.contains(&a), "tail bytes must not affect membership");
    }

    #[test]
    fn pkbuf_borrow_heterogeneous_lookup() {
        use std::collections::HashSet;
        let mut set: HashSet<PkBuf> = HashSet::new();
        set.insert(PkBuf::from_bytes(&123u64.to_le_bytes()));
        // Raw &[u8] lookup via Borrow<[u8]> — no PkBuf construction.
        assert!(set.contains(&123u64.to_le_bytes()[..]));
        assert!(!set.contains(&124u64.to_le_bytes()[..]));
    }

    // --- increment_key_in_place -------------------------------------------

    #[test]
    fn succ_increments_low_byte() {
        let mut k = [0x00, 0x00, 0x05];
        assert!(increment_key_in_place(&mut k));
        assert_eq!(k, [0x00, 0x00, 0x06]);
    }

    #[test]
    fn succ_ripples_carry() {
        let mut k = [0x00, 0x00, 0xFF];
        assert!(increment_key_in_place(&mut k));
        assert_eq!(k, [0x00, 0x01, 0x00]);
    }

    #[test]
    fn succ_carries_out_on_all_ff() {
        let mut k = [0xFF, 0xFF];
        assert!(!increment_key_in_place(&mut k));
        assert_eq!(k, [0x00, 0x00]);
    }

    #[test]
    fn succ_empty_carries_out() {
        let mut k: [u8; 0] = [];
        assert!(!increment_key_in_place(&mut k));
    }

    // --- range_shares_prefix ----------------------------------------------

    fn buf(bytes: &[u8]) -> PkBuf {
        PkBuf::from_bytes(bytes)
    }

    /// The last key is `end - 1`, so a range ending exactly at the next group's
    /// first key still shares the prefix — one past that does not.
    #[test]
    fn shares_prefix_stops_at_the_group_boundary() {
        let start = buf(&[0x07, 0x00]);
        assert!(
            range_shares_prefix(&start, Some(&buf(&[0x07, 0x01])), 1),
            "one key wide"
        );
        assert!(
            range_shares_prefix(&start, Some(&buf(&[0x08, 0x00])), 1),
            "the whole group"
        );
        assert!(
            !range_shares_prefix(&start, Some(&buf(&[0x08, 0x01])), 1),
            "one key past"
        );
        // A borrow chain out of the trailing byte still lands in the group.
        assert!(range_shares_prefix(&buf(&[0x07, 0x05]), Some(&buf(&[0x08, 0x00])), 1));
    }

    /// `end == None` runs to the table end (all-`0xFF`), which only the topmost
    /// group shares a prefix with — that is what confines a maximal-value point.
    #[test]
    fn shares_prefix_handles_an_unbounded_end() {
        assert!(range_shares_prefix(&buf(&[0xFF, 0xFF]), None, 1));
        assert!(!range_shares_prefix(&buf(&[0x07, 0x00]), None, 1));
        // A zero-width distribution prefix is shared by everything.
        assert!(range_shares_prefix(&buf(&[0x07, 0x00]), None, 0));
    }

    #[test]
    fn pkbuf_wide_differs_past_byte_16() {
        // Two 24-byte keys identical in the first 16 bytes but differing in the
        // last 8 must be distinct — the failure mode of any u128-truncating key.
        let mut x = Vec::new();
        x.extend_from_slice(&1u64.to_le_bytes());
        x.extend_from_slice(&2u64.to_le_bytes());
        x.extend_from_slice(&3u64.to_le_bytes());
        let mut y = x.clone();
        y[16..24].copy_from_slice(&999u64.to_le_bytes());
        let px = PkBuf::from_bytes(&x);
        let py = PkBuf::from_bytes(&y);
        assert!(px != py);
        let mut set = std::collections::HashSet::new();
        set.insert(px);
        assert!(!set.contains(&py));
    }

    // =======================================================================
    // ReindexPacker — the synthetic-key composer
    // =======================================================================

    /// Write `packer`'s key for every row of `src` into `out`'s PK region — what
    /// `expr::MapPlan`'s `PkSource::Pack` arm does in production, spelled here so
    /// the tests below drive the packer through a stored PK region rather than a
    /// scratch buffer.
    fn promote_into<R: RowSource>(packer: &ReindexPacker, src: &R, out: &mut Batch) {
        assert_eq!(out.pk_stride() as usize, packer.out_stride);
        let (n, stride) = (out.count, packer.out_stride);
        let pk = out.pk_data_mut();
        for row in 0..n {
            packer.pack_into(&mut pk[row * stride..(row + 1) * stride], src, row);
        }
    }

    use crate::storage::Batch;
    use crate::test_support::{make_schema_pk_u64_payload_blob, make_schema_pk_u64_payload_string, opk_pk};

    /// Worker count the co-partition pins route against. Any count works — the
    /// property is that producer and consumer agree — but the wide-arm formula
    /// pin needs a fixed one to recompute against.
    const NW: usize = 4;

    // -----------------------------------------------------------------------
    // german_string_promote_key — the content-hash arm's own contract
    // -----------------------------------------------------------------------

    #[test]
    fn test_german_string_promote_key_short_and_long() {
        // Two rows: one short ("foo", inline) and one long string (> 12 bytes,
        // stored in blob). Both German-string layouts execute, and distinct
        // strings hash to distinct PKs.
        let schema = make_schema_pk_u64_payload_string();
        let mut b = Batch::with_capacity(schema, 2);

        // Row 0: short string "foo" (3 bytes, inline).
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        let gs0 = gnitz_wire::encode_german_string(b"foo", &mut b.blob);
        b.extend_col(0, &gs0);
        b.count += 1;

        // Row 1: long string (15 bytes > SHORT_STRING_THRESHOLD=12), heap-allocated.
        let long_str: &[u8] = b"hello-world-xyz";
        b.extend_pk(2u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        let gs1 = gnitz_wire::encode_german_string(long_str, &mut b.blob);
        b.extend_col(0, &gs1);
        b.count += 1;

        let mb = b.as_mem_batch();
        let pk_short = german_string_promote_key(mb.get_col_ptr(0, 0, 16), mb.blob);
        let pk_long = german_string_promote_key(mb.get_col_ptr(1, 0, 16), mb.blob);

        // The digest is deterministic but not pinned. What matters: non-empty
        // content hashes non-zero, distinct content hashes distinctly, and the
        // high half is populated (a real 128-bit hash, not a widened 64-bit one).
        assert_ne!(pk_short, 0);
        assert_ne!(pk_long, 0);
        assert_ne!(pk_short, pk_long);
        assert_ne!(
            pk_short >> 64,
            0,
            "short string PK must populate high half via xxh3_128"
        );
        assert_ne!(pk_long >> 64, 0, "long string PK must populate high half via xxh3_128");
    }

    #[test]
    fn test_german_string_promote_key_empty_is_zero() {
        // The hash early-returns 0 for length==0 — assert this is the contract,
        // not an accidental side-effect of xxh on empty input.
        let schema = make_schema_pk_u64_payload_string();
        let mut b = Batch::with_capacity(schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        // 16-byte German string struct, length=0.
        let gs = [0u8; 16];
        b.extend_col(0, &gs);
        b.count += 1;

        let mb = b.as_mem_batch();
        assert_eq!(german_string_promote_key(mb.get_col_ptr(0, 0, 16), mb.blob), 0);
    }

    // -----------------------------------------------------------------------
    // ReindexPacker::output_schema — the layout the per-row packer writes through
    // -----------------------------------------------------------------------

    #[test]
    fn packer_output_schema_pk_width_policy() {
        // (key column type, expected output PK type, expected pk_stride)
        let cases = [
            (type_code::U64, type_code::U64, 8u8),
            (type_code::I32, type_code::I32, 4),
            (type_code::U16, type_code::U16, 2),
            (type_code::STRING, type_code::U128, 16),
            (type_code::BLOB, type_code::U128, 16),
            (type_code::U128, type_code::U128, 16),
            (type_code::UUID, type_code::U128, 16),
            (type_code::F64, type_code::U128, 16),
        ];
        for (key_tc, want_tc, want_stride) in cases {
            // in_schema: [U64 PK, <key col>]; reindex on the payload col so the
            // PK-ineligible key types (STRING/BLOB/float) are exercisable as keys.
            let in_schema = SchemaDescriptor::new(
                &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(key_tc, 0)],
                &[0],
            );
            let node_schema = ReindexPacker::new(&in_schema, &[1], &[])
                .unwrap()
                .output_schema(&in_schema, &[0, 1])
                .unwrap();
            assert_eq!(node_schema.columns[0].type_code, want_tc, "key {key_tc} → PK type");
            assert_eq!(node_schema.pk_stride(), want_stride, "key {key_tc} → pk_stride");
        }
    }

    #[test]
    fn packer_output_schema_compound() {
        // in_schema: [U64 pk, I32, U128]; reindex on (col1 I32, col2 U128).
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::U128, 0),
            ],
            &[0],
        );
        let out = ReindexPacker::new(&in_schema, &[1, 2], &[])
            .unwrap()
            .output_schema(&in_schema, &[0, 1, 2])
            .unwrap();
        assert_eq!(out.pk_indices(), &[0, 1], "2-slot compound PK");
        assert_eq!(out.columns[0].type_code, type_code::I32, "slot0 keeps I32 native width");
        assert_eq!(out.columns[1].type_code, type_code::U128, "slot1 U128");
        assert_eq!(out.pk_stride(), 4 + 16, "compound stride = Σ slot widths");
        // Input columns follow the synthetic PK slots.
        assert_eq!(out.num_columns(), 2 + 3);
        assert_eq!(out.columns[2].type_code, type_code::U64);
        assert_eq!(out.columns[3].type_code, type_code::I32);
        assert_eq!(out.columns[4].type_code, type_code::U128);
    }

    #[test]
    fn packer_output_schema_cross_width_promotes() {
        // in_schema: [U64 pk, I32, I64]; reindex on (col1 I32, col2 I64) with
        // slot 0 promoted to I64 (carried) and slot 1 self-deriving.
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let out = ReindexPacker::new(&in_schema, &[1, 2], &[type_code::I64, 0])
            .unwrap()
            .output_schema(&in_schema, &[0, 1, 2])
            .unwrap();
        assert_eq!(out.columns[0].type_code, type_code::I64, "slot0 carried T = I64");
        assert_eq!(out.columns[1].type_code, type_code::I64, "slot1 self-derives I64");
        assert_eq!(out.pk_stride(), 8 + 8, "both slots 8 bytes after promotion");
    }

    #[test]
    fn packer_output_schema_payload_prune() {
        // in_schema: [U64 pk, I32, U128, I16]; reindex on col1; keep payload {0, 3}.
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I16, 0),
            ],
            &[0],
        );
        let out = ReindexPacker::new(&in_schema, &[1], &[])
            .unwrap()
            .output_schema(&in_schema, &[0, 3])
            .unwrap();
        assert_eq!(out.pk_indices(), &[0], "single synthetic PK slot");
        assert_eq!(out.columns[0].type_code, type_code::I32, "PK slot = reindex col1 (I32)");
        // Only the two kept payload columns follow — not all four input columns.
        assert_eq!(out.num_columns(), 1 + 2, "1 PK + 2 kept payload");
        assert_eq!(out.columns[1].type_code, type_code::U64, "kept payload col 0");
        assert_eq!(out.columns[2].type_code, type_code::I16, "kept payload col 3");
    }

    // -----------------------------------------------------------------------
    // ReindexPacker — multi-column / compound reindex packing
    // -----------------------------------------------------------------------

    #[test]
    fn test_reindex_packer_multi_column_bytes() {
        // Compound key spanning every slot shape: a non-leading PK column (offset
        // 8), a sign-flipped I32 payload, a 16-byte U128 payload, and an F64
        // whose 8 source bytes zero-pad into a 16-byte slot.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::F64, 0),
            ],
            &[0, 1],
        );
        let pk0: u64 = 0x0102_0304_0506_0708;
        let pk1: u64 = 0xA0B0_C0D0_E0F0_0102;
        let iv: i32 = -3;
        let uv: u128 = 0xdead_beef_cafe_1234_5678_9abc_def0_0001;
        let fv: f64 = 2.5;

        let mut b = Batch::with_capacity(schema, 1);
        b.extend_pk_opk(&schema, &[pk0 as u128, pk1 as u128]);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &iv.to_le_bytes()); // I32 payload (pi 0)
        b.extend_col(1, &uv.to_le_bytes()); // U128 payload (pi 1)
        b.extend_col(2, &fv.to_le_bytes()); // F64 payload (pi 2)
        b.count += 1;
        let mb = b.as_mem_batch();

        let packer = ReindexPacker::new(&schema, &[1, 2, 3, 4], &[]).unwrap();
        // out_stride = 8 (Pk U64) + 4 (I32) + 16 (U128) + 16 (F64→U128) = 44.
        assert_eq!(packer.out_stride, 8 + 4 + 16 + 16);

        let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
        packer.pack_into(&mut buf[..packer.out_stride], &mb, 0);

        // Expected: each column's OPK bytes concatenated at its offset.
        let mut want = Vec::new();
        want.extend_from_slice(&pk1.to_be_bytes()); // col1 Pk: BE(pk1) verbatim
        let mut i32_opk = [0u8; 4];
        gnitz_wire::encode_pk_column(&iv.to_le_bytes(), type_code::I32, &mut i32_opk);
        want.extend_from_slice(&i32_opk); // col2: sign-aware OPK
        assert_eq!(i32_opk[0], 0x7F, "I32 -3 OPK leading byte is sign-flipped (0x7F)");
        want.extend_from_slice(&uv.to_be_bytes()); // col3 Wide: BE(u128)
        let mut f64_slot = [0u8; 16];
        f64_slot[8..].copy_from_slice(&fv.to_bits().to_be_bytes()); // high 8 zero-pad, low 8 = BE(bits)
        want.extend_from_slice(&f64_slot); // col4: float, zero-padded
        assert_eq!(&buf[..packer.out_stride], &want[..], "packed compound key bytes");
        // Float slot high pad is zeroed.
        assert_eq!(&buf[28..36], &[0u8; 8], "F64 slot high pad zeroed");
    }

    #[test]
    fn test_reindex_packer_arity1_byte_identity() {
        // The content-hash arm: a STRING and a BLOB key both pack to the
        // big-endian image of `german_string_promote_key` over the column's
        // content. The integer and PK-placement arms are the proptest's; this is
        // the arm it cannot generate (`arb_pk_type` yields PK-eligible integers).
        for schema in [make_schema_pk_u64_payload_string(), make_schema_pk_u64_payload_blob()] {
            // Three rows with distinct content, one of them empty (the zero
            // sentinel), exercising the per-row read.
            let contents: [&[u8]; 3] = [b"abc", b"", b"hello-world-xyz"];
            let mut b = Batch::with_capacity(schema, 3);
            for (r, content) in contents.iter().enumerate() {
                b.extend_pk((r + 1) as u128 * 11);
                b.extend_weight(&1i64.to_le_bytes());
                b.extend_null_bmp(&0u64.to_le_bytes());
                let gs = gnitz_wire::encode_german_string(content, &mut b.blob);
                b.extend_col(0, &gs);
                b.count += 1;
            }
            let mb = b.as_mem_batch();

            let out_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U128, 0)], &[0]);
            let packer = ReindexPacker::new(&schema, &[1], &[]).unwrap();
            assert_eq!(packer.out_stride, 16, "a content-hash key is a 16-byte U128 slot");
            let mut out = Batch::zeroed(out_schema, 3);
            promote_into(&packer, &mb, &mut out);

            for row in 0..3 {
                let want = german_string_promote_key(mb.get_col_ptr(row, 0, 16), mb.blob);
                assert_eq!(
                    out.get_pk_bytes(row),
                    &want.to_be_bytes()[..],
                    "{} row {row}: packed key is BE(content hash)",
                    schema.columns[1].type_code,
                );
            }
            // The empty-content row is the zero sentinel, and the two non-empty
            // rows do not collide with it or with each other.
            assert_eq!(out.get_pk_bytes(1), &[0u8; 16], "empty content hashes to zero");
            assert_ne!(out.get_pk_bytes(0), out.get_pk_bytes(2));
            assert_ne!(out.get_pk_bytes(0), out.get_pk_bytes(1));
        }
    }

    #[test]
    fn test_reindex_packer_float_arity1_zero_pad() {
        // A float key self-derives to a 16-byte slot from an 8-byte source: the
        // slot is 8 zero bytes ++ BE(bits).
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::F64, 0),
            ],
            &[0],
        );
        let mut b = Batch::with_capacity(schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        let fv: f64 = -7.25;
        b.extend_col(0, &fv.to_le_bytes());
        b.count += 1;
        let mb = b.as_mem_batch();

        let out_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U128, 0)], &[0]);
        let packer = ReindexPacker::new(&schema, &[1], &[]).unwrap();
        assert_eq!(packer.out_stride, 16);
        let mut packer_out = Batch::zeroed(out_schema, 1);
        promote_into(&packer, &mb, &mut packer_out);

        let mut want = [0u8; 16];
        want[8..].copy_from_slice(&fv.to_bits().to_be_bytes());
        assert_eq!(
            packer_out.get_pk_bytes(0),
            &want[..],
            "float slot = zero-pad ++ BE(bits)"
        );
    }

    #[test]
    fn test_reindex_packer_copartition_contract() {
        // The bytes the exchange scatter computes (pack_into into a scratch
        // buffer) must be byte-identical to the `_join_pk` stored by promote_into,
        // so the delta scatter and the reindexed trace land on the same partition.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        );
        // Reindex on (col2 U64 payload, col1 I32 payload) — a 2-column non-PK key.
        let cols = [2u32, 1u32];
        let rows: &[(u64, i32, u64)] = &[
            (1, -5, 100),
            (2, 7, 100), // same col2 as row 0, different col1
            (3, -5, 200),
            (4, i32::MIN, 0),
        ];
        let mut b = Batch::with_capacity(schema, rows.len());
        for &(pk, c1, c2) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &c1.to_le_bytes()); // I32 payload (pi 0)
            b.extend_col(1, &c2.to_le_bytes()); // U64 payload (pi 1)
            b.count += 1;
        }
        let mb = b.as_mem_batch();

        let packer = ReindexPacker::new(&schema, &cols, &[]).unwrap();
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0), // col2 → U64
                SchemaColumn::new(type_code::I32, 0), // col1 → I32
            ],
            &[0, 1],
        );
        let mut out = Batch::zeroed(out_schema, rows.len());
        promote_into(&packer, &mb, &mut out);

        for row in 0..rows.len() {
            let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
            packer.pack_into(&mut buf[..packer.out_stride], &mb, row);
            // Trace side (stored _join_pk) == scatter side (scratch buffer).
            assert_eq!(out.get_pk_bytes(row), &buf[..packer.out_stride], "row {row} key bytes");
            assert_eq!(
                gnitz_wire::worker_for_pk_bytes(out.get_pk_bytes(row), NW),
                gnitz_wire::worker_for_pk_bytes(&buf[..packer.out_stride], NW),
                "row {row} co-partition",
            );
        }
        // Rows 0 and 1 share col2 but differ in col1 → distinct keys.
        assert_ne!(out.get_pk_bytes(0), out.get_pk_bytes(1));
    }

    #[test]
    fn test_reindex_packer_copartition_contract_wide() {
        // WIDE-branch (24-byte key) co-partition pin: three independent builders
        // must agree on the bytes — the scatter (`pack_into`), the trace store
        // (`promote_into`), and the ingest OPK encoder (`opk_pk`, which never
        // touches `ReindexPacker`) — and the formula pin below catches a fork in
        // the wide routing arm, which byte-equality alone cannot.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0), // PK (not part of the key)
                SchemaColumn::new(type_code::U64, 0), // c1 payload → key slot 0
                SchemaColumn::new(type_code::U64, 0), // c2 payload → key slot 1
                SchemaColumn::new(type_code::U64, 0), // c3 payload → key slot 2
            ],
            &[0],
        );
        // Non-trivial, high-entropy column values (so a forked hash seed/shift in
        // the wide arm lands on a different bucket with overwhelming probability).
        let key: [u64; 3] = [0x0102_0304_0506_0708, 0xA0B0_C0D0_E0F0_0102, 0xdead_beef_cafe_1234];
        let mut b = Batch::with_capacity(schema, 1);
        b.extend_pk(42u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &key[0].to_le_bytes()); // payload pi 0 (c1)
        b.extend_col(1, &key[1].to_le_bytes()); // payload pi 1 (c2)
        b.extend_col(2, &key[2].to_le_bytes()); // payload pi 2 (c3)
        b.count += 1;
        let mb = b.as_mem_batch();

        // Reindex on the three U64 payload columns → a 24-byte (3×U64) OPK key.
        let cols = [1u32, 2u32, 3u32];
        let packer = ReindexPacker::new(&schema, &cols, &[]).unwrap();
        assert_eq!(packer.out_stride, 24, "3×U64 reindex key must be 24 bytes (wide)");

        // The reindex output schema = natural 3×U64 PK (what the reindex map stamps and
        // what the trace store holds); identical layout to `wide_pk_3xu64_schema`
        // minus the trailing payload.
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0, 1, 2],
        );
        assert!(out_schema.pk_stride() > 16, "test invariant: 24-byte key is wide");

        // PATH 1 — trace store: promote_into stamps the `_join_pk`; read it back.
        let mut out = Batch::zeroed(out_schema, 1);
        promote_into(&packer, &mb, &mut out);
        let consumer = out.get_pk_bytes(0);

        // PATH 2 — exchange scatter: pack_into into a scratch buffer.
        let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
        packer.pack_into(&mut buf[..packer.out_stride], &mb, 0);
        let producer = &buf[..packer.out_stride];

        // PATH 3 — storage/ingest OPK encoder (no ReindexPacker involved at all).
        let oracle = opk_pk(&out_schema, &[key[0] as u128, key[1] as u128, key[2] as u128]);

        // (1) BYTE-EQUALITY teeth: all three independent builders agree, and the
        // key is genuinely wide (> 16 bytes).
        assert_eq!(consumer.len(), 24, "consumer key is the 24-byte wide region");
        assert!(consumer.len() > 16, "wide branch requires key len > 16");
        assert_eq!(producer, consumer, "scatter (pack_into) == trace store (_join_pk)");
        assert_eq!(consumer, oracle.as_slice(), "trace store == ingest OPK encoder");
        assert_eq!(producer, oracle.as_slice(), "scatter == ingest OPK encoder");

        // (2) CO-PARTITION teeth: producer and consumer route to the same worker
        // through the WIDE arm of worker_for_pk_bytes.
        let p_consumer = gnitz_wire::worker_for_pk_bytes(consumer, NW);
        let p_producer = gnitz_wire::worker_for_pk_bytes(producer, NW);
        let p_oracle = gnitz_wire::worker_for_pk_bytes(oracle.as_slice(), NW);
        assert_eq!(p_producer, p_consumer, "producer/consumer co-partition (wide)");
        assert_eq!(p_consumer, p_oracle, "trace store / ingest co-partition (wide)");

        // (3) WIDE-ARM FORMULA pin: the owner is the multiply-shift re-bucketing
        // of XXH3-64 over the OPK bytes, recomputed here. A forked seed or shift
        // would still keep producer == consumer — both call the same function —
        // so only an independent reference catches it.
        let expected = ((crate::foundation::xxh::checksum(consumer) as u128 * NW as u128) >> 64) as usize;
        assert_eq!(p_consumer, expected, "wide owner == ((xxh3_64(opk) * W) >> 64)");
        assert!(expected < NW, "the owner is a launched worker");
    }

    #[test]
    fn test_reindex_packer_null_key_determinism() {
        // A NULL value in a nullable (unsigned) reindex key column is canonically
        // zeroed at the source; the packer reads those zeros (ignoring the null
        // bitmap) and OPK-encodes them. Two distinct rows both NULL in the key
        // column must pack that slot identically (all-zero for an unsigned key).
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U32, 1), // nullable U32 key
            ],
            &[0],
        );
        let mut b = Batch::with_capacity(schema, 2);
        // Row 0 and row 1: distinct PK, both NULL in col1 (slot zeroed, null bit set).
        for pk in [10u128, 20u128] {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            // null bit for payload col index 0 (col1) set.
            b.extend_null_bmp(&1u64.to_le_bytes());
            b.extend_col(0, &0u32.to_le_bytes());
            b.count += 1;
        }
        let mb = b.as_mem_batch();

        let packer = ReindexPacker::new(&schema, &[1], &[]).unwrap();
        assert_eq!(packer.out_stride, 4); // U32 key → 4-byte slot

        let mut buf0 = [0u8; crate::schema::MAX_PK_BYTES];
        let mut buf1 = [0u8; crate::schema::MAX_PK_BYTES];
        packer.pack_into(&mut buf0[..packer.out_stride], &mb, 0);
        packer.pack_into(&mut buf1[..packer.out_stride], &mb, 1);

        assert_eq!(&buf0[..4], &[0u8; 4], "NULL unsigned key slot is all-zero");
        assert_eq!(&buf0[..4], &buf1[..4], "two NULL-key rows pack identically");
    }

    // -----------------------------------------------------------------------
    // ReindexPacker::new_group_key — the presence bitmap
    // -----------------------------------------------------------------------

    #[test]
    fn test_group_key_bitmap_bit_positions() {
        // Two packed group columns, only the second nullable: the bitmap must set
        // **bit 1**, not bit 0. Getting it wrong silently merges a NULL group with
        // a `0` group, and the end-to-end coverage groups on one nullable column,
        // where every wrong position still lands on bit 0.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0), // A: NOT NULL   → packed slot 0
                SchemaColumn::new(type_code::U32, 1), // B: nullable   → packed slot 1
            ],
            &[0],
        );
        // Row 0: B is NULL (payload slot 1 → null-word bit 1). Row 1: B == 0.
        let mut b = Batch::with_capacity(schema, 2);
        for (pk, null_word) in [(10u128, 1u64 << 1), (20u128, 0u64)] {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&null_word.to_le_bytes());
            b.extend_col(0, &7i64.to_le_bytes()); // A, same in both rows
            b.extend_col(1, &0u32.to_le_bytes()); // B: the canonical zero under a NULL
            b.count += 1;
        }
        let mb = b.as_mem_batch();

        let packer = ReindexPacker::new_group_key(&schema, &[1, 2], &[]);
        assert_eq!(packer.out_stride, 1 + 8 + 4, "bitmap ++ I64 slot ++ U32 slot");

        let mut null_row = [0u8; crate::schema::MAX_PK_BYTES];
        let mut zero_row = [0u8; crate::schema::MAX_PK_BYTES];
        packer.pack_into(&mut null_row[..packer.out_stride], &mb, 0);
        packer.pack_into(&mut zero_row[..packer.out_stride], &mb, 1);

        assert_eq!(null_row[0], 0b10, "NULL in packed column 1 sets bit 1, not bit 0");
        assert_eq!(zero_row[0], 0, "no NULL, no bits");
        // The NULL slot is zeroed and B == 0 encodes to zeros, so the bitmap byte
        // is the *only* thing separating a NULL group from a `0` group.
        assert_eq!(
            &null_row[1..packer.out_stride],
            &zero_row[1..packer.out_stride],
            "the two rows differ in nothing but the bitmap"
        );
        assert_ne!(
            &null_row[..packer.out_stride],
            &zero_row[..packer.out_stride],
            "a NULL group must not collide with a 0 group"
        );
    }

    // -----------------------------------------------------------------------
    // ReindexPacker::pack_into — property test over every PK-eligible type
    // -----------------------------------------------------------------------

    mod pack_proptest {
        use super::*;
        use crate::test_support::{arb_pk_type, pk_only_schema};
        use proptest::prelude::*;

        /// A legal carried promotion target: the widest slot of the source's own
        /// signedness. For the already-widest codes that is the self-derived type,
        /// so the two passes below are always legal, not always distinct.
        fn carried_target(tc: u8) -> u8 {
            match tc {
                type_code::U8 | type_code::U16 | type_code::U32 | type_code::U64 => type_code::U64,
                type_code::I8 | type_code::I16 | type_code::I32 | type_code::I64 => type_code::I64,
                type_code::I128 => type_code::I128,
                _ => type_code::U128, // U128, UUID
            }
        }

        /// `(column type codes, one native-LE value per column)`. 1..=MAX_PK_COLUMNS
        /// columns over every PK-eligible code, at every width combination — the
        /// surface the running-sum slot offsets live on.
        fn arb_key_case() -> impl Strategy<Value = (Vec<u8>, Vec<Vec<u8>>)> {
            prop::collection::vec(arb_pk_type(), 1..=crate::schema::MAX_PK_COLUMNS).prop_flat_map(|types| {
                let vals: Vec<_> = types
                    .iter()
                    .map(|&t| prop::collection::vec(any::<u8>(), gnitz_wire::wire_stride(t)))
                    .collect();
                (Just(types), vals)
            })
        }

        proptest! {
            /// At every arity and every PK-eligible type, self-derived and carried:
            /// each slot equals its own wire encoder's output at the offset the
            /// running sum put it, **and** the two placements agree with each
            /// other. The second does not follow from the first — PK and payload
            /// placement go through different primitives — and it is the
            /// co-partition contract this file exists for.
            #[test]
            fn reindex_pack_matches_wire_encoders((types, vals) in arb_key_case()) {
                let n = types.len();

                // Payload placement: [U64 PK, c0 .. cn-1], key = the payload columns.
                let mut cols = vec![SchemaColumn::new(type_code::U64, 0)];
                cols.extend(types.iter().map(|&tc| SchemaColumn::new(tc, 0)));
                let pay_schema = SchemaDescriptor::new(&cols, &[0]);
                let mut pb = Batch::with_capacity(pay_schema, 1);
                pb.extend_pk(1u128);
                pb.extend_weight(&1i64.to_le_bytes());
                pb.extend_null_bmp(&0u64.to_le_bytes());
                for (i, v) in vals.iter().enumerate() {
                    pb.extend_col(i, v);
                }
                pb.count += 1;
                let pay_mb = pb.as_mem_batch();

                // PK placement: the same columns, all of them PK columns, OPK at rest.
                let pk_schema = pk_only_schema(&types);
                let mut opk = Vec::new();
                for (i, v) in vals.iter().enumerate() {
                    let mut slot = vec![0u8; v.len()];
                    gnitz_wire::encode_pk_column(v, types[i], &mut slot);
                    opk.extend_from_slice(&slot);
                }
                let mut kb = Batch::with_capacity(pk_schema, 1);
                kb.extend_pk_bytes(&opk);
                kb.extend_weight(&1i64.to_le_bytes());
                kb.extend_null_bmp(&0u64.to_le_bytes());
                kb.count += 1;
                let pk_mb = kb.as_mem_batch();

                let pay_cols: Vec<u32> = (1..=n as u32).collect();
                let key_cols: Vec<u32> = (0..n as u32).collect();

                for carried in [false, true] {
                    let targets: Vec<u8> = types
                        .iter()
                        .map(|&tc| if carried { carried_target(tc) } else { 0 })
                        .collect();
                    let pay_packer = ReindexPacker::new(&pay_schema, &pay_cols, &targets).unwrap();
                    let pk_packer = ReindexPacker::new(&pk_schema, &key_cols, &targets).unwrap();
                    let stride = pay_packer.out_stride;
                    prop_assert_eq!(stride, pk_packer.out_stride);

                    let mut pay_buf = [0u8; crate::schema::MAX_PK_BYTES];
                    let mut pk_buf = [0u8; crate::schema::MAX_PK_BYTES];
                    pay_packer.pack_into(&mut pay_buf[..stride], &pay_mb, 0);
                    pk_packer.pack_into(&mut pk_buf[..stride], &pk_mb, 0);

                    // (1) Absolute.
                    let (mut off, mut src_off) = (0usize, 0usize);
                    for (i, &tc) in types.iter().enumerate() {
                        let out_tc = gnitz_wire::resolve_reindex_type(tc, targets[i]);
                        let w = gnitz_wire::wire_stride(out_tc);
                        let src_w = gnitz_wire::wire_stride(tc);

                        let mut want_pay = vec![0u8; w];
                        gnitz_wire::encode_pk_column_promoted(&vals[i], tc, out_tc, &mut want_pay);
                        prop_assert_eq!(&pay_buf[off..off + w], &want_pay[..], "payload slot {}", i);

                        let mut want_pk = vec![0u8; w];
                        gnitz_wire::promote_opk_column(&opk[src_off..src_off + src_w], tc, out_tc, &mut want_pk);
                        prop_assert_eq!(&pk_buf[off..off + w], &want_pk[..], "pk slot {}", i);

                        off += w;
                        src_off += src_w;
                    }
                    prop_assert_eq!(off, stride, "slot widths sum to out_stride");

                    // (2) Cross-placement.
                    prop_assert_eq!(&pay_buf[..stride], &pk_buf[..stride]);
                }
            }
        }
    }

    /// Release-only microbench for `pack_into` — once per equijoin delta row and
    /// per scatter row. Both packer shapes: a 3-column join key (offset sum, no
    /// null word) and a nullable 2-column group key (bitmap + null-word read).
    /// `cd crates && cargo test -p gnitz-engine --release reindex_pack_bench -- --ignored --nocapture --test-threads=1`
    #[test]
    #[ignore]
    fn reindex_pack_bench() {
        use std::hint::black_box;
        use std::time::Instant;

        const N: usize = 1_000_000;
        const ITERS: usize = 20;

        // --- 3-column join key: [U64 PK, U64, U64, U64], reindex on (1, 2, 3).
        let join_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        );
        let mut jb = Batch::with_capacity(join_schema, N);
        for i in 0..N as u64 {
            jb.extend_pk(i as u128);
            jb.extend_weight(&1i64.to_le_bytes());
            jb.extend_null_bmp(&0u64.to_le_bytes());
            jb.extend_col(0, &i.wrapping_mul(2_654_435_761).to_le_bytes());
            jb.extend_col(1, &i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
            jb.extend_col(2, &(!i).to_le_bytes());
            jb.count += 1;
        }
        let jmb = jb.as_mem_batch();
        let join_packer = ReindexPacker::new(&join_schema, &[1, 2, 3], &[]).unwrap();
        assert_eq!(join_packer.out_stride, 24);

        // --- Nullable 2-column group key: [U64 PK, I64, U32 NULL], group on (1, 2).
        let grp_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::U32, 1),
            ],
            &[0],
        );
        let mut gb = Batch::with_capacity(grp_schema, N);
        for i in 0..N as u64 {
            gb.extend_pk(i as u128);
            gb.extend_weight(&1i64.to_le_bytes());
            // Every 8th row is NULL in the nullable group column.
            gb.extend_null_bmp(&(u64::from(i % 8 == 0) << 1).to_le_bytes());
            gb.extend_col(0, &(i as i64).wrapping_mul(-7).to_le_bytes());
            gb.extend_col(1, &(i as u32).to_le_bytes());
            gb.count += 1;
        }
        let gmb = gb.as_mem_batch();
        let grp_packer = ReindexPacker::new_group_key(&grp_schema, &[1, 2], &[]);

        for (name, packer, mb) in [("join3", &join_packer, &jmb), ("group2-nullable", &grp_packer, &gmb)] {
            let stride = packer.out_stride;
            let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
            // Warm up.
            packer.pack_into(&mut buf[..stride], mb, 0);

            let t = Instant::now();
            let mut acc = 0u64;
            for _ in 0..ITERS {
                for row in 0..N {
                    packer.pack_into(&mut buf[..stride], mb, row);
                    acc = acc.wrapping_add(black_box(buf[0]) as u64);
                }
            }
            let secs = t.elapsed().as_secs_f64();
            println!(
                "reindex_pack_bench[{name}]: {:.1} Mrows/s ({N} rows x {ITERS} iters in {secs:.3}s, stride {stride}, checksum {acc})",
                (N * ITERS) as f64 / secs / 1e6,
            );
        }
    }
}
