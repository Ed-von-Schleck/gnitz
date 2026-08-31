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
pub fn opk_key(schema: &SchemaDescriptor, native_le: &[u8]) -> PkBuf {
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
    pub fn write_span(&self, mb: &impl RowSource, row: usize, dst: &mut [u8]) -> bool {
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
    pub fn write_entry(&self, mb: &impl RowSource, row: usize, dst: &mut [u8]) -> bool {
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
    pub fn seek_prefix(&self, natives: &[u128]) -> PkBuf {
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
    pub(crate) fn new(out_tc: u8, nullable: bool, kind: PromoteKind) -> Self {
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
pub struct ReindexPacker {
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
    pub fn new(schema: &SchemaDescriptor, reindex_cols: &[u32], target_tcs: &[u8]) -> Option<Self> {
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
    pub fn output_schema(&self, in_schema: &SchemaDescriptor, payload_cols: &[u32]) -> Option<SchemaDescriptor> {
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
#[path = "tests/key.rs"]
mod tests;
