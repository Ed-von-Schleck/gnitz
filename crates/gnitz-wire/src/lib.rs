//! Shared wire-protocol definitions for GnitzDB.
//!
//! Single source of truth for constants and codecs that both the client
//! (gnitz-core) and server (gnitz-server) must agree on.
//!
//! The crate is organized into topic modules, and most items are re-exported
//! flat at the crate root (`gnitz_wire::FOO`) so callers need not track which
//! module a symbol lives in. `control`, `schema_block`, `sys_rows`, `txn_frame`
//! and `wal` stay named modules and are referenced by path
//! (`gnitz_wire::wal::encode`). Only `wal::encode` and `schema_block::encode`
//! actually collide at the root; the other three are namespaced for consistency
//! with them rather than by necessity. `wal`'s *constants* (`WAL_*`,
//! `MAX_WIRE_REGIONS`, the `REG_*` region-convention indices) are flat-exported,
//! since they are referenced pervasively.

#[cfg(not(target_endian = "little"))]
compile_error!("GnitzDB requires a little-endian target; the wire format is LE-only.");

/// Declare a **wire enum**: a closed set of values crossing the wire, with the
/// total encode and partial decode every such set needs.
///
/// `ALL`, `as_wire` and `from_wire` are all generated from the one variant list,
/// so a decode table cannot disagree with the discriminants it mirrors and a new
/// variant is covered without a second edit. Hand-written `from_wire` tables
/// needed a round-trip test to catch that drift; here it cannot happen.
///
/// `TypeCode` is deliberately not declared through this macro: it carries extra
/// per-variant data (`wire_name`) and its 15-arm `try_from_u8` is a jump table
/// on a hot path, which a linear `ALL` scan would replace with a walk.
macro_rules! wire_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident: $repr:ident {
            $($(#[$vmeta:meta])* $variant:ident = $value:expr),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        #[repr($repr)]
        $vis enum $name {
            $($(#[$vmeta])* $variant = $value),+
        }

        impl $name {
            /// Every variant, in declaration order.
            pub const ALL: &'static [$name] = &[$($name::$variant),+];

            /// This variant's wire value.
            #[inline]
            pub const fn as_wire(self) -> $repr {
                self as $repr
            }

            /// The variant `v` names, or `None` for a value outside the set.
            ///
            /// Resolved against the discriminants themselves rather than a
            /// second table of arms, which is what makes a decode/declaration
            /// disagreement unrepresentable. These sets are a handful of
            /// variants wide; a scan is not a table lookup worth writing twice.
            #[inline]
            pub const fn from_wire(v: $repr) -> Option<Self> {
                let mut i = 0;
                while i < Self::ALL.len() {
                    if Self::ALL[i] as $repr == v {
                        return Some(Self::ALL[i]);
                    }
                    i += 1;
                }
                None
            }
        }
    };
}

mod catalog;
mod circuit;
mod codec;
mod error;
mod expr;
mod flags;
mod german_string;
mod handshake;
mod pk;
mod range;
mod read_spec;
mod rel_descriptor;
mod types;
mod uuid;

pub mod control;
pub mod schema_block;
pub mod sys_rows;
pub mod txn_frame;
pub mod wal;

pub use catalog::*;
pub use circuit::*;
pub use error::*;
pub use expr::*;
pub use flags::*;
pub use german_string::*;
pub use handshake::*;
pub use pk::*;
pub use range::*;
pub use read_spec::*;
pub use rel_descriptor::*;
pub use types::*;
pub use uuid::*;
// Flat-export `wal`'s constants (referenced everywhere) but not its framer
// functions (`encode`/`block_size`/… stay `gnitz_wire::wal::`-qualified).
pub use wal::{
    MAX_WIRE_REGIONS, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT, WAL_FORMAT_VERSION, WAL_HEADER_SIZE,
    WAL_OFF_CHECKSUM, WAL_OFF_COUNT, WAL_OFF_NUM_REGIONS, WAL_OFF_SIZE, WAL_OFF_TID, WAL_OFF_VERSION,
};

// ---------------------------------------------------------------------------
// Low-level byte primitives.
//
// Two calling conventions over two rules: the `*_le(buf, off)` offset form for
// region walks (no slice construction at the call site), and
// `read_{signed,unsigned}_exact(cell)` for typed cell reads, where the slice IS
// the column. A caller holding a tail sub-slices it itself — at `opt-level=0`
// the `bytes[..size]` bound is an out-of-line `Range::index` call, so the
// comparators over already-exact windows must not pay it.
//
// **Inlining policy for this crate's per-row primitives:** they carry
// `#[inline(always)]`, not `#[inline]`. The E2E suite runs the debug binary, and
// at `opt-level=0` LLVM runs only the always-inline pass — a plain hint leaves a
// real call frame around a body that is often a single load or shift. Individual
// items below do not restate this.
// ---------------------------------------------------------------------------

/// Align `n` up to an 8-byte boundary.
pub const fn align8(n: usize) -> usize {
    (n + 7) & !7
}

/// XXH3-64 over `b` — the WAL body checksum. The one hash both ends compute;
/// the engine's `foundation::xxh::checksum` re-exports this.
#[inline]
pub fn checksum(b: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(b)
}

/// `V₀` — the group key of the ungrouped (global) aggregate: the XXH3-128
/// digest over no group columns at all. The single definition both ends share,
/// so the engine's emitted ground row and the client's synthesized one carry the
/// same key with no literal embedded on either side.
#[inline]
pub fn global_group_key() -> u128 {
    xxhash_rust::xxh3::xxh3_128(b"")
}

#[inline]
pub(crate) fn read_u16_le(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(buf[off..off + 2].try_into().unwrap())
}

#[inline]
pub fn read_u32_le(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

#[inline(always)]
pub fn read_u64_le(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

#[inline]
pub fn read_i64_le(buf: &[u8], off: usize) -> i64 {
    i64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

/// Padding-free little-endian scalars a region may be reinterpreted as. A sealed
/// bound rather than `T: Copy`, which also admits padded types whose padding
/// bytes are never initialized — reading those as `u8` is UB, and a safe fn must
/// not carry an unenforced precondition.
pub trait LeScalar: Copy {}
macro_rules! le_scalar {
    ($($t:ty),*) => { $(impl LeScalar for $t {})* };
}
le_scalar!(u8, i8, u16, i16, u32, i32, u64, i64, u128, i128);

/// Reinterpret a `&[T]` of LE scalars as the region bytes it already is. On the
/// little-endian target the native layout *is* the wire/shard layout, so this is
/// the zero-copy way to hand a typed column (weights, null words, `u128` cells)
/// to the byte-oriented region APIs.
/// `#[inline(always)]`: `gnitz-wire` has no dev opt-level override, and the
/// per-morsel callers in gnitz-expr are themselves `#[inline(always)]` for
/// opt-0 consumers, where only the always-inline pass runs.
#[inline(always)]
pub fn as_le_bytes<T: LeScalar>(v: &[T]) -> &[u8] {
    // SAFETY: `size_of_val(v)` initialized bytes borrowed from `v`, consumed as
    // opaque bytes and never as typed values.
    unsafe { std::slice::from_raw_parts(v.as_ptr().cast::<u8>(), std::mem::size_of_val(v)) }
}

/// Read a whole 1/2/4/8-byte little-endian **signed** cell, sign-extended to
/// i64 — the native-LE payload/decoded-PK read. `bytes.len()` IS the column
/// width. Sibling of [`read_unsigned_exact`]; the two are the one pair every
/// fixed-int value read goes through.
#[inline(always)]
pub fn read_signed_exact(bytes: &[u8]) -> i64 {
    match bytes.len() {
        1 => bytes[0] as i8 as i64,
        2 => i16::from_le_bytes(bytes.try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(bytes.try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes.try_into().unwrap()),
        // Static message on purpose: this body is `#[inline(always)]` and lands
        // at every per-row read site, and a formatted one duplicates its
        // `Arguments` block into each.
        _ => unreachable!("read_signed_exact: unexpected column width"),
    }
}

/// Read a whole 1/2/4/8-byte little-endian **unsigned** cell, zero-extended to
/// u64. See [`read_signed_exact`].
#[inline(always)]
pub fn read_unsigned_exact(bytes: &[u8]) -> u64 {
    match bytes.len() {
        1 => bytes[0] as u64,
        2 => u16::from_le_bytes(bytes.try_into().unwrap()) as u64,
        4 => u32::from_le_bytes(bytes.try_into().unwrap()) as u64,
        8 => u64::from_le_bytes(bytes.try_into().unwrap()),
        _ => unreachable!("read_unsigned_exact: unexpected column width"),
    }
}

/// True iff payload null-bit `pi` is set in `word` (the column is NULL).
///
/// The null word packs one bit per *payload* column: bit `pi` is the `pi`-th
/// non-PK column in schema order (the region convention, whose `REG_NULL_BMP`
/// index this crate already owns). These two accessors are the **one** read/write
/// convention for the bitmap, shared by the client (`gnitz-core`), the evaluator
/// (`gnitz-expr`) and the engine — it was previously spelled out in each.
#[inline(always)]
pub fn null_word_get(word: u64, pi: usize) -> bool {
    (word >> pi) & 1 == 1
}

/// Set (`is_null == true`) or clear (`is_null == false`) payload null-bit `pi`
/// in `word`. See [`null_word_get`].
#[inline(always)]
pub fn null_word_set(word: &mut u64, pi: usize, is_null: bool) {
    if is_null {
        *word |= 1u64 << pi;
    } else {
        *word &= !(1u64 << pi);
    }
}

/// Null word with the low `npc` payload bits set — "all `npc` payload columns
/// are null". `npc` reaches the row-major cap of 64 only when a schema has
/// exactly 64 payload columns; `1u64 << 64` would panic in debug builds, so
/// that boundary returns all-ones directly.
#[inline]
pub fn all_payload_null_mask(npc: usize) -> u64 {
    if npc < 64 {
        (1u64 << npc) - 1
    } else {
        u64::MAX
    }
}

/// Concatenate two rows' null words for an output row laid out as
/// `[left payload..., right payload...]`. The right bits shift up by `left_npc`.
/// `left_npc` reaches 64 only when the right side has no payload columns, in
/// which case `right` is 0 and the dropped shift is a no-op.
#[inline]
pub fn merge_null_words(left: u64, right: u64, left_npc: usize) -> u64 {
    if left_npc < 64 {
        left | (right << left_npc)
    } else {
        left
    }
}

// `write_u32_le` has only in-crate callers (the WAL framer and the control-block
// encoder), so it stays crate-internal rather than widening the public surface
// with a dead export.
#[inline]
pub(crate) fn write_u32_le(buf: &mut [u8], off: usize, val: u32) {
    buf[off..off + 4].copy_from_slice(&val.to_le_bytes());
}

#[inline(always)]
pub fn write_u64_le(buf: &mut [u8], off: usize, val: u64) {
    buf[off..off + 8].copy_from_slice(&val.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `checksum` must agree byte-for-byte with the C/Python `XXH3_64bits` the
    /// other end of the wire runs — the interop contract this crate defines.
    #[test]
    fn checksum_matches_c_xxh3_64bits() {
        let body_hex = "9800000008000000a000000008000000a800000008000000b000000008000000b800000008000000c000000008000000c800000008000000d000000008000000d800000008000000e000000008000000e800000008000000f00000001000000000010000000000000000000000000000000000000000000001000000000000008000000000000000000000000000000001000000000000000300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
        let body: Vec<u8> = (0..body_hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&body_hex[i..i + 2], 16).unwrap())
            .collect();
        assert_eq!(body.len(), 208);
        let computed = checksum(&body);
        assert_eq!(
            computed, 0x741C9E0BA1D8A9FD_u64,
            "xxhash-rust and C XXH3_64bits disagree: got 0x{computed:016X}"
        );
    }

    /// The null-bitmap convention, pinned where it is now defined: setting and
    /// clearing bit `pi` must leave every other payload slot untouched.
    #[test]
    fn null_word_get_set_roundtrip() {
        let mut w = 0u64;
        assert!(!null_word_get(w, 3));
        null_word_set(&mut w, 3, true);
        assert!(null_word_get(w, 3));
        assert_eq!(w, 0b1000);
        // Clearing leaves the other bits untouched.
        null_word_set(&mut w, 5, true);
        null_word_set(&mut w, 3, false);
        assert!(!null_word_get(w, 3));
        assert!(null_word_get(w, 5));
        assert_eq!(w, 0b100000);
    }

    /// `npc == 64` is the row-major cap, where the naive `(1 << npc) - 1` would
    /// shift by the word width.
    #[test]
    fn all_payload_null_mask_covers_the_full_word() {
        assert_eq!(all_payload_null_mask(0), 0);
        assert_eq!(all_payload_null_mask(1), 0b1);
        assert_eq!(all_payload_null_mask(63), u64::MAX >> 1);
        assert_eq!(all_payload_null_mask(64), u64::MAX);
    }

    #[test]
    fn read_unsigned_zero_extends() {
        // size 1: high-bit-set vs small — must match u8.cmp.
        assert_eq!(read_unsigned_exact(&[0xFF]), 0xFF);
        assert_eq!(read_unsigned_exact(&[0x01]), 0x01);
        assert!(read_unsigned_exact(&[0xFF]) > read_unsigned_exact(&[0x01]));

        // size 2: 0xFFFE > 0x0001 as unsigned (sign-extension would invert).
        assert_eq!(read_unsigned_exact(&0xFFFEu16.to_le_bytes()), 0xFFFE);
        assert_eq!(read_unsigned_exact(&0x0001u16.to_le_bytes()), 0x0001);
        assert!(read_unsigned_exact(&0xFFFEu16.to_le_bytes()) > read_unsigned_exact(&0x0001u16.to_le_bytes()),);

        // size 4.
        let big: u32 = 0xFFFF_FFFE;
        let small: u32 = 0x0000_0001;
        assert_eq!(read_unsigned_exact(&big.to_le_bytes()), big as u64);
        assert!(read_unsigned_exact(&big.to_le_bytes()) > read_unsigned_exact(&small.to_le_bytes()),);

        // size 8: full u64 round-trip.
        let v: u64 = 0xDEAD_BEEF_CAFE_BABE;
        assert_eq!(read_unsigned_exact(&v.to_le_bytes()), v);
    }
}
