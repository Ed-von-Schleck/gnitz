//! Order images: the byte strings whose plain lexicographic order *is* a
//! column's typed order, the keys of the indexes that walk a column in order.

use std::cmp::Ordering;

use crate::schema::{ColumnLocator, SchemaColumn, TypeCode};
use gnitz_expr::RowSource;
use gnitz_wire::{cmp_col_window, ScalarKind};

/// The column types outside [`ScalarKind`] that MIN/MAX still select over: the
/// 16-byte integers and the German-string pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WideKind {
    Fixed(TypeCode),
    Bytes,
}

/// How a column's value is held and ordered: as its [`ScalarKind`] register
/// image, or as the native bytes of a [`WideKind`] value too wide for one.
/// Every type has exactly one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImageKind {
    Scalar(ScalarKind),
    Wide(WideKind),
}

impl ImageKind {
    pub(crate) const fn of(tc: TypeCode) -> Self {
        match ScalarKind::from_type_code(tc) {
            Some(kind) => Self::Scalar(kind),
            None if tc.is_german_string() => Self::Wide(WideKind::Bytes),
            None => Self::Wide(WideKind::Fixed(tc)),
        }
    }
}

impl WideKind {
    /// Order two values by their **native** bytes — a string's content, already
    /// resolved out of its blob heap, not its 16-byte cell.
    #[inline(always)]
    pub(crate) fn cmp_native(self, a: &[u8], b: &[u8]) -> Ordering {
        match self {
            Self::Fixed(tc) => cmp_col_window(a, &[], b, &[], tc),
            Self::Bytes => a.cmp(b),
        }
    }
}

// ---------------------------------------------------------------------------
// Scalar order image: `order_bits` encodes a value, `order_inverse` undoes it.
// ---------------------------------------------------------------------------

/// The column's value in `row` as a `u64` whose unsigned order is the column's
/// typed order (`total_cmp`'s for floats). `kind` is the column's own.
#[inline(always)]
pub(crate) fn order_bits(loc: &ColumnLocator, src: &impl RowSource, row: usize, kind: ScalarKind) -> u64 {
    debug_assert_eq!(ScalarKind::from_type_code(loc.type_code()), Some(kind));
    match kind {
        ScalarKind::Int(fi) => (loc.decode_i64(src, row, fi) as u64) ^ ((fi.is_signed() as u64) << 63),
        // Floats are never PK columns, so these bytes are native.
        ScalarKind::F32 => ieee_order_bits_f32(u32::from_le_bytes(loc.bytes(src, row).try_into().unwrap())),
        ScalarKind::F64 => ieee_order_bits(u64::from_le_bytes(loc.bytes(src, row).try_into().unwrap())),
    }
}

/// Inverse of [`order_bits`]: the value's own little-endian bits.
#[inline(always)]
pub(crate) fn order_inverse(kind: ScalarKind, e: u64) -> u64 {
    match kind {
        ScalarKind::Int(fi) => e ^ ((fi.is_signed() as u64) << 63),
        ScalarKind::F32 => ieee_order_bits_f32_reverse(e) as u64,
        ScalarKind::F64 => ieee_order_bits_reverse(e),
    }
}

/// IEEE 754 order-preserving encoding of an `f64`'s raw bits: negatives invert
/// wholly, non-negatives flip the sign bit, so plain unsigned order over the
/// result is `total_cmp` order.
#[inline(always)]
pub(crate) fn ieee_order_bits(raw_bits: u64) -> u64 {
    if raw_bits >> 63 != 0 {
        !raw_bits
    } else {
        raw_bits ^ (1u64 << 63)
    }
}

/// [`ieee_order_bits`] for an `f32`.
#[inline(always)]
pub(crate) fn ieee_order_bits_f32(raw_bits: u32) -> u64 {
    (if raw_bits >> 31 != 0 {
        !raw_bits
    } else {
        raw_bits ^ (1u32 << 31)
    }) as u64
}

/// Reverse of [`ieee_order_bits`].
#[inline(always)]
fn ieee_order_bits_reverse(encoded: u64) -> u64 {
    if encoded >> 63 != 0 {
        encoded ^ (1u64 << 63)
    } else {
        !encoded
    }
}

/// Reverse of [`ieee_order_bits_f32`].
#[inline(always)]
fn ieee_order_bits_f32_reverse(encoded: u64) -> u32 {
    let e = encoded as u32;
    if e >> 31 != 0 {
        e ^ (1u32 << 31)
    } else {
        !e
    }
}

/// The index PK column [`write_image_slot`] fills with an image's leading bytes:
/// a scalar image whole, a wide one's first 16 bytes.
pub(crate) const fn image_slot_col(wide: bool) -> SchemaColumn {
    SchemaColumn::new(if wide { TypeCode::U128 } else { TypeCode::U64 }, false)
}

/// A whole image as a payload column: a BLOB orders by content.
pub(crate) const IMAGE_COL: SchemaColumn = SchemaColumn::new(TypeCode::Blob, false);

/// The native bytes of the wide column at `loc` in `row`: a string's content,
/// or the 16-byte little-endian integer.
#[inline(always)]
pub(crate) fn wide_native<'a>(
    loc: &ColumnLocator,
    kind: WideKind,
    mb: &'a impl RowSource,
    row: usize,
    scratch: &'a mut [u8; 16],
) -> &'a [u8] {
    match kind {
        WideKind::Bytes => gnitz_wire::german_string_content(loc.bytes(mb, row), mb.blob()),
        WideKind::Fixed(_) => loc.native_le_bytes(mb, row, scratch),
    }
}

/// Append the order image of byte string `content` to `out`: `0x00` escaped to
/// `0x00 0xFF`, then a `0x00 0x00` terminator, so no image is a prefix of another.
fn append_bytes_image(invert: bool, content: &[u8], out: &mut Vec<u8>) {
    let start = out.len();
    for &b in content {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0, 0]);
    if invert {
        out[start..].iter_mut().for_each(|b| *b = !*b);
    }
}

/// The order image of a 16-byte integer column at `loc` in `row`, **known
/// non-NULL**: its OPK bytes, complemented when `invert`.
#[inline(always)]
pub(crate) fn int16_image(loc: &ColumnLocator, invert: bool, src: &impl RowSource, row: usize) -> [u8; 16] {
    let image = loc.opk_image(src, row);
    (if invert { !image } else { image }).to_be_bytes()
}

/// Whether `kind`'s image has a fixed width: every kind's but a byte string's.
pub(crate) fn has_fixed_image(kind: ImageKind) -> bool {
    kind != ImageKind::Wide(WideKind::Bytes)
}

/// Write `image` into an index key slot, zero-padding or truncating to its width.
#[inline]
pub(crate) fn write_image_slot(slot: &mut [u8], image: &[u8]) {
    let take = image.len().min(slot.len());
    slot[..take].copy_from_slice(&image[..take]);
    slot[take..].fill(0);
}

/// [`append_bytes_image`]'s and [`int16_image`]'s inverse: the native bytes back
/// out of an index image.
pub(crate) fn wide_native_of_image(kind: WideKind, invert: bool, image: &[u8]) -> Vec<u8> {
    let mut v = image.to_vec();
    if invert {
        v.iter_mut().for_each(|b| *b = !*b);
    }
    match kind {
        WideKind::Fixed(tc) => {
            let (n, mut native) = (v.len(), [0u8; 16]);
            gnitz_wire::decode_pk_column(&v, tc, &mut native[..n]);
            v.copy_from_slice(&native[..n]);
            v
        }
        WideKind::Bytes => {
            debug_assert!(v.ends_with(&[0, 0]), "a byte-string image ends in its terminator");
            let n = v.len() - 2;
            let (mut r, mut w) = (0, 0);
            while r < n {
                let b = v[r];
                v[w] = b;
                w += 1;
                r += if b == 0 { 2 } else { 1 };
            }
            v.truncate(w);
            v
        }
    }
}

/// The order image of a scalar column at `loc` in `row`, **known non-NULL**:
/// [`order_bits`], complemented when `invert`.
#[inline]
pub(crate) fn scalar_image(
    loc: &ColumnLocator,
    kind: ScalarKind,
    invert: bool,
    src: &impl RowSource,
    row: usize,
) -> u64 {
    let v = order_bits(loc, src, row, kind);
    if invert {
        !v
    } else {
        v
    }
}

/// [`scalar_image`]'s inverse: the value's own little-endian bits back out of
/// an image.
#[inline(always)]
pub(crate) fn scalar_native_of_image(kind: ScalarKind, invert: bool, image: u64) -> u64 {
    order_inverse(kind, if invert { !image } else { image })
}

/// Append the order image of the column at `loc` in `row`, **known non-NULL**,
/// complemented when `invert`.
#[inline]
pub(crate) fn append_image(
    loc: &ColumnLocator,
    kind: ImageKind,
    invert: bool,
    src: &impl RowSource,
    row: usize,
    out: &mut Vec<u8>,
) {
    match kind {
        ImageKind::Scalar(kind) => out.extend_from_slice(&scalar_image(loc, kind, invert, src, row).to_be_bytes()),
        ImageKind::Wide(WideKind::Fixed(_)) => out.extend_from_slice(&int16_image(loc, invert, src, row)),
        ImageKind::Wide(WideKind::Bytes) => append_bytes_image(
            invert,
            gnitz_wire::german_string_content(loc.bytes(src, row), src.blob()),
            out,
        ),
    }
}

#[cfg(test)]
#[path = "tests/order_image.rs"]
mod tests;
