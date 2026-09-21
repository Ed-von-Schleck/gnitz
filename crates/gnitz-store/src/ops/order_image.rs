//! Order images: the byte strings whose plain lexicographic order *is* a
//! column's typed order, the keys of the indexes that walk a column in order.

use crate::schema::{type_code, ColumnLocator, SchemaColumn};
use gnitz_expr::RowSource;
use gnitz_wire::{ImageKind, ScalarKind, WideKind};

/// The index PK column [`write_image_slot`] fills with an image's leading bytes:
/// a scalar image whole, a wide one's first 16 bytes.
pub(crate) const fn image_slot_col(wide: bool) -> SchemaColumn {
    SchemaColumn::new(if wide { type_code::U128 } else { type_code::U64 }, 0)
}

/// A whole image as a payload column: a BLOB orders by content.
pub(crate) const IMAGE_COL: SchemaColumn = SchemaColumn::new(type_code::BLOB, 0);

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
            gnitz_wire::decode_pk_column(&v, tc as u8, &mut native[..n]);
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
/// [`ColumnLocator::order_bits`], complemented when `invert`.
#[inline]
pub(crate) fn scalar_image(
    loc: &ColumnLocator,
    kind: ScalarKind,
    invert: bool,
    src: &impl RowSource,
    row: usize,
) -> u64 {
    let v = loc.order_bits(src, row, kind);
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
    kind.order_inverse(if invert { !image } else { image })
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
