//! Order images: the byte strings whose plain lexicographic order *is* a
//! column's typed order, shared by the reduce's aggregate-value index (one
//! extreme per group) and the top-N index (every row of a group in ORDER BY
//! order). One encoder, so the two indexes cannot disagree on what "smaller"
//! means for any type.

use crate::schema::ColumnLocator;
use gnitz_expr::RowSource;
use gnitz_wire::{ScalarKind, WideKind};

/// How an ordered column's value is held and ordered: as its 8-byte order
/// image, or as the native bytes of a value too wide for one.
#[derive(Clone, Copy)]
pub(crate) enum ImageKind {
    Scalar(ScalarKind),
    Wide(WideKind),
}

impl ImageKind {
    /// Every column type has an image: the scalar one where a register holds
    /// it, the wide one otherwise. `None` only for a type that is neither.
    pub(crate) fn of(tc: crate::schema::TypeCode) -> Option<Self> {
        match ScalarKind::from_type_code(tc) {
            Some(kind) => Some(ImageKind::Scalar(kind)),
            None => WideKind::from_type_code(tc).map(ImageKind::Wide),
        }
    }
}

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

/// Append the order image of one wide value to `out`: the OPK bytes of a
/// 16-byte integer, or a **prefix-free** byte string (`0x00` escaped to
/// `0x00 0xFF`, `0x00 0x00` appended). Prefix-freeness is what lets `invert`
/// reverse the order exactly and a truncated `leading_u64` window still order.
pub(crate) fn append_wide_image(kind: WideKind, invert: bool, native: &[u8], out: &mut Vec<u8>) {
    let start = out.len();
    match kind {
        WideKind::Fixed(tc) => {
            out.resize(start + 16, 0);
            gnitz_wire::encode_pk_column(native, tc as u8, &mut out[start..]);
        }
        WideKind::Bytes => {
            for &b in native {
                out.push(b);
                if b == 0 {
                    out.push(0xFF);
                }
            }
            out.extend_from_slice(&[0, 0]);
        }
    }
    if invert {
        out[start..].iter_mut().for_each(|b| *b = !*b);
    }
}

/// [`append_wide_image`]'s inverse: the native bytes back out of an index image.
/// Only the wide arm needs one — a scalar image is a `u64` the reader decodes in
/// place.
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

/// The order image of a scalar column at `loc` in `row`, **known non-NULL**: the
/// `u64` whose unsigned order is the column's typed order, complemented when
/// `invert` so an ascending walk yields the column's *largest* value first. Its
/// big-endian bytes are what an index stores.
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

/// Append the order image of the column at `loc` in `row`, **known non-NULL**:
/// the scalar image big-endian, or the wide image. `invert` complements it, so
/// an ascending byte walk yields the column's *largest* value first.
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
        ImageKind::Wide(kind) => {
            let mut scratch = [0u8; 16];
            append_wide_image(kind, invert, wide_native(loc, kind, src, row, &mut scratch), out);
        }
    }
}

#[cfg(test)]
#[path = "tests/order_image.rs"]
mod tests;
