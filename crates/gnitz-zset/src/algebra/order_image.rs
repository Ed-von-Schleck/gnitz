//! Order images: the byte strings whose plain lexicographic order *is* a
//! column's typed order — the keys of the indexes that walk a column in order,
//! and the form a scalar MIN/MAX holds its running extreme in.

use std::borrow::Cow;
use std::cmp::Ordering;

use crate::repr::MemBatch;
use crate::schema::SchemaColumn;
use gnitz_expr::{order_bits, ColumnLocator};
use gnitz_wire::{cmp_col_window, for_each_fixed_int, int_cell, ScalarKind};
use gnitz_wire::{RowSource, TypeCode};

/// The column types outside [`ScalarKind`]: the 16-byte integers and the
/// German-string pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WideKind {
    Fixed(TypeCode),
    Bytes,
}

/// How a column's value is held and ordered: as its [`ScalarKind`] register
/// image, or as the native bytes of a [`WideKind`] value too wide for one.
/// Every type has exactly one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ImageKind {
    Scalar(ScalarKind),
    Wide(WideKind),
}

impl ImageKind {
    pub(super) const fn of(tc: TypeCode) -> Self {
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
    pub(super) fn cmp_native(self, a: &[u8], b: &[u8]) -> Ordering {
        match self {
            Self::Fixed(tc) => cmp_col_window(a, b, tc),
            Self::Bytes => a.cmp(b),
        }
    }
}

/// One column as an ordered index keys it: where it lies, how its image is
/// encoded, and whether the image is complemented, which reverses its order.
#[derive(Clone, Copy)]
pub(crate) struct ImageCol {
    pub(crate) loc: ColumnLocator,
    kind: ImageKind,
    invert: bool,
}

impl ImageCol {
    pub(crate) fn new(loc: ColumnLocator, invert: bool) -> Self {
        ImageCol {
            loc,
            kind: ImageKind::of(loc.type_code()),
            invert,
        }
    }

    /// Whether the image outgrows a scalar's 8-byte slot.
    pub(crate) fn is_wide(&self) -> bool {
        matches!(self.kind, ImageKind::Wide(_))
    }

    /// Whether a key slot holds the image whole: every kind's but a byte string's.
    pub(crate) fn fits_slot(&self) -> bool {
        self.kind != ImageKind::Wide(WideKind::Bytes)
    }

    /// Append `row`'s image, **known non-NULL**.
    #[inline]
    pub(crate) fn append(&self, src: &impl RowSource, row: usize, out: &mut Vec<u8>) {
        let ImageCol { loc, kind, invert } = self;
        match *kind {
            ImageKind::Scalar(kind) => out.extend_from_slice(&scalar_image(loc, kind, *invert, src, row).to_be_bytes()),
            ImageKind::Wide(WideKind::Fixed(_)) => out.extend_from_slice(&int16_image(loc, *invert, src, row)),
            ImageKind::Wide(WideKind::Bytes) => append_bytes_image(*invert, &loc.content(src, row), out),
        }
    }

    /// Write the leading bytes of `row`'s image, **known non-NULL**, into the key
    /// slot `slot`, zero-padded or cut to its width. A byte string's whole image
    /// is appended to `whole`.
    #[inline(always)]
    pub(crate) fn write_slot(&self, src: &impl RowSource, row: usize, slot: &mut [u8], whole: &mut Vec<u8>) {
        let ImageCol { loc, kind, invert } = self;
        match *kind {
            ImageKind::Scalar(kind) => {
                // Zero-padded where the slot is 16 bytes wide: a value index's, when
                // another of its aggregates is wide.
                let (image, pad) = slot.split_at_mut(8);
                image.copy_from_slice(&scalar_image(loc, kind, *invert, src, row).to_be_bytes());
                if let Ok(pad) = <&mut [u8; 8]>::try_from(pad) {
                    *pad = [0; 8];
                }
            }
            ImageKind::Wide(WideKind::Fixed(_)) => slot.copy_from_slice(&int16_image(loc, *invert, src, row)),
            ImageKind::Wide(WideKind::Bytes) => {
                let at = whole.len();
                append_bytes_image(*invert, &loc.content(src, row), whole);
                let image = &whole[at..];
                let take = image.len().min(slot.len());
                slot[..take].copy_from_slice(&image[..take]);
                slot[take..].fill(0);
            }
        }
    }

    /// [`Self::write_slot`] for every row of `runs`, all **known non-NULL**, a
    /// column at a time: the `n`-th row's image into the `slot`-byte window at
    /// `at` of the `n`-th `stride`-byte key of `keys`. `false`, writing nothing,
    /// for a wide image, which takes [`Self::write_slot`] a row at a time.
    pub(crate) fn write_slots(
        &self,
        mb: &MemBatch,
        runs: &[(usize, usize)],
        keys: &mut [u8],
        stride: usize,
        at: usize,
        slot: usize,
    ) -> bool {
        let ImageKind::Scalar(kind) = self.kind else {
            return false;
        };
        let flip = if self.invert { u64::MAX } else { 0 };
        // Each row's cell, `cs` bytes apart, holds the value at `off`.
        let (cells, cs, off) = match self.loc {
            ColumnLocator::Pk { byte_off, .. } => (mb.pk(), mb.pk_stride(), byte_off as usize),
            ColumnLocator::Payload { slot, size, .. } => (mb.col_data(slot as usize), size as usize, 0),
        };
        // Zero-padded where the slot is 16 bytes wide, as `write_slot` pads it.
        let pad = slot == 16;
        macro_rules! fill {
            ($w:expr, |$c:ident| $image:expr) => {{
                let mut keys = keys.chunks_exact_mut(stride);
                for &(s, e) in runs {
                    for (cell, key) in cells[s * cs..e * cs].chunks_exact(cs).zip(&mut keys) {
                        let $c = &cell[off..off + $w];
                        key[at..at + 8].copy_from_slice(&(($image) ^ flip).to_be_bytes());
                        if pad {
                            key[at + 8..at + 16].fill(0);
                        }
                    }
                }
            }};
        }
        match (kind, self.loc) {
            (ScalarKind::Int(fi), ColumnLocator::Pk { .. }) => for_each_fixed_int!(fi, |FI| {
                fill!(FI.width(), |c| ScalarKind::Int(FI)
                    .order_image(int_cell::<true>(FI, c) as u64))
            }),
            (ScalarKind::Int(fi), ColumnLocator::Payload { .. }) => for_each_fixed_int!(fi, |FI| {
                fill!(FI.width(), |c| ScalarKind::Int(FI)
                    .order_image(int_cell::<false>(FI, c) as u64))
            }),
            // A float is never a PK column.
            (ScalarKind::F32, _) => {
                fill!(4, |c| ScalarKind::F32
                    .order_image(u32::from_le_bytes(c.try_into().unwrap()) as u64))
            }
            (ScalarKind::F64, _) => fill!(8, |c| ScalarKind::F64
                .order_image(u64::from_le_bytes(c.try_into().unwrap()))),
        }
        true
    }
}

/// The index PK column [`ImageCol::write_slot`] fills with an image's leading bytes:
/// a scalar image whole, a wide one's first 16 bytes.
pub(crate) const fn image_slot_col(wide: bool) -> SchemaColumn {
    SchemaColumn::new(if wide { TypeCode::U128 } else { TypeCode::U64 }, false)
}

/// A whole image as a payload column: a BLOB orders by content.
pub(crate) const IMAGE_COL: SchemaColumn = SchemaColumn::new(TypeCode::Blob, false);

/// The native bytes of the wide column at `loc` in `row`: a string's content,
/// or the 16-byte little-endian integer.
#[inline(always)]
pub(super) fn wide_native<'a>(
    loc: &ColumnLocator,
    kind: WideKind,
    mb: &'a impl RowSource,
    row: usize,
    scratch: &'a mut [u8; 16],
) -> Cow<'a, [u8]> {
    match kind {
        WideKind::Bytes => loc.content(mb, row),
        WideKind::Fixed(_) => Cow::Borrowed(loc.native_le_bytes(mb, row, scratch)),
    }
}

/// Append the order image of byte string `content` to `out`: `0x00` escaped to
/// `0x00 0xFF`, then a `0x00 0x00` terminator, so no image is a prefix of another.
fn append_bytes_image(invert: bool, content: &[u8], out: &mut Vec<u8>) {
    let start = out.len();
    if content.contains(&0) {
        // Every byte an escape until a content byte is written over it: a
        // `0x00` leaves the one after it standing.
        out.resize(start + 2 * content.len(), 0xFF);
        let mut at = start;
        for &b in content {
            out[at] = b;
            at += 1 + usize::from(b == 0);
        }
        out.truncate(at);
    } else {
        out.extend_from_slice(content);
    }
    out.extend_from_slice(&[0, 0]);
    if invert {
        out[start..].iter_mut().for_each(|b| *b = !*b);
    }
}

/// The order image of a 16-byte integer column at `loc` in `row`, **known
/// non-NULL**: its OPK bytes, complemented when `invert`.
#[inline(always)]
fn int16_image(loc: &ColumnLocator, invert: bool, src: &impl RowSource, row: usize) -> [u8; 16] {
    let image = loc.opk_image(src, row);
    (if invert { !image } else { image }).to_be_bytes()
}

/// [`append_bytes_image`]'s and [`int16_image`]'s inverse, in place: decodes
/// the index image in `v` into the native bytes at its front and returns their
/// length.
pub(super) fn wide_native_of_image(kind: WideKind, invert: bool, v: &mut [u8]) -> usize {
    if invert {
        v.iter_mut().for_each(|b| *b = !*b);
    }
    match kind {
        WideKind::Fixed(tc) => {
            let (n, mut native) = (v.len(), [0u8; 16]);
            gnitz_wire::decode_pk_cell(v, tc.is_signed_int(), &mut native[..n]);
            v.copy_from_slice(&native[..n]);
            n
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
            w
        }
    }
}

/// The order image of a scalar column at `loc` in `row`, **known non-NULL**:
/// [`order_bits`], complemented when `invert`.
#[inline]
pub(super) fn scalar_image(
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
pub(super) fn scalar_native_of_image(kind: ScalarKind, invert: bool, image: u64) -> u64 {
    kind.order_inverse(if invert { !image } else { image })
}

#[cfg(test)]
#[path = "tests/order_image.rs"]
mod tests;
