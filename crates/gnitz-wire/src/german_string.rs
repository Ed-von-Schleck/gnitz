//! German string codec: 16-byte inline/blob string representation.

use crate::{read_u32_le, read_u64_le, write_u32_le, write_u64_le};

/// Threshold for inline German String storage (bytes).
pub const SHORT_STRING_THRESHOLD: usize = 12;

/// Where a short cell's content starts: right after its `u32` length.
pub const GERMAN_INLINE_OFF: usize = 4;

/// Encode a byte slice as a 16-byte German String struct, appending its
/// content to `blob` when it does not fit inline.
///
/// Layout:
///   [0..4]  length (u32 LE)
///   [4..8]  prefix — first min(4, len) bytes, zero-padded
///   [8..16] if len ≤ 12: suffix bytes [4..len], zero-padded
///           if len > 12: blob arena offset (u64 LE)
#[inline]
pub fn encode_german_string(s: &[u8], blob: &mut Vec<u8>) -> [u8; 16] {
    let len = s.len();
    assert!(
        len <= u32::MAX as usize,
        "encode_german_string: length {len} exceeds u32::MAX"
    );
    let mut st = [0u8; 16];
    write_u32_le(&mut st, 0, len as u32);
    if len > SHORT_STRING_THRESHOLD {
        st[4..8].copy_from_slice(&s[..4]);
        write_u64_le(&mut st, 8, blob.len() as u64);
        blob.extend_from_slice(s);
    } else {
        // Prefix and suffix are one run: the whole value, inline.
        st[GERMAN_INLINE_OFF..GERMAN_INLINE_OFF + len].copy_from_slice(s);
    }
    st
}

/// Shift every **long** cell's heap offset in `cells`, a run of whole 16-byte
/// cells, by `delta` — what a concatenating appender owes each cell once the
/// source arena has been appended onto the destination's at `delta`. A short
/// cell carries no offset and is left alone.
#[inline]
pub fn shift_german_string_heaps(cells: &mut [u8], delta: usize) {
    if delta == 0 {
        return;
    }
    for cell in cells.as_chunks_mut::<16>().0 {
        if german_string_inline(cell).is_none() {
            let off = read_u64_le(cell, 8) + delta as u64;
            write_u64_le(cell, 8, off);
        }
    }
}

/// `[heap_offset, heap_offset + length)` if it lies inside a heap of `blob_len`
/// bytes, checked in `u64` before any narrowing.
#[inline]
fn blob_extent(blob_len: usize, heap_offset: u64, length: usize) -> Option<std::ops::Range<usize>> {
    let end = heap_offset.checked_add(length as u64)?;
    if end > blob_len as u64 {
        return None;
    }
    Some(heap_offset as usize..end as usize)
}

/// A short cell's content, borrowed from the cell; `None` for a long cell.
#[inline(always)]
pub fn german_string_inline(cell: &[u8]) -> Option<&[u8]> {
    let length = read_u32_le(cell, 0) as usize;
    if length <= SHORT_STRING_THRESHOLD {
        Some(&cell[GERMAN_INLINE_OFF..GERMAN_INLINE_OFF + length])
    } else {
        None
    }
}

/// A long cell's content range in a heap of `blob_len` bytes; `None` for a
/// short cell or one that overruns the heap.
#[inline(always)]
pub fn german_string_heap(cell: &[u8], blob_len: usize) -> Option<std::ops::Range<usize>> {
    let None = german_string_inline(cell) else {
        return None;
    };
    blob_extent(blob_len, read_u64_le(cell, 8), read_u32_le(cell, 0) as usize)
}

/// True iff `cell` is in **canonical form** against `blob` — i.e. a cell
/// `encode_german_string` could have produced. This is the one predicate every
/// German-string trust boundary asks: a short cell's pad is zero, and a long
/// cell's extent fits `blob` and its prefix is the content's first four bytes.
/// [`compare_german_strings`] orders by the prefix word, so a cell failing
/// either would order apart from an equal-content one.
pub fn german_string_cell_ok(cell: &[u8; 16], blob: &[u8]) -> bool {
    if let Some(canonical) = canonical_short_cell(cell) {
        return *cell == canonical;
    }
    match german_string_heap(cell, blob.len()) {
        Some(r) => blob[r].starts_with(&cell[4..8]),
        None => false,
    }
}

/// A short cell rebuilt canonical — its content verbatim, every pad byte zero —
/// or `None` for a long cell.
#[inline]
fn canonical_short_cell(src: &[u8; 16]) -> Option<[u8; 16]> {
    if let Some(content) = german_string_inline(src) {
        // Length and content are the cell's low `4 + len` bytes.
        let keep = u128::MAX >> (128 - 8 * (4 + content.len()));
        return Some((u128::from_le_bytes(*src) & keep).to_le_bytes());
    }
    None
}

/// Every cell of German-string region `cells`, a run of whole 16-byte cells, is
/// canonical against `blob`.
pub fn german_string_region_ok(cells: &[u8], blob: &[u8]) -> bool {
    cells.as_chunks::<16>().0.iter().all(|c| german_string_cell_ok(c, blob))
}

/// `cell`, re-homed from `src_blob`: a short cell with its pad zeroed, a long
/// one pointing at the offset `place` returns for its content. A long cell
/// overrunning `src_blob` becomes the empty string, as
/// [`german_string_content`] reads it.
#[inline]
pub fn relocate_german_string_with(cell: &[u8; 16], src_blob: &[u8], place: impl FnOnce(&[u8]) -> usize) -> [u8; 16] {
    match canonical_short_cell(cell) {
        Some(short) => short,
        None => relocate_long(cell, src_blob, place),
    }
}

/// The long arm of [`relocate_german_string_with`]. A long cell's length and
/// prefix stay; only its offset moves.
#[inline]
fn relocate_long(cell: &[u8; 16], src_blob: &[u8], place: impl FnOnce(&[u8]) -> usize) -> [u8; 16] {
    let Some(span) = german_string_heap(cell, src_blob.len()) else {
        return [0; 16];
    };
    let mut out = *cell;
    write_u64_le(&mut out, 8, place(&src_blob[span]) as u64);
    out
}

/// [`relocate_german_string_with`] appending the content to `dst_blob`.
#[inline]
pub fn relocate_german_string(cell: &[u8; 16], src_blob: &[u8], dst_blob: &mut Vec<u8>) -> [u8; 16] {
    relocate_german_string_with(cell, src_blob, |content| {
        let at = dst_blob.len();
        dst_blob.extend_from_slice(content);
        at
    })
}

/// True if `cell` is short and all sixteen of its bytes are ASCII, so its
/// content is.
#[inline]
pub fn german_string_short_ascii(cell: &[u8; 16]) -> bool {
    german_string_inline(cell).is_some() && u128::from_le_bytes(*cell) & u128::from_le_bytes([0x80; 16]) == 0
}

/// The content of cell `s`, with a long cell that overruns `blob` read as empty
/// — the one degrading accessor, so every path degrades alike.
#[inline]
pub fn german_string_content<'a>(s: &'a [u8], blob: &'a [u8]) -> &'a [u8] {
    match german_string_inline(s) {
        Some(inline) => inline,
        None => match german_string_heap(s, blob.len()) {
            Some(r) => &blob[r],
            None => &[],
        },
    }
}

/// Byte-lexicographic order over two German string cells — the order
/// `compare_rows`, every merge heap and every compaction sorts by.
#[inline(always)]
pub fn compare_german_strings(a: &[u8; 16], blob_a: &[u8], b: &[u8; 16], blob_b: &[u8]) -> std::cmp::Ordering {
    // A canonical cell zero-pads its inline bytes, so each 4- and 8-byte word
    // orders as the content under it does.
    let pfx_a = u32::from_be_bytes(a[4..8].try_into().unwrap());
    let pfx_b = u32::from_be_bytes(b[4..8].try_into().unwrap());
    if pfx_a != pfx_b {
        return pfx_a.cmp(&pfx_b);
    }
    let (la, lb) = (read_u32_le(a, 0), read_u32_le(b, 0));
    // Nested, not one `&&`: joined, the long path re-tests `a`'s length.
    let content_a = if la as usize <= SHORT_STRING_THRESHOLD {
        if lb as usize <= SHORT_STRING_THRESHOLD {
            // Both inline: the rest of the content is bytes 8..16, masked to the
            // content so a stray pad byte orders as the content arm would read it;
            // when those tie too the shorter is a prefix of the longer.
            let tail = |c: &[u8; 16], len: u32| {
                u64::from_be_bytes(c[8..16].try_into().unwrap()) & u64::MAX.checked_shl(8 * (12 - len)).unwrap_or(0)
            };
            return tail(a, la).cmp(&tail(b, lb)).then(la.cmp(&lb));
        }
        &a[GERMAN_INLINE_OFF..GERMAN_INLINE_OFF + la as usize]
    } else {
        german_string_content(a, blob_a)
    };
    // Vectorised memcmp with the length tiebreak `[u8]::cmp` already applies.
    content_a.cmp(german_string_content(b, blob_b))
}

#[cfg(test)]
#[path = "tests/german_string.rs"]
mod tests;
