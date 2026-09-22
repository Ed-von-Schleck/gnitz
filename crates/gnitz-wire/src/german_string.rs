//! German string codec: 16-byte inline/blob string representation.

use crate::{read_u32_le, read_u64_le, write_u32_le, write_u64_le};

/// Threshold for inline German String storage (bytes).
pub const SHORT_STRING_THRESHOLD: usize = 12;

/// Encode a byte slice as a 16-byte German String struct destined for a heap
/// whose current end is `heap_off`, returning the cell and the bytes it spills
/// there — empty while the value fits inline. Returning the spill rather than
/// appending it lets a caller writing into a pre-sized slice place it directly.
///
/// Layout:
///   [0..4]  length (u32 LE)
///   [4..8]  prefix — first min(4, len) bytes, zero-padded
///   [8..16] if len ≤ 12: suffix bytes [4..len], zero-padded
///           if len > 12: blob arena offset (u64 LE)
#[inline]
pub(crate) fn encode_german_string_cell(s: &[u8], heap_off: usize) -> ([u8; 16], &[u8]) {
    let len = s.len();
    assert!(
        len <= u32::MAX as usize,
        "encode_german_string: length {len} exceeds u32::MAX"
    );
    let mut st = [0u8; 16];
    write_u32_le(&mut st, 0, len as u32);
    if len > SHORT_STRING_THRESHOLD {
        st[4..8].copy_from_slice(&s[..4]);
        write_u64_le(&mut st, 8, heap_off as u64);
        return (st, s);
    }
    // Prefix and suffix are one run: the whole value, inline.
    st[4..4 + len].copy_from_slice(s);
    (st, &[])
}

/// `encode_german_string_cell` against a growable arena: the spill, if any,
/// is appended to `blob`.
#[inline]
pub fn encode_german_string(s: &[u8], blob: &mut Vec<u8>) -> [u8; 16] {
    let (st, spill) = encode_german_string_cell(s, blob.len());
    blob.extend_from_slice(spill);
    st
}

/// Shift a **long** cell's heap offset by `delta` — what a concatenating
/// appender owes each cell once the source arena has been appended onto the
/// destination's. A short cell carries no offset and is left alone.
#[inline]
pub fn shift_german_string_heap(cell: &mut [u8], delta: usize) {
    let None = german_string_inline(cell) else {
        return;
    };
    let off = read_u64_le(cell, 8) + delta as u64;
    write_u64_le(cell, 8, off);
}

/// `[heap_offset, heap_offset + length)` if it lies inside a heap of `blob_len`
/// bytes, checked in `u64` before any narrowing.
#[inline]
pub fn blob_extent(blob_len: usize, heap_offset: u64, length: usize) -> Option<std::ops::Range<usize>> {
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
        Some(&cell[4..4 + length])
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

/// Decode a 16-byte German String struct into raw bytes, or `None` if a
/// long string's blob offset/length overruns `blob`. The owned, fallible
/// counterpart of [`german_string_content`] — for the trust boundaries that
/// must reject a corrupt cell rather than degrade it to empty.
pub fn try_decode_german_string(st: &[u8], blob: &[u8]) -> Option<Vec<u8>> {
    match german_string_inline(st) {
        Some(inline) => Some(inline.to_vec()),
        None => Some(blob[german_string_heap(st, blob.len())?].to_vec()),
    }
}

/// True iff `cell` is in **canonical form** against `blob` — i.e. a cell
/// `encode_german_string` could have produced. This is the one predicate every
/// German-string trust boundary asks: a short cell's pad is zero, and a long
/// cell's extent fits `blob` and its prefix is the content's first four bytes.
/// [`compare_german_strings`] orders by the prefix word, so a cell failing
/// either would order apart from an equal-content one.
pub fn german_string_cell_ok(cell: &[u8], blob: &[u8]) -> bool {
    let cell: &[u8; 16] = cell[..16].try_into().unwrap();
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
pub fn canonical_short_cell(src: &[u8; 16]) -> Option<[u8; 16]> {
    if let Some(content) = german_string_inline(src) {
        // Length and content are the cell's low `4 + len` bytes.
        let keep = u128::MAX >> (128 - 8 * (4 + content.len()));
        return Some((u128::from_le_bytes(*src) & keep).to_le_bytes());
    }
    None
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
pub fn compare_german_strings(a: &[u8], blob_a: &[u8], b: &[u8], blob_b: &[u8]) -> std::cmp::Ordering {
    // A canonical cell zero-pads its prefix word, so the word orders as the
    // content does.
    let pfx_a = u32::from_be_bytes(a[4..8].try_into().unwrap());
    let pfx_b = u32::from_be_bytes(b[4..8].try_into().unwrap());
    if pfx_a != pfx_b {
        return pfx_a.cmp(&pfx_b);
    }
    // Vectorised memcmp with the length tiebreak `[u8]::cmp` already applies.
    german_string_content(a, blob_a).cmp(german_string_content(b, blob_b))
}

#[cfg(test)]
#[path = "tests/german_string.rs"]
mod tests;
