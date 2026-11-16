//! Columns copied verbatim from one row layout into another, and the null bits
//! and string slots that follow from the copies.

use crate::{ColumnLocator, SchemaDescriptor};

/// A column copied verbatim into another row layout.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ColCopy {
    pub src: ColumnLocator,
    /// The output payload slot.
    pub slot: usize,
    /// The output slot's width: wider than the source only for a promoted integer.
    pub width: usize,
}

/// Columns copied verbatim from an input row into an output row, with the null
/// bits and German-string slots that follow from them.
pub struct ColMoves {
    copies: Box<[ColCopy]>,
    /// The nearest null move — for most copy lists the only one, held inline so
    /// a per-row [`Self::null_word`] reads no heap. Its mask is zero when no
    /// nullable column is copied.
    first: NullMove,
    rest: Vec<NullMove>,
    /// The input's German-string payload slots some copy reads.
    string_slots: u64,
}

/// The source bits that all travel one distance: `up` slots toward the high end
/// or `down` toward the low end, one of the two being zero.
#[derive(Clone, Copy)]
struct NullMove {
    mask: u64,
    up: u8,
    down: u8,
}

impl NullMove {
    #[inline(always)]
    fn apply(self, word: u64) -> u64 {
        ((word & self.mask) << self.up) >> self.down
    }
}

impl ColMoves {
    /// The moves of `copies`, each reading a column of `src`.
    pub fn new(copies: Vec<ColCopy>, src: &SchemaDescriptor) -> Self {
        let nullable = src.nullable_payload_slots();
        let mut moves: Vec<NullMove> = Vec::new();
        let mut string_slots = 0u64;
        for c in &copies {
            let ColumnLocator::Payload { slot: src, type_code, .. } = c.src else {
                continue;
            };
            if type_code.is_german_string() {
                string_slots |= 1u64 << src;
            }
            if !gnitz_wire::null_word_get(nullable, src as usize) {
                continue;
            }
            let dst = c.slot as u8;
            let (up, down) = (dst.saturating_sub(src), src.saturating_sub(dst));
            match moves.iter_mut().find(|m| (m.up, m.down) == (up, down)) {
                Some(m) => m.mask |= 1u64 << src,
                None => moves.push(NullMove { mask: 1u64 << src, up, down }),
            }
        }
        // Nearest first: bits that stay put take the first pass, which then
        // needs no shift.
        moves.sort_by_key(|m| m.up.max(m.down));
        let mut moves = moves.into_iter();
        let first = moves.next().unwrap_or(NullMove { mask: 0, up: 0, down: 0 });
        ColMoves {
            copies: copies.into_boxed_slice(),
            first,
            rest: moves.collect(),
            string_slots,
        }
    }

    /// The copies, in the order they were given.
    #[inline]
    pub fn copies(&self) -> &[ColCopy] {
        &self.copies
    }

    /// The input's German-string payload slots some copy reads.
    #[inline]
    pub fn string_slots(&self) -> u64 {
        self.string_slots
    }

    /// One input row's null word as its output row's.
    #[inline(always)]
    pub fn null_word(&self, word: u64) -> u64 {
        self.rest
            .iter()
            .fold(self.first.apply(word), |bits, m| bits | m.apply(word))
    }

    /// Derive the null words of `out` rows `[dst_base, dst_base + n)` from
    /// source rows `[src_start, src_start + n)`, one u64 per row. The whole
    /// window is written: the destination may be an uninitialized tail.
    pub fn write_null_rows(&self, in_null_bmp: &[u8], src_start: usize, out: &mut [u8], dst_base: usize, n: usize) {
        let dst = &mut out[dst_base * 8..(dst_base + n) * 8];
        let src = in_null_bmp[src_start * 8..(src_start + n) * 8].as_chunks::<8>().0;
        let word = |w: &[u8; 8]| u64::from_le_bytes(*w);
        let (first, rest) = (self.first, &self.rest[..]);
        if first.mask == 0 {
            return dst.fill(0);
        }
        // A move at a time, each pass one uniform shift; several moves go block by
        // block so the words they revisit stay in L1.
        let block = if rest.is_empty() { n.max(1) } else { 256 };
        for (d, s) in dst.as_chunks_mut::<8>().0.chunks_mut(block).zip(src.chunks(block)) {
            if first.up == 0 && first.down == 0 {
                for (d, s) in d.iter_mut().zip(s) {
                    *d = (word(s) & first.mask).to_le_bytes();
                }
            } else {
                for (d, s) in d.iter_mut().zip(s) {
                    *d = first.apply(word(s)).to_le_bytes();
                }
            }
            for &mv in rest {
                for (d, s) in d.iter_mut().zip(s) {
                    *d = (word(d) | mv.apply(word(s))).to_le_bytes();
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/copy.rs"]
mod tests;
