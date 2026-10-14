//! The heap a source's long German-string values lie in: their bytes verbatim,
//! or each value spelled in the codes of a symbol table (FSST). A long cell's
//! offset names where its value starts in the heap's stored bytes, so a value of
//! a coded heap is read alone: a coded value is its code count (LEB128), then
//! its codes, which decode to the length its cell holds.

use std::borrow::Cow;

/// The code byte whose next byte is the value's next byte, verbatim.
pub const SYMBOL_ESCAPE: u8 = 255;

/// The most symbols a table holds: every code byte but [`SYMBOL_ESCAPE`].
pub const MAX_SYMBOLS: usize = SYMBOL_ESCAPE as usize;

/// The most bytes a symbol stands for.
pub const MAX_SYMBOL_LEN: usize = 8;

/// Code `c` stands for the first `lens[c]` bytes of `symbols[c]`. A code past
/// the table's own symbols stands for one zero byte, so a corrupt code stream
/// decodes to some bytes of the length its cell holds and never stalls.
pub struct SymbolTable {
    symbols: [[u8; MAX_SYMBOL_LEN]; 256],
    lens: [u8; 256],
    /// How many codes stand for a symbol of the table's own.
    count: usize,
    /// See [`digest`](Self::digest).
    digest: u64,
}

impl SymbolTable {
    /// The table whose code `c` stands for `symbols[c][..lens[c]]`; `None`
    /// unless there are as many lengths as symbols, at most [`MAX_SYMBOLS`],
    /// each `1..=MAX_SYMBOL_LEN`.
    pub fn new(symbols: &[[u8; MAX_SYMBOL_LEN]], lens: &[u8]) -> Option<Box<Self>> {
        if symbols.len() != lens.len()
            || symbols.len() > MAX_SYMBOLS
            || lens.iter().any(|&n| !(1..=MAX_SYMBOL_LEN as u8).contains(&n))
        {
            return None;
        }
        let mut table = Box::new(SymbolTable {
            symbols: [[0; MAX_SYMBOL_LEN]; 256],
            lens: [1; 256],
            count: symbols.len(),
            digest: crate::checksum(&[symbols.as_flattened(), lens].concat()),
        });
        table.symbols[..symbols.len()].copy_from_slice(symbols);
        table.lens[..lens.len()].copy_from_slice(lens);
        Some(table)
    }

    /// The table's own symbols, each with its length.
    pub fn symbols(&self) -> impl Iterator<Item = (&[u8; MAX_SYMBOL_LEN], u8)> {
        self.symbols[..self.count]
            .iter()
            .zip(self.lens[..self.count].iter().copied())
    }

    /// A 64-bit digest of the symbols and their lengths. Two tables of one
    /// digest are taken to be one table, whose codes either may copy.
    pub fn digest(&self) -> u64 {
        self.digest
    }

    /// Decode `codes` into `out[..len]`, which has `MAX_SYMBOL_LEN - 1` bytes
    /// of slack past `len` that the last symbol may spill into. Codes running
    /// out first leave the rest zero.
    #[inline]
    fn decode(&self, codes: &[u8], len: usize, out: &mut [u8]) {
        assert!(out.len() >= len + MAX_SYMBOL_LEN - 1, "decode: no slack past the value");
        let dst = out.as_mut_ptr();
        let (mut o, mut codes) = (0, codes.iter());
        while o < len {
            match codes.next() {
                Some(&SYMBOL_ESCAPE) => {
                    // SAFETY: `o < len <= out.len()`.
                    unsafe { *dst.add(o) = codes.next().copied().unwrap_or(0) };
                    o += 1;
                }
                Some(&c) => {
                    // SAFETY: `o < len` and `out` holds `MAX_SYMBOL_LEN - 1` bytes
                    // past `len`, so the symbol's eight bytes at `o` lie in it.
                    unsafe {
                        dst.add(o)
                            .cast::<[u8; MAX_SYMBOL_LEN]>()
                            .write_unaligned(self.symbols[c as usize])
                    };
                    o += self.lens[c as usize] as usize;
                }
                None => {
                    out[o..len].fill(0);
                    return;
                }
            }
        }
    }
}

/// A heap whose values are spelled in `table`'s codes.
#[derive(Clone, Copy)]
pub struct CodedHeap<'a> {
    pub table: &'a SymbolTable,
    /// The values, each its code count (LEB128), then its codes.
    pub codes: &'a [u8],
    /// The bytes its values decode to: what a heap taking all of them in
    /// reserves.
    pub plain_len: usize,
}

/// A source's German-string heap.
#[derive(Clone, Copy)]
pub enum Heap<'a> {
    /// Each long value's bytes, verbatim, at its cell's offset.
    Plain(&'a [u8]),
    /// Each long value coded, starting at its cell's offset.
    Coded(CodedHeap<'a>),
}

impl<'a> Heap<'a> {
    /// The bytes the heap's values take decoded: what a destination heap taking
    /// all of them in reserves.
    #[inline]
    pub fn plain_len(self) -> usize {
        match self {
            Heap::Plain(bytes) => bytes.len(),
            Heap::Coded(coded) => coded.plain_len,
        }
    }

    /// The heap's bytes, when they are its values verbatim.
    #[inline]
    pub fn plain(self) -> Option<&'a [u8]> {
        match self {
            Heap::Plain(bytes) => Some(bytes),
            Heap::Coded(_) => None,
        }
    }

    /// The value of `len` bytes stored from `at`; `None` where the heap does
    /// not hold it whole.
    #[inline]
    pub fn value(self, at: u64, len: usize) -> Option<HeapValue<'a>> {
        match self {
            Heap::Plain(bytes) => {
                let end = usize::try_from(at.checked_add(len as u64)?)
                    .ok()
                    .filter(|&end| end <= bytes.len())?;
                Some(HeapValue {
                    stored: &bytes[at as usize..end],
                    len,
                    table: None,
                    end,
                })
            }
            Heap::Coded(coded) => {
                let (codes, end) = coded_extent(coded.codes, usize::try_from(at).ok()?)?;
                Some(HeapValue {
                    stored: codes,
                    len,
                    table: Some(coded.table),
                    end,
                })
            }
        }
    }
}

/// The codes of the coded value stored from `at` in `codes`, and where the next
/// value starts; `None` where `codes` does not hold them whole.
#[inline]
fn coded_extent(codes: &[u8], at: usize) -> Option<(&[u8], usize)> {
    let (mut n, mut shift, mut i) = (0usize, 0, at);
    loop {
        let byte = *codes.get(i)?;
        i += 1;
        n |= ((byte & 0x7F) as usize).checked_shl(shift)?;
        if byte < 0x80 {
            break;
        }
        shift += 7;
    }
    let end = i.checked_add(n).filter(|&end| end <= codes.len())?;
    Some((&codes[i..end], end))
}

/// The longest code count, LEB128.
const COUNT_ROOM: usize = usize::BITS.div_ceil(7) as usize;

/// `n` as LEB128, in the first of the bytes it returns as many.
#[inline]
fn count_bytes(mut n: usize) -> ([u8; COUNT_ROOM], usize) {
    let (mut count, mut k) = ([0u8; COUNT_ROOM], 0);
    loop {
        count[k] = (n & 0x7F) as u8 | if n > 0x7F { 0x80 } else { 0 };
        k += 1;
        n >>= 7;
        if n == 0 {
            return (count, k);
        }
    }
}

/// Append a coded value to `dst`: its code count, then `codes`.
#[inline]
pub fn push_codes(dst: &mut Vec<u8>, codes: &[u8]) {
    let (count, k) = count_bytes(codes.len());
    dst.extend_from_slice(&count[..k]);
    dst.extend_from_slice(codes);
}

/// Append a coded value to `dst`: its code count, then the codes `write`
/// appends.
#[inline]
pub fn push_coded(dst: &mut Vec<u8>, write: impl FnOnce(&mut Vec<u8>)) {
    // Room for the longest count, closed up once the codes are known.
    let at = dst.len();
    dst.resize(at + COUNT_ROOM, 0);
    write(dst);
    let (count, k) = count_bytes(dst.len() - at - COUNT_ROOM);
    dst.copy_within(at + COUNT_ROOM.., at + k);
    dst[at..at + k].copy_from_slice(&count[..k]);
    dst.truncate(dst.len() - (COUNT_ROOM - k));
}

/// One long value of a heap, not yet read.
#[derive(Clone, Copy)]
pub struct HeapValue<'a> {
    /// The bytes the heap stores for the value: the value itself, or its codes.
    stored: &'a [u8],
    /// The value's length, decoded.
    len: usize,
    /// The table its codes are spelled in; `None` for a value stored verbatim.
    table: Option<&'a SymbolTable>,
    /// Where the value after it starts, when a heap holds its values back to back.
    end: usize,
}

impl<'a> HeapValue<'a> {
    /// The bytes the heap stores for the value: the value itself, or its codes.
    #[inline]
    pub fn stored(self) -> &'a [u8] {
        self.stored
    }

    /// The digest of the table the value is coded under; `None` for a value
    /// stored verbatim.
    #[inline]
    pub fn table_digest(self) -> Option<u64> {
        self.table.map(SymbolTable::digest)
    }

    /// The value's length, decoded.
    #[inline]
    pub fn len(self) -> usize {
        self.len
    }

    /// Whether the value is empty; a long cell's never is.
    #[inline]
    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    /// The address of the value's first stored byte, and its length: two values
    /// of one heap are the same value iff their keys are equal.
    #[inline]
    pub fn key(self) -> (usize, usize) {
        (self.stored.as_ptr() as usize, self.len)
    }

    /// The value's bytes: borrowed from a plain heap, decoded from a coded one.
    #[inline]
    pub fn bytes(self) -> Cow<'a, [u8]> {
        match self.table {
            None => Cow::Borrowed(self.stored),
            Some(_) => {
                let mut out = Vec::new();
                self.append_to(&mut out);
                Cow::Owned(out)
            }
        }
    }

    /// Append the value's bytes to `dst`.
    #[inline]
    pub fn append_to(self, dst: &mut Vec<u8>) {
        match self.table {
            None => dst.extend_from_slice(self.stored),
            Some(table) => {
                let start = dst.len();
                dst.resize(start + self.len + MAX_SYMBOL_LEN - 1, 0);
                table.decode(self.stored, self.len, &mut dst[start..]);
                dst.truncate(start + self.len);
            }
        }
    }

    /// The value's first eight bytes; a long value has at least that many.
    #[inline]
    pub fn lead(self) -> [u8; 8] {
        debug_assert!(self.len >= 8, "a long value has at least eight bytes");
        match self.table {
            None => self.stored[..8].try_into().unwrap(),
            Some(table) => {
                let mut out = [0u8; 8 + MAX_SYMBOL_LEN - 1];
                table.decode(self.stored, 8, &mut out);
                out[..8].try_into().unwrap()
            }
        }
    }

    /// Where the value after this one starts, when a heap holds its values back
    /// to back.
    #[inline]
    pub fn end(self) -> usize {
        self.end
    }
}

#[cfg(test)]
#[path = "tests/heap.rs"]
mod tests;
