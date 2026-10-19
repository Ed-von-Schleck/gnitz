use super::*;
use crate::{
    compare_german_strings, encode_german_string, german_string_bytes, german_string_lead, relocate_german_value_with,
    write_u64_le,
};

/// `codes` as one value of a coded heap: their count, then the codes.
fn framed(codes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    push_coded(&mut out, |dst| dst.extend_from_slice(codes));
    out
}

/// Symbols of one to eight bytes, a prefix of one another among them.
const SYMBOLS: [&[u8]; 5] = [b"hello", b" ", b"world", b"ab", b"abcdefgh"];

/// The table of `SYMBOLS`, one per test process.
static TABLE: std::sync::LazyLock<Box<SymbolTable>> = std::sync::LazyLock::new(|| {
    let symbols: Vec<[u8; 8]> = SYMBOLS
        .iter()
        .map(|s| {
            let mut sym = [0u8; 8];
            sym[..s.len()].copy_from_slice(s);
            sym
        })
        .collect();
    let lens: Vec<u8> = SYMBOLS.iter().map(|s| s.len() as u8).collect();
    SymbolTable::new(&symbols, &lens).unwrap()
});

/// `codes` as a heap of `TABLE`'s codes whose values decode to `plain_len` bytes.
fn coded(codes: &[u8], plain_len: usize) -> Heap<'_> {
    Heap::Coded(CodedHeap { table: &TABLE, codes, plain_len })
}

/// `value` in `SYMBOLS`' codes: the longest symbol at each position, an escape
/// where none is.
fn code(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < value.len() {
        let best = (0..SYMBOLS.len())
            .filter(|&c| value[at..].starts_with(SYMBOLS[c]))
            .max_by_key(|&c| SYMBOLS[c].len());
        match best {
            Some(c) => {
                out.push(c as u8);
                at += SYMBOLS[c].len();
            }
            None => {
                out.extend([SYMBOL_ESCAPE, value[at]]);
                at += 1;
            }
        }
    }
    out
}

/// Cells over a heap, and the heap.
type CellsOver = (Vec<[u8; 16]>, Vec<u8>);

/// `values` as long cells over a plain heap and over a coded one, both holding
/// them back to back.
fn heaps(values: &[&[u8]]) -> (CellsOver, CellsOver) {
    let (mut plain, mut codes) = (Vec::new(), Vec::new());
    let (mut plain_cells, mut coded_cells) = (Vec::new(), Vec::new());
    for v in values {
        plain_cells.push(encode_german_string(v, &mut plain));
        let mut cell = encode_german_string(v, &mut Vec::new());
        if v.len() > crate::SHORT_STRING_THRESHOLD {
            write_u64_le(&mut cell, 8, codes.len() as u64);
            codes.extend(framed(&code(v)));
        }
        coded_cells.push(cell);
    }
    ((plain_cells, plain), (coded_cells, codes))
}

#[test]
fn a_table_refuses_a_symbol_it_cannot_hold() {
    let sym = [[0u8; 8]];
    assert!(SymbolTable::new(&sym, &[1]).is_some());
    assert!(SymbolTable::new(&sym, &[0]).is_none(), "an empty symbol");
    assert!(SymbolTable::new(&sym, &[9]).is_none(), "a symbol past eight bytes");
    assert!(SymbolTable::new(&sym, &[1, 1]).is_none(), "more lengths than symbols");
    let full = vec![[0u8; 8]; MAX_SYMBOLS + 1];
    assert!(
        SymbolTable::new(&full, &vec![1; MAX_SYMBOLS + 1]).is_none(),
        "a symbol on the escape code"
    );
}

/// A coded value decodes to its cell's length, reads alone, and ends where the
/// next one starts.
#[test]
fn coded_values_read_alone() {
    let values: [&[u8]; 3] = [
        b"hello world, hello abcdefgh!",
        b"ab ab ab ab abab ab",
        b"\xff\x00 hello world \x01",
    ];
    let ((_, plain), (cells, codes)) = heaps(&values);
    let heap = coded(&codes, plain.len());
    let mut at = 0;
    for (v, cell) in values.iter().zip(&cells) {
        let value = heap.value(at as u64, v.len()).unwrap();
        assert_eq!(*value.bytes(), **v);
        assert_eq!(value.lead(), v[..8]);
        let mut dst = vec![7u8; 3];
        value.append_to(&mut dst);
        assert_eq!((&dst[..3], &dst[3..]), (&[7u8; 3][..], *v));
        assert_eq!(*german_string_bytes(cell, heap), **v);
        at = value.end();
    }
    assert_eq!(at, codes.len());
    assert_eq!(heap.plain_len(), values.iter().map(|v| v.len()).sum());
}

/// Codes are body bytes no open verifies: a code past the table reads a zero
/// byte, codes running out leave zeroes, and a value whose count runs past the
/// heap, or that starts past it, is none.
#[test]
fn corrupt_codes_decode_to_the_cells_length() {
    let codes = framed(&[0, 200, SYMBOL_ESCAPE]);
    let heap = coded(&codes, 0);
    let value = heap.value(0, 13).unwrap();
    let mut want = b"hello\0".to_vec();
    want.resize(13, 0);
    assert_eq!(*value.bytes(), want[..]);
    assert_eq!(value.end(), codes.len());
    assert!(heap.value(codes.len() as u64, 13).is_none());
    assert!(heap.value(u64::MAX, 13).is_none());
    let overrun = [9, 0, 0];
    let heap = coded(&overrun, 0);
    assert!(heap.value(0, 13).is_none(), "a count past the heap");
    let unended = [0x80; 12];
    let heap = coded(&unended, 0);
    assert!(heap.value(0, 13).is_none(), "a count that never ends");
    let plain = Heap::Plain(b"0123456789abcdef");
    assert!(plain.value(4, 13).is_none(), "past a plain heap");
    assert_eq!(*plain.value(3, 13).unwrap().bytes(), b"3456789abcdef"[..]);
}

/// A coded heap orders, leads and relocates as the plain heap of the same
/// values does.
#[test]
fn a_coded_heap_reads_as_its_plain_heap() {
    let values: [&[u8]; 7] = [
        b"hello world",
        b"hello world!!",
        b"hello world!!!",
        b"hello worlds of ab",
        b"abcdefghabcdefgh!",
        b"abcdefgh abcdefgh",
        b"ab",
    ];
    let ((plain_cells, plain), (coded_cells, codes)) = heaps(&values);
    let coded = coded(&codes, plain.len());
    let plain = Heap::Plain(&plain);
    for (pa, ca) in plain_cells.iter().zip(&coded_cells) {
        assert_eq!(german_string_lead(ca, coded), german_string_lead(pa, plain));
        for (pb, cb) in plain_cells.iter().zip(&coded_cells) {
            let want = compare_german_strings(pa, plain, pb, plain);
            assert_eq!(compare_german_strings(ca, coded, cb, coded), want);
            assert_eq!(compare_german_strings(ca, coded, pb, plain), want);
            assert_eq!(compare_german_strings(pa, plain, cb, coded), want);
        }
        let mut dst = Vec::new();
        let moved = relocate_german_value_with(ca, coded, |value| {
            let at = dst.len();
            value.append_to(&mut dst);
            at
        });
        assert_eq!(
            *german_string_bytes(&moved, Heap::Plain(&dst)),
            *german_string_bytes(pa, plain)
        );
    }
}

/// A count is LEB128: one byte below 128 codes, more past it.
#[test]
fn a_coded_value_carries_its_code_count() {
    for n in [0usize, 1, 127, 128, 300, 70_000] {
        let codes = vec![1u8; n];
        let framed = framed(&codes);
        let mut copied = Vec::new();
        push_codes(&mut copied, &codes);
        assert_eq!(copied, framed, "{n} codes framed alike, written or copied");
        let head = framed.len() - n;
        assert_eq!(
            head,
            if n < 128 {
                1
            } else if n < 1 << 14 {
                2
            } else {
                3
            },
            "{n} codes"
        );
        let heap = coded(&framed, 0);
        let value = heap.value(0, n.max(13)).unwrap();
        assert_eq!((value.stored(), value.end()), (&codes[..], framed.len()));
    }
}
