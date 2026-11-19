use super::*;
use crate::test_support::{opk_pk, pk_only_schema};
use gnitz_foundation::rng::{Rng, mix};
use gnitz_wire::{FixedInt, TypeCode};
use gnitz_zset::schema::SchemaDescriptor;

/// Pack signed/unsigned values into a raw `fi.width()`-byte-per-cell LE
/// region, exactly as the payload region would appear on the batch.
pub(super) fn pack(vals: &[i128], fi: FixedInt) -> Vec<u8> {
    let width = fi.width();
    let mut b = Vec::with_capacity(vals.len() * width);
    for &v in vals {
        let le = (v as i64 as u64).to_le_bytes();
        b.extend_from_slice(&le[..width]);
    }
    b
}

/// Encode, parse, decode per row and in bulk, and assert each reproduces the
/// input region. Returns the offset width chosen (None ⇒ the encoder declined).
fn roundtrip(vals: &[i128], fi: FixedInt) -> Option<usize> {
    let raw = pack(vals, fi);
    let (n, w) = (vals.len(), fi.width());
    let image = for_encode(&raw, fi)?;
    let frame = ForImage::parse(&image, n, w - 1).expect("the encoder's image parses");
    let bw = frame.bw;
    assert!(
        ForImage::parse(&image, n, bw - 1).is_none(),
        "wider than the offsets admitted"
    );
    assert!(ForImage::parse(&image, n + 1, w - 1).is_none(), "another row count");
    let mut decoded = vec![0u8; n * w];
    frame.decode(0, w, &mut decoded);
    assert_eq!(decoded, raw, "byte-exact roundtrip (bw={bw}, {fi:?})");
    for (row, cell) in raw.chunks_exact(w).enumerate() {
        assert_eq!(frame.at(row).to_le_bytes()[..w], *cell, "row {row} (bw={bw}, {fi:?})");
    }
    if n >= 10 {
        let mut window = vec![0u8; 7 * w];
        frame.decode(3, w, &mut window);
        assert_eq!(
            window,
            raw[3 * w..10 * w],
            "rows [3, 10) decode alone (bw={bw}, {fi:?})"
        );
    }
    Some(bw)
}

/// The offset width `for_encode` must choose: the bytes `vals`' span needs, if
/// the image at that width is smaller than the raw region.
fn expected_bw(vals: &[i128], fi: FixedInt) -> Option<usize> {
    let span = vals.iter().max()? - vals.iter().min()?;
    let bw = (span.cast_unsigned().bit_width() as usize).div_ceil(8);
    (bw > 0 && for_image_len(vals.len(), bw) < vals.len() * fi.width()).then_some(bw)
}

#[test]
fn for_packs_at_the_width_the_span_needs() {
    use FixedInt::*;
    let series = |n: i128, f: fn(i128) -> i128| (0..n).map(f).collect::<Vec<_>>();
    let cases: Vec<(&str, Vec<i128>, FixedInt, Option<usize>)> = vec![
        // bw >= 1 == stride.
        ("stride 1", vec![0, 1, 2, 3, 200], U8, None),
        ("stride 1 signed", vec![-5, 0, 5, 100], I8, None),
        ("small", series(300, |i| i % 200), U32, Some(1)),
        ("two-byte span", series(300, |i| (i * 211) % 60000), U32, Some(2)),
        // A tight range far from zero frames on its min.
        ("high floor", series(256, |i| 1_000_000 + i % 50), U32, Some(1)),
        // A range spanning zero frames on the signed min, not on 0.
        ("across zero", series(256, |i| -100 + i % 150), I32, Some(1)),
        ("wide across zero", series(500, |i| -30_000 + i % 60000), I32, Some(2)),
        // Constant is claimed before FoR; the encoder declines too.
        ("all equal", vec![42; 100], U32, None),
        ("extremes", vec![0, u32::MAX as i128], U32, None),
        ("extremes i64", vec![i64::MIN as i128, i64::MAX as i128], I64, None),
        // NULL cells carry a zero that joins the frame.
        (
            "null zeros",
            series(200, |i| if i % 3 == 0 { 0 } else { 1_000_000 + i % 500 }),
            U32,
            Some(3),
        ),
        (
            "null zeros signed",
            series(200, |i| if i % 4 == 0 { 0 } else { -500_000 - i % 300 }),
            I32,
            Some(3),
        ),
        // Raw 20 B, packed 8 + 5·1 + 7 B.
        ("no smaller", series(5, |i| i * 50), U32, None),
        ("a byte smaller", series(6, |i| i * 40), U32, Some(1)),
    ];
    for (what, vals, fi, want) in cases {
        assert_eq!(roundtrip(&vals, fi), want, "{what}");
        assert_eq!(expected_bw(&vals, fi), want, "{what}: the oracle");
    }
}

/// Random regions of every eligible type, at spans from one value to the
/// type's whole range, pack exactly when and as tightly as [`expected_bw`] says.
#[test]
fn random_regions_pack_at_the_expected_width() {
    let mut rng = Rng::new(0x9E3779B97F4A7C15);
    let (mut packed, mut declined) = (0, 0);
    for fi in FixedInt::ALL {
        let bits = 8 * fi.width() as u32;
        let (lo, size) = (fi.range().0, 1u128 << bits);
        for &n in &[1usize, 2, 10, 1023, 1024, 5000] {
            for _ in 0..8 {
                let span = 1u128 << rng.gen_range(bits as u64 + 1);
                let base = lo + (rng.gen_u128() % (size - span + 1)) as i128;
                let vals: Vec<i128> = (0..n).map(|_| base + (rng.gen_u128() % span) as i128).collect();
                let got = roundtrip(&vals, fi);
                assert_eq!(got, expected_bw(&vals, fi), "{fi:?} n={n} span={span}");
                if got.is_some() {
                    packed += 1;
                } else {
                    declined += 1;
                }
            }
        }
    }
    assert!(packed > 50 && declined > 50, "{packed} packed, {declined} declined");
}

/// A region of two words reads back per row and in bulk from any row on, and a
/// region of one word or of three has no such image.
#[test]
fn two_value_roundtrips_from_any_row() {
    let (a, b) = (1u64, -1i64 as u64);
    let words = |n: usize, f: &dyn Fn(usize) -> u64| (0..n).flat_map(|i| f(i).to_le_bytes()).collect::<Vec<u8>>();
    for n in [2, 7, 8, 9, 64, 131] {
        let src = words(n, &|i| if i % 3 == 1 || i % 11 == 5 { b } else { a });
        let image = two_value_encode(&src).unwrap();
        let two = TwoValueImage::parse(&image, n).unwrap();
        assert!(TwoValueImage::parse(&image, n + 8).is_none(), "{n}: another row count");
        for row in 0..n {
            assert_eq!(two.at(row).to_le_bytes(), src[row * 8..][..8], "{n}: row {row}");
        }
        for first in 0..n.min(20) {
            for rows in [0, 1, n - first, (n - first) / 2] {
                let mut out = vec![0xAAu8; rows * 8];
                two.decode(first, &mut out);
                assert_eq!(out, src[first * 8..][..rows * 8], "{n}: {rows} rows from {first}");
            }
        }
    }
    assert!(two_value_encode(&words(9, &|_| a)).is_none(), "one word");
    assert!(two_value_encode(&words(9, &|i| i as u64 % 3)).is_none(), "three words");
    let third_last = words(131, &|i| if i == 130 { 7 } else { i as u64 % 2 });
    assert!(two_value_encode(&third_last).is_none(), "a third word in the last row");
    // Rows 128.. are the last byte's bits 0..=2: row 130 alone holds `value_b`.
    let image = two_value_encode(&words(131, &|i| if i == 130 { b } else { a })).unwrap();
    let mut bits = [0u8; 17];
    bits[16] = 0b100;
    assert_eq!(image[TWO_VALUE_BITS_AT..], bits, "no bit past the last row");
}

/// `n` distinct cells, cell `i` holding `i` in its low bytes.
fn dict_entries(n: usize) -> Vec<[u8; 16]> {
    (0..n as u128).map(|i| (i * 0x0101 + 7).to_le_bytes()).collect()
}

/// A dictionary reads back per row and in bulk, at codes of one bit, of a whole
/// byte and of more than one, and at the entry counts either side of where the
/// width changes.
#[test]
fn dict_roundtrips_at_every_code_width() {
    for n in [1, 2, 255, 256, 257, 4000] {
        let entries = dict_entries(n);
        let ids: Vec<u32> = (0..3 * n as u32 + 5).map(|i| (i * 7 + i / 3) % n as u32).collect();
        let image = dict_encode(&entries, &ids);
        assert_eq!(image.len(), dict_image_len(ids.len(), n));
        let bits = [(1, 1), (2, 1), (255, 8), (256, 8), (257, 9), (4000, 12)];
        let bits = bits.iter().find(|&&(entries, _)| entries == n).unwrap().1;
        assert_eq!(
            image.len() - 8 - n * 16,
            (ids.len() * bits).div_ceil(8) + 3,
            "{n} entries: code width"
        );
        let dict = DictImage::parse(&image, ids.len()).unwrap();
        for (row, &id) in ids.iter().enumerate() {
            assert_eq!(dict.cell(row), &entries[id as usize], "{n} entries: row {row}");
        }
        // A cell narrower than an entry is the entry's leading bytes.
        for width in [1, 2, 4, 8, 16] {
            for window in [0..ids.len(), 3..ids.len() - 1, 2..2] {
                let mut out = vec![0u8; window.len() * width];
                dict.decode(window.start, width, &mut out);
                let want: Vec<u8> = ids[window]
                    .iter()
                    .flat_map(|&id| &entries[id as usize][..width])
                    .copied()
                    .collect();
                assert_eq!(out, want, "{n} entries: bulk decode at {width} bytes");
            }
        }
    }
}

/// An image whose size its own entry count does not give is no dictionary.
#[test]
fn dict_parse_refuses_a_size_its_entry_count_does_not_give() {
    let entries = dict_entries(3);
    // Eight codes of two bits: two bytes.
    let image = dict_encode(&entries, &[0, 1, 2, 1, 0, 2, 2, 1]);
    assert!(DictImage::parse(&image, 8).is_some());
    for count in [4, 9] {
        assert!(DictImage::parse(&image, count).is_none(), "row count {count}");
    }
    assert!(DictImage::parse(&image[..image.len() - 1], 8).is_none());
    assert!(DictImage::parse(&image[..7], 8).is_none(), "shorter than its own count");
    for forged in [0u64, 2, 5, DICT_MAX_ENTRIES as u64 + 1, u64::MAX] {
        let mut image = image.clone();
        write_u64_le(&mut image, 0, forged);
        assert!(DictImage::parse(&image, 8).is_none(), "entry count {forged}");
    }
}

/// A code past the last entry reads the last entry, never past the dictionary.
#[test]
fn a_dict_code_past_the_last_entry_reads_the_last() {
    for n in [3, 300] {
        let entries = dict_entries(n);
        let mut image = dict_encode(&entries, &[0, 1]);
        image[8 + n * 16..].fill(0xFF);
        let dict = DictImage::parse(&image, 2).unwrap();
        assert_eq!(dict.cell(1), &entries[n - 1], "{n} entries");
        let mut out = [0u8; 32];
        dict.decode(0, 16, &mut out);
        assert_eq!(out[16..], entries[n - 1], "{n} entries: bulk decode");
    }
}

/// A column of `rows` values whose lengths span `spread`, short and long.
fn seq_values(rows: usize, spread: usize) -> Vec<Vec<u8>> {
    (0..rows)
        .map(|i| {
            let len = match (i % 7, i == rows / 2) {
                (_, true) => 5 + spread - 1,
                (0, _) => 5,
                (k, _) => 5 + (i * k) % spread.min(40),
            };
            (0..len).map(|b| (i + b) as u8).collect()
        })
        .collect()
}

/// `values` as a Seq image over `heap`.
fn seq_image(values: &[Vec<u8>], heap: &mut HeapWriter) -> Vec<u8> {
    let lens = values.iter().map(Vec::len);
    let span = (lens.clone().min().unwrap(), lens.max().unwrap());
    let mut src = Vec::new();
    let cells: Vec<[u8; 16]> = values
        .iter()
        .map(|v| gnitz_wire::encode_german_string(v, &mut src))
        .collect();
    seq_encode(&cells, span, Heap::Plain(&src), heap)
}

#[test]
fn seq_roundtrips_at_every_length_width() {
    // One length takes a byte as thirty do.
    for (spread, bw, rows) in [
        (1, 1, 9),
        (30, 1, 2),
        (30, 1, DECODE_BLOCK_ROWS),
        (300, 2, DECODE_BLOCK_ROWS + 1),
        (70_000, 3, 3 * DECODE_BLOCK_ROWS + 5),
    ] {
        let values = seq_values(rows, spread);
        // The heap a second column packs behind a first.
        let mut heap = HeapWriter::plain();
        heap.push(Heap::Plain(&[0xEE; 77]).value(0, 77).unwrap());
        let image = seq_image(&values, &mut heap);
        let heap = heap.finish().1;
        let lens = values.iter().map(Vec::len);
        let span = (lens.clone().min().unwrap(), lens.clone().max().unwrap());
        let (short, long): (Vec<usize>, Vec<usize>) = lens.partition(|&len| len <= SHORT_STRING_THRESHOLD);
        assert_eq!(
            image.len(),
            seq_image_len(rows, span, short.iter().sum()),
            "{bw}: {rows} rows"
        );
        assert_eq!(
            image.len()
                - short.iter().sum::<usize>()
                - SEQ_BLOCKS_AT
                - rows.div_ceil(DECODE_BLOCK_ROWS) * SEQ_BLOCK_ENTRY,
            for_image_len(rows, bw) + SEQ_POOL_SLACK,
            "{bw}: length width"
        );
        assert_eq!(
            heap.len(),
            77 + long.iter().sum::<usize>(),
            "{bw}: the long values alone reach the heap"
        );

        let seq = SeqImage::parse(&image, rows).unwrap();
        for window in [
            0..rows,
            rows / 3..rows,
            rows - 1..rows,
            DECODE_BLOCK_ROWS.min(rows)..rows,
            2..2,
        ] {
            let mut out = vec![0xAAu8; window.len() * 16];
            seq.decode(Heap::Plain(&heap), window.start, &mut out);
            for (cell, want) in out.as_chunks::<16>().0.iter().zip(&values[window.clone()]) {
                assert!(gnitz_wire::german_string_cell_ok(cell, &heap), "{bw}: a canonical cell");
                assert_eq!(
                    gnitz_wire::german_string_content(cell, &heap),
                    want,
                    "{bw}: rows {window:?}"
                );
            }
        }
    }
}

#[test]
fn seq_parse_refuses_a_size_its_header_does_not_give() {
    let values = seq_values(DECODE_BLOCK_ROWS + 3, 30);
    let image = seq_image(&values, &mut HeapWriter::plain());
    assert!(SeqImage::parse(&image, values.len()).is_some());
    for rows in [values.len() - 1, values.len() + 1, DECODE_BLOCK_ROWS] {
        assert!(SeqImage::parse(&image, rows).is_none(), "{rows} rows");
    }
    assert!(SeqImage::parse(&image[..image.len() - 1], values.len()).is_none());
    assert!(SeqImage::parse(&image[..SEQ_BLOCKS_AT - 1], values.len()).is_none());
    let mut forged = image.clone();
    forged[..SEQ_BLOCKS_AT].copy_from_slice(&(image.len() as u64).to_le_bytes());
    assert!(
        SeqImage::parse(&forged, values.len()).is_none(),
        "a pool larger than the image"
    );
}

/// The lengths and block entries are body bytes no open verifies.
#[test]
fn a_seq_value_past_its_heap_or_pool_reads_empty() {
    let values = seq_values(40, 30);
    let mut heap = HeapWriter::plain();
    let image = seq_image(&values, &mut heap);
    let heap = heap.finish().1;
    let mut forged = image.clone();
    forged[SEQ_BLOCKS_AT..SEQ_BLOCKS_AT + SEQ_BLOCK_ENTRY].fill(0xFF);
    let seq = SeqImage::parse(&forged, values.len()).unwrap();
    let mut out = vec![0xAAu8; values.len() * 16];
    seq.decode(Heap::Plain(&heap), 0, &mut out);
    assert_eq!(out, vec![0u8; out.len()]);
    // A heap shorter than the lengths name.
    let seq = SeqImage::parse(&image, values.len()).unwrap();
    seq.decode(Heap::Plain(&heap[..heap.len() / 2]), 0, &mut out);
    for (cell, want) in out.as_chunks::<16>().0.iter().zip(&values) {
        let got = gnitz_wire::german_string_content(cell, &heap);
        assert!(got == want.as_slice() || got.is_empty());
    }
}

/// A region of `rows` `width`-byte cells, NULL — and zero — wherever `is_null`
/// says, and holding `value(row)` elsewhere.
fn sparse_region(rows: usize, width: usize, is_null: impl Fn(usize) -> bool, value: impl Fn(usize) -> u128) -> Vec<u8> {
    (0..rows)
        .flat_map(|row| {
            let cell = if is_null(row) { 0 } else { value(row) };
            cell.to_le_bytes()[..width].to_vec()
        })
        .collect()
}

/// One bit per row of `rows`, set where `is_null` says the row holds a value.
fn held_rows(rows: usize, is_null: impl Fn(usize) -> bool) -> Vec<u64> {
    let mut held = vec![0u64; rows.div_ceil(64)];
    (0..rows)
        .filter(|&row| !is_null(row))
        .for_each(|row| held[row / 64] |= 1 << (row % 64));
    held
}

/// Decode rows `first_row..` of `sparse` under null words whose bit 3 is `is_null`'s.
fn sparse_decode(
    sparse: &SparseImage,
    first_row: usize,
    width: usize,
    out: &mut [u8],
    is_null: impl Fn(usize) -> bool,
) {
    sparse.decode(first_row, width, out, 3, |rows| {
        PooledBuf(
            rows.flat_map(|row| ((is_null(row) as u64) << 3 | 0b10111).to_le_bytes())
                .collect(),
        )
    });
}

#[test]
fn sparse_roundtrips_framed_and_unframed() {
    let is_null = |row: usize| row % 11 != 3 && row % 700 != 1;
    type Value = fn(usize) -> u128;
    // A cell width, its integer type, a row's value and the width a frame takes them at.
    let cases: [(usize, Option<FixedInt>, Value, usize); 4] = [
        // Values a frame of two bytes spans, though the zero of a NULL cell lies far below it.
        (8, Some(FixedInt::I64), |row| 1_700_000_000_000 + row as u128 * 7, 2),
        (4, Some(FixedInt::U32), |row| row as u128 % 200, 1),
        // No frame narrower than the cell, and a type no frame admits.
        (
            8,
            Some(FixedInt::U64),
            |row| ((row as u128 + 1) * 0x0123_4567_89AB_CDEF) & u64::MAX as u128,
            8,
        ),
        (16, None, |row| (row as u128 + 1).wrapping_mul(u128::MAX / 977), 16),
    ];
    for (width, fi, value, bw) in cases {
        for rows in [1, 40, DECODE_BLOCK_ROWS, 3 * DECODE_BLOCK_ROWS + 5] {
            let src = sparse_region(rows, width, is_null, value);
            let held: Vec<u128> = (0..rows).filter(|&row| !is_null(row)).map(value).collect();
            let image = sparse_encode(&src, width, fi, &held_rows(rows, is_null));
            let values = image.len() - SPARSE_RANKS_AT - rows.div_ceil(DECODE_BLOCK_ROWS) * SPARSE_RANK_ENTRY;
            let framed_at = fi.and_then(|fi| expected_bw(&held.iter().map(|&v| v as i128).collect::<Vec<_>>(), fi));
            assert_eq!(
                values,
                framed_at.map_or(held.len() * width, |bw| for_image_len(held.len(), bw)),
                "{width}: {rows} rows"
            );
            if rows > DECODE_BLOCK_ROWS {
                assert_eq!(framed_at.unwrap_or(width), bw, "{width}: {rows} rows frame");
            }

            let sparse = SparseImage::parse(&image, rows, width).unwrap();
            for window in [
                0..rows,
                rows / 3..rows,
                rows - 1..rows,
                DECODE_BLOCK_ROWS.min(rows)..rows,
                0..0,
            ] {
                let mut out = vec![0xAAu8; window.len() * width];
                sparse_decode(&sparse, window.start, width, &mut out, is_null);
                assert_eq!(
                    out,
                    src[window.start * width..window.end * width],
                    "{width}: rows {window:?}"
                );
            }
        }
    }
}

/// The image from the held bits of a null region is the one a row-by-row walk
/// of its null words writes: a rank per block, then the non-NULL cells.
#[test]
fn sparse_encode_from_null_words_matches_a_row_walk() {
    let walk = |src: &[u8], width: usize, fi: Option<FixedInt>, is_null: &dyn Fn(usize) -> bool| {
        let n = src.len() / width;
        let mut image = vec![0u8; SPARSE_RANKS_AT + n.div_ceil(DECODE_BLOCK_ROWS) * SPARSE_RANK_ENTRY];
        let mut values = Vec::new();
        for (row, cell) in src.chunks_exact(width).enumerate() {
            if row % DECODE_BLOCK_ROWS == 0 {
                let entry = SPARSE_RANKS_AT + row / DECODE_BLOCK_ROWS * SPARSE_RANK_ENTRY;
                write_u32_le(&mut image, entry, (values.len() / width) as u32);
            }
            if !is_null(row) {
                values.extend_from_slice(cell);
            }
        }
        write_u64_le(&mut image, 0, (values.len() / width) as u64);
        let framed = fi.and_then(|fi| for_encode(&values, fi));
        image.extend_from_slice(framed.as_ref().unwrap_or(&values));
        image
    };
    let shapes: [fn(usize) -> bool; 4] = [|_| false, |_| true, |row| row % 7 != 2, |row| (row / 300) % 2 == 0];
    for (width, fi) in [
        (1, Some(FixedInt::I8)),
        (2, Some(FixedInt::U16)),
        (4, Some(FixedInt::I32)),
        (8, Some(FixedInt::I64)),
        (8, None),
        (16, None),
    ] {
        for rows in [0, 1, 63, 64, 65, DECODE_BLOCK_ROWS, 5 * DECODE_BLOCK_ROWS + 77] {
            for is_null in shapes {
                let src = sparse_region(rows, width, is_null, |row| row as u128 * 0x0101_0101 + 5);
                // Payload column 5 of null words whose other bits are set.
                let words: Vec<u8> = (0..rows)
                    .flat_map(|row| ((is_null(row) as u64) << 5 | 0xFFFF_0000_0000_001F).to_le_bytes())
                    .collect();
                let mut held = vec![0u64; rows.div_ceil(64)];
                held_bits(gnitz_expr::Level::new(), &words, 5, &mut held);
                assert_eq!(held, held_rows(rows, is_null), "{width}: {rows} rows");
                assert_eq!(
                    sparse_encode(&src, width, fi, &held),
                    walk(&src, width, fi, &is_null),
                    "{width}: {rows} rows"
                );
            }
        }
    }
}

/// A frame the size of the cells it would replace is not taken, so that a
/// parse tells the two apart by size alone.
#[test]
fn sparse_keeps_cells_a_frame_would_not_shrink() {
    // `held` values spanning `bw` bytes: a frame of them takes `15 + held * bw`.
    for (width, fi, held, bw) in [
        (8, FixedInt::U64, 15, 7),
        (8, FixedInt::U64, 5, 5),
        (8, FixedInt::U64, 3, 3),
        (4, FixedInt::U32, 5, 1),
        // One value more, and the frame is the smaller.
        (8, FixedInt::U64, 16, 7),
        (4, FixedInt::U32, 6, 1),
    ] {
        let rows = 3 * held;
        let is_null = |row: usize| row % 3 != 1;
        let top = (1u128 << (8 * bw)) - 1;
        let src = sparse_region(rows, width, is_null, |row| if row == 1 { top } else { row as u128 });
        let image = sparse_encode(&src, width, Some(fi), &held_rows(rows, is_null));
        let values = image.len() - SPARSE_RANKS_AT - SPARSE_RANK_ENTRY;
        let framed = for_image_len(held, bw) < held * width;
        assert_eq!(
            values,
            if framed { for_image_len(held, bw) } else { held * width },
            "{held} values of {bw} bytes"
        );
        let sparse = SparseImage::parse(&image, rows, width).unwrap();
        let mut out = vec![0xAAu8; src.len()];
        sparse_decode(&sparse, 0, width, &mut out, is_null);
        assert_eq!(out, src, "{held} values of {bw} bytes");
    }
}

#[test]
fn sparse_parse_refuses_a_size_its_header_does_not_give() {
    let is_null = |row: usize| !row.is_multiple_of(5);
    let src = sparse_region(DECODE_BLOCK_ROWS + 3, 8, is_null, |row| 1000 + row as u128);
    let rows = src.len() / 8;
    let image = sparse_encode(&src, 8, Some(FixedInt::I64), &held_rows(src.len() / 8, is_null));
    assert!(SparseImage::parse(&image, rows, 8).is_some());
    assert!(
        SparseImage::parse(&image, DECODE_BLOCK_ROWS, 8).is_none(),
        "a block entry too many"
    );
    for width in [1, 2] {
        assert!(
            SparseImage::parse(&image, rows, width).is_none(),
            "offsets no narrower than the cell"
        );
    }
    assert!(SparseImage::parse(&image[..image.len() - 1], rows, 8).is_none());
    assert!(SparseImage::parse(&image[..SPARSE_RANKS_AT - 1], rows, 8).is_none());
    let mut forged = image.clone();
    forged[..4].copy_from_slice(&(rows as u32 + 1).to_le_bytes());
    assert!(SparseImage::parse(&forged, rows, 8).is_none(), "more values than rows");
}

/// The null words and the block ranks are body bytes no open verifies.
#[test]
fn a_sparse_row_past_the_last_value_reads_zero() {
    let is_null = |row: usize| row % 2 == 1;
    let src = sparse_region(600, 8, is_null, |row| 1000 + row as u128);
    for fi in [Some(FixedInt::I64), None] {
        let image = sparse_encode(&src, 8, fi, &held_rows(600, is_null));
        let sparse = SparseImage::parse(&image, 600, 8).unwrap();
        let mut out = vec![0xAAu8; src.len()];
        // Null words that name twice the values the image holds.
        sparse_decode(&sparse, 0, 8, &mut out, |_| false);
        assert_eq!(
            out[..300 * 8],
            sparse_region(300, 8, |_| false, |row| 1000 + 2 * row as u128)
        );
        assert_eq!(out[300 * 8..], vec![0u8; 300 * 8]);
        let mut forged = image.clone();
        forged[SPARSE_RANKS_AT + SPARSE_RANK_ENTRY..][..SPARSE_RANK_ENTRY].fill(0xFF);
        let sparse = SparseImage::parse(&forged, 600, 8).unwrap();
        sparse_decode(&sparse, DECODE_BLOCK_ROWS, 8, &mut out[..88 * 8], is_null);
        assert_eq!(out[..88 * 8], vec![0u8; 88 * 8]);
    }
}

/// A sorted PK region under `types`: row `i`'s column `c` holds `value(i, c)`.
pub(super) fn key_region(
    types: &[TypeCode],
    rows: usize,
    value: impl Fn(usize, usize) -> u128,
) -> (SchemaDescriptor, Vec<u8>) {
    let schema = pk_only_schema(types);
    let mut keys: Vec<Vec<u8>> = (0..rows)
        .map(|i| {
            let natives: Vec<u128> = (0..types.len()).map(|c| value(i, c)).collect();
            opk_pk(&schema, &natives)
        })
        .collect();
    keys.sort();
    (schema, keys.concat())
}

/// A 300-row (U16, U64) key region whose first field takes nine values.
fn two_field_key() -> (SchemaDescriptor, Vec<u8>) {
    key_region(&[TypeCode::U16, TypeCode::U64], 300, |i, c| {
        [(i % 9) as u128, i as u128][c]
    })
}

/// The first row of `raw` at or after `key`.
fn raw_lower_bound(raw: &[u8], stride: usize, key: &[u8]) -> usize {
    let rows = raw.chunks_exact(stride);
    let n = rows.len();
    rows.into_iter().position(|row| row >= key).unwrap_or(n)
}

/// `key` and, per field, `key` with that field a step below and above itself,
/// at its least and its greatest, and at random.
fn key_probes(fields: &KeyLayout, key: &[u8], rng: &mut Rng) -> Vec<Vec<u8>> {
    let mut probes = vec![key.to_vec()];
    for &KeyField { at, width: w, .. } in fields.fields() {
        let field = gnitz_wire::widen_pk_be(&key[at..at + w]) as u64;
        for v in [
            field.wrapping_sub(1),
            field.wrapping_add(1),
            0,
            u64::MAX,
            rng.next_u64(),
        ] {
            let mut probe = key.to_vec();
            probe[at..at + w].copy_from_slice(&v.to_be_bytes()[8 - w..]);
            probes.push(probe);
        }
    }
    probes.push((0..key.len()).map(|_| rng.next_u64() as u8).collect());
    probes
}

#[test]
fn key_roundtrips_and_seeks_as_the_region_it_packs() {
    use TypeCode::*;
    type Value = fn(usize, usize) -> u128;
    // A few values per column, whatever the columns.
    const SMALL: Value = |i, c| ((i * [3, 5, 7, 11, 13][c]) % [50, 7, 1000, 9, 300][c]) as u128;
    const FEW: Value = |i, c| (i * (c + 3) % 5) as u128;
    // Each case's packed row width at 1500 rows, or `None` where the encoder declines.
    let cases: Vec<(&str, Vec<TypeCode>, Value, Option<usize>)> = vec![
        ("a narrow u64", vec![U64], |i, _| 1000 + (i as u128 * 7) % 5000, Some(2)),
        (
            "an i64 across zero",
            vec![I64],
            |i, _| (i as i128 % 600 - 300) as u128,
            Some(2),
        ),
        (
            "u32, i16, u8",
            vec![U32, I16, U8],
            |i, c| [(i % 1000) as u128, (i as i128 % 100 - 50) as u128, (i % 4) as u128][c],
            Some(3),
        ),
        (
            "two u64 of forty bits: a row past one u64",
            vec![U64, U64],
            |i, c| (mix(i as u64) >> (24 - c)) as u128 | 1 << 39,
            Some(10),
        ),
        (
            "a u128 around the u64 boundary",
            vec![U128],
            |i, _| (1u128 << 64) - 700 + i as u128,
            Some(9),
        ),
        (
            "a constant leading column",
            vec![U64, U64],
            |i, c| [7, (i as u128 * 31) % 100_000][c],
            Some(2),
        ),
        (
            "five fields",
            vec![U8, U16, U32, U64, I64],
            |i, c| {
                [
                    (i % 3) as u128,
                    (i % 50) as u128,
                    (i % 7) as u128,
                    9,
                    (i as i128 - 750) as u128,
                ][c]
            },
            Some(3),
        ),
        ("four fields", vec![U32, U32, U16, U64], SMALL, Some(3)),
        ("six fields", vec![U128, U128, U128], SMALL, Some(3)),
        ("seven fields", vec![U128, U128, U128, U8], SMALL, Some(3)),
        ("eight fields", vec![U128, U128, U128, U128], SMALL, Some(3)),
        ("nine fields", vec![U128, U128, U128, U128, U16], SMALL, Some(4)),
        ("ten fields", vec![U128, U128, U128, U128, U128], SMALL, Some(4)),
        ("a full-width u64", vec![U64], |i, _| mix(i as u64) as u128, None),
        (
            "three full-width u64: a row past one u128",
            vec![U64, U64, U64],
            |i, c| mix(i as u64 + c as u64) as u128,
            None,
        ),
        ("one key", vec![U64, I32], |_, c| [5, 6][c], None),
        // Fields that end at their row's end: the wide loads and stores of the
        // last rows are the ones that could leave the region.
        ("u64, u16", vec![U64, U16], FEW, Some(1)),
        ("u8, u8", vec![U8, U8], FEW, Some(1)),
        ("u128, u8", vec![U128, U8], FEW, Some(1)),
    ];
    let mut rng = Rng::new(0x004B_4559);
    for (label, types, value, packed_stride) in cases {
        for n in [2, 3, 9, 20, DECODE_BLOCK_ROWS + 1, 1500] {
            let (schema, raw) = key_region(&types, n, value);
            let (fields, stride) = (KeyLayout::of(&schema), schema.pk_stride());
            let Some(image) = key_encode(&raw, &schema) else {
                assert!(n < 1500 || packed_stride.is_none(), "{label}: declined at {n} rows");
                continue;
            };
            assert!(image.len() < raw.len(), "{label}: no larger than the region");
            let key = KeyImage::parse(&image, n, &schema).expect("the encoder's image parses");
            if n == 1500 {
                assert_eq!(Some(key.stride), packed_stride, "{label}: packed row width");
            }
            assert!(
                KeyImage::parse(&image, n + 1, &schema).is_none(),
                "{label}: another row count"
            );
            assert!(
                KeyImage::parse(&image, n - 1, &schema).is_none(),
                "{label}: another row count"
            );

            let mut decoded = vec![0u8; raw.len()];
            key.decode(0, &mut decoded);
            assert!(decoded == raw, "{label}: byte-exact roundtrip at {n} rows");
            let window = n / 3..n - n / 4;
            let mut decoded = vec![0u8; window.len() * stride];
            key.decode(window.start, &mut decoded);
            assert!(
                decoded == raw[window.start * stride..window.end * stride],
                "{label}: a window decodes alone"
            );
            key.decode(n, &mut []);

            for row in (0..n).step_by(n / 60 + 1).chain([n - 1]) {
                for probe in key_probes(&fields, &raw[row * stride..][..stride], &mut rng) {
                    let want = raw_lower_bound(&raw, stride, &probe);
                    assert_eq!(key.seek(&probe, None), want, "{label}: lower bound of {probe:02x?}");
                    let held = raw.chunks_exact(stride).filter(|row| *row == probe).count();
                    assert_eq!(key.rows_of(&probe), want..want + held, "{label}: rows of {probe:02x?}");
                    for hint in [
                        0,
                        want,
                        want.saturating_sub(3),
                        (want + 5).min(n),
                        n,
                        rng.gen_range(n as u64 + 1) as usize,
                    ] {
                        assert_eq!(key.seek(&probe, Some(hint)), want, "{label}: {probe:02x?} from {hint}");
                    }
                }
            }
        }
    }
}

#[test]
fn key_parse_refuses_widths_its_fields_do_not_hold() {
    let (schema, raw) = two_field_key();
    let fields = KeyLayout::of(&schema);
    let image = key_encode(&raw, &schema).unwrap();
    let bits_at = fields.fields().len() * 8;
    assert_eq!(image[bits_at..bits_at + 2], [4, 9], "the fields' bit widths");
    // Widths their fields hold at the row's size parse, as any body bytes do;
    // a width past its field or another row size does not.
    for (forged, parses) in [
        ([16, 0], true),
        ([0, 13], true),
        ([17, 0], false),
        ([4, 65], false),
        ([4, 17], false),
    ] {
        let mut image = image.clone();
        image[bits_at..bits_at + 2].copy_from_slice(&forged);
        let parsed = KeyImage::parse(&image, 300, &schema);
        assert_eq!(parsed.is_some(), parses, "bit widths {forged:?}");
    }
    assert!(
        KeyImage::parse(&image[..bits_at], 300, &schema).is_none(),
        "shorter than its header"
    );
    assert!(
        KeyImage::parse(&image, 300, &pk_only_schema(&[TypeCode::U64])).is_none(),
        "another PK's fields"
    );
}

#[test]
fn a_key_minimum_past_its_field_wraps() {
    let (schema, raw) = two_field_key();
    let mut image = key_encode(&raw, &schema).unwrap();
    image[..16].fill(0xFF);
    let key = KeyImage::parse(&image, 300, &schema).unwrap();
    let mut decoded = vec![0u8; raw.len()];
    key.decode(0, &mut decoded);
    // Row 0 held each field's minimum, now all ones: every field reads one less than it was.
    assert_eq!(decoded[..10], [0xFF; 10]);
    let field = |rows: &[u8]| gnitz_wire::widen_pk_be(&rows[299 * 10 + 2..]);
    assert_eq!(field(&decoded), field(&raw) - 1);
    assert_eq!(key.seek(&raw[..10], None), 0, "below every forged minimum");
}
