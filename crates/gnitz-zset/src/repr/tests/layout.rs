use super::*;
use crate::test_support::Rng;
use gnitz_wire::FixedInt;

/// Pack signed/unsigned values into a raw `fi.width()`-byte-per-cell LE
/// region, exactly as the payload region would appear on the batch.
fn pack(vals: &[i128], fi: FixedInt) -> Vec<u8> {
    let width = fi.width();
    let mut b = Vec::with_capacity(vals.len() * width);
    for &v in vals {
        let le = (v as i64 as u64).to_le_bytes();
        b.extend_from_slice(&le[..width]);
    }
    b
}

/// Encode → recover `bw` from the image geometry → decode → assert
/// byte-exact reproduction of the input region. Returns the chosen `bw`
/// (None ⇒ region stays Raw).
fn roundtrip(vals: &[i128], fi: FixedInt) -> Option<usize> {
    let raw = pack(vals, fi);
    let n = vals.len();
    let image = for_encode(&raw, fi)?;
    let bw = for_image_bw(image.len(), n, fi.width()).expect("the encoder's image has FoR geometry");
    let mut decoded = vec![0u8; n * fi.width()];
    for_decode(&image, bw, fi.width(), 0, &mut decoded);
    assert_eq!(decoded, raw, "byte-exact roundtrip (bw={bw}, {fi:?})");
    if n >= 10 {
        let w = fi.width();
        let mut window = vec![0u8; 7 * w];
        for_decode(&image, bw, w, 3, &mut window);
        assert_eq!(
            window,
            raw[3 * w..10 * w],
            "rows [3, 10) decode alone (bw={bw}, {fi:?})"
        );
    }
    Some(bw)
}

/// The offset width `for_encode` must choose: the bytes `vals`' span needs, if
/// packing at that width drops an aligned block of the raw region.
fn expected_bw(vals: &[i128], fi: FixedInt) -> Option<usize> {
    let span = vals.iter().max()? - vals.iter().min()?;
    let bw = (128 - span.leading_zeros() as usize).div_ceil(8);
    (bw > 0 && region_start(for_image_len(vals.len(), bw)) < region_start(vals.len() * fi.width())).then_some(bw)
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
        ("small u64", series(300, |i| i % 200), U64, Some(1)),
        ("two-byte span", series(300, |i| (i * 211) % 60000), U32, Some(2)),
        // A tight range far from zero frames on its min.
        ("high floor", series(256, |i| 1_000_000 + i % 50), U32, Some(1)),
        ("high floor u64", series(256, |i| 5_000_000_000 + i % 40), U64, Some(1)),
        // A range spanning zero frames on the signed min, not on 0.
        ("across zero", series(256, |i| -100 + i % 150), I32, Some(1)),
        ("across zero i64", series(256, |i| -100 + i % 150), I64, Some(1)),
        ("wide across zero", series(500, |i| -30_000 + i % 60000), I32, Some(2)),
        // Constant is claimed before FoR; the encoder declines too.
        ("all equal", vec![42; 100], U32, None),
        ("extremes", vec![0, u32::MAX as i128], U32, None),
        ("extremes signed", vec![i32::MIN as i128, i32::MAX as i128], I32, None),
        ("extremes u64", vec![0, u64::MAX as i128], U64, None),
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
        // Raw 40 B, packed 8 + 10·2 + 7 B: both align to 64, so nothing is saved.
        ("aligned footprints tie", series(10, |i| i * 1000), U32, None),
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
    use FixedInt::*;
    let mut rng = Rng::new(0x9E3779B97F4A7C15);
    let (mut packed, mut declined) = (0, 0);
    for fi in [U8, I8, U16, I16, U32, I32, U64, I64] {
        let bits = 8 * fi.width() as u32;
        let signed = matches!(fi, I8 | I16 | I32 | I64);
        let (lo, size) = (if signed { -(1i128 << (bits - 1)) } else { 0 }, 1u128 << bits);
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

/// FoR decode-throughput + compression-ratio microbench. Run in release:
/// `cargo test -p gnitz-zset --release for_decode_bench --
///   --ignored --nocapture --test-threads=1`
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn for_decode_bench() {
    use std::hint::black_box;

    // A representative re-keyed ex-PK column: 1M I64 rows over a narrow
    // range far from zero (the `_int_`/`_hist_`/`_reduce_` shape).
    let n = 1_000_000usize;
    let vals: Vec<i128> = (0..n).map(|i| 3_000_000_000i128 + (i % 4000) as i128).collect();
    let raw = pack(&vals, FixedInt::I64);
    let image = for_encode(&raw, FixedInt::I64).expect("region must pack");
    let bw = for_image_bw(image.len(), n, 8).expect("FoR geometry");

    let raw_bytes = (n * 8).next_multiple_of(ALIGNMENT);
    let packed_bytes = for_image_len(n, bw).next_multiple_of(ALIGNMENT);
    println!(
        "FoR ratio: {n} I64 rows, bw={bw}, raw(aligned)={raw_bytes}B packed(aligned)={packed_bytes}B \
         ({:.2}x)",
        raw_bytes as f64 / packed_bytes as f64
    );

    let iters = 200;
    let mut decoded = vec![0u8; n * 8];
    let elapsed = crate::test_support::bench_time(iters, || {
        for_decode(black_box(&image), bw, 8, 0, &mut decoded);
        black_box(&decoded);
    });
    let vals_per_s = (n as f64 * iters as f64) / elapsed.as_secs_f64();
    println!(
        "FoR decode: {:.1} M values/s ({:.2} ms per {n}-row region)",
        vals_per_s / 1e6,
        elapsed.as_secs_f64() * 1e3 / iters as f64
    );
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
        for width in [2, 4, 8, 16] {
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
fn seq_image(values: &[Vec<u8>], heap: &mut Vec<u8>) -> Vec<u8> {
    let lens = values.iter().map(Vec::len);
    let span = (lens.clone().min().unwrap(), lens.max().unwrap());
    seq_encode(values.len(), span, values.iter().map(Vec::as_slice), heap)
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
        let mut heap = vec![0xEE; 77];
        let image = seq_image(&values, &mut heap);
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
            seq.decode(&heap, window.start, &mut out);
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
    let image = seq_image(&values, &mut Vec::new());
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
    let mut heap = Vec::new();
    let image = seq_image(&values, &mut heap);
    let mut forged = image.clone();
    forged[SEQ_BLOCKS_AT..SEQ_BLOCKS_AT + SEQ_BLOCK_ENTRY].fill(0xFF);
    let seq = SeqImage::parse(&forged, values.len()).unwrap();
    let mut out = vec![0xAAu8; values.len() * 16];
    seq.decode(&heap, 0, &mut out);
    assert_eq!(out, vec![0u8; out.len()]);
    // A heap shorter than the lengths name.
    let seq = SeqImage::parse(&image, values.len()).unwrap();
    seq.decode(&heap[..heap.len() / 2], 0, &mut out);
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

/// Decode rows `first_row..` of `sparse` under null words whose bit 3 is `is_null`'s.
fn sparse_decode(
    sparse: &SparseImage,
    first_row: usize,
    width: usize,
    out: &mut [u8],
    is_null: impl Fn(usize) -> bool,
) {
    let from = first_row - first_row % DECODE_BLOCK_ROWS;
    let nulls: Vec<u8> = (from..first_row + out.len() / width)
        .flat_map(|row| ((is_null(row) as u64) << 3 | 0b10111).to_le_bytes())
        .collect();
    sparse.decode(first_row, width, out, &nulls, 3);
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
            let held = (0..rows).filter(|&row| !is_null(row)).count();
            let image = sparse_encode(&src, width, fi, is_null);
            let values = image.len() - SPARSE_RANKS_AT - rows.div_ceil(DECODE_BLOCK_ROWS) * SPARSE_RANK_ENTRY;
            // Too few values for a frame to shrink them stay cells.
            let framed = bw < width && held > 0 && values != held * width;
            assert_eq!(
                values,
                if framed { for_image_len(held, bw) } else { held * width },
                "{width}: {rows} rows"
            );
            assert!(framed || bw == width || rows <= 40, "{width}: {rows} rows frame");

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

#[test]
fn sparse_parse_refuses_a_size_its_header_does_not_give() {
    let is_null = |row: usize| !row.is_multiple_of(5);
    let src = sparse_region(DECODE_BLOCK_ROWS + 3, 8, is_null, |row| 1000 + row as u128);
    let rows = src.len() / 8;
    let image = sparse_encode(&src, 8, Some(FixedInt::I64), is_null);
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
    let forged = |at: usize, v: u32| {
        let mut image = image.clone();
        image[at..at + 4].copy_from_slice(&v.to_le_bytes());
        SparseImage::parse(&image, rows, 8).is_none()
    };
    assert!(forged(0, rows as u32 + 1), "more values than rows");
}

/// The null words and the block ranks are body bytes no open verifies.
#[test]
fn a_sparse_row_past_the_last_value_reads_zero() {
    let is_null = |row: usize| row % 2 == 1;
    let src = sparse_region(600, 8, is_null, |row| 1000 + row as u128);
    for fi in [Some(FixedInt::I64), None] {
        let image = sparse_encode(&src, 8, fi, is_null);
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
