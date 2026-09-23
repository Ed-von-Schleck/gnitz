use super::*;

/// The narrowest real row: a 1-byte PK, a weight word and a null word.
const ROW_WIDTH: usize = 1 + 8 + 8;
const ROWS: usize = 2;

/// Two rows' region list, heap last.
fn fixture() -> Vec<Vec<u8>> {
    vec![vec![7; ROWS], vec![1; ROWS * 8], vec![0; ROWS * 8], b"a heap".to_vec()]
}

fn slices(regions: &[Vec<u8>]) -> Vec<&[u8]> {
    regions.iter().map(Vec::as_slice).collect()
}

/// Frame `regions` into a fresh buffer, as a SAL slot would.
fn encode_dead(regions: &[&[u8]], heap_dead: usize) -> Vec<u8> {
    let mut buf = vec![0u8; block_size(regions)];
    assert_eq!(write_block(regions, heap_dead, &mut buf), buf.len());
    buf
}

/// Rows, fixed regions, heap and dead-byte bound come back as written, up to a
/// wholly dead heap; the appending framer writes the same bytes as the in-place
/// one; and a second block appends behind the first and is recovered by its own
/// `SIZE` field.
#[test]
fn a_block_round_trips_through_both_framers() {
    let f = fixture();
    let regions = slices(&f);
    let (heap, fixed_regions) = regions.split_last().unwrap();
    for dead in [0, 1, heap.len()] {
        let buf = encode_dead(&regions, dead);
        let mut appended = vec![0xAB];
        append_block(&regions, dead, &mut appended);
        assert_eq!(appended[1..], buf, "dead={dead}");
        assert_eq!(
            parse_block(&buf, ROW_WIDTH),
            Ok((ROWS, &fixed_regions.concat()[..], *heap, dead)),
            "dead={dead}"
        );
    }

    let mut no_heap = regions.clone();
    *no_heap.last_mut().unwrap() = &[];
    let empty: [&[u8]; 4] = [&[], &[], &[], &[]];
    for list in [&no_heap[..], &empty[..]] {
        let mut appended = Vec::new();
        append_block(list, 0, &mut appended);
        assert_eq!(appended, encode_dead(list, 0), "{} regions", list.len());
    }

    let mut two = encode_dead(&regions, 0);
    let end1 = two.len();
    two.extend_from_slice(&encode_dead(&regions, 0));
    for off in [0, end1] {
        let block = block_slice(&two[off..]).expect("a framed block");
        assert_eq!(parse_block(block, ROW_WIDTH).unwrap().0, ROWS);
    }
}

/// A block holds at least one row: an empty delta ships no block.
#[test]
fn a_zero_row_block_is_refused() {
    for heap in [&b""[..], b"orphan heap"] {
        let buf = encode_dead(&[&[], &[], &[], heap], 0);
        assert_eq!(parse_block(&buf, ROW_WIDTH), Err("block holds no rows"));
    }
}

/// Every guard, against the block that trips it — including every one-bit
/// forgery of every header word, and of `ROWS` by one row.
#[test]
fn each_guard_rejects_its_forgery() {
    let clean = encode_dead(&slices(&fixture()), 0);
    let forged = |off: usize, f: &dyn Fn(u32) -> u32| {
        let mut buf = clean.clone();
        let v = f(read_u32_le(&buf, off));
        write_u32_le(&mut buf, off, v);
        buf
    };
    let mismatch = "block size does not match its rows and heap";

    let heap_len = b"a heap".len();
    let mut cases: Vec<(String, Vec<u8>, Option<&str>)> = vec![
        (
            "short of a header".into(),
            clean[..WAL_HEADER_SIZE - 1].to_vec(),
            Some("block shorter than header"),
        ),
        (
            "size past the buffer".into(),
            clean[..clean.len() - 1].to_vec(),
            Some("declared size past buffer"),
        ),
        ("one row fewer".into(), forged(WAL_OFF_ROWS, &|r| r - 1), Some(mismatch)),
    ];
    for bit in 0..32 {
        let flip = |off| forged(off, &|v| v ^ (1 << bit));
        let block = flip(WAL_OFF_SIZE);
        let size_err = if read_u32_le(&block, WAL_OFF_SIZE) as usize > clean.len() {
            "declared size past buffer"
        } else {
            mismatch
        };
        cases.push((format!("SIZE bit {bit}"), block, Some(size_err)));
        cases.push((
            format!("VERSION bit {bit}"),
            flip(WAL_OFF_VERSION),
            Some("unknown block version"),
        ));
        let rows_err = if ROWS ^ (1 << bit) == 0 {
            "block holds no rows"
        } else {
            mismatch
        };
        cases.push((format!("ROWS bit {bit}"), flip(WAL_OFF_ROWS), Some(rows_err)));
        cases.push((format!("HEAP_LEN bit {bit}"), flip(WAL_OFF_HEAP_LEN), Some(mismatch)));
        // A dead-byte bound within the heap is a legitimate claim.
        let dead_err = ((1usize << bit) > heap_len).then_some("block declares more dead heap than heap");
        cases.push((format!("HEAP_DEAD bit {bit}"), flip(WAL_OFF_HEAP_DEAD), dead_err));
    }
    for (what, block, want) in &cases {
        assert_eq!(parse_block(block, ROW_WIDTH).err(), *want, "{what}");
    }
}

/// The block layout, byte for byte: what a change to the header's words, a
/// German cell's fields, the OPK byte order or a region's place would move
/// without moving [`WAL_FORMAT_VERSION`].
#[test]
fn the_block_layout_is_pinned_byte_for_byte() {
    // A compound (I16, U8) PK, an I32 column and a STRING column.
    const ROW: usize = 3 + 8 + 8 + 4 + 16;
    const LONG: &[u8] = b"a value past the inline cell";

    let mut blob = Vec::new();
    let short = crate::encode_german_string(b"ab", &mut blob);
    let long = crate::encode_german_string(LONG, &mut blob);
    let mut pk = Vec::new();
    for (k, u) in [(-2i16, 3u8), (258, 6)] {
        crate::push_opk(&mut pk, 2, k as u16 as u128, true);
        crate::push_opk(&mut pk, 1, u as u128, false);
    }
    let regions: [&[u8]; 6] = [
        &pk,
        &[1i64.to_le_bytes(), (-2i64).to_le_bytes()].concat(),
        // Row 1 is NULL in payload slot 0.
        &[0u64.to_le_bytes(), 1u64.to_le_bytes()].concat(),
        &[7i32.to_le_bytes(), [0; 4]].concat(),
        &[short, long].concat(),
        &blob,
    ];
    let block = encode_dead(&regions, 0);

    let version = WAL_FORMAT_VERSION.to_le_bytes();
    #[rustfmt::skip]
    let want: &[u8] = &[
        // Header: SIZE, VERSION, ROWS, HEAP_LEN, HEAP_DEAD, each u32 LE.
        (20 + 2 * ROW + LONG.len()) as u8, 0, 0, 0,
        version[0], version[1], version[2], version[3],
        2, 0, 0, 0,
        LONG.len() as u8, 0, 0, 0,
        0, 0, 0, 0,
        // PK: the I16 sign-flipped and big-endian, then the U8.
        0x7F, 0xFE, 0x03, 0x81, 0x02, 0x06,
        // Weights.
        1, 0, 0, 0, 0, 0, 0, 0,
        0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        // Null words.
        0, 0, 0, 0, 0, 0, 0, 0,
        1, 0, 0, 0, 0, 0, 0, 0,
        // The I32 column.
        7, 0, 0, 0, 0, 0, 0, 0,
        // The STRING column: length, prefix, then the inline suffix or the heap offset.
        2, 0, 0, 0, b'a', b'b', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        LONG.len() as u8, 0, 0, 0, b'a', b' ', b'v', b'a', 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    assert_eq!(block, [want, LONG].concat(), "the block layout moved: bump `WAL_EPOCH`");
    assert_eq!(parse_block(&block, ROW).map(|(rows, ..)| rows), Ok(2));
}
