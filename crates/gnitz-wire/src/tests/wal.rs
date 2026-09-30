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

/// A zero-row block's heap is dropped: no cell can reference it.
#[test]
fn a_zero_row_blocks_heap_is_dropped() {
    let buf = encode_dead(&[&[], &[], &[], b"orphan heap"], 3);
    assert_eq!(parse_block(&buf, ROW_WIDTH), Ok((0, &[][..], &[][..], 0)));
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
        cases.push((format!("ROWS bit {bit}"), flip(WAL_OFF_ROWS), Some(mismatch)));
        cases.push((format!("HEAP_LEN bit {bit}"), flip(WAL_OFF_HEAP_LEN), Some(mismatch)));
        // A dead-byte bound within the heap is a legitimate claim.
        let dead_err = ((1usize << bit) > heap_len).then_some("block declares more dead heap than heap");
        cases.push((format!("HEAP_DEAD bit {bit}"), flip(WAL_OFF_HEAP_DEAD), dead_err));
    }
    for (what, block, want) in &cases {
        assert_eq!(parse_block(block, ROW_WIDTH).err(), *want, "{what}");
    }
}
