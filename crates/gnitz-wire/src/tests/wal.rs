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
fn encode(rows: usize, regions: &[&[u8]]) -> Vec<u8> {
    let mut buf = vec![0u8; block_size(regions)];
    assert_eq!(write_block(rows, regions, &mut buf), buf.len());
    buf
}

/// The row count, the fixed regions and the heap come back, and a second block
/// appends behind the first and is recovered by its own `SIZE` field.
#[test]
fn parse_block_returns_rows_fixed_bytes_and_heap() {
    let f = fixture();
    let regions = slices(&f);
    let (heap, fixed_regions) = regions.split_last().unwrap();

    let mut buf = encode(ROWS, &regions);
    assert_eq!(buf.len(), WAL_HEADER_SIZE + ROWS * ROW_WIDTH + heap.len());
    let (rows, fixed, got_heap) = parse_block(&buf, ROW_WIDTH).unwrap();
    assert_eq!(rows, ROWS);
    assert_eq!(fixed, fixed_regions.concat());
    assert_eq!(got_heap, *heap);

    let end1 = buf.len();
    buf.extend_from_slice(&encode(ROWS, &regions));
    for off in [0, end1] {
        let block = block_slice_at(&buf, off).expect("a framed block");
        assert_eq!(parse_block(block, ROW_WIDTH).unwrap().0, ROWS);
    }
}

/// The appending and the in-place framer produce the same bytes.
#[test]
fn append_block_matches_write_block() {
    let f = fixture();
    let with_heap = slices(&f);
    let mut no_heap = with_heap.clone();
    *no_heap.last_mut().unwrap() = &[];
    for (rows, list) in [(ROWS, &with_heap[..]), (ROWS, &no_heap[..]), (0, &[&[][..]][..])] {
        let mut appended = vec![0xAB];
        append_block(rows, list, &mut appended);
        assert_eq!(appended[1..], encode(rows, list), "{} regions", list.len());
    }
}

/// A zero-row block's heap is dropped: no cell can reference it.
#[test]
fn a_zero_row_blocks_heap_is_dropped() {
    let buf = encode(0, &[b"orphan heap"]);
    let (rows, fixed, heap) = parse_block(&buf, ROW_WIDTH).unwrap();
    assert_eq!((rows, fixed, heap), (0, &[][..], &[][..]));
}

/// Every guard, against the block that trips it — including a forgery of each
/// counted field by one bit, and of `ROWS` by one row.
#[test]
fn each_guard_rejects_its_forgery() {
    let clean = encode(ROWS, &slices(&fixture()));
    let forged = |off: usize, f: &dyn Fn(u32) -> u32| {
        let mut buf = clean.clone();
        let v = f(read_u32_le(&buf, off));
        write_u32_le(&mut buf, off, v);
        buf
    };
    let mismatch = "block size does not match its rows and heap";

    let mut cases: Vec<(String, Vec<u8>, &str)> = vec![
        (
            "short of a header".into(),
            clean[..WAL_HEADER_SIZE - 1].to_vec(),
            "block shorter than header",
        ),
        (
            "unknown version".into(),
            forged(WAL_OFF_VERSION, &|v| v ^ 1),
            "unknown block version",
        ),
        (
            "size past the buffer".into(),
            clean[..clean.len() - 1].to_vec(),
            "declared size past buffer",
        ),
        ("one row more".into(), forged(WAL_OFF_ROWS, &|r| r + 1), mismatch),
        ("one row fewer".into(), forged(WAL_OFF_ROWS, &|r| r - 1), mismatch),
    ];
    for bit in 0..32 {
        cases.push((
            format!("ROWS bit {bit}"),
            forged(WAL_OFF_ROWS, &|r| r ^ (1 << bit)),
            mismatch,
        ));
        cases.push((
            format!("HEAP_LEN bit {bit}"),
            forged(WAL_OFF_HEAP_LEN, &|h| h ^ (1 << bit)),
            mismatch,
        ));
        let block = forged(WAL_OFF_SIZE, &|s| s ^ (1 << bit));
        let want = if read_u32_le(&block, WAL_OFF_SIZE) as usize > clean.len() {
            "declared size past buffer"
        } else {
            mismatch
        };
        cases.push((format!("SIZE bit {bit}"), block, want));
    }
    for (what, block, want) in &cases {
        assert_eq!(parse_block(block, ROW_WIDTH).err(), Some(*want), "{what}");
    }
}
