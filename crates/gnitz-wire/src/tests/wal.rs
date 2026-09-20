use super::*;
use crate::region::REG_PK;

fn make_test_regions() -> Vec<Vec<u8>> {
    // 3 regions: pk_lo (16B), pk_hi (16B), weight (16B) — simulating 2 rows
    let r0 = vec![1u8, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0];
    let r1 = vec![0u8; 16];
    let r2 = vec![1u8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
    vec![r0, r1, r2]
}

fn wal_block<'a>(table_id: u32, entry_count: u32, regions: &[&'a [u8]]) -> WalBlock<'a> {
    let mut b = WalBlock::new(table_id, entry_count);
    for r in regions {
        b.regions.push(r);
    }
    b
}

/// Frame `regions` into a fresh buffer, as a SAL slot would.
fn encode(table_id: u32, entry_count: u32, regions: &[&[u8]]) -> Vec<u8> {
    let b = wal_block(table_id, entry_count, regions);
    let mut buf = vec![0u8; b.size()];
    b.write(&mut buf);
    buf
}

fn parse(block: &[u8]) -> Result<(u32, Vec<&[u8]>), &'static str> {
    let mut out = Regions::new();
    let count = validate_and_parse(block, &mut out)?;
    Ok((count, out.to_vec()))
}

/// The header fields and every region's bytes come back, a second block appends
/// behind the first, and regions pack end to end.
#[test]
fn encode_and_parse_roundtrip_append_and_packing() {
    let regions = make_test_regions();
    let slices: Vec<&[u8]> = regions.iter().map(Vec::as_slice).collect();

    let mut buf = encode(7, 2, &slices);
    let end1 = buf.len();
    let (count, parsed) = parse(&buf).unwrap();
    assert_eq!(count, 2);
    assert_eq!(read_u32_le(&buf, WAL_OFF_TID), 7);
    assert_eq!(parsed, slices);

    // A second block appends behind the first, as the IPC paths write them, and
    // is recovered by its own SIZE field rather than by a remembered offset.
    buf.extend_from_slice(&encode(20, 2, &slices));
    for (off, want_tid) in [(0, 7u32), (end1, 20)] {
        let block = block_slice_at(&buf, off).expect("a framed block");
        parse(block).unwrap();
        assert_eq!(read_u32_le(block, WAL_OFF_TID), want_tid);
    }

    // A 5-byte region leaves no gap behind it, so the block is exactly its
    // directory plus its region bytes.
    let (r0, r1) = ([1u8, 2, 3, 4, 5], [6u8, 7, 8, 9]);
    let buf = encode(0, 2, &[&r0, &r1]);
    assert_eq!(buf.len(), body_start(2) + r0.len() + r1.len());
    assert_eq!(parse(&buf).unwrap().1, vec![&r0[..], &r1[..]]);
}

/// The appending and the in-place framer produce the same bytes.
#[test]
fn append_to_matches_write() {
    let regions = make_test_regions();
    let slices: Vec<&[u8]> = regions.iter().map(Vec::as_slice).collect();
    let (odd, empty) = ([1u8, 2, 3], []);
    for list in [&slices[..], &[&odd, &empty, &odd], &[]] {
        let b = wal_block(9, 2, list);
        let mut appended = vec![0xAB];
        b.append_to(&mut appended);
        assert_eq!(appended[1..], encode(9, 2, list), "{} regions", list.len());
    }
}

/// Every framer guard, against the block that trips it.
#[test]
fn each_framer_guard_mints_its_own_error() {
    let regions = make_test_regions();
    let slices: Vec<&[u8]> = regions.iter().map(Vec::as_slice).collect();

    let header_only = |version: u32, size: u32, n: u32| {
        let mut buf = vec![0u8; WAL_HEADER_SIZE];
        write_u32_le(&mut buf, WAL_OFF_VERSION, version);
        write_u32_le(&mut buf, WAL_OFF_SIZE, size);
        write_u32_le(&mut buf, WAL_OFF_NUM_REGIONS, n);
        buf
    };
    let forged = |f: &dyn Fn(&mut Vec<u8>)| {
        let mut buf = encode(1, 2, &slices);
        f(&mut buf);
        buf
    };

    let cases: &[(&str, Vec<u8>, &str)] = &[
        ("short of a header", vec![0u8; 10], "block shorter than header"),
        (
            "unknown version",
            header_only(99, WAL_HEADER_SIZE as u32, 0),
            "unknown block version",
        ),
        (
            "size past the buffer",
            header_only(WAL_FORMAT_VERSION, WAL_HEADER_SIZE as u32 + 1, 0),
            "declared size past buffer",
        ),
        (
            "region count past the cap",
            forged(&|b| write_u32_le(b, WAL_OFF_NUM_REGIONS, MAX_WIRE_REGIONS as u32 + 1)),
            "region count past cap",
        ),
        // A header-sized buffer whose directory does not fit: the entry read
        // would run off the end before any other guard sees it.
        (
            "directory past the block",
            header_only(WAL_FORMAT_VERSION, WAL_HEADER_SIZE as u32, 1),
            "directory past block",
        ),
        (
            "region extent past the block",
            forged(&|b| {
                let end = b.len() as u32;
                write_u32_le(b, dir_entry_offset(REG_PK), end);
            }),
            "region extent past block",
        ),
        (
            "directory short of the block",
            forged(&|b| write_u32_le(b, dir_entry_offset(REG_PK), 8)),
            "directory does not cover the block",
        ),
    ];
    for (what, block, want) in cases {
        assert_eq!(parse(block).err(), Some(*want), "{what}");
    }
}

#[test]
fn empty_regions() {
    let buf = encode(1, 0, &[]);
    assert_eq!(buf.len(), WAL_HEADER_SIZE); // just a header, no directory, no data

    let (count, regions) = parse(&buf).unwrap();
    assert_eq!(count, 0);
    assert!(regions.is_empty());
}
