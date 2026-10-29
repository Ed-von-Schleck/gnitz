use super::*;
use crate::MAX_PK_BYTES;

fn key(bytes: &[u8]) -> Option<PkBuf> {
    Some(PkBuf::from_bytes(bytes))
}

fn request(limit: u64, after: Vec<Option<PkBuf>>) -> PageRequest {
    PageRequest { layout: 0xfeed, limit, after }
}

#[test]
fn a_page_request_reads_back_as_written() {
    for after in [
        vec![],
        vec![None],
        vec![key(&[1, 2, 3, 4, 5, 6, 7, 8]), None, key(&[9; 16])],
    ] {
        let req = request(7, after);
        assert_eq!(PageRequest::decode(&req.encode()).unwrap(), req);
    }
}

/// A page of rows `1..=MAX_PAGE_ROWS` naming at most one cursor per worker, each
/// a key some relation can have, decodes; anything else, or a trailing byte, is
/// refused by name.
#[test]
fn a_page_request_refuses_what_no_page_is() {
    let mut trailing = request(1, vec![]).encode();
    trailing.push(0);
    // Past what a `PkBuf` holds, so written by hand.
    let mut oversized = Writer::new();
    oversized
        .u64(0xfeed)
        .u64(1)
        .count(1)
        .bool(true)
        .bytes32(&[0; MAX_PK_BYTES + 1]);
    for (blob, want) in [
        (request(MAX_PAGE_ROWS, vec![]).encode(), None),
        (request(1, vec![None; MAX_WORKERS]).encode(), None),
        (request(1, vec![key(&[0; MAX_PK_BYTES])]).encode(), None),
        (request(0, vec![]).encode(), Some("a page of 0 rows")),
        (request(MAX_PAGE_ROWS + 1, vec![]).encode(), Some("rows (1..=")),
        (request(1, vec![None; MAX_WORKERS + 1]).encode(), Some("exceeds cap")),
        (request(1, vec![None, key(&[])]).encode(), Some("a 0-byte key")),
        (oversized.into_vec(), Some("-byte key")),
        (trailing, Some("trailing")),
    ] {
        match (PageRequest::decode(&blob), want) {
            (Ok(_), None) => {}
            (Err(err), Some(want)) => assert!(err.contains(want), "{err:?} does not name {want:?}"),
            (got, want) => panic!("{got:?}, want {want:?}"),
        }
    }
}

#[test]
fn a_store_reads_its_own_cursor() {
    let req = request(10, vec![None, key(&[7])]);
    assert_eq!(req.after_at(1, 2).unwrap(), Some(&[7u8][..]));
    assert_eq!(req.after_at(0, 2).unwrap(), None);
    assert!(req.after_at(0, 3).is_err());
    assert_eq!(request(10, vec![]).after_at(2, 3).unwrap(), None);
    assert_eq!(PageRequest::rows_per_store(10, 4), 3);
}
