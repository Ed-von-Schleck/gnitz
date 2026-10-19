use super::*;

fn request(limit: u64, after: Vec<Option<Vec<u8>>>) -> PageRequest {
    PageRequest { layout: 0xfeed, limit, after }
}

#[test]
fn a_page_request_reads_back_as_written() {
    for after in [
        vec![],
        vec![None],
        vec![Some(vec![1, 2, 3, 4, 5, 6, 7, 8]), None, Some(vec![9; 16])],
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
    for (blob, want) in [
        (request(MAX_PAGE_ROWS, vec![]).encode(), None),
        (request(1, vec![None; MAX_WORKERS]).encode(), None),
        (request(1, vec![Some(vec![0; MAX_PK_BYTES])]).encode(), None),
        (request(0, vec![]).encode(), Some("a page of 0 rows")),
        (request(MAX_PAGE_ROWS + 1, vec![]).encode(), Some("rows (1..=")),
        (
            request(1, vec![None; MAX_WORKERS + 1]).encode(),
            Some("cursors (at most"),
        ),
        (request(1, vec![None, Some(Vec::new())]).encode(), Some("a 0-byte key")),
        (
            request(1, vec![Some(vec![0; MAX_PK_BYTES + 1])]).encode(),
            Some("-byte key"),
        ),
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
    let req = request(10, vec![None, Some(vec![7])]);
    assert_eq!(req.after_at(1, 2).unwrap(), Some(&[7u8][..]));
    assert_eq!(req.after_at(0, 2).unwrap(), None);
    assert!(req.after_at(0, 3).is_err());
    assert_eq!(request(10, vec![]).after_at(2, 3).unwrap(), None);
    assert_eq!(PageRequest::rows_per_store(10, 4), 3);
}
