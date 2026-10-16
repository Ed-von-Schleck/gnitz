use super::*;

fn request(limit: u64, after: Option<Vec<u8>>) -> PageRequest {
    PageRequest { layout: 0xfeed, limit, after }
}

#[test]
fn a_page_request_reads_back_as_written() {
    for after in [None, Some(vec![1, 2, 3, 4, 5, 6, 7, 8])] {
        let req = request(7, after);
        assert_eq!(PageRequest::decode(&req.encode()).unwrap(), req);
    }
}

#[test]
fn a_key_no_relation_has_is_refused() {
    assert!(PageRequest::decode(&request(1, Some(Vec::new())).encode()).is_err());
    assert!(PageRequest::decode(&request(1, Some(vec![0; MAX_PK_BYTES + 1])).encode()).is_err());
    let mut trailing = request(1, None).encode();
    trailing.push(0);
    assert!(PageRequest::decode(&trailing).is_err());
}

#[test]
fn a_page_of_no_rows_or_past_the_cap_is_refused() {
    assert!(PageRequest::decode(&request(0, None).encode()).is_err());
    assert!(PageRequest::decode(&request(MAX_PAGE_ROWS + 1, None).encode()).is_err());
    assert!(PageRequest::decode(&request(MAX_PAGE_ROWS, None).encode()).is_ok());
}
