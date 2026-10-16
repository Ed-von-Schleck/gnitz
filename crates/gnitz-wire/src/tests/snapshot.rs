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

#[test]
fn a_key_no_relation_has_is_refused() {
    assert!(PageRequest::decode(&request(1, vec![None, Some(Vec::new())]).encode()).is_err());
    assert!(PageRequest::decode(&request(1, vec![Some(vec![0; MAX_PK_BYTES + 1])]).encode()).is_err());
    let mut trailing = request(1, vec![]).encode();
    trailing.push(0);
    assert!(PageRequest::decode(&trailing).is_err());
}

#[test]
fn a_page_of_no_rows_or_past_the_cap_is_refused() {
    assert!(PageRequest::decode(&request(0, vec![]).encode()).is_err());
    assert!(PageRequest::decode(&request(MAX_PAGE_ROWS + 1, vec![]).encode()).is_err());
    assert!(PageRequest::decode(&request(MAX_PAGE_ROWS, vec![]).encode()).is_ok());
}

#[test]
fn a_page_naming_more_cursors_than_workers_is_refused() {
    assert!(PageRequest::decode(&request(1, vec![None; MAX_WORKERS]).encode()).is_ok());
    assert!(PageRequest::decode(&request(1, vec![None; MAX_WORKERS + 1]).encode()).is_err());
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
