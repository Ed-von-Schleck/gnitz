use super::*;

/// A list too long to count is written at the saturated count, which every
/// reader's cap refuses rather than reading as a shorter list.
#[test]
fn a_saturated_count_is_refused_under_any_cap() {
    let mut w = Writer::new();
    w.list(&vec![0u32; 70_000]);
    let blob = w.into_vec();
    assert_eq!(blob[..2], u16::MAX.to_le_bytes());
    for cap in [0, 1, MAX_LIST_LEN] {
        let err = Reader::new(&blob).list::<u32>("xs", cap).unwrap_err();
        assert_eq!(err, format!("xs: 65535 entries exceeds cap {cap}"));
    }
}

/// An option is a presence byte then its value; text must be UTF-8; and a
/// decode labels its error with its format and the offset it ran out at.
#[test]
fn a_field_reads_back_as_written_or_is_refused() {
    let mut w = Writer::new();
    w.put(&Some("é".to_string())).put(&None::<String>);
    let blob = w.into_vec();
    let both = decode_all(&blob, "f", |r| {
        Ok((r.get::<Option<String>>()?, r.get::<Option<String>>()?))
    });
    assert_eq!(both, Ok((Some("é".to_string()), None)));

    let mut w = Writer::new();
    w.bytes32(b"\xff");
    assert_eq!(
        decode_all(&w.into_vec(), "f", |r| r.get::<String>()),
        Err("f: text is not UTF-8".into())
    );
    assert_eq!(
        decode_all(&blob[..3], "f", |r| r.get::<Option<String>>()),
        Err("f: truncated (need 4 bytes at offset 1, 2 remain)".into())
    );
    assert!(decode_all(&[0, 0], "f", |r| r.bool()).unwrap_err().contains("trailing"));
}
