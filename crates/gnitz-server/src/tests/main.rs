use super::parse_workers;
use gnitz_wire::MAX_WORKERS;

#[test]
fn parse_workers_accepts_the_valid_range_and_nothing_else() {
    assert_eq!(parse_workers("1"), Ok(1));
    assert_eq!(parse_workers(&MAX_WORKERS.to_string()), Ok(MAX_WORKERS as u32));
    // Above MAX_WORKERS once reached the SAL group writer, which cannot
    // describe a group that wide.
    for bad in ["0", &(MAX_WORKERS + 1).to_string(), "100000", "abc", ""] {
        assert!(parse_workers(bad).is_err(), "{bad:?} is not a worker count");
    }
}
