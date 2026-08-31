use super::parse_workers;
use gnitz_wire::MAX_WORKERS;

#[test]
fn parse_workers_accepts_valid_range() {
    assert_eq!(parse_workers("1"), Ok(1));
    assert_eq!(parse_workers(&MAX_WORKERS.to_string()), Ok(MAX_WORKERS as u32));
}

#[test]
fn parse_workers_rejects_zero() {
    assert!(parse_workers("0").is_err());
}

#[test]
fn parse_workers_rejects_above_max() {
    // Regression: values > MAX_WORKERS reached the SAL group writer, which
    // cannot describe a group that wide.
    assert!(parse_workers(&(MAX_WORKERS + 1).to_string()).is_err());
    assert!(parse_workers("100000").is_err());
}

#[test]
fn parse_workers_rejects_nonnumeric() {
    assert!(parse_workers("abc").is_err());
    assert!(parse_workers("").is_err());
}
