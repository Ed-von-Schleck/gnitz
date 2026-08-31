use super::*;
use gnitz_engine_testkit::{make_batch_raw, make_schema_u64_i64};

/// A batch whose rows carry `weights`, one distinct PK each.
fn weighted(weights: &[i64]) -> Batch {
    let rows: Vec<(u64, i64, i64)> = weights.iter().enumerate().map(|(i, &w)| (i as u64, w, 0)).collect();
    make_batch_raw(&make_schema_u64_i64(), &rows)
}

/// A stream is append-only, and nothing clamps its weights the way
/// `enforce_unique_pk` clamps a base table's. Bag multiplicity above 1 is the
/// legal case, so a `> 1` check would be wrong in the other direction.
#[test]
fn stream_push_rejects_only_non_positive_weights() {
    let ok = gnitz_wire::WireConflictMode::Update;
    assert_eq!(stream_push_error(7, &weighted(&[1, 5, 1]), ok), None);
    assert_eq!(stream_push_error(7, &weighted(&[]), ok), None);
    for bad in [vec![0], vec![-1], vec![1, 1, -3], vec![2, 0]] {
        let e = stream_push_error(7, &weighted(&bad), ok).expect("must be rejected");
        assert!(e.contains("append-only"), "got: {e}");
        assert!(e.contains("table 7"), "must name the relation: {e}");
    }
}

/// `Error` mode asserts a primary-key uniqueness a stream does not have, and
/// rejecting it is what keeps `push_reads_committed_state` false. It is refused
/// even when every weight is legal.
#[test]
fn stream_push_rejects_error_conflict_mode() {
    let e = stream_push_error(7, &weighted(&[1]), gnitz_wire::WireConflictMode::Error).expect("must be rejected");
    assert!(e.contains("conflict mode 'error'"), "got: {e}");
}
