use super::*;
use crate::test_support::{make_batch_raw, make_schema_u64_i64};
use gnitz_wire::WireConflictMode::{Error, Update};

/// A stream is append-only and nothing clamps its weights, so every weight `>= 1`
/// is admitted and the first `<= 0` one is named. `Error` mode is refused whatever
/// the rows, an empty delta included.
#[test]
fn a_stream_push_admits_only_positive_weights_outside_error_mode() {
    let weighted = |weights: &[i64]| {
        let rows: Vec<(u64, i64, i64)> = weights.iter().enumerate().map(|(i, &w)| (i as u64, w, 0)).collect();
        make_batch_raw(&make_schema_u64_i64(), &rows)
    };
    assert_eq!(check_stream_push(7, None, Update), Ok(()));
    assert_eq!(check_stream_push(7, Some(&weighted(&[1, 5, 1])), Update), Ok(()));
    for (weights, row) in [(&[0][..], 0), (&[-1], 0), (&[1, 1, -3], 2), (&[2, 0, -1], 1)] {
        let e = check_stream_push(7, Some(&weighted(weights)), Update).expect_err("a non-positive weight");
        let w = weights[row];
        assert!(
            e.contains("table 7") && e.contains(&format!("row {row} of this push carries weight {w}")),
            "{e}"
        );
    }
    for batch in [None, Some(weighted(&[1]))] {
        let e = check_stream_push(7, batch.as_ref(), Error).expect_err("Error mode");
        assert!(e.contains("conflict mode 'error'"), "{e}");
    }
}
