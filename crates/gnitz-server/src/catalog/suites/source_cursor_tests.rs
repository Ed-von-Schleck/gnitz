//! The source a view's backfill drives, beside the tick that follows it.

use super::*;
use crate::test_support::u64_i64_cols;
use gnitz_store::relation::PkRule;

/// A backfill over a source holding un-ticked rows is exact: it reads below the
/// cut, and the tick brings the rest once.
#[test]
fn a_backfill_leaves_the_rows_above_the_cut_to_their_tick() {
    let cols = u64_i64_cols();
    let (_dir, mut engine, tid) = ingest_fixture(&cols, 200, |id| [id * 10]);
    let vid = register_identity_view(&mut engine, tid, "v_base", &cols);
    let late = rows(&engine, tid, 1, 200..203, |id| [id * 10]);
    engine.ingest_unticked(tid, late, PkRule::Enforce).unwrap();

    backfill(&mut engine, vid);
    assert_eq!(held(&engine, vid), (200, 200));
    seal_and_tick(&mut engine, tid);
    assert_eq!(held(&engine, vid), (203, 203));
}
