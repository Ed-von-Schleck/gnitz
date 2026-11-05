use super::*;
use crate::test_support::{make_batch_raw, make_schema_u64_i64};

/// Walk `batch` through [`WireMsg::next_train_frame`] as a sender does, each
/// frame's returned row committed: the rows each frame carries and whether it
/// is flagged the train's last.
fn cut(batch: &Batch, budget: usize, ends: bool) -> Vec<(usize, bool)> {
    let mut frames = Vec::new();
    let mut next = 0;
    loop {
        let (msg, n) = WireMsg::next_train_frame(7, batch, next, budget, ends).expect("every row fits a frame");
        assert!(msg.size() <= budget);
        assert_eq!(msg.target_id, 7);
        assert_eq!(msg.data.map_or(0, |d| d.rows()), n - next);
        frames.push((n - next, msg.flags.scan_last));
        next = n;
        if msg.flags.scan_last || n == batch.len() {
            return frames;
        }
    }
}

/// Every row is cut once, and only the frame reaching the end of a batch no
/// row follows is the last — an empty one's only frame included.
#[test]
fn next_train_frame_cuts_every_row_once_and_flags_only_the_end() {
    let schema = make_schema_u64_i64();
    let four = make_batch_raw(&schema, &[(0, 1, 0); 4]);
    let budget = WireMsg::train_frame(7, false, four.wire_whole()).size();
    let batch = make_batch_raw(&schema, &(0..10).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());

    assert_eq!(cut(&batch, budget, true), [(4, false), (4, false), (2, true)]);
    assert_eq!(cut(&batch, budget, false), [(4, false), (4, false), (2, false)]);
    let empty = Batch::empty_with_schema(&schema);
    assert_eq!(cut(&empty, budget, true), [(0, true)]);
}
