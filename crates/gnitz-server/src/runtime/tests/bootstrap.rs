use super::*;

/// Which of a group's written slots this rank replays. A wrong range silently
/// loses or doubles ACKed rows, so every case is pinned.
#[test]
fn replay_slots_covers_every_width_and_placement() {
    /// `written` slots on disk, this process being rank `rank` of `of`.
    fn check(written: u32, rank: u32, of: u32, replicated: bool, want: Range<u32>, want_reslice: bool) {
        let (range, reslice) = replay_slots(written, Slot::new(rank, of), replicated);
        assert_eq!(
            (range, reslice),
            (want, want_reslice),
            "written={written} rank={rank} of={of} replicated={replicated}"
        );
    }

    // Written at the launched width: this rank's own slot, whether it holds its
    // share of a partitioned group or a whole replicated copy.
    check(4, 2, 4, false, 2..3, false);
    check(4, 2, 4, true, 2..3, false);
    check(1, 0, 1, false, 0..1, false);

    // Another width, replicated: every slot is the same whole copy, so exactly
    // one is read and never re-cut.
    check(4, 1, 2, true, 0..1, false);
    check(2, 1, 4, true, 0..1, false);

    // Another width, partitioned: no slot holds this rank's rows, so every
    // written slot is walked and re-cut.
    check(4, 1, 2, false, 0..4, true);
    check(2, 3, 4, false, 0..2, true);

    // A group with nothing written reads nothing on either arm: `of >= 1`, so it
    // can never match the launched width.
    check(0, 0, 1, false, 0..0, true);
    check(0, 0, 1, true, 0..1, false);
}
