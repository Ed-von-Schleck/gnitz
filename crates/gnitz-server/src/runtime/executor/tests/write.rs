use super::*;
use crate::runtime::reactor::Budget;
use crate::test_support::{make_batch, make_schema_u64_i64};
use gnitz_wire::WireConflictMode::{Error, Update};
use gnitz_zset::repr::Batch;

/// A stream is append-only and nothing clamps its weights, so every weight `>= 1`
/// is admitted and the first `<= 0` one is named. `Error` mode is refused whatever
/// the rows, an empty delta included.
#[test]
fn a_stream_push_admits_only_positive_weights_outside_error_mode() {
    let weighted = |weights: &[i64]| {
        let rows: Vec<(u64, i64, i64)> = weights.iter().enumerate().map(|(i, &w)| (i as u64, w, 0)).collect();
        make_batch(&make_schema_u64_i64(), &rows)
    };
    assert_eq!(check_stream_push(7, None, Update, 0), Ok(()));
    assert_eq!(
        check_stream_push(7, Some(weighted(&[1, 5, 1]).as_mem_batch()), Update, 0),
        Ok(())
    );
    for (weights, row) in [(&[0][..], 0), (&[-1], 0), (&[1, 1, -3], 2), (&[2, 0, -1], 1)] {
        let e =
            check_stream_push(7, Some(weighted(weights).as_mem_batch()), Update, 0).expect_err("a non-positive weight");
        let w = weights[row];
        assert!(
            e.contains("table 7") && e.contains(&format!("row {row} of this push carries weight {w}")),
            "{e}"
        );
    }
    for batch in [None, Some(weighted(&[1]))] {
        let e = check_stream_push(7, batch.as_ref().map(Batch::as_mem_batch), Error, 0).expect_err("Error mode");
        assert!(e.contains("conflict mode 'error'"), "{e}");
    }
}

/// A PUSH frame of `rows` u64 keys naming table 7, `more` setting
/// `continuation`.
fn push_frame(rows: u64, more: bool) -> (RecvBuf, DecodedControl) {
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &(0..rows).map(|pk| (pk, 1, 0)).collect::<Vec<_>>());
    let mut hdr = gnitz_wire::ControlHeader::naming(ClientVerb::Push, 7.into(), 0);
    hdr.flags.continuation = more;
    let mut frame = Vec::new();
    // The blob stands in for a shape record: the open write reads none.
    gnitz_wire::append_frame(&mut frame, &hdr, b"shape", Some(&batch.regions()));
    let ctrl = gnitz_wire::peek_control_block(&frame).unwrap();
    (RecvBuf::detached(&frame), ctrl)
}

/// An open write holds its frames in arrival order until its last, each
/// charged to the budget every open write shares until the write is done.
#[test]
fn an_open_write_holds_its_frames_in_order() {
    let spans = Budget::new(usize::MAX);
    let mut w = OpenWrite::new();
    let mut bytes = 0;
    for (rows, more) in [(3, true), (1, true), (2, false)] {
        let (buf, ctrl) = push_frame(rows, more);
        bytes += buf.as_slice().len();
        assert_eq!(w.take(buf, ctrl, &spans), more);
    }
    assert_eq!(spans.held(), bytes);
    let frames = w.finish().unwrap();
    assert_eq!(spans.held(), 0, "a finished write releases its charges");
    let blocks: Vec<usize> = frames.iter().map(|(_, c)| c.data.clone().unwrap().len()).collect();
    assert_eq!(frames.len(), 3);
    assert!(blocks[0] > blocks[2] && blocks[2] > blocks[1], "{blocks:?}");
}

/// A frame the shared budget has no room for, or one that does not decode,
/// refuses the write: every frame it holds is released at once, the ones
/// after it are dropped as they arrive, and the first reason is the answer.
/// Another write's frames take the room the budget has left.
#[test]
fn an_open_write_past_the_shared_limit_releases_every_frame() {
    let (buf, ctrl) = push_frame(10, true);
    let spans = Budget::new(buf.as_slice().len() + 1);
    let mut w = OpenWrite::new();
    w.take(buf, ctrl, &spans);
    let mut other = OpenWrite::new();
    let (buf, ctrl) = push_frame(1, false);
    other.take(buf, ctrl, &spans);
    assert!(other.finish().is_err(), "the other write finds no room");
    let (buf, ctrl) = push_frame(10, true);
    w.take(buf, ctrl, &spans);
    assert_eq!(spans.held(), 0, "the frames held are released");
    let (buf, ctrl) = push_frame(1, false);
    w.take(buf, ctrl, &spans);
    assert_eq!(spans.held(), 0, "a frame after the refusal is dropped");
    let why = w.finish().err().expect("refused");
    assert!(
        why.text.contains(&format!("{}-byte limit", spans.cap())),
        "{}",
        why.text
    );

    let mut w = OpenWrite::new();
    let (buf, mut ctrl) = push_frame(1, true);
    ctrl.hdr.flags.scan_last = true;
    w.take(buf, ctrl, &spans);
    let why = w.finish().err().expect("refused");
    assert!(why.text.contains("decode error"), "{}", why.text);
}
