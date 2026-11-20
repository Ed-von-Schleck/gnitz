use super::*;
use crate::test_support::{make_batch, make_schema_u64_i64};
use gnitz_wire::{ClientVerb, WireFlags, WireStatus, peek_control_block};

/// Walk `batch` through [`WireMsg::next_train_frame`] as a sender does, each
/// frame's returned row committed: the rows each frame carries and whether it
/// is flagged the train's last.
fn cut(batch: &Batch, budget: usize, ends: bool) -> Vec<(usize, bool)> {
    let mut frames = Vec::new();
    let mut next = 0;
    loop {
        let (msg, n) = WireMsg::next_train_frame(7, batch, next, budget, ends).expect("every row fits a frame");
        assert!(msg.size() <= budget);
        assert_eq!(msg.hdr.target_id, 7);
        assert_eq!(msg.data.map_or(0, |d| d.rows()), n - next);
        frames.push((n - next, msg.hdr.flags.scan_last));
        next = n;
        if msg.hdr.flags.scan_last || n == batch.len() {
            return frames;
        }
    }
}

/// Every row is cut once, and only the frame reaching the end of a batch no
/// row follows is the last — an empty one's only frame included.
#[test]
fn next_train_frame_cuts_every_row_once_and_flags_only_the_end() {
    let schema = make_schema_u64_i64();
    let four = make_batch(&schema, &[(0, 1, 0); 4]);
    let budget = WireMsg::train_frame(7, false, four.wire_whole()).size();
    let batch = make_batch(&schema, &(0..10).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());

    assert_eq!(cut(&batch, budget, true), [(4, false), (4, false), (2, true)]);
    assert_eq!(cut(&batch, budget, false), [(4, false), (4, false), (2, false)]);
    let empty = Batch::empty_with_schema(&schema);
    assert_eq!(cut(&empty, budget, true), [(0, true)]);
}

/// A frame's control header and blob decode to what was sent, and no proper
/// prefix of the frame decodes.
#[test]
fn a_control_only_frame_round_trips_and_no_prefix_decodes() {
    let shapes = [
        WireMsg {
            hdr: ControlHeader {
                target_id: 0xDEAD,
                flags: WireFlags {
                    verb: Some(ClientVerb::PushTxn),
                    continuation: true,
                    ..Default::default()
                },
                arg0: 0x1111_2222_3333_4444,
                arg1: 0x5555,
                ..Default::default()
            },
            ..Default::default()
        },
        WireMsg {
            hdr: ControlHeader {
                target_id: 7,
                status: WireStatus::Error,
                ..Default::default()
            },
            blob: b"something went wrong",
            ..Default::default()
        },
    ];
    for (shape, msg) in shapes.into_iter().enumerate() {
        let wire = msg.encode_to_vec();
        let control = peek_control_block(&wire).unwrap_or_else(|e| panic!("shape {shape}: {e}"));
        assert_eq!(control.hdr, msg.hdr, "shape {shape}");
        assert_eq!(&wire[control.blob], msg.blob, "shape {shape}");
        assert!(control.data.is_none(), "shape {shape}");
        for cut in 0..wire.len() {
            assert!(
                peek_control_block(&wire[..cut]).is_err(),
                "shape {shape}: a {cut}/{}-byte prefix decodes",
                wire.len()
            );
        }
    }
}

/// A fault frame carries its fault's status and its text, cut at the last
/// character boundary within [`MAX_FAULT_TEXT`] bytes, so the fault of a
/// request that was a whole frame of echoed text is itself a frame.
#[test]
fn a_fault_frame_carries_a_bounded_prefix_of_its_text() {
    let short = WireFault::from("no such relation");
    assert_eq!(WireMsg::fault(0, &short).blob, short.text.as_bytes());

    let kept = "x".repeat(MAX_FAULT_TEXT - 1);
    let straddling = WireFault {
        status: WireStatus::NotFound,
        text: format!("{kept}\u{e9}{}", "y".repeat(100)),
    };
    let msg = WireMsg::fault(0, &straddling);
    assert_eq!(msg.hdr.status, WireStatus::NotFound);
    assert_eq!(msg.blob, kept.as_bytes());

    let echo = WireFault::from("x".repeat(gnitz_wire::MAX_FRAME_PAYLOAD));
    let msg = WireMsg::fault(0, &echo);
    assert_eq!(msg.blob.len(), MAX_FAULT_TEXT);
    assert!(msg.check_size().is_ok());
}
