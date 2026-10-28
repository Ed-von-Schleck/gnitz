use crate::runtime::wire::{decode_client_rows, WireMsg};
use crate::test_support::{make_batch, make_schema_u64_i64, weighted_rows};
use gnitz_wire::peek_control_block;
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::encode_schema_block;

/// A client frame's rows are decoded as a foreign block under the schema the
/// caller names, past the frame's schema record: no consolidation claim, and
/// no null bit on a NOT NULL column. A frame without a data block has no rows.
#[test]
fn a_client_frames_rows_are_a_foreign_block_under_the_callers_schema() {
    let sd = make_schema_u64_i64();
    let batch = make_batch(&sd, &[(1, 1, 10), (2, 3, 20)]);
    let mut bb = BatchBuilder::new(&sd);
    bb.begin_row(1, 1);
    bb.put_null();
    bb.end_row();
    let nulled = bb.finish();
    let record = encode_schema_block(&sd);
    for (data, want) in [
        (batch.wire_whole(), Ok(Some(weighted_rows(&batch)))),
        (None, Ok(None)),
        (nulled.wire_whole(), Err("a null bit on a NOT NULL column")),
    ] {
        let wire = WireMsg {
            schema_block: Some(&record),
            data,
            ..Default::default()
        }
        .encode_to_vec();
        let control = peek_control_block(&wire).expect("a control block");
        let got = decode_client_rows(&wire, &control, &sd).map(|rows| {
            rows.map(|b| {
                assert!(!b.is_consolidated(), "a decoded frame carries no claim");
                weighted_rows(&b)
            })
        });
        assert_eq!(got, want);
    }
}
