#![cfg(feature = "integration")]

//! A client that sets a null bit under a `NOT NULL` column must be rejected at
//! the server's client-decode boundary.
//!
//! `ZSetBatch::nulls` is a `pub Vec<u64>`, and the client-side
//! `ZSetBatch::validate` that `Session::submit` runs is skippable by anyone who
//! encodes a frame and writes it to the socket themselves — it lives in the
//! client process, so it is a convenience, never a trust boundary. The bit it
//! lets through is one the engine's two camps read differently: `is_null`
//! (index projection, FK probe) and
//! `compare_by_group_cols` believe the bit, while the evaluator's
//! `nullable_slots`, a projection's `NullPerm` and the `FixedIntNonnull` row
//! comparator believe the schema. This test drives the frame the client library
//! would never build.

use gnitz_core::protocol::{
    encode_message_parts, hello_handshake, parse_response, ClientTransport, ColumnDef, Schema, TypeCode,
};
use gnitz_core::TableProps;
use gnitz_core::{BatchAppender, GnitzClient, ZSetBatch, FLAG_PUSH};
use gnitz_test_harness::{unique_schema, ServerHandle};

/// Ship `batch` as a PUSH, bypassing the client-side `ZSetBatch::validate` that
/// `Session::submit` runs by writing the encoded frame to the socket itself —
/// byte for byte the one `submit` would have built for a cold push. A scripted
/// peer, so it needs no spine at all.
fn hostile_push(t: &mut ClientTransport, tid: u64, schema: &Schema, batch: &ZSetBatch) -> Result<u64, String> {
    let parts = encode_message_parts(tid, 0xB0BA, FLAG_PUSH, 0, &[], 0, Some((schema, batch)));
    t.send_framed_iov(&parts.segments()).map_err(|e| e.to_string())?;
    let buf = t.recv_framed().map_err(|e| e.to_string())?;
    let (msg, _) = parse_response(&buf, None).map_err(|e| e.to_string())?;
    match msg.error_text {
        Some(text) => Err(text),
        None => Ok(msg.seek_pk as u64),
    }
}

#[test]
fn a_null_bit_on_a_not_null_column_is_rejected_at_the_client_boundary() {
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema("nnb");
    client.create_schema(&sn).unwrap();

    // `v` is NOT NULL, `w` is nullable — so the schema's not-null payload mask
    // is exactly slot 0, and a bit at slot 1 must stay legal.
    let cols = vec![
        ColumnDef::new("id", TypeCode::U64, false),
        ColumnDef::new("v", TypeCode::I64, false),
        ColumnDef::new("w", TypeCode::I64, true),
    ];
    client
        .create_table(&sn, "t", &cols, &[0], TableProps::default(), &[])
        .unwrap();
    let (tid, schema) = client.resolve_table_id(&sn, "t").unwrap();

    let build = || {
        let mut batch = ZSetBatch::new(&schema);
        let mut app = BatchAppender::new(&mut batch, &schema);
        for i in 1u64..=4 {
            app.add_row(i as u128, 1).i64_val(i as i64).i64_val(i as i64 * 10);
        }
        batch
    };

    // A second, raw connection: `GnitzClient`'s own session is private, and a
    // pre-encoded frame written straight to the socket is what walks past the
    // validator.
    let mut raw = ClientTransport::connect(srv.sock_path()).unwrap();
    hello_handshake(&mut raw).unwrap();

    // The same rows, unmodified, are accepted — so the rejection below is about
    // the bit and nothing else.
    let clean = build();
    clean.validate(&schema).expect("the honest batch is client-side valid");
    hostile_push(&mut raw, tid, &schema, &clean).expect("a conforming batch is accepted");

    // A bit under the NULLABLE column `w` (payload slot 1) is also fine.
    let mut nullable_bit = build();
    nullable_bit.nulls[2] |= 1 << 1;
    hostile_push(&mut raw, tid, &schema, &nullable_bit).expect("a null bit on a nullable column is legal");

    // One bit under the NOT NULL column `v` (payload slot 0), on one row of four.
    let mut hostile = build();
    hostile.nulls[2] |= 1 << 0;
    assert!(
        hostile.validate(&schema).is_err(),
        "the client-side validator would have caught this — the point is that a \
         client can skip it",
    );
    let err = hostile_push(&mut raw, tid, &schema, &hostile).expect_err("the server must reject the bit");
    assert!(
        err.contains("null bit on a NOT NULL column"),
        "rejected for the right reason, got: {err}",
    );
}
