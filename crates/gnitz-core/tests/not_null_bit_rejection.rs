#![cfg(feature = "integration")]

//! A client that sets a null bit under a `NOT NULL` column must be rejected at
//! the server's client-decode boundary.
//!
//! `ZSetBatch::nulls` is a `pub Vec<u64>` and `encode_message_parts` /
//! `Session::send_batch` are public, so the client-side `ZSetBatch::validate`
//! that `Session::push_with_mode` runs is skippable — it lives in the client
//! process. The bit it lets through is one the engine's two camps read
//! differently: `is_null` (index projection, FK probe) and
//! `compare_by_group_cols` believe the bit, while the evaluator's
//! `nullable_slots`, a projection's `NullPerm` and the `FixedIntNonnull` row
//! comparator believe the schema. This test drives the frame the client library
//! would never build.

use gnitz_core::protocol::{ColumnDef, Schema, TypeCode};
use gnitz_core::TableProps;
use gnitz_core::{
    encode_message_parts, BatchAppender, GnitzClient, PkTuple, Session, WireConflictMode, ZSetBatch, FLAG_PUSH,
};
use gnitz_test_harness::ServerHandle;

/// Per-test unique schema name (each test owns its server; uniqueness keeps a
/// failure unambiguous).
fn unique_schema() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!("nnb{}", SEQ.fetch_add(1, Ordering::Relaxed))
}

/// Ship `batch` as a cold PUSH frame over a raw session, bypassing
/// `Session::push_with_mode`'s client-side `ZSetBatch::validate`.
fn hostile_push(session: &mut Session, tid: u64, schema: &Schema, batch: &ZSetBatch) -> Result<u64, String> {
    let flags = gnitz_core::protocol::wire_flags_set_conflict_mode(FLAG_PUSH, WireConflictMode::Update);
    let parts = encode_message_parts(
        tid,
        session.client_id,
        flags,
        &PkTuple::EMPTY,
        0,
        Some(schema),
        Some(batch),
    );
    session.send_batch(&[parts]).map_err(|e| e.to_string())?;
    session.recv_push_ack(tid).map_err(|e| e.to_string())
}

#[test]
fn a_null_bit_on_a_not_null_column_is_rejected_at_the_client_boundary() {
    let Some(srv) = ServerHandle::start_n(4) else {
        return;
    };
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema();
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

    // A second, raw connection: `GnitzClient`'s own session is private, and this
    // one never runs the client-side validator.
    let (mut raw, _lsn) = Session::connect(srv.sock_path()).unwrap();

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
