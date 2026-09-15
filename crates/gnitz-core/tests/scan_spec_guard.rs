#![cfg(feature = "integration")]

//! `SCAN_SPEC` is a user-relation verb: a system tid is rejected outright, and
//! the rejection leaves the connection usable.

use gnitz_core::protocol::{ColumnDef, Schema, TypeCode};
use gnitz_core::{ClientError, GnitzClient, TABLE_TAB};
use gnitz_test_harness::ServerHandle;
use gnitz_wire::{ReadBound, ReadSpec};

#[test]
fn scan_spec_at_a_system_tid_is_rejected_and_the_connection_survives() {
    // W = 4: the guard itself is worker-count independent, but four workers is
    // the shape in which the behaviour it prevents — four concatenated copies of
    // `_tables`, each at `WireStatus::Ok` — would occur.
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();

    let spec = ReadSpec::all_rows(ReadBound::None);

    // `handle_scan_spec` rejects the system tid at the verb, before a single
    // worker is dispatched — so the reply schema below is never consulted.
    let reply_schema =
        std::sync::Arc::new(Schema::from_parts(vec![ColumnDef::new("k", TypeCode::U64, false)], vec![0]).unwrap());
    let err = client
        .scan_spec(TABLE_TAB, &spec, &reply_schema)
        .expect_err("a ReadSpec at a system tid must be rejected");
    let ClientError::ServerError(msg) = &err else {
        panic!("expected a WireStatus::Error reply, got {err:?}");
    };
    assert!(msg.contains("system catalog family"), "{msg}");

    // The error frame is non-continuation, so the connection is still usable for
    // a full round trip.
    client.create_schema("ss").unwrap();
}
