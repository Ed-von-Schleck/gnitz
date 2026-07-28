#![cfg(feature = "integration")]

//! `SCAN_SPEC` is a user-relation verb: a system tid is rejected outright, and
//! the rejection leaves the connection usable.

use gnitz_core::{ClientError, GnitzClient, TABLE_TAB};
use gnitz_test_harness::ServerHandle;
use gnitz_wire::{ReadBound, ReadSink, ReadSpec};

#[test]
fn scan_spec_at_a_system_tid_is_rejected_and_the_connection_survives() {
    // W = 4: the guard itself is worker-count independent, but four workers is
    // the shape in which the behaviour it prevents — four concatenated copies of
    // `_tables`, each at STATUS_OK — would occur.
    let Some(srv) = ServerHandle::start_n(4) else {
        return;
    };
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();

    let spec = ReadSpec {
        bound: ReadBound::None,
        predicate: Vec::new(),
        sink: ReadSink::all_rows(),
    }
    .encode();

    // The reply block is opaque bytes forwarded to the workers; the rejection
    // lands before anything decodes it.
    let err = client
        .scan_spec(TABLE_TAB, &spec, &[])
        .expect_err("a ReadSpec at a system tid must be rejected");
    let ClientError::ServerError(msg) = &err else {
        panic!("expected a STATUS_ERROR reply, got {err:?}");
    };
    assert!(msg.contains("not a user relation"), "{msg}");

    // The error frame is non-continuation, so the connection is still usable for
    // a full round trip.
    client.create_schema("ss").unwrap();
}
