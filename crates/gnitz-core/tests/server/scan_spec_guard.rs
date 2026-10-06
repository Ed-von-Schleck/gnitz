//! `SCAN_SPEC` at a system catalog family is answered off the master's own copy,
//! under the reply layout the client names: every row once, and a layout that is
//! not the family's refused with the connection left usable.

use super::*;
use gnitz_core::block_on;
use gnitz_core::sys_schema;
use gnitz_expr::SchemaFacts;
use gnitz_wire::{WireFault, WireStatus, TABLE_TAB};

#[test]
fn scan_spec_at_a_system_tid_is_served_once_and_checks_its_layout() {
    // W = 4: every worker holds a full copy of a catalog family, so a fanned-out
    // read would answer each row four times over.
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();

    let tables = sys_schema(TABLE_TAB);
    let batch = block_on(client.scan_spec(TABLE_TAB, &ReadSpec::all_rows(ReadBound::None), tables))
        .unwrap()
        .batch;
    let mut pks: Vec<u128> = (0..batch.len()).map(|i| batch.pks.get(i)).collect();
    pks.sort_unstable();
    pks.dedup();
    assert!(!pks.is_empty(), "the family holds its own seed rows");
    assert_eq!(pks.len(), batch.len(), "every row once");
    assert!(batch.weights.iter().all(|&w| w == 1), "{:?}", batch.weights);

    // A reply layout that is not the family's is refused.
    let one_col = Arc::new(schema_of(&[("k", TypeCode::U64)]));
    assert_ne!(one_col.layout_digest(), tables.layout_digest());
    let err = block_on(client.scan_spec(TABLE_TAB, &ReadSpec::all_rows(ReadBound::None), &one_col))
        .expect_err("a reply layout that is not the family's must be refused");
    assert!(
        matches!(&err, ClientError::Refused(WireFault { status: WireStatus::Error, .. })),
        "expected a WireStatus::Error reply, got {err:?}"
    );

    // The error frame is non-continuation, so the connection is still usable for
    // a full round trip.
    block_on(client.alloc_id()).unwrap();
}
