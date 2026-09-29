#![cfg(feature = "integration")]

//! `SCAN_SPEC` at a system catalog family is answered off the master's own copy,
//! under the reply layout the client names: every row once, and a layout that is
//! not the family's refused with the connection left usable.

use gnitz_core::{sys_schema, ClientError, GnitzClient, Schema};
use gnitz_expr::SchemaFacts;
use gnitz_test_harness::{unique_schema, ServerHandle};
use gnitz_wire::{ColumnDef, ReadBound, ReadSpec, TableProps, TypeCode, WireFault, WireStatus, TABLE_TAB};
use std::sync::Arc;

#[test]
fn scan_spec_at_a_system_tid_is_served_once_and_checks_its_layout() {
    // W = 4: every worker holds a full copy of a catalog family, so a fanned-out
    // read would answer each row four times over.
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema("ssg");
    client.create_schema(&sn).unwrap();
    let schema = Schema::from_parts(
        vec![
            ColumnDef::new("pk", TypeCode::I64, false),
            ColumnDef::new("v", TypeCode::I64, false),
        ],
        vec![0],
    )
    .unwrap();
    let created: Vec<u64> = ["a", "b"]
        .iter()
        .map(|t| {
            client
                .create_table(&sn, t, &schema, &[], TableProps::default(), &[])
                .unwrap()
        })
        .collect();

    let spec = ReadSpec::all_rows(ReadBound::None);
    let tables = sys_schema(TABLE_TAB);
    let batch = client.scan_spec(TABLE_TAB, &spec, tables).unwrap().batch;
    for tid in created {
        let weights: Vec<i64> = (0..batch.len())
            .filter(|&i| batch.pks.get(tables, i) as u64 == tid)
            .map(|i| batch.weights[i])
            .collect();
        assert_eq!(weights, [1], "table {tid}: one row at weight 1");
    }

    // A reply layout that is not the family's is refused.
    let one_col = Arc::new(Schema::from_parts(vec![ColumnDef::new("k", TypeCode::U64, false)], vec![0]).unwrap());
    assert_ne!(one_col.layout_digest(), tables.layout_digest());
    let err = client
        .scan_spec(TABLE_TAB, &spec, &one_col)
        .expect_err("a reply layout that is not the family's must be refused");
    assert!(
        matches!(&err, ClientError::Refused(WireFault { status: WireStatus::Error, .. })),
        "expected a WireStatus::Error reply, got {err:?}"
    );

    // The error frame is non-continuation, so the connection is still usable for
    // a full round trip.
    client.create_schema(&unique_schema("ssg")).unwrap();
}
