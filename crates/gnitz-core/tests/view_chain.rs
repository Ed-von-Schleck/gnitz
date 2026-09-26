#![cfg(feature = "integration")]

//! The raw `create_view_chain` contract: a bundle is one atomic zone, a view reads
//! only segments before it, and every segment names the user-named view as its
//! owner — which is what the drop cascade keys on and a rename leaves alone.
//!
//! Every segment here is a shard-free identity view, `input_delta → sink`.
//! Nothing about that shape orders a backfill: it runs no exchange, so no
//! cross-worker barrier can hold a consumer back — correct output rests
//! entirely on the driver visiting a producer before its consumer.

use gnitz_core::{
    BatchAppender, Circuit, ColumnDef, GnitzClient, PlannedView, Schema, TableProps, TypeCode, ViewBundle, ViewProps,
    ZSetBatch,
};
use gnitz_test_harness::{unique_schema, ServerHandle};
use std::sync::Arc;

const BASE_ROWS: [(i64, i64); 3] = [(1, 10), (2, 20), (3, 30)];

/// Base table `base(pk BIGINT PK, v BIGINT)` holding [`BASE_ROWS`]; returns
/// `(tid, its schema)`.
fn make_base(client: &mut GnitzClient, sn: &str) -> (u64, Schema) {
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::I64, false),
            ColumnDef::new("v", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    };
    let tid = client
        .create_table(sn, "base", &schema, &[], TableProps::default(), &[])
        .unwrap();
    let mut batch = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut batch, &schema);
    for (pk, v) in BASE_ROWS {
        app.add_row(pk as u128, 1).i64_val(v);
    }
    client.push(tid, &schema, &batch).unwrap();
    (tid, schema)
}

/// One identity segment reading `source_id`.
fn segment(source_id: u64, schema: &Schema) -> PlannedView {
    let mut circuit = gnitz_core::Circuit::default();
    let inp = circuit.input_delta(source_id, gnitz_wire::ReadBound::None);
    circuit.sink(inp);
    PlannedView {
        circuit,
        schema: Arc::new(schema.clone()),
        pk_repeats: false,
    }
}

/// A three-view bundle: segment `h` reads base, segment `g` reads `h`, and `f`
/// (the user-named view) reads `g`.
fn chain(base_tid: u64, schema: &Schema) -> ViewBundle {
    ViewBundle {
        segments: vec![segment(base_tid, schema), segment(gnitz_core::segment_id(0), schema)],
        view: segment(gnitz_core::segment_id(1), schema),
    }
}

/// The vids of every live VIEW_TAB row.
fn live_view_vids(client: &mut GnitzClient) -> Vec<u64> {
    let b = client.scan(gnitz_wire::VIEW_TAB).unwrap().batch;
    (0..b.len())
        .filter(|&i| b.weights[i] > 0)
        .map(|i| b.pks.get(gnitz_core::types::sys_schema(gnitz_wire::VIEW_TAB), i) as u64)
        .collect()
}

/// The vids of every live VIEW_TAB row naming `owner_vid` as its owner.
fn segment_vids_of(client: &mut GnitzClient, owner_vid: u64) -> Vec<u64> {
    let b = client.scan(gnitz_wire::VIEW_TAB).expect("scan VIEW_TAB").batch;
    let view_tab = gnitz_core::types::sys_schema(gnitz_wire::VIEW_TAB);
    let owners = &b.payload[gnitz_wire::VIEWTAB_PAY_OWNER_VIEW_ID].bytes;
    (0..b.len())
        .filter(|&i| b.weights[i] > 0)
        .filter(|&i| u64::from_le_bytes(owners[i * 8..i * 8 + 8].try_into().unwrap()) == owner_vid)
        .map(|i| b.pks.get(view_tab, i) as u64)
        .collect()
}

/// `(pk, v, weight)` of every row a `(pk, v)` relation holds, sorted.
fn weighted_rows(client: &mut GnitzClient, id: u64) -> Vec<(i64, i64, i64)> {
    let gnitz_core::ScanReply { schema, batch: b, .. } = client.scan(id).unwrap();
    let vs = &b.payload[0].bytes;
    let mut out: Vec<(i64, i64, i64)> = (0..b.len())
        .map(|i| {
            let v = i64::from_le_bytes(vs[i * 8..i * 8 + 8].try_into().unwrap());
            (b.pks.get(&schema, i) as i64, v, b.weights[i])
        })
        .collect();
    out.sort();
    out
}

fn base_at_weight_one() -> Vec<(i64, i64, i64)> {
    BASE_ROWS.iter().map(|&(pk, v)| (pk, v, 1)).collect()
}

#[test]
fn a_chain_bundle_backfills_cascades_on_drop_and_survives_a_rename() {
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema("chain");
    client.create_schema(&sn).unwrap();
    let (base_tid, schema) = make_base(&mut client, &sn);

    let owner = client
        .create_view_chain(&sn, "f", chain(base_tid, &schema), ViewProps::default(), false)
        .unwrap();
    assert_eq!(
        owner,
        client.resolve_table_or_view_id(&sn, "f").unwrap().0,
        "the returned id is the user view's"
    );
    let mut segs = segment_vids_of(&mut client, owner);
    segs.sort();
    assert_eq!(segs.len(), 2, "both segments name the user view as owner");
    let vids: Vec<u64> = segs.iter().copied().chain([owner]).collect();

    // Every view holds the base's rows at weight 1.
    for &vid in &vids {
        assert_eq!(weighted_rows(&mut client, vid), base_at_weight_one(), "view {vid}");
    }

    let err = client.drop_table(&sn, &["base"], false).unwrap_err().to_string();
    assert!(err.contains("View dependency"), "got: {err}");

    // A rename is a net-live rewrite of the owner's row: the cascade must not fire.
    client.alter_rename_relation(&sn, "f", "renamed").unwrap();
    let mut segs = segment_vids_of(&mut client, owner);
    segs.sort();
    assert_eq!(segs, vids[..2], "a rename keeps the owner's segments");
    assert_eq!(weighted_rows(&mut client, owner), base_at_weight_one());

    client.drop_view(&sn, &["renamed"], false).unwrap();
    for &vid in &vids {
        assert!(client.describe_by_id(vid).is_err(), "view {vid} must be gone");
    }
    client.drop_table(&sn, &["base"], false).unwrap();
}

#[test]
fn a_bundle_is_refused_whole_on_a_name_collision_or_over_the_segment_cap() {
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema("chain");
    client.create_schema(&sn).unwrap();
    let (base_tid, schema) = make_base(&mut client, &sn);

    // A committed view whose name the bundle's user-named view reuses.
    let taken = client
        .create_view_chain(
            &sn,
            "taken",
            segment(base_tid, &schema).into(),
            ViewProps::default(),
            false,
        )
        .unwrap();

    let err = client
        .create_view_chain(&sn, "taken", chain(base_tid, &schema), ViewProps::default(), false)
        .unwrap_err()
        .to_string();
    assert!(err.contains("already exists"), "got: {err}");
    // Nothing of the refused bundle committed: the only live views are `taken`
    // and no segment names it — or anything — as owner.
    assert!(segment_vids_of(&mut client, taken).is_empty());
    assert_eq!(live_view_vids(&mut client), vec![taken]);
    assert_eq!(weighted_rows(&mut client, taken), base_at_weight_one());

    // A segment naming a later segment scans a relation no older than itself.
    let forward = ViewBundle {
        segments: vec![segment(gnitz_core::segment_id(1), &schema), segment(base_tid, &schema)],
        view: segment(gnitz_core::segment_id(0), &schema),
    };
    let err = client
        .create_view_chain(&sn, "fwd", forward, ViewProps::default(), false)
        .unwrap_err()
        .to_string();
    assert!(err.contains("not older"), "got: {err}");
    assert_eq!(
        live_view_vids(&mut client),
        vec![taken],
        "a forward reference commits nothing"
    );

    // A node list past the column cap encodes, and the engine refuses it at load.
    let mut wide = Circuit::default();
    let scan = wide.input_delta(base_tid, gnitz_wire::ReadBound::None);
    let proj = wide.map(scan, &vec![0; gnitz_core::MAX_COLUMNS + 1]);
    wide.sink(proj);
    let planned = PlannedView {
        circuit: wide,
        ..segment(base_tid, &schema)
    };
    let err = client
        .create_view_chain(&sn, "wide", planned.into(), ViewProps::default(), false)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(&format!("exceeds cap {}", gnitz_core::MAX_COLUMNS)),
        "got: {err}"
    );
    assert_eq!(
        live_view_vids(&mut client),
        vec![taken],
        "an over-wide circuit commits nothing"
    );

    let over = gnitz_core::MAX_CHAIN_SEGMENTS + 1;
    let planned = ViewBundle {
        segments: (1..over).map(|_| segment(base_tid, &schema)).collect(),
        view: segment(base_tid, &schema),
    };
    let err = client
        .create_view_chain(&sn, "x", planned, ViewProps::default(), false)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(&format!(
            "{over} segments, exceeding the {}-segment limit",
            gnitz_core::MAX_CHAIN_SEGMENTS
        )),
        "got: {err}"
    );
}
