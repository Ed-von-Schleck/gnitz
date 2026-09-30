//! The raw `create_view_chain` contract: a bundle is one atomic zone, a view reads
//! only segments before it, and every segment names the user-named view as its
//! owner — which is what the drop cascade keys on and a rename leaves alone.
//!
//! Every segment here is a shard-free identity view, `input_delta → sink`.
//! Nothing about that shape orders a backfill: it runs no exchange, so no
//! cross-worker barrier can hold a consumer back — correct output rests
//! entirely on the driver visiting a producer before its consumer.

use super::*;
use gnitz_core::{PlannedView, ViewBundle};
use gnitz_wire::{Circuit, ViewProps, VIEWTAB_PAY_OWNER_VIEW_ID, VIEW_TAB};

/// Base table `t(pk BIGINT PK, v BIGINT)` in a fresh schema, holding pks 1..=3:
/// `(schema name, tid, schema)`.
fn make_base(client: &mut GnitzClient) -> (String, u64, Arc<Schema>) {
    let (sn, tid, schema) = create_table(client, schema_of(&[("pk", TypeCode::I64), ("v", TypeCode::I64)]));
    client
        .push(tid, &schema, &base_rows(&schema), WireConflictMode::Update)
        .unwrap();
    (sn, tid, schema)
}

fn base_rows(schema: &Schema) -> ZSetBatch {
    rows(schema, 1..=3)
}

/// One identity segment reading `source_id`.
fn segment(source_id: u64, schema: &Arc<Schema>) -> PlannedView {
    let mut circuit = Circuit::default();
    let inp = circuit.input_delta(source_id, ReadBound::None);
    circuit.sink(inp);
    PlannedView {
        circuit,
        schema: Arc::clone(schema),
        pk_repeats: false,
    }
}

/// A three-view bundle: segment `h` reads base, segment `g` reads `h`, and `f`
/// (the user-named view) reads `g`.
fn chain(base_tid: u64, schema: &Arc<Schema>) -> ViewBundle {
    ViewBundle {
        segments: vec![segment(base_tid, schema), segment(gnitz_core::segment_id(0), schema)],
        view: segment(gnitz_core::segment_id(1), schema),
    }
}

/// `(vid, owner_view_id)` of every live VIEW_TAB row, sorted.
fn live_views(client: &mut GnitzClient) -> Vec<(u64, u64)> {
    let view_tab = gnitz_core::sys_schema(VIEW_TAB);
    let b = client
        .scan_spec(VIEW_TAB, &ReadSpec::all_rows(ReadBound::None), view_tab)
        .unwrap()
        .batch;
    let owners = &b.payload[VIEWTAB_PAY_OWNER_VIEW_ID].bytes;
    let mut out: Vec<_> = (0..b.len())
        .filter(|&i| b.weights[i] > 0)
        .map(|i| (b.pks.get(view_tab, i) as u64, gnitz_wire::read_u64_le(owners, i * 8)))
        .collect();
    out.sort();
    out
}

/// Every row view `vid` holds, as [`weighted_rows`].
fn view_rows(client: &mut GnitzClient, vid: u64, schema: &Arc<Schema>) -> Vec<(u64, Vec<i64>, i64)> {
    weighted_rows(&scan_all(client, vid, schema), schema)
}

#[test]
fn a_chain_bundle_backfills_cascades_on_drop_and_survives_a_rename() {
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (sn, base_tid, schema) = make_base(&mut client);

    let owner = client
        .create_view_chain(&sn, "f", chain(base_tid, &schema), ViewProps::default(), None)
        .unwrap();
    assert_eq!(
        owner,
        client.resolve_relation(&sn, "f").unwrap().tid,
        "the returned id is the user view's"
    );
    // One contiguous id run, the user view last; both segments name it as owner.
    let chain_views = vec![(owner - 2, owner), (owner - 1, owner), (owner, 0)];
    assert_eq!(live_views(&mut client), chain_views);

    // Every view holds the base's rows at weight 1.
    let base = weighted_rows(&base_rows(&schema), &schema);
    for &(vid, _) in &chain_views {
        assert_eq!(view_rows(&mut client, vid, &schema), base, "view {vid}");
    }

    let err = client.drop_table(&sn, &["t"], false).unwrap_err().to_string();
    assert!(err.contains("View dependency"), "got: {err}");

    // A rename is a net-live rewrite of the owner's row: the cascade must not fire.
    let f = client.resolve_relation(&sn, "f").unwrap();
    client.alter_rename_relation(&f, "renamed").unwrap();
    assert_eq!(
        live_views(&mut client),
        chain_views,
        "a rename keeps the owner's segments"
    );
    assert_eq!(view_rows(&mut client, owner, &schema), base);

    client.drop_view(&sn, &["renamed"], false).unwrap();
    assert_eq!(live_views(&mut client), [], "the drop cascades to every segment");
    client.drop_table(&sn, &["t"], false).unwrap();
}

#[test]
fn a_bundle_is_refused_whole_on_a_name_collision_or_over_the_segment_cap() {
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (sn, base_tid, schema) = make_base(&mut client);

    // A committed view whose name the bundle's user-named view reuses.
    let taken = client
        .create_view_chain(
            &sn,
            "taken",
            segment(base_tid, &schema).into(),
            ViewProps::default(),
            None,
        )
        .unwrap();

    let err = client
        .create_view_chain(&sn, "taken", chain(base_tid, &schema), ViewProps::default(), None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("already exists"), "got: {err}");
    // Nothing of the refused bundle committed: the only live view is `taken`.
    let only_taken = [(taken, 0)];
    assert_eq!(live_views(&mut client), only_taken);
    assert_eq!(
        view_rows(&mut client, taken, &schema),
        weighted_rows(&base_rows(&schema), &schema)
    );

    // A segment naming a later segment scans a relation no older than itself.
    let forward = ViewBundle {
        segments: vec![segment(gnitz_core::segment_id(1), &schema), segment(base_tid, &schema)],
        view: segment(gnitz_core::segment_id(0), &schema),
    };
    let err = client
        .create_view_chain(&sn, "fwd", forward, ViewProps::default(), None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("not older"), "got: {err}");
    assert_eq!(
        live_views(&mut client),
        only_taken,
        "a forward reference commits nothing"
    );

    // A node list past the column cap encodes, and the engine refuses it at load.
    let mut wide = Circuit::default();
    let scan = wide.input_delta(base_tid, ReadBound::None);
    let proj = wide.map(scan, &vec![0; gnitz_wire::MAX_COLUMNS + 1]);
    wide.sink(proj);
    let planned = PlannedView {
        circuit: wide,
        ..segment(base_tid, &schema)
    };
    let err = client
        .create_view_chain(&sn, "wide", planned.into(), ViewProps::default(), None)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(&format!("exceeds cap {}", gnitz_wire::MAX_COLUMNS)),
        "got: {err}"
    );
    assert_eq!(
        live_views(&mut client),
        only_taken,
        "an over-wide circuit commits nothing"
    );

    let over = gnitz_core::MAX_CHAIN_SEGMENTS + 1;
    let planned = ViewBundle {
        segments: (1..over).map(|_| segment(base_tid, &schema)).collect(),
        view: segment(base_tid, &schema),
    };
    let err = client
        .create_view_chain(&sn, "x", planned, ViewProps::default(), None)
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
