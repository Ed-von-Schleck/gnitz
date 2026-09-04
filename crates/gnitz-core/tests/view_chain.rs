#![cfg(feature = "integration")]

//! The raw `create_view_chain` contract: a bundle is one atomic zone, its row
//! order is meaningless (the engine re-derives dependency order for the
//! backfill), its last element is the user-named view, and the internal
//! segments name that view as their owner — which is what the drop cascade
//! keys on and a rename leaves alone.
//!
//! Every segment here is a shard-free identity view, `input_delta → sink`.
//! Nothing about that shape orders a backfill: it runs no exchange, so no
//! cross-worker barrier can hold a consumer back — correct output rests
//! entirely on the driver visiting a producer before its consumer.

use gnitz_core::{
    BatchAppender, ColData, ColumnDef, GnitzClient, PlannedView, Schema, TableProps, TypeCode, ZSetBatch,
};
use gnitz_test_harness::{unique_schema, ServerHandle};

const BASE_ROWS: [(i64, i64); 3] = [(1, 10), (2, 20), (3, 30)];

/// Base table `base(pk BIGINT PK, v BIGINT)` holding [`BASE_ROWS`]; returns
/// `(tid, its column defs)`.
fn make_base(client: &mut GnitzClient, sn: &str) -> (u64, Vec<ColumnDef>) {
    let cols = vec![
        ColumnDef::new("pk", TypeCode::I64, false),
        ColumnDef::new("v", TypeCode::I64, false),
    ];
    let tid = client
        .create_table(sn, "base", &cols, &[0], TableProps::default(), &[])
        .unwrap();
    let schema = Schema { columns: cols.clone(), pk_cols: vec![0] };
    let mut batch = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut batch, &schema);
    for (pk, v) in BASE_ROWS {
        app.add_row(pk as u128, 1).i64_val(v);
    }
    client.push(tid, &schema, &batch).unwrap();
    (tid, cols)
}

/// One identity segment at chain-local slot `seg` reading `source_id`.
fn segment(seg: u32, source_id: u64, cols: &[ColumnDef]) -> PlannedView {
    let mut cb = gnitz_core::CircuitBuilder::new(source_id);
    let inp = cb.input_delta();
    cb.sink(inp);
    PlannedView {
        seg,
        circuit: cb.build(),
        output_columns: cols.to_vec(),
        pk_cols: vec![0],
        capacity_bytes: None,
        delta_bytes: None,
    }
}

/// A three-element bundle `[g, h, f]` whose two segments are in reverse
/// dependency order: `g` (slot 1) reads `h` (slot 2) reads base, and `f` (slot
/// 0, the user-named view) reads `g`.
fn misordered_chain(base_tid: u64, cols: &[ColumnDef]) -> Vec<PlannedView> {
    vec![
        segment(1, gnitz_core::segment_id(2), cols),
        segment(2, base_tid, cols),
        segment(0, gnitz_core::segment_id(1), cols),
    ]
}

/// The vids of every live VIEW_TAB row naming `owner_vid` as its owner.
fn segment_vids_of(client: &mut GnitzClient, owner_vid: u64) -> Vec<u64> {
    let (_, batch, _) = client.scan(gnitz_wire::VIEW_TAB).expect("scan VIEW_TAB");
    let Some(b) = batch else { return Vec::new() };
    let ColData::Fixed(owners) = &b.columns[gnitz_wire::VIEWTAB_COL_OWNER_VIEW_ID] else {
        panic!("owner_view_id is a fixed-width column");
    };
    (0..b.len())
        .filter(|&i| b.weights[i] > 0)
        .filter(|&i| u64::from_le_bytes(owners[i * 8..i * 8 + 8].try_into().unwrap()) == owner_vid)
        .map(|i| b.pks.get(i) as u64)
        .collect()
}

/// `(pk, v, weight)` of every row a `(pk, v)` relation holds, sorted.
fn weighted_rows(client: &mut GnitzClient, id: u64) -> Vec<(i64, i64, i64)> {
    let (_, batch, _) = client.scan(id).unwrap();
    let Some(b) = batch else { return Vec::new() };
    let ColData::Fixed(vs) = &b.columns[1] else {
        panic!("v is a fixed-width column");
    };
    let mut out: Vec<(i64, i64, i64)> = (0..b.len())
        .map(|i| {
            let v = i64::from_le_bytes(vs[i * 8..i * 8 + 8].try_into().unwrap());
            (b.pks.get(i) as i64, v, b.weights[i])
        })
        .collect();
    out.sort();
    out
}

fn base_at_weight_one() -> Vec<(i64, i64, i64)> {
    BASE_ROWS.iter().map(|&(pk, v)| (pk, v, 1)).collect()
}

#[test]
fn a_misordered_bundle_backfills_cascades_on_drop_and_survives_a_rename() {
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema("chain");
    client.create_schema(&sn).unwrap();
    let (base_tid, cols) = make_base(&mut client, &sn);

    let vids = client
        .create_view_chain(
            &sn,
            "f",
            misordered_chain(base_tid, &cols),
            gnitz_core::ViewReplace::Nothing,
        )
        .unwrap();
    let owner = *vids.last().unwrap();
    assert_eq!(
        owner,
        client.resolve_table_or_view_id(&sn, "f").unwrap().0,
        "vids come back in input order, the user view last"
    );
    let mut segs = segment_vids_of(&mut client, owner);
    segs.sort();
    assert_eq!(
        segs,
        vec![vids[0], vids[1]],
        "both segments name the user view as owner"
    );

    // Every element holds the base's rows at weight 1, whatever its row order.
    for &vid in &vids {
        assert_eq!(weighted_rows(&mut client, vid), base_at_weight_one(), "view {vid}");
    }

    let err = client.drop_table(&sn, "base", false).unwrap_err().to_string();
    assert!(err.contains("View dependency"), "got: {err}");

    // A rename is a net-live rewrite of the owner's row: the cascade must not fire.
    client.alter_rename_relation(&sn, "f", "renamed").unwrap();
    let mut segs = segment_vids_of(&mut client, owner);
    segs.sort();
    assert_eq!(segs, vec![vids[0], vids[1]], "a rename keeps the owner's segments");
    assert_eq!(weighted_rows(&mut client, owner), base_at_weight_one());

    client.drop_view(&sn, "renamed", false).unwrap();
    for &vid in &vids {
        assert!(client.describe_by_id(vid).is_err(), "view {vid} must be gone");
    }
    client.drop_table(&sn, "base", false).unwrap();
}

#[test]
fn a_bundle_is_refused_whole_on_a_name_collision_or_over_the_segment_cap() {
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema("chain");
    client.create_schema(&sn).unwrap();
    let (base_tid, cols) = make_base(&mut client, &sn);

    // A committed view whose name the bundle's user-named view reuses.
    let taken = *client
        .create_view_chain(
            &sn,
            "taken",
            vec![segment(0, base_tid, &cols)],
            gnitz_core::ViewReplace::Nothing,
        )
        .unwrap()
        .last()
        .unwrap();

    let err = client
        .create_view_chain(
            &sn,
            "taken",
            misordered_chain(base_tid, &cols),
            gnitz_core::ViewReplace::Nothing,
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("already exists"), "got: {err}");
    // Nothing of the refused bundle committed: the only live views are `taken`
    // and no segment names it — or anything — as owner.
    assert!(segment_vids_of(&mut client, taken).is_empty());
    let (_, views, _) = client.scan(gnitz_wire::VIEW_TAB).unwrap();
    let live: Vec<u64> = views
        .map(|b| {
            (0..b.len())
                .filter(|&i| b.weights[i] > 0)
                .map(|i| b.pks.get(i) as u64)
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(live, vec![taken]);
    assert_eq!(weighted_rows(&mut client, taken), base_at_weight_one());

    let over = gnitz_core::MAX_CHAIN_SEGMENTS + 1;
    let planned: Vec<PlannedView> = (0..over).map(|_| segment(0, base_tid, &cols)).collect();
    let err = client
        .create_view_chain(&sn, "x", planned, gnitz_core::ViewReplace::Nothing)
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
