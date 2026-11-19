//! The raw `create_view_chain` contract: a bundle is one atomic zone, a view reads
//! only segments before it, and every segment names the user-named view as its
//! owner — which is what the drop cascade keys on and a rename leaves alone.
//!
//! Every segment here is a shard-free identity view, `input_delta → sink`.
//! Nothing about that shape orders a backfill: it runs no exchange, so no
//! cross-worker barrier can hold a consumer back — correct output rests
//! entirely on the driver visiting a producer before its consumer.

use super::*;
use gnitz_wire::{SysFamily, ViewTabSlot};

/// Base table `t(pk BIGINT PK, v BIGINT)` in a fresh schema, holding pks 1..=3:
/// `(schema name, tid, schema)`.
fn make_base(client: &mut GnitzClient) -> (String, u64, Arc<Schema>) {
    let (sn, tid, schema) = create_table(client, schema_of(&[("pk", TypeCode::I64), ("v", TypeCode::I64)]));
    block_on(client.push(tid, base_rows(&schema), WireConflictMode::Update)).unwrap();
    (sn, tid, schema)
}

fn base_rows(schema: &Arc<Schema>) -> ZSetBatch {
    rows(schema, 1..=3)
}

/// A three-view bundle: segment `h` reads base, segment `g` reads `h`, and `f`
/// (the user-named view) reads `g`.
fn chain(name: RelName, base_tid: u64, schema: &Arc<Schema>) -> ViewBundle {
    let segments = vec![segment(base_tid, schema), segment(crate::pending_id(0), schema)];
    bundle(name, segments, segment(crate::pending_id(1), schema))
}

/// `(vid, owner_view_id)` of every VIEW_TAB row, sorted; each row is held at
/// weight 1.
fn live_views(client: &mut GnitzClient) -> Vec<(u64, u64)> {
    let view_tab = crate::sys_schema(SysFamily::View);
    let b = block_on(client.scan_served(SysFamily::View, &ReadSpec::all_rows(ReadBound::None), view_tab))
        .unwrap()
        .batch;
    let owners = b.view().col_data(ViewTabSlot::owner_view_id as usize);
    assert!(b.view().weights().all(|w| w == 1), "{b:?}");
    let mut out: Vec<_> = (0..b.len())
        .map(|i| (b.pk(i) as u64, gnitz_wire::read_u64_le(owners, i * 8)))
        .collect();
    out.sort();
    out
}

/// Every row view `vid` holds, as [`weighted_rows`].
fn view_rows(client: &mut GnitzClient, vid: u64, schema: &Arc<Schema>) -> Vec<(u64, Vec<i64>, i64)> {
    weighted_rows(&scan_all(client, vid, schema))
}

#[test]
fn a_chain_bundle_backfills_cascades_on_drop_and_survives_a_rename() {
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (sn, base_tid, schema) = make_base(&mut client);

    let owner = block_on(client.create_view_chain(chain(rel(&sn, "f"), base_tid, &schema))).unwrap();
    assert_eq!(
        owner,
        block_on(client.resolve_relation(&rel(&sn, "f"))).unwrap().tid,
        "the returned id is the user view's"
    );
    // One contiguous id run, the user view last; both segments name it as owner.
    let chain_views = vec![(owner - 2, owner), (owner - 1, owner), (owner, 0)];
    assert_eq!(live_views(&mut client), chain_views);

    // The user view holds the base's rows at weight 1, each backfilled through
    // both segments, which hold none once the chain is built.
    let base = weighted_rows(&base_rows(&schema));
    assert_eq!(view_rows(&mut client, owner, &schema), base);
    for segment in [owner - 2, owner - 1] {
        assert_eq!(view_rows(&mut client, segment, &schema), [], "segment {segment}");
    }

    let err = block_on(client.drop_table(&[rel(&sn, "t")], false))
        .unwrap_err()
        .to_string();
    // The table's one direct reader is the chain's first segment; the refusal
    // names the user view that owns it.
    assert!(err.contains(&format!("dependent views ({sn}.f)")), "got: {err}");

    // The catalog holds the chain as its user view, reading the base through
    // both segments, neither a relation of its own.
    let catalog = block_on(client.catalog()).unwrap();
    let f = catalog.get(&RelName::new(&sn, "f").unwrap()).unwrap();
    assert_eq!((f.id, f.reads.as_slice()), (owner, &[base_tid][..]));
    assert!(catalog.relation(owner - 1).is_none() && catalog.relation(owner - 2).is_none());
    let readers: Vec<u64> = catalog.readers(base_tid).map(|r| r.id).collect();
    assert_eq!(readers, [owner]);

    // A rename is a net-live rewrite of the owner's row: the cascade must not fire.
    let f = block_on(client.resolve_relation(&rel(&sn, "f"))).unwrap();
    block_on(client.alter_rename_relation(&f, "renamed")).unwrap();
    assert_eq!(
        live_views(&mut client),
        chain_views,
        "a rename keeps the owner's segments"
    );
    assert_eq!(view_rows(&mut client, owner, &schema), base);

    block_on(client.drop_view(&[rel(&sn, "renamed")], false)).unwrap();
    assert_eq!(live_views(&mut client), [], "the drop cascades to every segment");
    block_on(client.drop_table(&[rel(&sn, "t")], false)).unwrap();
}

#[test]
fn a_bundle_is_refused_whole_on_a_name_collision_or_over_the_segment_cap() {
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (sn, base_tid, schema) = make_base(&mut client);

    // A committed view whose name the bundle's user-named view reuses.
    let alone = bundle(rel(&sn, "taken"), Vec::new(), segment(base_tid, &schema));
    let taken = block_on(client.create_view_chain(alone)).unwrap();

    let err = block_on(client.create_view_chain(chain(rel(&sn, "taken"), base_tid, &schema)))
        .unwrap_err()
        .to_string();
    assert!(err.contains("already exists"), "got: {err}");
    // Nothing of the refused bundle committed: the only live view is `taken`.
    let only_taken = [(taken, 0)];
    assert_eq!(live_views(&mut client), only_taken);
    assert_eq!(
        view_rows(&mut client, taken, &schema),
        weighted_rows(&base_rows(&schema))
    );

    // A segment naming a later segment scans a relation no older than itself.
    let forward = bundle(
        rel(&sn, "fwd"),
        vec![segment(crate::pending_id(1), &schema), segment(base_tid, &schema)],
        segment(crate::pending_id(0), &schema),
    );
    let err = block_on(client.create_view_chain(forward)).unwrap_err().to_string();
    assert!(err.contains("not older"), "got: {err}");
    assert_eq!(
        live_views(&mut client),
        only_taken,
        "a forward reference commits nothing"
    );

    // A map slot list past the column cap encodes, and the engine refuses it at load.
    let mut wide = Circuit::default();
    let scan = wide.input_delta(base_tid, ReadBound::None);
    let slots = vec![(gnitz_wire::TypeCode::I64, false); gnitz_wire::MAX_COLUMNS + 1];
    wide.map_expr(scan, gnitz_wire::ComputeMap { program: Vec::new(), out_cols: slots });
    let planned = PlannedView {
        circuit: wide,
        ..segment(base_tid, &schema)
    };
    let err = block_on(client.create_view_chain(bundle(rel(&sn, "wide"), Vec::new(), planned)))
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
}

#[test]
fn a_schema_drop_retires_a_chain_with_its_segments() {
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (sn, base_tid, schema) = make_base(&mut client);
    block_on(client.create_view_chain(chain(rel(&sn, "f"), base_tid, &schema))).unwrap();
    assert_eq!(live_views(&mut client).len(), 3);

    block_on(client.drop_schema(&[sn.as_str()], true, false)).unwrap();
    assert_eq!(live_views(&mut client), []);
}

#[test]
fn a_multi_scan_reads_a_system_family_between_fanned_relations() {
    let srv = ServerHandle::start_n(4);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (sn, base_tid, schema) = make_base(&mut client);
    let alone = bundle(rel(&sn, "f"), Vec::new(), segment(base_tid, &schema));
    let vid = block_on(client.create_view_chain(alone)).unwrap();

    let view_tab = crate::sys_schema(SysFamily::View);
    let all = ReadSpec::all_rows(ReadBound::None);
    let scans = [
        (base_tid.into(), &all, &schema),
        (SysFamily::View.into(), &all, view_tab),
        (vid.into(), &all, &schema),
    ];
    let replies = block_on(client.scan_many(&scans)).unwrap();
    let base = weighted_rows(&base_rows(&schema));
    assert_eq!(weighted_rows(&replies[0].batch), base, "the table");
    let views = &replies[1].batch;
    let listed: Vec<u64> = (0..views.len())
        .filter(|&i| views.view().get_weight(i) > 0)
        .map(|i| views.pk(i) as u64)
        .collect();
    assert_eq!(listed, [vid], "the family lists the one view");
    assert_eq!(weighted_rows(&replies[2].batch), base, "the view");
}
