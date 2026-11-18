//! Snapshots: one cut of every base table and of the catalog, read a page at a
//! time while writes and DDL go on; and BACKUP, a data directory copied at a
//! checkpoint that a server boots on.

use super::*;
use crate::{BatchAppender, DEFAULT_PAGE_ROWS, PkColumn};
use gnitz_wire::TableDistribution;

const WORKERS: usize = 4;

/// Every row of `tid` as snapshot `id` holds it, read `page_rows` at a time.
fn read(client: &mut GnitzClient, id: u64, tid: u64, schema: &Arc<Schema>, page_rows: u64) -> ZSetBatch {
    let mut all = ZSetBatch::new(schema);
    block_on(client.snapshot_read(id, tid, schema, page_rows, |b| {
        all.extend_from_owned(b);
        Ok::<_, ClientError>(())
    }))
    .unwrap();
    all
}

fn push(client: &mut GnitzClient, tid: u64, schema: &Arc<Schema>, batch: ZSetBatch) {
    block_on(client.push(tid, schema, batch, WireConflictMode::Update)).unwrap();
}

/// Rows `pks` retracted at the payload [`rows`] gives them.
fn retractions(schema: &Schema, pks: impl IntoIterator<Item = u64>) -> ZSetBatch {
    let mut b = rows(schema, pks);
    b.fill_weights(-1);
    b
}

#[test]
fn a_snapshot_reads_the_cut_it_pinned_while_writes_go_on() {
    let srv = ServerHandle::start_n(WORKERS);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (_, tid, schema) = create_table(&mut client, schema_of(&[("pk", TypeCode::U64), ("v", TypeCode::I64)]));
    push(&mut client, tid, &schema, rows(&schema, 0..3000));

    let id = block_on(client.snapshot_open()).unwrap();
    push(&mut client, tid, &schema, retractions(&schema, 0..1000));
    push(&mut client, tid, &schema, rows(&schema, 5000..6000));

    let pinned = weighted_rows(&rows(&schema, 0..3000));
    for page_rows in [1, 7, 1000, DEFAULT_PAGE_ROWS] {
        assert_eq!(
            weighted_rows(&read(&mut client, id, tid, &schema, page_rows)),
            pinned,
            "page_rows={page_rows}"
        );
    }
    let mut live: Vec<u64> = (1000..3000).chain(5000..6000).collect();
    live.sort_unstable();
    assert_eq!(
        weighted_rows(&scan_all(&mut client, tid, &schema)),
        weighted_rows(&rows(&schema, live))
    );
    block_on(client.snapshot_close(id)).unwrap();
}

/// A page after a compound key starts at the next key in the full key order,
/// whichever column it moves in.
#[test]
fn pages_of_a_compound_key_return_every_row_once() {
    let srv = ServerHandle::start_n(WORKERS);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema("c");
    block_on(client.create_schema(&sn, false)).unwrap();
    let cols = vec![
        ColumnDef::new("a", TypeCode::I32, false),
        ColumnDef::new("b", TypeCode::U8, false),
        ColumnDef::new("v", TypeCode::I64, false),
    ];
    let schema = Arc::new(Schema::from_parts(cols, &[0, 1]).unwrap());
    let tid = block_on(client.create_table(&TableDef::new(rel(&sn, "t"), Schema::clone(&schema)))).unwrap();
    let mut batch = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut batch);
    let mut want = Vec::new();
    for a in -40i32..40 {
        for b in [0u8, 1, 254, 255] {
            app.add_row_natives(&[a as i128 as u128, b as u128], 1)
                .int_val(a as i64 * 1000 + b as i64);
            want.push(a as i64 * 1000 + b as i64);
        }
    }
    push(&mut client, tid, &schema, batch);
    let id = block_on(client.snapshot_open()).unwrap();
    for page_rows in [1, 3, 64] {
        let got = read(&mut client, id, tid, &schema, page_rows);
        let mut vals: Vec<i64> = (0..got.len()).map(|i| read_i64_le(got.col(0), i * 8)).collect();
        vals.sort_unstable();
        want.sort_unstable();
        assert_eq!(vals, want, "page_rows={page_rows}");
        assert!(got.weights().all(|w| w == 1));
    }
}

/// Only the owner answers a replicated table with rows; every other worker's
/// empty train still reaches the client, so each page names every worker.
#[test]
fn a_replicated_table_is_read_once() {
    let srv = ServerHandle::start_n(WORKERS);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema("r");
    block_on(client.create_schema(&sn, false)).unwrap();
    let schema = Arc::new(schema_of(&[("pk", TypeCode::U64), ("v", TypeCode::I64)]));
    let props = TableProps {
        distribution: TableDistribution::Replicated,
        ..TableProps::default()
    };
    let tid = block_on(client.create_table(&TableDef {
        props,
        ..TableDef::new(rel(&sn, "t"), Schema::clone(&schema))
    }))
    .unwrap();
    push(&mut client, tid, &schema, rows(&schema, 0..100));
    let id = block_on(client.snapshot_open()).unwrap();
    let page = block_on(client.snapshot_page(id, tid, &[], 40, &schema)).unwrap();
    assert_eq!(page.worker_ends.len(), WORKERS);
    let mut pages = 0;
    let mut all = ZSetBatch::new(&schema);
    block_on(client.snapshot_read(id, tid, &schema, 40, |b| {
        pages += 1;
        all.extend_from_owned(b);
        Ok::<_, ClientError>(())
    }))
    .unwrap();
    assert_eq!(weighted_rows(&all), weighted_rows(&rows(&schema, 0..100)));
    // The owner's share is 10 a page.
    assert_eq!(pages, 11);
}

/// Under `CLUSTER BY` a key prefix, a worker holds whole prefix groups larger
/// than its share of a page; each page still takes every worker's share, so the
/// read takes as many pages as the fullest worker's rows need.
#[test]
fn a_page_takes_every_workers_share_however_its_keys_interleave() {
    let srv = ServerHandle::start_n(WORKERS);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema("k");
    block_on(client.create_schema(&sn, false)).unwrap();
    let cols = vec![
        ColumnDef::new("a", TypeCode::I64, false),
        ColumnDef::new("b", TypeCode::I64, false),
    ];
    let schema = Arc::new(Schema::from_parts(cols, &[0, 1]).unwrap());
    let props = TableProps {
        distribution: TableDistribution::Keyed { prefix_len: 1 },
        ..TableProps::default()
    };
    let tid = block_on(client.create_table(&TableDef {
        props,
        ..TableDef::new(rel(&sn, "t"), Schema::clone(&schema))
    }))
    .unwrap();
    let mut batch = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut batch);
    for a in 0i64..64 {
        for b in 0i64..20 {
            app.add_row_natives(&[a as i128 as u128, b as i128 as u128], 1);
        }
    }
    push(&mut client, tid, &schema, batch);
    let id = block_on(client.snapshot_open()).unwrap();

    let whole = block_on(client.snapshot_page(id, tid, &[], 4000, &schema)).unwrap();
    assert_eq!(whole.batch.len(), 1280);
    let ends = &whole.worker_ends;
    let fullest = (0..ends.len())
        .map(|w| ends[w] - if w == 0 { 0 } else { ends[w - 1] })
        .max()
        .unwrap();

    let (mut pages, mut total) = (0, 0);
    block_on(client.snapshot_read(id, tid, &schema, 40, |b| {
        pages += 1;
        total += b.len();
        Ok::<_, ClientError>(())
    }))
    .unwrap();
    assert_eq!(total, 1280);
    assert_eq!(pages, fullest / 10 + 1);
}

/// The catalog is pinned with the rows: a relation dropped after the pin is
/// still in the snapshot's catalog, and still readable through it.
#[test]
fn a_snapshot_holds_the_catalog_and_the_tables_of_its_cut() {
    let srv = ServerHandle::start_n(WORKERS);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (sn, tid, schema) = create_table(&mut client, schema_of(&[("pk", TypeCode::U64), ("v", TypeCode::I64)]));
    push(&mut client, tid, &schema, rows(&schema, 0..100));
    let id = block_on(client.snapshot_open()).unwrap();

    block_on(client.drop_table(&[rel(&sn, "t")], false)).unwrap();
    let later = schema_of(&[("pk", TypeCode::U64)]);
    block_on(client.create_table(&TableDef::new(rel(&sn, "u"), later.clone()))).unwrap();

    let catalog = block_on(client.snapshot_catalog(id)).unwrap();
    assert!(catalog.get(&rel(&sn, "t")).is_some_and(|r| r.id == tid));
    assert!(catalog.get(&rel(&sn, "u")).is_none());
    assert_eq!(
        weighted_rows(&read(&mut client, id, tid, &schema, 16)),
        weighted_rows(&rows(&schema, 0..100))
    );
    let now = block_on(client.catalog()).unwrap();
    assert!(now.get(&rel(&sn, "t")).is_none());
}

#[test]
fn a_snapshot_holds_the_serial_positions_of_its_cut() {
    let srv = ServerHandle::start_n(WORKERS);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema("s");
    block_on(client.create_schema(&sn, false)).unwrap();
    let schema = schema_of(&[("id", TypeCode::I64), ("v", TypeCode::I64)]);
    let props = TableProps { serial: true, ..TableProps::default() };
    let tid = block_on(client.create_table(&TableDef {
        props,
        ..TableDef::new(rel(&sn, "t"), schema.clone())
    }))
    .unwrap();
    let desc = block_on(client.resolve_relation(&rel(&sn, "t"))).unwrap();
    let base = block_on(client.reserve_serial_ids(&desc, 40_000)).unwrap();
    let id = block_on(client.snapshot_open()).unwrap();
    let serials = block_on(client.snapshot_serials(id)).unwrap();
    assert_eq!(serials.get(&tid).copied(), Some(base as u64 + 39_999));
}

#[test]
fn a_snapshot_is_its_connections_alone_and_ends_with_it() {
    let srv = ServerHandle::start_n(WORKERS);
    let mut owner = GnitzClient::connect(srv.sock_path()).unwrap();
    let (_, tid, schema) = create_table(&mut owner, schema_of(&[("pk", TypeCode::U64), ("v", TypeCode::I64)]));
    push(&mut owner, tid, &schema, rows(&schema, 0..10));
    let id = block_on(owner.snapshot_open()).unwrap();

    let mut other = GnitzClient::connect(srv.sock_path()).unwrap();
    let err = block_on(other.snapshot_page(id, tid, &[], 5, &schema)).unwrap_err();
    assert!(err.to_string().contains("not this connection's"), "{err}");
    assert!(block_on(other.snapshot_close(id)).is_err());

    block_on(owner.snapshot_close(id)).unwrap();
    assert!(block_on(owner.snapshot_page(id, tid, &[], 5, &schema)).is_err());
    assert!(block_on(owner.snapshot_close(id)).is_err());

    // A connection holds a few at most; closing one makes room.
    let ids: Vec<u64> = (0..4).map(|_| block_on(owner.snapshot_open()).unwrap()).collect();
    assert!(block_on(owner.snapshot_open()).is_err());
    block_on(owner.snapshot_close(ids[0])).unwrap();
    block_on(owner.snapshot_open()).unwrap();
    // Dropped with its connection; the server goes on.
    drop(owner);
    let mut again = GnitzClient::connect(srv.sock_path()).unwrap();
    assert_eq!(
        weighted_rows(&scan_all(&mut again, tid, &schema)),
        weighted_rows(&rows(&schema, 0..10))
    );
}

#[test]
fn a_page_in_another_layout_is_refused() {
    let srv = ServerHandle::start_n(WORKERS);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (_, tid, _) = create_table(&mut client, schema_of(&[("pk", TypeCode::U64), ("v", TypeCode::I64)]));
    let id = block_on(client.snapshot_open()).unwrap();
    let wrong = Arc::new(schema_of(&[("pk", TypeCode::U64), ("v", TypeCode::I32)]));
    let err = block_on(client.snapshot_page(id, tid, &[], 5, &wrong)).unwrap_err();
    assert!(err.to_string().contains("layout"), "{err}");
    assert!(block_on(client.snapshot_page(id, tid, &[], 0, &wrong)).is_err());
}

// ── BACKUP ───────────────────────────────────────────────────────────

/// A backup holds every acknowledged write and nothing pushed after it, its
/// views and indexes answer as they did, and it boots at another worker count.
#[test]
fn a_backup_boots_as_the_server_stood_at_its_checkpoint() {
    let srv = ServerHandle::start_with_backups(WORKERS, &[]);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (sn, tid, schema) = create_table(&mut client, schema_of(&[("pk", TypeCode::U64), ("v", TypeCode::I64)]));
    block_on(client.create_view_chain(bundle(rel(&sn, "v"), Vec::new(), segment(tid, &schema)))).unwrap();
    push(&mut client, tid, &schema, rows(&schema, 0..2000));
    push(&mut client, tid, &schema, retractions(&schema, 0..10));

    block_on(client.backup("first")).unwrap();
    push(&mut client, tid, &schema, rows(&schema, 9000..9100));
    let err = block_on(client.backup("first")).unwrap_err();
    assert!(err.to_string().contains("already exists"), "{err}");
    for bad in ["", "../x", ".hidden", "a/b"] {
        assert!(block_on(client.backup(bad)).is_err(), "{bad:?}");
    }

    let entry = srv.backups().join("first");
    assert!(!entry.join("wal.sal").exists(), "a backup carries no log");
    let restored = ServerHandle::start_on(&entry, 2);
    let mut client = GnitzClient::connect(restored.sock_path()).unwrap();
    let want = weighted_rows(&rows(&schema, 10..2000));
    assert_eq!(weighted_rows(&scan_all(&mut client, tid, &schema)), want);
    let view = block_on(client.resolve_relation(&rel(&sn, "v"))).unwrap();
    assert_eq!(weighted_rows(&scan_all(&mut client, view.tid, &view.schema)), want);
    // The restored server is a live one.
    push(&mut client, tid, &schema, rows(&schema, 7000..7001));
    assert_eq!(scan_all(&mut client, tid, &schema).len(), 1991);
}

/// A backup carries no log, so a server restored from one starts a log of its
/// own: what it acknowledges survives its crash, its first DDL included.
#[test]
fn a_restored_backup_recovers_what_it_acknowledged() {
    let srv = ServerHandle::start_with_backups(WORKERS, &[]);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (_, tid, schema) = create_table(&mut client, schema_of(&[("pk", TypeCode::U64), ("v", TypeCode::I64)]));
    push(&mut client, tid, &schema, rows(&schema, 0..2000));
    block_on(client.backup("cut")).unwrap();

    let mut restored = ServerHandle::start_on(&srv.backups().join("cut"), WORKERS);
    let mut client = GnitzClient::connect(restored.sock_path()).unwrap();
    let (sn, created, _) = create_table(&mut client, schema_of(&[("pk", TypeCode::U64), ("v", TypeCode::I64)]));
    push(&mut client, created, &schema, rows(&schema, 0..10));
    drop(client);

    restored.restart().unwrap();
    let mut client = GnitzClient::connect(restored.sock_path()).unwrap();
    let recovered = block_on(client.resolve_relation(&rel(&sn, "t"))).unwrap();
    assert_eq!(recovered.tid, created);
    let want = weighted_rows(&rows(&schema, 0..10));
    assert_eq!(weighted_rows(&scan_all(&mut client, created, &schema)), want);
    assert_eq!(scan_all(&mut client, tid, &schema).len(), 2000);
}

/// A backup interrupted part-way leaves no entry under its name: its copy is
/// made under another, which the entry's takes only once the copy is whole.
#[test]
#[cfg_attr(not(debug_assertions), ignore = "the seam folds away in a release build")]
fn an_interrupted_backup_leaves_no_entry() {
    let srv = ServerHandle::start_with_backups(WORKERS, &[("GNITZ_INJECT_BACKUP_ABORT", "1")]);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (_, tid, schema) = create_table(&mut client, schema_of(&[("pk", TypeCode::U64), ("v", TypeCode::I64)]));
    push(&mut client, tid, &schema, rows(&schema, 0..100));

    assert!(block_on(client.backup("first")).is_err(), "the master dies part-way");
    let backups = srv.backups();
    assert!(
        backups.join(".first.partial").join("_relations").is_dir(),
        "the workers' copies were made before the abort"
    );
    assert!(!backups.join("first").exists(), "an interrupted backup is no entry");
}

#[test]
fn a_server_without_a_backup_directory_refuses_backup() {
    let srv = ServerHandle::start_n(1);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let err = block_on(client.backup("x")).unwrap_err();
    assert!(err.to_string().contains("--backup-dir"), "{err}");
}

/// Keys the engine's router places on every worker, so a page meets all of them.
#[test]
fn every_worker_holds_rows_of_the_tables_these_tests_page() {
    let schema = schema_of(&[("pk", TypeCode::U64), ("v", TypeCode::I64)]);
    let owners: std::collections::HashSet<usize> = (0u64..200)
        .map(|k| {
            let opk = PkColumn::from_natives(&schema, [k as u128]);
            let pk = opk.get_bytes(0);
            gnitz_zset::algebra::Placement::Keyed { dist_stride: pk.len() as u8 }
                .owner(pk, WORKERS)
                .unwrap()
        })
        .collect();
    assert_eq!(owners.len(), WORKERS);
}
