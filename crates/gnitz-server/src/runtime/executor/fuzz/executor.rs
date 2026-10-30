//! The executor's request handling in one process: a seed's edited traffic
//! played through the functions each request reaches in a server, under one
//! catalog that is both the master's and its one worker's.
//!
//! What only the process model has — locks, the SAL, the mesh and the reply
//! frames — the session fuzzer (`gnitz-fuzz`) drives against a real server;
//! this target runs the rest many times faster, and under AddressSanitizer when
//! `cargo bolero` builds it. A server fail-stops on a catalog write that
//! diverges and on a backfill or tick that fails once its DDL is applied, so
//! each is a panic here, and so is a catalog the session leaves that does not
//! open again.
//!
//! A push lands when the master would admit it: under the master's own access
//! rules, a stream's batch as the master checks one, and a table's as an
//! INSERT — weights of one, keys unique within the batch and absent from the
//! store. A transaction lands whole or not at all. The effective delta of any
//! other push is worked out with the workers, which only a server has.

use std::collections::HashSet;

use super::validate::TxnFamily;
use super::write::{check_stream_push, decode_push_frame, decode_push_txn_frame};
use super::*;
use crate::catalog::{SysFamily, ZoneError};
use crate::test_support::{
    col_def, col_tab_batch, left_join_circuit, nullable_def, sys_batch, table_tab_batch, view_blocks, view_tab_row,
};
use gnitz_fuzz::input::Input;
use gnitz_fuzz::seed::{Frame, Seed, Seeds, SEEDS_ENV};
use gnitz_store::read::Probe;
use gnitz_store::relation::RelationKind;
use gnitz_wire::txn_frame::{decode_scan_multi, encode_items, FrameItem, ScanItem};
use gnitz_wire::{append_frame, ControlHeader, PkKeys, ReadSpec};
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::encode_schema_block;

#[test]
fn traffic_fuzz() {
    let seeds = seeds();
    bolero::check!().for_each(|bytes: &[u8]| {
        let input = Input::decode(bytes);
        let seed = &seeds[input.seed as usize % seeds.len()];
        play(&input.edit(&seed.sent));
    });
}

/// The seeds `GNITZ_FUZZ_SEEDS` names, or else [`builtin`].
fn seeds() -> Vec<Seed> {
    let Some(dir) = gnitz_foundation::env::env_path(SEEDS_ENV) else {
        return vec![builtin()];
    };
    let all = Seeds::open(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
    assert!(!all.is_empty(), "{} holds no seeds", dir.display());
    (0..all.len()).map(|i| all.load(i).unwrap()).collect()
}

/// Play `sent` into a fresh catalog, then open what it left.
fn play(sent: &[Frame]) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let mut session = Session::open(path);
    for frame in sent {
        session.request(&frame.bytes);
    }
    session.cat.close();
    CatalogEngine::open(path, 1)
        .unwrap_or_else(|e| panic!("the catalog a session left does not open again: {e}"))
        .close();
}

/// One catalog, and what the requests played into it have left.
struct Session {
    cat: CatalogEngine,
    /// The next tick's round.
    round: u64,
    /// The tables a push has filled.
    filled: HashSet<u64>,
    /// The last snapshot id handed out.
    snapshots: u64,
}

impl Session {
    fn open(path: &str) -> Session {
        Session {
            cat: CatalogEngine::open(path, 1).expect("a fresh catalog opens"),
            round: 1,
            filled: HashSet::new(),
            snapshots: 0,
        }
    }

    /// Run the request `frame`; a refusal is an answer, and changes nothing.
    fn request(&mut self, frame: &[u8]) {
        let Ok(ctrl) = gnitz_wire::peek_control_block(frame) else {
            return;
        };
        let Ok(verb) = ctrl.client_verb() else { return };
        let body = &frame[ctrl.body.clone()];
        match verb {
            ClientVerb::DdlTxn => self.ddl(body),
            ClientVerb::Push => {
                let (tid, mode) = (ctrl.hdr.target_id, ctrl.hdr.flags.conflict_mode);
                let Ok(push) = decode_push_frame(&self.cat, frame, ctrl) else {
                    return;
                };
                let Ok((kind, Some(batch))) = push.admit(&self.cat, Access::Write) else {
                    return;
                };
                let admitted = match kind {
                    RelationKind::Stream => check_stream_push(tid, Some(&batch), mode).is_ok(),
                    _ => self.inserts(tid, &batch),
                };
                if admitted {
                    self.ingest(vec![TxnFamily { tid, mode, batch }]);
                }
            }
            ClientVerb::PushTxn => {
                let Ok(items) = decode_push_txn_frame(&self.cat, body) else {
                    return;
                };
                let Ok(families) = items
                    .into_iter()
                    .map(|item| item.admit(&self.cat).map(|(_, f)| f))
                    .collect::<Result<Vec<_>, _>>()
                else {
                    return;
                };
                if families.iter().all(|f| self.inserts(f.tid, &f.batch)) {
                    self.ingest(families);
                }
            }
            ClientVerb::ScanSpec => self.scan(ScanItem::of(frame, &ctrl)),
            ClientVerb::ScanMulti => {
                for s in decode_scan_multi(body).unwrap_or_default() {
                    self.scan(s);
                }
            }
            ClientVerb::AllocIds => drop(self.cat.allocate_ids(ctrl.hdr.arg0)),
            // One process is both the master and its worker, so it pins both.
            ClientVerb::SnapshotOpen => {
                self.snapshots += 1;
                self.cat.registry.pin(self.snapshots).expect("a fresh snapshot id pins");
            }
            ClientVerb::SnapshotPage => {
                if let Ok(page) = gnitz_wire::snapshot::PageRequest::decode(&frame[ctrl.blob.clone()]) {
                    drop(self.cat.registry.pinned_page(ctrl.hdr.arg0, ctrl.hdr.target_id, &page));
                }
            }
            ClientVerb::SnapshotClose => self.cat.registry.unpin(ctrl.hdr.arg0),
            // Each reads, or moves, nothing a circuit runs over.
            ClientVerb::Resolve
            | ClientVerb::AllocSerialRange
            | ClientVerb::DeltaPoll
            | ClientVerb::SyncPushed
            | ClientVerb::Backup => {}
        }
    }

    /// Apply a DDL bundle, then fill every view it created and tick every
    /// family a view scans, as the master does once the bundle is durable.
    fn ddl(&mut self, body: &[u8]) {
        let Ok(families) = ddl::decode_bundle(body) else { return };
        // The master refuses a unique index over rows a duplicate may be
        // among, by asking its workers; this one has none to ask.
        let unique_over_rows = families
            .unique_index_creates()
            .iter()
            .any(|(owner, _)| self.filled.contains(owner));
        if unique_over_rows {
            return;
        }
        let applied = match self.cat.apply_bundle(families, &mut |_, _| Ok(())) {
            Ok(applied) => applied,
            Err(ZoneError::Refused(_)) => return,
            Err(ZoneError::Diverged(e)) => panic!("a DDL diverged the catalog: {e}"),
        };
        let mut views = applied.created_views;
        // Ascending id order is dependency order.
        views.sort_unstable();
        for view in views {
            crate::query::backfill(&mut self.cat, view)
                .unwrap_or_else(|e| panic!("the backfill of new view {view} failed: {e}"));
        }
        applied.held_above.into_iter().for_each(|family| self.tick(family));
    }

    /// Ingest every one of `families`, which the master admitted, and tick each.
    fn ingest(&mut self, families: Vec<TxnFamily>) {
        for f in families {
            if self.cat.ingest_unticked(f.tid, f.batch).is_ok() {
                self.filled.insert(f.tid);
                self.tick(f.tid);
            }
        }
    }

    /// Whether `batch` is an INSERT into table `tid` as it stands: every weight
    /// one, every key once, and none held already.
    fn inserts(&self, tid: u64, batch: &Batch) -> bool {
        if (0..batch.len()).any(|i| batch.get_weight(i) != 1) {
            return false;
        }
        let stride = batch.schema().pk_stride();
        let mut keys: Vec<&[u8]> = batch.pk().chunks(stride).collect();
        keys.sort_unstable();
        if keys.windows(2).any(|w| w[0] == w[1]) {
            return false;
        }
        self.cat
            .registry
            .probe(tid, Probe::Pk, PkKeys::from_sorted(stride, keys.concat()))
            .is_ok_and(|held| held.is_empty())
    }

    /// Answer the scan `s` as a worker does.
    fn scan(&mut self, s: ScanItem<'_>) {
        let readable = target_kind(&self.cat, s.target, Access::Read).is_ok();
        if let (true, Ok(spec)) = (readable, ReadSpec::decode(s.spec)) {
            let _ = self.cat.scan_spec(s.target.tid, spec, s.reply_layout);
        }
    }

    fn tick(&mut self, source: u64) {
        crate::query::tick(&mut self.cat, source, self.round)
            .unwrap_or_else(|e| panic!("a tick of {source} failed: {e}"));
        self.round += 1;
    }
}

/// A session for a run without recorded seeds: two tables, a LEFT JOIN view
/// over them, and rows into each, framed as a client frames them. The push
/// frames are built under the schemas the DDL frames create, read off a
/// scratch catalog they are played into.
fn builtin() -> Seed {
    use gnitz_wire::TypeCode::U64;
    let scratch = tempfile::tempdir().unwrap();
    let mut session = Session::open(scratch.path().to_str().unwrap());
    let ids = 3;
    // The first ids a fresh catalog allocates.
    let base = gnitz_wire::FIRST_USER_TABLE_ID;
    let (a, b, view) = (base, base + 1, base + 2);
    let table = |tid: u64, name: &str, cols: &[gnitz_wire::CatalogColumn]| {
        ddl_frame([
            (SysFamily::Column, col_tab_batch(tid, cols, 1)),
            (SysFamily::Table, table_tab_batch(&[(tid, name, 1)])),
        ])
    };
    let mut alloc = Vec::new();
    append_frame(
        &mut alloc,
        &ControlHeader::naming(ClientVerb::AllocIds, Target::from(0), ids),
        &[],
        None,
        None,
    );
    let view_cols = [
        col_def("key", U64),
        col_def("id", U64),
        col_def("nn", U64),
        nullable_def("nullable", U64),
        nullable_def("w", U64),
    ];
    let (circuit, _) = left_join_circuit(a, b, 1);
    let ddl = [
        alloc,
        table(
            a,
            "a",
            &[col_def("id", U64), col_def("nn", U64), nullable_def("nullable", U64)],
        ),
        table(b, "b", &[col_def("id", U64), col_def("k", U64), col_def("w", U64)]),
        ddl_frame(view_blocks(
            [(view, &circuit, &view_cols[..])],
            sys_batch([(view_tab_row(view, "v"), 1)]),
        )),
    ];
    ddl.iter().for_each(|f| session.request(f));
    let rows = |tid: u64| {
        let schema = session.cat.registry.relation(tid).expect("the DDL created it").schema();
        let mut rows = BatchBuilder::new(&schema);
        for id in 0..8u64 {
            rows.begin_row(id as u128, 1);
            [id % 3, id].into_iter().for_each(|c| rows.put_u64(c));
            rows.end_row();
        }
        let rows = rows.finish();
        let mut frame = Vec::new();
        let hdr = ControlHeader::naming(ClientVerb::Push, Target::from(tid), 0);
        let schema_block = encode_schema_block(rows.schema());
        append_frame(&mut frame, &hdr, &[], Some(&schema_block), Some(&rows.wire_regions()));
        frame
    };
    let pushes = [rows(a), rows(b)];
    let sent = ddl
        .into_iter()
        .chain(pushes)
        .map(|bytes| Frame { conn: 0, bytes })
        .collect();
    Seed { sent, received: Vec::new() }
}

/// A `DDL_TXN` of `blocks`, one item per family.
fn ddl_frame(blocks: impl IntoIterator<Item = (SysFamily, Batch)>) -> Vec<u8> {
    let blocks: Vec<(SysFamily, Batch)> = blocks.into_iter().collect();
    let items: Vec<FrameItem> = blocks
        .iter()
        .map(|(family, batch)| FrameItem {
            target: family.id().into(),
            conflict_mode: Default::default(),
            basis: 0,
            schema: None,
            data: batch.wire_regions(),
        })
        .collect();
    encode_items(ClientVerb::DdlTxn, &items)
}

/// The built-in seed is a session the target plays through without a refusal:
/// its view ends up holding the join of the rows it pushed. Both sides key on
/// `id % 3` over eight ids, groups of 3, 3 and 2, so every row of `a` matches
/// and the join holds 3·3 + 3·3 + 2·2 rows.
#[test]
fn the_builtin_seed_plays_through() {
    let seed = builtin();
    let dir = tempfile::tempdir().unwrap();
    let mut session = Session::open(dir.path().to_str().unwrap());
    seed.sent.iter().for_each(|f| session.request(&f.bytes));
    assert_eq!(session.filled.len(), 2, "both pushes were admitted");
    let view = session
        .cat
        .registry
        .relation_or_err(session.filled.iter().max().unwrap() + 1)
        .unwrap();
    assert_eq!(crate::test_support::sum_weights(view.cursor()), 22);
}
