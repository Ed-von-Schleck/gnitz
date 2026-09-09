use super::*;

use super::super::flush_barrier::FlushRound;
use super::super::run_set::FOLD_THRESHOLD;
use super::super::shard_index::L0_COMPACT_THRESHOLD;
use gnitz_expr::RowSource;

/// Payload column 0 (an 8-byte integer) of a located row — the one read
/// every `retract_pk` assertion makes.
fn row_val(fr: &StoredRow) -> i64 {
    i64::from_le_bytes(RowSource::get_col_ptr(&fr.run, fr.row, 0, 8).try_into().unwrap())
}
use crate::schema::{type_code, SchemaDescriptor};
use crate::test_support::{
    make_batch_opk, make_batch_raw, make_schema_u64_i64, opk_pk, pk_payload_schema, wide_pk_3xu64_schema, wide_row,
};

/// Unsorted `Raw` rows for the U64+I64 schema; the ingest path runs the
/// canonical sort+fold.
fn make_batch(rows: &[(u64, i64, i64)]) -> Batch {
    make_batch_raw(&make_schema_u64_i64(), rows)
}

/// `Table::new(...)` at a `budget`-byte memtable, with the per-test boilerplate
/// folded away.
fn new_table(dir: &std::path::Path, schema: SchemaDescriptor, id: u32, budget: usize, rs: RecoverySource) -> Table {
    let mut t = Table::new(dir.to_str().unwrap(), schema, id, rs, StoreBudgets::default()).unwrap();
    t.set_memtable_budget(budget);
    t
}

/// Native-`u128` oracles over the byte-keyed production entry points, which key
/// on verbatim OPK bytes throughout: these `opk_key` the value first, so feeding
/// one an already-encoded key would double the sign flip.
impl Table {
    fn has_pk(&self, key: u128) -> bool {
        let opk = crate::schema::key::opk_key(&self.shard_index.schema, &key.to_le_bytes());
        self.has_pk_bytes(opk.pk_bytes())
    }

    fn retract_pk(&self, key: u128) -> (i64, Option<StoredRow>) {
        let opk = crate::schema::key::opk_key(&self.shard_index.schema, &key.to_le_bytes());
        self.live_row_at(opk.pk_bytes())
    }
}

/// Compaction output rather than flat spill? The `_L` level segment separates
/// the two grammars. Only these tests ask — production reads each shard's level
/// off the manifest.
fn is_compaction_output(name: &str) -> bool {
    name.contains("_L")
}

/// Names of the "flat" `shard_{table_id}_{lsn}.db` files directly in `dir` —
/// the unified spill/barrier naming. Excludes L1+ compaction outputs
/// (`shard_{tid}_{seq}_L{n}_P{part}.db`, distinguished by the `_L` level
/// marker).
fn shard_db_files(dir: &std::path::Path, table_id: u32) -> Vec<String> {
    let prefix = super::super::naming::shard_prefix(table_id);
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.starts_with(&prefix) && n.ends_with(".db") && !is_compaction_output(n))
                .collect()
        })
        .unwrap_or_default()
}

/// Count files directly in `dir` whose basename satisfies `pred`.
fn count_files(dir: &std::path::Path, pred: impl Fn(&str) -> bool) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| rd.flatten().filter(|e| pred(&e.file_name().to_string_lossy())).count())
        .unwrap_or(0)
}

/// Count compaction-output files for `table_id` — named
/// `shard_{tid}_{seq}_L{n}_P{part}.db`; the `_L` marker distinguishes them
/// from flat spill/barrier shards. Presence proves a compaction ran.
fn compaction_output_count(dir: &std::path::Path, table_id: u32) -> usize {
    let shard = super::super::naming::shard_prefix(table_id);
    count_files(dir, |n| n.starts_with(&shard) && is_compaction_output(n))
}

/// Count every on-disk shard/compaction-output file for `table_id`.
fn all_shard_file_count(dir: &std::path::Path, table_id: u32) -> usize {
    let shard = super::super::naming::shard_prefix(table_id);
    count_files(dir, |n| n.starts_with(&shard))
}

/// Run a full synchronous force-publish flush stamped at `generation` —
/// the ephemeral checkpoint round's prepare + commit, minus the fsyncs the
/// tests don't observe.
fn flush_ephemeral_at(t: &mut Table, generation: u64) {
    if let Some(work) = t.flush_prepare(FlushRound::Ephemeral(generation)).unwrap() {
        let _ = t.flush_commit(work).unwrap();
        t.drain_deletions();
    }
}

/// Materialize `open_cursor` into a (pk -> net_weight) map.
fn materialize_weights(t: &Table) -> std::collections::HashMap<u64, i64> {
    let mut out = std::collections::HashMap::new();
    let batch = t.open_cursor().materialize();
    for i in 0..batch.count {
        out.insert(batch.get_pk(i) as u64, batch.get_weight(i));
    }
    out
}
/// The table's basic contract in both recovery modes: ingested rows are
/// visible, absent keys are not, and they survive a flush. A durable table is
/// additionally reopened from its manifest — the property that separates the
/// two modes.
#[test]
fn table_lifecycle_serves_rows_across_flush_and_reopen() {
    for (tid, rs, reopens) in [
        (100, RecoverySource::Rederive { resume_at: None }, false),
        (200, RecoverySource::SalReplay, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("lifecycle");
        let schema = make_schema_u64_i64();
        let mut t = new_table(&tdir, schema, tid, 1 << 20, rs);
        assert!(t.memtable.is_empty());

        t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();
        assert!(t.has_pk(10), "table {tid}");
        assert!(t.has_pk(20));
        assert!(!t.has_pk(99), "table {tid}: an absent key must not be found");

        t.flush().unwrap();
        assert!(t.has_pk(10), "table {tid}: row must survive the flush");
        assert!(t.has_pk(20));

        if reopens {
            let t2 = new_table(&tdir, schema, tid, 1 << 20, rs);
            assert!(t2.has_pk(10), "table {tid}: row must reload from the manifest");
            assert!(t2.has_pk(20));
        }
    }
}

#[test]
fn table_retract_pk() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("retract_test");
    let schema = make_schema_u64_i64();

    let mut t = new_table(
        &tdir,
        schema,
        400,
        1 << 20,
        RecoverySource::Rederive { resume_at: None },
    );

    t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();

    let (w, found) = t.retract_pk(10);
    assert_eq!(w, 1);
    assert!(found.is_some());
    // The retracted row is the found row: a valid null word and an
    // accessible payload column, read through the RowSource view.
    let fr = found.expect("retracted row is the found row");
    assert_ne!(RowSource::get_null_word(&fr.run, fr.row), u64::MAX);
    assert_eq!(row_val(&fr), 100);

    let (w, found) = t.retract_pk(99);
    assert_eq!(w, 0);
    assert!(found.is_none());
}
/// After INSERT then UPDATE (which adds a retraction for the old payload and
/// an insertion for the new payload), `retract_pk` must return the NEW payload,
/// not the cancelled old one.
#[test]
fn test_retract_pk_after_update() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("retract_update_test");
    let schema = make_schema_u64_i64();

    let mut t = new_table(
        &tdir,
        schema,
        600,
        1 << 20,
        RecoverySource::Rederive { resume_at: None },
    );

    // Batch 1: INSERT (PK=10, weight=+1, val=100)
    t.ingest_owned_batch(make_batch(&[(10, 1, 100)])).unwrap();

    // Batch 2: UPDATE delta — retract val=100, insert val=200
    // Rows sorted by (PK, payload): (-1 for val=100) before (+1 for val=200)
    t.ingest_owned_batch(make_batch(&[(10, -1, 100), (10, 1, 200)]))
        .unwrap();

    // Net state: val=100 has weight 0 (cancelled), val=200 has weight 1
    let (w, found) = t.retract_pk(10);
    assert_eq!(w, 1);
    assert!(found.is_some());

    // The found row must be val=200, not the cancelled val=100
    let fr = found.expect("retracted row is the found row");
    let val = row_val(&fr);
    assert_eq!(
        val, 200,
        "retract_pk must return the live (val=200) row, not the retracted val=100"
    );
}

/// `ingest_owned_batch` must sort the batch before memtable insert, even
/// when the incoming Batch has `sorted=false` (reverse order).
#[test]
fn test_ingest_owned_batch_unsorted() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("ingest_owned_unsorted_test");
    let schema = make_schema_u64_i64();

    let mut t = new_table(
        &tdir,
        schema,
        700,
        1 << 20,
        RecoverySource::Rederive { resume_at: None },
    );

    // Build a reverse-sorted batch (PK order: 30, 20, 10).
    let batch = make_batch(&[(30, 1, 300), (20, 1, 200), (10, 1, 100)]);
    // make_batch produces a Raw (unsorted) batch.
    assert!(!batch.is_consolidated());

    t.ingest_owned_batch(batch).unwrap();

    // Cursor must yield rows in ascending PK order
    let cursor = t.open_cursor();
    assert!(cursor.valid);
    assert_eq!(
        cursor.current_key_narrow() as u64,
        10,
        "cursor should start at PK=10 (smallest)"
    );
}

/// Two ingest calls that cumulatively overflow the memtable's byte budget
/// must produce an L0 shard and an empty memtable with no explicit flush().
#[test]
fn test_memtable_overflow_auto_flush() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("overflow_auto_flush");
    let schema = make_schema_u64_i64();

    // Memtable budget = 96 bytes, so a second ingest overflows it.
    // Each row is 32 bytes (PK 8 + weight 8 + null_bmp 8 + col 8).
    // First call: 2 rows = 64 bytes, below threshold → no flush.
    // Second call: pre-check 64 < 96 → no pre-flush; upsert → 128 > 96 → post-flush.
    let mut t = new_table(&tdir, schema, 1200, 96, RecoverySource::Rederive { resume_at: None });

    t.ingest_owned_batch(make_batch(&[(1, 1, 10), (2, 1, 20)])).unwrap();
    assert!(!t.memtable.is_empty(), "two rows must not yet trigger overflow");

    t.ingest_owned_batch(make_batch(&[(3, 1, 30), (4, 1, 40)])).unwrap();

    assert!(t.memtable.is_empty(), "overflow post-check must auto-flush");
    // Non-durable flushes land in the in-memory run set, not disk shards.
    assert!(
        !t.ram_tier.is_empty(),
        "at least one in-memory L0 run must exist after overflow flush",
    );
    assert!(
        t.all_shard_arcs().is_empty(),
        "sub-ceiling ephemeral flush must not write a disk shard",
    );
    // Data from both batches must still be readable.
    for pk in [1u128, 2, 3, 4] {
        assert!(t.has_pk(pk), "PK {pk} must survive the in-memory flush");
    }
}

/// Bug 2: INSERT (PK=10, val=100) → flush → UPDATE delta → flush → retract_pk.
/// The shard fallback must pick the live payload (val=200), not the cancelled one.
#[test]
fn test_retract_pk_shard_fallback_multiple_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("retract_shard_fallback");
    let schema = make_schema_u64_i64();

    // Durable: `retract_pk` is base-table-only (base tables are durable), so
    // the flushed rows must land in a real shard for the shard-fallback path
    // under test — not the RAM tier (which a non-durable flush would use).
    let mut t = new_table(&tdir, schema, 1000, 1 << 20, RecoverySource::SalReplay);

    // Batch 1: INSERT (PK=10, weight=+1, val=100)
    t.ingest_owned_batch(make_batch(&[(10, 1, 100)])).unwrap();
    t.flush().unwrap();

    // Batch 2: UPDATE delta — retract val=100, insert val=200
    t.ingest_owned_batch(make_batch(&[(10, -1, 100), (10, 1, 200)]))
        .unwrap();
    t.flush().unwrap();

    // Both batches are now in shards, memtable is empty.
    // retract_pk must find val=200 (net weight 1), not val=100 (net weight 0).
    let (w, found) = t.retract_pk(10);
    assert_eq!(w, 1);
    assert!(found.is_some());

    let fr = found.expect("retracted row is the found row");
    let val = row_val(&fr);
    assert_eq!(
        val, 200,
        "shard fallback must pick live payload (val=200), not cancelled (val=100)"
    );
}

/// Dropping a staged `FlushWork` without committing must unlink the staged
/// manifest `.tmp`, leaving the directory clean for a future retry. The
/// folded shard was written at its final name (not a `.tmp`) and registered
/// in the index by `flush_prepare`, so it survives as an orphan the next
/// open's `gc_orphans` reclaims — no `.tmp` residue either way.
#[test]
fn flush_prepare_drop_cleans_tmp_files() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("drop_clean_test");
    let schema = make_schema_u64_i64();

    let mut t = new_table(&tdir, schema, 1100, 1 << 20, RecoverySource::SalReplay);

    t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();

    let work = t
        .flush_prepare(FlushRound::Base)
        .unwrap()
        .expect("expected a staged publish, got none");
    let dir_entries: Vec<String> = std::fs::read_dir(&tdir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();
    assert!(
        dir_entries.iter().any(|n| n == "manifest.bin.tmp"),
        "the staged manifest .tmp must exist before Drop, got {dir_entries:?}"
    );
    drop(work);

    let leftover_tmp: Vec<String> = std::fs::read_dir(&tdir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| n.ends_with(".tmp"))
        .collect();
    assert!(
        leftover_tmp.is_empty(),
        "Drop must unlink all .tmp files, found: {leftover_tmp:?}"
    );
}

/// A table's layout sequence is loaded at open and re-stamped by every
/// publish, so it survives an arbitrary number of checkpoints. That
/// persistence is what lets the boot relayout resolve two complete sets after
/// a crash by picking the newer one — a publish that reset it to 0 would make
/// a stale set win.
#[test]
fn base_publish_preserves_the_layout_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("layout_seq");
    let schema = make_schema_u64_i64();
    let cpath = crate::storage::cstr(super::super::manifest::path(tdir.to_str().unwrap())).unwrap();
    let peek_layout_seq = |p: &std::ffi::CStr| super::super::manifest::peek_header(p).unwrap().map(|h| h.layout_seq);

    // Stamp a set at sequence 7, as the relayout's target write would.
    {
        let mut t = new_table(&tdir, schema, 88, 1 << 20, RecoverySource::SalReplay);
        t.ingest_owned_batch(make_batch(&[(1, 1, 10)])).unwrap();
        t.flush().unwrap();
        let (entries, header) = super::super::manifest::read_file(&cpath).unwrap().unwrap();
        super::super::manifest::prepare_file(
            &cpath,
            &entries,
            super::super::manifest::ManifestHeader { layout_seq: 7, ..header },
        )
        .unwrap()
        .commit()
        .unwrap();
    }

    // Two further checkpoints, one with new rows and one without, must both
    // re-stamp 7 rather than reset it.
    let mut t = new_table(&tdir, schema, 88, 1 << 20, RecoverySource::SalReplay);
    t.ingest_owned_batch(make_batch(&[(2, 1, 20)])).unwrap();
    t.flush().unwrap();
    assert_eq!(peek_layout_seq(&cpath), Some(7));
    t.flush().unwrap();
    assert_eq!(
        peek_layout_seq(&cpath),
        Some(7),
        "an unchanged publish must not reset the layout sequence"
    );
}

/// Table::new on a corrupted manifest must return Err and must not run
/// gc_orphans — any stray shard files must survive untouched.
#[test]
fn table_new_corrupted_manifest_preserves_stray_shard() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("corrupted_manifest_test");
    std::fs::create_dir_all(&tdir).unwrap();
    let schema = make_schema_u64_i64();

    // Write a corrupted manifest (wrong magic).
    let manifest_path = tdir.join("manifest.bin");
    std::fs::write(&manifest_path, b"not a valid manifest").unwrap();

    // Drop a stray shard file.
    let stray = tdir.join(super::super::naming::spill_shard_name(200, 1));
    std::fs::write(&stray, b"orphan").unwrap();

    let result = Table::new(
        tdir.to_str().unwrap(),
        schema,
        200,
        RecoverySource::SalReplay,
        StoreBudgets::default(),
    );
    assert!(result.is_err(), "Table::new must fail on corrupted manifest");
    assert!(stray.exists(), "stray shard must survive when gc_orphans did not run");
}

/// Non-durable `flush_prepare` consolidates the snapshot into the in-memory
/// run set and stages nothing; the memtable is reset, no shard file is
/// written, and the rows stay visible to subsequent reads.
#[test]
fn flush_prepare_non_durable_done_inline() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("done_inline_test");
    let schema = make_schema_u64_i64();

    let mut t = new_table(
        &tdir,
        schema,
        1200,
        1 << 20,
        RecoverySource::Rederive { resume_at: None },
    );

    t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200)])).unwrap();
    assert!(
        t.flush_prepare(FlushRound::Base).unwrap().is_none(),
        "a rederived base round publishes nothing"
    );
    assert!(
        t.memtable.is_empty(),
        "memtable must be reset after non-durable flush_prepare"
    );
    assert!(!t.ram_tier.is_empty(), "snapshot must land in the RAM tier");
    assert!(
        shard_db_files(&tdir, 1200).is_empty(),
        "Rederive flush must not write a shard file"
    );
    assert!(t.has_pk(10));
    assert!(t.has_pk(20));
}

/// Wide (`pk_stride = 24`) PK in every tier its rows can live in.
/// `has_pk_bytes`/`live_row_at` must resolve prefix-twins — keys sharing
/// their OPK 16-byte prefix and differing only in the trailing column —
/// independently, so a retraction nets against the twin it names and no other.
///
/// `RecoverySource` picks the tier: a durable table flushes to a real shard, an
/// ephemeral one folds into the RAM tier. The lookup machinery is tier-agnostic,
/// so one body drives both.
#[test]
fn wide_pk_membership_and_retract_resolve_twins_in_every_tier() {
    for (tid, rs) in [
        (4242, RecoverySource::SalReplay),
        (5002, RecoverySource::Rederive { resume_at: None }),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let schema = wide_pk_3xu64_schema();
        assert_eq!(schema.pk_stride(), 24);
        let mut t = new_table(dir.path(), schema, tid, 1 << 20, rs);

        let pk3 = |a: u64, b: u64, c: u64| opk_pk(&schema, &[a as u128, b as u128, c as u128]);
        let twin_a = pk3(1, 1, 100);
        let twin_b = pk3(1, 1, 200);
        let other = pk3(2, 0, 0);

        t.ingest_owned_batch(make_batch_opk(
            &schema,
            &[(&twin_a, 1, 10), (&twin_b, 1, 20), (&other, 1, 30)],
        ))
        .unwrap();

        // Memtable lookups, before anything reaches a tier.
        for k in [&twin_a, &twin_b, &other] {
            assert!(t.has_pk_bytes(k), "table {tid}: key must be found in the memtable");
        }
        assert!(!t.has_pk_bytes(&pk3(1, 1, 300)), "absent prefix-twin must not be found");
        assert!(!t.has_pk_bytes(&pk3(9, 9, 9)));

        // The same keys, now served out of whichever tier the flush chose.
        t.flush().unwrap();
        for k in [&twin_a, &twin_b, &other] {
            assert!(t.has_pk_bytes(k), "table {tid}: key must survive the flush");
        }
        assert!(!t.has_pk_bytes(&pk3(1, 1, 300)));

        // A retraction in the memtable nets against the tier's copy...
        t.ingest_owned_batch(make_batch_opk(&schema, &[(&twin_a, -1, 10)]))
            .unwrap();
        assert!(!t.has_pk_bytes(&twin_a), "table {tid}: retracted twin must be gone");
        assert!(t.has_pk_bytes(&twin_b), "table {tid}: the other twin must survive");

        // ...and still nets once the retraction is itself in a tier.
        t.flush().unwrap();
        assert!(!t.has_pk_bytes(&twin_a));
        assert!(t.has_pk_bytes(&twin_b));

        let (w, found) = t.live_row_at(&twin_b);
        assert_eq!(w, 1);
        assert_eq!(
            row_val(&found.expect("live twin is the found row")),
            20,
            "found row must be the surviving twin's payload"
        );
        let (w2, found2) = t.live_row_at(&twin_a);
        assert_eq!(w2, 0, "net-zero twin reports absent");
        assert!(found2.is_none());
    }
}

/// Wide (`pk_stride = 24`) PK with a *signed* leading column, in every tier.
/// OPK must order a negative leading value before a positive one end to end —
/// compare, scan, membership, retract. This is the case where hand-written
/// big-endian bytes are wrong and only the encoder's sign-flip is correct.
#[test]
fn signed_compound_pk_keeps_opk_order_in_every_tier() {
    // (I64, U64, U64) PK [stride 24, wide] + I64 payload used as an order marker.
    let schema = pk_payload_schema(&[type_code::I64, type_code::U64, type_code::U64]);
    assert_eq!(schema.pk_stride(), 24);
    let key = |a: i64, b: u64, c: u64| opk_pk(&schema, &[a as u128, b as u128, c as u128]);

    // Plain big-endian (no flip) would place 0xFF.. (negatives) after 0x00..
    // (positives) and fail this.
    assert_eq!(
        crate::schema::key::compare_pk_bytes(&key(-1, 0, 0), &key(1, 0, 0)),
        std::cmp::Ordering::Less,
        "OPK must order a negative signed PK column before a positive one",
    );

    for (tid, rs) in [
        (4243, RecoverySource::SalReplay),
        (5003, RecoverySource::Rederive { resume_at: None }),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut t = new_table(dir.path(), schema, tid, 1 << 20, rs);

        // Payload marker == the signed leading value, so scan order is read
        // back without decoding the PK. `w` lets the same builder emit the
        // DBSP -1 retraction row below.
        let row = |a: i64, w: i64| wide_row(&schema, &key(a, 0, 0), w, a);
        for a in [3i64, -5, 0, -1] {
            // scrambled insertion order
            t.ingest_owned_batch(row(a, 1)).unwrap();
        }
        t.flush().unwrap();

        assert!(t.has_pk_bytes(&key(-5, 0, 0)), "table {tid}");
        assert!(t.has_pk_bytes(&key(3, 0, 0)));
        assert!(!t.has_pk_bytes(&key(-2, 0, 0)), "absent signed key must not be found");

        // full_scan returns rows in OPK (= typed signed) order: -5, -1, 0, 3.
        // A missing sign-flip would scan back as 0, 3, -5, -1 and fail here.
        let scanned = t.full_scan();
        let payloads: Vec<i64> = (0..scanned.count)
            .map(|r| i64::from_le_bytes(scanned.get_col_ptr(r, 0, 8).try_into().unwrap()))
            .collect();
        assert_eq!(
            payloads,
            vec![-5, -1, 0, 3],
            "table {tid}: wide signed compound PK must scan back in sign-flipped order",
        );

        // A read-only probe reports the live (weight, row) for the signed key.
        let (w, found) = t.live_row_at(&key(-5, 0, 0));
        assert_eq!(w, 1);
        assert_eq!(
            row_val(&found.expect("signed key is the found row")),
            -5,
            "found row payload marks the probed signed key"
        );

        // The row itself goes only via a DBSP -1 ingest, netting the +1 to zero.
        t.ingest_owned_batch(row(-5, -1)).unwrap();
        t.flush().unwrap();
        assert!(
            !t.has_pk_bytes(&key(-5, 0, 0)),
            "table {tid}: retracted signed key is gone"
        );
    }
}

// ── In-memory Rederive flush (RAM tier, no file I/O) ─────────────────

/// A sub-ceiling Rederive flush writes no file at all; rows are served
/// from the RAM tier via the cursor.
#[test]
fn nondurable_flush_writes_no_file() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("no_file_test");
    let schema = make_schema_u64_i64();
    let mut t = new_table(
        &tdir,
        schema,
        100,
        1 << 20,
        RecoverySource::Rederive { resume_at: None },
    );

    t.ingest_owned_batch(make_batch(&[(10, 1, 100), (20, 1, 200), (30, 1, 300)]))
        .unwrap();
    assert!(t.flush_prepare(FlushRound::Base).unwrap().is_none());

    assert!(
        shard_db_files(&tdir, 100).is_empty(),
        "a sub-ceiling base-round flush must write no shard file"
    );
    assert!(t.all_shard_arcs().is_empty());
    assert!(!t.ram_tier.is_empty());

    let w = materialize_weights(&t);
    assert_eq!(w.get(&10), Some(&1));
    assert_eq!(w.get(&20), Some(&1));
    assert_eq!(w.get(&30), Some(&1));
}

/// Cross-flush churn (insert in one flush, retraction in another) folds at
/// `FOLD_THRESHOLD` to net state — the cancelled key is gone, not
/// accumulated.
#[test]
fn nondurable_cross_flush_fold_nets_to_zero() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("cross_fold_test");
    let schema = make_schema_u64_i64();
    let mut t = new_table(
        &tdir,
        schema,
        100,
        1 << 20,
        RecoverySource::Rederive { resume_at: None },
    );

    // 6 alternating +1 / -1 flushes on (k=7, v=70): each lands in its own
    // run until the threshold folds them. Net weight is 0.
    for i in 0..6 {
        let w = if i % 2 == 0 { 1 } else { -1 };
        t.ingest_owned_batch(make_batch(&[(7, w, 70)])).unwrap();
        t.flush().unwrap();
    }
    assert!(!t.has_pk(7), "net-zero key must not be present");
    let weights = materialize_weights(&t);
    assert!(!weights.contains_key(&7), "net-zero key folds away (0 rows)");
    assert!(t.ram_tier.len() <= FOLD_THRESHOLD, "run set must stay folded",);
    assert!(
        shard_db_files(&tdir, 100).is_empty(),
        "churn must not spill (tiny, sub-ceiling)"
    );
}

/// More than `FOLD_THRESHOLD` distinct-key flushes keep the run
/// count bounded by folding, and every key survives.
#[test]
fn nondurable_run_count_stays_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("run_bound_test");
    let schema = make_schema_u64_i64();
    let mut t = new_table(
        &tdir,
        schema,
        100,
        1 << 20,
        RecoverySource::Rederive { resume_at: None },
    );

    let n = FOLD_THRESHOLD as u64 + 4;
    for k in 0..n {
        t.ingest_owned_batch(make_batch(&[(k, 1, (k * 10) as i64)])).unwrap();
        t.flush().unwrap();
        assert!(
            t.ram_tier.len() <= FOLD_THRESHOLD,
            "run count exceeded threshold after flush {k}",
        );
    }
    for k in 0..n {
        assert!(t.has_pk(k as u128), "key {k} must survive folding");
    }
}

/// A flush past the RAM-tier ceiling (shrunk via the test seam) spills the
/// folded run to a `shard_{tid}_{lsn}` file, drains heap, and keeps rows
/// readable.
#[test]
fn nondurable_ceiling_spill_to_disk() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("ceiling_spill_test");
    let schema = make_schema_u64_i64();
    let mut t = new_table(
        &tdir,
        schema,
        100,
        1 << 20,
        RecoverySource::Rederive { resume_at: None },
    );
    t.ram_tier.set_budget(100); // < one flush (~10 rows × 32 B)

    let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
    t.ingest_owned_batch(make_batch(&rows)).unwrap();
    t.flush().unwrap();

    assert!(
        !shard_db_files(&tdir, 100).is_empty(),
        "ceiling breach must spill to a shard file"
    );
    assert_eq!(t.ram_tier.len(), 0, "heap drained after spill");
    assert!(t.ram_tier.is_empty());
    assert!(!t.all_shard_arcs().is_empty());
    for k in 0..10u128 {
        assert!(t.has_pk(k), "row {k} must remain readable from disk after spill");
    }
}

/// Repeated over-ceiling flushes keep the on-disk shard count bounded: the
/// spill path's `compact_if_needed` folds L0→L1 and the publish that follows
/// unlinks the consumed inputs. `SalReplay`, because that is the source
/// `flush()`'s base round publishes — a `Rederive` table's inputs wait for
/// the ephemeral round instead.
#[test]
fn repeated_spill_stays_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("repeated_spill_test");
    let schema = make_schema_u64_i64();
    let mut t = new_table(&tdir, schema, 100, 1 << 20, RecoverySource::SalReplay);
    t.ram_tier.set_budget(100);

    const ROUNDS: u64 = 20;
    const PER: u64 = 10;
    for r in 0..ROUNDS {
        let rows: Vec<(u64, i64, i64)> = (0..PER)
            .map(|j| (r * PER + j, 1, ((r * PER + j) * 10) as i64))
            .collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
        t.flush().unwrap();
        assert_eq!(t.ram_tier.len(), 0, "round {r}: heap drained after spill");
        // Raw spill shards do not accumulate with rounds — disk L0 self-folds
        // into L1 and the consumed raw shards are unlinked.
        assert!(
            shard_db_files(&tdir, 100).len() <= L0_COMPACT_THRESHOLD + 1,
            "round {r}: {} raw shards accumulated",
            shard_db_files(&tdir, 100).len(),
        );
    }

    let weights = materialize_weights(&t);
    assert_eq!(weights.len() as u64, ROUNDS * PER, "all rows present");
    assert!(weights.values().all(|&w| w == 1), "every row nets +1");
}

/// `has_pk` reflects net weight held only in the RAM tier: true after an
/// insert flush, false after the net-zero retraction flush.
#[test]
fn nondurable_has_pk_over_in_memory_runs() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("has_pk_inmem_test");
    let schema = make_schema_u64_i64();
    let mut t = new_table(
        &tdir,
        schema,
        100,
        1 << 20,
        RecoverySource::Rederive { resume_at: None },
    );

    t.ingest_owned_batch(make_batch(&[(5, 1, 50)])).unwrap();
    t.flush().unwrap();
    assert!(t.memtable.is_empty());
    assert!(t.has_pk(5), "in-memory positive-weight row must be found");

    t.ingest_owned_batch(make_batch(&[(5, -1, 50)])).unwrap();
    t.flush().unwrap();
    assert!(!t.has_pk(5), "net-zero key across in-memory runs must be absent");
}

/// After a spill, fresh heap runs coexist with disk shards; `open_cursor`
/// returns their union with correct net weights, including a cross-tier
/// retraction (heap retraction cancelling a disk insert).
#[test]
fn nondurable_mixed_disk_and_heap_read() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("mixed_read_test");
    let schema = make_schema_u64_i64();
    let mut t = new_table(
        &tdir,
        schema,
        100,
        1 << 20,
        RecoverySource::Rederive { resume_at: None },
    );

    // Force keys 0..10 to disk.
    t.ram_tier.set_budget(100);
    let disk_rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
    t.ingest_owned_batch(make_batch(&disk_rows)).unwrap();
    t.flush().unwrap();
    assert!(!t.all_shard_arcs().is_empty(), "first flush must spill to disk");
    assert_eq!(t.ram_tier.len(), 0);

    // Raise ceiling so subsequent flushes stay in heap.
    t.ram_tier.set_budget(usize::MAX);
    let heap_rows: Vec<(u64, i64, i64)> = (100..105).map(|k| (k, 1, (k * 10) as i64)).collect();
    t.ingest_owned_batch(make_batch(&heap_rows)).unwrap();
    t.flush().unwrap();
    assert!(!t.ram_tier.is_empty(), "second flush stays in heap");

    // Cross-tier retraction: cancel disk key 3 (payload 30) from heap.
    t.ingest_owned_batch(make_batch(&[(3, -1, 30)])).unwrap();
    t.flush().unwrap();

    assert!(!t.has_pk(3), "disk insert + heap retraction nets zero");
    let weights = materialize_weights(&t);
    for k in 0..10u64 {
        if k == 3 {
            assert!(!weights.contains_key(&3), "retracted disk key must be gone");
        } else {
            assert_eq!(weights.get(&k), Some(&1), "disk key {k}");
        }
    }
    for k in 100..105u64 {
        assert_eq!(weights.get(&k), Some(&1), "heap key {k}");
    }
}

// ── RAM-tier (the RAM tier) found-row path ─────────────────────────
//
// Twins of the durable retract tests above, but the rows stay in
// the RAM tier (non-durable flush, sub-ceiling) instead of on disk, so the
// `retract_pk*` / `has_pk_bytes` / `for_each_pk_candidate` RAM-tier code
// is what's exercised. `retract_pk*` is production-invoked only on durable
// base tables, but the machinery is tier-agnostic and these drive it directly.

/// Twin of `test_retract_pk_shard_fallback_multiple_payloads`, in RAM: an
/// UPDATE delta split across two in-memory runs. The multi-candidate
/// global-net loop over the RAM tier must pick the live payload (200), not
/// the cancelled one (100).
#[test]
fn inmem_retract_multiple_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("inmem_retract_payloads");
    let schema = make_schema_u64_i64();
    let mut t = new_table(
        &tdir,
        schema,
        5001,
        1 << 20,
        RecoverySource::Rederive { resume_at: None },
    );

    // Run 1: INSERT (PK=10, +1, val=100).
    t.ingest_owned_batch(make_batch(&[(10, 1, 100)])).unwrap();
    t.flush().unwrap();
    // Run 2: UPDATE delta — retract val=100, insert val=200.
    t.ingest_owned_batch(make_batch(&[(10, -1, 100), (10, 1, 200)]))
        .unwrap();
    t.flush().unwrap();

    assert!(t.memtable.is_empty());
    assert_eq!(t.ram_tier.len(), 2, "two sub-ceiling flushes → two L0 runs");

    let (w, found) = t.retract_pk(10);
    assert_eq!(w, 1, "net weight 1 across the RAM runs");
    assert!(found.is_some());
    let fr = found.expect("retracted row is the found row");
    let val = row_val(&fr);
    assert_eq!(
        val, 200,
        "RAM-tier global-net must pick live payload 200, not cancelled 100"
    );
}

/// The live row sits in RAM while the memtable holds a *negative*-weight
/// entry for the same PK. `for_each_pk_candidate` + `live_row_at` must net
/// memtable + RAM per candidate (killing the payload that cancels to zero) and
/// return the globally-live payload from the RAM run.
#[test]
fn inmem_cross_tier_netting() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("inmem_cross_tier");
    let schema = make_schema_u64_i64();
    let mut t = new_table(
        &tdir,
        schema,
        5004,
        1 << 20,
        RecoverySource::Rederive { resume_at: None },
    );

    // RAM run: two payloads for PK=5 (val=50, val=60), each +1.
    t.ingest_owned_batch(make_batch(&[(5, 1, 50), (5, 1, 60)])).unwrap();
    t.flush().unwrap();
    // Memtable (unflushed): retract val=50, leaving (5,60) globally live.
    t.ingest_owned_batch(make_batch(&[(5, -1, 50)])).unwrap();
    assert!(!t.memtable.is_empty(), "retraction stays in the memtable");

    assert!(t.has_pk(5), "PK 5 nets +1 across RAM (+2) and memtable (-1)");

    let (w, found) = t.retract_pk(5);
    assert_eq!(w, 1, "global net weight is 1");
    assert!(found.is_some());
    let fr = found.expect("live row found");
    let val = row_val(&fr);
    assert_eq!(
        val, 60,
        "global oracle rejects the cancelled val=50, arms live val=60 from RAM"
    );
}

/// Three-tier retract grouping: one PK with distinct payloads split across
/// a durable shard, the RAM tier, and the memtable. The grouping pass must
/// net each payload across ALL tiers — the shard payload cancelled by a
/// memtable retraction must not be armed even though its shard row alone
/// carries +1; the RAM payload is the only globally-live group.
#[test]
fn retract_groups_across_all_three_tiers() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("retract_three_tier");
    let schema = make_schema_u64_i64();
    // Durable: shard tier participation requires SalReplay persistence.
    let mut t = new_table(&tdir, schema, 5014, 1 << 20, RecoverySource::SalReplay);

    // Shard: (7, val=70, +1) — flushed durably.
    t.ingest_owned_batch(make_batch(&[(7, 1, 70)])).unwrap();
    t.flush().unwrap();
    assert!(t.shard_index.max_lsn() > 0, "row landed in a durable shard");

    // RAM tier: (7, val=80, +1) — folded into the RAM tier, not durable.
    t.ingest_owned_batch(make_batch(&[(7, 1, 80)])).unwrap();
    t.flush_to_ram().unwrap();
    assert!(!t.ram_tier.is_empty(), "val=80 sits in the RAM tier");

    // Memtable: retract the SHARD payload (7, val=70, -1) — unflushed.
    t.ingest_owned_batch(make_batch(&[(7, -1, 70)])).unwrap();
    assert!(!t.memtable.is_empty(), "retraction stays in the memtable");

    // Global nets: val=70 → shard +1, memtable −1 = 0 (dead);
    //              val=80 → RAM +1 (live). Total = +1.
    let (w, found) = t.retract_pk(7);
    assert_eq!(w, 1, "total net weight across all three tiers");
    let fr = found.expect("live row found");
    let val = row_val(&fr);
    assert_eq!(
        val, 80,
        "grouping must net val=70 across shard+memtable to zero and arm the RAM-tier val=80"
    );
}

/// More than `FOLD_THRESHOLD` runs force a RAM-tier fold, after which
/// `RunSet::may_contain`'s `OnceCell` rebuilds the set's PK bloom over the live
/// runs. Live rows must still be found (no false negative from the rebuilt
/// bloom) and an all-runs-absent PK must report absent (bloom-miss path equals
/// the linear result).
#[test]
fn inmem_fold_rebuilds_run_bloom() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("inmem_fold_bloom");
    let schema = make_schema_u64_i64();
    let mut t = new_table(
        &tdir,
        schema,
        5005,
        1 << 20,
        RecoverySource::Rederive { resume_at: None },
    );

    // One key per flush past the fold threshold; the folded run's bloom is
    // rebuilt over the merged batch.
    let n = FOLD_THRESHOLD as u64 + 2;
    for k in 0..n {
        t.ingest_owned_batch(make_batch(&[(k, 1, 100 + k as i64)])).unwrap();
        t.flush().unwrap();
    }
    assert!(t.ram_tier.len() <= FOLD_THRESHOLD, "fold kept the run count bounded");

    // Every live key survives folding and is found through the rebuilt bloom.
    for k in 0..n {
        assert!(t.has_pk(k as u128), "key {k} must survive fold + bloom rebuild");
    }
    let (w, found) = t.retract_pk(0);
    assert_eq!(w, 1);
    assert!(found.is_some());
    let fr = found.expect("folded key is the found row");
    let val = row_val(&fr);
    assert_eq!(val, 100, "found row payload survives the fold");

    // A PK absent from every run: bloom-miss path must equal the linear miss.
    assert!(!t.has_pk(9999), "absent PK reports absent (bloom miss)");
    let (w_absent, found_absent) = t.retract_pk(9999);
    assert_eq!(w_absent, 0, "absent PK retract nets zero");
    assert!(found_absent.is_none(), "absent PK retract finds no row");
}

// ── Unified checkpointing: RAM-tier lifecycle for SalReplay tables ────

/// The fold-first data-loss guard: a `SalReplay` table whose ingest overflow
/// left the memtable empty and the RAM tier populated must barrier-flush to
/// a staged publish — else the RAM-tier data is silently dropped.
/// After commit + reopen the full contents survive.
#[test]
fn barrier_flush_folds_populated_l0_not_empty() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("barrier_fold_l0");
    let schema = make_schema_u64_i64();
    // Small memtable budget (96 B): an 8-row batch (256 B) overflows the
    // memtable into the RAM tier, leaving the memtable empty.
    let mut t = new_table(&tdir, schema, 7100, 96, RecoverySource::SalReplay);

    let rows: Vec<(u64, i64, i64)> = (0..8).map(|k| (k, 1, (k * 10) as i64)).collect();
    t.ingest_owned_batch(make_batch(&rows)).unwrap();
    assert!(t.memtable.is_empty(), "ingest must overflow the memtable into L0");
    assert!(!t.ram_tier.is_empty(), "the RAM tier must be populated");
    assert!(
        t.all_shard_arcs().is_empty(),
        "sub-ceiling overflow writes no disk shard"
    );

    t.flush().unwrap();

    assert!(t.ram_tier.is_empty(), "flush_commit clears the RAM tier");
    assert!(!t.all_shard_arcs().is_empty(), "barrier wrote a durable shard");

    // Reopen (SalReplay loads the manifest) → every row survives.
    let t2 = new_table(&tdir, schema, 7100, 96, RecoverySource::SalReplay);
    for k in 0..8u128 {
        assert!(t2.has_pk(k), "row {k} must survive barrier flush + reopen");
    }
}

/// Spill unification (SalReplay): a ceiling breach spills to
/// `shard_{tid}_{lsn}.db` (the unified naming), registered with
/// `max_lsn == current_lsn - 1`. Two spills land at distinct `current_lsn`
/// → two distinct filenames. After a manifest-publishing barrier flush,
/// reopen seeds `current_lsn = max_lsn + 1` and every row survives.
#[test]
fn salreplay_spill_unified_naming_and_lsn() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("salreplay_spill");
    let schema = make_schema_u64_i64();
    // Small memtable budget forces overflow → flush_to_ram; tiny ceiling forces
    // the folded L0 to spill.
    let mut t = new_table(&tdir, schema, 7200, 96, RecoverySource::SalReplay);
    t.ram_tier.set_budget(100);

    // First spill: 10 rows (320 B) overflow, fold to L0 (320 B > 100) → spill.
    let b1: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
    t.ingest_owned_batch(make_batch(&b1)).unwrap();
    assert_eq!(t.ram_tier.len(), 0, "spill drains the RAM tier");
    let lsn1 = t.current_lsn;
    assert_eq!(
        t.shard_index.max_lsn(),
        lsn1 - 1,
        "spill registers with real LSNs (max_lsn == current_lsn - 1)"
    );
    let files1 = shard_db_files(&tdir, 7200);
    assert_eq!(
        files1,
        vec![super::super::naming::spill_shard_name(7200, lsn1)],
        "unified spill naming"
    );

    // Second spill at a distinct current_lsn → distinct filename, no collision.
    let b2: Vec<(u64, i64, i64)> = (100..110).map(|k| (k, 1, (k * 10) as i64)).collect();
    t.ingest_owned_batch(make_batch(&b2)).unwrap();
    assert_eq!(t.ram_tier.len(), 0, "second spill drains the RAM tier");
    let lsn2 = t.current_lsn;
    assert!(lsn2 > lsn1, "current_lsn strictly increased between spills");
    let mut files2 = shard_db_files(&tdir, 7200);
    files2.sort();
    assert_eq!(
        files2,
        vec![
            super::super::naming::spill_shard_name(7200, lsn1),
            super::super::naming::spill_shard_name(7200, lsn2)
        ],
        "two spills → two distinct shard files"
    );

    // Publish a manifest so reopen sees the spilled shards: a small in-memtable
    // batch + barrier flush references the whole index.
    t.ingest_owned_batch(make_batch(&[(500, 1, 5000)])).unwrap();
    t.flush().unwrap();

    let t2 = new_table(&tdir, schema, 7200, 96, RecoverySource::SalReplay);
    assert_eq!(
        t2.current_lsn,
        t2.shard_index.max_lsn() + 1,
        "reopen seeds current_lsn = max_lsn + 1 from the registered shard LSNs"
    );
    assert!(t2.current_lsn > 1, "reopen recovered a non-trivial LSN");
    for k in 0..10u128 {
        assert!(t2.has_pk(k), "first-spill row {k} survives reopen");
    }
    for k in 100..110u128 {
        assert!(t2.has_pk(k), "second-spill row {k} survives reopen");
    }
    assert!(t2.has_pk(500), "barrier-flushed row survives reopen");
}

/// A barrier folds the live memtable and L0 into one shard, and a ceiling
/// breach spills to one shard. Both outputs go through the filter-building
/// writer, so each carries a PK filter — and the barrier's covers a PK that was
/// only ever live in the memtable, which is what pins that the fold happened
/// before the write.
#[test]
fn salreplay_barrier_folds_memtable_and_l0_then_spill_writes_one_shard() {
    let schema = make_schema_u64_i64();

    // Barrier shard: live memtable + populated L0.
    {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("barrier_filter");
        let mut t = new_table(&tdir, schema, 7300, 96, RecoverySource::SalReplay);

        // Overflow 8 rows into L0, then leave 2 rows live in the memtable.
        let over: Vec<(u64, i64, i64)> = (0..8).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&over)).unwrap();
        assert!(!t.ram_tier.is_empty());
        t.ingest_owned_batch(make_batch(&[(100, 1, 1000), (101, 1, 1010)]))
            .unwrap();
        assert!(!t.memtable.is_empty(), "small second batch stays live in the memtable");

        t.flush().unwrap();
        let shards = t.all_shard_arcs();
        assert_eq!(shards.len(), 1, "barrier folds memtable + L0 into one shard");
        assert!(shards[0].has_shard_filter(), "barrier shard must carry the PK filter");
        assert!(
            shards[0].shard_filter_may_contain(probe_key(&100u64.to_be_bytes())),
            "the PK filter must contain a folded memtable PK"
        );
    }

    // Ceiling-breach spill.
    {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("spill_filter");
        let mut t = new_table(&tdir, schema, 7301, 96, RecoverySource::SalReplay);
        t.ram_tier.set_budget(100);

        let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
        assert_eq!(t.ram_tier.len(), 0, "ceiling breach spilled to disk");
        let shards = t.all_shard_arcs();
        assert_eq!(shards.len(), 1, "one spilled shard");
        assert!(shards[0].has_shard_filter(), "SalReplay spill must carry the PK filter");
    }
}

/// `current_lsn` bumps on every ingest, including a `Rederive` table (which
/// previously pinned it at 1 because ephemeral flushes never advanced it).
#[test]
fn current_lsn_bumps_on_every_ingest_including_rederive() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("lsn_bump_rederive");
    let schema = make_schema_u64_i64();
    let mut t = new_table(
        &tdir,
        schema,
        7400,
        1 << 20,
        RecoverySource::Rederive { resume_at: None },
    );

    assert_eq!(t.current_lsn, 1, "fresh Rederive table starts at LSN 1");
    t.ingest_owned_batch(make_batch(&[(1, 1, 10)])).unwrap();
    assert_eq!(t.current_lsn, 2, "first ingest bumps current_lsn");
    t.ingest_owned_batch(make_batch(&[(2, 1, 20)])).unwrap();
    assert_eq!(t.current_lsn, 3, "second ingest bumps current_lsn again");
}

/// Twin of the RAM-tier retract tests on a real `SalReplay` table: ingest
/// overflow lands in the RAM tier naturally (no artificial Rederive), and
/// `has_pk`/`retract_pk` resolve the live row across the RAM tier.
#[test]
fn salreplay_overflow_into_l0_retract() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("salreplay_l0_retract");
    let schema = make_schema_u64_i64();
    // Small memtable budget so an 8-row ingest overflows it into the RAM tier.
    let mut t = new_table(&tdir, schema, 7500, 96, RecoverySource::SalReplay);

    let rows: Vec<(u64, i64, i64)> = (0..8).map(|k| (k, 1, (k * 10) as i64)).collect();
    t.ingest_owned_batch(make_batch(&rows)).unwrap();
    assert!(t.memtable.is_empty(), "overflow emptied the memtable");
    assert!(!t.ram_tier.is_empty(), "SalReplay overflow lands in the RAM tier");
    assert!(
        t.all_shard_arcs().is_empty(),
        "sub-ceiling overflow writes no disk shard"
    );

    for k in 0..8u128 {
        assert!(t.has_pk(k), "row {k} readable from the RAM tier");
    }
    let (w, found) = t.retract_pk(3);
    assert_eq!(w, 1, "retract resolves the live row in the RAM tier");
    assert!(found.is_some());
    let fr = found.expect("found row from RAM tier");
    let val = row_val(&fr);
    assert_eq!(val, 30, "found-row payload matches the RAM-tier row");
}

// ── Barrier-only durability: unsynced spills, deferred cleanup ────────

/// F1 regression. A `SalReplay` table whose only post-checkpoint write
/// spilled — clearing the RAM tier, one lone L0 shard, no compaction, so no
/// manifest was published — must still barrier-flush to a staged publish and durably
/// capture the spill. Without the unsynced-shard disjunct the barrier returns
/// `Empty`, the spill is never manifested, and a
/// reopen's `gc_orphans` deletes it — acknowledged rows lost.
#[test]
fn lone_spill_survives_checkpoint_barrier() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("lone_spill_ckpt");
    let schema = make_schema_u64_i64();
    let mut t = new_table(&tdir, schema, 7600, 96, RecoverySource::SalReplay);
    t.ram_tier.set_budget(100);

    // One over-ceiling ingest → one spill shard (1 < L0 compaction threshold,
    // so no compaction, no publish).
    let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, (k * 10) as i64)).collect();
    t.ingest_owned_batch(make_batch(&rows)).unwrap();
    assert_eq!(t.ram_tier.len(), 0, "ceiling breach spilled to disk");
    assert_eq!(t.all_shard_arcs().len(), 1, "exactly one lone spill shard");

    // F1 regression: the barrier must publish the lone unpublished spill
    // (verified below via the manifest/shard the reopen sees).
    t.flush().unwrap();

    let t2 = new_table(&tdir, schema, 7600, 96, RecoverySource::SalReplay);
    assert!(
        !t2.all_shard_arcs().is_empty(),
        "manifest must reference the spill shard"
    );
    for k in 0..10u128 {
        assert!(t2.has_pk(k), "spilled row {k} must survive checkpoint + reopen");
    }
}

/// Deferred-cleanup crash simulation. After a mid-epoch compaction that did
/// not republish (SalReplay defers), a crash (drop without a barrier) reopens
/// from the *old* manifest: the cut state is intact, and the unreferenced
/// compaction outputs + post-cut spills are orphans reclaimed by `gc_orphans`.
#[test]
fn deferred_cleanup_crash_sim_reopens_from_old_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("deferred_cleanup_crash");
    let schema = make_schema_u64_i64();
    let mut t = new_table(&tdir, schema, 7700, 96, RecoverySource::SalReplay);
    t.ram_tier.set_budget(100);

    // Cut A: one row, published by a barrier (manifest M0 references its shard).
    t.ingest_owned_batch(make_batch(&[(1000, 1, 1)])).unwrap();
    t.flush().unwrap();

    // Post-cut churn: spill > L0_COMPACT_THRESHOLD shards so run_compact fires,
    // swapping the index and deferring cleanup (no mid-epoch publish).
    for r in 0..6u64 {
        let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (r * 100 + k, 1, 1)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
    }
    assert!(
        compaction_output_count(&tdir, 7700) > 0,
        "post-cut compaction must have produced deferred outputs"
    );
    // Crash: drop without a barrier flush → reopen from the old manifest M0.
    drop(t);

    let t2 = new_table(&tdir, schema, 7700, 96, RecoverySource::SalReplay);
    assert!(t2.has_pk(1000), "cut A row survives the crash (loaded from M0)");
    assert!(!t2.has_pk(0), "post-cut orphaned data must not resurrect");
    assert_eq!(
        compaction_output_count(&tdir, 7700),
        0,
        "deferred compaction outputs (never in M0) must be gc'd at open"
    );
}

/// Compact-then-quiet. A table that compacts mid-epoch and then goes quiet
/// (empty memtable and RAM tier) must still publish at the barrier so the
/// manifest reflects the compacted index; a reopen finds every referenced
/// shard and every row.
#[test]
fn compact_then_quiet_barrier_publishes_compacted_index() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("compact_then_quiet");
    let schema = make_schema_u64_i64();
    let mut t = new_table(&tdir, schema, 7800, 96, RecoverySource::SalReplay);
    t.ram_tier.set_budget(100);

    let mut all_keys = Vec::new();
    for r in 0..6u64 {
        let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (r * 100 + k, 1, 1)).collect();
        for &(pk, _, _) in &rows {
            all_keys.push(pk);
        }
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
    }
    assert_eq!(t.ram_tier.len(), 0, "spilled: RAM tier empty");
    assert!(t.memtable.is_empty(), "no live memtable rows");
    assert!(compaction_output_count(&tdir, 7800) > 0, "compaction ran");

    t.flush().unwrap();

    let t2 = new_table(&tdir, schema, 7800, 96, RecoverySource::SalReplay);
    for pk in &all_keys {
        assert!(
            t2.has_pk(*pk as u128),
            "row {pk} present after compact + barrier + reopen"
        );
    }
}

/// The manifest generation is stamped from the value the publish path
/// passes explicitly (`flush_prepare_ephemeral(generation)`), so a
/// compaction republish carries the caller's generation, and a later
/// republish re-stamps a newer one.
#[test]
fn generation_preserved_by_compaction_republish() {
    use super::super::manifest::read_file;

    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("gen_republish");
    let schema = make_schema_u64_i64();
    let manifest_path = std::ffi::CString::new(tdir.join("manifest.bin").to_str().unwrap()).unwrap();
    let read_generation = |path: &std::ffi::CStr| -> u64 { read_file(path).unwrap().unwrap().1.checkpoint_gen };

    // Publish at generation G1 with a compaction pending.
    let mut t = new_table(&tdir, schema, 7900, 96, RecoverySource::SalReplay);
    t.ram_tier.set_budget(100);
    for r in 0..6u64 {
        let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (r * 100 + k, 1, 1)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
        flush_ephemeral_at(&mut t, 0x1111);
    }
    assert!(compaction_output_count(&tdir, 7900) > 0, "compaction ran");
    flush_ephemeral_at(&mut t, 0x1111);
    assert_eq!(
        read_generation(&manifest_path),
        0x1111,
        "republished manifest carries the passed generation",
    );

    // A later publish at a higher generation re-stamps the manifest.
    t.ingest_owned_batch(make_batch(&[(9999, 1, 1)])).unwrap();
    flush_ephemeral_at(&mut t, 0x2222);
    assert_eq!(
        read_generation(&manifest_path),
        0x2222,
        "later republish re-stamps the newer generation",
    );
}

/// `Table::new`'s `Rederive` open decision: a manifest whose
/// generation matches the caller's `committed` loads its shards; a mismatch
/// (or absence) erases the shards and unlinks the manifest.
#[test]
fn rederive_checkpointed_conditional_load() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("cond_load");
    let schema = make_schema_u64_i64();
    let manifest_path = tdir.join("manifest.bin");

    // Publish a durable shard + manifest stamped at generation 7.
    {
        let mut t = new_table(&tdir, schema, 7910, 96, RecoverySource::SalReplay);
        t.ingest_owned_batch(make_batch(&[(1, 1, 100), (2, 1, 200)])).unwrap();
        flush_ephemeral_at(&mut t, 7);
    }
    assert!(manifest_path.exists(), "manifest published at generation 7");

    // Matching generation ⇒ shards load, rows present.
    {
        let t = new_table(&tdir, schema, 7910, 96, RecoverySource::Rederive { resume_at: Some(7) });
        assert!(
            t.has_pk_bytes(&1u64.to_be_bytes()) && t.has_pk_bytes(&2u64.to_be_bytes()),
            "matching-generation reopen must load the checkpointed shards"
        );
    }
    assert!(manifest_path.exists(), "matching reopen leaves the manifest in place");

    // Mismatched generation ⇒ shards erased, manifest unlinked.
    {
        let t = new_table(&tdir, schema, 7910, 96, RecoverySource::Rederive { resume_at: Some(8) });
        assert!(
            !t.has_pk_bytes(&1u64.to_be_bytes()) && !t.has_pk_bytes(&2u64.to_be_bytes()),
            "mismatched-generation reopen must erase the stale shards"
        );
    }
    assert!(
        !manifest_path.exists(),
        "mismatched-generation reopen must unlink manifest.bin so a re-open cannot re-peek it"
    );
}

/// A damaged manifest names no checkpoint generation, so the
/// `Rederive` arm rebuilds rather than failing the boot. A failed
/// read is not evidence of staleness: it propagates and erases nothing.
#[test]
fn rederive_checkpointed_rebuilds_on_a_damaged_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let table_id = 7911;

    // A checkpointed table with one published shard, and the path to the
    // manifest that names it.
    let checkpointed = |name: &str| -> (std::path::PathBuf, std::path::PathBuf) {
        let tdir = dir.path().join(name);
        let mut t = new_table(&tdir, schema, table_id, 96, RecoverySource::SalReplay);
        t.ingest_owned_batch(make_batch(&[(1, 1, 100)])).unwrap();
        flush_ephemeral_at(&mut t, 7);
        assert_eq!(shard_db_files(&tdir, table_id).len(), 1, "{name}: shard published");
        let manifest = std::path::PathBuf::from(crate::storage::lsm::manifest::path(tdir.to_str().unwrap()));
        (tdir, manifest)
    };
    let reopen = |tdir: &std::path::Path| {
        Table::new(
            tdir.to_str().unwrap(),
            schema,
            table_id,
            RecoverySource::Rederive { resume_at: Some(7) },
            StoreBudgets::default(),
        )
    };

    type Damage = fn(&mut Vec<u8>);
    let damages: [(&str, Damage); 3] = [
        ("truncated", |b| b.truncate(20)),
        ("bad_magic", |b| b[0] ^= 0xFF),
        // Inside the first entry's filename, which only the digest checks.
        ("checksum", |b| b[60] ^= 0x01),
    ];
    for (name, damage) in damages {
        let (tdir, manifest) = checkpointed(name);
        let mut buf = std::fs::read(&manifest).unwrap();
        damage(&mut buf);
        std::fs::write(&manifest, &buf).unwrap();

        let t = reopen(&tdir)
            .unwrap_or_else(|e| panic!("{name}: a damaged manifest must rebuild, not fail the boot: {e:?}"));
        assert!(
            !t.has_pk_bytes(&1u64.to_be_bytes()),
            "{name}: the rebuild verdict opens empty"
        );
        assert!(
            shard_db_files(&tdir, table_id).is_empty(),
            "{name}: stale shards erased"
        );
    }

    // Replacing the manifest with a directory makes `std::fs::read` fail
    // with something other than NotFound.
    let (tdir, manifest) = checkpointed("io_err");
    std::fs::remove_file(&manifest).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    assert_eq!(
        reopen(&tdir).err(),
        Some(StorageError::Io(libc::EISDIR)),
        "reading a manifest that is a directory must surface the errno"
    );
    assert_eq!(
        shard_db_files(&tdir, table_id).len(),
        1,
        "a failed read must not erase the shards"
    );
}

/// A `SalReplay` table publishes on every barrier, whatever its tier holds:
/// an unchanged one re-stamps its manifest (which is what makes "every
/// `w{k}of{n}` has a manifest" a decidable completeness test for the boot
/// relayout), a lone unsynced spill lands in the sweep list, and compaction
/// outputs are swept while the inputs they superseded are not.
#[test]
fn barrier_gate_matrix() {
    let schema = make_schema_u64_i64();

    // Arm 1 — an unchanged tier still publishes, with nothing to sweep and no
    // new shard.
    {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("gate_empty");
        let mut t = new_table(&tdir, schema, 7900, 1 << 20, RecoverySource::SalReplay);
        t.ingest_owned_batch(make_batch(&[(1, 1, 1)])).unwrap();
        t.flush().unwrap();
        let shards_before = shard_db_files(&tdir, 7900).len();
        let w = t
            .flush_prepare(FlushRound::Base)
            .unwrap()
            .expect("a SalReplay table must publish even when unchanged");
        assert!(
            w.sync_paths.is_empty(),
            "an unchanged tier has nothing left to fdatasync"
        );
        assert_eq!(
            shard_db_files(&tdir, 7900).len(),
            shards_before,
            "an empty RAM tier publishes no new shard"
        );
    }

    // Arm 2 — a lone unsynced spill stages a publish with the spill swept.
    {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("gate_unsynced");
        let mut t = new_table(&tdir, schema, 7901, 96, RecoverySource::SalReplay);
        t.ram_tier.set_budget(100);
        let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (k, 1, 1)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
        assert_eq!(t.ram_tier.len(), 0, "spilled");
        let w = t
            .flush_prepare(FlushRound::Base)
            .unwrap()
            .expect("an unsynced spill must gate to a staged publish");
        assert!(!w.sync_paths.is_empty(), "the unsynced spill must be in the sweep list");
    }

    // Arm 3 — a spill-driven compaction stages a publish and sweeps its own
    // outputs: compaction writes them unsynced like any other shard, so the
    // publish that makes them reachable is what makes them durable.
    {
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("gate_pending");
        let mut t = new_table(&tdir, schema, 7902, 96, RecoverySource::SalReplay);
        t.ram_tier.set_budget(100);
        // Five spills put L0 over the threshold, and the fifth registration
        // compacts — so the last thing to leave a file unswept is that compaction.
        for r in 0..5u64 {
            let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (r * 100 + k, 1, 1)).collect();
            t.ingest_owned_batch(make_batch(&rows)).unwrap();
            assert_eq!(t.ram_tier.len(), 0, "round {r} spilled");
        }
        assert!(
            compaction_output_count(&tdir, 7902) > 0,
            "the spills drove a compaction"
        );

        let shards_before = shard_db_files(&tdir, 7902).len();
        let w = t
            .flush_prepare(FlushRound::Base)
            .unwrap()
            .expect("unsynced compaction outputs must gate to a staged publish");
        let swept: Vec<String> = w.sync_paths.iter().map(|c| c.to_string_lossy().into_owned()).collect();
        assert!(!swept.is_empty(), "the compaction outputs must be swept");
        assert!(
            swept.iter().all(|p| is_compaction_output(p)),
            "the superseded inputs must have left the sweep list, got {swept:?}",
        );
        assert_eq!(
            shard_db_files(&tdir, 7902).len(),
            shards_before,
            "an empty RAM tier publishes no new shard"
        );
    }
}

/// F5. A `Rederive` table publishes on the **ephemeral** round, so that is
/// what drains its superseded compaction inputs — the base round leaves them,
/// since a manifest could still reference them. Nothing leaks across a
/// checkpoint.
#[test]
fn rederive_ephemeral_flush_drains_deferred_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("rederive_eph_drain");
    let schema = make_schema_u64_i64();
    let mut t = new_table(&tdir, schema, 8000, 96, RecoverySource::Rederive { resume_at: None });
    t.ram_tier.set_budget(100);

    for r in 0..8u64 {
        let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (r * 100 + k, 1, 1)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
        assert_eq!(t.ram_tier.len(), 0, "round {r} spilled");
    }
    assert!(compaction_output_count(&tdir, 8000) > 0, "compaction must have run");

    let files_before = all_shard_file_count(&tdir, 8000);
    t.flush().unwrap();
    assert_eq!(
        all_shard_file_count(&tdir, 8000),
        files_before,
        "the base round must publish nothing for a Rederive table, so nothing drains"
    );

    let g = 1;
    super::super::flush_barrier::flush_barrier([&mut t], super::super::flush_barrier::FlushRound::Ephemeral(g))
        .unwrap();
    assert!(
        all_shard_file_count(&tdir, 8000) < files_before,
        "the ephemeral round republishes over the compacted index and drains the inputs"
    );

    for r in 0..8u64 {
        for k in 0..10u64 {
            assert!(t.has_pk((r * 100 + k) as u128), "row survives compaction");
        }
    }
}

/// F-sys. A `SalReplay` table stands in for a master `_sys` table:
/// `compact_if_needed` defers cleanup (a manifest could strand the inputs),
/// and the synchronous `flush()` republishes over the compacted index and
/// drains the deferred inputs — no intra-session leak.
#[test]
fn salreplay_flush_drains_deferred_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = dir.path().join("salreplay_flush_drain");
    let schema = make_schema_u64_i64();
    let mut t = new_table(&tdir, schema, 8100, 96, RecoverySource::SalReplay);
    t.ram_tier.set_budget(100);

    for r in 0..6u64 {
        let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (r * 100 + k, 1, 1)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
    }
    assert!(compaction_output_count(&tdir, 8100) > 0, "compaction ran");
    let files_before = all_shard_file_count(&tdir, 8100);

    t.flush().unwrap();
    assert!(
        all_shard_file_count(&tdir, 8100) < files_before,
        "flush must drain the deferred compaction inputs (no intra-session leak)"
    );

    for r in 0..6u64 {
        for k in 0..10u64 {
            assert!(t.has_pk((r * 100 + k) as u128), "row present after flush-drain");
        }
    }

    let t2 = new_table(&tdir, schema, 8100, 96, RecoverySource::SalReplay);
    for r in 0..6u64 {
        for k in 0..10u64 {
            assert!(t2.has_pk((r * 100 + k) as u128), "row survives flush-drain + reopen");
        }
    }
}

/// A cursor opened for a key range must answer exactly what the whole-index
/// cursor answers over that range. It reaches the shards by guard routing
/// instead of chaining every one, which is sound only because guards
/// partition the key line — a key reachable from two guards would be one this
/// gather could miss.
#[test]
fn a_range_opened_cursor_sees_every_row_the_whole_index_would() {
    let schema = make_schema_u64_i64();
    let dir = tempfile::tempdir().unwrap();
    let mut t = new_table(dir.path(), schema, 8200, 96, RecoverySource::SalReplay);
    t.ram_tier.set_budget(100);
    // Interleaved bands across several spills, so the shards the router has to
    // pick from overlap in neither key order nor write order.
    for r in 0..8u64 {
        let rows: Vec<(u64, i64, i64)> = (0..40).map(|k| (k * 8 + r, 1, (k * 8 + r) as i64)).collect();
        t.ingest_owned_batch(make_batch(&rows)).unwrap();
    }
    assert!(!t.all_shard_arcs().is_empty(), "the rows reached the shard tier");

    let drain = |c: &mut crate::storage::ReadCursor| {
        let mut out = Vec::new();
        while c.valid {
            out.push((c.current_pk_bytes().to_vec(), c.current_weight));
            c.advance();
        }
        out
    };
    for (lo, hi) in [(0u64, 8u64), (37, 200), (100, 100), (0, 319), (400, 500)] {
        let (lo_b, hi_b) = (lo.to_be_bytes(), hi.to_be_bytes());
        let mut whole = t.open_cursor();
        whole.seek_range_bytes(&lo_b, Some(&hi_b));
        let mut ranged = t.open_cursor_in_range(&lo_b, Some(&hi_b));
        ranged.seek_range_bytes(&lo_b, Some(&hi_b));
        assert_eq!(drain(&mut ranged), drain(&mut whole), "range [{lo}, {hi})");
    }
}

/// Only base-table paths point-probe a store by PK, and they are the only
/// readers of a shard's PK filter. Silent both ways: a missing filter
/// still answers every probe (just slower), a useless one costs only bytes.
#[test]
fn pk_filter_follows_whether_the_store_is_probed() {
    let schema = make_schema_u64_i64();
    // Six ceiling breaches: each spills an L0 shard, the fifth crosses the L0
    // threshold and the sixth leaves a spill sitting above the fold — so one
    // store registers both shard writers' output at once.
    let build = |dir: &std::path::Path, id: u32, rs: RecoverySource| {
        let mut t = new_table(dir, schema, id, 96, rs);
        t.ram_tier.set_budget(100);
        for r in 0..6u64 {
            let rows: Vec<(u64, i64, i64)> = (0..10).map(|k| (r * 100 + k, 1, 1)).collect();
            t.ingest_owned_batch(make_batch(&rows)).unwrap();
        }
        assert!(compaction_output_count(dir, id) > 0, "the spills drove a compaction");
        t
    };
    let dir = tempfile::tempdir().unwrap();

    let base = build(&dir.path().join("base"), 8100, RecoverySource::SalReplay);
    let shards = base.all_shard_arcs();
    assert!(shards.len() > 1, "spills and compaction outputs both present");
    assert!(
        shards.iter().all(|s| s.has_shard_filter()),
        "a probed store filters every shard"
    );

    let reder = build(
        &dir.path().join("rederive"),
        8101,
        RecoverySource::Rederive { resume_at: None },
    );
    assert!(
        reder.all_shard_arcs().iter().all(|s| !s.has_shard_filter()),
        "a store nothing probes builds no filter",
    );
    assert!(
        reder.has_pk_bytes(&404u64.to_be_bytes()),
        "filterless shards still answer"
    );
    assert!(
        !reder.has_pk_bytes(&9999u64.to_be_bytes()),
        "and still reject absent keys"
    );
}

/// Each flush writes an L0 shard; without the `compact_if_needed` call they
/// accumulate unbounded. Drive many flushes and assert the shard count stays
/// bounded. A `SalReplay` store, which is what the durable ingest path opens.
#[test]
fn repeated_flushes_compact_l0() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut t = new_table(dir.path(), schema, 8200, 96, RecoverySource::SalReplay);
    let flushes = 40u64;
    for i in 0..flushes {
        t.ingest_owned_batch(make_batch(&[(i, i as i64, 1)])).unwrap();
        t.flush().unwrap();
    }
    let (shards, _) = t.pk_filter_census();
    assert!(
        (shards as u64) < flushes / 2,
        "L0 must be compacted: {shards} shards after {flushes} flushes"
    );
}
