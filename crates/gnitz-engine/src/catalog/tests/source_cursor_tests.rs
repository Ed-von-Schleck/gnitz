//! `open_source_cursor` — the index-bound pushdown into a circuit's backfill scan.
//!
//! The bound is a physical access hint: the circuit's `Filter` is authoritative, so
//! `Full` and `Bounded` must yield the same rows AND the same weights. These pin
//! that, plus every fallback that degrades a bound to a full scan.

use super::super::store_io::SourceCursor;
use super::*;
use gnitz_wire::{Cut, PkColList, RangeDescriptor, ScanBound};

const NBASE: u64 = 200;

/// `ScanDelta(base, bound?) → Integrate` for `vid`: the shared identity-circuit
/// writer plus, for a bound, the descriptor blob on the scan node and its
/// `NODE_COL_KIND_SCAN_BOUND` col rows — the same wire channel the planner ships.
fn write_bounded_identity_circuit(engine: &mut CatalogEngine, vid: i64, base_tid: i64, bound: Option<ScanBound>) {
    write_identity_circuit(
        engine,
        vid,
        base_tid,
        bound.as_ref().map(|b| b.desc.encode()).as_deref(),
    );

    if let Some(b) = &bound {
        let mut bb = BatchBuilder::new(sys_tab_schema(CIRCUIT_NODE_COLUMNS_TAB_ID));
        for (i, &c) in b.idx_cols.as_slice().iter().enumerate() {
            bb.begin_row(pack_view_pk(vid, i as u64), 1);
            bb.put_u64(0); // node_id
            bb.put_u64(gnitz_wire::NODE_COL_KIND_SCAN_BOUND);
            bb.put_u64(i as u64); // position — the list's order
            bb.put_u64(c as u64); // value1 — the column index
            bb.put_u64(0);
            bb.end_row();
        }
        engine
            .ingest_to_family(CIRCUIT_NODE_COLUMNS_TAB_ID, &bb.finish())
            .unwrap();
    }
}

/// A `(id U64 PK | val I64)` base of `NBASE` rows with `val = id * 10`, indexed on
/// `val`, plus a registered identity view carrying `bound`. Returns
/// `(engine, base tid, view id)`.
fn fixture(name: &str, bound: Option<ScanBound>, unique_pk: bool) -> (CatalogEngine, i64, i64) {
    let dir = temp_dir(name);
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    let tid = engine.create_table("public.base", &cols, &[0], unique_pk).unwrap();

    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    for i in 0..NBASE {
        bb.begin_row(i as u128, 1);
        bb.put_u64(i * 10);
        bb.end_row();
    }
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.create_index("public.base", &["val"], false).unwrap();

    let vid = engine.allocate_table_id();
    write_bounded_identity_circuit(&mut engine, vid, tid, bound);
    engine.write_column_records(vid, OWNER_KIND_VIEW, &cols).unwrap();
    engine.write_view_deps(vid, &[tid]).unwrap();
    let batch = build_view_tab_row(vid, "v_base", "SELECT * FROM base");
    engine.ingest_to_family(VIEW_TAB_ID, &batch).unwrap();
    (engine, tid, vid)
}

/// A bound on index column 1 (`val`) over the half-open cut interval.
fn val_bound(start: Cut, end: Cut) -> ScanBound {
    ScanBound {
        idx_cols: PkColList::from_slice(&[1]),
        desc: RangeDescriptor::new(&[], start, end),
    }
}

/// Drain a cursor to `(pk, weight)` pairs, in chunks of `chunk` so a chunk
/// boundary lands mid-range.
fn drain_all(cur: &mut SourceCursor, chunk: usize) -> Vec<(u128, i64)> {
    let mut out = Vec::new();
    while let Some(b) = cur.drain_chunk(chunk) {
        for i in 0..b.count {
            out.push((b.get_pk(i), b.get_weight(i)));
        }
    }
    out.sort_unstable();
    out
}

/// The full-scan drain of `source`, as the reference every bounded drain is
/// compared against.
fn full_drain(engine: &mut CatalogEngine, source: i64) -> Vec<(u128, i64)> {
    let mut cur = SourceCursor::Full(Box::new(engine.open_store_cursor(source).unwrap()));
    drain_all(&mut cur, 64)
}

/// The headline case: a selective range takes the index, and yields exactly the
/// in-range rows — at full-scan-identical weights — across a mid-range chunk
/// boundary.
#[test]
fn bounded_cursor_yields_in_range_rows_across_chunks() {
    // val ∈ [500, 900) ⇒ ids 50..90: 40 of 200 rows, inside the 1/16 gate? No —
    // 40/200 = 1/5. Narrow it to ids 50..60 (10/200 = 1/20).
    let (mut engine, tid, vid) = fixture(
        "srccur_range",
        Some(val_bound(Cut::Before(500), Cut::Before(600))),
        true,
    );
    let mut cur = engine.open_source_cursor(vid, tid).unwrap();
    assert!(
        matches!(cur, SourceCursor::Bounded(_)),
        "a 1/20 range must take the index"
    );

    // Chunk of 3 forces several boundaries inside the range, each re-probing the
    // held base cursor backwards.
    let got = drain_all(&mut cur, 3);
    let want: Vec<(u128, i64)> = (50..60u128).map(|i| (i, 1)).collect();
    assert_eq!(got, want);

    // Equal to the full scan filtered by the same predicate — rows AND weights.
    let full: Vec<(u128, i64)> = full_drain(&mut engine, tid)
        .into_iter()
        .filter(|&(pk, _)| (50..60).contains(&pk))
        .collect();
    assert_eq!(got, full, "bounded and full must agree on rows and weights");
    engine.close();
}

/// A retracted row on a **single-source** cursor — the one shape where the full
/// scan's verbatim single-source slice copy (no weight predicate) and the bounded
/// gather's `current_weight > 0` gate could diverge. They agree because a
/// `unique_pk` base accumulates every live group to exactly +1.
#[test]
fn bounded_and_full_agree_with_a_retracted_row_single_source() {
    let (mut engine, tid, vid) = fixture(
        "srccur_retract",
        Some(val_bound(Cut::Before(500), Cut::Before(600))),
        true,
    );
    // Retract id=55 (val=550), inside the range. No flush: one memtable run, so
    // the cursor is single-source and takes the verbatim-copy drain path.
    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(55u128, -1);
    bb.put_u64(550);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();

    let mut cur = engine.open_source_cursor(vid, tid).unwrap();
    let got = drain_all(&mut cur, 4);
    let want: Vec<(u128, i64)> = (50..60u128).filter(|&i| i != 55).map(|i| (i, 1)).collect();
    assert_eq!(got, want, "a retracted row must be absent from the bounded scan");

    let full: Vec<(u128, i64)> = full_drain(&mut engine, tid)
        .into_iter()
        .filter(|&(pk, _)| (50..60).contains(&pk))
        .collect();
    assert_eq!(got, full, "the retracted row must vanish from BOTH paths identically");
    engine.close();
}

/// An UPDATE of the indexed column retracts the old index entry and inserts the
/// new one, so a range spanning both values must emit the row **once** at weight 1
/// — the walk's gate is `> 0` on the consolidated group, not a presence test.
#[test]
fn updated_indexed_column_emits_the_row_once() {
    // Range covers val ∈ [500, 600): ids 50..60 plus whatever moves in.
    let (mut engine, tid, vid) = fixture(
        "srccur_update",
        Some(val_bound(Cut::Before(500), Cut::Before(600))),
        true,
    );
    // Move id=10 from val=100 to val=555 (into the range): retract + insert.
    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(10u128, -1);
    bb.put_u64(100);
    bb.end_row();
    bb.begin_row(10u128, 1);
    bb.put_u64(555);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();

    let mut cur = engine.open_source_cursor(vid, tid).unwrap();
    let got = drain_all(&mut cur, 64);
    let mut want: Vec<(u128, i64)> = (50..60u128).map(|i| (i, 1)).collect();
    want.push((10, 1));
    want.sort_unstable();
    assert_eq!(got, want, "the updated row must appear exactly once, at weight 1");
    engine.close();
}

/// A range spanning an updated column's OLD and NEW value must still emit the row
/// once: the old key surfaces at net weight 0 and the walk skips it.
#[test]
fn range_spanning_old_and_new_indexed_value_emits_once() {
    // val ∈ [500, 600) after moving id=51 from 510 → 590 — both inside the range.
    let (mut engine, tid, vid) = fixture("srccur_span", Some(val_bound(Cut::Before(500), Cut::Before(600))), true);
    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(51u128, -1);
    bb.put_u64(510);
    bb.end_row();
    bb.begin_row(51u128, 1);
    bb.put_u64(590);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();

    let mut cur = engine.open_source_cursor(vid, tid).unwrap();
    let got = drain_all(&mut cur, 64);
    let want: Vec<(u128, i64)> = (50..60u128).map(|i| (i, 1)).collect();
    assert_eq!(
        got, want,
        "a range covering both the old and new value must not double-count"
    );
    engine.close();
}

/// A degenerate point range (what a pure `col = v` equality lowers to) returns
/// exactly that key group.
#[test]
fn degenerate_point_range_returns_one_key_group() {
    let (mut engine, tid, vid) = fixture("srccur_point", Some(val_bound(Cut::Before(730), Cut::After(730))), true);
    let mut cur = engine.open_source_cursor(vid, tid).unwrap();
    assert!(matches!(cur, SourceCursor::Bounded(_)));
    assert_eq!(drain_all(&mut cur, 64), vec![(73u128, 1)]);
    engine.close();
}

/// An in-range index entry whose base row is absent resolves to nothing. Its chunk
/// must yield `Some(empty)`, NOT `None`: both drivers read `None` as exhaustion, so
/// an escaping `None` would silently truncate the view mid-range.
///
/// The orphan is written straight into the index table — the projection path
/// retracts an index entry with its source row, so an orphan is unreachable
/// through it, and the point here is precisely that the walk does not assume so.
#[test]
fn orphaned_index_entry_yields_empty_not_exhaustion() {
    let (mut engine, tid, vid) = fixture(
        "srccur_orphan",
        Some(val_bound(Cut::Before(500), Cut::Before(600))),
        true,
    );

    // Clone a live in-range entry's key (val=550 ⇒ id=55) and swap its source-PK
    // suffix to an id no base row carries. The index schema is all-PK, zero
    // payload, so the key IS the row.
    let entry = engine.dag.tables.get(&tid).unwrap();
    let ic = &entry.index_circuits[0];
    let idx_schema = ic.index_schema;
    let src_pk_stride = entry.schema.pk_stride() as usize;
    let idx_pk_stride = idx_schema.pk_stride() as usize;
    let idx_key_size = idx_schema.leading_key_size(1);

    let mut probe = ic.table_mut().open_cursor();
    let mut orphan_key = Vec::new();
    while probe.valid {
        let k = probe.current_pk_bytes();
        // The leading key is val's OPK; find the entry for val=550.
        if k[idx_key_size..idx_key_size + src_pk_stride] == 55u64.to_be_bytes()[..src_pk_stride] {
            orphan_key = k.to_vec();
            break;
        }
        probe.advance();
    }
    drop(probe);
    assert_eq!(orphan_key.len(), idx_pk_stride, "expected a live index entry for id=55");
    // id 9999 exists in no base row — the same val group, an absent source PK.
    orphan_key[idx_key_size..idx_key_size + src_pk_stride].copy_from_slice(&9999u64.to_be_bytes()[8 - src_pk_stride..]);

    let mut ob = Batch::with_schema(idx_schema, 1);
    ob.extend_pk_bytes(&orphan_key);
    ob.extend_weight(&1i64.to_le_bytes());
    ob.extend_null_bmp(&0u64.to_le_bytes());
    ob.count += 1;
    ic.table_mut().ingest_owned_batch(ob).unwrap();

    // Chunk of 1 puts the orphan in a chunk of its own: were its `None` to escape,
    // the drain would stop there and lose every later id.
    let mut cur = engine.open_source_cursor(vid, tid).unwrap();
    assert!(matches!(cur, SourceCursor::Bounded(_)));
    let got = drain_all(&mut cur, 1);
    let want: Vec<(u128, i64)> = (50..60u128).map(|i| (i, 1)).collect();
    assert_eq!(
        got, want,
        "the orphan must be skipped and the walk continue to the range end"
    );
    engine.close();
}

/// A range wider than the gate takes the full scan, and its drain is identical to
/// the plain `open_store_cursor` drain — nothing was consumed by the measurement.
#[test]
fn unselective_range_falls_back_to_full_scan() {
    // val >= 0 matches all 200 rows — 1/1, far outside the 1/16 gate.
    let (mut engine, tid, vid) = fixture(
        "srccur_wide",
        Some(val_bound(Cut::Before(0), Cut::After(i64::MAX as u128))),
        true,
    );
    let mut cur = engine.open_source_cursor(vid, tid).unwrap();
    assert!(
        matches!(cur, SourceCursor::Full(_)),
        "an unselective range must full-scan"
    );
    let got = drain_all(&mut cur, 64);
    assert_eq!(got, full_drain(&mut engine, tid), "the gate must not consume any row");
    assert_eq!(got.len(), NBASE as usize);
    engine.close();
}

/// A bound whose index circuit is gone (dropped since the plan compiled) degrades
/// to a full scan rather than erroring or returning nothing.
#[test]
fn dropped_index_falls_back_to_full_scan() {
    let (mut engine, tid, vid) = fixture(
        "srccur_dropped",
        Some(val_bound(Cut::Before(500), Cut::Before(600))),
        true,
    );
    engine.drop_index("public__base__idx_val").unwrap();
    let mut cur = engine.open_source_cursor(vid, tid).unwrap();
    assert!(matches!(cur, SourceCursor::Full(_)));
    assert_eq!(drain_all(&mut cur, 64).len(), NBASE as usize);
    engine.close();
}

/// A non-`unique_pk` source full-scans: the gather resolves by source PK alone and
/// would lose rows / double weights where one PK carries several live payloads.
#[test]
fn non_unique_pk_source_falls_back_to_full_scan() {
    let (mut engine, tid, vid) = fixture(
        "srccur_nonuniq",
        Some(val_bound(Cut::Before(500), Cut::Before(600))),
        false,
    );
    // Two live payloads under one PK — exactly what the PK-only gather mishandles.
    let schema = engine.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(55u128, 1);
    bb.put_u64(555);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();

    let mut cur = engine.open_source_cursor(vid, tid).unwrap();
    assert!(
        matches!(cur, SourceCursor::Full(_)),
        "a non-unique_pk source must full-scan"
    );
    assert_eq!(
        drain_all(&mut cur, 64),
        full_drain(&mut engine, tid),
        "weights must match the full scan under a duplicated PK"
    );
    engine.close();
}

/// An inverted range is `Empty` — provably no rows — and NOT `None`, which the
/// drivers read as "source unregistered, skip it" (losing a global aggregate's
/// ground row).
#[test]
fn inverted_range_is_empty_not_none() {
    // start After(900) is above end Before(300): x > 900 AND x < 300.
    let (mut engine, tid, vid) = fixture(
        "srccur_inverted",
        Some(val_bound(Cut::After(900), Cut::Before(300))),
        true,
    );
    let mut cur = engine
        .open_source_cursor(vid, tid)
        .expect("an empty range is Some, never None");
    assert!(matches!(cur, SourceCursor::Empty));
    assert!(cur.drain_chunk(64).is_none());
    engine.close();
}

/// An unregistered source is the ONLY `None` — byte-identical to
/// `open_store_cursor`'s contract, which `backfill_view`'s `else { continue; }`
/// relies on.
#[test]
fn unregistered_source_is_none() {
    let (mut engine, _tid, vid) = fixture("srccur_unreg", None, true);
    assert!(engine.open_source_cursor(vid, 999_999).is_none());
    engine.close();
}

/// `open_source_cursor` must compile the plan itself before reading the bound.
///
/// `handle_backfill` — the driver for every exchange view, every ad-hoc transient,
/// and every post-recovery rebuild — reaches the cursor open BEFORE anything
/// compiles the view. With a cold cache and no `ensure_compiled`, the bound would
/// read back as "absent" and the motivating case would ship silently dead: every
/// other test here still passes, because a full scan is never *wrong*.
#[test]
fn cold_plan_cache_still_finds_the_bound() {
    let (mut engine, tid, vid) = fixture("srccur_cold", Some(val_bound(Cut::Before(500), Cut::Before(600))), true);
    // Exactly what a freshly-created view / transient looks like at the moment
    // `handle_backfill` opens its source cursor.
    engine.dag.invalidate(vid);
    let cur = engine.open_source_cursor(vid, tid).unwrap();
    assert!(
        matches!(cur, SourceCursor::Bounded(_)),
        "a cold plan cache must compile, not silently report 'no bound'"
    );
    engine.close();
}

/// An unbounded plan takes the full scan — the pre-change behaviour, unchanged.
#[test]
fn unbounded_plan_full_scans() {
    let (mut engine, tid, vid) = fixture("srccur_unbounded", None, true);
    let mut cur = engine.open_source_cursor(vid, tid).unwrap();
    assert!(matches!(cur, SourceCursor::Full(_)));
    assert_eq!(drain_all(&mut cur, 64).len(), NBASE as usize);
    engine.close();
}
