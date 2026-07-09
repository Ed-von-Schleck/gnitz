use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::Layout;
use crate::test_support::wide_pk_3xu64_schema;

fn make_schema_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// `(U64 PK | I64 payload)` — stride-8, the dominant single-PK table shape.
/// Used by the stride-8 drive bench, which exercises `pack_pk_be`'s 8-byte
/// register arm and the u128-vs-u64 compare in `compare_pk_ordering`.
fn make_schema_u64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Build an `Rc<Batch>` with i64-payload rows.  Tests pre-sort their
/// inputs and have at most one row per (PK, payload), so we mark the
/// batch as sorted+consolidated.
fn make_batch(rows: &[(u128, i64, i64)]) -> Rc<Batch> {
    let schema = make_schema_i64();
    let mut b = Batch::with_schema(schema, rows.len().max(1));
    for &(pk, w, val) in rows {
        b.extend_pk(pk);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated, &schema);
    Rc::new(b)
}

fn scan_all(cursor: &mut ReadCursor) -> Vec<(u64, u64, i64)> {
    let mut rows = Vec::new();
    while cursor.valid {
        rows.push((
            cursor.current_key_narrow() as u64,
            (cursor.current_key_narrow() >> 64) as u64,
            cursor.current_weight,
        ));
        cursor.advance();
    }
    rows
}

#[test]
fn test_empty_cursor() {
    let schema = make_schema_i64();
    let cursor = create_read_cursor(&[], &[], schema);
    assert!(!cursor.valid);
}

#[test]
fn test_single_batch_scan() {
    let schema = make_schema_i64();
    let batch = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    let mut cursor = create_read_cursor(&[batch], &[], schema);
    let rows = scan_all(&mut cursor);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, 0, 1));
    assert_eq!(rows[1], (2, 0, 1));
    assert_eq!(rows[2], (3, 0, 1));
}

#[test]
fn test_two_batch_merge() {
    let schema = make_schema_i64();
    let b1 = make_batch(&[(1, 1, 10), (3, 1, 30)]);
    let b2 = make_batch(&[(2, 1, 20), (4, 1, 40)]);
    let mut cursor = create_read_cursor(&[b1, b2], &[], schema);
    let rows = scan_all(&mut cursor);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[2].0, 3);
    assert_eq!(rows[3].0, 4);
}

#[test]
fn test_ghost_elimination_across_sources() {
    let schema = make_schema_i64();
    // Batch 1: pk=5 val=50 w=+1, pk=10 val=100 w=+1
    let b1 = make_batch(&[(5, 1, 50), (10, 1, 100)]);
    // Batch 2: pk=5 val=50 w=-1 (retraction)
    let b2 = make_batch(&[(5, -1, 50)]);
    let mut cursor = create_read_cursor(&[b1, b2], &[], schema);
    let rows = scan_all(&mut cursor);
    // pk=5 cancelled (w=+1-1=0), only pk=10 survives
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (10, 0, 1));
}

#[test]
fn test_seek() {
    let schema = make_schema_i64();
    let batch = make_batch(&[(1, 1, 10), (5, 1, 50), (10, 1, 100)]);
    let mut cursor = create_read_cursor(&[batch], &[], schema);

    // Seek to pk >= 5. OPK for a U128 PK is the value's big-endian bytes.
    cursor.seek_bytes(&5u128.to_be_bytes());
    assert!(cursor.valid);
    assert_eq!(cursor.current_key_narrow(), 5);

    // Seek to pk >= 7 → lands on 10
    cursor.seek_bytes(&7u128.to_be_bytes());
    assert!(cursor.valid);
    assert_eq!(cursor.current_key_narrow(), 10);

    // Seek past end
    cursor.seek_bytes(&100u128.to_be_bytes());
    assert!(!cursor.valid);
}

fn make_schema_compound_u64() -> SchemaDescriptor {
    // PK = (col0:U64, col1:U64); payload = I64. Stored first-column-major.
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    )
}

/// OPK bytes for a `(U64, U64)` compound PK: each column big-endian,
/// concatenated in pk-list order (the at-rest form).
fn compound_pk_bytes(col0: u64, col1: u64) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&col0.to_be_bytes());
    b[8..].copy_from_slice(&col1.to_be_bytes());
    b
}

/// A compound `(U64, U64)` PK's raw u128 order is last-column-major while
/// storage (OPK memcmp) sorts first-column-major. `seek_bytes` must land on
/// the exact row, not the u128-nearest one.
#[test]
fn test_seek_compound_pk_lands_on_exact_row() {
    let schema = make_schema_compound_u64();
    // Canonical (first-column-major) storage order: (1,5) then (2,3).
    // As u128 the order is reversed: pack(2,3) < pack(1,5).
    let mut b = Batch::with_schema(schema, 2);
    for &(c0, c1, v) in &[(1u64, 5u64, 100i64), (2, 3, 200)] {
        b.extend_pk_bytes(&compound_pk_bytes(c0, c1));
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &v.to_le_bytes());
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated, &schema);
    let mut cursor = create_read_cursor(&[Rc::new(b)], &[], schema);

    cursor.seek_bytes(&compound_pk_bytes(2, 3));
    assert!(cursor.valid);
    assert_eq!(cursor.current_pk_bytes(), &compound_pk_bytes(2, 3));

    // Seek the first group too.
    cursor.seek_bytes(&compound_pk_bytes(1, 5));
    assert!(cursor.valid);
    assert_eq!(cursor.current_pk_bytes(), &compound_pk_bytes(1, 5));
}

fn make_schema_signed_i64() -> SchemaDescriptor {
    // PK = single I64 column; payload = I64.
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// OPK bytes for a single I64 PK column (big-endian, sign bit flipped).
fn i64_opk(v: i64) -> [u8; 8] {
    let mut out = [0u8; 8];
    gnitz_wire::encode_pk_column(&v.to_le_bytes(), type_code::I64, &mut out);
    out
}

/// A signed single-column PK's negative keys sort *after* positives in raw
/// u128 order, while OPK (BE + sign-bit flip) sorts them first. `seek_bytes`
/// on the OPK key must land on the matching row.
#[test]
fn test_seek_signed_pk_lands_on_negative_row() {
    let schema = make_schema_signed_i64();
    // Storage (signed) order: -3, -1, 2.
    let mut b = Batch::with_schema(schema, 3);
    for &(pk, v) in &[(-3i64, 30i64), (-1, 10), (2, 20)] {
        b.extend_pk_bytes(&i64_opk(pk));
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &v.to_le_bytes());
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated, &schema);
    let mut cursor = create_read_cursor(&[Rc::new(b)], &[], schema);

    cursor.seek_bytes(&i64_opk(-1));
    assert!(cursor.valid);
    assert_eq!(cursor.current_pk_bytes(), &i64_opk(-1));

    cursor.seek_bytes(&i64_opk(-3));
    assert!(cursor.valid);
    assert_eq!(cursor.current_pk_bytes(), &i64_opk(-3));

    // A positive key still lands correctly.
    cursor.seek_bytes(&i64_opk(2));
    assert!(cursor.valid);
    assert_eq!(cursor.current_pk_bytes(), &i64_opk(2));
}

#[test]
fn test_same_pk_different_payload_ordering() {
    let schema = make_schema_i64();
    // Two entries with same PK but different payloads
    let b1 = make_batch(&[(5, 1, 200)]);
    let b2 = make_batch(&[(5, 1, 100)]);
    let mut cursor = create_read_cursor(&[b1, b2], &[], schema);
    let rows = scan_all(&mut cursor);
    // Both survive, sorted by payload (100 < 200)
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (5, 0, 1)); // payload=100
    assert_eq!(rows[1], (5, 0, 1)); // payload=200
}

/// Drain that READS THE PAYLOAD VALUE of each row (logical col 1, the
/// I64 payload), unlike `scan_all` which only captures PK + weight. The
/// payload read is what gives the root-adjacency pin its teeth: a PK-only
/// merge leaks an *extra* row whose payload value distinguishes it from
/// the survivor.
fn scan_all_with_val(cursor: &mut ReadCursor) -> Vec<(u64, i64, i64)> {
    let mut rows = Vec::new();
    while cursor.valid {
        let p = cursor.col_ptr(1, 8);
        assert!(!p.is_null(), "payload col_ptr null for a valid cursor row");
        let val = i64::from_le_bytes(unsafe { std::slice::from_raw_parts(p, 8) }.try_into().unwrap());
        rows.push((cursor.current_key_narrow() as u64, cursor.current_weight, val));
        cursor.advance();
    }
    rows
}

/// PIN — root adjacency of equal-(PK, payload) rows across cursor sources.
/// Three single-row batches, all PK=5: b1/b3 carry the *same* payload
/// (val=100) with opposite weights, b2 carries a different payload
/// (val=200) and sits between them in source order. Each batch is
/// (PK, payload)-sorted, but the matching val=100 rows are NOT adjacent in
/// source order.
///
/// The cursor's N-way merge heap MUST order by (PK, payload) so the two
/// val=100 rows reach the fold root consecutively and their +1/-1 weights
/// cancel via ghost elimination; only val=200 survives. A PK-only heap
/// `less` (dropping the payload tiebreak) leaves the three same-PK rows
/// unordered among themselves, the fold breaks on the first payload
/// mismatch, and the +1/-1 pair never folds — surfacing a spurious row.
/// We assert on the PAYLOAD VALUE (via `scan_all_with_val`) so the leaked
/// row cannot hide behind a matching PK/weight.
#[test]
fn test_cursor_same_pk_nonadjacent_payload_fold() {
    let schema = make_schema_i64();
    let b1 = make_batch(&[(5, 1, 100)]);
    let b2 = make_batch(&[(5, 1, 200)]);
    let b3 = make_batch(&[(5, -1, 100)]);

    let mut cursor = create_read_cursor(&[b1, b2, b3], &[], schema);
    let rows = scan_all_with_val(&mut cursor);
    assert_eq!(
        rows,
        vec![(5, 1, 200)],
        "val=100 +1/-1 pair must ghost-cancel; only (pk=5, w=1, val=200) survives"
    );
}

#[test]
fn test_weight_accumulation_across_sources() {
    let schema = make_schema_i64();
    // Same (PK, payload) in two batches: weights should sum
    let b1 = make_batch(&[(5, 3, 50)]);
    let b2 = make_batch(&[(5, 7, 50)]);
    let mut cursor = create_read_cursor(&[b1, b2], &[], schema);
    let rows = scan_all(&mut cursor);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (5, 0, 10)); // 3 + 7 = 10
}

#[test]
fn test_drain_single_source_full() {
    let schema = make_schema_i64();
    let batch = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    let mut cursor = create_read_cursor(&[batch], &[], schema);

    let result = cursor.drain_single_source(0);
    assert!(result.is_some());
    let out = result.unwrap();
    assert_eq!(out.count, 3);
    assert_eq!(out.get_pk(0), 1);
    assert_eq!(out.get_pk(2), 3);
    assert!(!cursor.valid);
}

#[test]
fn test_drain_single_source_with_limit() {
    let schema = make_schema_i64();
    let batch = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)]);
    let mut cursor = create_read_cursor(&[batch], &[], schema);

    // Drain first 2
    let out1 = cursor.drain_single_source(2).unwrap();
    assert_eq!(out1.count, 2);
    assert_eq!(out1.get_pk(0), 1);
    assert_eq!(out1.get_pk(1), 2);
    assert!(cursor.valid);

    // Drain remaining 2
    let out2 = cursor.drain_single_source(0).unwrap();
    assert_eq!(out2.count, 2);
    assert_eq!(out2.get_pk(0), 3);
    assert_eq!(out2.get_pk(1), 4);
    assert!(!cursor.valid);
}

#[test]
fn test_drain_multi_source_returns_none() {
    let schema = make_schema_i64();
    let b1 = make_batch(&[(1, 1, 10)]);
    let b2 = make_batch(&[(2, 1, 20)]);
    let mut cursor = create_read_cursor(&[b1, b2], &[], schema);
    assert!(cursor.drain_single_source(0).is_none());
}

#[test]
fn test_col_ptr_pk_returns_null() {
    // PK (logical col 0, pk_index=0) must always return null — callers read
    // the PK through current_key_narrow()/current_pk_bytes() instead.
    let schema = make_schema_i64();
    let batch = make_batch(&[(42, 1, 99)]);
    let cursor = create_read_cursor(&[batch], &[], schema);
    assert!(cursor.valid);
    let pk_index = cursor.schema.pk_indices()[0] as usize; // 0
    let ptr = cursor.col_ptr(pk_index, 16);
    assert!(ptr.is_null(), "col_ptr for PK index must return null");
}

#[test]
fn test_col_ptr_payload_returns_valid_pointer() {
    // Payload col at logical index 1 must return a non-null pointer with
    // the correct value.
    let schema = make_schema_i64();
    let batch = make_batch(&[(7, 1, 1234)]);
    let cursor = create_read_cursor(&[batch], &[], schema);
    assert!(cursor.valid);
    let ptr = cursor.col_ptr(1, 8); // logical col 1 = i64 payload
    assert!(!ptr.is_null(), "col_ptr for payload col must not be null");
    let val = i64::from_le_bytes(unsafe { *(ptr as *const [u8; 8]) });
    assert_eq!(val, 1234);
}

#[test]
fn test_col_ptr_invalid_cursor_returns_null() {
    let schema = make_schema_i64();
    let cursor = create_read_cursor(&[], &[], schema);
    assert!(!cursor.valid);
    assert!(cursor.col_ptr(1, 8).is_null());
}

#[test]
fn test_estimated_length_reflects_remaining() {
    let schema = make_schema_i64();
    let batch = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    let mut cursor = create_read_cursor(&[batch], &[], schema);
    assert_eq!(cursor.estimated_length(), 3);
    cursor.advance();
    assert_eq!(cursor.estimated_length(), 2);
    cursor.advance();
    assert_eq!(cursor.estimated_length(), 1);
    cursor.advance();
    assert_eq!(cursor.estimated_length(), 0);
}

#[test]
fn test_current_key() {
    let schema = make_schema_i64();
    let expected = (0xBEEFu128 << 64) | 0xDEADu128;
    let batch = make_batch(&[(expected, 1, 0)]);
    let cursor = create_read_cursor(&[batch], &[], schema);
    assert!(cursor.valid);
    assert_eq!(cursor.current_key_narrow(), expected);
}

/// Cursor backed by a shard whose PK region is Constant-encoded (single-row
/// shard).  Previously `to_unified` returned `None` for this, falling back
/// to the row-major scatter.  Now the column-major path handles it directly.
#[test]
fn test_scatter_constant_pk_shard() {
    crate::foundation::posix_io::raise_fd_limit_for_tests();
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_i64();

    // A single-row shard Constant-encodes its PK region at rest, which used to
    // fall back to the row-major scatter; the column-major path now handles it.
    let shard = write_test_shard(&dir, &schema, 0, &[(42, 1, 999)], 0);
    let cursor = create_read_cursor(&[], &[shard], schema);
    let result = cursor.materialize();

    assert_eq!(result.count, 1);
    assert_eq!(result.get_pk(0), 42u128);
}

/// Cursor with more than 16 entries (formerly above `MAX_INLINE_BATCH_SOURCES`)
/// previously fell through to the row-major scatter.  Now the column-major
/// path handles any number of sources.
#[test]
fn seek_bytes_lands_on_lower_bound_narrow() {
    // Narrow single-PK (U128, stride 16): seek_bytes lands on the first row
    // whose PK >= the (OPK) key — the lower bound. OPK for a U128 PK is the
    // value's big-endian bytes.
    let schema = make_schema_i64();
    let keys: &[u128] = &[10, 20, 30, 40];
    let batch = make_batch(&[(10u128, 1, 100), (20, 1, 200), (30, 1, 300), (40, 1, 400)]);
    let probes: &[u128] = &[0u128, 5, 10, 15, 20, 25, 30, 35, 40, 41];
    for &key in probes {
        let mut c = create_read_cursor(&[Rc::clone(&batch)], &[], schema);
        c.seek_bytes(&key.to_be_bytes());

        // Independent oracle: first stored key >= probe.
        let expected = keys.iter().copied().find(|&k| k >= key);
        match expected {
            Some(k) => {
                assert!(c.valid, "key={key} should land on {k}");
                assert_eq!(c.current_key_narrow(), k, "key={key}");
                // current_pk_bytes is OPK (BE) of the native value.
                assert_eq!(c.current_pk_bytes(), &k.to_be_bytes()[..], "key={key}");
            }
            None => assert!(!c.valid, "key={key} past end must be invalid"),
        }
    }
}

/// Drive one reused cursor over `sources` through `probes`, asserting each
/// `advance_to` lands exactly where a from-scratch `seek_bytes` on a fresh
/// cursor would. The fresh cursor is the oracle for any probe order — a
/// strict-forward step (the in-place loser-tree gallop), an `Equal` re-seek
/// of the current key, or a backward step (both the rebuild fallback). The
/// reused position must never change the landing — including the emitted
/// weight, which pins cross-source ghost folding.
fn assert_advance_to_matches_seek_oracle(schema: SchemaDescriptor, sources: &[Rc<Batch>], probes: &[u128]) {
    let n = sources.len();
    let mut adv = create_read_cursor(sources, &[], schema);
    for &key in probes {
        adv.advance_to(&key.to_be_bytes());
        let mut fresh = create_read_cursor(sources, &[], schema);
        fresh.seek_bytes(&key.to_be_bytes());
        assert_eq!(adv.valid, fresh.valid, "n_src={n} key={key}");
        if adv.valid {
            assert_eq!(adv.current_pk_bytes(), fresh.current_pk_bytes(), "n_src={n} key={key}");
            assert_eq!(adv.current_weight, fresh.current_weight, "n_src={n} key={key}");
        }
    }
}

/// `advance_to` (forward-only, position-seeded) lands on the identical row a
/// from-scratch `seek_bytes` would, across a monotone ascending probe sweep —
/// for both a single-source cursor (rebuild fallback) and a multi-source
/// cursor (the in-place loser-tree gallop). The sweep also re-seeks the
/// current key (probe `20` after landing on `20` from probe `15`), exercising
/// the `Equal` rebuild fallback on the same cursor.
#[test]
fn advance_to_lands_like_seek_bytes_monotone() {
    let schema = make_schema_i64();
    let b0 = make_batch(&[(10u128, 1, 100), (30, 1, 300), (50, 1, 500), (70, 1, 700)]);
    let b1 = make_batch(&[(20u128, 1, 200), (40, 1, 400), (60, 1, 600)]);
    // Monotone ascending: below-min, present, absent-between, above-max.
    let probes: &[u128] = &[0, 10, 15, 20, 35, 50, 55, 70, 71];
    assert_advance_to_matches_seek_oracle(schema, &[Rc::clone(&b0)], probes);
    assert_advance_to_matches_seek_oracle(schema, &[b0, b1], probes);
}

/// A strict-forward seek must skip a source already past the probe key while
/// galloping a lagging source up to it — the loser-tree maintenance touches
/// only laggards. After emitting `5`, `b_lag`'s head is `30` and `b_ahead`'s
/// is `50`; seeking `40` gallops `b_lag` (30 → 90) while leaving `b_ahead`
/// (50) untouched, then lands on `50`. The trailing `95` then exhausts the
/// last live source (the `pop_top` branch).
#[test]
fn advance_to_forward_skips_ahead_source() {
    let schema = make_schema_i64();
    let b_lag = make_batch(&[(5u128, 1, 50), (30, 1, 300), (90, 1, 900)]);
    let b_ahead = make_batch(&[(50u128, 1, 500), (60, 1, 600), (70, 1, 700)]);
    assert_advance_to_matches_seek_oracle(schema, &[b_lag, b_ahead], &[40, 55, 65, 95]);
}

/// Interleaved forward/backward sweep on one reused multi-source cursor: the
/// forward steps take the in-place gallop, the backward / current-key steps
/// the rebuild fallback. Each landing still matches the from-scratch oracle —
/// the fast path must leave the cursor in a state the rebuild can recover.
#[test]
fn advance_to_interleaved_forward_backward() {
    let schema = make_schema_i64();
    let b0 = make_batch(&[(10u128, 1, 100), (30, 1, 300), (50, 1, 500), (70, 1, 700)]);
    let b1 = make_batch(&[(20u128, 1, 200), (40, 1, 400), (60, 1, 600)]);
    // up, up, up, BACK, up, BACK, up, BACK.
    assert_advance_to_matches_seek_oracle(schema, &[b0, b1], &[0, 30, 50, 20, 60, 10, 70, 5]);
}

/// A forward gallop that lands on a ghost group (PK nets to 0 across two runs
/// at the seek target) must fold it and land on the first *live* row past it,
/// exactly as a from-scratch `seek_bytes` would. Pins the seek-phase /
/// ghost-fold handoff.
#[test]
fn advance_to_forward_lands_past_straddling_ghost() {
    let schema = make_schema_i64();
    // PK=200 nets to 0: +1 from b_a, -1 from b_b. Live trace = {10, 20, 400}.
    let b_a = make_batch(&[(10u128, 1, 100), (200, 1, 2000), (400, 1, 4000)]);
    let b_b = make_batch(&[(20u128, 1, 200), (200, -1, 2000)]);
    // After emitting 10, b_a head=200, b_b head=20. Seeking 150 gallops b_b
    // (20 → 200) past its live row, positions both at the ghost 200, folds it
    // to zero, and must land on the first live row past it (400).
    let mut adv = create_read_cursor(&[Rc::clone(&b_a), Rc::clone(&b_b)], &[], schema);
    adv.advance_to(&(150u128).to_be_bytes());
    assert!(adv.valid, "must land on a live row past the ghost");
    assert_eq!(
        adv.current_key_narrow(),
        400,
        "ghost 200 must be folded; first live row is 400"
    );
    assert_eq!(adv.current_weight, 1, "landed row's net weight");
    // The same landing also matches the from-scratch oracle.
    assert_advance_to_matches_seek_oracle(schema, &[b_a, b_b], &[150]);
}

/// A source whose `lower_bound(key)` is its end exhausts mid-sweep, forcing
/// the seek-phase `pop_top` branch; the remaining source must still merge
/// correctly, and a later forward seek over the now-drained heap must
/// invalidate cleanly (the fast path no-ops on an empty tree).
#[test]
fn advance_to_forward_exhausts_source_mid_sweep() {
    let schema = make_schema_i64();
    let b_short = make_batch(&[(10u128, 1, 100), (20, 1, 200)]); // max 20
    let b_long = make_batch(&[(10u128, 1, 100), (50, 1, 500), (90, 1, 900)]);
    // 40 gallops b_short to its end (pop_top), leaving b_long to emit 50;
    // 60 → 90; 100 → exhausted (fast path over an empty heap).
    assert_advance_to_matches_seek_oracle(schema, &[b_short, b_long], &[40, 60, 100]);
}

#[test]
fn test_scatter_many_sources_beyond_old_cap() {
    let schema = make_schema_i64();
    let n = 33usize;
    let batches: Vec<Rc<super::super::batch::Batch>> = (0..n)
        .map(|i| make_batch(&[(i as u128, 1i64, (i * 100) as i64)]))
        .collect();
    let cursor = create_read_cursor(&batches, &[], schema);
    let result = cursor.materialize();

    assert_eq!(result.count, n);
    for i in 0..n {
        assert_eq!(result.get_pk(i), i as u128);
    }
}

fn make_compound_pk_schema() -> SchemaDescriptor {
    // 2×U64 compound PK (stride 16): col_A in PK bytes 0..8, col_B in 8..16.
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    )
}

/// OPK bytes for a `(U64, U64)` compound PK: col_A big-endian ++ col_B
/// big-endian (the at-rest form). memcmp of these equals (col_A, col_B)
/// order, the inverse of the raw-u128 (col_B, col_A) order.
fn compound_opk(a: u64, b: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&a.to_be_bytes());
    k[8..].copy_from_slice(&b.to_be_bytes());
    k
}

/// Multi-source merge over a compound `(col_A, col_B)` PK must order by
/// `(col_A, col_B)`. As a raw `u128`, col_B occupies the high 64 bits, so
/// the integer-comparison shortcut would (wrongly) order by `(col_B,
/// col_A)`. Chosen rows make the two orderings disagree.
#[test]
fn test_compound_pk_multi_source_merge_order() {
    let schema = make_compound_pk_schema();
    let make = |a: u64, b: u64, val: i64| -> Rc<Batch> {
        let mut bt = Batch::with_schema(schema, 1);
        bt.extend_pk_bytes(&compound_opk(a, b));
        bt.extend_weight(&1i64.to_le_bytes());
        bt.extend_null_bmp(&0u64.to_le_bytes());
        bt.extend_col(0, &val.to_le_bytes());
        bt.count += 1;
        bt.certify_layout(Layout::Consolidated, &schema);
        Rc::new(bt)
    };
    // (1,2) precedes (2,1) by (col_A, col_B); the raw-u128 order is reversed.
    let b1 = make(1, 2, 100);
    let b2 = make(2, 1, 200);
    let mut cursor = create_read_cursor(&[b1, b2], &[], schema);

    // current_key_narrow() for a stride-16 PK is widen_pk_be of the OPK bytes: the
    // BE reading places col_A in the high 64 bits and col_B in the low.
    let mut emitted = Vec::new();
    while cursor.valid {
        let a = (cursor.current_key_narrow() >> 64) as u64;
        let b = cursor.current_key_narrow() as u64;
        emitted.push((a, b));
        cursor.advance();
    }
    assert_eq!(
        emitted,
        vec![(1u64, 2u64), (2u64, 1u64)],
        "compound PK must order by (col_A, col_B), not raw u128 (col_B, col_A)",
    );
}

/// OPK bytes for a 3×U64 compound PK: each column big-endian, concatenated
/// in pk-list order (the at-rest form).
fn pk3(a: u64, b: u64, c: u64) -> [u8; 24] {
    let mut k = [0u8; 24];
    k[0..8].copy_from_slice(&a.to_be_bytes());
    k[8..16].copy_from_slice(&b.to_be_bytes());
    k[16..24].copy_from_slice(&c.to_be_bytes());
    k
}

fn make_wide_batch(rows: &[([u8; 24], i64, i64)]) -> Rc<Batch> {
    let schema = wide_pk_3xu64_schema();
    let mut bt = Batch::with_schema(schema, rows.len().max(1));
    for (pk, w, val) in rows {
        bt.extend_pk_bytes(pk);
        bt.extend_weight(&w.to_le_bytes());
        bt.extend_null_bmp(&0u64.to_le_bytes());
        bt.extend_col(0, &val.to_le_bytes());
        bt.count += 1;
    }
    bt.certify_layout(Layout::Consolidated, &schema);
    Rc::new(bt)
}

/// seek_bytes over a 24-byte PK whose third column lies past the 16-byte
/// heap prefix. Two rows share their low-16 prefix `(col_0, col_1)=(1,0)`
/// and differ only in `col_2`; the prefix tie-break must keep them ordered.
#[test]
fn seek_bytes_wide_pk_24_byte_stride() {
    let schema = wide_pk_3xu64_schema();
    let pk_a = pk3(0, 0, 0);
    let pk_b = pk3(1, 0, 0);
    let pk_c = pk3(1, 0, 1); // differs from pk_b only past byte 16
    let batch = make_wide_batch(&[(pk_a, 1, 100), (pk_b, 1, 200), (pk_c, 1, 300)]);

    let mut cursor = create_read_cursor(&[batch], &[], schema);
    cursor.seek_bytes(&pk_b);
    assert!(cursor.valid);
    assert_eq!(cursor.current_pk_bytes(), &pk_b[..]);
    assert_eq!(cursor.current_weight, 1);

    cursor.advance();
    assert!(cursor.valid);
    assert_eq!(
        cursor.current_pk_bytes(),
        &pk_c[..],
        "the third row, distinguished only in its trailing 8 bytes, must follow"
    );
}

/// Two wide PKs that collide on their low-16 prefix `(col_0, col_1)=(1,1)`,
/// differ only in `col_2` (100 vs 200) and carry EQUAL payload must survive
/// as distinct outputs with their own weights — never folded into one
/// summed group. Regression for the `eq_payload` PK-equality term: a
/// payload-only `eq_payload` would collapse them.
#[test]
fn wide_pk_prefix_collision_not_consolidated() {
    let schema = wide_pk_3xu64_schema();
    let pk_x = pk3(1, 1, 100);
    let pk_y = pk3(1, 1, 200);
    // Two sources so the merge heap, not a pre-sorted single batch, drives
    // the group fold.
    let b1 = make_wide_batch(&[(pk_x, 3, 42)]);
    let b2 = make_wide_batch(&[(pk_y, 5, 42)]); // identical payload (42)
    let mut cursor = create_read_cursor(&[b1, b2], &[], schema);

    let mut emitted: Vec<([u8; 24], i64)> = Vec::new();
    while cursor.valid {
        let mut k = [0u8; 24];
        k.copy_from_slice(cursor.current_pk_bytes());
        emitted.push((k, cursor.current_weight));
        cursor.advance();
    }
    assert_eq!(emitted.len(), 2, "distinct wide PKs must not be folded");
    assert_eq!(emitted[0], (pk_x, 3));
    assert_eq!(emitted[1], (pk_y, 5));
}

/// Secondary-index shape `(U64 indexed_col, I64 source_pk)` (stride 16).
/// `seek_first_positive_with_prefix` on the leading column must return ALL
/// rows sharing that prefix, including ones whose signed suffix is negative.
/// Zero-padding the suffix (the bug) decodes to 0 and skips negatives.
#[test]
fn seek_first_positive_with_prefix_includes_negative_suffix() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    );
    assert_eq!(schema.pk_stride(), 16);
    // OPK: U64 column big-endian; I64 column big-endian with sign bit flipped.
    let mk = |a: u64, b: i64| -> [u8; 16] {
        let mut k = [0u8; 16];
        k[..8].copy_from_slice(&a.to_be_bytes());
        gnitz_wire::encode_pk_column(&b.to_le_bytes(), type_code::I64, &mut k[8..]);
        k
    };
    // Sorted by compare_pk_bytes: col0 asc, col1 signed asc (negatives first).
    let rows = [(mk(1, -5), 1i64), (mk(1, -1), 1), (mk(1, 3), 1), (mk(2, -9), 1)];
    let mut b = Batch::with_schema(schema, rows.len());
    for (pk, val) in &rows {
        b.extend_pk_bytes(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated, &schema);
    let batch = Rc::new(b);
    let mut cursor = create_read_cursor(&[batch], &[], schema);

    // Prefix is the OPK image of the leading U64 column (big-endian).
    let prefix = 1u64.to_be_bytes();
    let mut found: Vec<[u8; 16]> = Vec::new();
    if cursor.seek_first_positive_with_prefix(&prefix) {
        loop {
            let mut k = [0u8; 16];
            k.copy_from_slice(cursor.current_pk_bytes());
            found.push(k);
            cursor.advance();
            if !cursor.walk_to_positive_with_prefix(&prefix) {
                break;
            }
        }
    }
    assert_eq!(found.len(), 3, "negative-suffix rows must not be skipped");
    assert_eq!(found[0], mk(1, -5));
    assert_eq!(found[1], mk(1, -1));
    assert_eq!(found[2], mk(1, 3));
}

/// Drain a cursor via repeated `drain_chunk(n)` calls, concatenating
/// `(pk, weight, val)` rows. Asserts every chunk respects the row cap,
/// is non-empty, and carries the sorted+consolidated flags.
fn drain_chunks(cursor: &mut ReadCursor, n: usize) -> Vec<(u128, i64, i64)> {
    let mut rows = Vec::new();
    while let Some(chunk) = cursor.drain_chunk(n) {
        assert!(chunk.count <= n, "chunk overflow: {} > {}", chunk.count, n);
        assert!(chunk.count > 0, "drain_chunk returned an empty Some chunk");
        assert!(chunk.is_sorted() && chunk.is_consolidated());
        for row in 0..chunk.count {
            let val = i64::from_le_bytes(chunk.get_col_ptr(row, 0, 8).try_into().unwrap());
            rows.push((chunk.get_pk(row), chunk.get_weight(row), val));
        }
    }
    assert!(cursor.drain_chunk(n).is_none(), "exhausted cursor must stay exhausted");
    rows
}

fn materialize_rows(sources: &[Rc<Batch>]) -> Vec<(u128, i64, i64)> {
    let cursor = create_read_cursor(sources, &[], make_schema_i64());
    let batch = cursor.materialize();
    (0..batch.count)
        .map(|row| {
            let val = i64::from_le_bytes(batch.get_col_ptr(row, 0, 8).try_into().unwrap());
            (batch.get_pk(row), batch.get_weight(row), val)
        })
        .collect()
}

/// Single-source fast path: exact multiple of the chunk size, with
/// remainder, single oversized chunk — all equal to `materialize`.
#[test]
fn drain_chunk_single_source_matches_materialize() {
    let rows: Vec<(u128, i64, i64)> = (1..=5).map(|i| (i as u128, 1i64, (i * 10) as i64)).collect();
    let batch = make_batch(&rows);
    let expected = materialize_rows(&[Rc::clone(&batch)]);
    for chunk_rows in [1, 2, 5, 100] {
        let mut cursor = create_read_cursor(&[Rc::clone(&batch)], &[], make_schema_i64());
        assert_eq!(
            drain_chunks(&mut cursor, chunk_rows),
            expected,
            "chunk_rows={chunk_rows}"
        );
    }
    // 4 rows / chunk 2: exact multiple (no trailing partial chunk).
    let even = make_batch(&rows[..4]);
    let mut cursor = create_read_cursor(&[Rc::clone(&even)], &[], make_schema_i64());
    assert_eq!(drain_chunks(&mut cursor, 2), materialize_rows(&[even]));
}

#[test]
fn drain_chunk_empty_cursor_returns_none() {
    let mut cursor = create_read_cursor(&[], &[], make_schema_i64());
    assert!(cursor.drain_chunk(4).is_none());
}

/// Multi-source cursor (merge path) folds weights, drops ghosts, and
/// never splits a (PK, payload) group across chunks; the concatenation
/// equals both `materialize` and the single-source equivalent.
#[test]
fn drain_chunk_multi_source_matches_single_source() {
    // pk=1: +1; pk=2: +2-1 = +1 (cross-source fold); pk=3: +1-1 = ghost;
    // pk=4: weight 2 in one source.
    let b1 = make_batch(&[(1, 1, 10), (2, 2, 20), (3, 1, 30), (4, 2, 40)]);
    let b2 = make_batch(&[(2, -1, 20), (3, -1, 30)]);
    let consolidated_equivalent = make_batch(&[(1, 1, 10), (2, 1, 20), (4, 2, 40)]);

    let expected = materialize_rows(&[Rc::clone(&b1), Rc::clone(&b2)]);
    assert_eq!(expected, materialize_rows(&[Rc::clone(&consolidated_equivalent)]));
    for chunk_rows in [1, 2, 100] {
        let mut multi = create_read_cursor(&[Rc::clone(&b1), Rc::clone(&b2)], &[], make_schema_i64());
        assert_eq!(
            drain_chunks(&mut multi, chunk_rows),
            expected,
            "merge path, chunk_rows={chunk_rows}"
        );

        let mut single = create_read_cursor(&[Rc::clone(&consolidated_equivalent)], &[], make_schema_i64());
        assert_eq!(
            drain_chunks(&mut single, chunk_rows),
            expected,
            "fast path, chunk_rows={chunk_rows}"
        );
    }
}

/// `copy_current_row_into` on an invalid cursor must be a no-op; the
/// byte-form PK write would otherwise index empty `sources` and panic.
#[test]
fn copy_current_row_into_invalid_is_noop() {
    let schema = make_schema_i64();
    let cursor = create_read_cursor(&[], &[], schema);
    assert!(!cursor.valid);
    let mut out = Batch::with_schema(schema, 1);
    cursor.copy_current_row_into(&mut out, 1);
    assert_eq!(out.count, 0, "invalid cursor copy must not write a row");
}

// -- Phase A: for_each_pk_group_row ------------------------------------

/// `for_each_pk_group_row` visits one entry per non-ghost (PK, payload)
/// sub-group with `current_*` committed (the callback reads columns/weight),
/// then leaves the exit state at the first row past the group.
#[test]
fn for_each_pk_group_row_visits_subgroups_and_exits_clean() {
    let schema = make_schema_i64();
    let b1 = make_batch(&[(5, 1, 100), (10, 1, 1000)]);
    let b2 = make_batch(&[(5, 3, 200)]); // PK=5 payload 200 @ +3
    let mut c = create_read_cursor(&[b1, b2], &[], schema);

    let mut seen: Vec<(u64, i64)> = Vec::new();
    c.for_each_pk_group_row(&5u128.to_be_bytes(), |cur| {
        seen.push((cur.current_key_narrow() as u64, cur.current_weight));
    });
    // Two sub-groups at PK=5: payload 100 @ +1, payload 200 @ +3 (payload-sorted).
    assert_eq!(seen, vec![(5, 1), (5, 3)]);
    // Exit: positioned at PK=10, fully committed.
    assert!(c.valid);
    assert_eq!(c.current_key_narrow(), 10);
    assert_eq!(c.current_weight, 1);
}

// -- Phase C: Pair (k=2) bypass ≡ Multi --------------------------------

/// Build a 2-source cursor but FORCE `SourceMode::Multi` (the production
/// selector always picks `Pair` at len 2, so there is otherwise no Multi(k=2)
/// to compare against). Re-seats each head at the start, builds the loser
/// tree, and drives — mirroring `new()`'s Multi arm.
fn create_cursor_force_multi(batches: &[Rc<Batch>]) -> ReadCursor {
    let mut c = create_read_cursor(batches, &[], make_schema_i64());
    for state in c.states.iter_mut() {
        state.position = 0;
    }
    c.mode = SourceMode::Multi(ReadCursor::build_tree(&c.sources, &c.states, &c.schema, c.is_pk_unique));
    c.drive();
    c
}

/// Pair (the production k=2 path) must produce output identical to the loser
/// tree on the same two sources — full scan and every `advance_to` landing —
/// including cross-source ghost folding, same-PK / multi-payload groups, and
/// cross-source weight sums.
#[test]
fn pair_equiv_multi() {
    type Rows = &'static [(u128, i64, i64)];
    let cases: &[(Rows, Rows)] = &[
        (&[(1, 1, 10), (3, 1, 30), (5, 1, 50)], &[(2, 1, 20), (4, 1, 40)]), // interleaved
        (&[(1, 1, 10), (2, 1, 20)], &[(2, -1, 20), (3, 1, 30)]),            // pk=2 ghost-folds
        (&[(5, 1, 100)], &[(5, 1, 200)]),                                   // same PK, two payloads
        (&[(5, 1, 100)], &[(5, -1, 100)]),                                  // same (PK,payload) → ghost
        (&[(5, 3, 100), (5, 1, 200)], &[(5, -1, 100)]),                     // multi-payload, partial fold
        (&[(1, 2, 10), (2, 5, 20)], &[(1, 1, 10), (2, 1, 20)]),             // cross-source weight sum
    ];
    for (a, b) in cases {
        let srcs = [make_batch(a), make_batch(b)];

        let mut pair = create_read_cursor(&srcs, &[], make_schema_i64());
        assert!(
            matches!(pair.mode, SourceMode::Pair),
            "production must pick Pair at len 2"
        );
        let mut multi = create_cursor_force_multi(&srcs);
        assert!(matches!(multi.mode, SourceMode::Multi(_)));
        assert_eq!(scan_all(&mut pair), scan_all(&mut multi), "scan a={a:?} b={b:?}");

        // advance_to: each landing (valid, PK bytes, net weight) matches Multi.
        for key in 0u128..=6 {
            let mut p = create_read_cursor(&srcs, &[], make_schema_i64());
            let mut m = create_cursor_force_multi(&srcs);
            p.advance_to(&key.to_be_bytes());
            m.advance_to(&key.to_be_bytes());
            assert_eq!(p.valid, m.valid, "advance_to({key}) valid a={a:?} b={b:?}");
            if p.valid {
                assert_eq!(
                    p.current_pk_bytes(),
                    m.current_pk_bytes(),
                    "advance_to({key}) pk a={a:?} b={b:?}"
                );
                assert_eq!(
                    p.current_weight, m.current_weight,
                    "advance_to({key}) weight a={a:?} b={b:?}"
                );
            }
        }
    }
}

/// Write `rows` (each `(pk, weight, val)`) to a freshly-streamed `(U128 PK |
/// I64 payload)` shard tagged with `flag` (`SHARD_FLAG_PK_UNIQUE` ⇒ the opened
/// shard reports `is_pk_unique`, `0` ⇒ it does not). `rows` must be PK-ascending.
fn write_test_shard(
    dir: &tempfile::TempDir,
    schema: &SchemaDescriptor,
    idx: usize,
    rows: &[(u128, i64, i64)],
    flag: u8,
) -> Rc<MappedShard> {
    let pks: Vec<u8> = rows.iter().flat_map(|&(pk, _, _)| pk.to_be_bytes()).collect();
    let weights: Vec<i64> = rows.iter().map(|&(_, w, _)| w).collect();
    let nulls = vec![0u64; rows.len()];
    let vals: Vec<i64> = rows.iter().map(|&(_, _, v)| v).collect();
    let blob: Vec<u8> = Vec::new();
    let regions: Vec<(*const u8, usize)> = vec![
        (pks.as_ptr(), pks.len()),
        (weights.as_ptr() as *const u8, weights.len() * 8),
        (nulls.as_ptr() as *const u8, nulls.len() * 8),
        (vals.as_ptr() as *const u8, vals.len() * 8),
        (blob.as_ptr(), blob.len()),
    ];
    let path = dir.path().join(format!("rc_{flag}_{idx}.db"));
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    super::super::shard_file::write_shard_streaming(
        libc::AT_FDCWD,
        &cpath,
        rows.len() as u32,
        &regions,
        schema,
        super::super::shard_file::ShardWriteOpts {
            flags: flag,
            ..Default::default()
        },
    )
    .unwrap();
    Rc::new(MappedShard::open(&cpath, schema, false).unwrap())
}

/// Stride-8 sibling of `write_test_shard`: `(U64 PK | I64 payload)`, 8-byte
/// big-endian PKs. A separate fn (not a stride param on `write_test_shard`)
/// so its 6 existing callers are untouched.
fn write_test_shard_u64(
    dir: &tempfile::TempDir,
    schema: &SchemaDescriptor,
    idx: usize,
    rows: &[(u64, i64, i64)],
    flag: u8,
) -> Rc<MappedShard> {
    let pks: Vec<u8> = rows.iter().flat_map(|&(pk, _, _)| pk.to_be_bytes()).collect();
    let weights: Vec<i64> = rows.iter().map(|&(_, w, _)| w).collect();
    let nulls = vec![0u64; rows.len()];
    let vals: Vec<i64> = rows.iter().map(|&(_, _, v)| v).collect();
    let blob: Vec<u8> = Vec::new();
    let regions: Vec<(*const u8, usize)> = vec![
        (pks.as_ptr(), pks.len()),
        (weights.as_ptr() as *const u8, weights.len() * 8),
        (nulls.as_ptr() as *const u8, nulls.len() * 8),
        (vals.as_ptr() as *const u8, vals.len() * 8),
        (blob.as_ptr(), blob.len()),
    ];
    let path = dir.path().join(format!("rc8_{flag}_{idx}.db"));
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    super::super::shard_file::write_shard_streaming(
        libc::AT_FDCWD,
        &cpath,
        rows.len() as u32,
        &regions,
        schema,
        super::super::shard_file::ShardWriteOpts {
            flags: flag,
            ..Default::default()
        },
    )
    .unwrap();
    Rc::new(MappedShard::open(&cpath, schema, false).unwrap())
}

/// The PkUnique drive path (all sources flagged `is_pk_unique`, payload
/// comparison skipped) and the payload path (same bytes, flag cleared) must
/// produce identical output on contract-satisfying unique-PK data. Only the
/// shard flag differs, so this isolates the comparator-path choice that
/// `with_row_cmp!` unifies. Cross-source PK 1 (shards A+C) and PK 7 (A+B)
/// repeat with identical payloads, so both paths must fold their weights.
#[test]
fn pk_unique_and_payload_paths_agree() {
    crate::foundation::posix_io::raise_fd_limit_for_tests();
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_i64();
    let shard_rows: [&[(u128, i64, i64)]; 3] = [
        &[(1, 1, 100), (4, 1, 400), (7, 1, 700)],
        &[(2, 1, 200), (5, 1, 500), (7, 1, 700)],
        &[(1, 1, 100), (3, 1, 300), (6, 1, 600)],
    ];
    let build = |flag: u8| -> Vec<Rc<MappedShard>> {
        shard_rows
            .iter()
            .enumerate()
            .map(|(i, rows)| write_test_shard(&dir, &schema, i, rows, flag))
            .collect()
    };
    let pku = build(super::super::layout::SHARD_FLAG_PK_UNIQUE);
    let plain = build(0);

    let mut pku_cursor = create_read_cursor(&[], &pku, schema);
    let mut plain_cursor = create_read_cursor(&[], &plain, schema);
    assert!(pku_cursor.is_pk_unique, "flag=PK_UNIQUE ⇒ PkUnique path");
    assert!(!plain_cursor.is_pk_unique, "flag=0 ⇒ payload path");

    let got = scan_all(&mut pku_cursor);
    assert_eq!(
        got,
        scan_all(&mut plain_cursor),
        "PkUnique path must equal payload path"
    );
    // Independent oracle: cross-source same-(PK,payload) rows fold their weights.
    assert_eq!(
        got,
        vec![
            (1, 0, 2),
            (2, 0, 1),
            (3, 0, 1),
            (4, 0, 1),
            (5, 0, 1),
            (6, 0, 1),
            (7, 0, 2)
        ],
        "merge must fold PK 1 and PK 7 across sources",
    );
}

/// The relocated PkUnique debug-assert must still fire when a flag-tagged
/// (PkUnique) source set VIOLATES the contract (same PK, different payloads) —
/// the only direct probe of the comparator's invariant now that it lives in
/// `with_row_cmp!`. Debug-only (the assert is compiled out in release).
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "PK tie with differing payloads")]
fn pk_unique_flag_with_conflicting_payloads_panics() {
    crate::foundation::posix_io::raise_fd_limit_for_tests();
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_i64();
    let flag = super::super::layout::SHARD_FLAG_PK_UNIQUE;
    // Three "PkUnique" shards sharing PK 5 with DIFFERENT payloads — illegal.
    // Three sources force `Multi`; comparing the tied heads (tree build or
    // drive) trips the relocated debug_assert.
    let a = write_test_shard(&dir, &schema, 0, &[(5, 1, 100)], flag);
    let b = write_test_shard(&dir, &schema, 1, &[(5, 1, 200)], flag);
    let c = write_test_shard(&dir, &schema, 2, &[(5, 1, 300)], flag);
    let mut cursor = create_read_cursor(&[], &[a, b, c], schema);
    while cursor.valid {
        cursor.advance();
    }
}

/// Throughput of the all-PkUnique `Multi` drive (the path now collapsed onto
/// `drive_with_inner`). Parity gate: the rows/s must not regress after the
/// collapse — in `--release` the relocated `debug_assert!` vanishes and the
/// PkUnique comparator is codegen-identical to a bare trivial one.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn read_cursor_drive_pk_unique_multi_bench() {
    use std::time::Instant;
    crate::foundation::posix_io::raise_fd_limit_for_tests();
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_i64(); // U128 PK (stride 16) | I64 payload

    // 4 shards, interleaved unique PKs (round-robin) so the Multi merge does
    // real cross-source work; flags = SHARD_FLAG_PK_UNIQUE so the opened
    // shards report `is_pk_unique` and the cursor drives the PkUnique path.
    const N_SHARDS: usize = 4;
    const PER_SHARD: u128 = 200_000;
    let flag = super::super::layout::SHARD_FLAG_PK_UNIQUE;
    let shards: Vec<Rc<MappedShard>> = (0..N_SHARDS as u128)
        .map(|s| {
            let rows: Vec<(u128, i64, i64)> = (0..PER_SHARD).map(|i| (i * N_SHARDS as u128 + s, 1, 7)).collect();
            write_test_shard(&dir, &schema, s as usize, &rows, flag)
        })
        .collect();

    // Build the cursor ONCE (construction stays outside the timed region — the
    // measurement is drive-only) and confirm we exercise the all-PkUnique
    // Multi path. `rewind()` re-drives from row 0 each iteration.
    let mut c = create_read_cursor(&[], &shards, schema);
    assert!(c.is_pk_unique, "bench must drive the all-PkUnique path");
    assert!(matches!(c.mode, SourceMode::Multi(_)), "bench must drive Multi");

    const ITERS: usize = 20;
    let total_rows = N_SHARDS as u128 * PER_SHARD;
    let mut sink = 0i64;
    let t = Instant::now();
    for _ in 0..ITERS {
        c.rewind();
        while c.valid {
            sink = sink.wrapping_add(std::hint::black_box(c.current_weight));
            c.advance();
        }
    }
    let secs = t.elapsed().as_secs_f64();
    std::hint::black_box(sink);
    let rps = (ITERS as u128 * total_rows) as f64 / secs;
    println!("drive_pk_unique_multi: {total_rows} rows × {ITERS} iters in {secs:.3}s = {rps:.0} rows/s");
}

/// Stride-8 sibling of `read_cursor_drive_pk_unique_multi_bench` — the worst
/// case for `compare_pk_ordering` (the 8-byte `pack_pk_be` arm + the
/// u128-vs-u64 compare). Single `U64`/`I64` PKs are the dominant table shape;
/// this is the regression guard for the common-width loser-tree drive.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn read_cursor_drive_pk_unique_multi_u64_bench() {
    use std::time::Instant;
    crate::foundation::posix_io::raise_fd_limit_for_tests();
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64(); // U64 PK (stride 8) | I64 payload

    const N_SHARDS: usize = 4;
    const PER_SHARD: u64 = 200_000;
    let flag = super::super::layout::SHARD_FLAG_PK_UNIQUE;
    let shards: Vec<Rc<MappedShard>> = (0..N_SHARDS as u64)
        .map(|s| {
            let rows: Vec<(u64, i64, i64)> = (0..PER_SHARD).map(|i| (i * N_SHARDS as u64 + s, 1, 7)).collect();
            write_test_shard_u64(&dir, &schema, s as usize, &rows, flag)
        })
        .collect();

    let mut c = create_read_cursor(&[], &shards, schema);
    assert!(c.is_pk_unique, "bench must drive the all-PkUnique path");
    assert!(matches!(c.mode, SourceMode::Multi(_)), "bench must drive Multi");

    const ITERS: usize = 20;
    let total_rows = N_SHARDS as u64 * PER_SHARD;
    let mut sink = 0i64;
    let t = Instant::now();
    for _ in 0..ITERS {
        c.rewind();
        while c.valid {
            sink = sink.wrapping_add(std::hint::black_box(c.current_weight));
            c.advance();
        }
    }
    let secs = t.elapsed().as_secs_f64();
    std::hint::black_box(sink);
    let rps = (ITERS as u64 * total_rows) as f64 / secs;
    println!("drive_pk_unique_multi_u64: {total_rows} rows × {ITERS} iters in {secs:.3}s = {rps:.0} rows/s");
}

/// Baseline: shard-backed merge-scan throughput. Four overlapping-key shards
/// of 256K rows each (U64 PK; I64, nullable I64, STRING payload) built via the
/// production `BatchBuilder`/`write_as_shard` region layout — the STRING values
/// exceed `SHORT_STRING_THRESHOLD` so they live in the blob heap and the drain's
/// German-string blob relocation on the scatter path is exercised; the nullable
/// I64 column is NULL on a subset. Drains a 4-source `ReadCursor` via
/// `drain_to_batch`, pinning the loser-tree merge + scatter over mmap'd shards.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn shard_merge_scan_bench() {
    use super::super::batch::BatchBuilder;
    use std::time::Instant;
    crate::foundation::posix_io::raise_fd_limit_for_tests();
    let dir = tempfile::tempdir().unwrap();

    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),    // PK
            SchemaColumn::new(type_code::I64, 0),    // I64 payload
            SchemaColumn::new(type_code::I64, 1),    // nullable I64
            SchemaColumn::new(type_code::STRING, 0), // long-string payload
        ],
        &[0],
    );

    const N_SHARDS: usize = 4;
    const PER_SHARD: u64 = 256 * 1024;
    // Overlapping key ranges: shard `s` spans [s·OFFSET, s·OFFSET + PER_SHARD),
    // so adjacent shards share keys and the loser tree does real cross-source
    // merge/consolidation work.
    const OFFSET: u64 = 100_000;

    // Construction + open outside the timed region (validate_checksums = false,
    // the query-time read path's flag).
    let shards: Vec<Rc<MappedShard>> = (0..N_SHARDS as u64)
        .map(|s| {
            let mut bb = BatchBuilder::new(schema);
            for i in 0..PER_SHARD {
                let key = i + s * OFFSET;
                bb.begin_row(key as u128, 1);
                bb.put_u64(key); // I64 region (bytes reused; value irrelevant)
                if i % 4 == 0 {
                    bb.put_null(); // nullable I64 NULL on a subset
                } else {
                    bb.put_u64(key.wrapping_mul(7));
                }
                bb.put_string(&format!("payload-string-value-{key}"));
                bb.end_row();
            }
            let batch = bb.finish();
            let path = dir.path().join(format!("ms_{s}.db"));
            let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            batch
                .write_as_shard(&cpath, &schema, super::super::shard_file::ShardWriteOpts::default())
                .unwrap();
            Rc::new(MappedShard::open(&cpath, &schema, false).unwrap())
        })
        .collect();

    let input_rows = N_SHARDS as u64 * PER_SHARD;

    // Build the cursor once (construction stays off the clock). One untimed
    // warm-up drain faults in the cold PK/payload pages (lazy mmap, no
    // MAP_POPULATE) so the timed region reflects merge/scatter compute.
    let mut c = create_read_cursor(&[], &shards, schema);
    std::hint::black_box(c.drain_to_batch(0));

    const ITERS: usize = 10;
    let mut sink = 0usize;
    let t = Instant::now();
    for _ in 0..ITERS {
        c.rewind();
        let out = c.drain_to_batch(0).expect("non-empty drain");
        sink = sink.wrapping_add(out.count);
        std::hint::black_box(&out);
    }
    let secs = t.elapsed().as_secs_f64();
    std::hint::black_box(sink);
    let rps = (ITERS as u64 * input_rows) as f64 / secs;
    println!("shard_merge_scan: {input_rows} input rows × {ITERS} iters in {secs:.3}s = {rps:.0} rows/s");
}

/// Baseline: shard PK point-probe throughput. One 1M-row shard (U64 PK, single
/// I64 payload). 100K `advance_to_exact_live` probes over present keys in
/// ascending order (the exact-hit lower-bound / monotone-sweep path) plus 100K
/// shuffled `seek_bytes` probes drawn ~50/50 from present keys and absent keys
/// (odd keys landing *between* the sparse even shard keys → lower-bound
/// resolution). Both APIs run a raw-`memcmp` binary-search/gallop over the PK
/// region — they do not consult the XOR8 filter — so this pins the
/// gallop/binary-search cost.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn shard_point_probe_bench() {
    use std::time::Instant;
    crate::foundation::posix_io::raise_fd_limit_for_tests();
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64(); // U64 PK | I64 payload

    const N: u64 = 1_000_000;
    const PROBES: usize = 100_000;

    // Sparse (even) shard keys so odd probes resolve a lower bound *between*
    // two present keys. Construction + open outside the timed region.
    let rows: Vec<(u64, i64, i64)> = (0..N).map(|i| (i * 2, 1, i as i64)).collect();
    let shard = write_test_shard_u64(&dir, &schema, 0, &rows, 0);

    // Evenly-spaced ascending present keys for the monotone advance sweep.
    let step = (N / PROBES as u64).max(1);
    let asc_keys: Vec<[u8; 8]> = (0..PROBES as u64).map(|j| (j * step * 2).to_be_bytes()).collect();

    // Shuffled 50/50 present (even) / absent (odd, between keys) probes.
    let mut rng = crate::test_rng::Rng::new(0xC0DE_1234_5678_9ABC);
    let seek_keys: Vec<[u8; 8]> = (0..PROBES)
        .map(|_| {
            let base = rng.gen_range(N) * 2; // an even present key
            if rng.next_u64() & 1 == 0 {
                base.to_be_bytes() // present → exact hit
            } else {
                (base + 1).to_be_bytes() // odd → lower bound between keys
            }
        })
        .collect();

    let mut c = create_read_cursor(&[], std::slice::from_ref(&shard), schema);

    // Untimed warm-up: one full ascending sweep faults in the PK pages.
    for k in &asc_keys {
        std::hint::black_box(c.advance_to_exact_live(k));
    }

    // Timed: advance_to_exact_live monotone forward sweep over present keys.
    c.rewind();
    let mut hits = 0usize;
    let t1 = Instant::now();
    for k in &asc_keys {
        if std::hint::black_box(c.advance_to_exact_live(k)) {
            hits += 1;
        }
    }
    let s1 = t1.elapsed().as_secs_f64();
    std::hint::black_box(hits);
    println!(
        "point_probe advance_to_exact_live: {PROBES} probes in {s1:.4}s = {:.0} probes/s ({hits} hits)",
        PROBES as f64 / s1
    );

    // Timed: seek_bytes shuffled present/absent probes (absolute seeks).
    let mut sink = 0u64;
    let t2 = Instant::now();
    for k in &seek_keys {
        c.seek_bytes(k);
        if c.valid {
            sink = sink.wrapping_add(c.current_key_narrow() as u64);
        }
    }
    let s2 = t2.elapsed().as_secs_f64();
    std::hint::black_box(sink);
    println!(
        "point_probe seek_bytes: {PROBES} probes in {s2:.4}s = {:.0} probes/s",
        PROBES as f64 / s2
    );
}

/// A long-string struct (len > 12) whose blob offset overruns the (empty) blob
/// must read back empty rather than abort: the offset bounds check is part of
/// `read_german_bytes`' decode. This is the engine-side hardening the panicking
/// decoder lacked, and it runs under the default debug profile — there is
/// deliberately no `debug_assert` on the overrun case.
#[test]
fn test_read_german_bytes_out_of_bounds_offset_returns_empty() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    );
    let mut b = Batch::with_schema(schema, 1);
    b.extend_pk(1);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    // len = 100 (> 12 → reads blob), offset 0 into an empty blob → out of bounds.
    let mut st = [0u8; 16];
    st[0..4].copy_from_slice(&100u32.to_le_bytes());
    st[8..16].copy_from_slice(&0u64.to_le_bytes());
    b.extend_col(0, &st);
    b.count += 1;
    b.certify_layout(Layout::Consolidated, &schema);
    let cursor = create_read_cursor(&[Rc::new(b)], &[], schema);
    assert!(cursor.valid, "cursor must position on the single row");
    assert_eq!(
        cursor.read_german_bytes(1),
        Vec::<u8>::new(),
        "out-of-bounds long-string offset must decode to empty, not panic"
    );
}
