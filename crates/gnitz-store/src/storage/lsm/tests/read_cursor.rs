use super::super::batch::REG_PK;
use super::super::layout::ENCODING_CONSTANT;
use super::super::shard_file::region_dir;
use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::{BatchBuilder, Layout};
use crate::test_support::{
    make_batch_u128, make_schema_i64pk_i64, make_schema_pk_u64_payload_string, make_schema_u128_i64,
    make_schema_u64_i64, opk_pk, pk_payload_schema, wide_pk_3xu64_schema,
};

/// Build an `Rc<Batch>` with i64-payload rows.  Tests pre-sort their
/// inputs and have at most one row per (PK, payload), so the batch is
/// certified sorted+consolidated.
fn make_batch(rows: &[(u128, i64, i64)]) -> Rc<Batch> {
    Rc::new(make_batch_u128(&make_schema_u128_i64(), rows))
}

/// PK = (col0:U64, col1:U64); payload = I64. Stored first-column-major.
fn make_schema_compound_u64() -> SchemaDescriptor {
    pk_payload_schema(&[type_code::U64, type_code::U64])
}

/// OPK bytes of a `(U64, U64)` compound key. Encoded through the production
/// encoder, so a broken encoder fails the test rather than agreeing with it.
fn compound_pk_bytes(c0: u64, c1: u64) -> Vec<u8> {
    opk_pk(&make_schema_compound_u64(), &[c0 as u128, c1 as u128])
}

/// Drain reading each row's payload value (logical col 1) alongside its PK and
/// weight. The payload read is what gives the fold assertions their teeth: a
/// merge that ordered on PK alone leaks an extra row whose payload is the only
/// thing distinguishing it from the survivor.
fn scan_all_with_val(cursor: &mut ReadCursor) -> Vec<(u64, i64, i64)> {
    let mut rows = Vec::new();
    while cursor.valid {
        let (src, row) = cursor.current_row_source();
        // Logical col 1 is payload slot 0 of this fixture's schema.
        let val = crate::storage::payload_u64(src, row, 0) as i64;
        rows.push((cursor.current_key_narrow() as u64, cursor.current_weight, val));
        cursor.advance();
    }
    rows
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

/// The N-way merge over cursor sources: rows come out in (PK, payload) order,
/// entries sharing a full (PK, payload) identity fold their weights, a fold to
/// zero drops the row, and two rows sharing only a PK both survive. Asserted on
/// the payload value, so a row that leaked past the fold cannot hide behind a
/// matching PK and weight.
#[test]
fn cursor_merge_orders_folds_weights_and_drops_ghosts() {
    /// A name, one `(pk, weight, payload)` row list per source, and the
    /// `(pk, weight, payload)` rows the merge must emit.
    type Case = (&'static str, Vec<Vec<(u128, i64, i64)>>, Vec<(u64, i64, i64)>);
    let cases: Vec<Case> = vec![
        ("no sources", vec![], vec![]),
        (
            "one source",
            vec![vec![(1, 1, 10), (2, 1, 20), (3, 1, 30)]],
            vec![(1, 1, 10), (2, 1, 20), (3, 1, 30)],
        ),
        (
            "two interleaved sources",
            vec![vec![(1, 1, 10), (3, 1, 30)], vec![(2, 1, 20), (4, 1, 40)]],
            vec![(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)],
        ),
        (
            "a cross-source retraction cancels its row",
            vec![vec![(5, 1, 50), (10, 1, 100)], vec![(5, -1, 50)]],
            vec![(10, 1, 100)],
        ),
        (
            "one PK, two payloads — both survive, in payload order",
            vec![vec![(5, 1, 200)], vec![(5, 1, 100)]],
            vec![(5, 1, 100), (5, 1, 200)],
        ),
        (
            "one (PK, payload) in two sources — weights sum",
            vec![vec![(5, 3, 50)], vec![(5, 7, 50)]],
            vec![(5, 10, 50)],
        ),
    ];

    for (name, sources, want) in cases {
        let schema = make_schema_u128_i64();
        let batches: Vec<_> = sources.iter().map(|rows| make_batch(rows)).collect();
        let mut cursor = create_read_cursor(&batches, &[], schema);
        assert_eq!(scan_all_with_val(&mut cursor), want, "{name}");
        assert!(!cursor.valid, "{name}: a drained cursor is invalid");
    }
}

#[test]
fn test_seek_compound_pk_lands_on_exact_row() {
    let schema = make_schema_compound_u64();
    // Canonical (first-column-major) storage order: (1,5) then (2,3).
    // As u128 the order is reversed: pack(2,3) < pack(1,5).
    let mut b = Batch::with_capacity(schema, 2);
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

/// OPK bytes for a single I64 PK column (big-endian, sign bit flipped).
fn i64_opk(v: i64) -> [u8; 8] {
    opk_pk(&make_schema_i64pk_i64(), &[v as u128])
        .try_into()
        .expect("a single I64 PK column encodes to 8 bytes")
}

/// A signed single-column PK's negative keys sort *after* positives in raw
/// u128 order, while OPK (BE + sign-bit flip) sorts them first. `seek_bytes`
/// on the OPK key must land on the matching row.
#[test]
fn test_seek_signed_pk_lands_on_negative_row() {
    let schema = make_schema_i64pk_i64();
    // Storage (signed) order: -3, -1, 2.
    let mut b = Batch::with_capacity(schema, 3);
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
fn test_cursor_same_pk_nonadjacent_payload_fold() {
    let schema = make_schema_u128_i64();
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

/// The bound counts the row the cursor sits on, so it is what a walk from here
/// will emit — a freshly-opened 3-row cursor answers 3, even though the open's
/// own drive already moved `position` past the first row.
#[test]
fn test_estimated_length_counts_the_positioned_row() {
    let schema = make_schema_u128_i64();
    let batch = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    let mut cursor = create_read_cursor(&[batch], &[], schema);
    assert_eq!(cursor.estimated_length(), 3);
    cursor.advance();
    assert_eq!(cursor.estimated_length(), 2);
    cursor.advance();
    assert_eq!(cursor.estimated_length(), 1);
    cursor.advance();
    assert!(!cursor.valid);
    assert_eq!(cursor.estimated_length(), 0);
}

#[test]
fn test_scatter_constant_pk_shard() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u128_i64();

    let shard = write_test_shard(&dir, &schema, 0, &[(42, 1, 999)]);
    let on_disk = dir.path().join(format!("rc{}_0.db", schema.pk_stride()));
    assert_eq!(
        region_dir(&std::fs::read(&on_disk).unwrap(), REG_PK).1,
        ENCODING_CONSTANT,
        "fixture premise: a single-row PK region is Constant-encoded",
    );

    let cursor = create_read_cursor(&[], &[shard], schema);
    let result = cursor.materialize();
    assert_eq!(result.count, 1);
    assert_eq!(result.get_pk(0), 42u128);
}

#[test]
fn seek_bytes_lands_on_lower_bound_narrow() {
    // Narrow single-PK (U128, stride 16): seek_bytes lands on the first row
    // whose PK >= the (OPK) key — the lower bound. OPK for a U128 PK is the
    // value's big-endian bytes.
    let schema = make_schema_u128_i64();
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

/// OPK bytes for key `n` at `stride`: `n`'s big-endian image right-aligned into
/// `stride` zero-padded bytes. That is the column's OPK for a single-column
/// U64/U128 PK and the concat-OPK for a compound all-`U64` one — monotone in `n`
/// either way, and full-width through the stride's dispatch arm.
#[inline]
pub(super) fn adv_key(n: u64, stride: usize) -> [u8; 40] {
    let mut k = [0u8; 40];
    k[stride - 8..stride].copy_from_slice(&n.to_be_bytes());
    k
}

/// Drive one reused cursor through `keys`, asserting each `advance_to` lands where
/// a from-scratch `seek_bytes` on a fresh `mk()` cursor would — at any probe order:
/// strict-forward (the in-place gallop), `Equal`, or backward (both the replay).
/// Weight included, which is what pins cross-source ghost folding.
pub(super) fn adv_assert_cursor_oracle(mk: impl Fn() -> ReadCursor, stride: usize, keys: &[u64]) {
    let mut adv = mk();
    for &v in keys {
        let k = adv_key(v, stride);
        adv.advance_to(&k[..stride]);
        let mut fresh = mk();
        fresh.seek_bytes(&k[..stride]);
        assert_eq!(adv.valid, fresh.valid, "advance_to oracle valid v={v}");
        if adv.valid {
            assert_eq!(
                adv.current_pk_bytes(),
                fresh.current_pk_bytes(),
                "advance_to oracle pk v={v}"
            );
            assert_eq!(
                adv.current_weight, fresh.current_weight,
                "advance_to oracle weight v={v}"
            );
        }
    }
}

/// [`adv_assert_cursor_oracle`] over the `u128`-PK batch fixtures below.
fn assert_advance_to_matches_seek_oracle(schema: SchemaDescriptor, sources: &[Rc<Batch>], probes: &[u64]) {
    adv_assert_cursor_oracle(|| create_read_cursor(sources, &[], schema), 16, probes);
}

/// `advance_to` (forward-only, position-seeded) lands on the identical row a
/// from-scratch `seek_bytes` would, across a monotone ascending probe sweep —
/// for both a single-source cursor (rebuild fallback) and a multi-source
/// cursor (the in-place loser-tree gallop). The sweep also re-seeks the
/// current key (probe `20` after landing on `20` from probe `15`), exercising
/// the `Equal` rebuild fallback on the same cursor.
#[test]
fn advance_to_lands_like_seek_bytes_monotone() {
    let schema = make_schema_u128_i64();
    let b0 = make_batch(&[(10u128, 1, 100), (30, 1, 300), (50, 1, 500), (70, 1, 700)]);
    let b1 = make_batch(&[(20u128, 1, 200), (40, 1, 400), (60, 1, 600)]);
    // Monotone ascending: below-min, present, absent-between, above-max.
    let probes: &[u64] = &[0, 10, 15, 20, 35, 50, 55, 70, 71];
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
    let schema = make_schema_u128_i64();
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
    let schema = make_schema_u128_i64();
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
    let schema = make_schema_u128_i64();
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
    let schema = make_schema_u128_i64();
    let b_short = make_batch(&[(10u128, 1, 100), (20, 1, 200)]); // max 20
    let b_long = make_batch(&[(10u128, 1, 100), (50, 1, 500), (90, 1, 900)]);
    // 40 gallops b_short to its end (pop_top), leaving b_long to emit 50;
    // 60 → 90; 100 → exhausted (fast path over an empty heap).
    assert_advance_to_matches_seek_oracle(schema, &[b_short, b_long], &[40, 60, 100]);
}

/// The column-major scatter handles any number of sources; this drives it well
/// past the 16 an earlier inline cap allowed.
#[test]
fn test_scatter_many_sources_beyond_old_cap() {
    let schema = make_schema_u128_i64();
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

/// Multi-source merge over a compound `(col_A, col_B)` PK must order by
/// `(col_A, col_B)`. As a raw `u128`, col_B occupies the high 64 bits, so
/// the integer-comparison shortcut would (wrongly) order by `(col_B,
/// col_A)`. Chosen rows make the two orderings disagree.
#[test]
fn test_compound_pk_multi_source_merge_order() {
    let schema = make_schema_compound_u64();
    let make = |a: u64, b: u64, val: i64| -> Rc<Batch> {
        let mut bt = Batch::with_capacity(schema, 1);
        bt.extend_pk_bytes(&compound_pk_bytes(a, b));
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

/// OPK bytes for a 3×U64 compound PK, encoded through the production encoder.
fn pk3(a: u64, b: u64, c: u64) -> [u8; 24] {
    opk_pk(&wide_pk_3xu64_schema(), &[a as u128, b as u128, c as u128])
        .try_into()
        .expect("a 3xU64 PK encodes to 24 bytes")
}

fn make_wide_batch(rows: &[([u8; 24], i64, i64)]) -> Rc<Batch> {
    let schema = wide_pk_3xu64_schema();
    let mut bt = Batch::with_capacity(schema, rows.len().max(1));
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
    let schema = pk_payload_schema(&[type_code::U64, type_code::I64]);
    assert_eq!(schema.pk_stride(), 16);
    let mk = |a: u64, b: i64| -> [u8; 16] {
        opk_pk(&schema, &[a as u128, b as u128])
            .try_into()
            .expect("a (U64, I64) PK encodes to 16 bytes")
    };
    // Sorted by compare_pk_bytes: col0 asc, col1 signed asc (negatives first).
    let rows = [(mk(1, -5), 1i64), (mk(1, -1), 1), (mk(1, 3), 1), (mk(2, -9), 1)];
    let mut b = Batch::with_capacity(schema, rows.len());
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
    cursor.for_each_positive_with_prefix(&prefix, |c| {
        let mut k = [0u8; 16];
        k.copy_from_slice(c.current_pk_bytes());
        found.push(k);
    });
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
    let cursor = create_read_cursor(sources, &[], make_schema_u128_i64());
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
        let mut cursor = create_read_cursor(&[Rc::clone(&batch)], &[], make_schema_u128_i64());
        assert_eq!(
            drain_chunks(&mut cursor, chunk_rows),
            expected,
            "chunk_rows={chunk_rows}"
        );
    }
    // 4 rows / chunk 2: exact multiple (no trailing partial chunk).
    let even = make_batch(&rows[..4]);
    let mut cursor = create_read_cursor(&[Rc::clone(&even)], &[], make_schema_u128_i64());
    assert_eq!(drain_chunks(&mut cursor, 2), materialize_rows(&[even]));
}

#[test]
fn drain_chunk_empty_cursor_returns_none() {
    let mut cursor = create_read_cursor(&[], &[], make_schema_u128_i64());
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
        let mut multi = create_read_cursor(&[Rc::clone(&b1), Rc::clone(&b2)], &[], make_schema_u128_i64());
        assert_eq!(
            drain_chunks(&mut multi, chunk_rows),
            expected,
            "merge path, chunk_rows={chunk_rows}"
        );

        let mut single = create_read_cursor(&[Rc::clone(&consolidated_equivalent)], &[], make_schema_u128_i64());
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
    let schema = make_schema_u128_i64();
    let cursor = create_read_cursor(&[], &[], schema);
    assert!(!cursor.valid);
    let mut out = Batch::with_capacity(schema, 1);
    cursor.copy_current_row_into(&mut out, 1);
    assert_eq!(out.count, 0, "invalid cursor copy must not write a row");
}

// -- PK-group iteration -------------------------------------------------

/// `for_each_pk_group_row` visits one entry per non-ghost (PK, payload)
/// sub-group with `current_*` committed (the callback reads columns/weight),
/// then leaves the exit state at the first row past the group.
#[test]
fn for_each_pk_group_row_visits_subgroups_and_exits_clean() {
    let schema = make_schema_u128_i64();
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

// -- Two-source cursors -------------------------------------------------

/// Write `rows` (each `(pk, weight, val)`) to a freshly-streamed
/// `(unsigned PK | I64 payload)` shard. `rows` must be PK-ascending.
///
/// The PK width comes from `schema` — for an unsigned PK the OPK bytes ARE the
/// big-endian value, so the region is the low `pk_stride` bytes of each key's
/// BE image. That is what lets one writer serve both the 16-byte U128 and the
/// 8-byte U64 fixtures; a stride-specific twin would only restate the schema.
pub(super) fn write_test_shard(
    dir: &tempfile::TempDir,
    schema: &SchemaDescriptor,
    idx: usize,
    rows: &[(u128, i64, i64)],
) -> Rc<MappedShard> {
    let stride = schema.pk_stride() as usize;
    let rows: Vec<(Vec<u8>, i64, i64)> = rows
        .iter()
        .map(|&(pk, w, v)| (pk.to_be_bytes()[16 - stride..].to_vec(), w, v))
        .collect();
    let cpath = super::super::shard_file::write_test_shard(
        &dir.path().join(format!("rc{stride}_{idx}.db")),
        schema,
        &rows,
        super::super::shard_file::ShardWriteOpts::default(),
    );
    Rc::new(MappedShard::open(&cpath, schema, false).unwrap())
}

/// A `Multi` merge over several *shard* sources folds cross-source weights:
/// PK 1 (shards A+C) and PK 7 (A+B) repeat with identical payloads, so each
/// must emit once at the summed weight. The batch-source equivalent
/// two-source coverage above does not reach the shard path.
#[test]
fn multi_shard_merge_folds_cross_source_weights() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u128_i64();
    let shard_rows: [&[(u128, i64, i64)]; 3] = [
        &[(1, 1, 100), (4, 1, 400), (7, 1, 700)],
        &[(2, 1, 200), (5, 1, 500), (7, 1, 700)],
        &[(1, 1, 100), (3, 1, 300), (6, 1, 600)],
    ];
    let shards: Vec<Rc<MappedShard>> = shard_rows
        .iter()
        .enumerate()
        .map(|(i, rows)| write_test_shard(&dir, &schema, i, rows))
        .collect();

    let mut cursor = create_read_cursor(&[], &shards, schema);
    assert_eq!(
        scan_all(&mut cursor),
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

/// A long-string struct (len > 12) whose blob offset overruns the (empty) blob
/// must read back empty rather than abort: the offset bounds check is part of
/// `german_string_content`'s decode. This is the engine-side hardening the
/// panicking decoder lacked, and it runs under the default debug profile — there
/// is deliberately no `debug_assert` on the overrun case.
#[test]
fn a_long_string_whose_offset_overruns_the_blob_reads_back_empty() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    );
    let mut b = Batch::with_capacity(schema, 1);
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
    // Logical column 1 is the STRING; the U128 PK occupies no payload slot, so
    // it is payload index 0.
    let (src, row) = cursor.current_row_source();
    assert_eq!(
        crate::storage::payload_bytes(src, row, 0),
        &[] as &[u8],
        "out-of-bounds long-string offset must decode to empty, not panic"
    );
}

// ---------------------------------------------------------------------------
// seek_range_bytes' raw window count — the index-vs-full-scan gate reads the
// range size off the seek that positions the cursor, so it must be exact over
// raw entries.
// ---------------------------------------------------------------------------

/// Count `[start, end)` the expensive way: drive the merge and count the groups
/// a walk would step over, raw (every run's entry, ghosts and duplicates
/// included) rather than consolidated.
fn counted_walk_raw(
    batches: &[Rc<Batch>],
    shards: &[Rc<MappedShard>],
    schema: SchemaDescriptor,
    lo: u128,
    hi: Option<u128>,
) -> usize {
    let mut n = 0;
    for b in batches {
        for i in 0..b.count {
            let pk = b.get_pk(i);
            if pk >= lo && hi.is_none_or(|h| pk < h) {
                n += 1;
            }
        }
    }
    for s in shards {
        let cur = create_read_cursor(&[], &[Rc::clone(s)], schema);
        let mat = cur.materialize();
        for i in 0..mat.count {
            let pk = mat.get_pk(i);
            if pk >= lo && hi.is_none_or(|h| pk < h) {
                n += 1;
            }
        }
    }
    n
}

/// Over a multi-run cursor (memtable runs + a shard), the count `seek_range_bytes`
/// returns equals a counted walk of `[start, end)` — including a run that holds no
/// entry in range, and an `end = None` arm that counts to the end of every run.
/// A fresh cursor per range, because the window only ever narrows.
#[test]
fn seek_range_bytes_counts_the_raw_window() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u128_i64();

    // Overlapping runs: run A and the shard both hold pk=20 (a cross-run
    // duplicate the raw count counts twice, by contract), and run C holds
    // nothing inside [10, 40).
    let a = make_batch(&[(5, 1, 50), (20, 1, 200), (30, 1, 300)]);
    let b = make_batch(&[(10, 1, 100), (25, 1, 250), (60, 1, 600)]);
    let c = make_batch(&[(80, 1, 800), (90, 1, 900)]);
    let shard = write_test_shard(&dir, &schema, 0, &[(20, 1, 201), (35, 1, 350), (70, 1, 700)]);

    let batches = [Rc::clone(&a), Rc::clone(&b), Rc::clone(&c)];
    let shards = [Rc::clone(&shard)];

    for (lo, hi) in [
        (10u128, Some(40u128)), // spans all four runs; C contributes 0
        (0, Some(1000)),        // everything
        (0, Some(0)),           // empty
        (100, Some(200)),       // past every entry
        (20, Some(21)),         // the cross-run duplicate group alone
        (10, None),             // unbounded arm — to the end of every run
        (0, None),
    ] {
        let want = counted_walk_raw(&batches, &shards, schema, lo, hi);
        let mut cursor = create_read_cursor(&batches, &shards, schema);
        let got = cursor.seek_range_bytes(&lo.to_be_bytes(), hi.map(|h| h.to_be_bytes()).as_ref().map(|k| &k[..]));
        assert_eq!(got, want, "seek_range_bytes([{lo}, {hi:?}))");
    }
    // The cross-run duplicate is counted raw, once per run — the gate's inputs
    // are both raw counts, so both err the same direction.
    let mut cursor = create_read_cursor(&batches, &shards, schema);
    assert_eq!(
        cursor.seek_range_bytes(&20u128.to_be_bytes(), Some(&21u128.to_be_bytes()[..])),
        2
    );
}

// -- Live-source mode derivation ---------------------------------------

/// Four non-overlapping sources: source `i` holds PKs `i·100 + 1 ..= i·100 + 40`,
/// payload `pk · 10`. Any key window falls inside at most a few of them, so a seek
/// over the whole cursor empties the rest. The derivation reads only
/// `CursorState::is_valid`, so batches exercise it exactly as shards would.
fn four_disjoint_batches() -> Vec<Rc<Batch>> {
    (0..4)
        .map(|s| {
            let rows: Vec<(u128, i64, i64)> = (1..=40u128)
                .map(|i| {
                    let pk = s as u128 * 100 + i;
                    (pk, 1, pk as i64 * 10)
                })
                .collect();
            make_batch(&rows)
        })
        .collect()
}

/// `(pk, weight, payload)` for `lo ..= hi` of the fixture above.
fn expect_rows(lo: u64, hi: u64) -> Vec<(u64, i64, i64)> {
    (lo..=hi).map(|pk| (pk, 1, pk as i64 * 10)).collect()
}

/// The mode tracks which sources are live, at every count the dispatch
/// distinguishes, and never destroys a source to get there: a bounded or unbounded
/// seek collapses the merge mode down to a single source or to none, while
/// `rewind` and a backward `advance_to` re-liven what a range seek emptied. The
/// from-scratch oracle at the end pins the landing row and its weight across the
/// whole collapse, which the mode assertions alone do not.
#[test]
fn mode_follows_the_live_source_set() {
    let schema = make_schema_u128_i64();
    let b = four_disjoint_batches();
    let opk = |pk: u128| pk.to_be_bytes();

    let mut c = create_read_cursor(&b, &[], schema);
    assert!(c.mode.is_none());

    c.seek_range_bytes(&opk(301), Some(&opk(311)));
    assert_eq!(c.mode, Some(3), "range inside source 3");
    assert_eq!(c.sources.len(), 4, "no source is destroyed");
    assert_eq!(scan_all_with_val(&mut c), expect_rows(301, 310));

    // A source a range seek emptied is only unpositioned, so a backward
    // `advance_to` brings it back — in full, up to its clamped count.
    c.advance_to(&opk(101));
    assert!(c.mode.is_none(), "sources 1 and 2 are live again");
    let rows = scan_all_with_val(&mut c);
    assert_eq!(rows.len(), 40 + 40 + 10);
    assert_eq!(rows.first().copied(), Some((101, 1, 1010)));

    // A window spanning exactly two sources still merges through the tree.
    let mut c = create_read_cursor(&b, &[], schema);
    c.seek_range_bytes(&opk(220), Some(&opk(320)));
    assert!(c.mode.is_none());
    // 220..=240 from source 2, 301..=319 from source 3.
    assert_eq!(scan_all_with_val(&mut c).len(), 21 + 19);

    // The unbounded seek reaches the same collapse — a range-only derivation would
    // not — and `rewind` from it re-livens every source.
    let mut c = create_read_cursor(&b, &[], schema);
    c.seek_bytes(&opk(301));
    assert_eq!(c.mode, Some(3));
    assert_eq!(scan_all_with_val(&mut c).len(), 40);
    c.rewind();
    assert!(c.mode.is_none(), "rewind re-livens every source");
    assert_eq!(scan_all_with_val(&mut c).len(), 4 * 40);

    // A window covering nothing: no rows, no drain, no panic.
    let mut c = create_read_cursor(&b, &[], schema);
    c.seek_range_bytes(&opk(41), Some(&opk(51)));
    assert!(c.mode.is_none());
    assert!(!c.valid, "no live source, so the merge drives to invalid");
    assert!(c.drain_chunk(usize::MAX).is_none());

    assert_advance_to_matches_seek_oracle(schema, &b, &[0, 105, 250, 305, 120, 341, 220]);
}

/// A full drain of an untouched cursor reserves every source's heap — what the
/// blob proration collapses to when the row bound is the whole source set. The
/// fixture must sum past `POOL_BYPASS_BYTES` (2 MiB) so the reservation allocates
/// at its exact size, cancel nearly every row so the survivor is smaller, and keep
/// the FIRST key alive so the open's own drive stops there.
#[test]
fn materialize_reserves_the_whole_blob_arena() {
    let schema = make_schema_pk_u64_payload_string();
    const ROWS: u64 = 4096;
    let text = |pk: u64| format!("{pk:0>512}");
    let run = |lo: u64, weight: i64| {
        let mut bb = BatchBuilder::new(schema);
        for pk in lo..ROWS {
            bb.begin_row(pk as u128, weight);
            bb.put_string(&text(pk));
            bb.end_row();
        }
        Rc::new(bb.finish())
    };
    // Same keys and payloads at opposite weights, but for key 0.
    let inserts = run(0, 1);
    let retracts = run(1, -1);
    let reserved = inserts.blob.len() + retracts.blob.len();
    assert!(reserved > 2 * 1024 * 1024, "must exceed POOL_BYPASS_BYTES: {reserved}");

    let batch = create_read_cursor(&[inserts, retracts], &[], schema).materialize();
    assert_eq!(batch.count, 1, "all but the first key cancels");
    assert_eq!(batch.blob.len(), 512, "one surviving string");
    assert!(
        batch.blob.capacity() >= reserved,
        "a full drain reserves every source's heap: {} < {reserved}",
        batch.blob.capacity(),
    );
}

/// The chunked drain's blob reservation stays O(chunk) as the drain advances.
/// Prorating by the rows *remaining* would grow the per-row density every chunk,
/// reaching the whole heap on the last — the peak `drain_chunk` exists to avoid.
/// Observable because the pool retains nothing above `POOL_BYPASS_BYTES`.
#[test]
fn drain_chunk_blob_reservation_stays_o_chunk() {
    let schema = make_schema_pk_u64_payload_string();
    const PER_SOURCE: u64 = 4096;
    const CHUNK: usize = 512;
    let text = |pk: u64| format!("{pk:0>512}");
    // Interleaved keys, so both sources stay live and the drain runs through the
    // merge rather than the single-source bulk copy.
    let run = |parity: u64| {
        let mut bb = BatchBuilder::new(schema);
        for i in 0..PER_SOURCE {
            let pk = i * 2 + parity;
            bb.begin_row(pk as u128, 1);
            bb.put_string(&text(pk));
            bb.end_row();
        }
        Rc::new(bb.finish())
    };
    let (even, odd) = (run(0), run(1));
    let total_blob = even.blob.len() + odd.blob.len();
    assert!(
        total_blob > 2 * 1024 * 1024,
        "the whole heap must exceed POOL_BYPASS_BYTES for a whole-heap reservation to show: {total_blob}"
    );

    let mut cursor = create_read_cursor(&[even, odd], &[], schema);
    assert!(cursor.mode.is_none(), "both sources must stay live");
    let mut rows = 0usize;
    while let Some(chunk) = cursor.drain_chunk(CHUNK) {
        rows += chunk.count;
        assert!(
            chunk.blob.capacity() <= 2 * 1024 * 1024,
            "chunk reserved {} blob bytes of a {total_blob}-byte relation",
            chunk.blob.capacity(),
        );
    }
    assert_eq!(rows as u64, 2 * PER_SOURCE);
}

/// A key list interleaving present and absent keys over a multi-run cursor yields
/// exactly what a per-key fresh-cursor seek yields — including a key that is
/// absent only because its group folded to net zero, the case that makes
/// "the cursor is past the key ⇒ the key is absent" non-obvious.
#[test]
fn ascending_key_sweep_matches_per_key_fresh_seeks() {
    let schema = make_schema_u128_i64();
    // pk=30 nets to zero across the two runs; pk=50 carries two payloads.
    let a = make_batch(&[(10, 1, 100), (30, 1, 300), (50, 1, 500), (70, 1, 700)]);
    let b = make_batch(&[(20, 1, 200), (30, -1, 300), (50, 2, 501), (90, 1, 900)]);
    let sources = [Rc::clone(&a), Rc::clone(&b)];
    // Below the minimum, present, absent-between, the ghost, multi-payload, and
    // past the maximum.
    let keys: &[u128] = &[5, 10, 15, 20, 30, 40, 50, 70, 90, 95];

    let mut got = Batch::with_capacity(schema, 8);
    let mut sweep = create_read_cursor(&sources, &[], schema);
    for &k in keys {
        let key = k.to_be_bytes();
        if sweep.seek_pk_group_ascending(&key) {
            sweep.copy_positioned_pk_group_into(&key, &mut got);
        }
    }

    let mut want = Batch::with_capacity(schema, 8);
    for &k in keys {
        create_read_cursor(&sources, &[], schema).copy_live_pk_group_into(&k.to_be_bytes(), &mut want);
    }

    let rows = |b: &Batch| -> Vec<(u128, i64, i64)> {
        (0..b.count)
            .map(|r| {
                let val = i64::from_le_bytes(b.get_col_ptr(r, 0, 8).try_into().unwrap());
                (b.get_pk(r), b.get_weight(r), val)
            })
            .collect()
    };
    assert_eq!(rows(&got), rows(&want));
    assert_eq!(
        rows(&got),
        vec![
            (10, 1, 100),
            (20, 1, 200),
            (50, 1, 500),
            (50, 2, 501),
            (70, 1, 700),
            (90, 1, 900)
        ],
        "the ghost at 30 and every absent key contribute nothing",
    );
}

/// `next_chunk` stops on the row budget mid-list and resumes at the key it has not
/// consumed: a chunk boundary must not skip the next chunk's first key, and an
/// absent key inside a chunk must not shift the ones behind it.
#[test]
fn pk_set_gather_spans_chunk_boundaries() {
    let schema = make_schema_u128_i64();
    let present: Vec<u128> = (1..=8u128).map(|i| i * 10).collect();
    let batch = make_batch(&present.iter().map(|&pk| (pk, 1i64, pk as i64 * 10)).collect::<Vec<_>>());
    // 35 and 85 are absent; the rest are present, strictly ascending.
    let asked: Vec<u128> = vec![10, 20, 30, 35, 40, 50, 60, 70, 80, 85];
    let flat: Vec<u8> = asked.iter().flat_map(|k| k.to_be_bytes()).collect();

    for chunk in [1usize, 3, 7, 100] {
        let mut gather = PkSetGather::open(flat.clone(), schema, |_, _| {
            create_read_cursor(std::slice::from_ref(&batch), &[], schema)
        });
        let mut got: Vec<u128> = Vec::new();
        while let Some(out) = gather.next_chunk(chunk) {
            assert!(out.count > 0, "an empty chunk must be reported as None");
            got.extend((0..out.count).map(|r| out.get_pk(r)));
        }
        assert_eq!(got, present, "chunk={chunk}");
    }
}

/// A bounded read over a multi-shard STRING partition that only one shard covers
/// drains through the single-source bulk path, and the batch it returns carries
/// only the range's own heap bytes — the two halves composed, at a non-zero source
/// index so a drain writing back `states[0]` would be caught.
#[test]
fn bounded_string_read_carries_only_its_own_rows() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_pk_u64_payload_string();
    const PER_SHARD: u64 = 100;
    // 40-byte strings, all spilled to the heap.
    let text = |pk: u64| format!("{pk:0>40}");

    let shards: Vec<Rc<MappedShard>> = (0..2u64)
        .map(|s| {
            let mut bb = BatchBuilder::new(schema);
            for i in 0..PER_SHARD {
                let pk = s * 10_000 + i + 1;
                bb.begin_row(pk as u128, 1);
                bb.put_string(&text(pk));
                bb.end_row();
            }
            let cpath = std::ffi::CString::new(dir.path().join(format!("s{s}.db")).to_str().unwrap()).unwrap();
            bb.finish()
                .write_as_shard(&cpath, &schema, super::super::shard_file::ShardWriteOpts::default())
                .unwrap();
            Rc::new(MappedShard::open(&cpath, &schema, false).unwrap())
        })
        .collect();
    assert_eq!(shards[1].blob_len as u64, PER_SHARD * 40);

    let mut c = create_read_cursor(&[], &shards, schema);
    c.seek_range_bytes(&10_001u64.to_be_bytes(), Some(&10_004u64.to_be_bytes()));
    assert_eq!(c.mode, Some(1));

    let batch = c.drain_chunk(usize::MAX).expect("shard 1 window");
    assert_eq!(batch.count, 3);
    assert_eq!(batch.blob.len(), 3 * 40, "only the drained rows' strings");
    for i in 0..3 {
        assert_eq!(
            crate::test_support::read_german_string(&batch, 0, i),
            text(10_001 + i as u64).into_bytes(),
        );
    }
    assert!(!c.valid, "the window is fully drained");
}

// ---------------------------------------------------------------------------
// Skeleton shards: read-path coarsening
// ---------------------------------------------------------------------------

/// Write a payload-free skeleton shard: the PK-only projection of `schema`,
/// `SHARD_FLAG_SKELETON` stamped, one `(PK, coarse weight)` row per entry —
/// exactly what `compact::merge_and_route` emits for a dehydrated guard.
fn write_skeleton_shard(
    dir: &std::path::Path,
    name: &str,
    schema: &SchemaDescriptor,
    rows: &[(u64, i64)],
) -> MappedShard {
    let skel = crate::storage::lsm::compact::skeleton_schema(schema);
    let mut b = Batch::with_capacity(skel, rows.len().max(1));
    for &(pk, w) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.count += 1;
    }
    let cpath = std::ffi::CString::new(dir.join(name).to_str().unwrap()).unwrap();
    b.write_as_shard(&cpath, &skel, super::super::shard_file::ShardWriteOpts::SKELETON)
        .unwrap();
    // Opened under the *view* schema, which is how every reader sees it.
    MappedShard::open(&cpath, schema, false).unwrap()
}

/// `(U64 PK | nullable STRING)` — a `PayloadCmpKind::Generic` schema whose
/// payload can be NULL, so an all-NULL hydrated row is representable.
fn nullable_string_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 1),
        ],
        &[0],
    )
}

/// `(pk, weight)` of every group a full walk emits, plus whether it came out of
/// a skeleton source.
fn walk_groups(c: &mut ReadCursor) -> Vec<(u64, i64, bool)> {
    let mut out = Vec::new();
    while c.valid {
        out.push((c.current_key_narrow() as u64, c.current_weight, c.current_is_skeleton()));
        c.advance();
    }
    out
}

/// A skeleton shard opens under the full view schema: its payload columns read
/// `Absent`, its null word is the full pad mask, and its file is small enough to
/// be worth the trade.
#[test]
fn skeleton_shard_opens_under_the_view_schema() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64)> = (1..=8).map(|i| (i, i as i64)).collect();
    let shard = write_skeleton_shard(dir.path(), "sk.db", &schema, &rows);

    assert!(shard.is_skeleton());
    // The low bits are the writer's own arity — zero — so the ALTER-widening
    // decode maps every schema payload column to `Absent` and pads it NULL.
    let raw = std::fs::read(dir.path().join("sk.db")).unwrap();
    assert_eq!(
        gnitz_wire::read_u64_le(&raw, super::super::layout::OFF_FILE_NPC),
        super::super::layout::SHARD_FLAG_SKELETON,
    );
    assert_eq!(shard.null_pad_mask, 1);
    for (r, &(_, w)) in rows.iter().enumerate() {
        assert!(
            gnitz_wire::null_word_get(shard.get_null_word(r), 0),
            "row {r} pads NULL"
        );
        assert_eq!(shard.get_weight(r), w);
    }
    assert!(
        shard.file_len() > 0 && shard.file_len() < 1024,
        "skeleton files are tiny"
    );
}

/// Forging the flag bit onto a hydrated shard fails the descriptive digest — the
/// bit sits inside `desc_digest`'s span, so it is as unforgeable as any other
/// descriptive byte. Re-stamping the digest instead lets the forged word reach
/// the structural check, where an arity above `MAX_PAYLOAD_REGIONS` is still
/// rejected with the flag masked off.
#[test]
fn skeleton_flag_is_covered_by_the_digest_and_masked_before_the_bound() {
    use super::super::layout::{OFF_DESC_CHECKSUM, OFF_FILE_NPC, SHARD_FLAG_SKELETON};
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_int(42);
    bb.end_row();
    let path = dir.path().join("h.db");
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    bb.finish()
        .write_as_shard(&cpath, &schema, super::super::shard_file::ShardWriteOpts::default())
        .unwrap();
    let base = std::fs::read(&path).unwrap();

    let patched = |patch: &dyn Fn(&mut Vec<u8>), restamp: bool| {
        let mut d = base.clone();
        patch(&mut d);
        if restamp {
            let nr = crate::storage::lsm::batch::strides_from_schema(&schema).1 as usize + 1;
            let cs = super::super::layout::desc_digest(super::super::layout::shard_basename(cpath.to_bytes()), &d, nr);
            gnitz_wire::write_u64_le(&mut d, OFF_DESC_CHECKSUM, cs);
        }
        std::fs::write(&path, &d).unwrap();
        MappedShard::open(&cpath, &schema, false)
    };

    // Bare forgery: the digest rejects it.
    assert_eq!(
        patched(
            &|d| gnitz_wire::write_u64_le(d, OFF_FILE_NPC, SHARD_FLAG_SKELETON | 1),
            false
        )
        .err(),
        Some(super::super::error::StorageError::ChecksumMismatch),
    );
    // Re-stamped, with an out-of-range arity under the flag: the bound still
    // fires, so the flag cannot smuggle a count past it.
    assert_eq!(
        patched(
            &|d| gnitz_wire::write_u64_le(d, OFF_FILE_NPC, SHARD_FLAG_SKELETON | 66),
            true
        )
        .err(),
        Some(super::super::error::StorageError::InvalidShard),
    );
}

/// A PK group holding a skeleton row anywhere collapses to exactly one coarse
/// row, whatever hydrated rows sit above it — under **both** payload comparator
/// arms. `FixedIntNonnull` reads no null bitmap and gets `ZERO_CELL` back for an
/// absent column, so an all-zero hydrated row compares `Equal` to a skeleton one;
/// `Generic` tests the null bits first, so an all-NULL hydrated row does. The
/// skeleton-ness tiebreak is what makes the skeleton row the group exemplar in
/// either case.
#[test]
fn a_skeleton_row_coarsens_its_whole_pk_group() {
    let dir = tempfile::tempdir().unwrap();

    for (name, schema, zero_row) in [
        ("fixedint", make_schema_u64_i64(), true),
        ("generic", nullable_string_schema(), false),
    ] {
        // PK 1: skeleton (coarse +3) plus two newer hydrated rows.
        // PK 2: hydrated only. PK 3: skeleton whose coarse weight cancels.
        let sk = Rc::new(write_skeleton_shard(
            dir.path(),
            &format!("{name}_sk.db"),
            &schema,
            &[(1, 3), (3, 2)],
        ));
        let mut bb = BatchBuilder::new(schema);
        // A row that compares `Equal` to a skeleton row under this arm.
        bb.begin_row(1u128, 5);
        if zero_row {
            bb.put_int(0);
        } else {
            bb.put_null();
        }
        bb.end_row();
        bb.begin_row(1u128, 7);
        if zero_row {
            bb.put_int(9);
        } else {
            bb.put_string("nine");
        }
        bb.end_row();
        bb.begin_row(2u128, 4);
        if zero_row {
            bb.put_int(1);
        } else {
            bb.put_string("one");
        }
        bb.end_row();
        bb.begin_row(3u128, -2);
        if zero_row {
            bb.put_int(0);
        } else {
            bb.put_null();
        }
        bb.end_row();
        let mem = Rc::new(bb.finish().into_consolidated(&schema));

        let mut c = create_read_cursor(&[mem], &[sk], schema);
        assert_eq!(
            walk_groups(&mut c),
            vec![(1, 15, true), (2, 4, false)],
            "{name}: PK 1 folds to one coarse row (3+5+7), PK 3 ghosts at 2-2",
        );
    }
}

/// A cursor whose runs are all hydrated takes the `SKEL == false` path: element
/// identity stays (PK, payload), so two rows of one PK with different payloads
/// stay two groups.
#[test]
fn a_hydrated_only_cursor_is_unaffected() {
    let schema = make_schema_u128_i64();
    let b = make_batch(&[(1, 1, 10), (1, 1, 20), (2, 1, 30)]);
    let mut c = create_read_cursor(&[b], &[], schema);
    let mut n = 0;
    while c.valid {
        assert!(!c.current_is_skeleton());
        n += 1;
        c.advance();
    }
    assert_eq!(n, 3, "(PK, payload) identity is untouched without a skeleton run");
}
