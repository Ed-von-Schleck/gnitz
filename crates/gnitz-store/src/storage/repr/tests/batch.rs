use super::super::batch_pool::{drain_pool, recycle_buf};
use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::BatchBuilder;
use crate::test_support::{
    make_batch, make_batch_bytes, make_batch_opk, make_batch_raw, make_batch_u128_raw, make_schema_pk_u64_payload_blob,
    make_schema_pk_u64_payload_string, make_schema_u64_i64, make_string_batch, opk_pk, payload0_i64, pk_payload_schema,
    read_german_string, read_strings, weighted_rows, wide_pk_3xu64_schema,
};

/// `extend_pk`, through `BatchBuilder::begin_row`, stores a native key as its
/// right-aligned big-endian image at every narrow stride, and reads back as the
/// same value.
#[test]
fn extend_pk_round_trips_at_every_narrow_stride() {
    // Every key must fit its stride; `extend_pk` debug-asserts that nothing is
    // set above the window.
    let cases: &[(&[TypeCode], &[u128])] = &[
        (&[TypeCode::U8], &[0, 1, 200, 255]),
        (&[TypeCode::U16], &[0, 1, u16::MAX as u128]),
        (&[TypeCode::U32], &[0, 1, u32::MAX as u128]),
        (&[TypeCode::U64], &[0, 1, 1 << 32, u64::MAX as u128]),
        (
            &[TypeCode::U64, TypeCode::U32],
            &[1, 1 | ((u32::MAX as u128) << 64), 7 | (7 << 64)],
        ),
        (
            &[TypeCode::U128],
            &[0, 1, u64::MAX as u128, (u64::MAX as u128) + 1, u128::MAX],
        ),
    ];
    for &(tcs, keys) in cases {
        let schema = pk_payload_schema(tcs);
        let stride = schema.pk_stride();
        let rows: Vec<(u128, i64, i64)> = keys.iter().map(|&pk| (pk, 1, 0)).collect();
        let b = make_batch_u128_raw(&schema, &rows);
        for (i, &pk) in keys.iter().enumerate() {
            assert_eq!(
                b.get_pk_bytes(i),
                &pk.to_be_bytes()[16 - stride..],
                "stride {stride} row {i}: bytes"
            );
            assert_eq!(b.get_pk(i), pk, "stride {stride} row {i}: value");
        }
    }
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "does not fit 8 bytes")]
fn extend_pk_u64_batch_rejects_wide_pk() {
    let schema = make_schema_u64_i64();
    let mut b = Batch::empty_with_schema(&schema);
    b.reserve_rows(1);
    b.extend_pk((u64::MAX as u128) + 1);
}

#[test]
#[should_panic(expected = "extend_pk_bytes: length must equal pk_stride")]
fn extend_pk_bytes_length_mismatch_panics() {
    let mut b = Batch::empty_with_schema(&make_schema_u64_i64());
    b.reserve_rows(1);
    b.extend_pk_bytes(&[0u8; 7]);
}

/// `Batch` feeds its own count, stride and PK base into the shared seek kernel
/// (swept exhaustively in `seek`); both entry points must agree with a
/// linear scan over the same batch, at a narrow and a wide stride.
#[test]
fn pk_seeks_agree_with_a_linear_scan() {
    let narrow: Vec<Vec<u8>> = [10u64, 20, 30, 40, 50]
        .iter()
        .map(|v| v.to_be_bytes().to_vec())
        .collect();
    let narrow_probes: Vec<Vec<u8>> = [0u64, 5, 10, 15, 25, 50, 51, u64::MAX]
        .iter()
        .map(|v| v.to_be_bytes().to_vec())
        .collect();

    // A 3xU64 compound key ascending in compare_pk_bytes order (column 0
    // dominates, then 1, then 2); probes land on and between the stored keys.
    let wide = |a: u64, b: u64, c: u64| opk_pk(&wide_pk_3xu64_schema(), &[a as u128, b as u128, c as u128]);
    let wide_keys = vec![
        wide(0, 0, 0),
        wide(1, 0, 0),
        wide(1, 5, 0),
        wide(1, 5, 9),
        wide(2, 0, 0),
    ];
    let wide_probes = vec![
        wide(0, 0, 0),
        wide(0, 0, 1),
        wide(1, 0, 0),
        wide(1, 5, 0),
        wide(1, 5, 10),
        wide(3, 0, 0),
    ];

    for (schema, keys, probes) in [
        (pk_payload_schema(&[TypeCode::U64]), narrow, narrow_probes),
        (wide_pk_3xu64_schema(), wide_keys, wide_probes),
    ] {
        let rows: Vec<(&Vec<u8>, i64, i64)> = keys.iter().map(|k| (k, 1, 0)).collect();
        let b = make_batch_opk(&schema, &rows);

        for key in &probes {
            let want = (0..b.count)
                .find(|&i| crate::schema::key::compare_pk_bytes(b.get_pk_bytes(i), key) != std::cmp::Ordering::Less)
                .unwrap_or(b.count);
            assert_eq!(b.find_lower_bound_bytes(key), want, "probe={key:02x?}");
            // `advance_to` is the galloping form the merge drives; every hint,
            // a run-off past the end included, must land on the same index.
            for hint in 0..=b.count {
                assert_eq!(b.advance_to(key, hint), want, "probe={key:02x?} hint={hint}");
            }
        }
    }
}

/// `append_row_from_source` copies the PK verbatim and carries the
/// caller's weight, not the source row's.
#[test]
fn append_row_from_source_copies_pk_weight_and_payload() {
    let schema = make_schema_u64_i64();
    let src = make_batch(&schema, &[(0xDEAD_BEEF, 1, 0x4242)]);

    let mut dst = Batch::with_capacity(&schema, 1);
    dst.append_row_from_source(-1, &src, 0, None);

    assert_eq!(dst.count, 1);
    assert_eq!(dst.get_pk_bytes(0), &0xDEAD_BEEFu64.to_be_bytes());
    assert_eq!(dst.get_weight(0), -1);
    assert_eq!(payload0_i64(&dst, 0), 0x4242);
}

/// `rows` stamped `layout` unverified: a lying tag for the debug verifier to catch.
fn flagged_batch(rows: &[(u64, i64, i64)], layout: Layout) -> Batch {
    let mut b = make_batch_raw(&make_schema_u64_i64(), rows);
    b.set_layout_unchecked(layout);
    b
}

// The consolidated short-circuit trusts the Consolidated tag: an adjacent-equal
// (PK, payload) duplicate under that stamp must trip the verifier.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "not strictly")]
fn into_consolidated_panics_on_lying_consolidated_dup() {
    let _ = flagged_batch(&[(1, 1, 5), (1, 1, 5)], Layout::Consolidated).into_consolidated();
}

// Ghost clause: strictly ordered, but carrying a net-zero row under Consolidated.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "ghost not eliminated")]
fn into_consolidated_panics_on_consolidated_ghost() {
    let _ = flagged_batch(&[(1, 1, 0), (2, 0, 0), (3, 1, 0)], Layout::Consolidated).into_consolidated();
}

/// A layout rises only through `certify_layout`, and every append or `clear`
/// drops it to `Raw`.
#[test]
fn layout_lifecycle_default_raise_and_lower() {
    let schema = make_schema_u64_i64();
    let empty = Batch::with_capacity(&schema, 4);
    assert_eq!(empty.layout(), Layout::Raw, "constructor defaults Raw");
    assert!(empty.is_consolidated(), "an empty batch is structurally consolidated");
    let one = make_batch_raw(&schema, &[(7, 1, 70)]);
    assert_eq!(one.layout(), Layout::Raw);
    assert!(
        one.is_consolidated(),
        "a lone non-ghost row is structurally consolidated"
    );
    let ghost = make_batch_raw(&schema, &[(7, 0, 70)]);
    assert!(!ghost.is_consolidated(), "a lone zero-weight row is a ghost");

    let mut b = make_batch_raw(&schema, &[(1, 1, 10), (2, 1, 20)]);
    assert_eq!(b.layout(), Layout::Raw, "appending rows never raises the layout");
    // Genuinely (PK, payload)-sorted, ghost-free → certify Consolidated.
    b.certify_layout(Layout::Consolidated);
    assert!(b.is_consolidated());

    // Any append downgrades all the way to Raw (the W2M-class fail-safe).
    let src = make_batch_raw(&schema, &[(3, 1, 30)]);
    b.append_ranges(&src.as_mem_batch(), &[(0, 1)]);
    assert_eq!(b.layout(), Layout::Raw, "append downgrades to Raw");

    b.clear();
    assert_eq!(b.layout(), Layout::Raw, "clear resets to Raw");
    assert_eq!(b.count, 0, "clear drops the rows");
}

/// Appends grow the arena — from none at all, and in place within a pooled
/// arena that already has room, shifting each region to its new offset — and
/// every row reads back.
#[test]
fn appends_grow_the_arena_and_keep_every_row() {
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64, i64)> = (1..=200).map(|i| (i, 1 + i as i64 % 3, i as i64 * 10)).collect();
    let src = make_batch(&schema, &rows);

    let mut from_empty = Batch::empty_with_schema(&schema);
    from_empty.append_batch(&src);
    assert_eq!(weighted_rows(&from_empty), weighted_rows(&src));

    // An arena for 8 rows here is 256 bytes, so it takes this 512-byte buffer,
    // which then holds 16 rows.
    drain_pool();
    recycle_buf(Vec::with_capacity(512));
    let mut roomy = Batch::with_capacity(&schema, 8);
    let arena = roomy.data.as_ptr();
    roomy.append_ranges(&src.as_mem_batch(), &[(0, 8)]);
    roomy.append_ranges(&src.as_mem_batch(), &[(8, 16)]);
    assert_eq!(roomy.data.as_ptr(), arena, "precondition: the growth stays in place");
    assert_eq!(weighted_rows(&roomy), &weighted_rows(&src)[..16]);
}

/// Dropping a batch returns its data buffer to the thread-local pool; an empty
/// batch holds none to return.
#[test]
fn drop_pools_a_batch_buffer() {
    let schema = make_schema_u64_i64();
    drain_pool();
    let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
    let cap = batch.data.capacity();
    drop(batch);
    assert!(
        drain_pool().iter().any(|b| b.capacity() == cap),
        "the data buffer is pooled"
    );

    let empty = Batch::empty_with_schema(&schema);
    assert_eq!(empty.data.capacity(), 0);
    drop(empty);
    assert!(drain_pool().is_empty(), "an empty batch pools nothing");
}

/// Rows dropped by `truncate_to` re-enter `[count, capacity)`, so a later read
/// of a cell no refill wrote sees the poison, not the dropped row.
#[cfg(debug_assertions)]
#[test]
fn truncate_to_poisons_dropped_rows() {
    let mut batch = make_batch_raw(&make_schema_u64_i64(), &[(1, 1, 10), (2, 1, 20)]);
    batch.truncate_to(RowMark { count: 1, blob_len: 0 });
    for r in 0..batch.arena_regions() {
        let s = batch.strides[r] as usize;
        let start = batch.offsets[r] + s;
        assert!(
            batch.data[start..start + s].iter().all(|&b| b == 0xA5),
            "region {r} of the dropped row is not poisoned"
        );
    }
}

/// A batch whose buffers hold more than twice what its rows need comes back as
/// a tight copy with its layout; a tight one comes back as
/// the same allocation.
#[test]
fn trimmed_copies_only_oversized_batches() {
    let schema = make_schema_u64_i64();
    let src = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)]);

    let mut loose = Batch::with_capacity(&schema, 1000);
    loose.append_ranges(&src.as_mem_batch(), &[(0, 1)]);
    loose.certify_layout(Layout::Consolidated);
    let cap = loose.data.capacity();
    let trimmed = loose.trimmed();
    assert!(trimmed.data.capacity() < cap, "an oversized batch is copied down");
    assert_eq!(trimmed.count, 1);
    assert_eq!(trimmed.layout, Layout::Consolidated);

    let ptr = trimmed.data.as_ptr();
    let again = trimmed.trimmed();
    assert_eq!(again.data.as_ptr(), ptr, "a trimmed batch is not copied again");

    let mut tight = Batch::with_capacity(&schema, 4);
    tight.append_ranges(&src.as_mem_batch(), &[(0, 4)]);
    let ptr = tight.data.as_ptr();
    assert_eq!(
        tight.trimmed().data.as_ptr(),
        ptr,
        "a batch filled to its capacity is kept"
    );

    // 1-byte columns pad each region to 8 bytes: a copy holding one row is
    // still tight.
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend((0..6).map(|_| SchemaColumn::new(TypeCode::U8, false)));
    let narrow = Batch::clone(&Batch::zeroed(&SchemaDescriptor::new(&cols, &[0]), 1));
    let ptr = narrow.data.as_ptr();
    assert_eq!(narrow.trimmed().data.as_ptr(), ptr, "a padded one-row copy is kept");
}

// ── Gather / widen: blob arms, layout propagation, empty shape ──────────

/// A long (> 12 byte) value must resolve in the gathered output under BOTH blob
/// arms — sharing carries the source heap verbatim, relocating rewrites each
/// surviving cell into a fresh one.
#[test]
fn from_ranges_resolves_long_values_under_both_blob_arms() {
    let schema = make_schema_pk_u64_payload_blob();

    // Sharing arm: a small heap, where a per-cell rewrite would cost more
    // than the whole-heap memcpy it replaces.
    let long: &[u8] = b"a-fairly-long-blob-value-xyz"; // 28 bytes > 12
    let small = make_batch_bytes(&schema, &[(1, 1, long), (2, 1, b"hi")]);
    assert!(
        !super::super::merge::should_relocate_blob(small.blob.len(), small.count, 1),
        "precondition: this shape takes the sharing arm",
    );
    let shared = Batch::from_ranges(&small, &[(0, 1)], 0);
    assert_eq!(shared.count, 1);
    assert_eq!(read_german_string(&shared, 0, 0), long);
    assert_eq!(
        shared.blob.len(),
        small.blob.len(),
        "the sharing arm carries the heap whole"
    );

    // Relocating arm: a wide heap where one survivor out of a hundred would
    // otherwise carry every dropped row's span.
    let vals: Vec<Vec<u8>> = (0..100u64).map(|i| vec![b'a' + (i % 26) as u8; 1024]).collect();
    let rows: Vec<(u64, i64, &[u8])> = vals.iter().enumerate().map(|(i, v)| (i as u64, 1, &v[..])).collect();
    let wide = make_batch_bytes(&schema, &rows);
    assert!(
        super::super::merge::should_relocate_blob(wide.blob.len(), wide.count, 1),
        "precondition: this shape takes the relocating arm",
    );
    let relocated = Batch::from_ranges(&wide, &[(7, 8)], 0);
    assert_eq!(relocated.count, 1);
    assert_eq!(read_german_string(&relocated, 0, 0), vals[7]);
    assert!(
        relocated.blob.len() < wide.blob.len(),
        "the relocating arm carries only the survivor's span",
    );
}

/// `inherit_layout` is a bare field store with no debug verification, so a
/// gather that dropped the tag is caught nowhere at the producer — only
/// downstream, at the next skip-point that trusts the claim.
#[test]
fn from_ranges_inherits_its_source_layout() {
    let src = make_batch(&make_schema_u64_i64(), &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    assert!(src.is_consolidated(), "precondition: the source claims consolidated");

    let subset = Batch::from_ranges(&src, &[(0, 1), (2, 3)], 0);
    assert_eq!(subset.count, 2);
    assert!(
        subset.is_consolidated(),
        "a disjoint ascending subset keeps order, weights and distinctness",
    );
}

/// The widen at both NULL placements: the input's payload lands where the
/// placement leaves it, the fill column reads NULL, and the input's blob heap
/// comes along — without it a long (> 12 byte) string reads back as garbage.
#[test]
fn widened_with_nulls_places_the_fill_on_either_side() {
    let long: &[u8] = b"a-fairly-long-string-value"; // 26 bytes > 12
    let b = make_batch_bytes(&make_schema_pk_u64_payload_string(), &[(1, 1, long)]);

    let cols = |first: bool| -> SchemaDescriptor {
        let pk = SchemaColumn::new(TypeCode::U64, false);
        let s = SchemaColumn::new(TypeCode::String, false);
        let fill = SchemaColumn::new(TypeCode::I64, true);
        let cols = match first {
            true => [pk, fill, s],
            false => [pk, s, fill],
        };
        SchemaDescriptor::new(&cols, &[0])
    };

    for nulls_first in [false, true] {
        let out_schema = cols(nulls_first);
        let out = b.widened_with_nulls(&out_schema, nulls_first);
        let (str_slot, fill_slot) = match nulls_first {
            true => (1, 0),
            false => (0, 1),
        };

        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk(0), 1);
        assert!(!out.blob.is_empty(), "output blob must be propagated ({nulls_first})");
        assert_eq!(
            read_german_string(&out, str_slot, 0),
            long,
            "long string must resolve to the original ({nulls_first})"
        );
        let nw = out.get_null_word(0);
        assert!(gnitz_wire::null_word_get(nw, fill_slot), "the fill column reads NULL");
        assert!(!gnitz_wire::null_word_get(nw, str_slot), "the input column stays live");
        assert_eq!(out.layout(), Layout::Consolidated, "({nulls_first})");
    }
}

/// `consolidate_in_place` folds a raw batch, leaves a certified one alone, and
/// is a no-op on an empty one; `consolidate_if_needed` borrows a certified one.
#[test]
fn consolidate_in_place_folds_once_and_certifies() {
    let schema = make_schema_u64_i64();

    let mut raw = make_batch_raw(&schema, &[(2, 1, 20), (1, 1, 10), (1, 2, 10)]);
    assert_eq!(raw.layout(), Layout::Raw);
    raw.consolidate_in_place();
    assert_eq!(raw.layout(), Layout::Consolidated);
    assert_eq!(
        weighted_rows(&raw),
        weighted_rows(&make_batch(&schema, &[(1, 3, 10), (2, 1, 20)]))
    );

    let mut already = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
    assert!(Batch::consolidate_if_needed(&already).is_none());
    already.consolidate_in_place();
    assert_eq!(already.count, 2);
    assert_eq!(already.layout(), Layout::Consolidated);

    let mut empty = Batch::empty_with_schema(&schema);
    empty.consolidate_in_place();
    assert_eq!(empty.count, 0);
}

/// Every operator's empty early-return goes through one of these two, so both
/// must carry the schema's PK stride rather than a fixed width: a placeholder
/// whose shape disagrees with its register's is one a later reader trusts.
#[test]
fn empty_constructors_carry_the_schema_pk_stride() {
    let narrow = make_schema_u64_i64(); // U64 PK → stride 8
    let empty = Batch::empty_with_schema(&narrow);
    assert_eq!(empty.count, 0);
    assert_eq!(empty.pk_stride(), 8);
    assert_eq!(empty.empty_like().pk_stride(), 8);

    let wide = Batch::empty_with_schema(&wide_pk_3xu64_schema()); // 3×U64 → stride 24
    assert_eq!(wide.pk_stride(), 24);
    assert_eq!(wide.empty_like().pk_stride(), 24);
}

/// `release_buffers` against `drop(take())` on the case that dominates: clearing
/// a register that is already free. The VM does that for every register of every
/// plan once per epoch, so the per-call constant is the whole comparison.
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn batch_release_bench() {
    use crate::test_support::bench_time;
    const ITERS: usize = 2_000_000;
    let schema = make_schema_u64_i64();

    let mut regs: Vec<Batch> = (0..64).map(|_| Batch::empty_with_schema(&schema)).collect();
    let release = bench_time(ITERS / 64, || {
        for b in &mut regs {
            std::hint::black_box(&mut *b).release_buffers();
        }
    });
    let take = bench_time(ITERS / 64, || {
        for b in &mut regs {
            drop(std::hint::black_box(&mut *b).take());
        }
    });

    println!(
        "already-empty register clear: release_buffers {:.1} ns, drop(take()) {:.1} ns",
        release.as_nanos() as f64 / ITERS as f64,
        take.as_nanos() as f64 / ITERS as f64,
    );
}

/// A weight-0 source row projects to no index entry; a surviving row's entry
/// carries that row's own weight, retractions included.
#[test]
fn project_index_drops_ghosts_and_carries_each_weight() {
    use crate::schema::{make_index_schema, KeySpec};

    let owner = make_schema_u64_i64();
    let cols = [1u32];
    let idx_schema = make_index_schema(&cols, &owner).unwrap();
    let spec = KeySpec::new(&cols, &owner).unwrap();
    let src = make_batch_raw(&owner, &[(1, 1, 10), (2, 0, 20), (3, -1, 30)]);

    let projected = src.project_index(&spec, &idx_schema);
    assert_eq!(projected.len(), 2, "the weight-0 row projects to no entry");
    assert_eq!(projected.get_weight(0), 1);
    assert_eq!(projected.get_weight(1), -1, "a retraction projects as a retraction");
}

/// Stamping a delta key on and off through `rekeyed` gives back every row whole:
/// compound key, weight, NULL word, and a long string's heap span.
#[test]
fn rekeyed_round_trips_a_key_prefix() {
    let view = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::String, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0, 1],
    );
    let delta = crate::schema::make_delta_schema(&view).expect("the stamped shape fits");
    // `(key, key, weight, string, nullable)`.
    type Row<'a> = (u64, u32, i64, &'a [u8], Option<i64>);
    let long: &[u8] = b"a-fairly-long-string-value"; // 26 bytes > 12
    let rows: [Row; 2] = [(1, 7, 1, long, Some(5)), (2, 3, -2, b"hi", None)];

    let mut b = BatchBuilder::new(view);
    for &(k0, k1, w, s, n) in &rows {
        b.begin_row_opk(&[k0 as u128, k1 as u128], w);
        b.put_blob(s);
        b.put_opt_int(n.map(|v| v as u128));
        b.end_row();
    }
    let mut b = b.finish();
    b.certify_layout(Layout::Consolidated);

    let stamped = b.rekeyed(&delta, |src, dst| crate::schema::write_delta_key(7, src, dst));
    assert_eq!(stamped.layout(), Layout::Raw, "rekeyed claims no order");
    assert_eq!(&stamped.get_pk_bytes(1)[..8], &7u64.to_be_bytes());
    let out = stamped.rekeyed(&view, |src, dst| {
        dst.copy_from_slice(crate::schema::delta_view_key(src))
    });
    assert_eq!(out.count, b.count);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(out.get_pk_bytes(i), b.get_pk_bytes(i), "row {i}: key");
        assert_eq!(out.get_weight(i), b.get_weight(i), "row {i}: weight");
        assert_eq!(out.get_null_word(i), b.get_null_word(i), "row {i}: null word");
        assert_eq!(read_german_string(&out, 0, i), row.3, "row {i}: string");
        assert_eq!(
            out.get_col_ptr(i, 1, 8),
            b.get_col_ptr(i, 1, 8),
            "row {i}: nullable cell"
        );
    }
}

/// Negate is the Z-Set group inverse: every weight flips sign and nothing else
/// moves. `i64::MIN` is its own inverse in ℤ/2⁶⁴, so `wrapping_neg` leaves it
/// where it is instead of overflowing.
#[test]
fn negate_flips_every_weight() {
    let out = make_batch(&make_schema_u64_i64(), &[(1, 3, 10), (2, -1, 20), (3, i64::MIN, 30)]).negated();

    let got: Vec<(i64, i64)> = (0..out.count)
        .map(|r| (out.get_weight(r), payload0_i64(&out, r)))
        .collect();
    assert_eq!(got, vec![(-3, 10), (1, 20), (i64::MIN, 30)]);
    assert!(out.is_consolidated());
}

// ── Dead heap bytes: sharing, truncation, concatenation, trimming ───────

/// `b`'s rows over its heap padded with `pad` unreferenced bytes, charged dead.
fn padded(mut b: Batch, pad: usize) -> Batch {
    b.blob.extend(std::iter::repeat_n(0u8, pad));
    b.dead_heap += pad;
    b
}

/// Carrying a heap for a subset charges exactly the long bytes of the rows left
/// out, on top of what the source already held dead; a short cell costs nothing.
/// A carry that would leave the heap wasteful, or that keeps nothing, is refused.
#[test]
fn carry_heap_charges_exactly_the_excluded_rows() {
    let mut rows: Vec<(u64, i64, &[u8])> = (1..=8).map(|pk| (pk, 1, &[b'x'; 40][..])).collect();
    rows[2].2 = &[b'c'; 30];
    rows[3].2 = b"short";
    let src = padded(make_string_batch(&rows), 5);
    let mask = src.schema().string_payload_slots();
    let mb = src.as_mem_batch();

    let mut out = Batch::with_capacity(src.schema(), 8);
    assert_eq!(out.carry_heap(&mb, mask, mask, &[(0, 2), (3, 8)]), Some(0));
    assert_eq!(out.dead_heap, 5 + 30, "row 2's span and the source's own padding");

    let mut short = Batch::with_capacity(src.schema(), 8);
    assert_eq!(short.carry_heap(&mb, mask, mask, &[(0, 3), (4, 8)]), Some(0));
    assert_eq!(short.dead_heap, 5, "a short row leaves no span behind");

    let mut all = Batch::with_capacity(src.schema(), 8);
    assert_eq!(all.carry_heap(&mb, mask, mask, &[(0, 8)]), Some(0));
    assert_eq!(all.dead_heap, 5, "keeping every row charges only the padding");

    for kept in [&[][..], &[(0, 2)][..]] {
        let mut refused = Batch::with_capacity(src.schema(), 8);
        assert_eq!(refused.carry_heap(&mb, mask, mask, kept), None, "{kept:?}");
        assert_eq!((refused.blob.len(), refused.dead_heap), (0, 0));
    }

    let mut no_string = Batch::with_capacity(src.schema(), 8);
    assert_eq!(
        no_string.carry_heap(&mb, mask, 0, &[(0, 8)]),
        None,
        "a copy keeping no string slot"
    );
}

/// A copy that drops a string slot is charged that slot's long bytes of every
/// row it keeps, and refused once they leave the heap wasteful.
#[test]
fn carry_heap_charges_the_dropped_slots() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::String, false),
            SchemaColumn::new(TypeCode::String, false),
        ],
        &[0],
    );
    let mut b = BatchBuilder::new(schema);
    for pk in 1..=4u128 {
        b.begin_row(pk, 1);
        b.put_blob(&[b'a'; 100]);
        b.put_blob(&[b'b'; 20]);
        b.end_row();
    }
    let src = b.finish();
    let mask = schema.string_payload_slots();
    let mb = src.as_mem_batch();

    let mut keeps_a = Batch::with_capacity(&schema, 4);
    assert_eq!(keeps_a.carry_heap(&mb, mask, 0b01, &[(0, 4)]), Some(0));
    assert_eq!(keeps_a.dead_heap, 4 * 20, "slot 1's span on every kept row");

    let mut keeps_b = Batch::with_capacity(&schema, 4);
    assert_eq!(
        keeps_b.carry_heap(&mb, mask, 0b10, &[(0, 4)]),
        None,
        "400 of 480 bytes dead"
    );
}

/// Truncation drops the heap appended since the mark along with its rows.
#[test]
fn truncate_to_drops_the_heap_appended_since_the_mark() {
    let src = make_string_batch(&[(1, 1, &[b'a'; 20]), (2, 1, &[b'b'; 30]), (3, 1, &[b'c'; 40])]);
    let mut out = Batch::concat(src.schema(), std::iter::once(src.as_mem_batch()));
    assert_eq!((out.blob.len(), out.dead_heap), (src.blob.len(), 0));
    let shared = out.mark();

    let fresh = make_string_batch(&[(4, 1, &[b'e'; 50])]);
    out.append_batch(&fresh);
    assert!(out.blob.len() > shared.blob_len);
    out.truncate_to(shared);
    assert_eq!((out.blob.len(), out.dead_heap), (src.blob.len(), 0));
    assert_eq!(read_strings(&out), read_strings(&src));
}

/// Concatenation carries each heap whole: every string reads back, the output
/// heap is the sum of its inputs', and so is its dead-byte bound.
#[test]
fn concat_carries_every_heap_whole() {
    let a = padded(make_string_batch(&[(1, 1, &[b'a'; 20]), (2, 1, b"tiny")]), 3);
    let b = padded(make_string_batch(&[(3, 1, &[b'b'; 30]), (4, 1, &[b'c'; 25])]), 4);
    let out = Batch::concat(a.schema(), [a.as_mem_batch(), b.as_mem_batch()].into_iter());
    assert_eq!(
        read_strings(&out),
        [vec![b'a'; 20], b"tiny".to_vec(), vec![b'b'; 30], vec![b'c'; 25]]
    );
    assert_eq!(out.blob.len(), a.blob.len() + b.blob.len());
    assert_eq!(out.dead_heap, 3 + 4);
}

/// A source more than a quarter dead is relocated cell by cell, not carried: its
/// padding never reaches the output.
#[test]
fn a_wasteful_source_relocates_rather_than_adopts() {
    let live = make_string_batch(&[(1, 1, &[b'a'; 20])]);
    let wasteful = padded(make_string_batch(&[(2, 1, &[b'b'; 20])]), 100);
    assert!(super::super::merge::heap_is_wasteful(
        wasteful.dead_heap,
        wasteful.blob.len()
    ));
    let out = Batch::concat(
        live.schema(),
        [live.as_mem_batch(), wasteful.as_mem_batch()].into_iter(),
    );
    assert_eq!(read_strings(&out), [vec![b'a'; 20], vec![b'b'; 20]]);
    assert_eq!(out.blob.len(), 40, "the carried heap and the relocated span alone");
    assert_eq!(out.dead_heap, 0);
}

/// `trimmed` keeps a heap at most a quarter dead and compacts one past it,
/// keeping rows, order and layout.
#[test]
fn trimmed_compacts_past_a_quarter_dead() {
    // 60 live bytes: 20 dead is exactly a quarter of 80, 21 is past it.
    let rows: [(u64, i64, &[u8]); 2] = [(1, 1, &[b'a'; 20]), (2, 1, &[b'b'; 40])];
    let kept = padded(make_string_batch(&rows), 20).trimmed();
    assert_eq!((kept.blob.len(), kept.dead_heap), (80, 20), "a quarter dead is carried");

    let mut overcounted = make_string_batch(&rows);
    overcounted.dead_heap = 60;
    let measured = overcounted.trimmed();
    assert_eq!(
        (measured.blob.len(), measured.dead_heap),
        (60, 0),
        "an overcount is measured down"
    );

    let compacted = padded(make_string_batch(&rows), 21).trimmed();
    assert_eq!((compacted.blob.len(), compacted.dead_heap), (60, 0));
    assert_eq!(read_strings(&compacted), [vec![b'a'; 20], vec![b'b'; 40]]);
    assert_eq!(compacted.layout, Layout::Consolidated);
}

/// Retired instructions of `append_batch` over a string-bearing source: a 64-row
/// batch of 40-byte strings appended 10⁴ times into one growing destination.
/// `#[ignore]`; run release:
///   cargo test -p gnitz-store --release append_batch_strings_bench -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn append_batch_strings_bench() {
    const ROWS: usize = 64;
    const APPENDS: usize = 10_000;
    let values: Vec<Vec<u8>> = (0..ROWS).map(|i| format!("{i:040}").into_bytes()).collect();
    let rows: Vec<(u64, i64, &[u8])> = values.iter().enumerate().map(|(i, v)| (i as u64, 1, &v[..])).collect();
    let src = make_string_batch(&rows);
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let (dst, instructions) = counter.measure(|| {
        let mut dst = Batch::empty_with_schema(src.schema());
        for _ in 0..APPENDS {
            dst.append_batch(std::hint::black_box(&src));
        }
        dst
    });
    assert_eq!(dst.count, ROWS * APPENDS);
    println!(
        "append_batch_strings_bench: {} instr/append, heap {} bytes",
        instructions / APPENDS as u64,
        dst.blob.len()
    );
}
