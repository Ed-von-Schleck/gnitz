use super::super::batch_pool::drain_pool;
use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{
    make_batch, make_batch_bytes, make_batch_raw, make_schema_pk_u64_payload_blob, make_schema_pk_u64_payload_string,
    make_schema_u64_i64, make_string_batch, payload0_i64, read_german_string, read_strings, weighted_rows,
};

/// `append_row_from_source` copies the PK verbatim and carries the
/// caller's weight, not the source row's; a zero weight appends nothing.
#[test]
fn append_row_from_source_carries_the_callers_weight() {
    let schema = make_schema_u64_i64();
    let src = make_batch(&schema, &[(0xDEAD_BEEF, 1, 0x4242)]);

    let mut dst = Batch::with_capacity(&schema, 1);
    dst.append_row_from_source(-1, &src, 0, None);
    dst.append_row_from_source(0, &src, 0, None);

    assert_eq!(dst.count, 1);
    assert_eq!(dst.get_pk_bytes(0), &0xDEAD_BEEFu64.to_be_bytes());
    assert_eq!(dst.get_weight(0), -1);
    assert_eq!(payload0_i64(&dst, 0), 0x4242);
}

/// `rows` claimed consolidated unverified: a lying claim for the debug verifier
/// to catch.
#[cfg(debug_assertions)]
fn flagged_batch(rows: &[(u64, i64, i64)]) -> Batch {
    let mut b = make_batch_raw(&make_schema_u64_i64(), rows);
    b.set_consolidated_unchecked();
    b
}

// The consolidated short-circuit trusts the claim: an adjacent-equal
// (PK, payload) duplicate under it must trip the verifier.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "not strictly")]
fn into_consolidated_panics_on_lying_consolidated_dup() {
    let _ = flagged_batch(&[(1, 1, 5), (1, 1, 5)]).into_consolidated();
}

// Ghost clause: strictly ordered, but carrying a net-zero row under the claim.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "ghost not eliminated")]
fn into_consolidated_panics_on_consolidated_ghost() {
    let _ = flagged_batch(&[(1, 1, 0), (2, 0, 0), (3, 1, 0)]).into_consolidated();
}

/// `is_consolidated` holds structurally for a batch nothing can mis-fold, and
/// otherwise only under a certified claim.
#[test]
fn is_consolidated_without_a_claim_only_where_nothing_can_fold() {
    let schema = make_schema_u64_i64();
    assert!(Batch::with_capacity(&schema, 4).is_consolidated());
    assert!(make_batch_raw(&schema, &[(7, 1, 70)]).is_consolidated());
    assert!(
        !make_batch_raw(&schema, &[(7, 0, 70)]).is_consolidated(),
        "a lone ghost"
    );
    let mut two = make_batch_raw(&schema, &[(1, 1, 10), (2, 1, 20)]);
    assert!(!two.is_consolidated());
    two.certify_consolidated();
    assert!(two.is_consolidated());
}

/// An order- and weight-preserving copy inherits its source's claim; anything
/// else carries none.
#[test]
fn every_producer_states_its_claim() {
    const RAW: bool = false;
    const CONSOLIDATED: bool = true;
    let schema = make_schema_u64_i64();
    let wider = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    let src = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
    let mut appended = src.clone();
    appended.append_ranges(&src.as_mem_batch(), &[(0, 1)]);
    let mut cleared = src.clone();
    cleared.clear();
    let mut above = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
    above.append_above(make_batch(&schema, &[(3, 1, 30), (4, 1, 40)]));
    for (what, b, want) in [
        ("with_capacity", Batch::with_capacity(&schema, 4), RAW),
        ("clone", src.clone(), CONSOLIDATED),
        (
            "from_ranges",
            Batch::from_ranges(&src, &[(0, 1), (2, 3)], 0),
            CONSOLIDATED,
        ),
        ("ascending_subset", src.ascending_subset(&[0, 2]), CONSOLIDATED),
        ("negated", src.clone().negated(), CONSOLIDATED),
        ("compacted", src.compacted(), CONSOLIDATED),
        ("append_above", above, CONSOLIDATED),
        ("widened", src.widened_with_nulls(&wider, false), CONSOLIDATED),
        ("append", appended, RAW),
        ("clear", cleared, RAW),
        ("indexed_rows", src.indexed_rows(&[2, 0]), RAW),
        ("rekeyed", src.rekeyed(&schema, |s, d| d.copy_from_slice(s)), RAW),
    ] {
        assert_eq!(b.consolidated, want, "{what}");
    }
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
    drop(PooledBuf(Vec::with_capacity(512)));
    let mut roomy = Batch::with_capacity(&schema, 8);
    let arena = roomy.data.as_ptr();
    roomy.append_ranges(&src.as_mem_batch(), &[(0, 8)]);
    roomy.append_ranges(&src.as_mem_batch(), &[(8, 16)]);
    assert_eq!(roomy.data.as_ptr(), arena, "precondition: the growth stays in place");
    assert_eq!(weighted_rows(&roomy), &weighted_rows(&src)[..16]);
}

/// A `U64` key over `U8`, nullable `I64` and `U16` columns: a row is 35 bytes
/// and no payload region starts at a multiple of 8 of them.
fn ragged_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U8, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::U16, false),
        ],
        &[0],
    )
}

/// `rows` rows of [`ragged_schema`], every fifth with its `I64` NULL.
fn ragged_batch(rows: usize) -> Batch {
    let mut b = BatchBuilder::new(&ragged_schema());
    for i in 0..rows as u128 {
        b.begin_row(i, 1 + i as i64 % 3);
        b.put_int(i % 251);
        b.put_opt_int((i % 5 != 0).then_some(i * 10));
        b.put_int(i % 65_521);
        b.end_row();
    }
    b.finish()
}

/// An arena below eight rows holds exactly its rows; from eight up its capacity
/// is rounded so that every region starts 8-aligned, by as little as the
/// schema needs.
#[test]
fn an_arena_of_eight_rows_or_more_starts_every_region_aligned() {
    let schema = ragged_schema();
    assert_eq!([1, 7, 9].map(|rows| schema.arena_rows(rows)), [1, 7, 16]);
    let b = Batch::with_capacity(&schema, 9);
    assert_eq!(b.capacity, 16);
    for r in 0..schema.num_regions() {
        assert!(b.region_start(r).is_multiple_of(8), "region {r}");
    }
    assert_eq!(
        make_schema_u64_i64().arena_rows(9),
        9,
        "8-byte regions align at any count"
    );
    let key = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U32, false),
        ],
        &[0, 1],
    );
    assert_eq!(key.arena_rows(9), 10, "a 12-byte key aligns at every second row");
}

/// Rows appended three at a time under a layout whose regions move with the
/// capacity read back whole, whether a growth copies onto a fresh buffer or
/// shifts the regions within the one it has.
#[test]
fn growth_keeps_every_row_under_a_ragged_layout() {
    let schema = ragged_schema();
    let src = ragged_batch(300);
    let append_to = |dst: &mut Batch, end: usize| {
        for start in (dst.count..end).step_by(3) {
            dst.append_ranges(&src.as_mem_batch(), &[(start, start + 3)]);
        }
    };

    drain_pool();
    let mut fresh = Batch::empty_with_schema(&schema);
    append_to(&mut fresh, 300);
    assert_eq!(weighted_rows(&fresh), weighted_rows(&src));

    // Eight rows are 280 bytes, so the arena takes this 560-byte buffer, which
    // then holds sixteen.
    drain_pool();
    drop(PooledBuf(Vec::with_capacity(560)));
    let mut roomy = Batch::with_capacity(&schema, 8);
    let arena = roomy.data.as_ptr();
    append_to(&mut roomy, 15);
    assert_eq!(roomy.capacity, 16);
    assert_eq!(roomy.data.as_ptr(), arena, "precondition: the growth stays in place");
    assert_eq!(weighted_rows(&roomy), &weighted_rows(&src)[..15]);
    append_to(&mut roomy, 300);
    assert_eq!(weighted_rows(&roomy), weighted_rows(&src));
}

/// A NULL written into the open row is a zeroed cell under its bit, over an
/// arena that held other bytes.
#[test]
fn put_null_leaves_a_zeroed_cell_under_its_bit() {
    let schema = ragged_schema();
    let mut b = Batch::with_capacity(&schema, 2);
    b.data.fill(0xFF);
    for pk in [1u64, 2] {
        b.begin_row(&pk.to_be_bytes(), 1);
        b.extend_col(0, &[7]);
        b.put_null(1);
        b.extend_col(2, &9u16.to_le_bytes());
        b.commit_row();
    }
    for row in 0..2 {
        assert_eq!(b.get_null_word(row), 0b10, "row {row}");
        assert_eq!(b.get_col_ptr(row, 1, 8), &[0; 8], "row {row}");
        assert_eq!(b.get_col_ptr(row, 0, 1), &[7], "row {row}");
    }
    b.debug_verify_null_bits();
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

/// A batch whose buffers hold more than twice what its rows need comes back as
/// a tight copy with its layout; a tight one comes back as
/// the same allocation.
#[test]
fn trimmed_copies_only_oversized_batches() {
    let schema = make_schema_u64_i64();
    let src = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)]);

    let mut loose = Batch::with_capacity(&schema, 1000);
    loose.append_ranges(&src.as_mem_batch(), &[(0, 1)]);
    loose.certify_consolidated();
    let cap = loose.data.capacity();
    let trimmed = loose.trimmed();
    assert!(trimmed.data.capacity() < cap, "an oversized batch is copied down");
    assert_eq!(trimmed.count, 1);
    assert!(trimmed.consolidated);

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

    // A one-row copy's arena is that row, 1-byte columns and all.
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend((0..6).map(|_| SchemaColumn::new(TypeCode::U8, false)));
    let mut narrow = BatchBuilder::new(&SchemaDescriptor::new(&cols, &[0]));
    narrow.begin_row(0, 1);
    (0..6).for_each(|_| narrow.put_int(0));
    narrow.end_row();
    let narrow = narrow.finish().clone();
    assert_eq!(narrow.capacity, 1);
    let ptr = narrow.data.as_ptr();
    assert_eq!(narrow.trimmed().data.as_ptr(), ptr, "a one-row copy is kept");
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
        !super::super::string_heap::should_relocate_blob(small.blob.len(), small.count, 1),
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
        super::super::string_heap::should_relocate_blob(wide.blob.len(), wide.count, 1),
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
    }
}

/// `consolidate_in_place` folds a raw batch and leaves a certified one's arena
/// untouched.
#[test]
fn consolidate_in_place_folds_only_an_uncertified_batch() {
    let schema = make_schema_u64_i64();
    let mut raw = make_batch_raw(&schema, &[(2, 1, 20), (1, 1, 10), (1, 2, 10)]);
    raw.consolidate_in_place();
    assert!(raw.consolidated);
    assert_eq!(
        weighted_rows(&raw),
        weighted_rows(&make_batch(&schema, &[(1, 3, 10), (2, 1, 20)]))
    );

    let mut already = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
    let arena = already.data.as_ptr();
    already.consolidate_in_place();
    assert_eq!(already.data.as_ptr(), arena, "no refold");
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

/// Stamping a delta key on and off through `rekeyed` gives back every row whole:
/// compound key, weight, NULL word, and a long string's heap span.
#[test]
fn a_key_prefix_round_trips() {
    let view = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::String, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0, 1],
    );
    // The view's key behind an 8-byte stamp, over the same payload space.
    let stamped_schema = crate::schema::key_prefixed_schema(SchemaColumn::new(TypeCode::U64, false), &view)
        .expect("a key column to spare");
    // `(key, key, weight, string, nullable)`.
    type Row<'a> = (u64, u32, i64, &'a [u8], Option<i64>);
    let long: &[u8] = b"a-fairly-long-string-value"; // 26 bytes > 12
    let rows: [Row; 2] = [(1, 7, 1, long, Some(5)), (2, 3, -2, b"hi", None)];

    let mut b = BatchBuilder::new(&view);
    for &(k0, k1, w, s, n) in &rows {
        b.begin_row_natives(&[k0 as u128, k1 as u128], w);
        b.put_blob(s);
        b.put_opt_int(n.map(|v| v as u128));
        b.end_row();
    }
    let mut b = b.finish();
    b.certify_consolidated();

    let stamped = b.with_key_prefix(&stamped_schema, &7u64.to_be_bytes());
    assert!(stamped.consolidated);
    assert_eq!(&stamped.get_pk_bytes(1)[..8], &7u64.to_be_bytes());
    let out = stamped.without_key_prefix(&view);
    assert!(!out.consolidated);
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

/// Rows keyed by their leading key bytes keep their weights, and two that
/// differed only past the cut both remain.
#[test]
fn keyed_by_prefix_cuts_each_key_to_the_output_stride() {
    let wide = SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false); 2], &[0, 1]);
    let narrow = SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false)], &[0]);
    let rows = [(1u64, 5u64, 1i64), (1, 9, -2), (3, 0, 4)];
    let mut b = Batch::empty_with_schema(&wide);
    for &(a, c, w) in &rows {
        b.push_key_row(&[a.to_be_bytes(), c.to_be_bytes()].concat(), w);
    }

    let out = b.keyed_by_prefix(&narrow);
    assert!(!out.consolidated);
    let got: Vec<_> = (0..out.count)
        .map(|i| (out.get_pk_bytes(i).to_vec(), out.get_weight(i)))
        .collect();
    let want: Vec<_> = rows.iter().map(|&(a, _, w)| (a.to_be_bytes().to_vec(), w)).collect();
    assert_eq!(got, want);
}

/// Negate is the Z-Set group inverse: every weight flips sign and nothing else
/// moves. `i64::MIN` is its own inverse in ℤ/2⁶⁴.
#[test]
fn negate_flips_every_weight() {
    let schema = make_schema_u64_i64();
    let out = make_batch(&schema, &[(1, 3, 10), (2, -1, 20), (3, i64::MIN, 30)]).negated();
    let want = make_batch_raw(&schema, &[(1, -3, 10), (2, 1, 20), (3, i64::MIN, 30)]);
    assert_eq!(weighted_rows(&out), weighted_rows(&want));
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
    assert_eq!(out.carry_heap(&mb, mask, &[(0, 2), (3, 8)]), Some(0));
    assert_eq!(out.dead_heap, 5 + 30, "row 2's span and the source's own padding");

    let mut short = Batch::with_capacity(src.schema(), 8);
    assert_eq!(short.carry_heap(&mb, mask, &[(0, 3), (4, 8)]), Some(0));
    assert_eq!(short.dead_heap, 5, "a short row leaves no span behind");

    let mut all = Batch::with_capacity(src.schema(), 8);
    assert_eq!(all.carry_heap(&mb, mask, &[(0, 8)]), Some(0));
    assert_eq!(all.dead_heap, 5, "keeping every row charges only the padding");

    for kept in [&[][..], &[(0, 2)][..]] {
        let mut refused = Batch::with_capacity(src.schema(), 8);
        assert_eq!(refused.carry_heap(&mb, mask, kept), None, "{kept:?}");
        assert_eq!((refused.blob.len(), refused.dead_heap), (0, 0));
    }

    let mut no_string = Batch::with_capacity(src.schema(), 8);
    assert_eq!(
        no_string.carry_heap(&mb, 0, &[(0, 8)]),
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
    let mut b = BatchBuilder::new(&schema);
    for pk in 1..=4u128 {
        b.begin_row(pk, 1);
        b.put_blob(&[b'a'; 100]);
        b.put_blob(&[b'b'; 20]);
        b.end_row();
    }
    let src = b.finish();
    let mb = src.as_mem_batch();

    let mut keeps_a = Batch::with_capacity(&schema, 4);
    assert_eq!(keeps_a.carry_heap(&mb, 0b01, &[(0, 4)]), Some(0));
    assert_eq!(keeps_a.dead_heap, 4 * 20, "slot 1's span on every kept row");

    let mut keeps_b = Batch::with_capacity(&schema, 4);
    assert_eq!(keeps_b.carry_heap(&mb, 0b10, &[(0, 4)]), None, "400 of 480 bytes dead");
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
    assert!(super::super::string_heap::heap_is_wasteful(
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
    assert!(compacted.consolidated);
}

/// Retired instructions of `append_batch` over a string-bearing source: a 64-row
/// batch of 40-byte strings appended 10⁴ times into one growing destination.
/// `#[ignore]`; run release:
///   cargo test -p gnitz-zset --release append_batch_strings_bench -- --ignored --nocapture --test-threads=1
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

/// Retired instructions per row of `Batch::from_ranges` copying every other run
/// of a `U64` key over three `U64` columns, by run length. `#[ignore]`; run
/// release:
///   cargo test -p gnitz-zset --release from_ranges_run_length_bench -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn from_ranges_run_length_bench() {
    const ROWS: usize = 1 << 16;
    const PASSES: usize = 20;
    let u64_col = SchemaColumn::new(TypeCode::U64, false);
    let schema = SchemaDescriptor::new(&[u64_col; 4], &[0]);
    let mut b = BatchBuilder::new(&schema);
    for i in 0..ROWS as u128 {
        b.begin_row(i, 1);
        (1..4).for_each(|c| b.put_int(i * c));
        b.end_row();
    }
    let src = b.finish();
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    for run in [1usize, 2, 4, 16, 256] {
        let ranges: Vec<(usize, usize)> = (0..ROWS).step_by(2 * run).map(|s| (s, s + run)).collect();
        let (copied, instructions) = counter.measure(|| {
            let mut copied = 0;
            for _ in 0..PASSES {
                let out = Batch::from_ranges(std::hint::black_box(&src), std::hint::black_box(&ranges), 0);
                copied += std::hint::black_box(&out).count;
            }
            copied
        });
        assert_eq!(copied, PASSES * ROWS / 2);
        println!(
            "from_ranges_run_length_bench: run {run:>3}: {:.1} instr/row",
            instructions as f64 / copied as f64
        );
    }
}

/// Retired instructions and cycles of a relocating append session — one long
/// cell, and 100 rows of two long cells — on a thread whose last session left
/// the pool a small map, and on one whose last session relocated 50 000
/// distinct spans. `#[ignore]`; run release:
///   cargo test -p gnitz-zset --release blob_cache_session_bench -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn blob_cache_session_bench() {
    const SESSIONS: u64 = 200;
    let two_strings = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::String, false),
            SchemaColumn::new(TypeCode::String, false),
        ],
        &[0],
    );
    let build = |rows: usize| {
        let mut b = BatchBuilder::new(&two_strings);
        for i in 0..rows {
            b.begin_row(i as u128, 1);
            b.put_string(&format!("{i:040}"));
            b.put_string(&format!("{i:041}"));
            b.end_row();
        }
        b.finish()
    };
    let (one, hundred, many) = (make_string_batch(&[(1, 1, &[b'x'; 40])]), build(100), build(25_000));
    // A relocating session over every row of `src`: `compacted` carries no heap.
    let session = |src: &Batch| std::hint::black_box(std::hint::black_box(src).compacted()).count;
    let counters = [
        (
            "instr",
            gnitz_foundation::perf::Counter::instructions().expect("instructions counter"),
        ),
        (
            "cycles",
            gnitz_foundation::perf::Counter::cycles().expect("cycles counter"),
        ),
    ];
    for (shape, src) in [("one cell", &one), ("100 rows x 2 string columns", &hundred)] {
        for (unit, counter) in &counters {
            let small: u64 = (0..SESSIONS)
                .map(|_| {
                    session(src);
                    counter.measure(|| session(src)).1
                })
                .sum();
            let large: u64 = (0..SESSIONS)
                .map(|_| {
                    session(&many);
                    counter.measure(|| session(src)).1
                })
                .sum();
            println!(
                "blob_cache_session_bench: {shape}: {} {unit} after a session of its own shape, {} after a 50 000-span session",
                small / SESSIONS,
                large / SESSIONS,
            );
        }
    }
}
