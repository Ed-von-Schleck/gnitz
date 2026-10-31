use super::super::batch_pool::drain_pool;
use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{
    make_batch, make_batch_bytes, make_batch_raw, make_schema_pk_u64_payload_blob, make_schema_pk_u64_payload_string,
    make_schema_u64_i64, make_string_batch, payload0_i64, pk_only_schema, ragged_batch, ragged_schema, read_strings,
    u64_pk_schema, weighted_rows,
};
use gnitz_wire::TypeCode;

/// `append_row_from_source` copies the PK verbatim and carries the
/// caller's weight, not the source row's.
#[test]
fn append_row_from_source_carries_the_callers_weight() {
    let schema = make_schema_u64_i64();
    let src = make_batch(&schema, &[(0xDEAD_BEEF, 1, 0x4242)]);

    let mut dst = Batch::with_capacity(&schema, 1);
    dst.append_row_from_source(-1, &src, 0, None);

    assert_eq!(dst.count, 1);
    assert_eq!(dst.get_pk_bytes(0), &0xDEAD_BEEFu64.to_be_bytes());
    assert_eq!(dst.get_weight(0), -1);
    assert_eq!(payload0_i64(&dst, 0), 0x4242);
}

/// A `MemBatch`'s per-row accessors read the same bytes as its region
/// accessors, at every row and payload column of a compound-PK batch whose arena
/// has room past its rows.
#[test]
fn mem_batch_row_accessors_match_its_regions() {
    use gnitz_expr::BatchView;
    use gnitz_wire::RowSource;
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::U8, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::String, true),
        ],
        &[2, 0],
    );
    let mut b = BatchBuilder::new(&schema);
    for i in 0..3u128 {
        b.begin_row_natives(&[1000 + i, (i as i32 - 1) as u128], 1 + i as i64);
        b.put_int(i + 7);
        b.put_opt_int((i != 1).then_some((-(i as i64) * 100) as u128));
        match i {
            0 => b.put_null(),
            _ => b.put_string(&"a long string the heap holds".repeat(i as usize)),
        }
        b.end_row();
    }
    let batch = b.finish();
    let mb = batch.as_mem_batch();
    assert!(mb.cap > mb.count, "the arena has room past the rows");
    let (pk, stride) = BatchView::pk_region(&mb);
    assert_eq!(stride, schema.pk_stride());
    for row in 0..mb.count {
        assert_eq!(RowSource::get_pk_bytes(&mb, row), &pk[row * stride..(row + 1) * stride]);
        assert_eq!(
            RowSource::get_null_word(&mb, row),
            gnitz_wire::read_u64_le(BatchView::null_bmp(&mb), row * 8)
        );
        for (pi, col) in schema.payload_columns() {
            let sz = col.size();
            assert_eq!(
                RowSource::get_col_ptr(&mb, row, pi, sz),
                &BatchView::col_data(&mb, pi, sz)[row * sz..(row + 1) * sz],
                "row {row}, payload column {pi}"
            );
        }
    }
}

// Raising the claim verifies it: an adjacent-equal (PK, payload) duplicate
// trips the verifier.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "not strictly")]
fn certify_consolidated_panics_on_a_duplicate() {
    make_batch_raw(&make_schema_u64_i64(), &[(1, 1, 5), (1, 1, 5)]).certify_consolidated();
}

// Ghost clause: strictly ordered, but carrying a net-zero row.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "ghost not eliminated")]
fn certify_consolidated_panics_on_a_ghost() {
    make_batch_raw(&make_schema_u64_i64(), &[(1, 1, 0), (2, 0, 0), (3, 1, 0)]).certify_consolidated();
}

// A weight map that makes a ghost falsifies the claim it keeps.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "mapped to zero")]
fn map_weights_panics_on_a_ghost_under_the_claim() {
    make_batch(&make_schema_u64_i64(), &[(1, 1, 5), (2, 1, 6)]).map_weights(|_| 0);
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
    let wider = u64_pk_schema(&[
        SchemaColumn::new(TypeCode::I64, false),
        SchemaColumn::new(TypeCode::I64, true),
    ]);
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
        ("keyed_by_prefix", src.keyed_by_prefix(&schema), RAW),
    ] {
        assert_eq!(b.consolidated, want, "{what}");
    }
}

/// Rows appended whole or three at a time under a layout whose regions move
/// with the capacity read back whole, whether a growth copies onto a fresh
/// buffer or shifts the regions within the one it has.
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
    let mut whole = Batch::empty_with_schema(&schema);
    whole.append_batch(&src);
    assert_eq!(weighted_rows(&whole), weighted_rows(&src));
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

/// `key_retractions` is one `-1` row per key, every payload cell a non-NULL
/// zero, over a recycled buffer and under NOT NULL and string columns.
#[test]
fn key_retractions_are_minus_one_rows_of_zeroed_non_null_cells() {
    let schema = u64_pk_schema(&[
        SchemaColumn::new(TypeCode::String, false),
        SchemaColumn::new(TypeCode::I64, true),
        SchemaColumn::new(TypeCode::U16, false),
    ]);
    // Leave a dirty buffer of the same size in the pool.
    let mut dirty = Batch::with_capacity(&schema, 3);
    dirty.data.fill(0xFF);
    drop(dirty);

    let pks: Vec<u8> = [7u64, 3, 9].iter().flat_map(|pk| pk.to_be_bytes()).collect();
    let b = Batch::key_retractions(&schema, &pks);
    assert_eq!(b.len(), 3);
    assert_eq!(b.pk(), &pks[..]);
    for row in 0..3 {
        assert_eq!(b.get_weight(row), -1, "row {row}");
        assert_eq!(b.get_null_word(row), 0, "row {row}");
        assert_eq!(b.get_col_ptr(row, 0, 16), &[0; 16], "row {row}");
        assert_eq!(b.get_col_ptr(row, 1, 8), &[0; 8], "row {row}");
        assert_eq!(b.get_col_ptr(row, 2, 2), &[0; 2], "row {row}");
    }
    b.debug_verify_null_bits();
    assert!(Batch::key_retractions(&schema, &[]).is_empty());
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

/// A gathered subset's long values read back under both heap arms: kept whole
/// with the left-out row's bytes charged dead when most rows stay, and relocated
/// to the survivor's span alone when one of a hundred does.
#[test]
fn from_ranges_resolves_long_values_under_both_heap_arms() {
    let vals: Vec<Vec<u8>> = (0..100u64).map(|i| vec![b'a' + (i % 26) as u8; 28]).collect();
    let src = |n: usize| {
        let rows: Vec<(u64, i64, &[u8])> = vals[..n]
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64, 1, &v[..]))
            .collect();
        make_batch_bytes(&make_schema_pk_u64_payload_blob(), &rows)
    };
    let carried = Batch::from_ranges(&src(8), &[(0, 7)], 0);
    assert_eq!(read_strings(&carried), vals[..7]);
    assert_eq!(
        (carried.blob.len(), carried.dead_heap),
        (8 * 28, 28),
        "the heap is carried whole"
    );

    let relocated = Batch::from_ranges(&src(100), &[(7, 8)], 0);
    assert_eq!(read_strings(&relocated), vals[7..8]);
    assert_eq!(
        (relocated.blob.len(), relocated.dead_heap),
        (28, 0),
        "only the survivor's span"
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
        assert_eq!(gnitz_wire::widen_pk_be(out.get_pk_bytes(0)), 1);
        assert!(!out.blob.is_empty(), "output blob must be propagated ({nulls_first})");
        assert_eq!(
            gnitz_wire::payload_bytes(&out, 0, str_slot),
            long,
            "long string must resolve to the original ({nulls_first})"
        );
        let nw = out.get_null_word(0);
        assert!(gnitz_wire::null_word_get(nw, fill_slot), "the fill column reads NULL");
        assert!(!gnitz_wire::null_word_get(nw, str_slot), "the input column stays live");
    }
}

/// A batch whose rows stand consolidated without the claim is certified where it
/// is, by all three entry points, and a certified one is not folded again.
#[test]
fn consolidation_certifies_rows_already_in_order() {
    let schema = make_schema_u64_i64();
    let ordered: &[(u64, i64, i64)] = &[(1, 1, 10), (1, -1, 11), (2, 1, 5)];

    let mut in_place = make_batch_raw(&schema, ordered);
    let arena = in_place.data.as_ptr();
    in_place.consolidate_in_place();
    assert!(in_place.consolidated);
    assert_eq!(in_place.data.as_ptr(), arena, "no refold");

    let owned = make_batch_raw(&schema, ordered);
    let arena = owned.data.as_ptr();
    let owned = owned.into_consolidated();
    assert!(owned.consolidated);
    assert_eq!(owned.data.as_ptr(), arena, "returned by move");

    let copy = make_batch_raw(&schema, ordered).to_consolidated();
    assert!(copy.consolidated);
    assert_eq!(weighted_rows(&copy), weighted_rows(&in_place));

    let mut already = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
    let arena = already.data.as_ptr();
    already.consolidate_in_place();
    assert_eq!(already.data.as_ptr(), arena, "a certified batch is not refolded");
}

/// Rows keyed by their leading key bytes keep their weights, and two that
/// differed only past the cut both remain.
#[test]
fn keyed_by_prefix_cuts_each_key_to_the_output_stride() {
    let (wide, narrow) = (pk_only_schema(&[TypeCode::U64; 2]), pk_only_schema(&[TypeCode::U64]));
    let rows = [(1u64, 5u64, 1i64), (1, 9, -2), (3, 0, 4)];
    let mut b = Batch::empty_with_schema(&wide);
    for &(a, c, w) in &rows {
        b.push_key_row(&[a.to_be_bytes(), c.to_be_bytes()].concat(), w);
    }

    let out = b.keyed_by_prefix(&narrow);
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

/// Carrying a heap for a subset charges exactly the long bytes of the rows left
/// out, on top of what the source already held dead; a short cell costs nothing.
/// A carry that would leave the heap wasteful, or that keeps nothing, is refused.
#[test]
fn carry_heap_charges_exactly_the_excluded_rows() {
    let mut rows: Vec<(u64, i64, &[u8])> = (1..=8).map(|pk| (pk, 1, &[b'x'; 40][..])).collect();
    rows[2].2 = &[b'c'; 30];
    rows[3].2 = b"short";
    let src = make_string_batch(&rows).padded(5);
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
    let schema = u64_pk_schema(&[
        SchemaColumn::new(TypeCode::String, false),
        SchemaColumn::new(TypeCode::String, false),
    ]);
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
    let a = make_string_batch(&[(1, 1, &[b'a'; 20]), (2, 1, b"tiny")]).padded(3);
    let b = make_string_batch(&[(3, 1, &[b'b'; 30]), (4, 1, &[b'c'; 25])]).padded(4);
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
    let wasteful = make_string_batch(&[(2, 1, &[b'b'; 20])]).padded(100);
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
    let kept = make_string_batch(&rows).padded(20).trimmed();
    assert_eq!((kept.blob.len(), kept.dead_heap), (80, 20), "a quarter dead is carried");

    let mut overcounted = make_string_batch(&rows);
    overcounted.dead_heap = 60;
    let measured = overcounted.trimmed();
    assert_eq!(
        (measured.blob.len(), measured.dead_heap),
        (60, 0),
        "an overcount is measured down"
    );

    let compacted = make_string_batch(&rows).padded(21).trimmed();
    assert_eq!((compacted.blob.len(), compacted.dead_heap), (60, 0));
    assert_eq!(read_strings(&compacted), [vec![b'a'; 20], vec![b'b'; 40]]);
    assert!(compacted.consolidated);
}

/// A batch cut to its rows holds every row as it was — keys, weights, NULL
/// words, short and long strings — in buffers of exactly their size, whatever
/// room it was built with.
#[test]
fn a_shrunk_batch_holds_its_rows_in_no_more_than_they_take() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::String, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0, 1],
    );
    for rows in [1usize, 7, 8, 9, 100, 1000] {
        let mut b = BatchBuilder::new(&schema);
        for i in 0..rows {
            b.begin_row_natives(&[i as u128, 3], 1 + i as i64 % 3);
            match i % 2 {
                0 => b.put_blob(format!("a-fairly-long-string-value-{i}").as_bytes()),
                _ => b.put_blob(b"hi"),
            }
            b.put_opt_int((i % 5 != 0).then_some(i as u128));
            b.end_row();
        }
        let mut b = b.finish();
        b.certify_consolidated();
        let before = b.clone();
        let roomy = b.allocated_bytes();
        let out = b.shrunk();
        assert!(out.consolidated);
        assert_eq!(
            out.allocated_bytes(),
            schema.arena_rows(rows) * schema.row_width() + out.blob.len(),
            "{rows} rows, built with {roomy} bytes"
        );
        assert_eq!(weighted_rows(&out), weighted_rows(&before), "{rows} rows");
    }
}

/// A key set names the rows keyed by one of its keys: every row of a key
/// several rows share, listed keys side by side as one run, and nothing for a
/// key between two rows or past the last.
#[test]
fn key_ranges_are_the_rows_a_key_set_lists() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0, 1],
    );
    let key = |k: usize| [(k as u64 / 10).to_be_bytes(), (k as u64 % 10).to_be_bytes()].concat();
    // Key 155 is between two rows; 250 and 999 are past the last of 100.
    let listed = [3usize, 7, 13, 14, 15, 40, 99, 155, 250, 999];
    let keys: Vec<Vec<u8>> = listed.iter().map(|&k| key(k)).collect();
    for n in [100usize, 0, 10, 1] {
        // Key `k` holds `1 + k % 3` rows, apart in their payload.
        let mut b = BatchBuilder::new(&schema);
        let (mut want, mut row) = (Vec::new(), 0);
        for k in 0..n {
            for copy in 0..=k % 3 {
                if listed.contains(&k) {
                    want.push(row);
                }
                row += 1;
                b.begin_row_natives(&[k as u128 / 10, k as u128 % 10], 1);
                b.put_int(copy as u128);
                b.end_row();
            }
        }
        let mut b = b.finish();
        b.certify_consolidated();
        let mut ranges = vec![(7, 9)];
        b.key_ranges(keys.iter().map(|k| &k[..]), &mut ranges);
        assert!(ranges.windows(2).all(|w| w[0].1 < w[1].0), "maximal runs: {ranges:?}");
        let got: Vec<usize> = ranges.iter().flat_map(|&(start, end)| start..end).collect();
        assert_eq!(got, want, "n={n}");
    }
}

#[test]
fn holds_element_of_matches_pk_and_payload() {
    let schema = make_schema_u64_i64();
    let mut rows: Vec<(u64, i64, i64)> = (0..50).map(|v| (7, 1, 2 * v)).collect();
    rows.extend([(3, 1, 10), (9, -2, 4)]);
    let held = make_batch_raw(&schema, &rows).into_consolidated();
    let probes = make_batch_raw(
        &schema,
        &[(3, -1, 10), (3, 1, 11), (7, -1, 48), (7, -1, 49), (8, 1, 0), (9, 1, 4)],
    )
    .into_consolidated();
    let got: Vec<(u128, i64, bool)> = (0..probes.len())
        .map(|i| {
            (
                gnitz_wire::widen_pk_be(probes.get_pk_bytes(i)),
                payload0_i64(&probes, i),
                held.holds_element_of(&probes, i),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (3, 10, true),
            (3, 11, false),
            (7, 48, true),
            (7, 49, false),
            (8, 0, false),
            (9, 4, true)
        ]
    );
}

/// The maximal runs of `0..n` whose position `keep` admits, one position at a time.
fn naive_runs(n: usize, keep: impl Fn(usize) -> bool) -> Vec<(usize, usize)> {
    let mut runs: Vec<(usize, usize)> = Vec::new();
    for i in (0..n).filter(|&i| keep(i)) {
        match runs.last_mut() {
            Some((_, e)) if *e == i => *e += 1,
            _ => runs.push((i, i + 1)),
        }
    }
    runs
}

#[test]
fn runs_where_zip_matches_a_per_position_scan_across_word_boundaries() {
    // Lengths around one and two 64-position words, and a short tail.
    for n in [0, 1, 63, 64, 65, 127, 128, 129, 200] {
        let a: Vec<u64> = (0..n as u64)
            .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 61)
            .collect();
        let b: Vec<u64> = (0..n as u64).map(|i| (i * 7 + 3) % 5).collect();
        let keep = |x: &u64, y: &u64| *x != 0 && *y != 0;
        assert_eq!(
            runs_where_zip(&a, &b, keep),
            naive_runs(n, |i| keep(&a[i], &b[i])),
            "n = {n}"
        );
        assert_eq!(runs_where(&a, |x| *x != 0), naive_runs(n, |i| a[i] != 0), "n = {n}");
        assert_eq!(runs_where(&a, |_| true), naive_runs(n, |_| true), "n = {n}");
    }
}
