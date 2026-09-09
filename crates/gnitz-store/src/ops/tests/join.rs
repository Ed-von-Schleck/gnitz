use std::collections::HashMap;

use proptest::prelude::*;

use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, Layout};
use crate::test_support::{
    make_batch, make_batch_i64pk as make_signed_batch, make_batch_opk, make_schema_i64pk_i64 as make_schema_signed,
    make_schema_u64_i64, opk_pk, pk_only_schema, pk_payload_schema, row_key, trace_cursor, zset_of, RowKey,
};
use gnitz_wire::read_i64_le;

const RELS: [RangeRel; 4] = [RangeRel::Lt, RangeRel::Le, RangeRel::Gt, RangeRel::Ge];

/// The join output schema `reg_meta` carries and `exec.rs` hands the op — the
/// operator's own builder, not a look-alike, so a change to the layout reaches
/// these tests instead of silently passing against a stale copy.
fn join_out_schema(left: &SchemaDescriptor, right: &SchemaDescriptor) -> SchemaDescriptor {
    merge_schemas_for_join(left, right).expect("test join schema exceeds MAX_COLUMNS")
}

#[test]
fn test_merge_schemas_for_join() {
    let left = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let right = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    );
    let joined = merge_schemas_for_join(&left, &right).unwrap();
    assert_eq!(joined.num_columns(), 3); // PK + left_I64 + right_STRING
    assert_eq!(joined.columns[0].type_code, type_code::U128);
    assert_eq!(joined.columns[1].type_code, type_code::I64);
    assert_eq!(joined.columns[2].type_code, type_code::STRING);
}

#[test]
fn test_merge_schemas_for_join_compound_pk() {
    // Compound-PK left: 4 columns [U64, U64, U64, U64], PK = (col1, col2).
    let left = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[1, 2],
    );
    let right = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let joined = merge_schemas_for_join(&left, &right).unwrap();
    // Two PK columns up front, then left payload (2), then right payload (1) = 5.
    assert_eq!(joined.num_columns(), 5);
    assert_eq!(joined.pk_indices(), &[0, 1]);
    assert_eq!(joined.columns[0].type_code, type_code::U64);
    assert_eq!(joined.columns[1].type_code, type_code::U64);

    // Single-PK left collapses back to pk_indices = [0].
    let left_single = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let joined_single = merge_schemas_for_join(&left_single, &right).unwrap();
    assert_eq!(joined_single.pk_indices(), &[0]);
}

#[test]
fn test_merge_schemas_for_join_column_overflow() {
    // A merged column count over MAX_COLUMNS returns None (compile rejected),
    // rather than aborting.
    use crate::schema::MAX_COLUMNS;
    let half = MAX_COLUMNS / 2 + 2;
    let make = |n: usize| {
        let mut cols = [SchemaColumn::EMPTY; MAX_COLUMNS];
        cols[0] = SchemaColumn::new(type_code::U128, 0);
        for col in cols.iter_mut().take(n).skip(1) {
            *col = SchemaColumn::new(type_code::I64, 0);
        }
        SchemaDescriptor::new(&cols[..n], &[0])
    };
    assert!(
        merge_schemas_for_join(&make(half), &make(half)).is_none(),
        "an over-wide join output must be rejected (None), not aborted"
    );
}

/// The equi join over one schema on both sides.
fn equi_join(schema: &SchemaDescriptor, delta: &Batch, cursor: &mut ReadCursor) -> Batch {
    op_join_delta_trace(
        delta,
        cursor,
        schema,
        schema,
        &join_out_schema(schema, schema),
        JoinProbe::Equi,
    )
}

/// The range join over one schema on both sides, with the probe the compiler
/// would bake for `(n_eq, rel)`.
fn range_join(schema: &SchemaDescriptor, n_eq: usize, rel: RangeRel, delta: &Batch, cursor: &mut ReadCursor) -> Batch {
    op_join_delta_trace(
        delta,
        cursor,
        schema,
        schema,
        &join_out_schema(schema, schema),
        JoinProbe::Range(RangeProbe::new(schema, schema, n_eq as u8, rel).expect("fixture probe is well-formed")),
    )
}

/// The cross join of `delta` (under `left`) against the cursor's trace (under
/// `right`).
fn cross_join(left: &SchemaDescriptor, right: &SchemaDescriptor, delta: &Batch, cursor: &mut ReadCursor) -> Batch {
    op_join_delta_trace(
        delta,
        cursor,
        left,
        right,
        &join_out_schema(left, right),
        JoinProbe::Cross,
    )
}

// -----------------------------------------------------------------------
// Equi delta-trace
// -----------------------------------------------------------------------

/// The equi join at every PK stride: the delta rows at a key the trace holds
/// emit the full cartesian product against that key's trace group, at
/// `w_delta × w_trace` and in trace-major order; a key the trace does not hold
/// emits nothing. The absent key sits directly after the held one in the delta
/// and, for the wide shape, shares its leading 16 OPK bytes — so a delta group
/// that compared anything less than the whole key would fuse the two and
/// product the extra row against the trace group.
#[test]
fn equi_join_products_the_trace_group_at_every_pk_shape() {
    let shapes: [(&str, SchemaDescriptor, &[u128], &[u128]); 3] = [
        ("i32", pk_payload_schema(&[type_code::I32]), &[-7i64 as u128], &[1]),
        ("u64", pk_payload_schema(&[type_code::U64]), &[1], &[2]),
        (
            "3xu64",
            pk_payload_schema(&[type_code::U64; 3]),
            &[1, 1, 2],
            &[1, 1, 1 << 56],
        ),
    ];
    for (name, schema, held, absent) in shapes {
        let (held, absent) = (opk_pk(&schema, held), opk_pk(&schema, absent));

        let mut trace = make_batch_opk(&schema, &[(&held, 1, 100), (&held, 2, 200)]);
        trace.certify_layout(Layout::Consolidated);
        let mut ch = trace_cursor(trace, schema);

        let mut delta = make_batch_opk(
            &schema,
            &[(&held, 1, 10), (&held, 1, 20), (&held, 1, 30), (&absent, 1, 40)],
        );
        delta.certify_layout(Layout::Consolidated);

        let out = equi_join(&schema, &delta, &mut ch);
        // (left payload, right payload, weight) — trace-major: each trace row is
        // walked once and producted against the whole delta group. `absent`
        // contributes nothing.
        let got = out_triples(&out);
        let want: Vec<(i64, i64, i64)> = [(100i64, 1i64), (200, 2)]
            .into_iter()
            .flat_map(|(right, w)| [10i64, 20, 30].map(move |left| (left, right, w)))
            .collect();
        assert_eq!(got, want, "{name}");
        for r in 0..out.count {
            assert_eq!(out.get_pk_bytes(r), &held[..], "{name}: the output PK is the left PK");
        }
        // The emission is trace-major, so neither (PK, payload)-sorted nor
        // folded; the output carries no layout claim and downstream re-sorts.
        assert_eq!(out.layout(), Layout::Raw, "{name}");
    }
}

// -----------------------------------------------------------------------
// Cross delta-trace
// -----------------------------------------------------------------------

/// The cross join pairs every delta row with every trace row at `w_delta × w_trace`,
/// trace-major, keyed by the left PK. The sides differ in PK width here: neither
/// key is read. The walk rewinds, so an exhausted cursor still yields the product.
#[test]
fn cross_join_products_every_delta_row_with_every_trace_row() {
    let left = pk_payload_schema(&[type_code::U64]);
    let right = pk_payload_schema(&[type_code::U64; 3]);
    let (l1, l2) = (opk_pk(&left, &[1]), opk_pk(&left, &[2]));
    let (r1, r2, r3) = (
        opk_pk(&right, &[1, 1, 1]),
        opk_pk(&right, &[1, 1, 2]),
        opk_pk(&right, &[9, 0, 0]),
    );

    let mut trace = make_batch_opk(&right, &[(&r1, 1, 100), (&r2, 2, 200), (&r3, 1, 300)]);
    trace.certify_layout(Layout::Consolidated);
    let mut ch = trace_cursor(trace, right);
    // The probe states its own start, so an exhausted cursor still yields the
    // whole product.
    ch.advance_to(&opk_pk(&right, &[u64::MAX as u128, 0, 0]));
    assert!(!ch.valid, "the fixture must start with an exhausted cursor");

    let mut delta = make_batch_opk(&left, &[(&l1, 3, 10), (&l2, -1, 20)]);
    delta.certify_layout(Layout::Consolidated);

    let out = cross_join(&left, &right, &delta, &mut ch);
    let got = out_triples(&out);
    let want: Vec<(i64, i64, i64)> = [(100i64, 1i64), (200, 2), (300, 1)]
        .into_iter()
        .flat_map(|(right_v, w)| [(10i64, 3i64), (20, -1)].map(move |(left_v, wd)| (left_v, right_v, wd * w)))
        .collect();
    assert_eq!(got, want);
    let left_pks: Vec<&[u8]> = (0..out.count).map(|r| out.get_pk_bytes(r)).collect();
    assert_eq!(left_pks, [&l1[..], &l2[..]].repeat(3), "the output PK is the left PK");
    assert_eq!(out.layout(), Layout::Raw, "trace-major: not even PK-sorted");

    // An empty trace pairs with nothing; an empty delta emits nothing.
    let mut empty = trace_cursor(Batch::empty_with_schema(&right), right);
    assert_eq!(cross_join(&left, &right, &delta, &mut empty).count, 0);
    let none = Batch::empty_with_schema(&left);
    assert_eq!(cross_join(&left, &right, &none, &mut ch).count, 0);
}

// -----------------------------------------------------------------------
// Range delta-trace: literal output
// -----------------------------------------------------------------------

/// All four rels over an unsigned key, `n_eq = 0`. The boundary case `x == y`
/// must match `Le`/`Ge` and not `Lt`/`Gt`. The trace payload tags the rows
/// {y=10→110, y=20→120, y=30→130}; the delta probes x = 20.
#[test]
fn range_join_cuts_the_span_each_rel_names() {
    let schema = make_schema_u64_i64();
    for (rel, want) in [
        (RangeRel::Lt, vec![110]),      // y < 20
        (RangeRel::Le, vec![110, 120]), // y <= 20
        (RangeRel::Gt, vec![130]),      // y > 20
        (RangeRel::Ge, vec![120, 130]), // y >= 20
    ] {
        let mut ch = trace_cursor(make_batch(&schema, &[(10, 1, 110), (20, 1, 120), (30, 1, 130)]), schema);
        let delta = make_batch(&schema, &[(20, 1, 200)]);
        let out = range_join(&schema, 0, rel, &delta, &mut ch);

        let got: Vec<i64> = range_out_pairs(&out).into_iter().map(|(p, _)| p).collect();
        assert_eq!(got, want, "rel {rel:?}");
        // The left PK (the delta range key x) and left payload survive on every row.
        for r in 0..out.count {
            assert_eq!(out.get_pk(r) as u64, 20);
            assert_eq!(read_i64_le(out.col_data(0), r * 8), 200);
        }
    }
}

/// `n_eq = 0`, `rel = Lt`: two delta rows form one equality group spanning the
/// whole trace, and the monotone suffix pointer must widen the emitted run as
/// the trace slot ascends. Pinned as literal output.
#[test]
fn range_join_suffix_pointer_widens_across_the_delta_group() {
    let schema = make_schema_u64_i64();
    let mut ch = trace_cursor(make_batch(&schema, &[(5, 1, 105), (15, 1, 115), (25, 1, 125)]), schema);
    let delta = make_batch(&schema, &[(10, 1, 100), (30, 1, 300)]);
    let out = range_join(&schema, 0, RangeRel::Lt, &delta, &mut ch);

    // x=10 → {y<10}={105}; x=30 → {y<30}={105,115,125}.
    let mut pairs: Vec<(u64, i64)> = (0..out.count)
        .map(|r| (out.get_pk(r) as u64, read_i64_le(out.col_data(1), r * 8)))
        .collect();
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(10, 105), (30, 105), (30, 115), (30, 125)]);
}

/// A signed range pair (both sides reindex to I64). Negative trace keys must
/// order below positives, which they do only because the probe reads the OPK
/// image — raw bytes would put -100 above +50.
#[test]
fn range_join_orders_a_signed_key_by_its_opk_image() {
    let schema = make_schema_signed();
    let trace_rows = [(-100i64, 1i64, 1i64), (0, 1, 2), (50, 1, 3)];
    let delta = make_signed_batch(&schema, &[(0, 1, 9)]);
    for (rel, want) in [(RangeRel::Gt, vec![3]), (RangeRel::Lt, vec![1])] {
        let mut ch = trace_cursor(make_signed_batch(&schema, &trace_rows), schema);
        let out = range_join(&schema, 0, rel, &delta, &mut ch);
        let got: Vec<i64> = range_out_pairs(&out).into_iter().map(|(p, _)| p).collect();
        assert_eq!(got, want, "rel {rel:?}");
    }
}

/// The range op against a *used* trace cursor: `bind_trace_cursors` binds one
/// per trace register, so two ops on one trace share it for the whole epoch. A
/// parked and an exhausted cursor must both produce the fresh-cursor output —
/// the group skip reads the cursor position.
#[test]
fn range_join_reuses_a_stale_trace_cursor() {
    let schema = make_range_schema(1, false);
    let out_schema = join_out_schema(&schema, &schema);
    let delta = make_range_batch(&schema, &[(vec![1], 5, 1, 1), (vec![3], 5, 1, 3)]);
    let trace_rows = [
        (vec![1u64], 0u64, 1i64, 10i64),
        (vec![1], 9, 1, 19),
        (vec![3], 0, 1, 30),
        (vec![3], 9, 1, 39),
    ];
    for rel in RELS {
        let mut fresh_ch = trace_cursor(make_range_batch(&schema, &trace_rows), schema);
        let want = range_join(&schema, 1, rel, &delta, &mut fresh_ch);

        for park_past_end in [false, true] {
            let mut ch = trace_cursor(make_range_batch(&schema, &trace_rows), schema);
            ch.advance_to(&opk_pk(&schema, &[3, 9]));
            if park_past_end {
                ch.advance();
                assert!(!ch.valid);
            }
            let got = range_join(&schema, 1, rel, &delta, &mut ch);
            assert_eq!(got.count, want.count, "rel {rel:?} past_end={park_past_end}");
            assert_eq!(
                zset_of(&got, &out_schema),
                zset_of(&want, &out_schema),
                "rel {rel:?} past_end={park_past_end}",
            );
        }
    }
}

// -----------------------------------------------------------------------
// Range delta-trace versus a brute-force reference
// -----------------------------------------------------------------------

/// A range-join fixture: the eq-column arity, the delta and trace rows, and the
/// least number of output rows the four rels together must produce (which keeps
/// a shape from passing vacuously).
type RangeCase = (&'static str, usize, RangeRows, RangeRows, usize);
type RangeRows = &'static [(&'static [u64], u64, i64, i64)];

/// The shapes worth pinning deterministically, each against the brute-force
/// reference for all four rels.
#[test]
fn range_join_fixtures_match_the_reference() {
    let cases: &[RangeCase] = &[
        // A maximal slot: `Gt` of `u64::MAX` is a provably-empty cut, and the
        // trace *contains* `u64::MAX`, so a walk that failed to skip would emit.
        (
            "maximal slot, no eq prefix",
            0,
            &[(&[], u64::MAX, 1, 9)],
            &[(&[], 0, 1, 100), (&[], 50, 1, 150), (&[], u64::MAX, 1, 199)],
            3,
        ),
        // The same inside a non-maximal eq group: no match, and no spill into
        // the k=2 group.
        (
            "maximal slot inside an eq group",
            1,
            &[(&[1], u64::MAX, 1, 9)],
            &[(&[1], 0, 1, 100), (&[1], 50, 1, 150), (&[2], 0, 1, 200)],
            3,
        ),
        // `Le` over a maximal slot: the end cut's carry ripples into the eq
        // prefix, landing on the next group's first key.
        (
            "maximal slot present in the trace group",
            1,
            &[(&[1], u64::MAX, 1, 9)],
            &[(&[1], 0, 1, 100), (&[1], u64::MAX, 1, 199), (&[2], 0, 1, 200)],
            3,
        ),
        // A trace row in the next eq group with an in-range slot must not match.
        (
            "eq prefix stops at the group edge",
            1,
            &[(&[1], 15, 1, 200)],
            &[(&[1], 10, 1, 110), (&[1], 20, 1, 120), (&[2], 5, 1, 205)],
            3,
        ),
        // A weight-0 row advances the monotone pointer but emits nothing; a
        // negative-weight row emits at its product's sign.
        (
            "tombstone and retraction in one trace group",
            0,
            &[(&[], 40, -1, 400)],
            &[
                (&[], 10, 1, 110),
                (&[], 20, 0, 120),
                (&[], 25, -1, 125),
                (&[], 30, 1, 130),
            ],
            6,
        ),
        // Duplicate `[eq‖d]` on both sides with distinct payloads: the full
        // cross-product per trace row, weights multiplied.
        (
            "multiset delta against a multi-payload trace",
            0,
            &[(&[], 15, 1, 1), (&[], 15, 3, 2)],
            &[(&[], 10, 1, 101), (&[], 10, 2, 102), (&[], 20, 1, 200)],
            8,
        ),
        // A trace eq group with no delta group, and a delta group with no trace
        // group — the shape the walk's trailing group skip elides.
        (
            "non-matching eq groups on each side",
            1,
            &[(&[2], 7, 1, 2), (&[3], 7, 1, 3)],
            &[(&[1], 5, 1, 15), (&[3], 5, 1, 35), (&[3], 9, 1, 39)],
            4,
        ),
        // Output ≫ |trace|: every delta row matches most of its trace group, so
        // the monotone pointer runs the full width of the group both ways.
        (
            "high fan-out",
            0,
            &[(&[], 3, 1, 1), (&[], 4, 1, 2), (&[], 5, 1, 3)],
            &[
                (&[], 0, 1, 100),
                (&[], 1, 1, 101),
                (&[], 2, 1, 102),
                (&[], 3, 1, 103),
                (&[], 4, 1, 104),
                (&[], 5, 1, 105),
                (&[], 6, 1, 106),
                (&[], 7, 1, 107),
            ],
            40,
        ),
        // A narrow covered span inside a large trace: `Gt`/`Ge` seek past the
        // dead low head, `Lt`/`Le` stop before the dead high tail.
        (
            "narrow span over a large trace",
            0,
            &[(&[], 25, 1, 1), (&[], 26, 1, 2)],
            LARGE_TRACE,
            50,
        ),
        ("empty delta", 0, &[], &[(&[], 10, 1, 110)], 0),
        ("empty trace", 0, &[(&[], 10, 1, 1), (&[], 20, 1, 2)], &[], 0),
    ];

    for &(name, n_eq, delta_rows, trace_rows, min_rows) in cases {
        let schema = make_range_schema(n_eq, false);
        let delta = make_range_batch(&schema, &owned(delta_rows));
        let mut total = 0;
        for rel in RELS {
            let trace = make_range_batch(&schema, &owned(trace_rows));
            total += assert_matches_reference(schema, n_eq, rel, &delta, trace, name);
        }
        assert!(
            total >= min_rows,
            "{name}: emitted {total} rows, expected at least {min_rows}"
        );
    }
}

/// A 30-row trace, the size at which the seek must skip a dead head or tail
/// rather than walk it.
const LARGE_TRACE: RangeRows = &[
    (&[], 0, 1, 100),
    (&[], 1, 1, 101),
    (&[], 2, 1, 102),
    (&[], 3, 1, 103),
    (&[], 4, 1, 104),
    (&[], 5, 1, 105),
    (&[], 10, 1, 110),
    (&[], 15, 1, 115),
    (&[], 20, 1, 120),
    (&[], 24, 1, 124),
    (&[], 25, 1, 125),
    (&[], 26, 1, 126),
    (&[], 27, 1, 127),
    (&[], 28, 1, 128),
    (&[], 29, 1, 129),
];

/// The literal fixture rows in the owned shape [`make_range_batch`] takes.
fn owned(rows: RangeRows) -> Vec<(Vec<u64>, u64, i64, i64)> {
    rows.iter().map(|&(eq, d, w, v)| (eq.to_vec(), d, w, v)).collect()
}

/// Random `(eq.., range, weight, payload)` rows over a tiny key space, returned
/// in `(eq.., range, payload)` order — the full (PK, payload) order the range
/// join's walk reads them in. Weights span `{-2..=2}` so a trace
/// carries tombstones and a delta retractions; the four-value key space forces
/// dense eq groups, boundary equality (`d == s`) and non-matching groups, and
/// the occasional `u64::MAX` slot reaches the maximal cuts.
fn arb_range_rows(n_eq: usize) -> impl Strategy<Value = Vec<(Vec<u64>, u64, i64, i64)>> {
    let slot = prop_oneof![9 => 0u64..4, 1 => Just(u64::MAX)];
    let row = (prop::collection::vec(0u64..4, n_eq), slot, -2i64..=2i64, -2i64..2i64);
    prop::collection::vec(row, 0..8).prop_map(|mut rows| {
        rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.3.cmp(&b.3)));
        rows
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// The range join matches the brute-force reference over random delta and
    /// trace batches, for all four rels, `n_eq ∈ {0, 1, 2}`, and both payload
    /// shapes — `wide` adds the NULL and STRING columns a fixed-int fixture
    /// cannot exercise. Sparse eq groups at `n_eq ∈ {1, 2}` are what drive the
    /// walk's trailing group skip.
    #[test]
    fn range_dt_matches_reference(
        (n_eq, wide, rel_i, delta_rows, trace_rows) in (0usize..3, any::<bool>(), 0usize..4).prop_flat_map(
            |(n_eq, wide, rel_i)| {
                (Just(n_eq), Just(wide), Just(rel_i), arb_range_rows(n_eq), arb_range_rows(n_eq))
            },
        ),
    ) {
        let schema = make_range_schema(n_eq, wide);
        let delta = make_range_batch(&schema, &delta_rows);
        let trace = make_range_batch(&schema, &trace_rows);
        assert_matches_reference(schema, n_eq, RELS[rel_i], &delta, trace, "proptest");
    }
}

// -----------------------------------------------------------------------
// Range fixtures and the reference
// -----------------------------------------------------------------------

/// Payload strings for the `wide` fixtures: an empty one, one short enough to
/// stay inline, and two past `SHORT_STRING_THRESHOLD` that live in the blob
/// heap — so the emit's German-string relocation runs on some rows and not
/// others.
const FIXTURE_STRINGS: [&[u8]; 4] = [
    b"",
    b"abc",
    b"a-long-string-past-the-inline-limit",
    b"another-long-payload-string",
];

/// `n_eq` U64 equality columns + 1 U64 range column (all PK) and a trailing I64
/// payload — the canonical band-join reindex shape. `wide` adds a nullable I64
/// and a STRING, the columns a null-merge or blob-relocation bug in the row
/// writer would show up in.
fn make_range_schema(n_eq: usize, wide: bool) -> SchemaDescriptor {
    let mut cols: Vec<SchemaColumn> = (0..n_eq + 1).map(|_| SchemaColumn::new(type_code::U64, 0)).collect();
    cols.push(SchemaColumn::new(type_code::I64, 0)); // payload
    if wide {
        cols.push(SchemaColumn::new(type_code::I64, 1));
        cols.push(SchemaColumn::new(type_code::STRING, 0));
    }
    let pk: Vec<u32> = (0..n_eq as u32 + 1).collect();
    SchemaDescriptor::new(&cols, &pk)
}

/// Build a batch over [`make_range_schema`] from `(eq_cols, range, weight,
/// payload)` rows, which must arrive sorted by `(eq.., range, payload)`. On a
/// wide schema the two extra cells are *derived* from `payload`, so equal
/// payloads stay equal cell-for-cell and that sort order is the full
/// (PK, payload) order.
///
/// Left `Raw`: these fixtures carry tombstones and multiset duplicates, so no
/// layout claim holds over them.
fn make_range_batch(schema: &SchemaDescriptor, rows: &[(Vec<u64>, u64, i64, i64)]) -> Batch {
    let wide = schema.num_payload_cols() > 1;
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    for (eq, range, w, val) in rows {
        let mut vals: Vec<u128> = eq.iter().map(|&x| x as u128).collect();
        vals.push(*range as u128);
        b.extend_pk_opk(&vals);
        b.extend_weight(&w.to_le_bytes());
        let null_word = u64::from(wide && *val < 0) << 1;
        b.extend_null_bmp(&null_word.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        if wide {
            b.extend_col(1, &val.wrapping_mul(3).to_le_bytes());
            let s = FIXTURE_STRINGS[val.rem_euclid(FIXTURE_STRINGS.len() as i64) as usize];
            b.extend_col_blob(2, s);
        }
        b.count += 1;
    }
    b
}

/// Right-payload (trace I64 col) + weight of each emitted range-join row, in
/// emission order. The trace payload identifies which trace rows matched.
/// Each output row as `(left payload, right payload, weight)` — what a join whose
/// two sides carry one payload column each denotes, in emission order.
fn out_triples(out: &Batch) -> Vec<(i64, i64, i64)> {
    (0..out.count)
        .map(|r| {
            (
                read_i64_le(out.col_data(0), r * 8),
                read_i64_le(out.col_data(1), r * 8),
                out.get_weight(r),
            )
        })
        .collect()
}

fn range_out_pairs(out: &Batch) -> Vec<(i64, i64)> {
    (0..out.count)
        .map(|r| (read_i64_le(out.col_data(1), r * 8), out.get_weight(r)))
        .collect()
}

/// Brute-force reference: every `(delta row, trace row)` pair whose eq prefixes
/// agree and whose range slots satisfy `rel`, composed into the output row the
/// join must build, at weight `w_d · w_t`. Returns the Z-Set it denotes and the
/// number of pairs with a non-zero product. The composition spells the output
/// layout — left PK, then left payload cells, then right — independently of the
/// row writer under test.
fn reference(
    schema: &SchemaDescriptor,
    n_eq: usize,
    rel: RangeRel,
    delta: &Batch,
    trace: &Batch,
) -> (HashMap<RowKey, i64>, usize) {
    let eq_size = schema.leading_key_size(n_eq);
    let mut m: HashMap<RowKey, i64> = HashMap::new();
    let mut rows = 0usize;
    for i in 0..delta.count {
        let dpk = delta.get_pk_bytes(i);
        for j in 0..trace.count {
            let tpk = trace.get_pk_bytes(j);
            if dpk[..eq_size] != tpk[..eq_size] {
                continue;
            }
            // Equal-width OPK slot slices, so a raw byte compare IS the typed
            // comparison the relation names.
            let (d, s) = (&dpk[eq_size..], &tpk[eq_size..]);
            let hit = match rel {
                RangeRel::Lt => s < d,
                RangeRel::Le => s <= d,
                RangeRel::Gt => s > d,
                RangeRel::Ge => s >= d,
            };
            if !hit {
                continue;
            }
            let w = delta.get_weight(i).wrapping_mul(trace.get_weight(j));
            if w == 0 {
                continue;
            }
            rows += 1;
            let (_, mut cells) = row_key(delta, schema, i);
            cells.extend(row_key(trace, schema, j).1);
            *m.entry((dpk.to_vec(), cells)).or_insert(0) += w;
        }
    }
    m.retain(|_, w| *w != 0);
    (m, rows)
}

/// Assert the range join's output matches [`reference`] on the Z-Set it denotes
/// *and* on the raw row count — the count is what catches an equal-and-opposite
/// miss/spurious pair that the folded Z-Set alone hides. Returns it, so callers
/// can assert non-vacuous coverage.
///
/// The reference reads the *consolidated* delta, which is what the op joins; the
/// trace needs no such step, since a single-source cursor emits every row
/// verbatim.
fn assert_matches_reference(
    schema: SchemaDescriptor,
    n_eq: usize,
    rel: RangeRel,
    delta: &Batch,
    trace: Batch,
    what: &str,
) -> usize {
    let out_schema = join_out_schema(&schema, &schema);
    let cs = Batch::consolidate_if_needed(delta, &schema);
    let (want, want_rows) = reference(&schema, n_eq, rel, cs.as_ref().unwrap_or(delta), &trace);

    let mut ch = trace_cursor(trace, schema);
    let out = range_join(&schema, n_eq, rel, delta, &mut ch);

    assert_eq!(out.count, want_rows, "{what}: row count, n_eq={n_eq} rel={rel:?}");
    assert_eq!(
        zset_of(&out, &out_schema),
        want,
        "{what}: z-set, n_eq={n_eq} rel={rel:?}"
    );
    // The walk emits trace-major with a delta run per trace row, so it is
    // neither (PK, payload)-sorted nor folded, and claims no layout.
    assert_eq!(out.layout(), Layout::Raw, "{what}: n_eq={n_eq} rel={rel:?}");
    out.count
}

// -----------------------------------------------------------------------
// RangeProbe::cut_points, over 1-byte slots so the keys are exact-comparable
// -----------------------------------------------------------------------

/// The cut points for `pk = eq ‖ d` under `rel`, over the all-`U8` PK schema
/// whose one-byte slots make `pk` its own OPK image.
fn cuts(eq: &[u8], d: &[u8], rel: RangeRel) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
    let mut pk = eq.to_vec();
    pk.extend_from_slice(d);
    let schema = pk_only_schema(&vec![type_code::U8; pk.len()]);
    RangeProbe::new(&schema, &schema, eq.len() as u8, rel)
        .expect("u8-slot fixture is a well-formed range key")
        .cut_points(&pk)
        .map(|(s, e)| (s.pk_bytes().to_vec(), e.map(|e| e.pk_bytes().to_vec())))
}

/// Every cut point, for every rel, at each boundary the slot arithmetic has:
/// an interior value, the minimal and maximal slot, and a maximal eq group.
/// `None` for the whole cut is a provably-empty interval, which the walk skips
/// without a seek; a `None` end means "scan to the table end".
#[test]
fn cut_points_bound_every_rel_within_its_eq_group() {
    /// A `(start, end)` cut, in `RELS` order: `Lt`, `Le`, `Gt`, `Ge`.
    type Cut = Option<(&'static [u8], Option<&'static [u8]>)>;
    let cases: &[(&[u8], &[u8], [Cut; 4])] = &[
        // No eq prefix: the slot is the whole key, so every cut runs to the
        // table's own ends.
        (
            &[],
            &[0x05],
            [
                Some((&[0x00], Some(&[0x05]))),
                Some((&[0x00], Some(&[0x06]))),
                Some((&[0x06], None)),
                Some((&[0x05], None)),
            ],
        ),
        // The minimal slot: `Lt` spans [0, 0) and so is provably empty.
        (
            &[],
            &[0x00],
            [
                None,
                Some((&[0x00], Some(&[0x01]))),
                Some((&[0x01], None)),
                Some((&[0x00], None)),
            ],
        ),
        // The maximal slot: `Gt` matches nothing, and `Le`'s successor carries
        // out, so it scans to the table end.
        (
            &[],
            &[0xFF],
            [
                Some((&[0x00], Some(&[0xFF]))),
                Some((&[0x00], None)),
                None,
                Some((&[0xFF], None)),
            ],
        ),
        // With an eq prefix every cut stays within, or caps at, the group.
        (
            &[0x07],
            &[0x05],
            [
                Some((&[0x07, 0x00], Some(&[0x07, 0x05]))),
                Some((&[0x07, 0x00], Some(&[0x07, 0x06]))),
                Some((&[0x07, 0x06], Some(&[0x08, 0x00]))),
                Some((&[0x07, 0x05], Some(&[0x08, 0x00]))),
            ],
        ),
        // A maximal slot inside a non-maximal group: the `Le` and `Ge` ends both
        // ripple to the next group's first key, and `Gt` is a zero-width — so
        // provably empty — interval there.
        (
            &[0x07],
            &[0xFF],
            [
                Some((&[0x07, 0x00], Some(&[0x07, 0xFF]))),
                Some((&[0x07, 0x00], Some(&[0x08, 0x00]))),
                None,
                Some((&[0x07, 0xFF], Some(&[0x08, 0x00]))),
            ],
        ),
        // The last eq group: the `Gt`/`Ge` end carries out of the eq prefix, so
        // it scans to the table end rather than wrapping to a lower key.
        (
            &[0xFF],
            &[0x05],
            [
                Some((&[0xFF, 0x00], Some(&[0xFF, 0x05]))),
                Some((&[0xFF, 0x00], Some(&[0xFF, 0x06]))),
                Some((&[0xFF, 0x06], None)),
                Some((&[0xFF, 0x05], None)),
            ],
        ),
    ];

    for (eq, d, want) in cases {
        for (rel, w) in RELS.iter().zip(want) {
            let want = w.map(|(s, e)| (s.to_vec(), e.map(<[u8]>::to_vec)));
            assert_eq!(cuts(eq, d, *rel), want, "eq={eq:02x?} d={d:02x?} rel={rel:?}");
        }
    }
}
