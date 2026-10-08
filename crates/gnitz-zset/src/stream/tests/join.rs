use proptest::prelude::*;

use super::*;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{
    join_reference, make_batch, make_batch_opk, make_schema_i64pk_i64, make_schema_u128_i64, make_schema_u64_i64,
    opens, opk_pk, pk_only_schema, pk_payload_schema, rekey_plan, trace_cursor, zset_of, TestTrace,
};
use gnitz_wire::read_i64_le;

/// The plan the compiler would bake for this pair, through the operator's own
/// constructor rather than a look-alike.
fn plan(
    kind: JoinKind,
    delta_is_right: bool,
    delta_schema: &SchemaDescriptor,
    trace_schema: &SchemaDescriptor,
) -> JoinPlan {
    JoinPlan::from_wire(kind, delta_is_right, delta_schema, trace_schema).expect("fixture join plan is well-formed")
}

/// The join as the VM dispatches it.
fn join(
    kind: JoinKind,
    delta_is_right: bool,
    delta_schema: &SchemaDescriptor,
    trace_schema: &SchemaDescriptor,
    delta: &Batch,
    cursor: ReadCursor,
) -> Batch {
    let p = plan(kind, delta_is_right, delta_schema, trace_schema);
    // The VM hands the kernel a folded register; these fixtures build raw ones.
    op_join_delta_trace(&delta.to_consolidated(), &mut opens(cursor), &p)
}

// -----------------------------------------------------------------------
// The output schema
// -----------------------------------------------------------------------

/// The output is the key — the shared one, or under `Cross` the pair of both
/// PKs — then the left payload, then the right, whichever port the delta
/// arrives on.
#[test]
fn the_output_lays_out_the_key_then_both_payloads() {
    use TypeCode::*;
    let u128_string = SchemaDescriptor::new(
        &[SchemaColumn::new(U128, false), SchemaColumn::new(String, false)],
        &[0],
    );
    let compound = SchemaDescriptor::new(&[SchemaColumn::new(U64, false); 4], &[1, 2]);
    /// `(kind, left, right, output PK columns, output column types)`.
    type Case = (
        JoinKind,
        SchemaDescriptor,
        SchemaDescriptor,
        &'static [u32],
        &'static [TypeCode],
    );
    let cases: [Case; 3] = [
        (
            JoinKind::Equi,
            make_schema_u128_i64(),
            u128_string,
            &[0],
            &[U128, I64, String],
        ),
        (
            JoinKind::Cross,
            make_schema_u128_i64(),
            u128_string,
            &[0, 1],
            &[U128, U128, I64, String],
        ),
        (
            JoinKind::Equi,
            compound,
            pk_payload_schema(&[U64; 2]),
            &[0, 1],
            &[U64, U64, U64, U64, I64],
        ),
    ];
    for (kind, left, right, pk, types) in cases {
        let joined = plan(kind, false, &left, &right).out_schema;
        assert_eq!(
            plan(kind, true, &right, &left).out_schema,
            joined,
            "{kind:?}: the delta's port"
        );
        assert_eq!(joined.pk_cols(), pk, "{kind:?}");
        let got: Vec<TypeCode> = (0..joined.num_columns()).map(|c| joined.columns[c].type_code).collect();
        assert_eq!(got, types, "{kind:?}");
    }
}

/// A keyed join reads one side's PK region as the other's, so a mismatched pair
/// is refused — including one whose strides agree but whose OPK images differ by
/// the sign flip.
#[test]
fn a_keyed_join_refuses_mismatched_pk_types() {
    let signed = pk_payload_schema(&[TypeCode::I64]);
    let unsigned = pk_payload_schema(&[TypeCode::U64]);
    let narrow = pk_payload_schema(&[TypeCode::U32]);
    for kind in [JoinKind::Equi, JoinKind::Range { rel: RangeRel::Lt }] {
        for (a, b) in [(&signed, &unsigned), (&unsigned, &narrow)] {
            let err = JoinPlan::from_wire(kind, false, a, b)
                .err()
                .expect("a keyed join must refuse mismatched PK types")
                .to_string();
            assert!(err.contains("PK column types differ"), "{kind:?}: {err}");
        }
    }
    // The keyless join reads neither key, so it takes any pair.
    assert!(JoinPlan::from_wire(JoinKind::Cross, false, &signed, &narrow).is_ok());
}

// -----------------------------------------------------------------------
// Literal output
// -----------------------------------------------------------------------

/// The equi join at every PK stride. The absent key sits directly after the
/// held one in the delta and, for the wide shape, shares its leading 16 OPK
/// bytes — so a delta group that compared anything less than the whole key
/// would fuse the two and product the extra row against the trace group.
#[test]
fn equi_join_products_the_trace_group_at_every_pk_shape() {
    let shapes: [(&str, SchemaDescriptor, &[u128], &[u128]); 3] = [
        ("i32", pk_payload_schema(&[TypeCode::I32]), &[-7i64 as u128], &[1]),
        ("u64", pk_payload_schema(&[TypeCode::U64]), &[1], &[2]),
        (
            "3xu64",
            pk_payload_schema(&[TypeCode::U64; 3]),
            &[1, 1, 2],
            &[1, 1, 1 << 56],
        ),
    ];
    for (name, schema, held, absent) in shapes {
        let (held, absent) = (opk_pk(&schema, held), opk_pk(&schema, absent));
        let trace = make_batch_opk(&schema, &[(&held, 1, 100), (&held, 2, 200)]);
        let delta = make_batch_opk(
            &schema,
            &[(&held, 1, 10), (&held, 1, 20), (&held, 1, 30), (&absent, 1, 40)],
        );
        let rows = assert_matches_reference(JoinKind::Equi, false, schema, schema, &delta, &trace, name);
        assert_eq!(rows, 6, "{name}");
    }
}

/// An equal-key walk claims its output consolidated exactly where it wrote the
/// rows in output order: a delta run against one trace row, one delta row
/// against several, and several against several only where the trace's columns
/// lead the payload. The claim is verified as it is raised, and the rows are
/// the reference's either way.
#[test]
fn an_equi_join_claims_the_output_it_writes_in_order() {
    let schema = make_schema_u64_i64();
    let trace = make_batch(&schema, &[(1, 1, 100), (1, 1, 200), (2, 1, 300)]);
    /// `(what, delta rows, claimed with the delta on the left, on the right)`.
    type Case = (&'static str, &'static [(u64, i64, i64)], bool, bool);
    let cases: [Case; 4] = [
        ("one delta row per key", &[(1, 1, 10), (2, 1, 20)], true, true),
        ("a delta run on a one-row key", &[(2, 1, 20), (2, 1, 21)], true, true),
        ("a delta run on a two-row key", &[(1, 1, 10), (1, 1, 11)], false, true),
        ("no match", &[(3, 1, 30)], false, false),
    ];
    for (what, rows, left, right) in cases {
        let delta = make_batch(&schema, rows);
        for (delta_is_right, claimed) in [(false, left), (true, right)] {
            let cursor = trace_cursor(Batch::clone(&trace));
            let out = join(JoinKind::Equi, delta_is_right, &schema, &schema, &delta, cursor);
            assert_eq!(
                out.is_consolidated(),
                claimed || out.is_empty(),
                "{what}, right={delta_is_right}"
            );
            let (want, _) = join_reference(JoinKind::Equi, delta_is_right, &schema, &schema, &delta, &trace);
            assert_eq!(zset_of(&out, out.schema()), want, "{what}, right={delta_is_right}");
        }
    }
}

/// The range join with the delta on the right, so the wire's `left REL right`
/// reads as `trace_slot REL delta_slot` — how every literal case below is spelled.
fn range_join(schema: &SchemaDescriptor, rel: RangeRel, delta: &Batch, cursor: ReadCursor) -> Batch {
    let kind = JoinKind::Range { rel };
    join(kind, true, schema, schema, delta, cursor)
}

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
        let ch = trace_cursor(make_batch(&schema, &[(10, 1, 110), (20, 1, 120), (30, 1, 130)]));
        let delta = make_batch(&schema, &[(20, 1, 200)]);
        let out = range_join(&schema, rel, &delta, ch);

        // Delta on the right, so the trace payload leads each output row.
        let got: Vec<i64> = out_triples(&out).into_iter().map(|(t, _, _)| t).collect();
        assert_eq!(got, want, "rel {rel:?}");
        // The delta PK (the range key x) and its payload survive on every row.
        for r in 0..out.count {
            assert_eq!(out.get_pk(r) as u64, 20);
            assert_eq!(read_i64_le(out.col_data(1), r * 8), 200);
        }
    }
}

/// A signed range pair (both sides reindex to I64). Negative trace keys must
/// order below positives, which they do only because the probe reads the OPK
/// image — raw bytes would put -100 above +50.
#[test]
fn range_join_orders_a_signed_key_by_its_opk_image() {
    let schema = make_schema_i64pk_i64();
    let trace_rows = [(-100i64 as u64, 1, 1), (0, 1, 2), (50, 1, 3)];
    let delta = make_batch(&schema, &[(0, 1, 9)]);
    for (rel, want) in [(RangeRel::Gt, vec![3]), (RangeRel::Lt, vec![1])] {
        let ch = trace_cursor(make_batch(&schema, &trace_rows));
        let out = range_join(&schema, rel, &delta, ch);
        let got: Vec<i64> = out_triples(&out).into_iter().map(|(t, _, _)| t).collect();
        assert_eq!(got, want, "rel {rel:?}");
    }
}

// -----------------------------------------------------------------------
// Delta-trace versus a brute-force reference
// -----------------------------------------------------------------------

/// A range-join fixture: the eq-column arity, the delta and trace rows, and the
/// least number of output rows the four rels together must produce (which keeps
/// a shape from passing vacuously).
type RangeCase = (&'static str, usize, RangeRows, RangeRows, usize);
type RangeRows = &'static [(&'static [u64], u64, i64, i64)];

/// The slot boundaries worth pinning deterministically, each against the
/// brute-force reference for all four rels.
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
        // A narrow covered span inside a large trace: `Gt`/`Ge` seek past the
        // dead low head, `Lt`/`Le` stop before the dead high tail.
        (
            "narrow span over a large trace",
            0,
            &[(&[], 25, 1, 1), (&[], 26, 1, 2)],
            LARGE_TRACE,
            50,
        ),
    ];

    for &(name, n_eq, delta_rows, trace_rows, min_rows) in cases {
        let schema = make_range_schema(n_eq, false);
        let delta = make_range_batch(&schema, &owned(delta_rows));
        let trace = make_range_batch(&schema, &owned(trace_rows));
        let mut total = 0;
        for &rel in RangeRel::ALL {
            let kind = JoinKind::Range { rel };
            total += assert_matches_reference(kind, true, schema, schema, &delta, &trace, name);
        }
        assert!(
            total >= min_rows,
            "{name}: emitted {total} rows, expected at least {min_rows}"
        );
    }
}

/// A 15-row trace, the size at which the seek must skip a dead head or tail
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
/// join's walk reads them in. Weights span `{-2..=2}` so a delta carries
/// retractions and a trace cancelling pairs that fold away; the four-value key
/// space forces dense eq groups, boundary equality (`d == s`) and non-matching
/// groups, and the occasional `u64::MAX` slot reaches the maximal cuts.
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

    /// Every join kind matches the brute-force reference over random delta and
    /// trace batches, on either side, for all four rels and `n_eq ∈ {0, 1, 2}`.
    /// Each side draws its payload shape on its own — `wide` adds the NULL and
    /// STRING columns a fixed-int fixture cannot exercise — and under `Cross`
    /// its own key arity, so a probe that split the output at the wrong side's
    /// width shows.
    #[test]
    fn join_dt_matches_reference(
        (kind, delta_is_right, d_wide, t_wide, (d_eq, t_eq, delta_rows, trace_rows)) in (
            prop_oneof![
                Just(JoinKind::Equi),
                prop::sample::select(RangeRel::ALL).prop_map(|rel| JoinKind::Range { rel }),
                Just(JoinKind::Cross),
            ],
            any::<bool>(), any::<bool>(), any::<bool>(),
        ).prop_flat_map(|(kind, delta_is_right, d_wide, t_wide)| {
            // A cross join's output key is both PK regions, so the pair must fit
            // inside MAX_PK_COLUMNS.
            let arities = match kind {
                JoinKind::Cross => (0usize..2, 0usize..2).boxed(),
                _ => (0usize..3).prop_map(|n| (n, n)).boxed(),
            };
            let rows = arities.prop_flat_map(|(d_eq, t_eq)| (Just(d_eq), Just(t_eq), arb_range_rows(d_eq), arb_range_rows(t_eq)));
            (Just(kind), Just(delta_is_right), Just(d_wide), Just(t_wide), rows)
        }),
    ) {
        let (d_schema, t_schema) = (make_range_schema(d_eq, d_wide), make_range_schema(t_eq, t_wide));
        let delta = make_range_batch(&d_schema, &delta_rows);
        let trace = make_range_batch(&t_schema, &trace_rows);
        assert_matches_reference(kind, delta_is_right, d_schema, t_schema, &delta, &trace, "proptest");
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
/// and a STRING, the columns a null-merge or blob-relocation bug in the emit
/// loop would show up in.
fn make_range_schema(n_eq: usize, wide: bool) -> SchemaDescriptor {
    let mut cols: Vec<SchemaColumn> = (0..n_eq + 1).map(|_| SchemaColumn::new(TypeCode::U64, false)).collect();
    cols.push(SchemaColumn::new(TypeCode::I64, false)); // payload
    if wide {
        cols.push(SchemaColumn::new(TypeCode::I64, true));
        cols.push(SchemaColumn::new(TypeCode::String, false));
    }
    let pk: Vec<u32> = (0..n_eq as u32 + 1).collect();
    SchemaDescriptor::new(&cols, &pk)
}

/// Build a batch over [`make_range_schema`] from `(eq_cols, range, weight,
/// payload)` rows, which must arrive sorted by `(eq.., range, payload)`. On a
/// wide schema the two extra cells are *derived* from `payload`, so equal
/// payloads stay equal cell-for-cell and that sort order is the full
/// (PK, payload) order.
fn make_range_batch(schema: &SchemaDescriptor, rows: &[(Vec<u64>, u64, i64, i64)]) -> Batch {
    let wide = schema.num_payload_cols() > 1;
    let mut b = BatchBuilder::new(schema);
    for (eq, range, w, val) in rows {
        let mut vals: Vec<u128> = eq.iter().map(|&x| x as u128).collect();
        vals.push(*range as u128);
        b.begin_row_natives(&vals, *w);
        b.put_int(*val as u128);
        if wide {
            match *val < 0 {
                true => b.put_null(),
                false => b.put_int(val.wrapping_mul(3) as u128),
            }
            b.put_blob(FIXTURE_STRINGS[val.rem_euclid(FIXTURE_STRINGS.len() as i64) as usize]);
        }
        b.end_row();
    }
    b.finish()
}

/// Each output row as `(first payload, second payload, weight)` — what a join
/// whose two sides carry one payload column each denotes, in emission order and
/// in the side order the output schema fixes.
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

/// Assert the join's output matches [`join_reference`] on the Z-Set it denotes *and*
/// on the raw row count — the count is what catches an equal-and-opposite
/// miss/spurious pair that the folded Z-Set alone hides — over the trace as one
/// consolidated run and as its raw rows dealt round-robin into three, each
/// consolidated alone, so an element can cancel across runs. Returns the count,
/// so callers can assert non-vacuous coverage.
fn assert_matches_reference(
    kind: JoinKind,
    delta_is_right: bool,
    delta_schema: SchemaDescriptor,
    trace_schema: SchemaDescriptor,
    delta: &Batch,
    trace: &Batch,
    what: &str,
) -> usize {
    let p = plan(kind, delta_is_right, &delta_schema, &trace_schema);
    let delta = &delta.to_consolidated();
    let folded = Batch::clone(trace).into_consolidated();
    let (want, want_rows) = join_reference(kind, delta_is_right, &delta_schema, &trace_schema, delta, &folded);

    let cursors = [
        ("one run", trace_cursor(folded)),
        ("three runs", TestTrace::dealt(trace, 3).cursor()),
    ];
    for (runs, ch) in cursors {
        let out = op_join_delta_trace(delta, &mut opens(ch), &p);
        let at = format!("{what}: kind={kind:?} delta_is_right={delta_is_right}, {runs}");
        assert_eq!(out.count, want_rows, "{at}: row count");
        assert_eq!(zset_of(&out, &p.out_schema), want, "{at}: z-set");
    }
    want_rows
}

// -----------------------------------------------------------------------
// RangeProbe::cut_points, over 1-byte slots so the keys are exact-comparable
// -----------------------------------------------------------------------

/// The cut points for `pk = eq ‖ d` under `rel`, over the all-`U8` PK schema
/// whose one-byte slots make `pk` its own OPK image. The delta sits on the
/// right, so `rel` reads as the trace's relation to it.
fn cuts(eq: &[u8], d: &[u8], rel: RangeRel) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
    let mut pk = eq.to_vec();
    pk.extend_from_slice(d);
    let schema = pk_only_schema(&vec![TypeCode::U8; pk.len()]);
    RangeProbe::new(&schema, rel, true)
        .cut_points(&pk)
        .map(|(s, e)| (s.pk_bytes().to_vec(), e.map(|e| e.pk_bytes().to_vec())))
}

/// Every cut point, for every rel, at each boundary the slot arithmetic has:
/// an interior value, the minimal and maximal slot, and a maximal eq group.
/// `None` for the whole cut is a provably-empty interval, which the walk skips
/// without a seek; a `None` end means "scan to the table end".
#[test]
fn cut_points_bound_every_rel_within_its_eq_group() {
    /// A `(start, end)` cut, in `RangeRel::ALL` order: `Lt`, `Le`, `Gt`, `Ge`.
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
        for (rel, w) in RangeRel::ALL.iter().zip(want) {
            let want = w.map(|(s, e)| (s.to_vec(), e.map(<[u8]>::to_vec)));
            assert_eq!(cuts(eq, d, *rel), want, "eq={eq:02x?} d={d:02x?} rel={rel:?}");
        }
    }
}

// -----------------------------------------------------------------------
// A trace read off the relation it re-keys
// -----------------------------------------------------------------------

/// `(a, b | s, v)`: a two-column key, a string and a nullable integer.
fn keyed_source() -> SchemaDescriptor {
    use TypeCode::*;
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(I64, false),
            SchemaColumn::new(U64, false),
            SchemaColumn::new(String, false),
            SchemaColumn::new(I64, true),
        ],
        &[0, 1],
    )
}

/// `rows` of [`keyed_source`], each `(a, b, weight)`, with a long string that
/// repeats and a NULL in every third `v`.
fn keyed_source_batch(rows: &[(i64, u64, i64)]) -> Batch {
    let schema = keyed_source();
    let mut b = BatchBuilder::new(&schema);
    for &(a, k, w) in rows {
        b.begin_row_natives(&[a as u128, k as u128], w);
        b.put_string(&format!("a string long enough for the heap, number {}", k % 3));
        b.put_opt_int((k % 3 != 0).then_some((a * 100 + k as i64) as u128));
        b.end_row();
    }
    b.finish().into_consolidated()
}

/// A join whose trace is read off its source is the join over the stored trace:
/// keyed on the whole PK and on a prefix of it, with the kept columns in any
/// order and a PK column among them, on either port, over one run and three.
#[test]
fn a_join_over_its_traces_source_is_the_join_over_the_trace() {
    let source_schema = keyed_source();
    let source = keyed_source_batch(&[
        (-3, 1, 1),
        (-3, 2, 1),
        (1, 0, 2),
        (1, 4, 1),
        (1, 5, -1),
        (2, 9, 1),
        (7, 3, 1),
        (7, 6, 1),
    ]);
    // (key columns, kept columns)
    let cases: &[(&[u32], &[u32])] = &[
        (&[0], &[1, 2, 3]),
        (&[0], &[3, 2]),
        (&[0], &[]),
        (&[0, 1], &[2, 3]),
        (&[0, 1], &[3, 1, 0, 2]),
    ];
    for &(key, keep) in cases {
        let mut map = rekey_plan(&source_schema, key, keep);
        let trace = map.evaluate_map_batch(&source).into_consolidated();
        let trace_schema = *map.out_schema();

        // A delta on the same key: a payload of its own, keys present and absent.
        let delta_schema = SchemaDescriptor::new(
            &key.iter()
                .map(|&c| source_schema.columns[c as usize])
                .chain([SchemaColumn::new(TypeCode::I64, false)])
                .collect::<Vec<_>>(),
            &(0..key.len() as u32).collect::<Vec<_>>(),
        );
        let mut d = BatchBuilder::new(&delta_schema);
        for (a, k, w) in [
            (-9i64, 0u64, 1i64),
            (-3, 2, 2),
            (1, 4, -1),
            (1, 5, 1),
            (5, 5, 1),
            (7, 6, 3),
            (8, 0, 1),
        ] {
            let pk: Vec<u128> = [a as u128, k as u128][..key.len()].to_vec();
            d.begin_row_natives(&pk, w);
            d.put_int((a * 7) as u128);
            d.end_row();
        }
        let delta = d.finish().into_consolidated();

        for delta_is_right in [false, true] {
            let stored = plan(JoinKind::Equi, delta_is_right, &delta_schema, &trace_schema);
            let want = op_join_delta_trace(&delta, &mut opens(trace_cursor(trace.clone())), &stored);
            let over = JoinPlan::over_source(delta_is_right, &delta_schema, &source_schema, &map).unwrap();
            assert!(over.out_schema.same_layout(&stored.out_schema));
            assert!(!want.is_empty(), "premise: key {key:?} matches something");
            for runs in [1, 3] {
                let got = op_join_delta_trace(&delta, &mut opens(TestTrace::dealt(&source, runs).cursor()), &over);
                assert_eq!(
                    zset_of(&got, &over.out_schema),
                    zset_of(&want, &stored.out_schema),
                    "key {key:?} keep {keep:?} delta_is_right={delta_is_right} runs={runs}"
                );
            }
        }
    }
}

/// A trace that is no re-key of its source onto leading PK columns is refused:
/// a key that skips the first PK column, runs out of PK order, or is a payload
/// column.
#[test]
fn over_source_refuses_a_trace_its_source_does_not_prefix() {
    let source = keyed_source();
    let col = |c: u32| source.columns[c as usize];
    for key in [&[1u32][..], &[1, 0], &[3]] {
        let delta = SchemaDescriptor::new(
            &key.iter()
                .map(|&c| SchemaColumn::new(col(c).type_code, false))
                .collect::<Vec<_>>(),
            &(0..key.len() as u32).collect::<Vec<_>>(),
        );
        let map = rekey_plan(&source, key, &[2]);
        assert!(
            JoinPlan::over_source(false, &delta, &source, &map).is_err(),
            "key {key:?}"
        );
    }
    let delta = SchemaDescriptor::new(&[col(0)], &[0]);
    assert!(JoinPlan::over_source(false, &delta, &source, &rekey_plan(&source, &[0], &[2])).is_ok());
}
