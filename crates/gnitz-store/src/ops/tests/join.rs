use std::collections::HashMap;
use std::rc::Rc;

use proptest::prelude::*;

use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, Layout};
use crate::test_support::{
    make_batch, make_batch_i64pk as make_signed_batch, make_batch_opk, make_schema_i64pk_i64 as make_schema_signed,
    make_schema_u64_i64, opk_pk, pk_only_schema, pk_payload_schema, row_key, trace_cursor, zset_of, RowKey,
};
use gnitz_wire::read_i64_le;

const RELS: &[RangeRel] = RangeRel::ALL;

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
    cursor: &mut ReadCursor,
) -> Batch {
    let p = plan(kind, delta_is_right, delta_schema, trace_schema);
    op_join_delta_trace(delta, cursor, delta_schema, &p.out_schema, p.probe)
}

/// The probe's own equality-prefix width, so the oracle slices a key exactly
/// where the walk does.
fn probe_eq_size(probe: &JoinProbe) -> usize {
    match probe.walk {
        Walk::Range(r) => r.eq_size,
        _ => 0,
    }
}

// -----------------------------------------------------------------------
// The output schema
// -----------------------------------------------------------------------

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
    let joined = merge_schemas_for_join(JoinKind::Equi, &left, &right).unwrap();
    assert_eq!(joined.num_columns(), 3); // PK + left_I64 + right_STRING
    assert_eq!(joined.columns[0].type_code, type_code::U128);
    assert_eq!(joined.columns[1].type_code, type_code::I64);
    assert_eq!(joined.columns[2].type_code, type_code::STRING);

    // A keyless join takes no part in either key, so it mints the pair.
    let crossed = merge_schemas_for_join(JoinKind::Cross, &left, &right).unwrap();
    assert_eq!(crossed.pk_indices(), &[0, 1]);
    assert_eq!(crossed.num_columns(), 4);
    assert_eq!(crossed.columns[2].type_code, type_code::I64);
    assert_eq!(crossed.columns[3].type_code, type_code::STRING);
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
    let joined = merge_schemas_for_join(JoinKind::Equi, &left, &right).unwrap();
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
    let joined_single = merge_schemas_for_join(JoinKind::Equi, &left_single, &right).unwrap();
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
        merge_schemas_for_join(JoinKind::Equi, &make(half), &make(half)).is_none(),
        "an over-wide join output must be rejected (None), not aborted"
    );
}

/// A keyed join reads one side's PK region as the other's, so a mismatched pair
/// is refused — including one whose strides agree but whose OPK images differ by
/// the sign flip.
#[test]
fn a_keyed_join_refuses_mismatched_pk_types() {
    let signed = pk_payload_schema(&[type_code::I64]);
    let unsigned = pk_payload_schema(&[type_code::U64]);
    let narrow = pk_payload_schema(&[type_code::U32]);
    for kind in [JoinKind::Equi, JoinKind::Range { n_eq: 0, rel: RangeRel::Lt }] {
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

        let out = join(JoinKind::Equi, false, &schema, &schema, &delta, &mut ch);
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
            assert_eq!(out.get_pk_bytes(r), &held[..], "{name}: the output PK is the delta PK");
        }
        // The emission is trace-major, so neither (PK, payload)-sorted nor
        // folded; the output carries no layout claim and downstream re-sorts.
        assert_eq!(out.layout(), Layout::Raw, "{name}");
    }
}

/// The delta port is the join's right side: the payload halves swap, and the
/// output is `[key, trace payload, delta payload]`.
#[test]
fn a_right_sided_delta_writes_the_trace_half_first() {
    let schema = make_schema_u64_i64();
    let mut ch = trace_cursor(make_batch(&schema, &[(1, 1, 100), (1, 2, 200)]), schema);
    let delta = make_batch(&schema, &[(1, 1, 10)]);

    let out = join(JoinKind::Equi, true, &schema, &schema, &delta, &mut ch);
    assert_eq!(out_triples(&out), vec![(100, 10, 1), (200, 10, 2)]);
}

// -----------------------------------------------------------------------
// The equi merge walk
// -----------------------------------------------------------------------

/// One emission of [`equi_merge_walk`]: the delta group's PK, the delta run, and
/// the PK of the trace row the cursor stood on.
type Emission = (u64, Range<usize>, u64);

/// Naive reference: for every delta PK group the trace also holds, one emission
/// per matching trace row, carrying the whole delta group as its run.
fn naive_walk(delta: &Batch, m: &Batch) -> Vec<Emission> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < delta.count {
        let dk = delta.get_pk_bytes(i).to_vec();
        let mut j = i + 1;
        while j < delta.count && delta.get_pk_bytes(j) == &dk[..] {
            j += 1;
        }
        for r in 0..m.count {
            if m.get_pk_bytes(r) == &dk[..] {
                out.push((delta.get_pk(i) as u64, i..j, m.get_pk(r) as u64));
            }
        }
        i = j;
    }
    out
}

fn record_walk(delta: &Batch, m: &mut ReadCursor) -> Vec<Emission> {
    let mut out = Vec::new();
    equi_merge_walk(delta, m, |rs, re, c| {
        out.push((delta.get_pk(rs) as u64, rs..re, c.current_key_narrow() as u64));
    });
    out
}

fn walk_batch(rows: &[(u64, i64, i64)]) -> Rc<Batch> {
    Rc::new(make_batch(&make_schema_u64_i64(), rows))
}

/// The walk's emissions must match the naive reference (keys, delta runs, and
/// the trace rows it stood on) over a range of shapes: empty on each side,
/// disjoint keys, fully shared keys, duplicate keys on each side, and a large
/// size skew in each direction.
#[test]
fn equi_merge_walk_matches_the_reference_over_every_shape() {
    type Case = (&'static [(u64, i64, i64)], &'static [(u64, i64, i64)]);
    let s = make_schema_u64_i64();
    let cases: &[Case] = &[
        (&[], &[(1, 1, 10)]),
        (&[(1, 1, 10)], &[]),
        (&[(1, 1, 10), (3, 1, 30)], &[(2, 1, 20), (4, 1, 40)]),
        (&[(1, 1, 10), (2, 1, 20)], &[(1, 1, 11), (2, 1, 22)]),
        // multiset delta against a multi-payload match group
        (
            &[(1, 1, 10), (1, 1, 11), (5, 1, 50)],
            &[(1, 1, 90), (1, 1, 91), (1, 1, 92), (5, 1, 55)],
        ),
        // huge delta, tiny trace
        (
            &[(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5), (6, 1, 6)],
            &[(4, 1, 44)],
        ),
        // tiny delta, huge trace
        (
            &[(4, 1, 4)],
            &[(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 44), (5, 1, 5), (6, 1, 6)],
        ),
    ];

    for (di, mi) in cases {
        let delta = walk_batch(di);
        let mb = walk_batch(mi);
        let mut ch = ReadCursor::over_batches(std::slice::from_ref(&mb), s);
        assert_eq!(
            record_walk(&delta, &mut ch),
            naive_walk(&delta, &mb),
            "delta={di:?} trace={mi:?}",
        );
    }
}

/// A multi-source trace whose consolidation produces a ghost group (PK=3 nets to
/// weight 0 across two sources). The walk must behave as if that key is absent.
#[test]
fn equi_merge_walk_skips_a_ghost_group_across_sources() {
    let s = make_schema_u64_i64();
    let src_a = walk_batch(&[(1, 1, 10), (3, 1, 30), (5, 1, 50)]);
    let src_b = walk_batch(&[(3, -1, 30)]);
    let delta = walk_batch(&[(1, 1, 1), (3, 1, 3), (5, 1, 5)]);

    // Reference: a trace holding only pk 1 and 5.
    let live = walk_batch(&[(1, 1, 10), (5, 1, 50)]);

    let mut ch = ReadCursor::over_batches(&[Rc::clone(&src_a), Rc::clone(&src_b)], s);
    assert_eq!(
        record_walk(&delta, &mut ch),
        naive_walk(&delta, &live),
        "ghost group pk=3 must be skipped",
    );
}

/// A pre-advanced trace cursor must still produce the full walk: the merge walk
/// self-positions rather than assuming a fresh cursor.
#[test]
fn equi_merge_walk_self_positions_a_stale_cursor() {
    let s = make_schema_u64_i64();
    let mb = walk_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40), (5, 1, 50)]);
    let delta = walk_batch(&[(1, 1, 1), (3, 1, 3), (5, 1, 5)]);

    let mut ch = ReadCursor::over_batches(std::slice::from_ref(&mb), s);
    ch.advance_to(&(4u128).to_be_bytes()[8..]);
    assert!(ch.valid && ch.current_key_narrow() == 4, "precondition: stale at pk=4");

    assert_eq!(
        record_walk(&delta, &mut ch),
        naive_walk(&delta, &mb),
        "stale cursor must be reset by self-positioning",
    );
}

// -----------------------------------------------------------------------
// Cross delta-trace
// -----------------------------------------------------------------------

/// The cross join pairs every delta row with every trace row at `w_delta × w_trace`,
/// trace-major, keyed by the pair `[left PK, right PK]`. The sides differ in PK
/// width here: neither key is read. The walk rewinds, so an exhausted cursor
/// still yields the product.
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

    let cross = |delta: &Batch, ch: &mut ReadCursor| join(JoinKind::Cross, false, &left, &right, delta, ch);
    let out = cross(&delta, &mut ch);
    let got = out_triples(&out);
    let want: Vec<(i64, i64, i64)> = [(100i64, 1i64), (200, 2), (300, 1)]
        .into_iter()
        .flat_map(|(right_v, w)| [(10i64, 3i64), (20, -1)].map(move |(left_v, wd)| (left_v, right_v, wd * w)))
        .collect();
    assert_eq!(got, want);
    let got_keys: Vec<Vec<u8>> = (0..out.count).map(|r| out.get_pk_bytes(r).to_vec()).collect();
    let want_keys: Vec<Vec<u8>> = [&r1, &r2, &r3]
        .into_iter()
        .flat_map(|r| [&l1, &l2].map(|l| [&l[..], &r[..]].concat()))
        .collect();
    assert_eq!(got_keys, want_keys, "the output PK is [left PK, right PK]");
    assert_eq!(out.layout(), Layout::Raw, "trace-major: not even PK-sorted");

    // An empty trace pairs with nothing; an empty delta emits nothing.
    let mut empty = trace_cursor(Batch::empty_with_schema(&right), right);
    assert_eq!(cross(&delta, &mut empty).count, 0);
    let none = Batch::empty_with_schema(&left);
    assert_eq!(cross(&none, &mut ch).count, 0);
}

// -----------------------------------------------------------------------
// Range delta-trace: literal output
// -----------------------------------------------------------------------

/// The range join with the delta on the right, so the wire's `left REL right`
/// reads as `trace_slot REL delta_slot` — how every literal case below is spelled.
fn range_join(schema: &SchemaDescriptor, n_eq: usize, rel: RangeRel, delta: &Batch, cursor: &mut ReadCursor) -> Batch {
    let kind = JoinKind::Range { n_eq: n_eq as u8, rel };
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
        let mut ch = trace_cursor(make_batch(&schema, &[(10, 1, 110), (20, 1, 120), (30, 1, 130)]), schema);
        let delta = make_batch(&schema, &[(20, 1, 200)]);
        let out = range_join(&schema, 0, rel, &delta, &mut ch);

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
        .map(|r| (out.get_pk(r) as u64, read_i64_le(out.col_data(0), r * 8)))
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
        let got: Vec<i64> = out_triples(&out).into_iter().map(|(t, _, _)| t).collect();
        assert_eq!(got, want, "rel {rel:?}");
    }
}

/// The range op against a *used* trace cursor: a parked and an exhausted cursor
/// must both produce the fresh-cursor output — the group skip reads the cursor
/// position.
#[test]
fn range_join_reuses_a_stale_trace_cursor() {
    let schema = make_range_schema(1, false);
    let out_schema = plan(JoinKind::Range { n_eq: 1, rel: RangeRel::Lt }, true, &schema, &schema).out_schema;
    let delta = make_range_batch(&schema, &[(vec![1], 5, 1, 1), (vec![3], 5, 1, 3)]);
    let trace_rows = [
        (vec![1u64], 0u64, 1i64, 10i64),
        (vec![1], 9, 1, 19),
        (vec![3], 0, 1, 30),
        (vec![3], 9, 1, 39),
    ];
    for &rel in RELS {
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
// Delta-trace versus a brute-force reference
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
        for &rel in RELS {
            let trace = make_range_batch(&schema, &owned(trace_rows));
            let kind = JoinKind::Range { n_eq: n_eq as u8, rel };
            total += assert_matches_reference(kind, true, schema, schema, &delta, trace, name);
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

    /// Every join kind matches the brute-force reference over random delta and
    /// trace batches, on either side, for all four rels, `n_eq ∈ {0, 1, 2}`, and
    /// both payload shapes — `wide` adds the NULL and STRING columns a fixed-int
    /// fixture cannot exercise.
    #[test]
    fn join_dt_matches_reference(
        (n_eq, wide, rel_i, kind_i, delta_is_right, delta_rows, trace_rows) in (
            0usize..3, any::<bool>(), 0usize..4, 0usize..3, any::<bool>(),
        ).prop_flat_map(|(n_eq, wide, rel_i, kind_i, delta_is_right)| {
            // A cross join's output key is both PK regions, so the pair must fit
            // inside MAX_PK_COLUMNS.
            let n_eq = if kind_i == 2 { n_eq.min(1) } else { n_eq };
            (
                Just(n_eq), Just(wide), Just(rel_i), Just(kind_i), Just(delta_is_right),
                arb_range_rows(n_eq), arb_range_rows(n_eq),
            )
        }),
    ) {
        let schema = make_range_schema(n_eq, wide);
        let kind = match kind_i {
            0 => JoinKind::Equi,
            1 => JoinKind::Range { n_eq: n_eq as u8, rel: RELS[rel_i] },
            _ => JoinKind::Cross,
        };
        let delta = make_range_batch(&schema, &delta_rows);
        let trace = make_range_batch(&schema, &trace_rows);
        assert_matches_reference(kind, delta_is_right, schema, schema, &delta, trace, "proptest");
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

/// Whether the kind admits the pair of PK regions `(dpk, tpk)`. `rel` relates the
/// **left** slot to the right one, so the delta's side decides the operand order.
fn pair_matches(kind: JoinKind, delta_is_right: bool, eq_size: usize, dpk: &[u8], tpk: &[u8]) -> bool {
    match kind {
        JoinKind::Cross => true,
        JoinKind::Equi => dpk == tpk,
        JoinKind::Range { rel, .. } => {
            if dpk[..eq_size] != tpk[..eq_size] {
                return false;
            }
            // Equal-width OPK slot slices, so a raw byte compare IS the typed
            // comparison the relation names.
            let (d, s) = (&dpk[eq_size..], &tpk[eq_size..]);
            let (l, r) = match delta_is_right {
                true => (s, d),
                false => (d, s),
            };
            match rel {
                RangeRel::Lt => l < r,
                RangeRel::Le => l <= r,
                RangeRel::Gt => l > r,
                RangeRel::Ge => l >= r,
            }
        }
    }
}

/// Brute-force reference: every `(delta row, trace row)` pair the kind admits,
/// composed at weight `w_d · w_t` into the key, the left side's payload cells and
/// then the right's — spelled out independently of the emit loop under test.
/// Returns the Z-Set it denotes and the number of pairs with a non-zero product.
fn reference(
    kind: JoinKind,
    delta_is_right: bool,
    eq_size: usize,
    delta_schema: &SchemaDescriptor,
    trace_schema: &SchemaDescriptor,
    delta: &Batch,
    trace: &Batch,
) -> (HashMap<RowKey, i64>, usize) {
    let mut m: HashMap<RowKey, i64> = HashMap::new();
    let mut rows = 0usize;
    for i in 0..delta.count {
        let dpk = delta.get_pk_bytes(i);
        for j in 0..trace.count {
            let tpk = trace.get_pk_bytes(j);
            if !pair_matches(kind, delta_is_right, eq_size, dpk, tpk) {
                continue;
            }
            let w = delta.get_weight(i).wrapping_mul(trace.get_weight(j));
            if w == 0 {
                continue;
            }
            rows += 1;
            let key: Vec<u8> = match (kind, delta_is_right) {
                (JoinKind::Cross, true) => [tpk, dpk].concat(),
                (JoinKind::Cross, false) => [dpk, tpk].concat(),
                _ => dpk.to_vec(),
            };
            let d_cells = row_key(delta, delta_schema, i).1;
            let t_cells = row_key(trace, trace_schema, j).1;
            let (mut cells, tail) = match delta_is_right {
                true => (t_cells, d_cells),
                false => (d_cells, t_cells),
            };
            cells.extend(tail);
            *m.entry((key, cells)).or_insert(0) += w;
        }
    }
    m.retain(|_, w| *w != 0);
    (m, rows)
}

/// Assert the join's output matches [`reference`] on the Z-Set it denotes *and*
/// on the raw row count — the count is what catches an equal-and-opposite
/// miss/spurious pair that the folded Z-Set alone hides. Returns it, so callers
/// can assert non-vacuous coverage.
///
/// The reference reads the *consolidated* delta, which is what the op joins; the
/// trace needs no such step, since a single-source cursor emits every row
/// verbatim.
fn assert_matches_reference(
    kind: JoinKind,
    delta_is_right: bool,
    delta_schema: SchemaDescriptor,
    trace_schema: SchemaDescriptor,
    delta: &Batch,
    trace: Batch,
    what: &str,
) -> usize {
    let p = plan(kind, delta_is_right, &delta_schema, &trace_schema);
    let cs = Batch::consolidate_if_needed(delta, &delta_schema);
    let (want, want_rows) = reference(
        kind,
        delta_is_right,
        probe_eq_size(&p.probe),
        &delta_schema,
        &trace_schema,
        cs.as_ref().unwrap_or(delta),
        &trace,
    );

    let mut ch = trace_cursor(trace, trace_schema);
    let out = op_join_delta_trace(delta, &mut ch, &delta_schema, &p.out_schema, p.probe);

    let at = format!("{what}: kind={kind:?} delta_is_right={delta_is_right}");
    assert_eq!(out.count, want_rows, "{at}: row count");
    assert_eq!(zset_of(&out, &p.out_schema), want, "{at}: z-set");
    // The walk emits trace-major with a delta run per trace row, so it is
    // neither (PK, payload)-sorted nor folded, and claims no layout.
    assert_eq!(out.layout(), Layout::Raw, "{at}: layout");
    out.count
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
    let schema = pk_only_schema(&vec![type_code::U8; pk.len()]);
    RangeProbe::new(&schema, eq.len() as u8, rel, true)
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
