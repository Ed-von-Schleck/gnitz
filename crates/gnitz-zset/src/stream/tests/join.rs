use proptest::prelude::*;

use super::*;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{
    join_reference, make_batch, make_schema_u128_i64, make_schema_u64_i64, pk_only_schema, pk_payload_schema,
    rekey_plan, zset_of, TestTrace,
};
use gnitz_wire::{NullKeys, TypeCode};

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
        let got: Vec<TypeCode> = (0..joined.num_columns())
            .map(|c| joined.columns()[c].type_code)
            .collect();
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

/// An equal-key walk claims its output consolidated where it writes the rows
/// in output order: a delta run against one trace row on either port, and
/// several against several where the trace's columns lead the payload. The
/// claim is verified as it is raised.
#[test]
fn an_equi_join_claims_the_output_it_writes_in_order() {
    let schema = make_schema_u64_i64();
    let trace = TestTrace::dealt(&make_batch(&schema, &[(1, 1, 100), (1, 1, 200), (2, 1, 300)]), 1);
    let cases = [
        (&[(1, 1, 10), (2, 1, 20)], &[false, true][..]),
        (&[(2, 1, 20), (2, 1, 21)], &[false, true]),
        (&[(1, 1, 10), (1, 1, 11)], &[true]),
    ];
    for (rows, ports) in cases {
        for &delta_is_right in ports {
            let p = plan(JoinKind::Equi, delta_is_right, &schema, &schema);
            let out = op_join_delta_trace(
                &make_batch(&schema, &rows[..]),
                &mut |f, l| trace.cursor_within(f, l),
                &p,
            );
            assert!(out.is_consolidated(), "{rows:?}, right={delta_is_right}");
        }
    }
}

// -----------------------------------------------------------------------
// Delta-trace versus a brute-force reference
// -----------------------------------------------------------------------

/// A side of a join: PK columns of the drawn types and one payload column of
/// the drawn type; `wide` adds a nullable I64 and a STRING, the columns a
/// null-merge or blob-relocation bug in the emit loop would show up in.
fn side_schema(pk: &[TypeCode], payload: TypeCode, wide: bool) -> SchemaDescriptor {
    let mut cols: Vec<SchemaColumn> = pk.iter().map(|&t| SchemaColumn::new(t, false)).collect();
    cols.push(SchemaColumn::new(payload, false));
    if wide {
        cols.push(SchemaColumn::new(TypeCode::I64, true));
        cols.push(SchemaColumn::new(TypeCode::String, false));
    }
    SchemaDescriptor::new(&cols, &(0..pk.len() as u32).collect::<Vec<_>>())
}

/// Payload strings for the `wide` sides: an empty one, one short enough to stay
/// inline, and two that live in the blob heap.
const FIXTURE_STRINGS: [&[u8]; 4] = [
    b"",
    b"abc",
    b"a-long-string-past-the-inline-limit",
    b"another-long-payload-string",
];

/// `(per-column slot, weight, payload)` rows over `n_pk` key columns. A slot is
/// `0..4` in a column's last OPK byte — so dense eq groups, boundary equality and
/// non-matching groups are common, and a wide column shares all its leading
/// bytes — or the maximal all-`0xFF` image. Weights reach `±2^32`, whose
/// products wrap to the zero that is no Z-set element.
fn arb_side_rows(n_pk: usize) -> impl Strategy<Value = Vec<(Vec<Option<u8>>, i64, i64)>> {
    let slot = prop_oneof![9 => (0u8..4).prop_map(Some), 1 => Just(None)];
    let weight = prop_oneof![8 => -2i64..=2, 1 => Just(1 << 32), 1 => Just(-(1 << 32))];
    prop::collection::vec((prop::collection::vec(slot, n_pk), weight, -2i64..2), 0..10)
}

/// `rows` over `schema`, unconsolidated. On a wide schema the extra cells are
/// derived from the payload, so equal payloads stay equal cell for cell.
fn side_batch(schema: &SchemaDescriptor, rows: &[(Vec<Option<u8>>, i64, i64)]) -> Batch {
    let wide = schema.num_payload_cols() > 1;
    let mut b = BatchBuilder::new(schema);
    for (slots, w, val) in rows {
        let mut pk = Vec::new();
        for (&c, slot) in schema.pk_cols().iter().zip(slots) {
            let size = schema.columns()[c as usize].size();
            match slot {
                Some(v) => pk.extend((1..size).map(|_| 0).chain([*v])),
                None => pk.extend(std::iter::repeat_n(0xFF, size)),
            }
        }
        b.begin_row_bytes(&pk, *w);
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

fn arb_pk_types(n: std::ops::RangeInclusive<usize>) -> impl Strategy<Value = Vec<TypeCode>> {
    use TypeCode::*;
    prop::collection::vec(prop::sample::select(&[U8, I32, U64, U128][..]), n)
}

fn arb_payload_type() -> impl Strategy<Value = TypeCode> {
    use TypeCode::*;
    prop::sample::select(&[U8, I16, I32, I64, U128][..])
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Every join kind matches the brute-force reference over random delta and
    /// trace batches, on either side, for all four rels, over one to three key
    /// columns of mixed widths. Each side draws its payload on its own, and
    /// under `Cross` its own key, so a probe that split the output at the wrong
    /// side's width shows.
    #[test]
    fn join_dt_matches_reference(
        (kind, delta_is_right, (d_pk, t_pk), (d_pay, d_wide), (t_pay, t_wide)) in (
            prop_oneof![
                Just(JoinKind::Equi),
                prop::sample::select(RangeRel::ALL).prop_map(|rel| JoinKind::Range { rel }),
                Just(JoinKind::Cross),
            ],
            any::<bool>(),
            (arb_payload_type(), any::<bool>()),
            (arb_payload_type(), any::<bool>()),
        ).prop_flat_map(|(kind, delta_is_right, d, t)| {
            // A cross join's output key is both PK regions, so the pair must fit
            // inside MAX_PK_COLUMNS.
            let keys = match kind {
                JoinKind::Cross => (arb_pk_types(1..=2), arb_pk_types(1..=2)).boxed(),
                _ => arb_pk_types(1..=3).prop_map(|k| (k.clone(), k)).boxed(),
            };
            (Just(kind), Just(delta_is_right), keys, Just(d), Just(t))
        }),
        (delta_rows, trace_rows) in (arb_side_rows(3), arb_side_rows(3)),
    ) {
        let d_schema = side_schema(&d_pk, d_pay, d_wide);
        let t_schema = side_schema(&t_pk, t_pay, t_wide);
        let delta = side_batch(&d_schema, &delta_rows);
        let trace = side_batch(&t_schema, &trace_rows);
        assert_matches_reference(kind, delta_is_right, d_schema, t_schema, &delta, &trace);
    }
}

/// Assert the join's output matches [`join_reference`] on the Z-Set it denotes *and*
/// on the raw row count — the count is what catches an equal-and-opposite
/// miss/spurious pair that the folded Z-Set alone hides — over the trace as one
/// consolidated run and as its raw rows dealt round-robin into three, each
/// consolidated alone, so an element can cancel across runs.
fn assert_matches_reference(
    kind: JoinKind,
    delta_is_right: bool,
    delta_schema: SchemaDescriptor,
    trace_schema: SchemaDescriptor,
    delta: &Batch,
    trace: &Batch,
) {
    let p = plan(kind, delta_is_right, &delta_schema, &trace_schema);
    let delta = &delta.to_consolidated();
    let folded = trace.to_consolidated();
    let (want, want_rows) = join_reference(kind, delta_is_right, &delta_schema, &trace_schema, delta, &folded);
    for runs in [1, 3] {
        let trace = TestTrace::dealt(trace, runs);
        let out = op_join_delta_trace(delta, &mut |f, l| trace.cursor_within(f, l), &p);
        let at = format!("kind={kind:?} delta_is_right={delta_is_right}, {runs} runs");
        assert_eq!(out.count, want_rows, "{at}: row count");
        assert_eq!(zset_of(&out, &p.out_schema), want, "{at}: z-set");
    }
}

// -----------------------------------------------------------------------
// RangeProbe::cut_points, over 1-byte slots so the keys are exact-comparable
// -----------------------------------------------------------------------

/// The cut points are exact: for `pk = eq ‖ d` under every rel, `[start, end)`
/// holds exactly the keys of `pk`'s eq group whose slot stands in `rel` to `d`
/// — the delta on the right, so `rel` reads as the trace's relation to it —
/// and `None` only when no key does. Every key of one and two `U8` columns, at
/// the slots and eq groups where the cut arithmetic carries.
#[test]
fn cut_points_hold_exactly_the_keys_rel_admits() {
    const EDGES: [u8; 6] = [0x00, 0x01, 0x07, 0x7F, 0xFE, 0xFF];
    for len in [1usize, 2] {
        let schema = pk_only_schema(&vec![TypeCode::U8; len]);
        let keys: Vec<Vec<u8>> = (0..1usize << (8 * len))
            .map(|k| k.to_be_bytes()[size_of::<usize>() - len..].to_vec())
            .collect();
        let pks = keys.iter().filter(|k| k.iter().all(|b| EDGES.contains(b)));
        for pk in pks {
            let (eq, d) = pk.split_at(len - 1);
            for &rel in RangeRel::ALL {
                let admits = |k: &[u8]| {
                    let (k_eq, s) = k.split_at(len - 1);
                    k_eq == eq
                        && match rel {
                            RangeRel::Lt => s < d,
                            RangeRel::Le => s <= d,
                            RangeRel::Gt => s > d,
                            RangeRel::Ge => s >= d,
                        }
                };
                let want: Vec<&Vec<u8>> = keys.iter().filter(|k| admits(k)).collect();
                let got: Vec<&Vec<u8>> = match RangeProbe::new(&schema, rel, true).cut_points(pk) {
                    None => Vec::new(),
                    Some((start, end)) => keys
                        .iter()
                        .filter(|k| {
                            k.as_slice() >= start.pk_bytes() && end.as_ref().is_none_or(|e| k.as_slice() < e.pk_bytes())
                        })
                        .collect(),
                };
                assert_eq!(got, want, "pk={pk:02x?} rel={rel:?}");
            }
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
        let mut map = rekey_plan(&source_schema, key, keep, NullKeys::Drop);
        let trace = map.evaluate_map_batch(&source).into_consolidated();
        let trace_schema = *map.out_schema();

        // A delta on the same key: a payload of its own, keys present and absent.
        let delta_schema = SchemaDescriptor::new(
            &key.iter()
                .map(|&c| source_schema.columns()[c as usize])
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
            let (want, _) = join_reference(
                JoinKind::Equi,
                delta_is_right,
                &delta_schema,
                &trace_schema,
                &delta,
                &trace,
            );
            let over = JoinPlan::over_source(delta_is_right, &delta_schema, &source_schema, &map).unwrap();
            assert!(over.out_schema.same_layout(&stored.out_schema));
            assert!(!want.is_empty(), "premise: key {key:?} matches something");
            for runs in [1, 3] {
                let source = TestTrace::dealt(&source, runs);
                let got = op_join_delta_trace(&delta, &mut |f, l| source.cursor_within(f, l), &over);
                assert_eq!(
                    zset_of(&got, &over.out_schema),
                    want,
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
    let col = |c: u32| source.columns()[c as usize];
    for key in [&[1u32][..], &[1, 0], &[3]] {
        let delta = SchemaDescriptor::new(
            &key.iter()
                .map(|&c| SchemaColumn::new(col(c).type_code, false))
                .collect::<Vec<_>>(),
            &(0..key.len() as u32).collect::<Vec<_>>(),
        );
        let map = rekey_plan(&source, key, &[2], NullKeys::Drop);
        assert!(
            JoinPlan::over_source(false, &delta, &source, &map).is_err(),
            "key {key:?}"
        );
    }
    let delta = SchemaDescriptor::new(&[col(0)], &[0]);
    assert!(JoinPlan::over_source(false, &delta, &source, &rekey_plan(&source, &[0], &[2], NullKeys::Drop)).is_ok());
}
