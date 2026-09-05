use super::*;
use crate::schema::{type_code, PayloadCmpKind, SchemaColumn};
use crate::test_support::{
    make_batch, make_batch_bytes, make_batch_opk, make_schema_pk_u64_payload_blob, make_schema_u128_i64,
    make_schema_u64_i64, opk_pk, pk_payload_schema, trace_cursor, u64_pk_schema,
};

/// DBSP distinct (set-membership clamp `[-1, 1]`): the named `op_weight_clamp`
/// preset for the unit tests. Production dispatches `Instr::WeightClamp`
/// straight to [`op_weight_clamp`].
fn op_distinct(delta: Batch, cursor: &mut ReadCursor, schema: &SchemaDescriptor) -> (Batch, Batch) {
    op_weight_clamp(delta, cursor, schema, -1, 1)
}

/// The clamp's per-element arithmetic, `clamp(w_old + Δw, lo, hi) − clamp(w_old,
/// lo, hi)`, at both presets: `(-1, 1)` is `distinct`, `(0, i64::MAX)` is
/// `positive_part`. Only the latter ever sees a net-negative pre-image.
#[test]
fn weight_clamp_emits_the_clamped_transition_at_both_presets() {
    let schema = make_schema_u64_i64();
    // (lo, hi, integral weight, delta weight, emitted weight; 0 = no row).
    let cases = [
        (-1i64, 1i64, 0i64, 3i64, 1i64), // 0 → positive: the element enters
        (-1, 1, 3, -2, 0),               // positive → positive: no transition
        (-1, 1, 1, -1, -1),              // positive → 0: the element leaves
        (-1, 1, 1, 1, 0),                // already a member
        (-1, 1, 0, -2, -1),              // 0 → negative
        (0, i64::MAX, 5, 3, 3),          // max(0,8) − max(0,5)
        (0, i64::MAX, 8, -10, -8),       // max(0,−2) − max(0,8)
        (0, i64::MAX, -2, 4, 2),         // a negative pre-image clamps to 0
    ];
    for (lo, hi, w_old, w_delta, want) in cases {
        let mut ch = if w_old == 0 {
            crate::storage::empty_cursor(schema)
        } else {
            trace_cursor(make_batch(&schema, &[(1, w_old, 10)]), schema)
        };
        let delta = make_batch(&schema, &[(1, w_delta, 10)]);
        let (out, consolidated) = op_weight_clamp(delta, &mut ch, &schema, lo, hi);

        let got: Vec<i64> = (0..out.count).map(|r| out.get_weight(r)).collect();
        let want: Vec<i64> = if want == 0 { vec![] } else { vec![want] };
        assert_eq!(got, want, "({lo},{hi}) w_old={w_old} Δ={w_delta}");
        assert!(out.is_consolidated());
        assert!(consolidated.is_consolidated());
    }

    // An empty delta short-circuits to an empty output.
    let mut ch = crate::storage::empty_cursor(schema);
    let (out, _) = op_distinct(make_batch(&schema, &[]), &mut ch, &schema);
    assert_eq!(out.count, 0);
}

/// Several payloads at one PK, exercising the (PK, payload) sub-merge inside the
/// `cogroup_left` group: a retraction to zero, a no-op bump, and a brand-new
/// payload, all walked against a multi-payload trace group in lockstep.
#[test]
fn distinct_sub_merges_the_payloads_within_one_pk_group() {
    let schema = make_schema_u64_i64();
    // Trace PK=1 carries payloads 10, 20, 30, each at weight 1.
    let trace = make_batch(&schema, &[(1, 1, 10), (1, 1, 20), (1, 1, 30)]);
    let mut ch = trace_cursor(trace, schema);

    // Retract 10 (1→0 ⇒ −1), bump 20 (1→2 ⇒ no change), add 40 (0→1 ⇒ +1).
    // Payload 30 is untouched.
    let delta = make_batch(&schema, &[(1, -1, 10), (1, 1, 20), (1, 1, 40)]);
    let (out, _) = op_distinct(delta, &mut ch, &schema);

    let got: Vec<(i64, i64)> = (0..out.count)
        .map(|r| (gnitz_wire::read_i64_le(out.col_data(0), r * 8), out.get_weight(r)))
        .collect();
    assert_eq!(got, vec![(10, -1), (40, 1)], "only the 10-retract and the 40-insert");
}

/// One distinct case: the delta rows, and the `(PK bytes, weight)` output.
type ProbeCase<'a> = (&'a [(&'a [u8], i64, i64)], &'a [(&'a [u8], i64)]);

/// The trace probe compares whole OPK keys, at every PK shape. `absent` always
/// sorts *below* `held`, so the group's `advance_to` parks the cursor on the
/// trace row and the key comparison — not cursor exhaustion — is what has to
/// reject it. Each pair is picked to break a probe that read the key as
/// anything but whole bytes: the 3×U64 pair shares its leading 16 bytes and
/// carries the same payload, the I64 pair straddles zero, and the U128 pair
/// sits at the extremes.
#[test]
fn distinct_probes_the_trace_by_whole_opk_keys_at_every_pk_shape() {
    let shapes: [(&str, SchemaDescriptor, &[u128], &[u128]); 5] = [
        ("u64", pk_payload_schema(&[type_code::U64]), &[2], &[1]),
        ("2xu64", pk_payload_schema(&[type_code::U64; 2]), &[2, 3], &[1, 5]),
        (
            "3xu64",
            pk_payload_schema(&[type_code::U64; 3]),
            &[1, 1, 1 << 56],
            &[1, 1, 2],
        ),
        ("i64", pk_payload_schema(&[type_code::I64]), &[2], &[-1i64 as u128]),
        ("u128", make_schema_u128_i64(), &[u128::MAX], &[0]),
    ];
    for (name, schema, held, absent) in shapes {
        let (held, absent) = (opk_pk(&schema, held), opk_pk(&schema, absent));
        // (delta rows, expected (PK bytes, weight) output).
        let cases: [ProbeCase; 4] = [
            (&[(&held, 1, 10)], &[]),               // re-add an existing element
            (&[(&held, 1, 99)], &[(&held, 1)]),     // a new payload at a held PK
            (&[(&held, -1, 10)], &[(&held, -1)]),   // full retraction
            (&[(&absent, 1, 10)], &[(&absent, 1)]), // a key the trace does not hold
        ];
        for (delta_rows, want) in cases {
            let trace = make_batch_opk(&schema, &[(&held, 1, 10)]);
            let mut ch = trace_cursor(trace, schema);
            let (out, _) = op_distinct(make_batch_opk(&schema, delta_rows), &mut ch, &schema);

            let got: Vec<(&[u8], i64)> = (0..out.count)
                .map(|r| (out.get_pk_bytes(r), out.get_weight(r)))
                .collect();
            assert_eq!(got, want, "{name}: delta={delta_rows:?}");
        }
    }
}

/// Two elements at one PK — `mk(false)` and `mk(true)` — must compare as
/// distinct: re-adding the first transitions nothing, adding the second emits
/// `+1`.
fn assert_payload_dispatch(schema: &SchemaDescriptor, mk: impl Fn(bool) -> Batch, what: &str) {
    let mut ch = trace_cursor(mk(false), *schema);
    let (out, _) = op_distinct(mk(false), &mut ch, schema);
    assert_eq!(out.count, 0, "{what}: an existing element must not transition");

    let mut ch = trace_cursor(mk(false), *schema);
    let (out, _) = op_distinct(mk(true), &mut ch, schema);
    assert_eq!((out.count, out.get_weight(0)), (1, 1), "{what}: a new element emits +1");
}

/// The payload comparator the schema selects. `op_weight_clamp` hoists it once
/// per scan, so the two arms it can pick are the whole dispatch: a fixed-int
/// payload narrower than the `i64` the comparator reads through, and a BLOB,
/// which shares the German-string layout and so must compare as STRING does
/// rather than through the fixed-width path.
#[test]
fn distinct_compares_payloads_through_the_schema_selected_comparator() {
    let narrow = u64_pk_schema(SchemaColumn::new(type_code::I32, 0));
    let blob = make_schema_pk_u64_payload_blob();
    assert_eq!(narrow.payload_cmp, PayloadCmpKind::FixedIntNonnull);
    assert_eq!(blob.payload_cmp, PayloadCmpKind::Generic);

    assert_payload_dispatch(
        &narrow,
        |other| make_batch(&narrow, &[(1, 1, if other { 99 } else { 42 })]),
        "i32 payload",
    );
    assert_payload_dispatch(
        &blob,
        |other| make_batch_bytes(&blob, &[(1, 1, if other { &b"bye"[..] } else { &b"hi"[..] })]),
        "blob payload",
    );
}
