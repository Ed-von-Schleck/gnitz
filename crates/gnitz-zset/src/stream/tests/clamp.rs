use super::*;
use crate::test_support::{make_batch, make_schema_u64_i64, opens, trace_cursor, weighted_rows};
use gnitz_wire::ClampKind::{Distinct, PositivePart};

/// `(pk, weight, payload)` fixture rows, pre-sorted by `(pk, payload)`.
pub(super) type ClampRow = (u64, i64, i64);

/// The clamp applied to one accumulated weight, from its definition:
/// `distinct` is membership, `positive_part` the weight floored at 0.
fn clamped(kind: ClampKind, w: i64) -> i64 {
    match kind {
        Distinct => i64::from(w > 0),
        PositivePart => w.max(0),
    }
}

/// Per delta element, the clamp's value after the tick minus its value before,
/// the trace's elements summed first; the transitions that are not zero.
fn reference(kind: ClampKind, delta: &[ClampRow], trace: &[ClampRow]) -> Vec<ClampRow> {
    delta
        .iter()
        .filter_map(|&(pk, dw, val)| {
            let w_old: i64 = trace
                .iter()
                .filter(|&&(p, _, v)| (p, v) == (pk, val))
                .map(|&(_, w, _)| w)
                .sum();
            let out = clamped(kind, w_old + dw) - clamped(kind, w_old);
            (out != 0).then_some((pk, out, val))
        })
        .collect()
}

/// At both kinds, the clamp emits each delta element's transition against its
/// integral weight, in delta order and certified consolidated: entering and
/// leaving, no transition inside the clamp's range or below it, a negative
/// history weight, several payloads at one PK, a delta element the trace lacks,
/// a trace element the delta skips, and an insert-only tick that emits every
/// row at its own weight.
#[test]
fn weight_clamp_emits_each_elements_transition() {
    let schema = make_schema_u64_i64();
    let cases: [(&[ClampRow], &[ClampRow]); 12] = [
        (&[(1, 3, 10)], &[]),
        (&[(1, -2, 10)], &[(1, 3, 10)]),
        (&[(1, -1, 10)], &[(1, 1, 10)]),
        (&[(1, 1, 10)], &[(1, 1, 10)]),
        (&[(1, -2, 10)], &[]),
        (&[(1, 3, 10)], &[(1, 5, 10)]),
        (&[(1, -10, 10)], &[(1, 8, 10)]),
        (&[(1, 4, 10)], &[(1, -2, 10)]),
        (&[(1, 1, 5), (2, 1, 20)], &[]),
        (
            &[(1, 1, 10), (1, -1, 11), (5, 1, 50)],
            &[(1, 1, 11), (1, 1, 12), (1, 1, 13), (5, 1, 55)],
        ),
        (
            &[(1, -1, 10), (1, 1, 20), (1, 1, 40), (3, 1, 30)],
            &[(0, 1, 5), (1, 1, 10), (1, 1, 20), (1, 1, 30), (2, 1, 7)],
        ),
        (&[(1, 4, 10), (2, 1, 20)], &[(1, -2, 10), (2, -1, 20)]),
    ];
    for (delta, trace) in cases {
        for kind in [Distinct, PositivePart] {
            let ch = trace_cursor(make_batch(&schema, trace));
            let out = op_weight_clamp(&make_batch(&schema, delta), &mut opens(ch), kind);
            assert!(out.is_consolidated());
            assert_eq!(
                weighted_rows(&out),
                weighted_rows(&make_batch(&schema, &reference(kind, delta, trace))),
                "{kind:?}: delta={delta:?} trace={trace:?}"
            );
        }
    }
}

/// An empty delta touches no element: the clamp returns it empty and opens no
/// history.
#[test]
fn weight_clamp_of_an_empty_delta_opens_nothing() {
    let schema = make_schema_u64_i64();
    for kind in [Distinct, PositivePart] {
        let out = op_weight_clamp(
            &Batch::empty_with_schema(&schema),
            &mut |_, _| panic!("an empty delta opened its history"),
            kind,
        );
        assert!(out.is_empty());
    }
}
