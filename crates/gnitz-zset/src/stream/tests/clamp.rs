use super::*;
use crate::test_support::{arb_fold_case, fold_batch, make_schema_u64_i64, zset_of, zset_sum, TestTrace};
use gnitz_wire::ClampKind::{Distinct, PositivePart};
use proptest::prelude::*;

/// The clamp applied to one accumulated weight, from its definition:
/// `distinct` is membership, `positive_part` the weight floored at 0.
fn clamped(kind: ClampKind, w: i64) -> i64 {
    match kind {
        Distinct => i64::from(w > 0),
        PositivePart => w.max(0),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// The integral of the clamp's output is the clamp of its input's integral,
    /// element by element, at every tick: whether its history is read before
    /// or after it absorbs the tick, one run per tick so an element cancels
    /// across runs, and at every PK width and payload comparator. Each tick's
    /// output holds one row per element.
    #[test]
    fn the_output_integral_is_the_clamped_input_integral(
        (s, rows) in arb_fold_case(),
        tick_len in 1usize..8,
        kind in prop_oneof![Just(Distinct), Just(PositivePart)],
        absorbed: bool,
    ) {
        let mut hist = TestTrace::new(s);
        let mut input = Vec::new();
        let mut outs = Vec::new();
        for tick in rows.chunks(tick_len) {
            let delta = fold_batch(&s, tick).into_consolidated();
            if absorbed {
                hist.ingest(delta.clone());
            }
            let open = &mut |f: &[u8], l: &[u8]| hist.cursor_within(f, l);
            let at = if absorbed { History::Through(open) } else { History::Before(open) };
            outs.push(op_weight_clamp(Cow::Borrowed(&delta), at, kind));
            if !absorbed {
                hist.ingest(delta.clone());
            }
            input.push(delta);
            let mut want = zset_sum(&input, &s);
            want.values_mut().for_each(|w| *w = clamped(kind, *w));
            want.retain(|_, w| *w != 0);
            prop_assert_eq!(zset_sum(&outs, &s), want);
            prop_assert_eq!(zset_of(outs.last().unwrap(), &s).len(), outs.last().unwrap().len());
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
            Cow::Borrowed(&Batch::empty_with_schema(&schema)),
            History::Before(&mut |_, _| panic!("an empty delta opened its history")),
            kind,
        );
        assert!(out.is_empty());
    }
}
