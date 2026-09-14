use super::*;
use gnitz_core::TypeCode;

/// The NULL gate must build at every key arity: one leaf at `k = 1` (byte-
/// identical to a single-column filter), a multi-leaf AND for `want_null =
/// false` and a multi-leaf OR for `true` at `k ≥ 2`. A compound join key that
/// failed to build here would silently admit NULL-keyed rows into the match.
#[test]
fn multi_null_filter_builds_at_every_arity() {
    let cols: Vec<ColumnDef> = (0..3)
        .map(|i| ColumnDef::new(format!("c{i}"), TypeCode::U64, true))
        .collect();
    for want_null in [false, true] {
        multi_null_filter_prog(&[0], &cols, want_null).unwrap();
        multi_null_filter_prog(&[0, 1], &cols, want_null).unwrap();
        multi_null_filter_prog(&[0, 1, 2], &cols, want_null).unwrap();
    }
}

/// A NOT NULL key needs no gate at all — `null_gate` reports `false` and hands
/// the input back untouched, which is what lets the null-fill reuse the
/// already-emitted reindex instead of re-keying.
#[test]
fn null_gate_is_a_no_op_on_a_non_nullable_key() {
    let cols = vec![
        ColumnDef::new("k", TypeCode::U64, false),
        ColumnDef::new("n", TypeCode::U64, true),
    ];
    let mut cb = CircuitBuilder::new();
    let inp = cb.input_delta(1, None);
    let (node, nullable) = null_gate(&mut cb, inp, &[0], &cols).unwrap();
    assert!(!nullable, "a NOT NULL key is not nullable");
    assert_eq!(node, inp, "a NOT NULL key emits no filter node");
    let (node, nullable) = null_gate(&mut cb, inp, &[0, 1], &cols).unwrap();
    assert!(nullable, "one nullable component makes the key nullable");
    assert_ne!(node, inp, "a nullable key emits a gate");
}
