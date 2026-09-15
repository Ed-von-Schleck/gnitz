use super::*;
use gnitz_core::TypeCode;

/// The NULL gate must build at every key arity: one leaf at `k = 1` (byte-
/// identical to a single-column filter) and a multi-leaf AND at `k ≥ 2`. A
/// compound join key that failed to build here would silently admit NULL-keyed
/// rows into the match.
#[test]
fn multi_null_filter_builds_at_every_arity() {
    let cols: Vec<ColumnDef> = (0..3)
        .map(|i| ColumnDef::new(format!("c{i}"), TypeCode::U64, true))
        .collect();
    multi_null_filter_prog(&[0], &cols).unwrap();
    multi_null_filter_prog(&[0, 1], &cols).unwrap();
    multi_null_filter_prog(&[0, 1, 2], &cols).unwrap();
}

/// A NOT NULL key needs no gate at all: `null_gate` hands the input back
/// untouched, and one nullable component is enough to emit the filter.
#[test]
fn null_gate_is_a_no_op_on_a_non_nullable_key() {
    let cols = vec![
        ColumnDef::new("k", TypeCode::U64, false),
        ColumnDef::new("n", TypeCode::U64, true),
    ];
    let mut cb = Circuit::default();
    let inp = cb.input_delta(1, gnitz_wire::ReadBound::None);
    let node = null_gate(&mut cb, inp, &[0], &cols).unwrap();
    assert_eq!(node, inp, "a NOT NULL key emits no filter node");
    let node = null_gate(&mut cb, inp, &[0, 1], &cols).unwrap();
    assert_ne!(node, inp, "a nullable key emits a gate");
}
