use super::*;
use Placed::*;

/// `x CMP lit` against a value of `x`'s type, or a verdict for every value.
#[test]
fn a_comparison_with_a_placed_literal_is_one_against_a_value_or_a_constant() {
    use CmpOp::*;
    let between = Between { lo: 2, nearest: 3 };
    let below = Below { nearest: None };
    let above = Above { nearest: Some(9) };
    for (p, cmp, want) in [
        (At(5), Lt, Compared::Cmp(Lt, 5)),
        (between, Eq, Compared::Always(false)),
        (between, Ne, Compared::Always(true)),
        (between, Lt, Compared::Cmp(Le, 2)),
        (between, Le, Compared::Cmp(Le, 2)),
        (between, Gt, Compared::Cmp(Gt, 2)),
        (between, Ge, Compared::Cmp(Gt, 2)),
        (below, Gt, Compared::Always(true)),
        (below, Le, Compared::Always(false)),
        (below, Eq, Compared::Always(false)),
        (above, Lt, Compared::Always(true)),
        (above, Ge, Compared::Always(false)),
        (above, Ne, Compared::Always(true)),
    ] {
        assert_eq!(p.compare(cmp), want, "{p:?} {cmp:?}");
    }
}

/// What an assignment stores: the value itself, or the rounded one while that
/// lies inside the type.
#[test]
fn an_assignment_stores_the_nearest_value_inside_the_type() {
    let image = |v: i128| FixedInt::I8.pack(v);
    for (n, placed, stored) in [
        (1270, At(image(127)), Some(image(127))),
        (1264, Between { lo: image(126), nearest: image(126) }, Some(image(126))),
        (1274, Above { nearest: Some(image(127)) }, Some(image(127))),
        (1276, Above { nearest: None }, None),
        (-1284, Below { nearest: Some(image(-128)) }, Some(image(-128))),
    ] {
        let p = place_ratio(FixedInt::I8, n, 10, Round::HalfAwayFromZero);
        assert_eq!(p, placed, "{n}/10");
        assert_eq!(p.stored(), stored, "{n}/10");
    }
}

/// A scale gap whose power of ten is past `i128` still places: every `i128`
/// over it is under a half, on its own side of zero.
#[test]
fn a_scale_gap_past_i128_places_strictly_inside_a_half() {
    let i64_of = |v: i128| FixedInt::I64.pack(v);
    for (v, lo) in [(i128::MAX, 0), (1, 0), (i128::MIN + 1, -1), (-1, -1)] {
        let p = place_scaled(FixedInt::I64, v, 60, 0);
        assert_eq!(p, Between { lo: i64_of(lo), nearest: i64_of(0) }, "{v}");
    }
    assert_eq!(place_scaled(FixedInt::I64, 0, 60, 2), At(0));
    // An unsigned type holds nothing below zero, and rounding still reaches it.
    assert_eq!(place_scaled(FixedInt::U8, -1, 60, 0), Below { nearest: Some(0) });
}
