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
