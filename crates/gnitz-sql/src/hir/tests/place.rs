use super::*;
use crate::hir::bind::tests::bound;
use crate::hir::GetSource;

/// The tree under a bound body's projection, a filter with its conjunct count,
/// over `t` and `u` of the bind tests' catalog.
fn tree(sql: &str) -> String {
    let rel = bound(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    let RelExpr::Project { input, .. } = rel.as_ref() else {
        panic!("{sql}: no projection")
    };
    render(input)
}

fn render(rel: &RelExpr) -> String {
    match rel {
        RelExpr::Get { source: GetSource::Catalog { desc }, .. } => if desc.tid == 1 { "t" } else { "u" }.to_string(),
        RelExpr::Filter { input, preds } => format!("Filter[{}]({})", preds.len(), render(input)),
        RelExpr::Project { input, .. } => format!("Project({})", render(input)),
        RelExpr::Join { left, right, on, .. } => format!(
            "Join[eq={}{}]({}, {})",
            on.eq.len(),
            if on.range.is_some() { ", range" } else { "" },
            render(left),
            render(right)
        ),
        _ => "?".to_string(),
    }
}

#[test]
fn an_inner_join_takes_a_one_sided_where_into_its_input() {
    assert_eq!(
        tree("SELECT t.id FROM t JOIN u ON t.a = u.a WHERE t.b > 5"),
        "Join[eq=1](Filter[1](t), u)"
    );
}

#[test]
fn a_left_join_takes_a_where_only_into_its_preserved_side() {
    assert_eq!(
        tree("SELECT t.id FROM t LEFT JOIN u ON t.a = u.a WHERE t.b > 5 AND u.uid > 1"),
        "Filter[1](Join[eq=1](Filter[1](t), u))"
    );
    // The anti-join idiom names the null-supplying side, so it stays above.
    assert_eq!(
        tree("SELECT t.id FROM t LEFT JOIN u ON t.a = u.a WHERE u.uid IS NULL"),
        "Filter[1](Join[eq=1](t, u))"
    );
}

#[test]
fn a_left_joins_on_goes_into_its_null_supplying_side() {
    assert_eq!(
        tree("SELECT t.id FROM t LEFT JOIN u ON t.a = u.a AND u.uid > 3"),
        "Join[eq=1](t, Filter[1](u))"
    );
    // A conjunct naming no column goes wherever the rule admits it.
    assert_eq!(
        tree("SELECT t.id FROM t LEFT JOIN u ON t.a = u.a AND 1 = 0"),
        "Join[eq=1](t, Filter[1](u))"
    );
}

#[test]
fn a_full_join_places_nothing() {
    assert_eq!(
        tree("SELECT t.id FROM t FULL JOIN u ON t.a = u.a WHERE t.b > 5"),
        "Filter[1](Join[eq=1](t, u))"
    );
}

#[test]
fn a_where_keys_a_comma_join_and_filters_what_does_not_key() {
    assert_eq!(
        tree("SELECT t.id FROM t, u WHERE t.a = u.a AND t.b <> u.uid"),
        "Filter[1](Join[eq=1](t, u))"
    );
}

#[test]
fn an_on_conjunct_reaches_the_step_whose_input_it_names() {
    assert_eq!(
        tree("SELECT t.id FROM t JOIN u x ON t.a = x.a JOIN u y ON y.a = t.a AND x.uid > 1"),
        "Join[eq=1](Join[eq=1](t, Filter[1](u)), u)"
    );
}

#[test]
fn a_derived_table_input_is_filtered_over_its_projection() {
    assert_eq!(
        tree("SELECT t.id FROM t JOIN (SELECT uid, a FROM u) d ON t.a = d.a WHERE d.uid > 1"),
        "Join[eq=1](t, Filter[1](Project(u)))"
    );
}
