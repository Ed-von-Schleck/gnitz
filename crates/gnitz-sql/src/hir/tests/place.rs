use super::*;
use crate::hir::bind::tests::bound;

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
        RelExpr::Get { desc, .. } => if desc.tid == 1 { "t" } else { "u" }.to_string(),
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

/// Each conjunct lands as low as it is exact: a one-sided WHERE into an inner
/// join's input or a LEFT join's preserved side, a LEFT join's ON into its
/// null-supplying side, a FULL join's nowhere, an ON conjunct into the step whose
/// input it names; a keying WHERE comparison keys a comma join.
#[test]
fn each_conjunct_is_placed_as_low_as_it_is_exact() {
    for (sql, want) in [
        (
            "SELECT t.id FROM t JOIN u ON t.a = u.a WHERE t.b > 5",
            "Join[eq=1](Filter[1](t), u)",
        ),
        (
            "SELECT t.id FROM t LEFT JOIN u ON t.a = u.a WHERE t.b > 5 AND u.uid > 1",
            "Filter[1](Join[eq=1](Filter[1](t), u))",
        ),
        // The anti-join idiom names the null-supplying side, so it stays above.
        (
            "SELECT t.id FROM t LEFT JOIN u ON t.a = u.a WHERE u.uid IS NULL",
            "Filter[1](Join[eq=1](t, u))",
        ),
        (
            "SELECT t.id FROM t LEFT JOIN u ON t.a = u.a AND u.uid > 3",
            "Join[eq=1](t, Filter[1](u))",
        ),
        // A conjunct naming no column goes wherever the rule admits it.
        (
            "SELECT t.id FROM t LEFT JOIN u ON t.a = u.a AND 1 = 0",
            "Join[eq=1](t, Filter[1](u))",
        ),
        (
            "SELECT t.id FROM t FULL JOIN u ON t.a = u.a WHERE t.b > 5",
            "Filter[1](Join[eq=1](t, u))",
        ),
        (
            "SELECT t.id FROM t, u WHERE t.a = u.a AND t.b <> u.uid",
            "Filter[1](Join[eq=1](t, u))",
        ),
        // A WHERE comparison whose pair cannot key stays a filter, where the same
        // pair in an ON is refused.
        ("SELECT t.id FROM t, u WHERE t.f = u.a", "Filter[1](Join[eq=0](t, u))"),
        // A pair already keyed is not keyed twice, in either orientation.
        (
            "SELECT t.id FROM t JOIN u ON t.a = u.a AND u.a = t.a",
            "Join[eq=1](t, u)",
        ),
        // A conjunct reaching a filter already there joins it.
        (
            "SELECT t.id FROM t JOIN u ON t.a = u.a AND t.b > 1 WHERE t.b < 9",
            "Join[eq=1](Filter[2](t), u)",
        ),
        (
            "SELECT t.id FROM t JOIN u x ON t.a = x.a JOIN u y ON y.a = t.a AND x.uid > 1",
            "Join[eq=1](Join[eq=1](t, Filter[1](u)), u)",
        ),
        (
            "SELECT t.id FROM t JOIN (SELECT uid, a FROM u) d ON t.a = d.a WHERE d.uid > 1",
            "Join[eq=1](t, Filter[1](Project(u)))",
        ),
    ] {
        assert_eq!(tree(sql), want, "{sql}");
    }
}
