//! `NOW()` in a view body: a comparison against `_system.clock`, joined in under
//! no name and keyed as a range (or equality) join.

use gnitz_core::RelName;
use gnitz_wire::{JoinKind, JoinSide};

use super::*;

/// [`base`] plus `ev`, a table with a TIMESTAMP column, and `_system.clock`.
fn cat() -> TestCatalog {
    let cat = base();
    let clock = RelName::new(gnitz_wire::SYSTEM_SCHEMA, gnitz_wire::CLOCK.name).unwrap();
    let clock_cols = gnitz_wire::CLOCK.column_defs().collect();
    cat.insert(
        &clock,
        rel(gnitz_wire::CLOCK.id, RelClass::Catalog, clock_cols, vec![0], vec![]),
    );
    cat.insert(
        &in_sn("ev"),
        table(
            40,
            vec![
                col("id", TypeCode::I64),
                col("ts", TypeCode::Timestamp),
                col("v", TypeCode::I64),
            ],
            vec![0],
        ),
    );
    cat
}

/// Every join of every segment of `chain`.
fn joins(chain: &ViewBundle) -> Vec<JoinKind> {
    all_views(chain)
        .flat_map(|pv| pv.circuit.ops().map(|(_, op)| op.clone()))
        .filter_map(|op| match op {
            OpNode::Join(kind) => Some(kind),
            _ => None,
        })
        .collect()
}

/// The visible output column names.
fn visible(chain: &ViewBundle) -> Vec<String> {
    output_shape(chain).into_iter().filter(|c| !c.1).map(|c| c.0).collect()
}

/// A comparison against the current time keys a range join whose clock side is
/// marked, whatever side of the comparison NOW() stands on and whatever else the
/// WHERE filters; no column of the clock reaches the output, `*` included.
#[test]
fn now_keys_a_clock_range_join() {
    let cat = cat();
    for (body, cols) in [
        ("SELECT id FROM ev WHERE ts > NOW() - 3600000000", &["id"][..]),
        ("SELECT id FROM ev WHERE NOW() - 3600000000 < ts", &["id"]),
        ("SELECT id FROM ev WHERE ts <= CURRENT_TIMESTAMP", &["id"]),
        ("SELECT id, v FROM ev WHERE ts + 5 > NOW() AND v > 3", &["id", "v"]),
        ("SELECT * FROM ev WHERE ts > NOW() - 5", &["id", "ts", "v"]),
    ] {
        let chain = view(&cat, body);
        let clock = joins(&chain).into_iter().filter_map(|k| match k {
            JoinKind::Range { clock, .. } => clock,
            _ => None,
        });
        assert_eq!(clock.collect::<Vec<_>>(), [JoinSide::Right], "`{body}`");
        assert_eq!(visible(&chain), cols, "`{body}`");
    }
}

/// An unqualified name still resolves against the body's own relations alone:
/// the clock's `id` and `now` are no names.
#[test]
fn the_clock_takes_no_name() {
    let cat = cat();
    view(&cat, "SELECT id FROM ev WHERE ts > NOW() - 5");
    rejects(&cat, &[("SELECT now FROM ev WHERE ts > NOW() - 5", "now")]);
}

/// The time read off the clock under an equality keys an equi join, and a
/// grouped body reads it in its WHERE as any other does.
#[test]
fn now_under_equality_or_grouping() {
    let cat = cat();
    let chain = view(&cat, "SELECT id FROM ev WHERE ts = NOW()");
    assert!(joins(&chain).contains(&JoinKind::Equi));
    view(&cat, "SELECT COUNT(*) AS n FROM ev WHERE ts > NOW() - 5");
}

/// NOW() anywhere but one keyable WHERE conjunct of a view body is refused.
#[test]
fn now_is_refused_off_one_keyable_where_conjunct() {
    let cat = cat();
    rejects(
        &cat,
        &[
            ("SELECT id, NOW() AS t FROM ev", "only in a view body's WHERE"),
            (
                "SELECT id FROM ev WHERE ts > NOW() - 5 AND ts <= NOW()",
                "one WHERE conjunct",
            ),
            ("SELECT id FROM ev WHERE NOW() - ts < 5", "must be compared"),
            ("SELECT id FROM ev WHERE ts > NOW() OR v = 1", "must be compared"),
            (
                "SELECT id FROM ev WHERE ts BETWEEN NOW() - 5 AND NOW()",
                "one WHERE conjunct",
            ),
        ],
    );
    assert_rejects(
        "SELECT id FROM ev WHERE ts > NOW()",
        read(&cat, "SELECT id FROM ev WHERE ts > NOW()"),
        "only in a view body's WHERE",
    );
}
