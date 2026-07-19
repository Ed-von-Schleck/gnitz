#![cfg(feature = "integration")]

//! Semantics rejection pins for scalar / quantified subquery CREATE VIEW bodies,
//! authored against the *current* compiler on the Rust surface — the exact inner
//! `String` **and** the `GnitzSqlError` variant.
//!
//! Several of these sit on a bare `raises(Exception)` in the Python e2e today
//! (`test_scalar_subquery.py::test_rejections`), so a migration that keeps them
//! rejected via a *different* path or message would pass there silently — the
//! exact guard-identity is pinned here.

use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

fn setup_ab(client: &mut gnitz_core::GnitzClient, sn: &str) {
    exec(
        client,
        sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
    );
    exec(
        client,
        sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL)",
    );
}

/// A projected non-aggregate scalar subquery is rejected: the projection must be
/// a single aggregate over the correlation group (`classify_aggregate`).
#[test]
fn non_aggregate_scalar_subquery_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_ab(&mut client, &sn);
    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE VIEW bad AS SELECT a.id, (SELECT b.w FROM b WHERE b.k = a.k) FROM a",
        "Unsupported",
        "a scalar subquery must be a single aggregate over its correlation group",
    );
}

/// An uncorrelated scalar aggregate subquery in projection position is rejected —
/// only a top-level WHERE comparison conjunct supports it (`substitute_scalar`).
#[test]
fn uncorrelated_scalar_projected_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_ab(&mut client, &sn);
    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE VIEW bad AS SELECT a.id, (SELECT COUNT(*) FROM b) FROM a",
        "Unsupported",
        "an uncorrelated scalar aggregate subquery is only supported as a top-level WHERE",
    );
}

/// `= ALL (SELECT …)` is unsupported (`reject_unsupported_quantifier`). Today a
/// bare `raises(Exception)` — pin the exact inner String a migration must
/// reproduce.
#[test]
fn eq_all_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_ab(&mut client, &sn);
    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE VIEW bad AS SELECT a.v FROM a WHERE a.k = ALL (SELECT k FROM b)",
        "Unsupported",
        "`= ALL (SELECT …)` is not supported",
    );
}

/// `<> ANY (SELECT …)` is unsupported (`reject_unsupported_quantifier`).
#[test]
fn neq_any_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_ab(&mut client, &sn);
    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE VIEW bad AS SELECT a.v FROM a WHERE a.k <> ANY (SELECT k FROM b)",
        "Unsupported",
        "`<> ANY (SELECT …)` is not supported",
    );
}

/// An uncorrelated range ALL (`v < ALL (…)`) is unsupported (empty-set-TRUE would
/// need a keyless preserved join).
#[test]
fn uncorrelated_range_all_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_ab(&mut client, &sn);
    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE VIEW bad AS SELECT a.v FROM a WHERE a.v < ALL (SELECT w FROM b)",
        "Unsupported",
        "uncorrelated range ALL is unsupported",
    );
}

/// An uncorrelated scalar with a `<>` comparison is rejected (`<>` is neither the
/// equi nor a range join predicate the uncorrelated arm supports).
#[test]
fn uncorrelated_neq_comparison_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    setup_ab(&mut client, &sn);
    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE VIEW bad AS SELECT a.id FROM a WHERE a.v <> (SELECT COUNT(*) FROM b)",
        "Unsupported",
        "an uncorrelated scalar aggregate subquery is only supported as a top-level WHERE",
    );
}

/// A nullable IN in a **mark** position (under OR) is rejected: a migration can
/// change which guard fires, so the message is pinned; the Python pin is a broad
/// `raises(Exception)`.
#[test]
fn nullable_in_mark_position_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    // `a.k` nullable so the IN operand is nullable; the OR puts the IN in a mark
    // position (not a top-level AND conjunct).
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, v BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
    );
    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE VIEW bad AS SELECT id FROM a WHERE a.k IN (SELECT k FROM b) OR a.v = 1",
        "Unsupported",
        "IN (SELECT …) in a mark position",
    );
}
