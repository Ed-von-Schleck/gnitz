use super::*;
use crate::test_support::parse_stmt;

#[test]
fn validate_user_name_rejects_reserved_and_malformed() {
    // Leading `_` is reserved (system prefix + synthesized `__h…` views).
    assert!(matches!(validate_user_name("_hidden"), Err(GnitzSqlError::Plan(_))));
    assert!(matches!(validate_user_name("_seg4096"), Err(GnitzSqlError::Plan(_))));
    // Empty and illegal characters.
    assert!(matches!(validate_user_name(""), Err(GnitzSqlError::Plan(_))));
    assert!(matches!(validate_user_name("bad-name"), Err(GnitzSqlError::Plan(_))));
    assert!(matches!(validate_user_name("a.b"), Err(GnitzSqlError::Plan(_))));
    // Ordinary names — including an internal `_` — are accepted.
    assert!(validate_user_name("orders").is_ok());
    assert!(validate_user_name("my_view2").is_ok());
}

/// Only the plain `EXPLAIN <statement>` is honored; every option asks for a
/// rendering the output shape does not have, and is named in the rejection.
#[test]
fn explain_rejected_clause_matrix() {
    assert!(reject_unhonored_explain_clauses(&parse_stmt("EXPLAIN SELECT v FROM t"), "EXPLAIN").is_ok());
    for (sql, clause) in [
        ("EXPLAIN ANALYZE SELECT v FROM t", "ANALYZE"),
        ("EXPLAIN VERBOSE SELECT v FROM t", "VERBOSE"),
        ("EXPLAIN QUERY PLAN SELECT v FROM t", "QUERY PLAN"),
        ("EXPLAIN ESTIMATE SELECT v FROM t", "ESTIMATE"),
        ("EXPLAIN FORMAT JSON SELECT v FROM t", "FORMAT"),
        ("EXPLAIN (FORMAT JSON) SELECT v FROM t", "the parenthesized option list"),
    ] {
        match reject_unhonored_explain_clauses(&parse_stmt(sql), "EXPLAIN") {
            Err(GnitzSqlError::Unsupported(m)) => assert!(m.contains(clause), "`{sql}`: {m}"),
            other => panic!("`{sql}`: expected Unsupported naming {clause}, got {other:?}"),
        }
    }
}

#[test]
fn transaction_control_rejected_clause_matrix() {
    // BEGIN / START TRANSACTION: bare forms are honored; transaction modes,
    // a BEGIN modifier, and a BEGIN..END block are rejected.
    let begin_ok = ["BEGIN", "START TRANSACTION"];
    for sql in begin_ok {
        assert!(
            reject_unhonored_start_transaction_clauses(&parse_stmt(sql), "BEGIN").is_ok(),
            "{sql} should be honored"
        );
    }
    let begin_bad = [
        "BEGIN READ ONLY",
        "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "BEGIN DEFERRED",
    ];
    for sql in begin_bad {
        assert!(
            reject_unhonored_start_transaction_clauses(&parse_stmt(sql), "BEGIN").is_err(),
            "{sql} should be rejected"
        );
    }

    // COMMIT / END: bare forms honored; AND CHAIN rejected.
    assert!(reject_unhonored_commit_clauses(&parse_stmt("COMMIT"), "COMMIT").is_ok());
    assert!(reject_unhonored_commit_clauses(&parse_stmt("END"), "COMMIT").is_ok());
    assert!(reject_unhonored_commit_clauses(&parse_stmt("COMMIT AND CHAIN"), "COMMIT").is_err());

    // ROLLBACK: bare honored; AND CHAIN and TO SAVEPOINT rejected.
    assert!(reject_unhonored_rollback_clauses(&parse_stmt("ROLLBACK"), "ROLLBACK").is_ok());
    assert!(reject_unhonored_rollback_clauses(&parse_stmt("ROLLBACK AND CHAIN"), "ROLLBACK").is_err());
    assert!(reject_unhonored_rollback_clauses(&parse_stmt("ROLLBACK TO SAVEPOINT sp"), "ROLLBACK").is_err());
}

#[test]
fn alter_view_rejected_clause_matrix() {
    let guard = |sql: &str| alter_view_parts(&parse_stmt(sql), "ALTER VIEW").map(|_| ());
    assert!(guard("ALTER VIEW v AS SELECT a FROM t").is_ok());
    // The column list is consumed as positional output aliases, exactly as
    // `CREATE VIEW v (x, y) AS` is — the planner applies it and checks its arity.
    assert!(guard("ALTER VIEW v (x, y) AS SELECT a, b FROM t").is_ok());
    // `WITH (…)` parses here and is rejected: ALTER VIEW retargets a body, so
    // letting it set a budget would make the clause-less form silently drop one.
    assert!(guard("ALTER VIEW v WITH (security_barrier = true) AS SELECT a FROM t").is_err());
}
