use super::*;

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

fn first_stmt(sql: &str) -> sqlparser::ast::Statement {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}

#[test]
fn transaction_control_rejected_clause_matrix() {
    // BEGIN / START TRANSACTION: bare forms are honored; transaction modes,
    // a BEGIN modifier, and a BEGIN..END block are rejected.
    let begin_ok = ["BEGIN", "START TRANSACTION"];
    for sql in begin_ok {
        assert!(
            reject_unhonored_start_transaction_clauses(&first_stmt(sql), "BEGIN").is_ok(),
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
            reject_unhonored_start_transaction_clauses(&first_stmt(sql), "BEGIN").is_err(),
            "{sql} should be rejected"
        );
    }

    // COMMIT / END: bare forms honored; AND CHAIN rejected.
    assert!(reject_unhonored_commit_clauses(&first_stmt("COMMIT"), "COMMIT").is_ok());
    assert!(reject_unhonored_commit_clauses(&first_stmt("END"), "COMMIT").is_ok());
    assert!(reject_unhonored_commit_clauses(&first_stmt("COMMIT AND CHAIN"), "COMMIT").is_err());

    // ROLLBACK: bare honored; AND CHAIN and TO SAVEPOINT rejected.
    assert!(reject_unhonored_rollback_clauses(&first_stmt("ROLLBACK"), "ROLLBACK").is_ok());
    assert!(reject_unhonored_rollback_clauses(&first_stmt("ROLLBACK AND CHAIN"), "ROLLBACK").is_err());
    assert!(reject_unhonored_rollback_clauses(&first_stmt("ROLLBACK TO SAVEPOINT sp"), "ROLLBACK").is_err());
}
