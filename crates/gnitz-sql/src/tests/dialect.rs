use super::*;
use sqlparser::tokenizer::Tokenizer;

const CORPUS: &[&str] = &[
    "INSERT INTO t VALUES (1, -2, 3.25, -0.5, 1e3, 'a', '', 'it''s', NULL), (2, +7, .5, 'b', 'x', 'y', 'z', 'w', NULL)",
    "INSERT INTO s.t (a, b) VALUES (9223372036854775807, -9223372036854775808)",
    "SELECT a, 'lit', 42 FROM t WHERE a = 1 AND b <> 'x' AND c IN (1, 2, 3) AND d BETWEEN 10 AND 20",
    "SELECT * FROM t WHERE s IN ('a', 'b') OR n NOT BETWEEN -1 AND 1 LIMIT 10 OFFSET 5",
    "UPDATE t SET a = 5, b = 'new' WHERE id = 3",
    "DELETE FROM t WHERE id IN (1, 2) OR name = 'gone'",
    "SELECT DATE '2020-01-01', TIMESTAMP '2020-01-01 10:00:00', INTERVAL '1' DAY, INTERVAL '2 hours'",
    "SELECT CASE WHEN a > 1 THEN 'big' WHEN a = 1 THEN 'one' ELSE 'small' END FROM t",
    "SELECT 'a' || 'b', 'x' COLLATE \"C\", '1'::BIGINT, CAST('2' AS INT), 3::TEXT FROM t",
    "SELECT 'a' 'b', E'esc\\n', N'nat', X'ff', $1, ? FROM t",
    "SELECT -1, - 1, +'2', NOT 'a' LIKE 'b', 1 + 2 * 3 FROM t",
    "CREATE TABLE t (id BIGINT PRIMARY KEY, kind BIGINT, body TEXT DEFAULT 'none', n INT DEFAULT 0) WITH (stream = true)",
    "CREATE VIEW recent WITH (capacity = '4 MB') AS SELECT id, body FROM messages WHERE kind = 3",
    "CREATE VIEW d WITH (delta = '32 MB') AS SELECT kind, SUM(amount) AS total FROM events GROUP BY kind HAVING SUM(amount) > 100",
    "SELECT a FROM t ORDER BY 1 DESC NULLS LAST; SELECT 1",
];

/// Parsing under `GnitzDialect` yields the same statements, spans included,
/// as under `GenericDialect`.
#[test]
fn parses_as_generic_dialect() {
    for sql in CORPUS {
        let ours = Parser::parse_sql(&GnitzDialect, sql).unwrap();
        let generic = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        assert_eq!(format!("{ours:?}"), format!("{generic:?}"), "{sql}");
    }
}

/// A parse error reads the same under both dialects.
#[test]
fn fails_as_generic_dialect() {
    for sql in ["SELECT 1 +", "INSERT INTO t VALUES ('a' 1)", "SELECT 'x' 'y' z w"] {
        let ours = Parser::parse_sql(&GnitzDialect, sql).map(|s| format!("{s:?}"));
        let generic = Parser::parse_sql(&GenericDialect {}, sql).map(|s| format!("{s:?}"));
        assert_eq!(ours, generic, "{sql}");
    }
}

/// Tokenizing under `GnitzDialect` yields the same tokens and locations as
/// under `GenericDialect`.
#[test]
fn tokenizes_as_generic_dialect() {
    let src = "SELECT \"Q\", `b`, _x, #t, @v, a$1, ü, 'it''s', 1.5e3 FROM s.t -- c\n/* /* nested */ */ WHERE x <> 2;";
    let ours = Tokenizer::new(&GnitzDialect, src).tokenize_with_location().unwrap();
    let generic = Tokenizer::new(&GenericDialect {}, src)
        .tokenize_with_location()
        .unwrap();
    assert_eq!(format!("{ours:?}"), format!("{generic:?}"));
}
