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
    // `E'…'` parses only in an arm gated on `GenericDialect`'s type id.
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

/// Every method `GenericDialect` overrides is overridden by `GnitzDialect`
/// too, so none falls back to the trait default. The overrides are read from
/// the source of the sqlparser this crate resolves.
#[test]
fn overrides_every_generic_dialect_method() {
    let out = std::process::Command::new(env!("CARGO"))
        .args(["metadata", "--format-version=1", "--offline", "--manifest-path"])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let meta: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let manifest = meta["packages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "sqlparser")
        .and_then(|p| p["manifest_path"].as_str())
        .unwrap();
    let generic = std::path::Path::new(manifest).with_file_name("src/dialect/generic.rs");
    let generic = std::fs::read_to_string(generic).unwrap();
    let generic = &generic[generic.find("impl Dialect for GenericDialect").unwrap()..];

    let ours: Vec<&str> = include_str!("../dialect.rs")
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .collect();
    let missing: Vec<&str> = generic
        .split("fn ")
        .skip(1)
        .map(|s| s.split('(').next().unwrap())
        .filter(|m| !ours.contains(m))
        .collect();
    assert!(missing.is_empty(), "not forwarded: {missing:?}");
}
