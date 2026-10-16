use super::*;

#[test]
fn each_statement_keeps_its_own_text() {
    let script = "SELECT 1;\n  CREATE VIEW \"ü\" AS SELECT 'ä;' AS a ;;SELECT 2";
    let texts: Vec<&str> = split_statements(script)
        .unwrap()
        .iter()
        .map(|(_, t)| t.text())
        .collect();
    assert_eq!(texts, ["SELECT 1", "CREATE VIEW \"ü\" AS SELECT 'ä;' AS a", "SELECT 2"]);
}

#[test]
fn statements_run_together_without_a_delimiter_are_refused() {
    assert!(split_statements("SELECT 1 SELECT 2").is_err());
}

#[test]
fn a_view_body_starts_after_the_first_top_level_as() {
    let cases = [
        ("CREATE VIEW v AS SELECT a AS b FROM t", "SELECT a AS b FROM t"),
        (
            "CREATE VIEW v (x) WITH (delta = '1 MB') AS\n  (SELECT a FROM t)",
            "(SELECT a FROM t)",
        ),
        ("CREATE OR REPLACE VIEW \"as\" AS SELECT 1 AS k", "SELECT 1 AS k"),
        (
            "ALTER VIEW v AS SELECT a FROM t WHERE s LIKE 'a%' ESCAPE '!'",
            "SELECT a FROM t WHERE s LIKE 'a%' ESCAPE '!'",
        ),
    ];
    for (script, body) in cases {
        let (_, text) = split_statements(script).unwrap().remove(0);
        let range = text.body_after_as().unwrap();
        assert_eq!(&script[range], body, "{script}");
    }
}

#[test]
fn a_location_maps_to_its_byte_offset_across_lines_and_wide_characters() {
    let script = "SELECT 'ä'\n, x FROM \"ü\".t";
    let text = StatementText::whole(script);
    let at = |line, column| text.offset(Location { line, column }).unwrap();
    assert_eq!(&script[at(1, 9)..], "ä'\n, x FROM \"ü\".t");
    assert_eq!(&script[at(2, 10)..], "\"ü\".t");
    assert_eq!(at(2, 15), script.len());
    assert_eq!(text.offset(Location { line: 2, column: 16 }), None);
    assert_eq!(text.offset(Location { line: 3, column: 1 }), None);
    // A line's end past its last column is no location of it.
    assert_eq!(text.offset(Location { line: 1, column: 12 }), None);
    let mut offsets = Offsets::new(script);
    assert_eq!(offsets.byte(Location { line: 2, column: 10 }), Some(at(2, 10)));
    assert_eq!(offsets.byte(Location { line: 1, column: 9 }), Some(at(1, 9)));
}
