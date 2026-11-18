use super::*;

fn styled(src: &str) -> Vec<(Style, String)> {
    let h = SqlHighlighter::default();
    h.spans(src, usize::MAX)
        .into_iter()
        .map(|(s, r)| (s, src[r].to_string()))
        .collect()
}

fn style_of(src: &str, text: &str) -> Style {
    styled(src)
        .into_iter()
        .find(|(_, t)| t == text)
        .unwrap_or_else(|| panic!("{text} in {src}"))
        .0
}

#[test]
fn spans_cover_the_input_in_order() {
    for src in [
        "SELECT a, 'x' FROM t -- c",
        "SELECT 'open",
        "\\watch SELECT * FROM v",
        "  \\dt foo",
        "/* open",
    ] {
        let h = SqlHighlighter::default();
        let mut at = 0;
        for (_, r) in h.spans(src, 0) {
            assert_eq!(r.start, at, "{src}");
            at = r.end;
        }
        assert_eq!(at, src.len(), "{src}");
    }
}

#[test]
fn token_classes_get_their_styles() {
    let p = SqlHighlighter::default();
    let src = "SELECT upper(name), 42, 'x', t.select FROM t -- note";
    assert_eq!(style_of(src, "SELECT"), p.keyword);
    assert_eq!(style_of(src, "upper"), p.function);
    assert_eq!(style_of(src, "42"), p.number);
    assert_eq!(style_of(src, "'x'"), p.string);
    // A keyword spelled as a qualified column is an identifier.
    assert_eq!(style_of(src, "select"), p.identifier);
    assert_eq!(style_of(src, "-- note"), p.comment);
}

#[test]
fn an_unterminated_string_reads_as_open() {
    assert_eq!(style_of("SELECT 'abc", "'abc"), SqlHighlighter::default().open);
}

#[test]
fn a_meta_command_word_is_styled_and_watch_reads_sql() {
    let p = SqlHighlighter::default();
    assert_eq!(style_of("\\dt t*", "\\dt"), p.meta);
    assert_eq!(style_of("\\watch SELECT * FROM v", "SELECT"), p.keyword);
}

/// Where the bracket style lands with the cursor at `cursor`.
fn marked(src: &str, cursor: usize) -> Vec<usize> {
    let h = SqlHighlighter::default();
    h.spans(src, cursor)
        .into_iter()
        .filter(|(s, _)| *s == h.bracket)
        .map(|(_, r)| r.start)
        .collect()
}

#[test]
fn the_bracket_at_the_cursor_and_its_partner_are_marked() {
    let src = "SELECT f((1), 2)";
    let (open, close) = (src.find('(').unwrap(), src.len() - 1);
    assert_eq!(marked(src, open), [open, close]);
    assert_eq!(marked(src, src.len()), [open, close]);
    assert!(marked("SELECT (1", 7).is_empty());
}

#[test]
fn only_the_shell_s_vocabulary_is_a_keyword() {
    let p = SqlHighlighter::default();
    let src = "SELECT id, name FROM t";
    assert_eq!(style_of(src, "id"), p.identifier);
    assert_eq!(style_of(src, "name"), p.identifier);
    assert_eq!(style_of("CREATE TABLE t (a BIGINT)", "BIGINT"), p.keyword);
    assert_eq!(style_of("CREATE TABLE t (a bigint)", "bigint"), p.keyword);
    assert_eq!(style_of("SELECT 1 = ANY (SELECT 1)", "ANY"), p.keyword);
    assert_eq!(style_of("SELECT EXTRACT(year FROM d) FROM t", "year"), p.keyword);
}

#[test]
fn every_string_literal_the_dialect_reads_is_a_string() {
    let p = SqlHighlighter::default();
    let src = "SELECT r'a', q'[b]', E'c', X'0f'";
    for lit in ["r'a'", "q'[b]'", "E'c'", "X'0f'"] {
        assert_eq!(style_of(src, lit), p.string, "{lit}");
    }
}

#[test]
fn copy_data_and_a_comment_hint_are_styled_in_place() {
    for src in ["COPY t FROM STDIN;\nSELECT 1 FROM t;", "SELECT 1 /*!+ 1*/"] {
        let text: String = styled(src).into_iter().map(|(_, t)| t).collect();
        assert_eq!(text, src);
    }
}
