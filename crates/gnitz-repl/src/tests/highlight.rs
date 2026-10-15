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
    let p = Palette::default();
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
    assert_eq!(style_of("SELECT 'abc", "'abc"), Palette::default().open);
}

#[test]
fn a_meta_command_word_is_styled_and_watch_reads_sql() {
    let p = Palette::default();
    assert_eq!(style_of("\\dt t*", "\\dt"), p.meta);
    assert_eq!(style_of("\\watch SELECT * FROM v", "SELECT"), p.keyword);
}

#[test]
fn the_bracket_at_the_cursor_and_its_partner_are_marked() {
    let src = "SELECT f((1), 2)";
    let h = SqlHighlighter::default();
    let open = src.find('(').unwrap();
    let marked: Vec<usize> = h
        .spans(src, open)
        .into_iter()
        .filter(|(s, _)| *s == Palette::default().bracket)
        .map(|(_, r)| r.start)
        .collect();
    assert_eq!(marked, vec![open, src.len() - 1]);
}

#[test]
fn only_the_shell_s_vocabulary_is_a_keyword() {
    let p = Palette::default();
    let src = "SELECT id, name FROM t";
    assert_eq!(style_of(src, "id"), p.identifier);
    assert_eq!(style_of(src, "name"), p.identifier);
    assert_eq!(style_of("CREATE TABLE t (a BIGINT)", "BIGINT"), p.keyword);
    assert_eq!(style_of("CREATE TABLE t (a bigint)", "bigint"), p.keyword);
    assert_eq!(style_of("SELECT 1 = ANY (SELECT 1)", "ANY"), p.keyword);
    assert_eq!(style_of("SELECT EXTRACT(year FROM d) FROM t", "year"), p.keyword);
}
