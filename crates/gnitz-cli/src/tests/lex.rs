use super::*;

/// The pieces of `lines` fed one at a time, then finished.
fn pieces(lines: &[&str]) -> Vec<Piece> {
    let mut s = Splitter::default();
    let mut out: Vec<Piece> = lines.iter().flat_map(|l| s.push_line(l)).collect();
    out.extend(s.finish());
    out
}

fn sql(texts: &[&str]) -> Vec<Piece> {
    texts.iter().map(|t| Piece::Sql(t.to_string())).collect()
}

#[test]
fn statements_split_on_top_level_semicolons_only() {
    assert_eq!(
        pieces(&["SELECT 'a;b' FROM t; -- c; d", "INSERT INTO t VALUES (1) ;  "]),
        sql(&["SELECT 'a;b' FROM t", "INSERT INTO t VALUES (1)"])
    );
}

#[test]
fn statements_skip_empty_ones_and_keep_a_trailing_one() {
    assert_eq!(pieces(&[";; SELECT 1; SELECT 2"]), sql(&["SELECT 1", "SELECT 2"]));
}

#[test]
fn input_that_does_not_tokenize_runs_whole_for_the_planner_to_report() {
    assert_eq!(pieces(&["SELECT 1; SELECT 'open"]), sql(&["SELECT 1; SELECT 'open\n"]));
}

#[test]
fn a_buffer_is_complete_once_its_last_token_is_a_semicolon() {
    assert!(is_complete("SELECT 1;"));
    assert!(is_complete("SELECT 1; -- trailing comment"));
    assert!(is_complete("\\dt"));
    assert!(is_complete("quit"));
    assert!(is_complete("   "));
    assert!(!is_complete("SELECT 1"));
    assert!(!is_complete("SELECT ';"));
    assert!(!is_complete("SELECT 1; SELECT"));
    assert!(!is_complete("SELECT 1 /* ; */"));
}

#[test]
fn the_splitter_runs_meta_lines_alone_and_sql_at_its_semicolon() {
    let mut s = Splitter::default();
    assert_eq!(s.push_line("\\dt"), [Piece::Meta("\\dt".to_string())]);
    assert_eq!(s.push_line("SELECT a,"), []);
    // A backslash inside a statement is SQL, not a meta-command.
    assert_eq!(s.push_line("  b FROM t WHERE x = '\\n'"), []);
    assert_eq!(s.push_line("\\dt"), []);
    assert_eq!(
        s.push_line("; SELECT"),
        sql(&["SELECT a,\n  b FROM t WHERE x = '\\n'\n\\dt"])
    );
    assert_eq!(s.finish(), sql(&["SELECT"]));
    assert_eq!(s.finish(), []);
}

#[test]
fn a_semicolon_inside_an_open_string_ends_nothing() {
    let mut s = Splitter::default();
    assert_eq!(s.push_line("SELECT 'a;"), []);
    assert_eq!(s.push_line("b' AS x;"), sql(&["SELECT 'a;\nb' AS x"]));
}

#[test]
fn a_comment_only_remainder_runs_nothing() {
    let mut s = Splitter::default();
    assert_eq!(s.push_line("SELECT 1;"), sql(&["SELECT 1"]));
    assert_eq!(s.push_line("-- done"), []);
    assert_eq!(s.finish(), []);
}

#[test]
fn a_meta_line_after_a_finished_statement_and_a_comment_is_a_meta_command() {
    assert_eq!(
        pieces(&["SELECT 1; -- note", "\\x"]),
        [Piece::Sql("SELECT 1".to_string()), Piece::Meta("\\x".to_string())]
    );
}

#[test]
fn lex_prefix_stops_at_an_unterminated_string() {
    let src = "SELECT 'abc";
    let (lexemes, end) = lex_prefix(src);
    assert_eq!(&src[end..], "'abc");
    assert!(lexemes.iter().all(|l| l.range.end <= end));
}

#[test]
fn lexeme_ranges_are_byte_ranges_past_multibyte_text() {
    let src = "SELECT 'ünï' AS x";
    let lexemes = lex(src).expect("tokenizes");
    let last = lexemes.last().unwrap();
    assert_eq!(&src[last.range.clone()], "x");
}

#[test]
fn exit_words_ignore_case_and_a_semicolon() {
    assert!(is_exit_word("EXIT;"));
    assert!(is_exit_word(" quit "));
    assert!(!is_exit_word("exits"));
}

#[test]
fn lex_prefix_stops_at_an_unterminated_comment() {
    let src = "SELECT 1 /* open";
    let (_, end) = lex_prefix(src);
    assert_eq!(&src[end..], "/* open");
}
