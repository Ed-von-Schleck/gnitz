use super::*;

#[test]
fn statements_split_on_top_level_semicolons_only() {
    let src = "SELECT 'a;b' FROM t; -- c; d\nINSERT INTO t VALUES (1) ;  ";
    let parts: Vec<&str> = statements(src).into_iter().map(|r| &src[r]).collect();
    assert_eq!(parts, ["SELECT 'a;b' FROM t", "INSERT INTO t VALUES (1)"]);
}

#[test]
fn statements_skip_empty_ones_and_keep_a_trailing_one() {
    let src = ";; SELECT 1; SELECT 2";
    let parts: Vec<&str> = statements(src).into_iter().map(|r| &src[r]).collect();
    assert_eq!(parts, ["SELECT 1", "SELECT 2"]);
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
fn complete_prefix_ends_after_the_last_semicolon() {
    assert_eq!(complete_prefix("SELECT 1; SELECT"), Some(9));
    assert_eq!(complete_prefix("SELECT 'x;"), None);
    assert_eq!(complete_prefix("SELECT 1"), None);
}

#[test]
fn the_splitter_runs_meta_lines_alone_and_sql_at_its_semicolon() {
    let mut s = Splitter::default();
    assert_eq!(s.push_line("\\dt"), Some(Piece::Meta("\\dt".to_string())));
    assert_eq!(s.push_line("SELECT a,"), None);
    // A backslash inside a statement is SQL, not a meta-command.
    assert_eq!(s.push_line("  b FROM t WHERE x = '\\n'"), None);
    assert_eq!(s.push_line("\\dt"), None);
    assert_eq!(
        s.push_line("; SELECT"),
        Some(Piece::Sql("SELECT a,\n  b FROM t WHERE x = '\\n'\n\\dt\n;".to_string()))
    );
    assert_eq!(s.finish(), Some(Piece::Sql(" SELECT\n".to_string())));
    assert_eq!(s.finish(), None);
}

#[test]
fn a_semicolon_inside_an_open_string_ends_nothing() {
    let mut s = Splitter::default();
    assert_eq!(s.push_line("SELECT 'a;"), None);
    assert_eq!(
        s.push_line("b' AS x;"),
        Some(Piece::Sql("SELECT 'a;\nb' AS x;".to_string()))
    );
}

#[test]
fn a_comment_only_remainder_runs_nothing() {
    let mut s = Splitter::default();
    assert!(s.push_line("SELECT 1;").is_some());
    assert_eq!(s.push_line("-- done"), None);
    assert_eq!(s.finish(), None);
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
