//! The input read as `sqlparser` tokenizes it: where statements end, whether a
//! buffer is ready to run, and the spans the highlighter styles. The tokenizer
//! runs under [`gnitz_sql::DIALECT`], the dialect the planner parses with.

use std::ops::Range;

use gnitz_sql::sqlparser::keywords::Keyword;
use gnitz_sql::sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer, Whitespace, Word};

/// A token and the bytes of the input it covers.
#[derive(Debug, Clone)]
pub struct Lexeme {
    pub token: Token,
    pub range: Range<usize>,
}

impl Lexeme {
    pub fn is_trivia(&self) -> bool {
        matches!(self.token, Token::Whitespace(_))
    }
}

/// The lexemes of `src` as far as it tokenizes, and the byte the rest starts
/// at: `src.len()` when it all does, else where an unterminated string or
/// comment begins.
pub fn lex_prefix(src: &str) -> (Vec<Lexeme>, usize) {
    let mut tokens: Vec<TokenWithSpan> = Vec::new();
    let whole = Tokenizer::new(&gnitz_sql::DIALECT, src)
        .tokenize_with_location_into_buf(&mut tokens)
        .is_ok();
    let mut offsets = gnitz_sql::Offsets::new(src);
    // A span the tokenizer reports lies in `src`.
    let mut byte = |loc| offsets.byte(loc).unwrap_or(src.len());
    let lexemes: Vec<Lexeme> = tokens
        .into_iter()
        .filter(|t| t.token != Token::EOF)
        .map(|TokenWithSpan { token, span }| Lexeme {
            token,
            range: byte(span.start)..byte(span.end),
        })
        .collect();
    let end = match whole {
        true => src.len(),
        false => lexemes.last().map_or(0, |l| l.range.end),
    };
    (lexemes, end)
}

/// `src` tokenized, or `None` when a string or comment is left open.
pub fn lex(src: &str) -> Option<Vec<Lexeme>> {
    let (lexemes, end) = lex_prefix(src);
    (end == src.len()).then_some(lexemes)
}

/// The statement and clause words the shell styles and completes as keywords,
/// beside the planner's type names and calendar fields. A function spelled as
/// a keyword (`CAST`, `EXTRACT`) is a function name instead.
const KEYWORDS: &[Keyword] = &[
    Keyword::ACTION,
    Keyword::ADD,
    Keyword::ALL,
    Keyword::ALTER,
    Keyword::AND,
    Keyword::ANY,
    Keyword::AS,
    Keyword::ASC,
    Keyword::BEGIN,
    Keyword::BETWEEN,
    Keyword::BOTH,
    Keyword::BY,
    Keyword::CASCADE,
    Keyword::CASE,
    Keyword::CLUSTER,
    Keyword::COLUMN,
    Keyword::COMMIT,
    Keyword::CONFLICT,
    Keyword::CONSTRAINT,
    Keyword::CREATE,
    Keyword::CROSS,
    Keyword::CURRENT,
    Keyword::DELETE,
    Keyword::DESC,
    Keyword::DESCRIBE,
    Keyword::DISTINCT,
    Keyword::DO,
    Keyword::DROP,
    Keyword::ELSE,
    Keyword::END,
    Keyword::ESCAPE,
    Keyword::EXCEPT,
    Keyword::EXCLUDE,
    Keyword::EXISTS,
    Keyword::EXPLAIN,
    Keyword::FALSE,
    Keyword::FIRST,
    Keyword::FOLLOWING,
    Keyword::FOR,
    Keyword::FOREIGN,
    Keyword::FROM,
    Keyword::FULL,
    Keyword::GROUP,
    Keyword::HAVING,
    Keyword::IF,
    Keyword::ILIKE,
    Keyword::IN,
    Keyword::INDEX,
    Keyword::INNER,
    Keyword::INSERT,
    Keyword::INTERSECT,
    Keyword::INTO,
    Keyword::IS,
    Keyword::JOIN,
    Keyword::KEY,
    Keyword::LAST,
    Keyword::LEADING,
    Keyword::LEFT,
    Keyword::LIKE,
    Keyword::LIMIT,
    Keyword::MATERIALIZED,
    Keyword::NATURAL,
    Keyword::NO,
    Keyword::NOT,
    Keyword::NOTHING,
    Keyword::NULL,
    Keyword::NULLS,
    Keyword::OFFSET,
    Keyword::ON,
    Keyword::OR,
    Keyword::ORDER,
    Keyword::OUTER,
    Keyword::OVER,
    Keyword::PARTITION,
    Keyword::PRECEDING,
    Keyword::PRIMARY,
    Keyword::QUALIFY,
    Keyword::RANGE,
    Keyword::REFERENCES,
    Keyword::RENAME,
    Keyword::REPLACE,
    Keyword::RESTRICT,
    Keyword::RETURNING,
    Keyword::RIGHT,
    Keyword::ROLLBACK,
    Keyword::ROW,
    Keyword::ROWS,
    Keyword::SELECT,
    Keyword::SET,
    Keyword::SOME,
    Keyword::START,
    Keyword::TABLE,
    Keyword::THEN,
    Keyword::TO,
    Keyword::TRAILING,
    Keyword::TRANSACTION,
    Keyword::TRUE,
    Keyword::UNBOUNDED,
    Keyword::UNION,
    Keyword::UNIQUE,
    Keyword::UNKNOWN,
    Keyword::UPDATE,
    Keyword::USING,
    Keyword::VALUES,
    Keyword::VIEW,
    Keyword::WHEN,
    Keyword::WHERE,
    Keyword::WINDOW,
    Keyword::WITH,
];

/// The words of the shell's SQL vocabulary besides [`KEYWORDS`]: each word of a
/// type name, and the calendar fields.
fn planner_words() -> impl Iterator<Item = &'static str> {
    gnitz_sql::type_names()
        .flat_map(str::split_whitespace)
        .chain(gnitz_sql::calendar_field_names())
}

/// Every word of the shell's SQL vocabulary, as completion offers it: one word
/// at a time, so a type spelled in several is offered word by word.
pub fn vocabulary() -> impl Iterator<Item = String> {
    KEYWORDS
        .iter()
        .map(Keyword::to_string)
        .chain(planner_words().map(str::to_string))
}

/// Whether `w` is a word of the shell's SQL [`vocabulary`].
pub fn is_keyword(w: &Word) -> bool {
    KEYWORDS.contains(&w.keyword) || planner_words().any(|k| k.eq_ignore_ascii_case(&w.value))
}

/// Where the SQL word `before` ends in starts: the chain of adjacent words and
/// `.`s at its end, a quoted name being typed, or an empty word at its end.
/// `None` where no word is being typed: inside a string, comment or a quoted
/// name after a qualifier, or at a number.
pub fn word_at(before: &str) -> Option<usize> {
    let (lexemes, lexed) = lex_prefix(before);
    if lexed < before.len() {
        let tail = &before[lexed..];
        let qualified = lexemes
            .last()
            .is_some_and(|l| l.token == Token::Period && l.range.end == lexed);
        return (tail.starts_with('"') && !tail.contains(char::is_whitespace) && !qualified).then_some(lexed);
    }
    let Some(last) = lexemes.last() else {
        return Some(before.len());
    };
    match &last.token {
        Token::Word(_) | Token::Period => {
            let mut start = last.range.start;
            for l in lexemes.iter().rev().skip(1) {
                if l.range.end != start || !matches!(l.token, Token::Word(_) | Token::Period) {
                    break;
                }
                start = l.range.start;
            }
            Some(start)
        }
        Token::Number(..) => None,
        Token::Whitespace(Whitespace::SingleLineComment { .. }) if !before.ends_with('\n') => None,
        _ => Some(before.len()),
    }
}

/// Whether `src` holds nothing but whitespace and comments.
pub fn is_blank(src: &str) -> bool {
    match lex(src) {
        Some(lexemes) => lexemes.iter().all(Lexeme::is_trivia),
        None => src.trim().is_empty(),
    }
}

/// Whether an interactive buffer is ready to run: a meta-command, an exit word,
/// or SQL whose last token is a `;`.
pub fn is_complete(src: &str) -> bool {
    let trimmed = src.trim();
    if trimmed.is_empty() || trimmed.starts_with('\\') || is_exit_word(trimmed) {
        return true;
    }
    let Some(lexemes) = lex(src) else {
        return false;
    };
    lexemes
        .iter()
        .rev()
        .find(|l| !l.is_trivia())
        .is_some_and(|l| l.token == Token::SemiColon)
}

/// `exit` and `quit`, which a shell user types expecting to leave.
pub fn is_exit_word(s: &str) -> bool {
    let s = s.trim().trim_end_matches(';').trim();
    s.eq_ignore_ascii_case("exit") || s.eq_ignore_ascii_case("quit")
}

/// Turns a stream of input lines into the pieces the shell runs: a
/// meta-command line, or one statement.
#[derive(Default)]
pub struct Splitter {
    pending: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Piece {
    /// One statement, without its `;`.
    Sql(String),
    Meta(String),
}

impl Splitter {
    /// Feed one line, without its newline: the pieces it completes. Only a
    /// line that is a meta-command or holds a `;` can complete one, so only
    /// such a line re-reads what is pending.
    pub fn push_line(&mut self, line: &str) -> Vec<Piece> {
        if line.trim_start().starts_with('\\') && is_blank(&self.pending) {
            self.pending.clear();
            return vec![Piece::Meta(line.trim().to_string())];
        }
        self.pending.push_str(line);
        self.pending.push('\n');
        if !line.contains(';') {
            return Vec::new();
        }
        let Some(lexemes) = lex(&self.pending) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let done = split(&self.pending, &lexemes, false, &mut out);
        self.pending.drain(..done);
        out
    }

    /// What is left at the end of the input: SQL with no closing `;` runs too.
    pub fn finish(&mut self) -> Vec<Piece> {
        let rest = std::mem::take(&mut self.pending);
        let mut out = Vec::new();
        match lex(&rest) {
            Some(lexemes) => {
                split(&rest, &lexemes, true, &mut out);
            }
            // The planner reports what does not tokenize.
            None => out.push(Piece::Sql(rest)),
        }
        out
    }
}

/// Push each statement of `src` that a top-level `;` ends onto `out`, and
/// with `tail` the one after the last `;` too; a statement of nothing but
/// whitespace and comments is none. The byte just past the last `;`, or 0.
fn split(src: &str, lexemes: &[Lexeme], tail: bool, out: &mut Vec<Piece>) -> usize {
    let mut start: Option<usize> = None;
    let mut end = 0;
    let mut done = 0;
    for l in lexemes {
        if l.token == Token::SemiColon {
            if let Some(s) = start.take() {
                out.push(Piece::Sql(src[s..end].to_string()));
            }
            done = l.range.end;
        } else if !l.is_trivia() {
            start.get_or_insert(l.range.start);
            end = l.range.end;
        }
    }
    if let Some(s) = start.filter(|_| tail) {
        out.push(Piece::Sql(src[s..end].to_string()));
    }
    done
}

#[cfg(test)]
#[path = "tests/lex.rs"]
mod tests;
