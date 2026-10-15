//! The input read as `sqlparser` tokenizes it: where statements end, whether a
//! buffer is ready to run, and the spans the highlighter styles. The tokenizer
//! runs under [`gnitz_sql::DIALECT`], the dialect the planner parses with.

use std::ops::Range;

use gnitz_sql::sqlparser::keywords::Keyword;
use gnitz_sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer, Whitespace, Word};

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

/// Maps tokenizer `Location`s (1-based line, 1-based column in chars) to byte
/// offsets of the input. The tokens' locations only move forward, so it walks
/// the input once.
struct Offsets<'a> {
    src: &'a str,
    /// The last location mapped, and its byte.
    at: Location,
    byte: usize,
}

impl<'a> Offsets<'a> {
    fn new(src: &'a str) -> Self {
        Offsets {
            src,
            at: Location { line: 1, column: 1 },
            byte: 0,
        }
    }

    fn byte(&mut self, loc: Location) -> usize {
        let mut chars = self.src[self.byte..].char_indices();
        let base = self.byte;
        while (self.at.line, self.at.column) < (loc.line, loc.column) {
            let Some((i, c)) = chars.next() else {
                return self.src.len();
            };
            self.byte = base + i + c.len_utf8();
            match c {
                '\n' => self.at = Location { line: self.at.line + 1, column: 1 },
                _ => self.at.column += 1,
            }
        }
        self.byte
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
    let mut offsets = Offsets::new(src);
    let lexemes: Vec<Lexeme> = tokens
        .into_iter()
        .filter(|t| t.token != Token::EOF)
        .map(|TokenWithSpan { token, span }| Lexeme {
            token,
            range: offsets.byte(span.start)..offsets.byte(span.end),
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
pub const KEYWORDS: &[Keyword] = &[
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

/// Whether `w` is a word of the shell's SQL vocabulary: one of [`KEYWORDS`], a
/// word of a type name, or a calendar field.
pub fn is_keyword(w: &Word) -> bool {
    KEYWORDS.contains(&w.keyword)
        || gnitz_sql::type_names()
            .flat_map(str::split_whitespace)
            .chain(gnitz_sql::calendar_field_names())
            .any(|k| k.eq_ignore_ascii_case(&w.value))
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

/// One statement of a buffer: its text, without the terminating `;`.
pub fn statements(src: &str) -> Vec<Range<usize>> {
    let Some(lexemes) = lex(src) else {
        // The planner reports what does not tokenize.
        return std::iter::once(0..src.len()).collect();
    };
    let mut out = Vec::new();
    let mut start: Option<usize> = None;
    let mut end = 0;
    for l in &lexemes {
        if l.token == Token::SemiColon {
            if let Some(s) = start.take() {
                out.push(s..end);
            }
            continue;
        }
        if l.is_trivia() {
            continue;
        }
        start.get_or_insert(l.range.start);
        end = l.range.end;
    }
    if let Some(s) = start {
        out.push(s..end);
    }
    out
}

/// The byte just past the last top-level `;` of `src`: everything before it is
/// whole statements. `None` when there is none, or when `src` does not
/// tokenize because a string or comment is still open.
pub fn complete_prefix(src: &str) -> Option<usize> {
    let lexemes = lex(src)?;
    lexemes
        .iter()
        .rev()
        .find(|l| l.token == Token::SemiColon)
        .map(|l| l.range.end)
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

/// Whether a token is a comment.
pub fn is_comment(t: &Token) -> bool {
    matches!(
        t,
        Token::Whitespace(Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment(_))
    )
}

/// Turns a stream of input lines into the pieces the shell runs: a
/// meta-command line, or the SQL up to and including a line's last `;`.
#[derive(Default)]
pub struct Splitter {
    pending: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Piece {
    Sql(String),
    Meta(String),
}

impl Splitter {
    /// Feed one line, without its newline. Only a line that is a
    /// meta-command or holds a `;` can complete a piece, so only such a line
    /// re-reads what is pending.
    pub fn push_line(&mut self, line: &str) -> Option<Piece> {
        if line.trim_start().starts_with('\\') && is_blank(&self.pending) {
            self.pending.clear();
            return Some(Piece::Meta(line.trim().to_string()));
        }
        self.pending.push_str(line);
        self.pending.push('\n');
        if !line.contains(';') {
            return None;
        }
        let end = complete_prefix(&self.pending)?;
        let rest = self.pending.split_off(end);
        Some(Piece::Sql(std::mem::replace(&mut self.pending, rest)))
    }

    /// What is left at the end of the input: SQL with no closing `;` runs too.
    pub fn finish(&mut self) -> Option<Piece> {
        let rest = std::mem::take(&mut self.pending);
        (!is_blank(&rest)).then_some(Piece::Sql(rest))
    }
}

#[cfg(test)]
#[path = "tests/lex.rs"]
mod tests;
