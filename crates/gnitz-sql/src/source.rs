//! Statement text: a script split into its statements, each with the exact text
//! it was parsed from, and the conversion from the parser's line/column
//! locations to byte offsets in that text.

use std::ops::Range;

use sqlparser::ast::Statement;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Location, Token, Tokenizer};

use crate::GnitzDialect;

/// One statement's place in the script it was parsed from. The parser's spans
/// are relative to the whole script, so the script travels with the range.
#[derive(Clone, Copy)]
pub(crate) struct StatementText<'a> {
    pub(crate) script: &'a str,
    pub(crate) range: (usize, usize),
    /// A location at or ahead of the statement's start, and its byte: where a
    /// location in the statement is mapped from.
    origin: (Location, usize),
}

impl<'a> StatementText<'a> {
    /// The statement's own text.
    pub(crate) fn text(&self) -> &'a str {
        &self.script[self.range.0..self.range.1]
    }

    /// The byte offset in the script of a location the parser reported in it.
    pub(crate) fn offset(&self, at: Location) -> Option<usize> {
        let (loc, byte) = self.origin;
        Offsets { src: self.script, at: loc, byte }.byte(at)
    }

    /// The byte range in the script of what follows the statement's first `AS`
    /// outside parentheses — a `CREATE VIEW`'s or an `ALTER VIEW`'s body.
    pub(crate) fn body_after_as(&self) -> Option<Range<usize>> {
        let text = self.text();
        let tokens = Tokenizer::new(&GnitzDialect, text).tokenize_with_location().ok()?;
        let mut depth = 0usize;
        for t in &tokens {
            match &t.token {
                Token::LParen => depth += 1,
                Token::RParen => depth = depth.saturating_sub(1),
                Token::Word(w) if depth == 0 && w.quote_style.is_none() && w.value.eq_ignore_ascii_case("as") => {
                    let after = Offsets::new(text).byte(t.span.end)?;
                    let lead = text[after..].len() - text[after..].trim_start().len();
                    return Some(self.range.0 + after + lead..self.range.1);
                }
                _ => {}
            }
        }
        None
    }
}

/// `script`'s statements, each with its [`StatementText`]: what
/// `Parser::parse_sql` returns, and where each statement stands. An `END` after
/// a statement is refused, where `Parser::parse_sql` stops there and drops the
/// rest of the script.
pub(crate) fn split_statements(script: &str) -> Result<Vec<(Statement, StatementText<'_>)>, ParserError> {
    let mut parser = Parser::new(&GnitzDialect).try_with_sql(script)?;
    let mut offsets = Offsets::new(script);
    let mut out = Vec::new();
    let mut expecting_delimiter = false;
    loop {
        while parser.consume_token(&Token::SemiColon) {
            expecting_delimiter = false;
        }
        if parser.peek_token_ref().token == Token::EOF {
            break;
        }
        if expecting_delimiter {
            return parser.expected("end of statement", parser.peek_token());
        }
        let start = parser.peek_token_ref().span.start;
        let stmt = parser.parse_statement()?;
        let end = parser.get_current_token().span.end;
        let (range, origin) = match (offsets.byte(start), offsets.byte(end)) {
            (Some(a), Some(b)) if a <= b => ((a, b), (start, a)),
            _ => ((0, script.len()), (Location { line: 1, column: 1 }, 0)),
        };
        out.push((stmt, StatementText { script, range, origin }));
        expecting_delimiter = true;
    }
    Ok(out)
}

/// Maps the tokenizer's locations (1-based line, 1-based column counted in
/// characters) to byte offsets of `src`. It walks forward from the last
/// location it mapped, so locations mapped in order cost one pass of `src`.
pub struct Offsets<'a> {
    src: &'a str,
    /// The last location mapped, and its byte.
    at: Location,
    byte: usize,
}

impl<'a> Offsets<'a> {
    pub fn new(src: &'a str) -> Self {
        Offsets {
            src,
            at: Location { line: 1, column: 1 },
            byte: 0,
        }
    }

    /// The byte offset of `loc`; `None` for a location `src` does not hold.
    pub fn byte(&mut self, loc: Location) -> Option<usize> {
        let key = |l: Location| (l.line, l.column);
        if key(loc) < key(self.at) {
            *self = Offsets::new(self.src);
        }
        let mut chars = self.src[self.byte..].chars();
        while key(self.at) < key(loc) {
            let c = chars.next()?;
            self.byte += c.len_utf8();
            match c {
                '\n' => self.at = Location { line: self.at.line + 1, column: 1 },
                _ => self.at.column += 1,
            }
        }
        (key(self.at) == key(loc)).then_some(self.byte)
    }
}

#[cfg(test)]
#[path = "tests/source.rs"]
mod tests;
