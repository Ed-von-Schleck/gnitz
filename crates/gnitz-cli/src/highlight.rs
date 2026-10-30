//! Syntax highlighting of the input line, by the tokens `sqlparser` reads.

use gnitz_sql::sqlparser::tokenizer::{Token, Whitespace};
use nu_ansi_term::{Color, Style};
use reedline::{Highlighter, StyledText};

use crate::lex::{is_keyword, lex_prefix, Lexeme};
use crate::meta::{ArgKind, MetaLine};

/// The highlighter: the style of each token class.
pub struct SqlHighlighter {
    keyword: Style,
    function: Style,
    identifier: Style,
    quoted: Style,
    string: Style,
    number: Style,
    comment: Style,
    operator: Style,
    meta: Style,
    /// Input the tokenizer stopped at: an unterminated string or comment.
    open: Style,
    /// The bracket under the cursor and its partner.
    bracket: Style,
}

impl Default for SqlHighlighter {
    fn default() -> Self {
        SqlHighlighter {
            keyword: Style::new().fg(Color::LightBlue).bold(),
            function: Style::new().fg(Color::LightCyan),
            identifier: Style::new(),
            quoted: Style::new().fg(Color::Cyan),
            string: Style::new().fg(Color::Green),
            number: Style::new().fg(Color::LightYellow),
            comment: Style::new().fg(Color::DarkGray).italic(),
            operator: Style::new().fg(Color::LightPurple),
            meta: Style::new().fg(Color::Yellow).bold(),
            open: Style::new().fg(Color::Green).italic(),
            bracket: Style::new().fg(Color::Black).on(Color::LightYellow).bold(),
        }
    }
}

impl SqlHighlighter {
    /// The style of each byte range of `src`, in order, covering it whole.
    pub fn spans(&self, src: &str, cursor: usize) -> Vec<(Style, std::ops::Range<usize>)> {
        let Some(MetaLine { name, command, .. }) = MetaLine::parse(src) else {
            return self.sql_spans(src, cursor);
        };
        let cmd_end = name.end;
        let mut spans = vec![(Style::new(), 0..name.start), (self.meta, name)];
        // A command that takes SQL, `\watch`, has its argument read as SQL.
        if command.is_some_and(|c| matches!(c.arg, ArgKind::Sql(_))) {
            spans.extend(
                self.sql_spans(&src[cmd_end..], cursor.saturating_sub(cmd_end))
                    .into_iter()
                    .map(|(s, r)| (s, r.start + cmd_end..r.end + cmd_end)),
            );
        } else {
            spans.push((Style::new(), cmd_end..src.len()));
        }
        spans
    }

    fn sql_spans(&self, src: &str, cursor: usize) -> Vec<(Style, std::ops::Range<usize>)> {
        let (lexemes, lexed) = lex_prefix(src);
        let brackets = matching_brackets(&lexemes, cursor);
        let mut spans = Vec::with_capacity(lexemes.len() + 1);
        let mut at = 0;
        for (i, l) in lexemes.iter().enumerate() {
            if l.range.start > at {
                spans.push((Style::new(), at..l.range.start));
            }
            let style = if brackets.is_some_and(|b| b.contains(&i)) {
                self.bracket
            } else {
                self.token_style(&lexemes, i)
            };
            spans.push((style, l.range.clone()));
            at = l.range.end;
        }
        if lexed > at {
            spans.push((Style::new(), at..lexed));
        }
        if lexed < src.len() {
            spans.push((self.open, lexed..src.len()));
        }
        spans
    }

    fn token_style(&self, lexemes: &[Lexeme], i: usize) -> Style {
        match &lexemes[i].token {
            Token::Word(w) if w.quote_style.is_some() => self.quoted,
            Token::Word(w) => {
                let solid = |l: &&Lexeme| !l.is_trivia();
                let prev = lexemes[..i].iter().rev().find(solid).map(|l| &l.token);
                let next = lexemes[i + 1..].iter().find(solid).map(|l| &l.token);
                let qualified = matches!(prev, Some(Token::Period)) || matches!(next, Some(Token::Period));
                if matches!(next, Some(Token::LParen)) && !qualified && is_function(&w.value) {
                    self.function
                } else if is_keyword(w) && !qualified {
                    self.keyword
                } else {
                    self.identifier
                }
            }
            Token::Number(..) => self.number,
            Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::TripleSingleQuotedString(_)
            | Token::TripleDoubleQuotedString(_)
            | Token::DollarQuotedString(_)
            | Token::SingleQuotedByteStringLiteral(_)
            | Token::DoubleQuotedByteStringLiteral(_)
            | Token::TripleSingleQuotedByteStringLiteral(_)
            | Token::TripleDoubleQuotedByteStringLiteral(_)
            | Token::SingleQuotedRawStringLiteral(_)
            | Token::DoubleQuotedRawStringLiteral(_)
            | Token::TripleSingleQuotedRawStringLiteral(_)
            | Token::TripleDoubleQuotedRawStringLiteral(_)
            | Token::NationalStringLiteral(_)
            | Token::QuoteDelimitedStringLiteral(_)
            | Token::NationalQuoteDelimitedStringLiteral(_)
            | Token::EscapedStringLiteral(_)
            | Token::UnicodeStringLiteral(_)
            | Token::HexStringLiteral(_) => self.string,
            Token::Whitespace(Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment(_)) => self.comment,
            Token::Whitespace(_) | Token::Comma | Token::SemiColon | Token::Period | Token::LParen | Token::RParen => {
                Style::new()
            }
            _ => self.operator,
        }
    }
}

fn is_function(name: &str) -> bool {
    gnitz_sql::function_names().any(|f| f.eq_ignore_ascii_case(name))
}

/// The bracket at or just before `cursor` and the one it pairs with, as
/// lexeme indices.
fn matching_brackets(lexemes: &[Lexeme], cursor: usize) -> Option<[usize; 2]> {
    let depth_step = |t: &Token| match t {
        Token::LParen => 1isize,
        Token::RParen => -1,
        _ => 0,
    };
    let at = lexemes
        .iter()
        .position(|l| depth_step(&l.token) != 0 && (l.range.start == cursor || l.range.end == cursor))?;
    // Towards the partner: forward from a `(`, back from a `)`.
    let direction = depth_step(&lexemes[at].token);
    let (mut depth, mut j) = (0, at);
    loop {
        depth += depth_step(&lexemes.get(j)?.token);
        if depth == 0 {
            return Some([at, j]);
        }
        j = j.checked_add_signed(direction)?;
    }
}

impl Highlighter for SqlHighlighter {
    fn highlight(&self, line: &str, cursor: usize) -> StyledText {
        let mut text = StyledText::new();
        for (style, range) in self.spans(line, cursor) {
            if !range.is_empty() {
                text.push((style, line[range].to_string()));
            }
        }
        text
    }
}

#[cfg(test)]
#[path = "tests/highlight.rs"]
mod tests;
