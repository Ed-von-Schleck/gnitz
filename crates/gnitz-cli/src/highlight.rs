//! Syntax highlighting of the input line, by the tokens `sqlparser` reads.

use gnitz_sql::sqlparser::tokenizer::Token;
use nu_ansi_term::{Color, Style};
use reedline::{Highlighter, StyledText};

use crate::lex::{is_comment, is_keyword, lex_prefix, Lexeme};
use crate::meta::{ArgKind, MetaLine};

/// The style of each token class.
pub struct Palette {
    pub keyword: Style,
    pub function: Style,
    pub identifier: Style,
    pub quoted: Style,
    pub string: Style,
    pub number: Style,
    pub comment: Style,
    pub operator: Style,
    pub meta: Style,
    /// Input the tokenizer stopped at: an unterminated string or comment.
    pub open: Style,
    /// The bracket under the cursor and its partner.
    pub bracket: Style,
}

impl Default for Palette {
    fn default() -> Self {
        Palette {
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

#[derive(Default)]
pub struct SqlHighlighter {
    palette: Palette,
}

impl SqlHighlighter {
    /// The style of each byte range of `src`, in order, covering it whole.
    pub fn spans(&self, src: &str, cursor: usize) -> Vec<(Style, std::ops::Range<usize>)> {
        let Some(MetaLine { name, command, .. }) = MetaLine::parse(src) else {
            return self.sql_spans(src, cursor);
        };
        let cmd_end = name.end;
        let mut spans = vec![(Style::new(), 0..name.start), (self.palette.meta, name)];
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
        let p = &self.palette;
        let (lexemes, lexed) = lex_prefix(src);
        let brackets = matching_brackets(&lexemes, cursor);
        let mut spans = Vec::with_capacity(lexemes.len() + 1);
        let mut at = 0;
        for (i, l) in lexemes.iter().enumerate() {
            if l.range.start > at {
                spans.push((Style::new(), at..l.range.start));
            }
            let style = if brackets.contains(&i) {
                p.bracket
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
            spans.push((p.open, lexed..src.len()));
        }
        spans
    }

    fn token_style(&self, lexemes: &[Lexeme], i: usize) -> Style {
        let p = &self.palette;
        let solid = |l: &&Lexeme| !l.is_trivia();
        let prev = lexemes[..i].iter().rev().find(solid).map(|l| &l.token);
        let next = lexemes[i + 1..].iter().find(solid).map(|l| &l.token);
        match &lexemes[i].token {
            Token::Word(w) if w.quote_style.is_some() => p.quoted,
            Token::Word(w) => {
                let qualified = matches!(prev, Some(Token::Period)) || matches!(next, Some(Token::Period));
                if matches!(next, Some(Token::LParen)) && !qualified && is_function(&w.value) {
                    p.function
                } else if is_keyword(w) && !qualified {
                    p.keyword
                } else {
                    p.identifier
                }
            }
            Token::Number(..) => p.number,
            Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::NationalStringLiteral(_)
            | Token::EscapedStringLiteral(_)
            | Token::UnicodeStringLiteral(_)
            | Token::HexStringLiteral(_)
            | Token::DollarQuotedString(_)
            | Token::SingleQuotedByteStringLiteral(_)
            | Token::DoubleQuotedByteStringLiteral(_) => p.string,
            t if is_comment(t) => p.comment,
            Token::Whitespace(_) => Style::new(),
            Token::Comma | Token::SemiColon | Token::Period | Token::LParen | Token::RParen => Style::new(),
            _ => p.operator,
        }
    }
}

fn is_function(name: &str) -> bool {
    gnitz_sql::function_names().any(|f| f.eq_ignore_ascii_case(name))
}

/// The bracket at or just before `cursor` and the one it pairs with, as
/// lexeme indices.
fn matching_brackets(lexemes: &[Lexeme], cursor: usize) -> Vec<usize> {
    let at = lexemes.iter().position(|l| {
        matches!(l.token, Token::LParen | Token::RParen) && (l.range.start == cursor || l.range.end == cursor)
    });
    let Some(at) = at else {
        return Vec::new();
    };
    let forward = lexemes[at].token == Token::LParen;
    let (open, close) = match forward {
        true => (Token::LParen, Token::RParen),
        false => (Token::RParen, Token::LParen),
    };
    let order: Box<dyn Iterator<Item = usize>> = match forward {
        true => Box::new(at..lexemes.len()),
        false => Box::new((0..=at).rev()),
    };
    let mut depth = 0usize;
    for j in order {
        let t = &lexemes[j].token;
        if *t == open {
            depth += 1;
        } else if *t == close {
            depth -= 1;
            if depth == 0 {
                return vec![at, j];
            }
        }
    }
    Vec::new()
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
