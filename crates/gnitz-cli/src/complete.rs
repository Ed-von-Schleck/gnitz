//! Tab completion: meta-commands and their arguments, and SQL — keywords,
//! functions, and the relations and columns of the catalog snapshot.

use std::borrow::Cow;

use gnitz_sql::sqlparser::keywords::Keyword;
use gnitz_sql::sqlparser::parser::Parser;
use gnitz_sql::sqlparser::tokenizer::Token;
use gnitz_wire::RelClass;
use nu_ansi_term::{Color, Style};
use reedline::{Completer, CompletionResult, Span, Suggestion};

use gnitz_core::{Catalog, CatalogRelation};

use crate::lex::{lex_prefix, vocabulary, word_at, Lexeme};
use crate::meta::{self, ArgKind, MetaLine};
use crate::settings::{choose, variants, Setting};

/// What completion reads: the catalog snapshot and the current schema. The
/// shell builds one at each prompt.
pub struct Context {
    pub catalog: Catalog,
    pub schema: String,
}

/// Keywords after which a relation name comes next.
const RELATION_KEYWORDS: &[Keyword] = &[
    Keyword::FROM,
    Keyword::JOIN,
    Keyword::UPDATE,
    Keyword::INTO,
    Keyword::TABLE,
    Keyword::VIEW,
    Keyword::EXISTS,
];

/// Suggestion kinds, in the order they are listed.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Column,
    Relation,
    Schema,
    Function,
    Keyword,
    Meta,
    Value,
}

impl Kind {
    fn style(self) -> Style {
        match self {
            Kind::Column => Style::new().fg(Color::White),
            Kind::Relation => Style::new().fg(Color::LightGreen),
            Kind::Schema => Style::new().fg(Color::Green),
            Kind::Function => Style::new().fg(Color::LightCyan),
            Kind::Keyword => Style::new().fg(Color::LightBlue),
            Kind::Meta => Style::new().fg(Color::Yellow),
            Kind::Value => Style::new(),
        }
    }
}

struct Candidate {
    kind: Kind,
    value: String,
    description: Option<String>,
}

impl Completer for Context {
    fn complete(&mut self, line: &str, pos: usize) -> CompletionResult {
        CompletionResult::fresh(suggest(self, line, pos))
    }
}

/// The suggestions for the cursor at `pos` of `line`.
pub fn suggest(ctx: &Context, line: &str, pos: usize) -> Vec<Suggestion> {
    let before = &line[..pos];
    let (candidates, from) = match MetaLine::parse(before) {
        Some(meta) => meta_candidates(ctx, line, pos, meta),
        None => {
            let (mut c, from) = sql_candidates(ctx, line, pos, |_| true);
            // A buffer is a meta-command only when it starts with one.
            if line[..from].trim().is_empty() {
                c.extend(meta_commands());
            }
            (c, from)
        }
    };
    finish(candidates, &before[from..], Span::new(from, pos))
}

/// Rank `candidates` against what was `typed`: prefix matches first, then
/// ones containing it; each kind together, alphabetically. A leading `"` on
/// either side is not matched.
fn finish(candidates: Vec<Candidate>, typed: &str, span: Span) -> Vec<Suggestion> {
    let needle = typed.trim_start_matches('"').to_lowercase();
    let mut scored: Vec<(u8, Candidate)> = candidates
        .into_iter()
        .filter_map(|c| {
            let hay = c.value.trim_start_matches('"').to_lowercase();
            if hay.starts_with(&needle) {
                Some((0, c))
            } else if hay.contains(&needle) {
                Some((1, c))
            } else {
                None
            }
        })
        .collect();
    scored.sort_by(|(sa, a), (sb, b)| (sa, a.kind, &a.value).cmp(&(sb, b.kind, &b.value)));
    let mut seen = std::collections::HashSet::new();
    scored.retain(|(_, c)| seen.insert(c.value.clone()));
    scored
        .into_iter()
        .map(|(_, c)| Suggestion {
            append_whitespace: !c.value.ends_with(['.', '/']),
            value: c.value,
            description: c.description,
            style: Some(c.kind.style()),
            span,
            ..Default::default()
        })
        .collect()
}

/// Every meta-command, by its name.
fn meta_commands() -> impl Iterator<Item = Candidate> {
    meta::COMMANDS.iter().map(|m| Candidate {
        kind: Kind::Meta,
        value: m.name().to_string(),
        description: Some(m.help.to_string()),
    })
}

/// A meta-command's arguments are whitespace-separated, so its word under the
/// cursor is whatever follows the last whitespace.
fn meta_candidates(ctx: &Context, line: &str, pos: usize, meta: MetaLine) -> (Vec<Candidate>, usize) {
    let MetaLine { name, command, .. } = meta;
    if pos == name.end {
        return (meta_commands().collect(), name.start);
    }
    let Some(cmd) = command else {
        return (Vec::new(), pos);
    };
    if let ArgKind::Sql(keep) = cmd.arg {
        let (out, start) = watch_candidates(ctx, &line[name.end..], pos - name.end, keep);
        return (out, name.end + start);
    }
    let before = &line[..pos];
    let arg_start = before.trim_end_matches(|c: char| !c.is_whitespace()).len();
    let args: Vec<&str> = before[name.end..arg_start].split_whitespace().collect();
    let word = &before[arg_start..];
    let out = match (cmd.arg, &args[..]) {
        (ArgKind::Setting, []) => value_candidates(variants::<Setting>()),
        (ArgKind::Setting, [n]) => choose::<Setting>(n).map_or_else(|_| Vec::new(), |s| value_candidates(s.values())),
        (ArgKind::Value(s), []) => value_candidates(s.values()),
        (ArgKind::Relation(keep), []) => relation_candidates(ctx, word, keep),
        (ArgKind::Index, []) => index_candidates(ctx),
        (ArgKind::Schema, []) => schema_candidates(ctx),
        (ArgKind::Path, []) => path_candidates(word),
        _ => Vec::new(),
    };
    (out, arg_start)
}

/// `\watch`'s argument `sql`: a relation name or a `SELECT` as its first word,
/// then SQL over the relations `keep` keeps.
fn watch_candidates(ctx: &Context, sql: &str, pos: usize, keep: fn(RelClass) -> bool) -> (Vec<Candidate>, usize) {
    let (out, start) = sql_candidates(ctx, sql, pos, keep);
    if !sql[..start].trim().is_empty() {
        return (out, start);
    }
    let word = &sql[start..pos];
    let mut out = relation_candidates(ctx, word, keep);
    out.push(Candidate {
        kind: Kind::Keyword,
        value: cased("SELECT", word),
        description: None,
    });
    (out, start)
}

fn path_candidates(typed: &str) -> Vec<Candidate> {
    let shown = &typed[..typed.rfind('/').map_or(0, |i| i + 1)];
    let dir = match shown {
        "" => Cow::Borrowed("."),
        s => meta::expand_home(s),
    };
    let Ok(entries) = std::fs::read_dir(&*dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|e| {
            let mut name = format!("{shown}{}", e.file_name().to_string_lossy());
            if e.file_type().is_ok_and(|t| t.is_dir()) {
                name.push('/');
            }
            Candidate {
                kind: Kind::Value,
                value: name,
                description: None,
            }
        })
        .collect()
}

fn value_candidates(values: Vec<String>) -> Vec<Candidate> {
    values
        .into_iter()
        .map(|value| Candidate {
            kind: Kind::Value,
            value,
            description: None,
        })
        .collect()
}

/// Every index, by its name.
fn index_candidates(ctx: &Context) -> Vec<Candidate> {
    ctx.catalog
        .relations
        .iter()
        .flat_map(|r| {
            r.indexes.iter().map(|i| Candidate {
                kind: Kind::Value,
                value: i.name.clone(),
                description: Some(format!("index on {}", r.name)),
            })
        })
        .collect()
}

fn schema_candidates(ctx: &Context) -> Vec<Candidate> {
    ctx.catalog
        .schemas
        .iter()
        .map(|s| Candidate {
            kind: Kind::Schema,
            value: gnitz_sql::relation_text(s).into_owned(),
            description: Some("schema".to_string()),
        })
        .collect()
}

/// The relations of the classes `keep` keeps: unqualified in the current
/// schema, qualified elsewhere, beside each other schema holding one; with a
/// qualifier typed, that schema's. Each part is written as SQL reads it back.
fn relation_candidates(ctx: &Context, word: &str, keep: fn(RelClass) -> bool) -> Vec<Candidate> {
    let kept = ctx.catalog.relations.iter().filter(|r| keep(r.class()));
    let candidate = |r: &CatalogRelation, qualify: bool| Candidate {
        kind: Kind::Relation,
        value: match qualify {
            true => gnitz_sql::qualified_name(r),
            false => gnitz_sql::relation_text(&r.name).into_owned(),
        },
        description: Some(r.class().describe().to_string()),
    };
    if let Some((qual, _)) = word.rsplit_once('.') {
        return kept
            .filter(|r| r.schema.eq_ignore_ascii_case(qual))
            .map(|r| candidate(r, true))
            .collect();
    }
    let mut out: Vec<Candidate> = kept.clone().map(|r| candidate(r, r.schema != ctx.schema)).collect();
    out.extend(
        ctx.catalog
            .schemas
            .iter()
            .filter(|s| **s != ctx.schema && kept.clone().any(|r| r.schema == **s))
            .map(|s| Candidate {
                kind: Kind::Schema,
                value: format!("{}.", gnitz_sql::relation_text(s)),
                description: Some("schema".to_string()),
            }),
    );
    out
}

/// `s` in the case the user is typing in.
fn cased(s: &str, word: &str) -> String {
    match !word.is_empty() && word.chars().all(|c| !c.is_ascii_uppercase()) {
        true => s.to_ascii_lowercase(),
        false => s.to_ascii_uppercase(),
    }
}

/// The relations `stmt` names, in statement order, each under the name the
/// binder reads it by: its alias, else its unqualified name. Each introducer's
/// items are read with sqlparser's own grammar, independently of the others,
/// so an unfinished item ends only its own list.
fn referenced<'a>(ctx: &'a Context, stmt: &[Lexeme]) -> Vec<(String, &'a CatalogRelation)> {
    let mut out = Vec::new();
    for (i, l) in stmt.iter().enumerate() {
        let Token::Word(w) = &l.token else {
            continue;
        };
        if !RELATION_KEYWORDS.contains(&w.keyword) {
            continue;
        }
        let rest = stmt[i + 1..].iter().map(|l| l.token.clone()).collect();
        let mut p = Parser::new(&gnitz_sql::DIALECT).with_tokens(rest);
        match w.keyword {
            // A table factor: the dialect's alias rule, derived tables, comma lists.
            Keyword::FROM | Keyword::JOIN | Keyword::UPDATE => {
                while let Ok(factor) = p.parse_table_factor() {
                    let named = gnitz_sql::table_factor_name(&factor, &ctx.schema);
                    out.extend(named.and_then(|(n, alias)| ctx.catalog.get(&n).map(|r| (alias, r))));
                    if !p.consume_token(&Token::Comma) {
                        break;
                    }
                }
            }
            // A bare name: `INTO t (a, b)` must not parse as a table function.
            _ => {
                let name = p.parse_object_name(false).ok();
                let rel = name.and_then(|n| gnitz_sql::relation_name(&n, &ctx.schema));
                out.extend(rel.and_then(|n| ctx.catalog.get(&n)).map(|r| (r.name.clone(), r)));
            }
        }
    }
    out
}

/// `rel`'s visible columns, each written after `prefix` as SQL reads it back.
fn column_candidates<'a>(rel: &'a CatalogRelation, prefix: &'a str) -> impl Iterator<Item = Candidate> + 'a {
    rel.visible_columns().map(move |c| Candidate {
        kind: Kind::Column,
        value: format!("{prefix}{}", gnitz_sql::column_text(&c.name)),
        description: Some(format!("{} · {}", gnitz_sql::sql_type_name(c.ty), rel.name)),
    })
}

/// `sql` with the cursor at `pos`, read one statement at a time: the one
/// under the cursor names the relations its columns come from. The span
/// returned starts at the word under the cursor, in `sql`'s coordinates.
fn sql_candidates(ctx: &Context, sql: &str, pos: usize, keep: fn(RelClass) -> bool) -> (Vec<Candidate>, usize) {
    let Some(start) = word_at(&sql[..pos]) else {
        return (Vec::new(), pos);
    };
    let word = &sql[start..pos];
    let (lexemes, _) = lex_prefix(sql);
    let lexemes: Vec<Lexeme> = lexemes.into_iter().filter(|l| !l.is_trivia()).collect();
    let is_semi = |l: &Lexeme| l.token == Token::SemiColon;
    let from = lexemes
        .iter()
        .rposition(|l| is_semi(l) && l.range.end <= pos)
        .map_or(0, |i| i + 1);
    let to = lexemes
        .iter()
        .position(|l| is_semi(l) && l.range.start >= pos)
        .unwrap_or(lexemes.len());
    let stmt = &lexemes[from..to];
    let prev = stmt.iter().rev().find(|l| l.range.end <= start).map(|l| &l.token);
    if let Some(Token::Word(w)) = prev {
        if RELATION_KEYWORDS.contains(&w.keyword) {
            return (relation_candidates(ctx, word, keep), start);
        }
    }
    let refs = referenced(ctx, stmt);

    if let Some((qual, _)) = word.rsplit_once('.') {
        // A qualifier names one of the statement's relations; only a statement
        // naming none reads it as a relation of the catalog, by a one-part
        // name, as the binder reads a column reference.
        let rel = match refs.is_empty() {
            false => refs.iter().find(|(n, _)| n.eq_ignore_ascii_case(qual)).map(|&(_, r)| r),
            true => gnitz_sql::parse_relation_name(qual, &ctx.schema)
                .filter(|n| !n.is_qualified())
                .and_then(|n| ctx.catalog.get(&n)),
        };
        let out = match rel {
            Some(r) => column_candidates(r, &word[..qual.len() + 1]).collect(),
            None => relation_candidates(ctx, word, keep),
        };
        return (out, start);
    }
    let mut out: Vec<Candidate> = refs.iter().flat_map(|&(_, r)| column_candidates(r, "")).collect();
    out.extend(gnitz_sql::function_names().map(|f| Candidate {
        kind: Kind::Function,
        value: cased(f, word),
        description: Some("function".to_string()),
    }));
    out.extend(vocabulary().map(|k| Candidate {
        kind: Kind::Keyword,
        value: cased(&k, word),
        description: None,
    }));
    out.extend(gnitz_sql::with_option_names().map(|o| Candidate {
        kind: Kind::Keyword,
        value: o.to_string(),
        description: Some("WITH option".to_string()),
    }));
    out.extend(relation_candidates(ctx, word, keep));
    (out, start)
}

#[cfg(test)]
#[path = "tests/complete.rs"]
mod tests;
