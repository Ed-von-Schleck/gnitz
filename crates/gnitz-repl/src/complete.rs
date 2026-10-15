//! Tab completion: meta-commands and their arguments, and SQL — keywords,
//! functions, and the relations and columns of the catalog snapshot.

use std::borrow::Cow;

use gnitz_sql::sqlparser::ast::{ObjectName, TableFactor};
use gnitz_sql::sqlparser::keywords::Keyword;
use gnitz_sql::sqlparser::parser::Parser;
use gnitz_sql::sqlparser::tokenizer::Token;
use gnitz_wire::RelClass;
use nu_ansi_term::{Color, Style};
use reedline::{Completer, CompletionResult, Span, Suggestion};

use gnitz_core::{Catalog, CatalogRelation};

use crate::lex::{lex_prefix, Lexeme};
use crate::meta::{self, ArgKind, MetaLine, Setting};
use crate::render::{choose, variants};

/// What completion reads: the catalog snapshot and the current schema. The
/// shell builds one at each prompt.
#[derive(Default)]
pub struct Context {
    pub catalog: Catalog,
    pub schema: String,
}

/// The statement and clause words offered beside the catalog's names and the
/// planner's function, type and `WITH` option names.
const KEYWORDS: &[&str] = &[
    "ACTION",
    "ADD",
    "ALL",
    "ALTER",
    "AND",
    "ANY",
    "AS",
    "ASC",
    "BEGIN",
    "BETWEEN",
    "BY",
    "CASCADE",
    "CASE",
    "CLUSTER",
    "COLUMN",
    "COMMIT",
    "CONFLICT",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CURRENT",
    "DEFAULT",
    "DELETE",
    "DESC",
    "DISTINCT",
    "DO",
    "DROP",
    "ELSE",
    "END",
    "EXCEPT",
    "EXISTS",
    "EXPLAIN",
    "FALSE",
    "FIRST",
    "FOLLOWING",
    "FOREIGN",
    "FROM",
    "FULL",
    "GROUP",
    "HAVING",
    "IF",
    "ILIKE",
    "IN",
    "INDEX",
    "INNER",
    "INSERT",
    "INTERSECT",
    "INTO",
    "IS",
    "JOIN",
    "KEY",
    "LAST",
    "LEFT",
    "LIKE",
    "LIMIT",
    "NATURAL",
    "NO",
    "NOT",
    "NOTHING",
    "NULL",
    "NULLS",
    "OFFSET",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "OVER",
    "PARTITION",
    "PRECEDING",
    "PRIMARY",
    "QUALIFY",
    "RANGE",
    "REFERENCES",
    "RENAME",
    "RESTRICT",
    "RETURNING",
    "RIGHT",
    "ROLLBACK",
    "ROW",
    "ROWS",
    "SELECT",
    "SET",
    "START",
    "TABLE",
    "THEN",
    "TO",
    "TRANSACTION",
    "TRUE",
    "UNBOUNDED",
    "UNION",
    "UNIQUE",
    "UNKNOWN",
    "UPDATE",
    "USING",
    "VALUES",
    "VIEW",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
];

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

/// The one relation class `\watch` reads: `plan_subscription` takes a `SELECT`
/// over a delta-fed view.
fn fed(k: RelClass) -> bool {
    k == RelClass::FedView
}

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

/// The start of the SQL word `before` ends in: identifier characters, `.` and
/// a quote.
fn word_start(before: &str) -> usize {
    before
        .char_indices()
        .rev()
        .find(|&(_, c)| !(c.is_alphanumeric() || matches!(c, '_' | '.' | '"' | '$')))
        .map_or(0, |(i, c)| i + c.len_utf8())
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
        None => sql_candidates(ctx, line, pos, 0, |_| true),
    };
    finish(candidates, &before[from..], Span::new(from, pos))
}

/// Rank `candidates` against what was `typed`: prefix matches first, then
/// ones containing it; each kind together, alphabetically.
fn finish(candidates: Vec<Candidate>, typed: &str, span: Span) -> Vec<Suggestion> {
    let needle = typed.trim_start_matches('"').to_lowercase();
    let mut scored: Vec<(u8, Candidate)> = candidates
        .into_iter()
        .filter_map(|c| {
            let hay = c.value.to_lowercase();
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
    // `\watch`'s argument is SQL to the end of the line.
    if matches!(cmd.arg(0), ArgKind::Sql) {
        return watch_candidates(ctx, line, pos, name.end);
    }
    let before = &line[..pos];
    let arg_start = before.trim_end_matches(|c: char| !c.is_whitespace()).len();
    let args: Vec<&str> = before[name.end..arg_start].split_whitespace().collect();
    let word = &before[arg_start..];
    let out = match cmd.arg(args.len()) {
        ArgKind::None | ArgKind::Sql => Vec::new(),
        ArgKind::Relation(keep) => relation_candidates(ctx, word, keep),
        ArgKind::Index => index_candidates(ctx),
        ArgKind::Schema => schema_candidates(ctx),
        ArgKind::SettingName => value_candidates(variants::<Setting>()),
        ArgKind::Value(setting) => {
            let setting = setting.or_else(|| args.first().and_then(|a| choose::<Setting>(a).ok()));
            value_candidates(setting.map_or_else(Vec::new, Setting::values))
        }
        ArgKind::Path => path_candidates(word),
    };
    (out, arg_start)
}

/// `\watch`'s argument, the SQL from `base`: a view name or a `SELECT` as its
/// first word, then SQL over the delta-fed views.
fn watch_candidates(ctx: &Context, line: &str, pos: usize, base: usize) -> (Vec<Candidate>, usize) {
    let start = base + word_start(&line[base..pos]);
    if !line[base..start].trim().is_empty() {
        return sql_candidates(ctx, line, pos, base, fed);
    }
    let word = &line[start..pos];
    let mut out = relation_candidates(ctx, word, fed);
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
            value: s.clone(),
            description: Some("schema".to_string()),
        })
        .collect()
}

/// The relations of the classes `keep` keeps: unqualified in the current
/// schema, qualified elsewhere, beside each other schema holding one; with a
/// qualifier typed, that schema's.
fn relation_candidates(ctx: &Context, word: &str, keep: fn(RelClass) -> bool) -> Vec<Candidate> {
    let kept = ctx.catalog.relations.iter().filter(|r| keep(r.class()));
    let candidate = |r: &CatalogRelation, value: String| Candidate {
        kind: Kind::Relation,
        value,
        description: Some(r.class().describe().to_string()),
    };
    if let Some((qual, _)) = word.rsplit_once('.') {
        return kept
            .filter(|r| r.schema.eq_ignore_ascii_case(qual))
            .map(|r| candidate(r, r.qualified()))
            .collect();
    }
    let mut out: Vec<Candidate> = kept
        .clone()
        .map(|r| match r.schema == ctx.schema {
            true => candidate(r, r.name.clone()),
            false => candidate(r, r.qualified()),
        })
        .collect();
    out.extend(
        ctx.catalog
            .schemas
            .iter()
            .filter(|s| **s != ctx.schema && kept.clone().any(|r| r.schema == **s))
            .map(|s| Candidate {
                kind: Kind::Schema,
                value: format!("{s}."),
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
                    if let TableFactor::Table { name, alias, .. } = factor {
                        push(&mut out, ctx, &name, alias.map(|a| a.name.value));
                    }
                    if !p.consume_token(&Token::Comma) {
                        break;
                    }
                }
            }
            // A bare name: `INTO t (a, b)` must not parse as a table function.
            _ => {
                if let Ok(name) = p.parse_object_name(false) {
                    push(&mut out, ctx, &name, None);
                }
            }
        }
    }
    out
}

/// `name` under `alias`, else under its own name, when the catalog holds it.
fn push<'a>(out: &mut Vec<(String, &'a CatalogRelation)>, ctx: &'a Context, name: &ObjectName, alias: Option<String>) {
    let parts: Option<Vec<&str>> = name.0.iter().map(|p| p.as_ident().map(|i| i.value.as_str())).collect();
    let Some(rel) = parts.and_then(|p| ctx.catalog.find(&ctx.schema, &p.join("."))) else {
        return;
    };
    out.push((alias.unwrap_or_else(|| rel.name.clone()), rel));
}

/// `rel`'s visible columns, each written after `prefix`.
fn column_candidates<'a>(rel: &'a CatalogRelation, prefix: &'a str) -> impl Iterator<Item = Candidate> + 'a {
    rel.columns
        .iter()
        .map(|c| &c.def)
        .filter(|c| !c.is_hidden)
        .map(move |c| Candidate {
            kind: Kind::Column,
            value: format!("{prefix}{}", c.name),
            description: Some(format!("{} · {}", gnitz_sql::sql_type_name(c.ty), rel.name)),
        })
}

/// The SQL from `base` with the cursor at `pos`, read one statement at a time:
/// the one under the cursor names the relations its columns come from.
fn sql_candidates(
    ctx: &Context,
    line: &str,
    pos: usize,
    base: usize,
    keep: fn(RelClass) -> bool,
) -> (Vec<Candidate>, usize) {
    let start = base + word_start(&line[base..pos]);
    let word = &line[start..pos];
    let (lexemes, _) = lex_prefix(&line[base..]);
    let lexemes: Vec<Lexeme> = lexemes
        .into_iter()
        .filter(|l| !l.is_trivia())
        .map(|l| Lexeme {
            range: l.range.start + base..l.range.end + base,
            token: l.token,
        })
        .collect();
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
    let refs = referenced(ctx, stmt);

    if let Some((qual, _)) = word.rsplit_once('.') {
        // A qualifier names one of the statement's relations; only a statement
        // naming none reads it as a relation of the catalog.
        let rel = match refs.is_empty() {
            false => refs.iter().find(|(n, _)| n.eq_ignore_ascii_case(qual)).map(|&(_, r)| r),
            true if !qual.contains('.') => ctx.catalog.find(&ctx.schema, qual),
            true => None,
        };
        let out = match rel {
            Some(r) => column_candidates(r, &word[..qual.len() + 1]).collect(),
            None => relation_candidates(ctx, word, keep),
        };
        return (out, start);
    }
    if let Some(Token::Word(w)) = prev {
        if RELATION_KEYWORDS.contains(&w.keyword) {
            return (relation_candidates(ctx, word, keep), start);
        }
    }
    let mut out: Vec<Candidate> = refs.iter().flat_map(|&(_, r)| column_candidates(r, "")).collect();
    out.extend(gnitz_sql::function_names().map(|f| Candidate {
        kind: Kind::Function,
        value: cased(f, word),
        description: Some("function".to_string()),
    }));
    // Completion replaces one word, so a type spelled in several is offered
    // word by word.
    let words = KEYWORDS
        .iter()
        .copied()
        .chain(gnitz_sql::type_names().flat_map(str::split_whitespace));
    out.extend(words.map(|k| Candidate {
        kind: Kind::Keyword,
        value: cased(k, word),
        description: None,
    }));
    out.extend(gnitz_sql::with_option_names().map(|o| Candidate {
        kind: Kind::Keyword,
        value: o.to_string(),
        description: Some("WITH option".to_string()),
    }));
    // A buffer is a meta-command only when it starts with one.
    if base == 0 && line[..start].trim().is_empty() {
        out.extend(meta_commands());
    }
    out.extend(relation_candidates(ctx, word, keep));
    (out, start)
}

#[cfg(test)]
#[path = "tests/complete.rs"]
mod tests;
