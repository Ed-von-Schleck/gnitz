//! Tab completion: meta-commands and their arguments, and SQL — keywords,
//! functions, and the relations and columns of the catalog snapshot.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use gnitz_sql::sqlparser::keywords::Keyword;
use gnitz_sql::sqlparser::tokenizer::Token;
use nu_ansi_term::{Color, Style};
use reedline::{Completer, CompletionResult, Span, Suggestion};

use gnitz_core::{Catalog, CatalogRelation};

use crate::lex::lex_prefix;
use crate::meta::{self, ArgKind, MetaLine};

/// What completion reads: the catalog snapshot and the current schema, which
/// the shell replaces as they change.
#[derive(Default)]
pub struct Context {
    pub catalog: Catalog,
    pub schema: String,
}

pub type SharedContext = Arc<RwLock<Context>>;

/// The SQL words offered beside the catalog's names and the planner's function
/// and `WITH` option names: the statements and clauses gnitz runs, and its
/// column types.
const KEYWORDS: &[&str] = &[
    "ADD",
    "ALL",
    "ALTER",
    "AND",
    "AS",
    "ASC",
    "BEGIN",
    "BETWEEN",
    "BIGINT",
    "BIGSERIAL",
    "BOOLEAN",
    "BY",
    "CASCADE",
    "CASE",
    "CAST",
    "COLUMN",
    "COMMIT",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "DATE",
    "DECIMAL",
    "DEFAULT",
    "DELETE",
    "DESC",
    "DISTINCT",
    "DOUBLE",
    "DROP",
    "ELSE",
    "END",
    "EXCEPT",
    "EXISTS",
    "EXPLAIN",
    "EXTRACT",
    "FALSE",
    "FIRST",
    "FLOAT",
    "FOREIGN",
    "FROM",
    "FULL",
    "GROUP",
    "HAVING",
    "HUGEINT",
    "IF",
    "IN",
    "INDEX",
    "INNER",
    "INSERT",
    "INT",
    "INTERSECT",
    "INTO",
    "IS",
    "JOIN",
    "KEY",
    "LAST",
    "LEFT",
    "LIKE",
    "LIMIT",
    "NOT",
    "NULL",
    "NULLS",
    "OFFSET",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "OVER",
    "PARTITION",
    "PRIMARY",
    "QUALIFY",
    "REFERENCES",
    "RENAME",
    "RETURNING",
    "RIGHT",
    "ROLLBACK",
    "SCHEMA",
    "SELECT",
    "SERIAL",
    "SET",
    "SMALLINT",
    "START",
    "TABLE",
    "TEXT",
    "THEN",
    "TIMESTAMP",
    "TINYINT",
    "TO",
    "TRANSACTION",
    "TRUE",
    "UHUGEINT",
    "UNION",
    "UNIQUE",
    "UNSIGNED",
    "UPDATE",
    "USING",
    "UUID",
    "VALUES",
    "VIEW",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
];

/// Keywords after which a relation name comes next.
const BEFORE_RELATION: &[Keyword] = &[
    Keyword::FROM,
    Keyword::JOIN,
    Keyword::INTO,
    Keyword::UPDATE,
    Keyword::TABLE,
    Keyword::VIEW,
    Keyword::DESCRIBE,
    Keyword::EXISTS,
];

pub struct SqlCompleter {
    context: SharedContext,
}

impl SqlCompleter {
    pub fn new(context: SharedContext) -> Self {
        SqlCompleter { context }
    }
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
    Path,
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
            Kind::Value | Kind::Path => Style::new(),
        }
    }
}

struct Candidate {
    kind: Kind,
    value: String,
    description: String,
}

/// The start of the word `before` ends in: identifier characters, `.`, a
/// quote, and a leading backslash.
fn word_start(before: &str) -> usize {
    before
        .char_indices()
        .rev()
        .find(|&(_, c)| !(c.is_alphanumeric() || matches!(c, '_' | '.' | '"' | '\\' | '$')))
        .map_or(0, |(i, c)| i + c.len_utf8())
}

impl Completer for SqlCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> CompletionResult {
        let ctx = self.context.read().expect("completion context lock");
        let suggestions = suggest(&ctx, line, pos);
        CompletionResult::fresh(suggestions)
    }
}

/// The suggestions for the cursor at `pos` of `line`.
pub fn suggest(ctx: &Context, line: &str, pos: usize) -> Vec<Suggestion> {
    let before = &line[..pos];
    let start = word_start(before);
    let word = &before[start..];
    let (candidates, replace_from) = match MetaLine::parse(line) {
        Some(meta) => meta_candidates(ctx, line, meta, start, word),
        None => sql_candidates(ctx, line, start, word),
    };
    let typed = &before[replace_from..];
    finish(candidates, typed, Span::new(replace_from, pos))
}

/// Rank `candidates` against what was `typed`: prefix matches first, then
/// ones containing it; each kind together, alphabetically.
fn finish(candidates: Vec<Candidate>, typed: &str, span: Span) -> Vec<Suggestion> {
    let needle = typed.trim_start_matches('"').to_lowercase();
    let mut scored: Vec<(u8, Candidate)> = candidates
        .into_iter()
        .filter_map(|c| {
            let hay = c.value.trim_start_matches('"').to_lowercase();
            if hay.starts_with(&needle) {
                Some((0, c))
            } else if !needle.is_empty() && hay.contains(&needle) {
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
            append_whitespace: !matches!(c.kind, Kind::Path | Kind::Schema) && !c.value.ends_with('.'),
            value: c.value,
            description: (!c.description.is_empty()).then_some(c.description),
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
        description: m.help.to_string(),
    })
}

fn meta_candidates(ctx: &Context, line: &str, meta: MetaLine, start: usize, word: &str) -> (Vec<Candidate>, usize) {
    let MetaLine { name, command } = meta;
    if start <= name.start || start < name.end && word.starts_with('\\') {
        return (meta_commands().collect(), name.start);
    }
    let args: Vec<&str> = line[name.end..start].split_whitespace().collect();
    let kind = command.map_or(ArgKind::None, |m| m.arg(args.len()));
    match kind {
        ArgKind::None => (Vec::new(), start),
        ArgKind::Relation => (relation_candidates(ctx, word), start),
        ArgKind::Schema => (schema_candidates(ctx), start),
        ArgKind::Sql => {
            // `\watch` takes a view name or a SELECT over one.
            let mut c = relation_candidates(ctx, word);
            if args.is_empty() {
                c.push(keyword("SELECT", word));
            } else {
                return sql_candidates(ctx, line, start, word);
            }
            (c, start)
        }
        ArgKind::Choice(values) => (
            values
                .iter()
                .map(|v| Candidate {
                    kind: Kind::Value,
                    value: v.to_string(),
                    description: String::new(),
                })
                .collect(),
            start,
        ),
        ArgKind::Path => {
            let path_start = line[..line.len().min(start + word.len())]
                .rfind(char::is_whitespace)
                .map_or(start, |i| i + 1);
            (path_candidates(&line[path_start..start + word.len()]), path_start)
        }
    }
}

fn path_candidates(typed: &str) -> Vec<Candidate> {
    let (dir, shown_dir) = match typed.rfind('/') {
        Some(i) => (
            if i == 0 {
                "/".to_string()
            } else {
                typed[..i].to_string()
            },
            &typed[..=i],
        ),
        None => (".".to_string(), ""),
    };
    let dir = match dir.strip_prefix("~") {
        Some(rest) => dirs::home_dir().map_or(dir.clone(), |h| format!("{}{rest}", h.display())),
        None => dir,
    };
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|e| {
            let mut name = format!("{shown_dir}{}", e.file_name().to_string_lossy());
            if e.file_type().is_ok_and(|t| t.is_dir()) {
                name.push('/');
            }
            Candidate {
                kind: Kind::Path,
                value: name,
                description: String::new(),
            }
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
            description: "schema".to_string(),
        })
        .collect()
}

/// The relations, unqualified in the current schema and qualified elsewhere;
/// with a qualifier typed, that schema's relations.
fn relation_candidates(ctx: &Context, word: &str) -> Vec<Candidate> {
    if let Some((qual, _)) = word.rsplit_once('.') {
        return ctx
            .catalog
            .relations
            .iter()
            .filter(|r| r.schema.eq_ignore_ascii_case(qual))
            .map(|r| Candidate {
                kind: Kind::Relation,
                value: format!("{qual}.{}", r.name),
                description: r.class().describe().to_string(),
            })
            .collect();
    }
    let mut out: Vec<Candidate> = ctx
        .catalog
        .relations
        .iter()
        .map(|r| Candidate {
            kind: Kind::Relation,
            value: match r.schema == ctx.schema {
                true => r.name.clone(),
                false => r.qualified(),
            },
            description: r.class().describe().to_string(),
        })
        .collect();
    out.extend(ctx.catalog.schemas.iter().map(|s| Candidate {
        kind: Kind::Schema,
        value: format!("{s}."),
        description: "schema".to_string(),
    }));
    out
}

/// `kw` in the case the user is typing in.
fn keyword(kw: &str, word: &str) -> Candidate {
    let lower = !word.is_empty() && word.chars().all(|c| !c.is_ascii_uppercase());
    Candidate {
        kind: Kind::Keyword,
        value: match lower {
            true => kw.to_ascii_lowercase(),
            false => kw.to_string(),
        },
        description: String::new(),
    }
}

/// The relations a statement names after FROM, JOIN, INTO or UPDATE, keyed by
/// the name it is read under there: its alias, else its own name.
fn referenced<'a>(ctx: &'a Context, line: &str) -> HashMap<String, &'a CatalogRelation> {
    let (lexemes, _) = lex_prefix(line);
    let words: Vec<&Token> = lexemes.iter().filter(|l| !l.is_trivia()).map(|l| &l.token).collect();
    let mut out = HashMap::new();
    let mut i = 0;
    while i < words.len() {
        let introduces = matches!(words[i], Token::Word(w) if matches!(w.keyword, Keyword::FROM | Keyword::JOIN | Keyword::INTO | Keyword::UPDATE))
            || (matches!(words[i], Token::Comma) && !out.is_empty());
        i += 1;
        if !introduces {
            continue;
        }
        let Some(Token::Word(first)) = words.get(i) else {
            continue;
        };
        let mut name = first.value.clone();
        i += 1;
        if let (Some(Token::Period), Some(Token::Word(second))) = (words.get(i), words.get(i + 1)) {
            name = format!("{name}.{}", second.value);
            i += 2;
        }
        let Some(rel) = ctx.catalog.find(&ctx.schema, &name) else {
            continue;
        };
        let alias = match (words.get(i), words.get(i + 1)) {
            (Some(Token::Word(w)), Some(Token::Word(a))) if w.keyword == Keyword::AS => Some(a.value.clone()),
            (Some(Token::Word(a)), _) if a.keyword == Keyword::NoKeyword => Some(a.value.clone()),
            _ => None,
        };
        out.insert(rel.name.to_lowercase(), rel);
        out.insert(rel.qualified().to_lowercase(), rel);
        if let Some(a) = alias {
            out.insert(a.to_lowercase(), rel);
        }
    }
    out
}

fn column_candidates<'a>(rel: &'a CatalogRelation, qualifier: Option<&str>) -> impl Iterator<Item = Candidate> + 'a {
    let qualifier = qualifier.map(str::to_string);
    rel.columns
        .iter()
        .map(|c| &c.def)
        .filter(|c| !c.is_hidden)
        .map(move |c| Candidate {
            kind: Kind::Column,
            value: match &qualifier {
                Some(q) => format!("{q}.{}", c.name),
                None => c.name.clone(),
            },
            description: format!("{} · {}", gnitz_sql::sql_type_name(c.ty), rel.name),
        })
}

fn sql_candidates(ctx: &Context, line: &str, start: usize, word: &str) -> (Vec<Candidate>, usize) {
    let refs = referenced(ctx, line);
    if let Some((qual, _)) = word.rsplit_once('.') {
        if let Some(rel) = refs
            .get(&qual.to_lowercase())
            .copied()
            .or_else(|| ctx.catalog.find(&ctx.schema, qual))
        {
            return (column_candidates(rel, Some(qual)).collect(), start);
        }
        return (relation_candidates(ctx, word), start);
    }
    let (prior, _) = lex_prefix(&line[..start]);
    let prev = prior.iter().rev().find(|l| !l.is_trivia()).map(|l| &l.token);
    if let Some(Token::Word(w)) = prev {
        if BEFORE_RELATION.contains(&w.keyword) {
            return (relation_candidates(ctx, word), start);
        }
    }
    let mut out: Vec<Candidate> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for rel in refs.values() {
        if seen.insert(rel.id) {
            out.extend(column_candidates(rel, None));
        }
    }
    out.extend(gnitz_sql::function_names().map(|f| Candidate {
        kind: Kind::Function,
        description: "function".to_string(),
        ..keyword(&f.to_ascii_uppercase(), word)
    }));
    out.extend(KEYWORDS.iter().map(|k| keyword(k, word)));
    out.extend(gnitz_sql::with_option_names().map(|o| Candidate {
        kind: Kind::Keyword,
        value: o.to_string(),
        description: "WITH option".to_string(),
    }));
    if prev.is_none() {
        out.extend(meta_commands());
    }
    out.extend(relation_candidates(ctx, word));
    (out, start)
}

#[cfg(test)]
#[path = "tests/complete.rs"]
mod tests;
