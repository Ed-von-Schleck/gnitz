//! How SQL text spells a name, both ways: a name written so it reads back as
//! itself, and a written name read as the relation or index it names.

use crate::GnitzDialect;
use crate::error::{GnitzSqlError, reject_if};
use crate::rules::canonical_user_name;
use gnitz_core::RelName;
use sqlparser::ast::{Expr, Ident, ObjectName, SelectItem, Spanned, TableAliasColumnDef, TableFactor};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Span;
use sqlparser::tokenizer::Token;
use std::borrow::Cow;

/// The relation a `FROM` item names, and the name the statement reads it by:
/// its alias, else its unqualified name. `None` for anything but a plain
/// table reference the binder accepts.
pub fn table_factor_name(tf: &TableFactor, schema: &str) -> Option<(RelName, String)> {
    let tref = extract_table_ref(tf, schema, "").ok()?;
    Some((tref.name, tref.alias))
}

/// The relation `name` names, unqualified ones in `schema`.
pub fn relation_name(name: &ObjectName, schema: &str) -> Option<RelName> {
    extract_object_name(name, schema, "").ok()
}

/// `text`, the whole of it, read as a relation name in `schema`.
pub fn parse_relation_name(text: &str, schema: &str) -> Option<RelName> {
    relation_name(&read_back(text, |p| p.parse_object_name(false))?, schema)
}

/// `text`, the whole of it, read as one identifier: a quoted one unquoted,
/// otherwise as written.
pub fn parse_identifier(text: &str) -> Option<String> {
    Some(read_back(text, |p| p.parse_identifier())?.value)
}

/// `text`, the whole of it, read as a schema name: canonical, as the schema
/// part of a qualified name reads.
pub fn parse_schema_name(text: &str) -> Option<String> {
    RelName::schema_key(&parse_identifier(text)?).ok()
}

/// `text` parsed whole by `parse`.
fn read_back<T>(
    text: &str,
    parse: impl FnOnce(&mut Parser<'_>) -> Result<T, sqlparser::parser::ParserError>,
) -> Option<T> {
    let mut p = Parser::new(&GnitzDialect).try_with_sql(text).ok()?;
    let out = parse(&mut p).ok()?;
    p.expect_token(&Token::EOF).ok()?;
    Some(out)
}

/// `name` as written: itself when `bare`, else a quoted identifier.
fn bare_or_quoted(name: &str, bare: bool) -> Cow<'_, str> {
    match bare {
        true => Cow::Borrowed(name),
        false => Cow::Owned(Ident::with_quote('"', name).to_string()),
    }
}

/// `name` as a column reference reads it: bare when it parses back as that
/// identifier in a select list, else quoted. A standalone expression is too
/// lenient a test: sqlparser reads `from` there as an identifier.
pub fn column_text(name: &str) -> Cow<'_, str> {
    let bare = read_back(&format!("SELECT {name} FROM t"), |p| p.parse_select()).is_some_and(|s| {
        matches!(&s.projection[..], [SelectItem::UnnamedExpr(Expr::Identifier(i))]
            if i.quote_style.is_none() && i.value == name)
    });
    bare_or_quoted(name, bare)
}

/// One part of a relation name as a `FROM` item reads it: bare when it parses
/// back as a table of that one-part name, else quoted.
pub fn relation_text(part: &str) -> Cow<'_, str> {
    let bare = match read_back(part, |p| p.parse_table_factor()) {
        Some(TableFactor::Table { name, alias: None, .. }) => match &name.0[..] {
            [p] => p.as_ident().is_some_and(|i| i.quote_style.is_none() && i.value == part),
            _ => false,
        },
        _ => false,
    };
    bare_or_quoted(part, bare)
}

/// An `ObjectName`'s parts as plain identifiers. `Err` when the name is empty or
/// any part is not a plain identifier — the shape every extractor below rejects
/// identically, before it classifies what a qualifier would have meant.
fn object_name_parts<'a>(name: &'a sqlparser::ast::ObjectName, context: &str) -> Result<Vec<&'a str>, GnitzSqlError> {
    let parts: Vec<&str> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|i| i.value.as_str())
        .collect();
    if parts.is_empty() || parts.len() != name.0.len() {
        return Err(GnitzSqlError::rejected(format!("empty name in {context}")));
    }
    Ok(parts)
}

/// A table or view: `name` in `session_schema`, or `schema.name`.
pub(crate) fn extract_object_name(
    name: &sqlparser::ast::ObjectName,
    session_schema: &str,
    context: &str,
) -> Result<RelName, GnitzSqlError> {
    match object_name_parts(name, context)?.as_slice() {
        [n] => RelName::unqualified(session_schema, n),
        [s, n] => RelName::new(s, n),
        // `object_name_parts` rejects the empty name, so this is 3+ parts.
        _ => Err(format!("{context}: '{name}' has too many name parts")),
    }
    .map_err(GnitzSqlError::rejected)
}

/// A schema or an index name, canonical. Both are global, so neither takes a
/// qualifier; `noun` names which one the statement wants.
pub(crate) fn extract_global_name(
    name: &sqlparser::ast::ObjectName,
    noun: &str,
    context: &str,
) -> Result<String, GnitzSqlError> {
    match object_name_parts(name, context)?.as_slice() {
        [n] => canonical_user_name(n),
        _ => Err(GnitzSqlError::rejected(format!(
            "{context}: {noun} names are global, so one takes no qualifier"
        ))),
    }
}

/// A plain-table FROM factor as written: the relation it names, the name the
/// statement reads it by, and the positional column aliases its alias carries.
pub(crate) struct TableRef<'a> {
    pub(crate) name: RelName,
    /// The declared alias when present, else the name without its schema.
    pub(crate) alias: String,
    /// `FROM t AS d(x, y)`'s `(x, y)`: empty where none was written.
    pub(crate) columns: &'a [TableAliasColumnDef],
    /// Where the relation name lies in the statement text.
    pub(crate) name_span: Span,
    /// Where the declared alias's name lies; `None` without one.
    pub(crate) alias_span: Option<Span>,
}

/// The [`TableRef`] a plain-table FROM factor writes.
pub(crate) fn extract_table_ref<'a>(
    tf: &'a sqlparser::ast::TableFactor,
    session_schema: &str,
    context: &str,
) -> Result<TableRef<'a>, GnitzSqlError> {
    let sqlparser::ast::TableFactor::Table {
        name,
        alias,
        with_hints: _,       // advisory locking hints (T-SQL WITH (NOLOCK)): no result impact
        index_directives: _, // advisory index hints (USE/FORCE INDEX, INDEXED BY): no result impact
        inheritance: _,      // ONLY / `*`: gnitz has no table inheritance, so no result impact
        args,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
    } = tf
    else {
        return Err(GnitzSqlError::rejected(format!(
            "{context}: only simple table references supported"
        )));
    };
    reject_if(args.is_some(), context, "a table function in FROM")?;
    reject_if(version.is_some(), context, "time travel (AS OF / AT)")?;
    reject_if(*with_ordinality, context, "WITH ORDINALITY")?;
    reject_if(!partitions.is_empty(), context, "PARTITION selection")?;
    reject_if(json_path.is_some(), context, "a JSON path on a table")?;
    reject_if(sample.is_some(), context, "TABLESAMPLE")?;
    let table_name = extract_object_name(name, session_schema, context)?;
    let (alias, columns, alias_span) = match alias {
        Some(a) => {
            let sqlparser::ast::TableAlias {
                explicit: _,
                name: alias_name,
                columns,
                at,
            } = a;
            // Unreachable under GenericDialect; named so a dialect change is not silent.
            reject_if(at.is_some(), context, "AT (PartiQL index alias)")?;
            (alias_name.value.clone(), columns.as_slice(), Some(alias_name.span))
        }
        None => (table_name.spelled_name().to_string(), &[][..], None),
    };
    Ok(TableRef {
        name: table_name,
        alias,
        columns,
        name_span: name.span(),
        alias_span,
    })
}

#[cfg(test)]
#[path = "tests/names.rs"]
mod tests;
