//! The `WITH (…)` clause of CREATE TABLE and CREATE VIEW, read and written: each
//! key list, its decoder and its inverse.

use crate::bind::bind_constant;
use crate::error::GnitzSqlError;
use crate::ir::BExpr;
use gnitz_core::RelProps;
use gnitz_wire::{TableDistribution, TableProps, ViewBudget, ViewProps};
use sqlparser::ast::CreateTableOptions;
use std::num::NonZeroU64;

/// Every `WITH (…)` option key a `CREATE TABLE` or `CREATE VIEW` accepts, once.
pub fn with_option_names() -> impl Iterator<Item = &'static str> {
    TABLE_OPTIONS
        .into_iter()
        .chain(VIEW_OPTIONS.into_iter().filter(|k| !TABLE_OPTIONS.contains(k)))
}

/// The `WITH (…)` entries that declare `props`, as a `CREATE` writes them; the
/// defaults are omitted.
pub fn with_options(props: &RelProps) -> Vec<String> {
    match props {
        RelProps::Table(t) => table_with_options(t),
        RelProps::View(v) => view_with_options(v),
    }
}

/// The boolean a `WITH (key = …)` entry names; `false` where it is absent.
fn bool_option(key: &str, value: Option<&sqlparser::ast::Expr>) -> Result<bool, GnitzSqlError> {
    match value.map(bind_constant) {
        None => Ok(false),
        Some(Ok(BExpr::LitBool(b))) => Ok(b),
        Some(_) => Err(GnitzSqlError::Rejected(format!(
            "WITH ({key} = …) expects a boolean (true/false)"
        ))),
    }
}

/// The values of a `CREATE` statement's `WITH (key = value, …)` clause, one slot per
/// entry of `keys`. Any other option form, unknown key or repeated key is rejected.
fn kv_options<'a, const N: usize>(
    options: &'a CreateTableOptions,
    context: &str,
    keys: [&str; N],
) -> Result<[Option<&'a sqlparser::ast::Expr>; N], GnitzSqlError> {
    let mut slots = [None; N];
    let form = match options {
        CreateTableOptions::With(opts) => {
            for opt in opts {
                let sqlparser::ast::SqlOption::KeyValue { key, value } = opt else {
                    return Err(GnitzSqlError::Rejected(format!(
                        "unsupported {context} option `{opt}` in WITH (…), which takes `key = value` entries"
                    )));
                };
                let name = crate::ast_util::single_part_ident(key);
                let Some(at) = keys
                    .iter()
                    .position(|k| name.is_some_and(|n| n.eq_ignore_ascii_case(k)))
                else {
                    let listed: Vec<String> = keys.iter().map(|k| format!("`{k}`")).collect();
                    return Err(GnitzSqlError::Rejected(format!(
                        "unknown {context} option '{key}'; the supported options are {}",
                        listed.join(", ")
                    )));
                };
                if slots[at].replace(value).is_some() {
                    return Err(GnitzSqlError::Rejected(format!(
                        "{context} option `{}` is given more than once",
                        keys[at]
                    )));
                }
            }
            return Ok(slots);
        }
        CreateTableOptions::None => return Ok(slots),
        CreateTableOptions::Options(_) => "OPTIONS (…)",
        CreateTableOptions::Plain(_) => "space-separated options",
        CreateTableOptions::TableProperties(_) => "TBLPROPERTIES (…)",
    };
    Err(GnitzSqlError::Rejected(format!(
        "{form} is not supported; options must be given as WITH (…)"
    )))
}

/// The `CREATE TABLE … WITH (…)` keys.
const TABLE_OPTIONS: [&str; 3] = ["replicated", "stream", "compress"];

/// The boolean properties of `CREATE TABLE … WITH (…)`. A `Keyed` distribution's
/// prefix is left at its default; the CLUSTER BY phase fills it.
pub(super) fn parse_table_options(table_options: &CreateTableOptions) -> Result<TableProps, GnitzSqlError> {
    let [replicated, stream, compress] = kv_options(table_options, "CREATE TABLE", TABLE_OPTIONS)?;
    let distribution = match bool_option("replicated", replicated)? {
        true => TableDistribution::Replicated,
        false => TableDistribution::default(),
    };
    Ok(TableProps {
        stream: bool_option("stream", stream)?,
        serial: false,
        distribution,
        compress: bool_option("compress", compress)?,
    })
}

/// The `WITH (…)` entries that declare `props`, as a `CREATE TABLE` writes
/// them; the defaults are omitted. [`parse_table_options`]'s inverse.
fn table_with_options(props: &TableProps) -> Vec<String> {
    let replicated = props.distribution == TableDistribution::Replicated;
    [
        ("replicated", replicated),
        ("stream", props.stream),
        ("compress", props.compress),
    ]
    .into_iter()
    .filter(|&(_, on)| on)
    .map(|(key, _)| format!("{key} = true"))
    .collect()
}

/// The `CREATE VIEW … WITH (…)` keys.
const VIEW_OPTIONS: [&str; 3] = ["capacity", "delta", "compress"];

/// Binary units accepted by a `WITH (<option> = '<uint><unit>')` size string,
/// ascending.
const SIZE_UNITS: [(&str, u64); 4] = [("B", 1), ("KB", 1 << 10), ("MB", 1 << 20), ("GB", 1 << 30)];

/// Decode the `WITH (...)` clause of a `CREATE VIEW`.
pub(super) fn decode_view_options(options: &CreateTableOptions) -> Result<ViewProps, GnitzSqlError> {
    let [capacity, delta, compress] = kv_options(options, "CREATE VIEW", VIEW_OPTIONS)?;
    let budget = ViewBudget::from_budgets(
        capacity.map(|v| size_option("capacity", v)).transpose()?,
        delta.map(|v| size_option("delta", v)).transpose()?,
    )
    .map_err(|e| GnitzSqlError::Rejected(format!("CREATE VIEW WITH (capacity …, delta …): {e}")))?;
    Ok(ViewProps {
        budget,
        compress: bool_option("compress", compress)?,
    })
}

/// The `WITH (…)` entries that declare `props`, as a `CREATE VIEW` writes them;
/// the defaults are omitted. [`decode_view_options`]'s inverse.
fn view_with_options(props: &ViewProps) -> Vec<String> {
    let ViewProps { budget, compress } = *props;
    let mut out = Vec::new();
    if let Some(bytes) = budget.capacity_bytes() {
        out.push(format!("capacity = '{}'", size_text(bytes)));
    }
    if let Some(bytes) = budget.delta_bytes() {
        out.push(format!("delta = '{}'", size_text(bytes)));
    }
    if compress {
        out.push("compress = true".to_string());
    }
    out
}

/// `bytes` in the largest of [`SIZE_UNITS`] that divides it: [`parse_size`]'s
/// inverse.
fn size_text(bytes: u64) -> String {
    let (unit, mult) = SIZE_UNITS
        .iter()
        .rev()
        .find(|&&(_, m)| bytes.is_multiple_of(m))
        .expect("every count is a whole number of bytes");
    format!("{} {unit}", bytes / mult)
}

/// The byte count of one size-valued `CREATE VIEW` option.
fn size_option(option: &str, value: &sqlparser::ast::Expr) -> Result<NonZeroU64, GnitzSqlError> {
    let Ok(BExpr::LitStr(text)) = bind_constant(value) else {
        return Err(GnitzSqlError::Rejected(format!(
            "CREATE VIEW option `{option}` takes a single-quoted size string, e.g. '256 MB'"
        )));
    };
    parse_size(option, &text)
}

/// `<uint><unit>` with an optional space, unit in {B, KB, MB, GB}, binary
/// (KB = 2^10). Zero and a `u64`-overflowing product are rejected: a zero
/// capacity names a store that cannot hold its own skeleton, and a zero delta
/// budget one that retains nothing.
fn parse_size(option: &str, text: &str) -> Result<NonZeroU64, GnitzSqlError> {
    let bad = || {
        GnitzSqlError::Rejected(format!(
            "CREATE VIEW option `{option}`: '{text}' is not a size like '256 MB' \
             (a positive integer followed by B, KB, MB or GB)"
        ))
    };
    let trimmed = text.trim();
    let digits = trimmed.len() - trimmed.trim_start_matches(|c: char| c.is_ascii_digit()).len();
    let (num, unit) = trimmed.split_at(digits);
    let num: u64 = num.parse().map_err(|_| bad())?;
    let unit = unit.trim();
    let mult = SIZE_UNITS
        .iter()
        .find(|(u, _)| unit.eq_ignore_ascii_case(u))
        .map(|&(_, m)| m)
        .ok_or_else(bad)?;
    num.checked_mul(mult).and_then(NonZeroU64::new).ok_or_else(|| {
        GnitzSqlError::Rejected(format!(
            "CREATE VIEW option `{option}`: '{text}' is out of range (must be positive and fit a u64)"
        ))
    })
}

#[cfg(test)]
#[path = "tests/options.rs"]
mod tests;
