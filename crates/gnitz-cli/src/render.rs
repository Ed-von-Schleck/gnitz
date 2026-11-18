//! Result rendering: the output formats `\pset` selects, over decoded rows.

use std::borrow::Cow;
use std::fmt::Write as _;

use comfy_table::{Attribute, Cell, CellAlignment, ContentArrangement, Table, presets};
use console::measure_text_width;
use nu_ansi_term::{Color, Style};

use gnitz_wire::{FixedInt, TypeCode};

use crate::settings::{Expanded, Format, Settings};
use crate::value::Rows;

/// What a render shows of each row's weight.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Weights {
    /// A row at weight `w` is printed `w` times, as SQL reads a bag. Only for
    /// rows that all stand at a positive weight.
    Repeat,
    /// A leading signed weight column: the rows as the Z-set they are.
    Column,
}

impl Weights {
    /// How many times a row at weight `w` is printed.
    fn copies(self, w: i64) -> usize {
        match self {
            Weights::Repeat => usize::try_from(w).unwrap_or(0),
            Weights::Column => 1,
        }
    }
}

/// Where a render goes.
#[derive(Clone, Copy, Debug)]
pub struct Target {
    /// The terminal's width, where the output goes to one: aligned output
    /// wider than it expands under [`Expanded::Auto`].
    pub width: Option<u16>,
    /// Whether an aligned table wraps its cells to `width`.
    pub wrap: bool,
    /// Whether the output carries ANSI styles.
    pub color: bool,
    /// Whether CSV output starts with its header line.
    pub header: bool,
}

/// `text` in `style` when `color`, else as it is.
pub fn paint(color: bool, style: Style, text: &str) -> Cow<'_, str> {
    match color {
        true => Cow::Owned(style.paint(text).to_string()),
        false => Cow::Borrowed(text),
    }
}

/// The column a [`Weights::Column`] render adds.
const WEIGHT_HEADER: &str = "weight";

/// The columns a render prints, each's name and type: the weight's where it
/// shows one, then each column's.
fn printed_columns(rows: &Rows, weights: Weights) -> impl Iterator<Item = (&str, TypeCode)> {
    (weights == Weights::Column)
        .then_some((WEIGHT_HEADER, TypeCode::I64))
        .into_iter()
        .chain(rows.columns.iter().map(|c| (c.name.as_str(), c.tc)))
}

/// The header a render prints.
fn header(rows: &Rows, weights: Weights) -> impl Iterator<Item = &str> {
    printed_columns(rows, weights).map(|(name, _)| name)
}

/// Row `row`'s fields as a text format prints them: its weight where the
/// render shows it, then each cell's text, `None` for NULL.
fn fields(rows: &Rows, row: usize, weights: Weights) -> impl Iterator<Item = Option<Cow<'_, str>>> {
    let w = rows.weights[row];
    (weights == Weights::Column)
        .then(|| Some(Cow::Owned(format!("{w:+}"))))
        .into_iter()
        .chain(rows.row(row).map(|c| c.map(Cow::Borrowed)))
}

/// Every printed line: each row's index, as many times as it is printed.
fn printed(rows: &Rows, weights: Weights) -> impl Iterator<Item = usize> + '_ {
    (0..rows.weights.len()).flat_map(move |r| std::iter::repeat_n(r, weights.copies(rows.weights[r])))
}

/// Append each printed line of a script format to `out`: `line` writes a
/// row's once, and the line is copied as many times as the row is printed.
fn script_lines(rows: &Rows, weights: Weights, out: &mut String, mut line: impl FnMut(&mut String, usize)) {
    let mut buf = String::new();
    for (r, &w) in rows.weights.iter().enumerate() {
        buf.clear();
        line(&mut buf, r);
        for _ in 0..weights.copies(w) {
            out.push_str(&buf);
        }
    }
}

/// The width `table` takes unwrapped: each column's widest line, plus a
/// space either side and one border each.
fn table_width(table: &Table) -> usize {
    let widths = table.column_max_content_widths();
    widths.iter().map(|&w| usize::from(w)).sum::<usize>() + 3 * widths.len() + 1
}

/// `rows` in `settings`' format, for `target`.
pub fn render(rows: &Rows, weights: Weights, settings: &Settings, target: Target) -> String {
    match settings.format {
        Format::Aligned => table(rows, weights, settings, target),
        Format::Unaligned => unaligned(rows, weights, settings),
        Format::Csv => csv(rows, weights, target),
        Format::Json => json(rows, weights),
    }
}

/// The aligned format: a table, or one record per printed line, and the
/// footer.
fn table(rows: &Rows, weights: Weights, settings: &Settings, target: Target) -> String {
    let null = &settings.null;
    let table = (settings.expanded != Expanded::On).then(|| build_table(rows, weights, null, target));
    let mut out = match table {
        Some(table)
            if settings.expanded == Expanded::Off
                || target.width.is_none_or(|w| table_width(&table) <= usize::from(w)) =>
        {
            aligned(table, target.width.filter(|_| target.wrap))
        }
        _ => expanded(rows, weights, null, target),
    };
    if settings.footer {
        let n: usize = rows.weights.iter().map(|&w| weights.copies(w)).sum();
        let noun = if n == 1 { "row" } else { "rows" };
        let _ = writeln!(out, "({n} {noun})");
    }
    out
}

/// The aligned table, its header and NULLs styled where `target` is: never
/// by whether the process's stdout is a terminal, which comfy-table would
/// read.
fn build_table(rows: &Rows, weights: Weights, null: &str, target: Target) -> Table {
    let mut table = Table::new();
    table.load_style(presets::UTF8_FULL_CONDENSED.with_rounded_corners());
    table.force_no_tty();
    if target.color {
        table.enforce_styling();
    }
    table.set_header(header(rows, weights).map(|h| {
        match target.color {
            true => Cell::new(h)
                .fg(comfy_table::Color::DarkCyan)
                .add_attribute(Attribute::Bold),
            false => Cell::new(h),
        }
    }));
    for r in printed(rows, weights) {
        table.add_row(fields(rows, r, weights).map(|c| match c {
            Some(text) => Cell::new(text),
            None if target.color => Cell::new(null).add_attribute(Attribute::Dim),
            None => Cell::new(null),
        }));
    }
    for (i, (_, tc)) in printed_columns(rows, weights).enumerate() {
        if let Some(col) = table.column_mut(i) {
            col.set_cell_alignment(match is_numeric(tc) {
                true => CellAlignment::Right,
                false => CellAlignment::Left,
            });
        }
    }
    table
}

/// `table` drawn, its cells wrapped to `width` where one is given.
fn aligned(mut table: Table, width: Option<u16>) -> String {
    match width {
        Some(w) => {
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_width(w);
        }
        None => {
            table.set_content_arrangement(ContentArrangement::Disabled);
        }
    }
    let mut s = table.to_string();
    s.push('\n');
    s
}

fn expanded(rows: &Rows, weights: Weights, null: &str, target: Target) -> String {
    let label_width = header(rows, weights).map(measure_text_width).max().unwrap_or(0);
    let labels: Vec<String> = header(rows, weights)
        .map(|h| {
            let pad = " ".repeat(label_width - measure_text_width(h));
            paint(target.color, Style::new().bold(), &format!("{h}{pad}")).into_owned()
        })
        .collect();
    let widest = |s: &str| s.split('\n').map(measure_text_width).max().unwrap_or(0);
    let painted_null = paint(target.color, Style::new().dimmed(), null);
    let gutter = format!("{} | ", " ".repeat(label_width));
    let mut out = String::new();
    for (i, r) in printed(rows, weights).enumerate() {
        let line: Vec<_> = fields(rows, r, weights).collect();
        let banner = format!("-[ RECORD {} ]", i + 1);
        let value_width = line
            .iter()
            .map(|c| widest(c.as_deref().unwrap_or(null)))
            .max()
            .unwrap_or(0);
        let rule = "-".repeat((label_width + 3 + value_width).saturating_sub(banner.len()).max(1));
        let _ = writeln!(
            out,
            "{}",
            paint(target.color, Style::new().fg(Color::Cyan), &format!("{banner}{rule}"))
        );
        // A value's later lines go under its first, past the labels.
        for (label, cell) in labels.iter().zip(&line) {
            let mut value = cell.as_deref().unwrap_or(&painted_null).split('\n');
            let _ = writeln!(out, "{label} | {}", value.next().unwrap_or_default());
            for more in value {
                let _ = writeln!(out, "{gutter}{more}");
            }
        }
    }
    out
}

fn unaligned(rows: &Rows, weights: Weights, settings: &Settings) -> String {
    let mut out = header(rows, weights).collect::<Vec<_>>().join("|");
    out.push('\n');
    script_lines(rows, weights, &mut out, |line, r| {
        for (i, field) in fields(rows, r, weights).enumerate() {
            if i > 0 {
                line.push('|');
            }
            line.push_str(field.as_deref().unwrap_or(&settings.null));
        }
        line.push('\n');
    });
    out
}

/// RFC 4180 quoting, each record ended by `\n`, NULL as an empty field.
fn csv(rows: &Rows, weights: Weights, target: Target) -> String {
    let mut out = String::new();
    if target.header {
        csv_record(&mut out, header(rows, weights));
    }
    script_lines(rows, weights, &mut out, |line, r| {
        csv_record(line, fields(rows, r, weights).map(Option::unwrap_or_default));
    });
    out
}

/// One CSV record: a field holding a `,`, `"`, CR or LF is quoted, its `"`
/// doubled; a record with no text at all is `""`, so it does not read as a
/// blank line.
fn csv_record(out: &mut String, fields: impl IntoIterator<Item = impl AsRef<str>>) {
    let start = out.len();
    for (i, field) in fields.into_iter().enumerate() {
        let field = field.as_ref();
        if i > 0 {
            out.push(',');
        }
        match field.contains([',', '"', '\r', '\n']) {
            true => {
                out.push('"');
                out.push_str(&field.replace('"', "\"\""));
                out.push('"');
            }
            false => out.push_str(field),
        }
    }
    if out.len() == start {
        out.push_str("\"\"");
    }
    out.push('\n');
}

/// A JSON array, one object per printed row on its own line, its keys in
/// column order. Under [`Weights::Column`] each row is
/// `{"weight": w, "row": {...}}`, so no column's name can collide with the
/// weight.
fn json(rows: &Rows, weights: Weights) -> String {
    let keys: Vec<String> = rows.columns.iter().map(|c| json_string(&c.name)).collect();
    let mut out = String::from("[");
    let weighted = weights == Weights::Column;
    script_lines(rows, weights, &mut out, |line, r| {
        line.push('\n');
        if weighted {
            let w = rows.weights[r];
            let _ = write!(line, "{{\"weight\":{w},\"row\":");
        }
        line.push('{');
        for (i, ((key, column), cell)) in keys.iter().zip(&rows.columns).zip(rows.row(r)).enumerate() {
            if i > 0 {
                line.push(',');
            }
            line.push_str(key);
            line.push(':');
            write_json(line, column.tc, cell);
        }
        line.push('}');
        if weighted {
            line.push('}');
        }
        line.push(',');
    });
    match out.pop() {
        Some(',') => out.push_str("\n]\n"),
        _ => out.push_str("[]\n"),
    }
    out
}

/// `s` as a JSON string.
fn json_string(s: &str) -> String {
    serde_json::to_string(s).expect("a string serializes")
}

/// Append a cell of a `tc` column as JSON: integers up to 64 bits and
/// booleans as JSON's own, a float as a number where it is finite, with a
/// fraction so it reads back as one; anything else as its text in a string,
/// so no precision is lost.
fn write_json(out: &mut String, tc: TypeCode, cell: Option<&str>) {
    let Some(text) = cell else {
        return out.push_str("null");
    };
    // A non-finite float, as `FloatText` spells it, has no JSON number.
    if tc.is_float() && !matches!(text, "NaN" | "Infinity" | "-Infinity") {
        out.push_str(text);
        if !text.contains(['.', 'e']) {
            out.push_str(".0");
        }
    } else if tc == TypeCode::Bool || FixedInt::exact(tc).is_some() {
        out.push_str(text);
    } else {
        out.push_str(&json_string(text));
    }
}

/// Whether a column of `tc` reads right-aligned: a number's.
fn is_numeric(tc: TypeCode) -> bool {
    match tc {
        TypeCode::U8
        | TypeCode::U16
        | TypeCode::U32
        | TypeCode::U64
        | TypeCode::I8
        | TypeCode::I16
        | TypeCode::I32
        | TypeCode::I64
        | TypeCode::U128
        | TypeCode::I128
        | TypeCode::F32
        | TypeCode::F64
        | TypeCode::Decimal => true,
        TypeCode::Bool | TypeCode::UUID | TypeCode::Date | TypeCode::Timestamp | TypeCode::String | TypeCode::Blob => {
            false
        }
    }
}

#[cfg(test)]
#[path = "tests/render.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/render.rs"]
mod bench;
