//! Result rendering: the output formats `\pset` selects, over decoded rows.

use std::borrow::Cow;
use std::fmt::Write as _;

use comfy_table::{Cell, CellAlignment, ContentArrangement, Table, presets};
use console::measure_text_width;
use nu_ansi_term::{Color, Style};

use gnitz_wire::TypeCode;

use crate::settings::{Expanded, Format, Settings};
use crate::value::{Rows, Value, json_string};

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

/// A printed line's cells: each one's text, `None` for NULL.
type Line<'a> = Vec<Option<Cow<'a, str>>>;

/// The header a render prints: the weight's where it shows one, then each
/// column's name.
fn header(rows: &Rows, weights: Weights) -> impl Iterator<Item = &str> {
    (weights == Weights::Column)
        .then_some(WEIGHT_HEADER)
        .into_iter()
        .chain(rows.columns.iter().map(|c| c.name.as_str()))
}

/// A row's fields as a text format prints them: its weight `w` where the
/// render shows it, then each cell's text, `None` for NULL.
fn fields(cells: &[Value], w: Option<i64>) -> impl Iterator<Item = Option<Cow<'_, str>>> {
    w.map(|w| Some(Cow::Owned(format!("{w:+}"))))
        .into_iter()
        .chain(cells.iter().map(Value::text))
}

/// Append each printed line of a script format to `out`: `line` writes a
/// row's once, given its weight where the render shows it, and the line is
/// copied as many times as the row is printed.
fn script_lines(
    rows: &Rows,
    weights: Weights,
    out: &mut String,
    mut line: impl FnMut(&mut String, &[Value], Option<i64>),
) {
    let mut buf = String::new();
    for (cells, w) in &rows.rows {
        buf.clear();
        line(&mut buf, cells, (weights == Weights::Column).then_some(*w));
        for _ in 0..weights.copies(*w) {
            out.push_str(&buf);
        }
    }
}

/// The rows the aligned format prints, each as its cells' text and the number
/// of times it is printed, and the header.
struct Grid<'a> {
    header: Vec<&'a str>,
    numeric: Vec<bool>,
    rows: Vec<(Line<'a>, usize)>,
}

impl<'a> Grid<'a> {
    fn of(rows: &'a Rows, weights: Weights) -> Grid<'a> {
        let weight_col = weights == Weights::Column;
        let numeric = weight_col
            .then_some(true)
            .into_iter()
            .chain(rows.columns.iter().map(|c| is_numeric(c.tc)))
            .collect();
        let lines = rows
            .rows
            .iter()
            .map(|(cells, w)| (fields(cells, weight_col.then_some(*w)).collect(), weights.copies(*w)))
            .collect();
        Grid {
            header: header(rows, weights).collect(),
            numeric,
            rows: lines,
        }
    }

    /// Every printed line: each row as many times as it is printed.
    fn lines(&self) -> impl Iterator<Item = &[Option<Cow<'a, str>>]> {
        self.rows
            .iter()
            .flat_map(|(line, n)| std::iter::repeat_n(line.as_slice(), *n))
    }

    /// The width the aligned table takes unwrapped, measured as comfy-table
    /// measures it: each column as wide as its widest line and at least one
    /// wide, plus a space either side and one border each.
    fn table_width(&self, null: &str) -> usize {
        let widest = |s: &str| s.lines().map(measure_text_width).max().unwrap_or(0);
        let null = widest(null);
        let content: usize = (0..self.header.len())
            .map(|i| {
                let cells = self
                    .rows
                    .iter()
                    .map(|(line, _)| line[i].as_deref().map_or(null, widest));
                cells.chain([widest(self.header[i]), 1]).max().unwrap_or(1)
            })
            .sum();
        content + 3 * self.header.len() + 1
    }

    fn line_count(&self) -> usize {
        self.rows.iter().map(|&(_, n)| n).sum()
    }
}

/// `rows` in `settings`' format, for `target`.
pub fn render(rows: &Rows, weights: Weights, settings: &Settings, target: Target) -> String {
    match settings.format {
        Format::Aligned => table(&Grid::of(rows, weights), settings, target),
        Format::Unaligned => unaligned(rows, weights, settings),
        Format::Csv => csv(rows, weights, target),
        Format::Json => json(rows, weights),
    }
}

/// The aligned format: a table, or one record per printed line, and the
/// footer.
fn table(grid: &Grid<'_>, settings: &Settings, target: Target) -> String {
    let null = paint(target.color, Style::new().dimmed(), &settings.null);
    let expand = match settings.expanded {
        Expanded::On => true,
        Expanded::Off => false,
        Expanded::Auto => target.width.is_some_and(|w| grid.table_width(&null) > usize::from(w)),
    };
    let mut out = match expand {
        true => expanded(grid, &null, target),
        false => aligned(build_table(grid, &null, target), target.width.filter(|_| target.wrap)),
    };
    if settings.footer {
        let n = grid.line_count();
        let noun = if n == 1 { "row" } else { "rows" };
        let _ = writeln!(out, "({n} {noun})");
    }
    out
}

/// The aligned table, styled by the shell alone: comfy-table's own styling,
/// which reads whether the process's stdout is a terminal, is off.
fn build_table(grid: &Grid<'_>, null: &str, target: Target) -> Table {
    let mut table = Table::new();
    table.load_style(presets::UTF8_FULL_CONDENSED.with_rounded_corners());
    table.force_no_tty();
    let heading = Style::new().bold().fg(Color::Cyan);
    table.set_header(grid.header.iter().map(|h| Cell::new(paint(target.color, heading, h))));
    for line in grid.lines() {
        table.add_row(line.iter().map(|c| Cell::new(c.as_deref().unwrap_or(null))));
    }
    for (i, &numeric) in grid.numeric.iter().enumerate() {
        if let Some(col) = table.column_mut(i) {
            col.set_cell_alignment(match numeric {
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

fn expanded(grid: &Grid<'_>, null: &str, target: Target) -> String {
    let label_width = grid.header.iter().map(|h| measure_text_width(h)).max().unwrap_or(0);
    let labels: Vec<String> = grid
        .header
        .iter()
        .map(|h| {
            let pad = " ".repeat(label_width - measure_text_width(h));
            paint(target.color, Style::new().bold(), &format!("{h}{pad}")).into_owned()
        })
        .collect();
    let mut out = String::new();
    for (i, line) in grid.lines().enumerate() {
        let banner = format!("-[ RECORD {} ]", i + 1);
        let value_width = line
            .iter()
            .map(|c| measure_text_width(c.as_deref().unwrap_or(null)))
            .max()
            .unwrap_or(0);
        let rule = "-".repeat((label_width + 3 + value_width).saturating_sub(banner.len()).max(1));
        let _ = writeln!(
            out,
            "{}",
            paint(target.color, Style::new().fg(Color::Cyan), &format!("{banner}{rule}"))
        );
        for (label, cell) in labels.iter().zip(line) {
            let _ = writeln!(out, "{label} | {}", cell.as_deref().unwrap_or(null));
        }
    }
    out
}

fn unaligned(rows: &Rows, weights: Weights, settings: &Settings) -> String {
    let mut out = header(rows, weights).collect::<Vec<_>>().join("|");
    out.push('\n');
    script_lines(rows, weights, &mut out, |line, cells, w| {
        for (i, field) in fields(cells, w).enumerate() {
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
    script_lines(rows, weights, &mut out, |line, cells, w| {
        csv_record(line, fields(cells, w).map(Option::unwrap_or_default));
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
    script_lines(rows, weights, &mut out, |line, cells, w| {
        line.push('\n');
        if let Some(w) = w {
            let _ = write!(line, "{{\"weight\":{w},\"row\":");
        }
        line.push('{');
        for (i, (key, cell)) in keys.iter().zip(cells).enumerate() {
            if i > 0 {
                line.push(',');
            }
            line.push_str(key);
            line.push(':');
            cell.write_json(line);
        }
        line.push('}');
        if w.is_some() {
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
