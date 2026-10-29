//! Result rendering: the output formats `\pset` selects, over decoded rows.

use std::borrow::Cow;
use std::fmt::Write as _;

use comfy_table::{presets, Cell, CellAlignment, ContentArrangement, Table};
use console::measure_text_width;
use nu_ansi_term::{Color, Style};

use crate::settings::{Expanded, Format, Settings};
use crate::value::{is_numeric, write_json_string, Rows};

/// What a render shows of each row's weight.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Weights {
    /// A row at weight `w` is printed `w` times, as SQL reads a bag. Only for
    /// rows that all stand at a positive weight.
    Repeat,
    /// A leading signed weight column: the rows as the Z-set they are.
    Column,
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

impl Target {
    /// `text` in `style` where the output carries styles, else as it is.
    pub fn paint<'a>(&self, style: Style, text: &'a str) -> Cow<'a, str> {
        paint(self.color, style, text)
    }
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

/// A cell's text; `None` for NULL.
type Line<'a> = Vec<Option<Cow<'a, str>>>;

/// The rows a text format prints, each as its cells' text and the number of
/// times it is printed, and the header.
struct Grid<'a> {
    header: Vec<&'a str>,
    numeric: Vec<bool>,
    rows: Vec<(Line<'a>, usize)>,
}

impl<'a> Grid<'a> {
    fn of(rows: &'a Rows, weights: Weights) -> Grid<'a> {
        let weight_col = weights == Weights::Column;
        let header = weight_col
            .then_some(WEIGHT_HEADER)
            .into_iter()
            .chain(rows.columns.iter().map(|c| c.name.as_str()))
            .collect();
        let numeric = weight_col
            .then_some(true)
            .into_iter()
            .chain(rows.columns.iter().map(|c| is_numeric(c.ty.tc)))
            .collect();
        let rows = rows
            .rows
            .iter()
            .map(|(cells, w)| {
                let line = weight_col
                    .then(|| Some(Cow::Owned(format!("{w:+}"))))
                    .into_iter()
                    .chain(cells.iter().map(|c| c.text()))
                    .collect();
                let copies = match weights {
                    Weights::Repeat => usize::try_from(*w).unwrap_or(0),
                    Weights::Column => 1,
                };
                (line, copies)
            })
            .collect();
        Grid { header, numeric, rows }
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
        Format::Unaligned => unaligned(&Grid::of(rows, weights), settings),
        Format::Csv => csv(&Grid::of(rows, weights), target),
        Format::Json => json(rows, weights),
    }
}

/// The aligned format: a table, or one record per printed line, and the
/// footer.
fn table(grid: &Grid<'_>, settings: &Settings, target: Target) -> String {
    let null = target.paint(Style::new().dimmed(), &settings.null);
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
    table.set_header(grid.header.iter().map(|h| Cell::new(target.paint(heading, h))));
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
            target.paint(Style::new().bold(), &format!("{h}{pad}")).into_owned()
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
            target.paint(Style::new().fg(Color::Cyan), &format!("{banner}{rule}"))
        );
        for (label, cell) in labels.iter().zip(line) {
            let _ = writeln!(out, "{label} | {}", cell.as_deref().unwrap_or(null));
        }
    }
    out
}

fn unaligned(grid: &Grid<'_>, settings: &Settings) -> String {
    let mut out = grid.header.join("|");
    out.push('\n');
    for line in grid.lines() {
        for (i, cell) in line.iter().enumerate() {
            if i > 0 {
                out.push('|');
            }
            out.push_str(cell.as_deref().unwrap_or(&settings.null));
        }
        out.push('\n');
    }
    out
}

/// RFC 4180, NULL as an empty field.
fn csv(grid: &Grid<'_>, target: Target) -> String {
    let mut w = csv::Writer::from_writer(Vec::new());
    let fail = "a CSV write to memory cannot fail";
    if target.header {
        w.write_record(&grid.header).expect(fail);
    }
    for line in grid.lines() {
        w.write_record(line.iter().map(|c| c.as_deref().unwrap_or("")))
            .expect(fail);
    }
    String::from_utf8(w.into_inner().expect(fail)).expect("CSV of UTF-8 fields is UTF-8")
}

/// A JSON array, one object per printed row on its own line, its keys in
/// column order. Under [`Weights::Column`] each row is
/// `{"weight": w, "row": {...}}`, so no column's name can collide with the
/// weight.
fn json(rows: &Rows, weights: Weights) -> String {
    let keys: Vec<String> = rows
        .columns
        .iter()
        .map(|c| {
            let mut key = String::new();
            let _ = write_json_string(&mut key, &c.name);
            key
        })
        .collect();
    let mut out = String::from("[");
    let mut line = String::new();
    let mut first = true;
    for (cells, w) in &rows.rows {
        line.clear();
        let copies = match weights {
            Weights::Repeat => usize::try_from(*w).unwrap_or(0),
            Weights::Column => {
                let _ = write!(line, "{{\"weight\":{w},\"row\":");
                1
            }
        };
        line.push('{');
        for (i, (key, cell)) in keys.iter().zip(cells).enumerate() {
            if i > 0 {
                line.push(',');
            }
            line.push_str(key);
            line.push(':');
            cell.write_json(&mut line);
        }
        line.push('}');
        if weights == Weights::Column {
            line.push('}');
        }
        for _ in 0..copies {
            out.push_str(if first { "\n" } else { ",\n" });
            out.push_str(&line);
            first = false;
        }
    }
    out.push_str(if first { "]\n" } else { "\n]\n" });
    out
}

#[cfg(test)]
#[path = "tests/render.rs"]
mod tests;
