//! Result rendering: the output formats `\pset` selects, over decoded rows.

use comfy_table::{presets, Cell, CellAlignment, ContentArrangement, Table};
use nu_ansi_term::{Color, Style};

use crate::value::{is_numeric, Rows, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    /// A bordered table, aligned per column.
    Aligned,
    /// One line per row, cells joined by `|`.
    Unaligned,
    Csv,
    /// A JSON array, one object per row.
    Json,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Expanded {
    Off,
    On,
    /// Expanded only where the aligned table would be wider than the terminal.
    Auto,
}

/// How a `\pset` value is spelled.
pub fn name_of(v: &impl clap::ValueEnum) -> String {
    v.to_possible_value()
        .expect("no variant is skipped")
        .get_name()
        .to_string()
}

/// Every variant's spelling.
pub fn variants<E: clap::ValueEnum>() -> Vec<String> {
    E::value_variants().iter().map(name_of).collect()
}

/// The variant `v` spells, in any case; the error lists [`variants`].
pub fn choose<E: clap::ValueEnum>(v: &str) -> Result<E, String> {
    E::from_str(v, true).map_err(|_| format!("\"{v}\" is not one of {}", variants::<E>().join(", ")))
}

/// How results are printed.
#[derive(Clone, Debug)]
pub struct Settings {
    pub format: Format,
    pub expanded: Expanded,
    /// How a NULL cell is spelled.
    pub null: String,
    /// Whether a table ends with its row count.
    pub footer: bool,
    /// Whether output to a terminal too long or wide for it goes to the pager.
    pub pager: bool,
    /// Whether the output carries ANSI styles.
    pub color: bool,
    /// Whether each statement reports how long it took.
    pub timing: bool,
}

impl Settings {
    /// `text` in `style` where the output carries styles, else plain.
    pub fn paint(&self, style: Style, text: &str) -> String {
        match self.color {
            true => style.paint(text).to_string(),
            false => text.to_string(),
        }
    }
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            format: Format::Aligned,
            expanded: Expanded::Off,
            null: "NULL".to_string(),
            footer: true,
            pager: true,
            color: false,
            timing: false,
        }
    }
}

/// What a render shows of each row's weight.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Weights {
    /// A row at weight `w` is printed `w` times, as SQL reads a bag. Only for
    /// rows that all stand at a positive weight.
    Repeat,
    /// A leading signed weight column: the rows as the Z-set they are.
    Column,
}

/// The column a [`Weights::Column`] render adds.
const WEIGHT_HEADER: &str = "weight";

/// The rows a text format prints, each as its cells' text (`None` for NULL),
/// and the header.
struct Grid {
    header: Vec<String>,
    numeric: Vec<bool>,
    rows: Vec<Vec<Option<String>>>,
}

impl Grid {
    fn of(rows: &Rows, weights: Weights) -> Grid {
        let weight_col = weights == Weights::Column;
        let header = weight_col
            .then(|| WEIGHT_HEADER.to_string())
            .into_iter()
            .chain(rows.columns.iter().map(|c| c.name.clone()))
            .collect();
        let numeric = weight_col
            .then_some(true)
            .into_iter()
            .chain(rows.columns.iter().map(|c| is_numeric(c.ty.tc)))
            .collect();
        let mut text = Vec::new();
        for (cells, w) in &rows.rows {
            let line: Vec<Option<String>> = weight_col
                .then(|| Some(format!("{w:+}")))
                .into_iter()
                .chain(cells.iter().map(Value::text))
                .collect();
            let copies = if weight_col { 1 } else { *w };
            for _ in 1..copies {
                text.push(line.clone());
            }
            text.push(line);
        }
        Grid { header, numeric, rows: text }
    }

    /// The width the aligned table takes unwrapped: each column as wide as
    /// its widest line, plus a space either side and one border each.
    fn table_width(&self, settings: &Settings) -> usize {
        let widest = |s: &str| s.lines().map(console_width).max().unwrap_or(0);
        let null = widest(&settings.null);
        let cols = self.header.len();
        let content: usize = (0..cols)
            .map(|i| {
                let cells = self.rows.iter().map(|r| r[i].as_deref().map_or(null, widest));
                cells.chain([widest(&self.header[i])]).max().unwrap_or(0)
            })
            .sum();
        content + 3 * cols + 1
    }
}

/// `rows` in `settings`' format. `width` is the terminal's, where the output
/// goes to one: aligned output wider than it expands under [`Expanded::Auto`],
/// and wraps its cells to fit when `wrap`.
pub fn render(rows: &Rows, weights: Weights, settings: &Settings, width: Option<u16>, wrap: bool) -> String {
    match settings.format {
        Format::Aligned => table(&Grid::of(rows, weights), settings, width, wrap),
        Format::Unaligned => unaligned(&Grid::of(rows, weights), settings),
        Format::Csv => csv(&Grid::of(rows, weights)),
        Format::Json => json(rows, weights),
    }
}

/// The aligned format: a table, or one record per row block, and the footer.
fn table(grid: &Grid, settings: &Settings, width: Option<u16>, wrap: bool) -> String {
    let expand = match settings.expanded {
        Expanded::On => true,
        Expanded::Off => false,
        Expanded::Auto => width.is_some_and(|w| grid.table_width(settings) > usize::from(w)),
    };
    let mut out = match expand {
        true => expanded(grid, settings),
        false => aligned(grid, settings, width.filter(|_| wrap)),
    };
    if settings.footer {
        let n = grid.rows.len();
        let noun = if n == 1 { "row" } else { "rows" };
        out.push_str(&format!("({n} {noun})\n"));
    }
    out
}

fn null_cell(settings: &Settings) -> String {
    settings.paint(Style::new().dimmed(), &settings.null)
}

fn build_table(grid: &Grid, settings: &Settings) -> Table {
    let mut table = Table::new();
    table.load_style(presets::UTF8_FULL_CONDENSED.with_rounded_corners());
    if !settings.color {
        table.force_no_tty();
    }
    table.set_header(grid.header.iter().map(|h| {
        match settings.color {
            true => Cell::new(h)
                .add_attribute(comfy_table::Attribute::Bold)
                .fg(comfy_table::Color::Cyan),
            false => Cell::new(h),
        }
    }));
    for row in &grid.rows {
        table.add_row(row.iter().map(|c| match c {
            Some(s) => Cell::new(s),
            None => Cell::new(null_cell(settings)),
        }));
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

/// The columns `s` takes on a terminal: ANSI escapes take none.
pub fn console_width(s: &str) -> usize {
    let mut width = 0;
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            width += 1;
        }
    }
    width
}

fn aligned(grid: &Grid, settings: &Settings, width: Option<u16>) -> String {
    let mut table = build_table(grid, settings);
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

fn expanded(grid: &Grid, settings: &Settings) -> String {
    let label_width = grid.header.iter().map(|h| h.chars().count()).max().unwrap_or(0);
    let mut out = String::new();
    for (i, row) in grid.rows.iter().enumerate() {
        let banner = format!("-[ RECORD {} ]", i + 1);
        let value_width = row
            .iter()
            .map(|c| {
                c.as_deref()
                    .map_or(settings.null.chars().count(), |s| s.chars().count())
            })
            .max()
            .unwrap_or(0);
        let rule = "-".repeat((label_width + 3 + value_width).saturating_sub(banner.len()).max(1));
        let banner = format!("{banner}{rule}");
        out.push_str(&settings.paint(Style::new().fg(Color::Cyan), &banner));
        out.push('\n');
        for (h, cell) in grid.header.iter().zip(row) {
            let value = cell.clone().unwrap_or_else(|| null_cell(settings));
            let label = settings.paint(Style::new().bold(), &format!("{h:<label_width$}"));
            out.push_str(&format!("{label} | {value}\n"));
        }
    }
    out
}

fn unaligned(grid: &Grid, settings: &Settings) -> String {
    let mut out = grid.header.join("|");
    out.push('\n');
    for row in &grid.rows {
        let cells: Vec<String> = row
            .iter()
            .map(|c| c.clone().unwrap_or_else(|| settings.null.clone()))
            .collect();
        out.push_str(&cells.join("|"));
        out.push('\n');
    }
    out
}

/// RFC 4180, NULL as an empty field.
fn csv(grid: &Grid) -> String {
    let mut w = csv::Writer::from_writer(Vec::new());
    let fail = "a CSV write to memory cannot fail";
    w.write_record(&grid.header).expect(fail);
    for row in &grid.rows {
        w.write_record(row.iter().map(|c| c.as_deref().unwrap_or("")))
            .expect(fail);
    }
    String::from_utf8(w.into_inner().expect(fail)).expect("CSV of UTF-8 fields is UTF-8")
}

/// A JSON array, one object per row on its own line. Under
/// [`Weights::Column`] each row is `{"weight": w, "row": {...}}`, so no
/// column's name can collide with the weight.
fn json(rows: &Rows, weights: Weights) -> String {
    use serde_json::Value as J;
    let mut out = String::from("[");
    let mut first = true;
    for (cells, w) in &rows.rows {
        let obj: serde_json::Map<String, J> = rows
            .columns
            .iter()
            .map(|c| c.name.clone())
            .zip(cells.iter().map(Value::to_json))
            .collect();
        let (line, copies) = match weights {
            Weights::Repeat => (J::Object(obj).to_string(), *w),
            Weights::Column => (serde_json::json!({ "weight": w, "row": obj }).to_string(), 1),
        };
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
