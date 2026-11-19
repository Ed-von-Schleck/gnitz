//! A reply decoded into display text: each visible cell's text, as a
//! terminal shows it, and each row's weight beside it.

use std::fmt::Write as _;
use std::ops::Range;

use gnitz_core::ScanReply;
use gnitz_expr::ColumnLocator;
use gnitz_wire::{ColType, RowSource, TypeCode, ValueText};

/// Append the text of the non-NULL cell at `loc` in `row`.
fn write_cell(out: &mut String, batch: &impl RowSource, loc: ColumnLocator, ty: ColType, row: usize) {
    let _ = match ty.tc {
        TypeCode::String => out.write_str(&String::from_utf8_lossy(&loc.content(batch, row))),
        TypeCode::Blob => {
            out.push_str("\\x");
            loc.content(batch, row)
                .iter()
                .try_for_each(|byte| write!(out, "{byte:02x}"))
        }
        _ => {
            let (mut scratch, mut native) = ([0u8; 16], [0u8; 16]);
            let b = loc.native_le_bytes(batch, row, &mut scratch);
            native[..b.len()].copy_from_slice(b);
            write!(out, "{}", ValueText(ty, u128::from_le_bytes(native)))
        }
    };
}

/// One column of a decoded reply.
pub struct Column {
    pub name: String,
    pub tc: TypeCode,
}

/// A reply's visible columns and its rows, each at its weight.
#[derive(Default)]
pub struct Rows {
    pub columns: Vec<Column>,
    pub weights: Vec<i64>,
    /// Every non-NULL cell's text, back to back.
    text: String,
    /// Each cell's span of `text`, row by row; `None` for NULL.
    cells: Vec<Option<Range<usize>>>,
}

impl Rows {
    /// Rows of `columns`, each at weight 1.
    pub fn of<S: AsRef<str>>(
        columns: &[(&str, TypeCode)],
        rows: impl IntoIterator<Item = impl IntoIterator<Item = S>>,
    ) -> Rows {
        let columns = columns
            .iter()
            .map(|&(name, tc)| Column { name: name.to_string(), tc })
            .collect();
        let mut out = Rows { columns, ..Rows::default() };
        for row in rows {
            out.push(row.into_iter().map(Some), 1);
        }
        out
    }

    /// Append a row of `cells`, `None` for NULL, at `weight`.
    pub fn push<S: AsRef<str>>(&mut self, cells: impl IntoIterator<Item = Option<S>>, weight: i64) {
        for cell in cells {
            let cell = cell.map(|c| {
                let start = self.text.len();
                self.text.push_str(c.as_ref());
                start..self.text.len()
            });
            self.cells.push(cell);
        }
        self.weights.push(weight);
    }

    /// `reply`'s visible cells.
    pub fn decode(reply: &ScanReply) -> Rows {
        let schema = &reply.schema;
        let (shown, columns): (Vec<(ColumnLocator, ColType)>, Vec<Column>) = schema
            .visible_columns()
            .map(|(ci, c)| {
                let column = Column { name: c.name.to_string(), tc: c.ty.tc };
                ((schema.layout().locate(ci), c.ty), column)
            })
            .unzip();
        let batch = &reply.batch.view();
        let mut out = Rows { columns, ..Rows::default() };
        out.cells.reserve(batch.len() * shown.len());
        for row in 0..batch.len() {
            for &(loc, ty) in &shown {
                let cell = (!loc.is_null(batch, row)).then(|| {
                    let start = out.text.len();
                    write_cell(&mut out.text, batch, loc, ty, row);
                    start..out.text.len()
                });
                out.cells.push(cell);
            }
        }
        out.weights = reply.batch.view().weights().collect();
        out
    }

    /// Row `row`'s cells' text, `None` for NULL.
    pub fn row(&self, row: usize) -> impl Iterator<Item = Option<&str>> {
        let n = self.columns.len();
        self.cells[row * n..(row + 1) * n]
            .iter()
            .map(|c| c.clone().map(|r| &self.text[r]))
    }

    /// Whether every row stands at a positive weight, so the rows read as a
    /// bag: a row at weight `w` is `w` equal rows.
    pub fn is_bag(&self) -> bool {
        self.weights.iter().all(|&w| w > 0)
    }
}
