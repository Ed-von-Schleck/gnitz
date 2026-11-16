//! The expression VM on programs any client can send — a view's filter or map,
//! or a read's predicate — read off arbitrary bytes, resolved every way a
//! program is (as a filter, a scalar and a map) against a schema holding every
//! column type, and run over rows holding each type's edge values across a
//! morsel boundary. Each run's result is asserted the same on every arm the
//! program can run and at every instruction set its kernels can run at.

use super::*;
use crate::morsel::MORSEL;
use crate::test_support::{Cell, TestView, arms_agree, filter_verdicts, map_cells, scalar_cells};
use crate::{ScalarCell, SchemaColumn, SchemaDescriptor};
use gnitz_wire::{FixedInt, TypeCode};

#[test]
fn program_fuzz() {
    let schema = every_type();
    let rows = edge_rows(&schema);
    bolero::check!().for_each(|blob: &[u8]| {
        let Ok(program) = LogicalProgram::from_blob(blob) else {
            return;
        };
        if let Ok(mut filter) = program.resolve_filter(&schema) {
            arms_agree(&mut filter, &rows, |f, mb| filter_verdicts(f, mb));
        }
        if let Ok(mut scalar) = program.resolve_scalar(&schema) {
            arms_agree(&mut scalar, &rows, |s, mb| {
                scalar_cells(s, mb, |cell| match cell {
                    ScalarCell::Int(v) => Ok(v),
                    ScalarCell::Str(s) => Err(s.map(<[u8]>::to_vec)),
                })
            });
        }
        if let Ok(mut map) = program.resolve_map(&schema, &schema) {
            arms_agree(&mut map, &rows, |m, mb| map_cells(m, &schema, mb));
        }
    });
}

/// Past one morsel, so a run of rows crosses a morsel's end.
const ROWS: usize = MORSEL + 5;

/// A `U64` key, then one column of every type, NOT NULL and nullable.
fn every_type() -> SchemaDescriptor {
    let cols: Vec<SchemaColumn> = std::iter::once(SchemaColumn::new(TypeCode::U64, false))
        .chain(
            TypeCode::ALL
                .iter()
                .flat_map(|&tc| [false, true].map(|nullable| SchemaColumn::new(tc, nullable))),
        )
        .collect();
    SchemaDescriptor::new(&cols, &[0])
}

/// Rows past one morsel, cycling each column through the values its kernels
/// are written around; every third row of a nullable column is NULL.
fn edge_rows(schema: &SchemaDescriptor) -> TestView {
    const FLOATS: [f64; 6] = [0.0, -0.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.5];
    const TEXTS: [&str; 5] = ["", "a", "a string past the inline twelve", "ünïcødé", "%_\\"];
    let mut view = TestView::new(schema, ROWS);
    for pi in 0..schema.num_payload_cols() {
        let ci = schema.payload_col_idx(pi);
        let col = schema.columns()[ci];
        for row in 0..ROWS {
            if col.nullable && row % 3 == 0 {
                view.set_null(row, ci);
                continue;
            }
            let float = FLOATS[row % FLOATS.len()];
            let cell: Cell = match col.type_code {
                TypeCode::String | TypeCode::Blob => TEXTS[row % TEXTS.len()].into(),
                TypeCode::F64 => u128::from(float.to_bits()).into(),
                TypeCode::F32 => u128::from((float as f32).to_bits()).into(),
                TypeCode::Bool => ((row % 2) as u128).into(),
                // Each integer type's own ends; `set` keeps the cell's low bytes.
                tc => {
                    let (min, max) = FixedInt::from_type_code(tc).map_or((i128::MIN, i128::MAX), FixedInt::range);
                    ([0, 1, -1, min, max, 7][row % 6] as u128).into()
                }
            };
            view.set(row, ci, cell);
        }
    }
    view
}
