//! The expression VM on programs any client can send — a view's filter or map,
//! or a read's predicate — read off arbitrary bytes, resolved every way a
//! program is (as a filter, a scalar and a map) against a schema holding every
//! column type, and run over rows holding each type's edge values across a
//! morsel boundary.

use super::*;
use crate::batch::MORSEL;
use crate::test_support::TestView;
use crate::{SchemaColumn, SchemaDescriptor};
use gnitz_wire::TypeCode;

#[test]
fn program_fuzz() {
    let schema = every_type();
    let rows = edge_rows(&schema);
    bolero::check!().for_each(|blob: &[u8]| {
        // Decoded once per resolution: a resolve consumes its program.
        let program = || LogicalProgram::from_blob(blob);
        if let Ok(mut filter) = program().and_then(|p| p.resolve_filter(&schema)) {
            let mut runs = Vec::new();
            filter.ranges(&rows, &mut runs);
            let mut end = 0;
            for &(start, stop) in &runs {
                assert!(end <= start && start < stop && stop <= ROWS, "{runs:?}");
                end = stop;
            }
        }
        if let Ok(mut scalar) = program().and_then(|p| p.resolve_scalar(&schema)) {
            scalar.eval_all(&rows);
        }
        if let Ok(mut map) = program().and_then(|p| p.resolve_map(&schema, &schema)) {
            let mut out = TestView::for_schema(&schema, ROWS);
            map.write_computed(&rows, 0, ROWS, &mut out, 0);
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
    const INTS: [i128; 6] = [0, 1, -1, i128::MIN, i128::MAX, 7];
    const FLOATS: [f64; 6] = [0.0, -0.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.5];
    const TEXTS: [&str; 5] = ["", "a", "a string past the inline twelve", "ünïcødé", "%_\\"];
    let mut view = TestView::for_schema(schema, ROWS);
    for pi in 0..schema.num_payload_cols() {
        let col = schema.columns()[schema.payload_col_idx(pi)];
        for row in 0..ROWS {
            if col.nullable && row % 3 == 0 {
                view.set_null(row, pi);
                continue;
            }
            match col.type_code {
                TypeCode::String | TypeCode::Blob => view.set_string(row, pi, TEXTS[row % TEXTS.len()].as_bytes()),
                TypeCode::F64 => view.set_payload(row, pi, &FLOATS[row % FLOATS.len()].to_le_bytes()),
                TypeCode::F32 => view.set_payload(row, pi, &(FLOATS[row % FLOATS.len()] as f32).to_le_bytes()),
                TypeCode::Bool => view.set_payload(row, pi, &[(row % 2) as u8]),
                _ => view.set_payload(row, pi, &INTS[row % INTS.len()].to_le_bytes()[..col.size()]),
            }
        }
    }
    view
}
