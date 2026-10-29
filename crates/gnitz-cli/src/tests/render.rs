use super::*;
use crate::value::{Column, Value};
use gnitz_wire::{ColType, TypeCode};

fn rows() -> Rows {
    Rows {
        columns: vec![
            Column {
                name: "id".into(),
                ty: ColType::of(TypeCode::I64),
            },
            Column {
                name: "name".into(),
                ty: ColType::of(TypeCode::String),
            },
        ],
        rows: vec![
            (vec![Value::Int(1), Value::Text("a,b".into())], 1),
            (vec![Value::Int(2), Value::Null], 2),
        ],
    }
}

/// A file or a pipe.
const PLAIN: Target = Target {
    width: None,
    wrap: false,
    color: false,
    header: true,
};

/// A terminal `width` columns wide.
fn wide(width: u16) -> Target {
    Target { width: Some(width), ..PLAIN }
}

fn plain(format: Format) -> Settings {
    Settings { format, ..Settings::default() }
}

#[test]
fn aligned_repeats_a_row_by_its_weight_and_counts_the_bag() {
    let out = render(&rows(), Weights::Repeat, &plain(Format::Aligned), PLAIN);
    assert_eq!(out.matches("NULL").count(), 2, "{out}");
    assert!(out.ends_with("(3 rows)\n"), "{out}");
}

#[test]
fn the_weight_column_shows_signed_weights_once_per_row() {
    let mut r = rows();
    r.rows[1].1 = -1;
    let out = render(&r, Weights::Column, &plain(Format::Unaligned), PLAIN);
    assert_eq!(out, "weight|id|name\n+1|1|a,b\n-1|2|NULL\n");
}

#[test]
fn csv_quotes_and_leaves_null_empty() {
    let out = render(&rows(), Weights::Repeat, &plain(Format::Csv), PLAIN);
    assert_eq!(out, "id,name\n1,\"a,b\"\n2,\n2,\n");
}

#[test]
fn json_keeps_types_and_spells_wide_integers_as_strings() {
    let r = Rows {
        columns: vec![
            Column {
                name: "big".into(),
                ty: ColType::of(TypeCode::U128),
            },
            Column {
                name: "f".into(),
                ty: ColType::of(TypeCode::F64),
            },
        ],
        rows: vec![(vec![Value::UInt(u128::MAX), Value::F64(1.5)], 1)],
    };
    let out = render(&r, Weights::Repeat, &plain(Format::Json), PLAIN);
    let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(parsed[0]["big"], serde_json::Value::String(u128::MAX.to_string()));
    assert_eq!(parsed[0]["f"], serde_json::json!(1.5));
}

#[test]
fn an_empty_result_is_an_empty_json_array() {
    let r = Rows { rows: vec![], ..rows() };
    assert_eq!(render(&r, Weights::Repeat, &plain(Format::Json), PLAIN), "[]\n");
}

#[test]
fn expanded_prints_one_field_per_line() {
    let settings = Settings {
        expanded: Expanded::On,
        footer: false,
        ..Settings::default()
    };
    let r = Rows {
        rows: rows().rows[..1].to_vec(),
        ..rows()
    };
    let out = render(&r, Weights::Repeat, &settings, PLAIN);
    assert_eq!(out, "-[ RECORD 1 ]-\nid   | 1\nname | a,b\n");
}

#[test]
fn auto_expands_only_what_is_wider_than_the_terminal() {
    let settings = Settings {
        expanded: Expanded::Auto,
        footer: false,
        ..Settings::default()
    };
    assert!(render(&rows(), Weights::Repeat, &settings, wide(200)).contains('│'));
    assert!(render(&rows(), Weights::Repeat, &settings, wide(10)).starts_with("-[ RECORD 1 ]"));
}

#[test]
fn a_wide_character_cell_expands_auto_where_its_char_count_would_fit() {
    let settings = Settings {
        expanded: Expanded::Auto,
        footer: false,
        ..Settings::default()
    };
    let r = Rows {
        columns: vec![Column {
            name: "x".into(),
            ty: ColType::of(TypeCode::String),
        }],
        rows: vec![(vec![Value::Text("数据库数据库数据库数".into())], 1)],
    };
    // Ten chars take 14 columns as a table, twenty wide ones 24.
    assert!(render(&r, Weights::Repeat, &settings, wide(20)).starts_with("-[ RECORD 1 ]"));
    assert!(render(&r, Weights::Repeat, &settings, wide(24)).contains('│'));
}

#[test]
fn the_measured_table_width_is_the_rendered_one() {
    let settings = Settings { footer: false, ..Settings::default() };
    let mut r = rows();
    r.rows
        .push((vec![Value::Int(-30), Value::Text("two\nlines".into())], 1));
    r.rows.push((vec![Value::Int(8), Value::Text(String::new())], 1));
    r.rows.push((vec![Value::Int(7), Value::Text("数据库".into())], 1));
    for weights in [Weights::Repeat, Weights::Column] {
        let out = render(&r, weights, &settings, PLAIN);
        let widest = out.lines().map(console::measure_text_width).max().unwrap_or(0);
        assert_eq!(Grid::of(&r, weights).table_width(&settings.null), widest, "{out}");
    }
}

#[test]
fn json_keys_follow_the_columns_and_a_real_is_spelled_as_itself() {
    let r = Rows::of(
        &[("zeta", TypeCode::I64), ("alpha", TypeCode::F32), ("b", TypeCode::Blob)],
        [vec![Value::Int(1), Value::F32(0.1), Value::Bytes(vec![0xab])]],
    );
    let out = render(&r, Weights::Repeat, &plain(Format::Json), PLAIN);
    assert_eq!(out, "[\n{\"zeta\":1,\"alpha\":0.1,\"b\":\"\\\\xab\"}\n]\n");
    let out = render(&r, Weights::Column, &plain(Format::Json), PLAIN);
    assert!(out.contains("{\"weight\":1,\"row\":{\"zeta\":1,"), "{out}");
}

#[test]
fn a_csv_continuation_has_no_header() {
    let out = render(
        &rows(),
        Weights::Column,
        &plain(Format::Csv),
        Target { header: false, ..PLAIN },
    );
    assert_eq!(out, "+1,1,\"a,b\"\n+2,2,\n");
}

#[test]
fn styles_are_left_out_of_plain_output() {
    let settings = Settings {
        expanded: Expanded::On,
        ..Settings::default()
    };
    let out = render(&rows(), Weights::Repeat, &settings, PLAIN);
    assert!(!out.contains('\x1b'), "{out:?}");
    let out = render(&rows(), Weights::Repeat, &settings, Target { color: true, ..PLAIN });
    assert!(out.contains('\x1b'), "{out:?}");
}

#[test]
fn weighted_json_keeps_the_weight_apart_from_the_columns() {
    let mut r = rows();
    r.columns[1].name = "weight".into();
    r.rows[1].1 = -1;
    let out = render(&r, Weights::Column, &plain(Format::Json), PLAIN);
    let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(parsed[1]["weight"], serde_json::json!(-1));
    assert_eq!(parsed[1]["row"]["weight"], serde_json::Value::Null);
    assert_eq!(parsed[0]["row"]["weight"], serde_json::json!("a,b"));
}
