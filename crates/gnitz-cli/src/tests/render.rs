use super::*;

/// Two rows, the second at weight 2.
fn rows() -> Rows {
    let mut r = Rows::of(&[("id", TypeCode::I64), ("name", TypeCode::String)], [["1", "a,b"]]);
    r.push([Some("2"), None], 2);
    r
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
    r.weights[1] = -1;
    let out = render(&r, Weights::Column, &plain(Format::Unaligned), PLAIN);
    assert_eq!(out, "weight|id|name\n+1|1|a,b\n-1|2|NULL\n");
}

#[test]
fn csv_quotes_and_leaves_null_empty() {
    let out = render(&rows(), Weights::Repeat, &plain(Format::Csv), PLAIN);
    assert_eq!(out, "id,name\n1,\"a,b\"\n2,\n2,\n");
}

#[test]
fn json_keeps_types_and_spells_every_wide_integer_as_a_string() {
    let r = Rows::of(
        &[("big", TypeCode::U128), ("small", TypeCode::I128), ("f", TypeCode::F64)],
        [
            [u128::MAX.to_string(), "-5".into(), "1.5".into()],
            ["5".into(), i128::MIN.to_string(), "NaN".into()],
        ],
    );
    let out = render(&r, Weights::Repeat, &plain(Format::Json), PLAIN);
    let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(parsed[0]["big"], serde_json::json!(u128::MAX.to_string()));
    assert_eq!(parsed[0]["small"], serde_json::json!("-5"));
    assert_eq!(parsed[0]["f"], serde_json::json!(1.5));
    assert_eq!(parsed[1]["big"], serde_json::json!("5"));
    assert_eq!(parsed[1]["small"], serde_json::json!(i128::MIN.to_string()));
    assert_eq!(parsed[1]["f"], serde_json::json!("NaN"));
}

#[test]
fn json_repeats_a_bag_row_by_its_weight() {
    let out = render(&rows(), Weights::Repeat, &plain(Format::Json), PLAIN);
    let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
    let parsed = parsed.as_array().unwrap();
    assert_eq!(parsed.len(), 3);
    assert_eq!(parsed[1], serde_json::json!({"id": 2, "name": null}));
    assert_eq!(parsed[1], parsed[2]);
}

#[test]
fn an_empty_result_is_an_empty_json_array() {
    let r = Rows::of(&[("id", TypeCode::I64)], [[""; 0]; 0]);
    assert_eq!(render(&r, Weights::Repeat, &plain(Format::Json), PLAIN), "[]\n");
}

#[test]
fn a_csv_record_with_no_text_is_an_empty_quoted_field() {
    let r = Rows::of(&[("x", TypeCode::String)], [[""]]);
    let mut r = r;
    r.push([None::<&str>], 1);
    assert_eq!(
        render(&r, Weights::Repeat, &plain(Format::Csv), PLAIN),
        "x\n\"\"\n\"\"\n"
    );
    let mut out = String::new();
    csv_record(&mut out, ["a\"b", "c\rd", "", "e"]);
    assert_eq!(out, "\"a\"\"b\",\"c\rd\",,e\n");
}

#[test]
fn expanded_prints_one_field_per_line() {
    let settings = Settings {
        expanded: Expanded::On,
        footer: false,
        ..Settings::default()
    };
    let r = Rows::of(&[("id", TypeCode::I64), ("name", TypeCode::String)], [["1", "a,b"]]);
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
    let r = Rows::of(&[("x", TypeCode::String)], [["数据库数据库数据库数"]]);
    // Ten chars take 14 columns as a table, twenty wide ones 24.
    assert!(render(&r, Weights::Repeat, &settings, wide(20)).starts_with("-[ RECORD 1 ]"));
    assert!(render(&r, Weights::Repeat, &settings, wide(24)).contains('│'));
}

#[test]
fn the_measured_table_width_is_the_rendered_one() {
    let settings = Settings { footer: false, ..Settings::default() };
    let mut r = rows();
    r.push([Some("-30"), Some("two\nlines")], 1);
    r.push([Some("8"), Some("")], 1);
    r.push([Some("7"), Some("数据库")], 1);
    r.push([Some("9"), Some("abcdef\r\nb")], 1);
    r.push([Some("10"), Some("x\x1b[31mredred\x1b[0m")], 1);
    for weights in [Weights::Repeat, Weights::Column] {
        let out = render(&r, weights, &settings, PLAIN);
        let widest = out.lines().map(console::measure_text_width).max().unwrap_or(0);
        assert_eq!(
            table_width(&build_table(&r, weights, &settings.null, PLAIN)),
            widest,
            "{out}"
        );
    }
}

#[test]
fn json_keys_follow_the_columns_and_a_real_is_spelled_as_itself() {
    let r = Rows::of(
        &[("zeta", TypeCode::I64), ("alpha", TypeCode::F32), ("b", TypeCode::Blob)],
        [["1", "0.1", "\\xab"]],
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
    r.weights[1] = -1;
    let out = render(&r, Weights::Column, &plain(Format::Json), PLAIN);
    let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(parsed[1]["weight"], serde_json::json!(-1));
    assert_eq!(parsed[1]["row"]["weight"], serde_json::Value::Null);
    assert_eq!(parsed[0]["row"]["weight"], serde_json::json!("a,b"));
}

#[test]
fn an_expanded_value_s_later_lines_stay_under_its_first() {
    let settings = Settings {
        expanded: Expanded::On,
        footer: false,
        ..Settings::default()
    };
    let r = Rows::of(
        &[("id", TypeCode::I64), ("body", TypeCode::String)],
        [["1", "two\nlines"]],
    );
    let out = render(&r, Weights::Repeat, &settings, PLAIN);
    assert_eq!(out, "-[ RECORD 1 ]-\nid   | 1\nbody | two\n     | lines\n");
}

#[test]
fn a_json_float_is_a_number_only_where_it_is_finite() {
    let r = Rows::of(&[("f", TypeCode::F64)], [["2.0"], ["NaN"], ["-Infinity"]]);
    let out = render(&r, Weights::Repeat, &plain(Format::Json), PLAIN);
    assert_eq!(out, "[\n{\"f\":2.0},\n{\"f\":\"NaN\"},\n{\"f\":\"-Infinity\"}\n]\n");
}
