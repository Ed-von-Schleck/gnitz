use super::*;

fn sample() -> Manifest {
    Manifest {
        schemas: vec!["sales".into()],
        tables: vec![
            TableEntry {
                schema: "public".into(),
                name: "t".into(),
                file: Some("data/0.parquet".into()),
                rows: 12,
                serial: Some(40),
            },
            TableEntry {
                schema: "sales".into(),
                name: "events".into(),
                file: None,
                rows: 0,
                serial: None,
            },
        ],
        views: vec![ViewEntry {
            schema: "sales".into(),
            name: "hot".into(),
        }],
        indexes: vec!["hot_k".into()],
    }
}

/// A manifest of another format or version, or one whose body a version-1
/// reader cannot parse, is refused in its own words.
#[test]
fn another_format_or_version_is_refused() {
    let body_less = r#"{"format": "gnitz-dump", "version": 1}"#;
    for (text, refusal) in [
        (
            V1.replace("\"version\": 1", "\"version\": 0"),
            "format version 0, this build restores 1",
        ),
        (
            V1.replace("\"version\": 1", "\"version\": 2"),
            "format version 2, this build restores 1",
        ),
        (V1.replace("gnitz-dump", "pg_dump"), "not a gnitz dump"),
        (body_less.into(), "missing field `schemas` at line 1 column 38"),
        ("{".into(), "EOF while parsing an object at line 1 column 1"),
    ] {
        let err = Manifest::from_json(&text).unwrap_err();
        assert_eq!(err.0, format!("manifest.json: {refusal}"));
    }
}

/// A version 1 manifest as a release wrote it, which every build restoring
/// version 1 reads as [`sample`], and which this build writes for it.
const V1: &str = r#"{
  "format": "gnitz-dump",
  "version": 1,
  "gnitz": "0.1.0",
  "schemas": [
    "sales"
  ],
  "tables": [
    {
      "schema": "public",
      "name": "t",
      "file": "data/0.parquet",
      "rows": 12,
      "serial": 40
    },
    {
      "schema": "sales",
      "name": "events",
      "file": null,
      "rows": 0,
      "serial": null
    }
  ],
  "views": [
    {
      "schema": "sales",
      "name": "hot"
    }
  ],
  "indexes": [
    "hot_k"
  ]
}"#;

#[test]
fn a_manifest_is_written_and_read_as_version_1() {
    let written = V1.replace("\"0.1.0\"", &format!("\"{}\"", gnitz_wire::RELEASE));
    assert_eq!(sample().to_json(), written);
    assert_eq!(Manifest::from_json(V1).unwrap(), sample());
}
