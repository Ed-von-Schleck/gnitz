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

#[test]
fn another_format_or_version_is_refused() {
    let text = sample().to_json();
    for version in [0, VERSION + 1] {
        let other = text.replace(&format!("\"version\": {VERSION}"), &format!("\"version\": {version}"));
        assert!(Manifest::from_json(&other).unwrap_err().0.contains("format version"));
    }
    assert!(Manifest::from_json(&text.replace("gnitz-dump", "pg_dump")).is_err());
    assert!(Manifest::from_json("{").is_err());
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
