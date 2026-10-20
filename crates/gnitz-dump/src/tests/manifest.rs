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
fn a_manifest_reads_back_as_written() {
    assert_eq!(Manifest::from_json(&sample().to_json()).unwrap(), sample());
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
