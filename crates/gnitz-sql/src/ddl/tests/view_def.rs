use super::*;

fn sample() -> ViewDefinition {
    let query = "SELECT x.a, b FROM s.t AS x JOIN \"ü\" ON x.a = \"ü\".a".to_string();
    let t = query.find("s.t").unwrap() as u32;
    let x_end = query.find(" JOIN").unwrap() as u32;
    let u = query.find("\"ü\" ON").unwrap() as u32;
    let u_end = u + "\"ü\"".len() as u32;
    ViewDefinition {
        body_columns: vec!["a".into(), "b".into()],
        refs: vec![
            RelationRef {
                tid: 17,
                name: t..t + 3,
                alias_end: Some(x_end),
                alias: "x".into(),
                columns: Some(vec!["a".into(), "c".into()]),
            },
            RelationRef {
                tid: 18,
                name: u..u_end,
                alias_end: None,
                alias: "ü".into(),
                columns: None,
            },
        ],
        query,
    }
}

/// Each rule `check` holds a definition to, refused on decode.
#[test]
fn a_malformed_definition_is_refused() {
    let refused = |mutate: fn(&mut ViewDefinition), needle: &str| {
        let mut def = sample();
        mutate(&mut def);
        let err = ViewDefinition::decode(&def.encode()).unwrap_err();
        assert!(err.contains(needle), "{def:?}: {err}");
    };
    refused(|d| d.refs.swap(0, 1), "out of order");
    // Starts inside the alias of the reference before it.
    refused(
        |d| d.refs[1].name.start = d.refs[0].alias_end.unwrap() - 1,
        "out of order",
    );
    refused(|d| d.refs[1].name.end = d.refs[1].name.start, "out of order");
    refused(|d| d.refs[0].alias_end = Some(d.refs[0].name.end - 1), "out of order");
    // One byte into the two-byte `ü`.
    refused(|d| d.refs[1].name.start += 2, "outside the query");
    // An alias ending one byte into the query's last `ü`.
    refused(
        |d| d.refs[1].alias_end = Some(d.query.len() as u32 - 4),
        "outside the query",
    );
    refused(|d| d.refs[1].name.end = d.query.len() as u32 + 1, "outside the query");
}

/// The bytes version 1 writes. A change to them is a new layout, and bumps
/// `gnitz_wire::VIEW_DEF_VERSION` with them.
#[test]
fn version_1_encodes_these_bytes() {
    let query = sample().query;
    let mut golden = Vec::new();
    golden.extend_from_slice(&(query.len() as u32).to_le_bytes());
    golden.extend_from_slice(query.as_bytes());
    golden.extend_from_slice(&[
        2, 0, // body_columns
        1, 0, 0, 0, b'a', //
        1, 0, 0, 0, b'b', //
        2, 0, // refs
        17, 0, 0, 0, 0, 0, 0, 0, 19, 0, 0, 0, 22, 0, 0, 0, // tid, name
        1, 27, 0, 0, 0, // alias_end
        1, 0, 0, 0, b'x', // alias
        1, 2, 0, 1, 0, 0, 0, b'a', 1, 0, 0, 0, b'c', // columns
        18, 0, 0, 0, 0, 0, 0, 0, 33, 0, 0, 0, 37, 0, 0, 0, // tid, name
        0, // alias_end
        2, 0, 0, 0, 0xc3, 0xbc, // alias
        0,    // columns
    ]);
    assert_eq!(
        (sample().encode(), gnitz_wire::VIEW_DEF_VERSION),
        (golden.clone(), 1),
        "the view-definition layout changed: bump VIEW_DEF_VERSION, then re-pin"
    );
    assert_eq!(ViewDefinition::decode(&golden).unwrap(), sample());
}
