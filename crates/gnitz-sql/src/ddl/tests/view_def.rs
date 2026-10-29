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

#[test]
fn a_definition_reads_back_as_it_was_written() {
    let def = sample();
    assert_eq!(ViewDefinition::decode(&def.encode()).unwrap(), def);
}

#[test]
fn another_encoding_version_is_refused() {
    let mut bytes = sample().encode();
    bytes[0] = ENCODING_VERSION + 1;
    let err = ViewDefinition::decode(&bytes).unwrap_err();
    assert!(err.contains("encoding version"), "{err}");
}

#[test]
fn trailing_bytes_are_refused() {
    let mut bytes = sample().encode();
    bytes.push(0);
    assert!(ViewDefinition::decode(&bytes).is_err());
}

#[test]
fn references_out_of_order_are_refused() {
    let mut def = sample();
    def.refs.swap(0, 1);
    let err = ViewDefinition::decode(&def.encode()).unwrap_err();
    assert!(err.contains("out of order"), "{err}");
}

#[test]
fn a_reference_inside_a_character_is_refused() {
    let mut def = sample();
    // One byte into the two-byte `ü`.
    def.refs[1].name.start += 2;
    let err = ViewDefinition::decode(&def.encode()).unwrap_err();
    assert!(err.contains("outside the query"), "{err}");
}

#[test]
fn a_reference_past_the_query_is_refused() {
    let mut def = sample();
    def.refs[1].name.end = def.query.len() as u32 + 1;
    assert!(ViewDefinition::decode(&def.encode()).is_err());
}
