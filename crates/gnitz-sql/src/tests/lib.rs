use super::*;

/// The names in `names` that no code span of the user documentation's
/// `docs/src/<page>` holds as a whole word, any case.
fn undocumented(page: &str, names: impl Iterator<Item = &'static str>) -> Vec<&'static str> {
    let path = format!("{}/../../docs/src/{page}", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let spans: Vec<String> = text
        .split('`')
        .skip(1)
        .step_by(2)
        .map(str::to_ascii_lowercase)
        .collect();
    let word = |c: Option<char>| c.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_');
    let holds = |span: &str, n: &str| {
        span.match_indices(n)
            .any(|(at, _)| !word(span[..at].chars().next_back()) && !word(span[at + n.len()..].chars().next()))
    };
    names
        .filter(|n| !spans.iter().any(|s| holds(s, &n.to_ascii_lowercase())))
        .collect()
}

#[test]
fn the_docs_name_every_function_type_and_calendar_field() {
    assert_eq!(undocumented("sql/functions.md", function_names()), Vec::<&str>::new());
    assert_eq!(
        undocumented("sql/functions.md", calendar_field_names()),
        Vec::<&str>::new()
    );
    assert_eq!(undocumented("sql/types.md", type_names()), Vec::<&str>::new());
}
