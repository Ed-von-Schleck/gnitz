use super::*;
use gnitz_core::{ColumnDef, TypeCode};

fn col(name: &str, tc: TypeCode) -> ColumnDef {
    ColumnDef::new(name, tc, false)
}

/// The index-bound gate: a cached name reports back the descriptor it was
/// cached with, keyed case-insensitively, and an unseen name resolves to
/// nothing. Shadowing replaces id and descriptor together, so a scan is never
/// bounded against the shadowed relation's indexes.
#[test]
fn catalog_provenance_tracks_the_cached_kind() {
    let schema = Arc::new(Schema {
        columns: vec![col("a", TypeCode::U64)],
        pk_cols: vec![0],
    });
    let desc = |tid, class| {
        Arc::new(RelDescriptor {
            tid,
            class,
            replicated: false,
            delta: false,
            schema: Arc::new(Schema {
                columns: vec![col("a", TypeCode::U64)],
                pk_cols: vec![0],
            }),
            indexes: Arc::new(Vec::new()),
        })
    };
    let class = |b: &Binder<'_>, n: &str| b.cache.get(&n.to_ascii_lowercase()).map(|e| e.desc.class);
    let mut b = Binder::new("public");
    b.cache_relation("real", 16, Arc::clone(&schema), desc(16, RelClass::Table));
    b.cache_relation("aview", 17, Arc::clone(&schema), desc(17, RelClass::View));

    assert_eq!(class(&b, "real"), Some(RelClass::Table));
    assert_eq!(class(&b, "aview"), Some(RelClass::View));
    assert_eq!(class(&b, "unseen"), None);
    // Provenance keys on the same lowercased string as the resolution.
    assert_eq!(class(&b, "REAL"), Some(RelClass::Table));

    // Shadowing replaces id and descriptor together.
    b.cache_relation("real", 17, Arc::clone(&schema), desc(17, RelClass::View));
    assert_eq!(class(&b, "real"), Some(RelClass::View));
}

#[test]
fn test_find_unique_column_unique() {
    let cols = vec![col("a", TypeCode::U64), col("b", TypeCode::I64)];
    assert_eq!(find_unique_column(&cols, "a").unwrap(), Some(0));
    assert_eq!(find_unique_column(&cols, "B").unwrap(), Some(1)); // case-insensitive
}

#[test]
fn test_find_unique_column_absent() {
    let cols = vec![col("a", TypeCode::U64)];
    assert_eq!(find_unique_column(&cols, "missing").unwrap(), None);
}

#[test]
fn test_find_unique_column_duplicate_is_ambiguous() {
    // Two case-insensitively equal names (as a `SELECT *` join view produces).
    let cols = vec![col("Id", TypeCode::U64), col("ID", TypeCode::U64)];
    match find_unique_column(&cols, "id") {
        Err(GnitzSqlError::Bind(s)) => assert!(s.contains("ambiguous"), "got: {s}"),
        other => panic!("expected Bind(ambiguous), got {other:?}"),
    }
}
