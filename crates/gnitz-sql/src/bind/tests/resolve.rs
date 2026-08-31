use super::*;
use gnitz_core::{ColumnDef, TypeCode};

fn col(name: &str, tc: TypeCode) -> ColumnDef {
    ColumnDef::new(name, tc, false)
}

/// The index-bound gate: a cached alias reports back the descriptor it was
/// cached with, keyed case-insensitively, and an unseen name resolves to
/// nothing. A chain-minted id has no catalog rows yet, so carrying a
/// descriptor for one would bound a scan against an index the segment has not
/// got.
#[test]
fn catalog_provenance_tracks_the_cache_alias_kind() {
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
    let class = |b: &Binder<'_>, n: &str| {
        b.cache
            .get(&n.to_ascii_lowercase())
            .map(|e| e.desc.as_ref().map(|d| d.class))
    };
    let mut b = Binder::new("public");
    b.cache_alias("minted", (1, Arc::clone(&schema), None)).unwrap();
    b.cache_alias("real", (16, Arc::clone(&schema), Some(desc(16, RelClass::Table))))
        .unwrap();
    b.cache_alias("aview", (17, Arc::clone(&schema), Some(desc(17, RelClass::View))))
        .unwrap();

    assert_eq!(
        class(&b, "minted"),
        Some(None),
        "a chain-minted id is not catalog-issued"
    );
    assert_eq!(class(&b, "real"), Some(Some(RelClass::Table)));
    assert_eq!(class(&b, "aview"), Some(Some(RelClass::View)));
    assert_eq!(class(&b, "unseen"), None);
    // Provenance keys on the same lowercased string as the resolution.
    assert_eq!(class(&b, "REAL"), Some(Some(RelClass::Table)));
    assert_eq!(class(&b, "MINTED"), Some(None));

    // Shadowing: a minted alias overwriting a catalog resolution must drop
    // the descriptor with it — `FROM (SELECT * FROM t) t` re-caches `t` as a
    // chain-minted segment, and a stale `Some(_)` here would bound a scan
    // against the shadowed relation's indexes.
    b.cache_alias("real", (2, Arc::clone(&schema), None)).unwrap();
    assert_eq!(
        class(&b, "real"),
        Some(None),
        "an alias shadowing a catalog resolution must shed its provenance"
    );
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
