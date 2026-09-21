use super::{ensure_dir, staged_dir};
use crate::storage::StoreError;

#[test]
fn staged_dir_reclaims_only_what_it_created() {
    let tmp = tempfile::tempdir().unwrap();

    let fresh = format!("{}/fresh", tmp.path().display());
    let out: Result<(), StoreError> = staged_dir(&fresh, || {
        ensure_dir(&fresh)?;
        Err(StoreError::rejected("fail after create".to_string()))
    });
    assert!(out.is_err());
    assert!(
        !std::path::Path::new(&fresh).exists(),
        "a directory the call created is reclaimed"
    );

    let existing = format!("{}/existing", tmp.path().display());
    ensure_dir(&existing).unwrap();
    let out: Result<(), StoreError> = staged_dir(&existing, || {
        ensure_dir(&existing)?;
        Err(StoreError::rejected("fail on an existing directory".to_string()))
    });
    assert!(out.is_err());
    assert!(
        std::path::Path::new(&existing).is_dir(),
        "a directory that predates the call survives"
    );
}
