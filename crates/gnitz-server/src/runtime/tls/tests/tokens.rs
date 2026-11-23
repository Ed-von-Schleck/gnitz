use super::*;

fn file_in(dir: &tempfile::TempDir, body: &str) -> String {
    let path = dir.path().join("tokens");
    std::fs::write(&path, body).unwrap();
    path.to_str().unwrap().to_string()
}

#[test]
fn a_minted_token_is_accepted_under_its_name() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tokens").to_str().unwrap().to_string();
    let a = mint(&path, "app1").unwrap();
    let b = mint(&path, "app.2").unwrap();
    assert!(a.starts_with("gnitz_") && a.len() == 6 + 64, "{a}");
    assert_ne!(a, b);

    let text = std::fs::read_to_string(&path).unwrap();
    assert!(
        !text.contains(&a[6..]) && !text.contains(&b[6..]),
        "the file holds digests only"
    );
    use std::os::unix::fs::PermissionsExt as _;
    let mode = std::fs::metadata(&path).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600, "created owner-only");

    let tokens = Tokens::load(&path).unwrap();
    assert_eq!(tokens.len(), 2);
    assert_eq!(tokens.check(a.as_bytes()), Some("app1"));
    assert_eq!(tokens.check(b.as_bytes()), Some("app.2"));
    assert_eq!(tokens.check(b""), None);
    let mut wrong = a.clone().into_bytes();
    *wrong.last_mut().unwrap() ^= 1;
    assert_eq!(tokens.check(&wrong), None);
}

#[test]
fn a_refused_mint_leaves_the_file_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = file_in(&dir, &format!("# operators\napp1 {}", "ab".repeat(32)));
    let before = std::fs::read_to_string(&path).unwrap();
    for (name, names) in [
        ("app1", "already held"),
        ("", "must be"),
        ("a b", "must be"),
        ("ü", "must be"),
    ] {
        let e = mint(&path, name).unwrap_err();
        assert!(e.contains(names), "{name:?}: {e}");
    }
    assert_eq!(std::fs::read_to_string(&path).unwrap(), before);

    // A file without a final newline still gets the new token on a line of its own.
    let t = mint(&path, "app2").unwrap();
    let tokens = Tokens::load(&path).unwrap();
    assert_eq!(tokens.check(t.as_bytes()), Some("app2"));
    assert_eq!(tokens.len(), 2);
}

/// Every malformed line refuses the whole file, naming the file and the line.
#[test]
fn a_malformed_token_file_is_refused_naming_the_line() {
    let dir = tempfile::tempdir().unwrap();
    let hex = "0f".repeat(32);
    let other = "1e".repeat(32);
    for (body, names) in [
        ("\n\napp1\n".to_string(), "line 3"),
        (format!("app1 {hex} extra\n"), "line 1"),
        (format!("app1 {}\n", &hex[..63]), "64 hex digits"),
        (format!("app1 {}g\n", &hex[..63]), "64 hex digits"),
        (format!("bad/name {hex}\n"), "must be"),
        (format!("app1 {hex}\napp1 {other}\n"), "already held"),
        (
            format!("app1 {hex}\napp2 {}\n", hex.to_uppercase()),
            "line 2: \"app2\" and \"app1\" hold the same digest",
        ),
    ] {
        let path = file_in(&dir, &body);
        let e = Tokens::load(&path).unwrap_err();
        assert!(e.contains(names) && e.contains(&path), "{body:?}: {e}");
    }
    let e = Tokens::load("/nonexistent/tokens").unwrap_err();
    assert!(e.contains("/nonexistent/tokens"), "{e}");

    let path = file_in(&dir, &format!("  # a comment\n\n  app1   {hex}  \n"));
    assert_eq!(
        Tokens::load(&path).unwrap().len(),
        1,
        "whitespace and comments are skipped"
    );
}
