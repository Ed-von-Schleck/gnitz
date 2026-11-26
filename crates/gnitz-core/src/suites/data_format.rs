//! A server refuses a data directory stamped with another storage format, and
//! leaves every file in it as it was.

use super::*;
use std::collections::BTreeMap;
use std::path::Path;

/// Every file under `dir` but the logs, by path, with its bytes.
fn files(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(p: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for e in std::fs::read_dir(p).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.is_file() && p.extension().is_none_or(|x| x != "log") {
                out.insert(p.display().to_string(), std::fs::read(&p).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, &mut out);
    out
}

#[test]
fn a_data_directory_of_another_format_is_refused_untouched() {
    let mut srv = ServerHandle::start_n(2);
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (_, tid, schema) = create_table(&mut client, schema_of(&[("pk", TypeCode::U64), ("v", TypeCode::I64)]));
    block_on(client.push(tid, rows(&schema, 1..=50), WireConflictMode::Update)).unwrap();
    drop(client);

    let stamp = srv.data_dir().join("FORMAT");
    let ours = std::fs::read_to_string(&stamp).expect("a server stamps its data directory");
    std::fs::write(&stamp, "gnitz data directory\nformat 0000000000000001\nrelease 0.0.1\n").unwrap();

    let refused = srv.restart().expect_err("another format's directory is refused");
    assert!(refused.contains("written by gnitz 0.0.1"), "{refused}");
    let before = files(srv.data_dir());
    let refused = srv.restart().expect_err("and refused again");
    assert!(refused.contains("exit status: 65"), "{refused}");
    assert!(refused.contains("nothing in it was changed"), "{refused}");
    assert_eq!(files(srv.data_dir()), before, "a refused boot changes no file");

    // Its own format again, the same data serves.
    std::fs::write(&stamp, &ours).unwrap();
    srv.restart().unwrap();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    assert_eq!(
        weighted_rows(&scan_all(&mut client, tid, &schema)),
        weighted_rows(&rows(&schema, 1..=50)),
    );
}
