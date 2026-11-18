//! [`GnitzClient::read_modify_write`] against a real server: two connections on
//! one table `t(pk, val)` holding `(1, 0)`, and `a` incrementing pk 1's `val` in
//! autocommit while `b` writes around it.

use super::*;
use crate::{PkColumn, RelDescriptor};
use gnitz_wire::{WireStatus, write_u64_le};

struct Fixture {
    _srv: ServerHandle,
    schema_name: String,
    a: GnitzClient,
    b: GnitzClient,
    target: Arc<RelDescriptor>,
}

fn boot() -> Fixture {
    let srv = ServerHandle::start_n(2);
    let mut a = GnitzClient::connect(srv.sock_path()).unwrap();
    let b = GnitzClient::connect(srv.sock_path()).unwrap();
    let (schema_name, ..) = create_table(&mut a, schema_of(&[("pk", TypeCode::I64), ("val", TypeCode::I64)]));
    let target = block_on(a.resolve_relation(&rel(&schema_name, "t"))).unwrap();
    commit(&mut a, &target, 1, 0);
    Fixture { _srv: srv, schema_name, a, b, target }
}

/// Commit `(pk, val)` into `t` through `client`.
fn commit(client: &mut GnitzClient, target: &RelDescriptor, pk: u128, val: i64) {
    let s = &target.schema;
    let mut batch = ZSetBatch::new(s);
    BatchAppender::new(&mut batch).add_row(pk, 1).int_val(val);
    block_on(client.push(target.tid, s, &batch, WireConflictMode::Update)).unwrap();
}

/// `a`'s read of pk 1 and its `val + 1` build, which calls `side` first.
fn run_increment(f: &mut Fixture, side: impl FnOnce(&mut GnitzClient, &RelDescriptor)) -> Result<usize, ClientError> {
    let s = Arc::clone(&f.target.schema);
    let keys = PkColumn::from_natives(&s, [1]).keys();
    let Fixture { a, b, target, .. } = f;
    block_on(
        a.read_modify_write(target, ReadBound::PkSet(keys), Vec::new(), false, |mut rows| {
            side(&mut *b, target);
            for r in 0..rows.len() {
                let v = read_i64_le(rows.col(0), r * 8);
                write_u64_le(rows.col_mut(0), r * 8, (v + 1) as u64);
            }
            Ok::<_, ClientError>(rows)
        }),
    )
}

/// Every row of `t` at pk 1, as `(val, weight)`.
fn row_1(client: &mut GnitzClient, target: &RelDescriptor) -> Vec<(i64, i64)> {
    let b = scan_all(client, target.tid, &target.schema);
    weighted_rows(&b)
        .into_iter()
        .filter(|&(pk, ..)| pk == 1)
        .map(|(_, cells, w)| (cells[0], w))
        .collect()
}

/// A write landing between the read and the push refuses the push, naming the
/// table, after one read and one push; the increment does not land.
#[test]
fn a_write_after_the_read_is_refused_naming_the_table() {
    let mut f = boot();
    let before = f.a.requests_sent();
    let err = run_increment(&mut f, |b, t| commit(b, t, 2, 0)).expect_err("the push conflicts");
    assert_eq!(err.status(), Some(WireStatus::TxnConflict), "{err:?}");
    assert!(err.to_string().contains(&format!("'{}.t'", f.schema_name)), "{err}");
    assert_eq!(f.a.requests_sent() - before, 2, "SCAN_MULTI and PUSH_TXN");
    assert_eq!(row_1(&mut f.a, &f.target), [(0, 1)]);
}

/// A write committed before the statement's read is inside its watermark: the
/// statement commits, in one read and one push.
#[test]
fn a_write_the_read_saw_is_no_conflict() {
    let mut f = boot();
    let target = Arc::clone(&f.target);
    commit(&mut f.b, &target, 1, 100);
    let before = f.a.requests_sent();
    let result = run_increment(&mut f, |_, _| {});
    assert_eq!(result.unwrap(), 1);
    assert_eq!(f.a.requests_sent() - before, 2, "SCAN_MULTI and PUSH_TXN");
    assert_eq!(row_1(&mut f.a, &target), [(101, 1)]);
}
