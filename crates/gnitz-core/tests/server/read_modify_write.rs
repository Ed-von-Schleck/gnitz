//! [`GnitzClient::read_modify_write`] against a real server: two connections on
//! one table `t(pk, val)` holding `(1, 0)`, and `a` incrementing pk 1's `val` in
//! autocommit while `b` writes around it.

use super::*;
use gnitz_core::block_on;
use gnitz_core::{PkColumn, RelDescriptor, RMW_MAX_ATTEMPTS};
use gnitz_wire::{WireFault, WireStatus};

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
    let target = block_on(a.resolve_relation(&schema_name, "t")).unwrap();
    commit(&mut a, &target, 1, 0);
    Fixture { _srv: srv, schema_name, a, b, target }
}

/// Commit `(pk, val)` into `t` through `client`.
fn commit(client: &mut GnitzClient, target: &RelDescriptor, pk: u128, val: i64) {
    let s = &target.schema;
    let mut batch = ZSetBatch::new(s);
    BatchAppender::new(&mut batch).add_row(pk, 1).i64_val(val);
    block_on(client.push(target.tid, s, &batch, WireConflictMode::Update)).unwrap();
}

/// `a`'s read of pk 1 and its `val + 1` build, which calls `side` first on every
/// attempt with its 0-based attempt number.
fn run_increment(
    f: &mut Fixture,
    mut side: impl FnMut(&mut GnitzClient, &RelDescriptor, usize),
) -> (Result<usize, ClientError>, usize) {
    let s = Arc::clone(&f.target.schema);
    let keys = PkColumn::from_natives(&s, [1]).keys();
    let mut attempts = 0;
    let Fixture { a, b, target, .. } = f;
    let result = block_on(
        a.read_modify_write(target, ReadBound::PkSet(keys), Vec::new(), false, |mut rows| {
            side(&mut *b, target, attempts);
            attempts += 1;
            for r in 0..rows.len() {
                let v = read_i64_le(&rows.payload[0].bytes, r * 8);
                rows.set_u64_cell(r, 0, (v + 1) as u64);
            }
            Ok::<_, ClientError>(rows)
        }),
    );
    (result, attempts)
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

/// A write landing after every read exhausts the bound, the conflict names the
/// table, and none of the attempts' increments landed.
#[test]
fn sustained_contention_surfaces_a_conflict_naming_the_table() {
    let mut f = boot();
    let (result, attempts) = run_increment(&mut f, |b, t, i| commit(b, t, 2, i as i64));
    let err = result.expect_err("every attempt conflicts");
    assert!(
        matches!(
            &err,
            ClientError::Refused(WireFault { status: WireStatus::TxnConflict, .. })
        ),
        "{err:?}"
    );
    assert!(err.to_string().contains(&format!("'{}.t'", f.schema_name)), "{err}");
    assert_eq!(attempts, RMW_MAX_ATTEMPTS);
    assert_eq!(row_1(&mut f.a, &f.target), [(0, 1)]);
}

/// A write landing after the first read forces one retry, which re-reads: the
/// increment applies to the row that write left, so no update is lost.
#[test]
fn a_conflict_retries_over_a_fresh_read() {
    let mut f = boot();
    let (result, attempts) = run_increment(&mut f, |b, t, i| {
        if i == 0 {
            commit(b, t, 1, 100);
        }
    });
    assert_eq!(result.unwrap(), 1);
    assert_eq!(attempts, 2);
    assert_eq!(row_1(&mut f.a, &f.target), [(101, 1)]);
}

/// A write committed before the statement's read is inside its watermark: the
/// first attempt commits, in one read and one push.
#[test]
fn a_write_the_read_saw_is_no_conflict() {
    let mut f = boot();
    let target = Arc::clone(&f.target);
    commit(&mut f.b, &target, 1, 100);
    let before = f.a.requests_sent();
    let (result, attempts) = run_increment(&mut f, |_, _, _| {});
    assert_eq!(result.unwrap(), 1);
    assert_eq!(attempts, 1);
    assert_eq!(f.a.requests_sent() - before, 2, "SCAN_SPEC and PUSH_TXN");
    assert_eq!(row_1(&mut f.a, &target), [(101, 1)]);
}
