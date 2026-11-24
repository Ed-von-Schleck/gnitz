//! [`planned_kept`]'s reruns, over a plan that reads no relation and so sends
//! no request of its own.

use super::*;
use gnitz_core::{ClientError, block_on};
use gnitz_test_harness::ServerHandle;
use gnitz_wire::WireFault;

/// `planned_kept` over `client` with a `run` refused `status` on its first
/// `refusals` runs: the outcome, and the runs it made.
fn reruns(client: &mut GnitzClient, status: WireStatus, refusals: usize) -> (Result<SqlResult, GnitzSqlError>, usize) {
    let runs = Cell::new(0);
    let result = block_on(planned_kept(
        client,
        "s",
        Descriptors::KEPT,
        |_: &dyn Catalog| Ok(()),
        |_: &()| true,
        async |_: &mut GnitzClient, (): ()| {
            runs.update(|v| v + 1);
            match runs.get() <= refusals {
                true => Err(GnitzSqlError::Client(ClientError::Refused(WireFault {
                    status,
                    text: String::new(),
                }))),
                false => Ok(SqlResult::TransactionStarted),
            }
        },
    ));
    (result, runs.get())
}

/// A run refused with nothing written — a stale catalog, or an autocommit
/// statement's conflict — is run again up to the bound, and past it the refusal
/// is the caller's. Any other refusal, and a conflict inside an open
/// transaction, is the caller's at once.
#[test]
fn only_a_refusal_that_wrote_nothing_is_run_again() {
    use WireStatus as S;
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let n = RUN_MAX_ATTEMPTS;
    for (in_txn, status, refusals, ok, want_runs) in [
        (false, S::StaleCatalog, n - 1, true, n),
        (false, S::StaleCatalog, n, false, n),
        (false, S::TxnConflict, n - 1, true, n),
        (false, S::TxnConflict, n, false, n),
        (false, S::NotFound, 1, false, 1),
        (true, S::TxnConflict, 1, false, 1),
    ] {
        if in_txn && !client.txn_active() {
            client.txn_begin().unwrap();
        }
        let (result, runs) = reruns(&mut client, status, refusals);
        let what = format!("{status:?} refused {refusals} times, in a transaction: {in_txn}");
        match ok {
            true => assert!(matches!(result, Ok(SqlResult::TransactionStarted)), "{what}"),
            false => assert!(
                matches!(&result, Err(GnitzSqlError::Client(e)) if e.status() == Some(status)),
                "{what}"
            ),
        }
        assert_eq!(runs, want_runs, "{what}");
    }
}
