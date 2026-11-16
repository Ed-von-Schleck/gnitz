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

/// A run refused with nothing written is run again up to the bound, and past
/// it the refusal is the caller's.
#[test]
fn a_refusal_that_wrote_nothing_is_run_again() {
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    for status in [WireStatus::StaleCatalog, WireStatus::TxnConflict] {
        let (result, runs) = reruns(&mut client, status, RUN_MAX_ATTEMPTS - 1);
        assert!(matches!(result, Ok(SqlResult::TransactionStarted)), "{status:?}");
        assert_eq!(runs, RUN_MAX_ATTEMPTS, "{status:?}");
        let (result, runs) = reruns(&mut client, status, RUN_MAX_ATTEMPTS);
        assert!(
            matches!(&result, Err(GnitzSqlError::Client(e)) if e.status() == Some(status)),
            "{status:?}"
        );
        assert_eq!(runs, RUN_MAX_ATTEMPTS, "{status:?}");
    }
}

/// Any other refusal is the caller's at once.
#[test]
fn another_refusal_is_not_run_again() {
    let srv = ServerHandle::start();
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let (result, runs) = reruns(&mut client, WireStatus::NotFound, 1);
    assert!(result.is_err());
    assert_eq!(runs, 1);
}
