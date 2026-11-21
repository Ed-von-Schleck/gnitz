use super::*;
use crate::runtime::executor::family::FamilyRows;
use crate::test_support::{make_batch, make_schema_u64_i64};
use gnitz_wire::WireConflictMode::{Error, Update};

/// A run is the consecutive families of one table and mode: a change of
/// either starts the next, and a family's chunks are one run.
#[test]
fn a_run_is_the_consecutive_families_of_one_table_and_mode() {
    let family = |tid, mode| TxnFamily::new(tid, mode, FamilyRows::Built(make_batch(&make_schema_u64_i64(), &[])));
    let families = [
        family(1, Update),
        family(1, Update),
        family(2, Error),
        family(1, Update),
        family(1, Error),
        family(1, Error),
        family(1, Error),
    ];
    assert_eq!(&runs(&families)[..], [0, 0, 2, 3, 4, 4, 4]);
}
