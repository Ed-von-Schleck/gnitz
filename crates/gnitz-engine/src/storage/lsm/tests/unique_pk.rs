use super::enforce_unique_pk;
use crate::schema::type_code;
use crate::storage::{Batch, RecoverySource, Table};
use crate::test_support::{make_batch_opk, opk_pk, pk_payload_schema, wide_row};
use gnitz_expr::RowSource;

/// `enforce_unique_pk` keys its intra-batch state, its store probe and the
/// retraction it emits on verbatim OPK bytes, so it is blind to PK width and
/// signedness — a single-column `I64` and a 24-byte compound key take the same
/// path. Each case is a PK shape and every case runs the same script, which
/// walks each arm the routine has:
///
/// - weight normalization, so a push at `|w| > 1` lands one live instance
///   (a weight-2 row would leave a copy the `-1` retraction can never delete);
/// - the cross-batch upsert, which retracts the stored payload at `-1` on the
///   key's full OPK bytes before inserting the new one;
/// - a delete that nets the store to exactly zero;
/// - a retraction of an absent key and of a tombstoned one, neither of which
///   may pass a negative-weight phantom row through (base tables stay positive);
/// - an intra-batch `+1, -1, +1`, whose delete must clear the insert fact so
///   the re-insert is not re-negated.
///
/// The signed case is also the regression for feeding a sign-flipped `get_pk`
/// back to a re-encoding `retract_pk`: the double flip matched no stored row
/// and dropped the retraction silently.
#[test]
fn enforce_unique_pk_holds_at_every_pk_shape() {
    struct Case {
        name: &'static str,
        pk_types: &'static [u8],
        /// Native PK column values for the two keys the script uses.
        k1: &'static [u128],
        k2: &'static [u128],
    }

    const NEG5: u128 = -5i64 as u64 as u128;
    let cases = [
        Case {
            name: "u64",
            pk_types: &[type_code::U64],
            k1: &[1],
            k2: &[7],
        },
        // A negative leading value: OPK sign-flips it, and nothing may flip twice.
        Case {
            name: "signed i64",
            pk_types: &[type_code::I64],
            k1: &[NEG5],
            k2: &[7],
        },
        Case {
            name: "narrow u8",
            pk_types: &[type_code::U8],
            k1: &[1],
            k2: &[7],
        },
        Case {
            name: "wide 3xu64",
            pk_types: &[type_code::U64; 3],
            k1: &[1, 2, 3],
            k2: &[7, 8, 9],
        },
    ];

    for case in cases {
        let name = case.name;
        let schema = pk_payload_schema(case.pk_types);
        let dir = tempfile::tempdir().unwrap();
        let mut pt = Table::new(
            dir.path().to_str().unwrap(),
            schema,
            1234,
            RecoverySource::Rederive { resume_at: None },
        )
        .unwrap();

        let k1 = opk_pk(&schema, case.k1);
        let k2 = opk_pk(&schema, case.k2);
        let row = |pk: &[u8], w: i64, payload: i64| wide_row(&schema, pk, w, payload);

        // Fresh insert pushed at weight 2 lands exactly one live instance.
        let eff = enforce_unique_pk(&pt, &schema, row(&k1, 2, 100));
        assert_eq!(eff.count, 1, "{name}: fresh insert is one effective row");
        assert_eq!(eff.get_weight(0), 1, "{name}: insert weight normalized to 1");
        pt.ingest_owned_batch(eff).unwrap();
        assert!(pt.has_pk_bytes(&k1), "{name}: row live after the clamped insert");

        // Upsert: the stored payload is retracted at -1 on the key's full OPK
        // bytes, then the new payload is inserted.
        let eff = enforce_unique_pk(&pt, &schema, row(&k1, 2, 200));
        assert_eq!(eff.count, 2, "{name}: stored retraction + new insert");
        assert_eq!(eff.get_weight(0), -1, "{name}: stored-row retraction at -1");
        assert_eq!(eff.get_pk_bytes(0), &k1[..], "{name}: retraction keys on the full PK");
        assert_eq!(eff.get_weight(1), 1, "{name}: upsert weight normalized to 1");
        pt.ingest_owned_batch(eff).unwrap();
        assert!(pt.has_pk_bytes(&k1), "{name}: row live after the upsert");

        // Delete pushed at weight -3 nets the store to exactly zero.
        let eff = enforce_unique_pk(&pt, &schema, row(&k1, -3, 200));
        assert_eq!(eff.count, 1, "{name}: one net retraction row");
        assert_eq!(eff.get_weight(0), -1, "{name}: retraction weight normalized to -1");
        pt.ingest_owned_batch(eff).unwrap();
        assert!(!pt.has_pk_bytes(&k1), "{name}: row fully gone after the delete");

        // Retracting the now-tombstoned key, and an absent one, emit nothing.
        let eff = enforce_unique_pk(&pt, &schema, row(&k1, -1, 0));
        assert_eq!(eff.count, 0, "{name}: tombstoned-key retraction emits no phantom");
        let eff = enforce_unique_pk(&pt, &schema, row(&k2, -1, 0));
        assert_eq!(eff.count, 0, "{name}: absent-key retraction emits no phantom");

        // Intra-batch +1, -1, +1 on a fresh key nets to one live row.
        let churn = make_batch_opk(&schema, &[(&k2, 1, 10), (&k2, -1, 10), (&k2, 1, 20)]);
        let eff = enforce_unique_pk(&pt, &schema, churn);
        pt.ingest_owned_batch(eff).unwrap();
        assert!(pt.has_pk_bytes(&k2), "{name}: key survives +1,-1,+1 at net +1");
    }
}

/// The lazy build's three divergence points, against the row-by-row oracle it
/// replaced. Each case pins where the verbatim run is cut, so a batch that
/// diverges nowhere, at row 0, or in the middle all come out identical.
#[test]
fn enforce_unique_pk_lazy_build_matches_the_row_by_row_oracle() {
    let schema = pk_payload_schema(&[type_code::U64]);
    let key = |v: u128| opk_pk(&schema, &[v]);

    /// The old shape: every surviving row appended one at a time.
    fn oracle(eff: &Batch) -> Vec<(Vec<u8>, i64, i64)> {
        (0..eff.count)
            .map(|r| {
                (
                    eff.get_pk_bytes(r).to_vec(),
                    eff.get_weight(r),
                    i64::from_le_bytes(RowSource::get_col_ptr(eff, r, 0, 8).try_into().unwrap()),
                )
            })
            .collect()
    }

    // A store holding exactly one row, at k0.
    let dir = tempfile::tempdir().unwrap();
    let mut pt = Table::new(
        dir.path().to_str().unwrap(),
        schema,
        4321,
        RecoverySource::Rederive { resume_at: None },
    )
    .unwrap();
    let seed = enforce_unique_pk(&pt, &schema, make_batch_opk(&schema, &[(&key(0), 1, 700)]));
    pt.ingest_owned_batch(seed).unwrap();

    // No divergence: three fresh keys pass through in arrival order, unchanged.
    let fresh = make_batch_opk(&schema, &[(&key(10), 1, 1), (&key(11), 1, 2), (&key(12), 1, 3)]);
    let eff = enforce_unique_pk(&pt, &schema, fresh);
    assert_eq!(
        oracle(&eff),
        vec![(key(10), 1, 1), (key(11), 1, 2), (key(12), 1, 3)],
        "a batch with no divergence is returned verbatim",
    );

    // First divergence in the middle: row 1 hits the stored row, so its
    // retraction lands between the two verbatim runs.
    let mid = make_batch_opk(&schema, &[(&key(20), 1, 1), (&key(0), 1, 2), (&key(21), 1, 3)]);
    let eff = enforce_unique_pk(&pt, &schema, mid);
    assert_eq!(
        oracle(&eff),
        vec![(key(20), 1, 1), (key(0), -1, 700), (key(0), 1, 2), (key(21), 1, 3)],
        "the cut preserves arrival order around the emitted retraction",
    );

    // First divergence at row 0, with the run resuming behind it.
    let head = make_batch_opk(&schema, &[(&key(0), 1, 5), (&key(30), 1, 6)]);
    let eff = enforce_unique_pk(&pt, &schema, head);
    assert_eq!(
        oracle(&eff),
        vec![(key(0), -1, 700), (key(0), 1, 5), (key(30), 1, 6)],
        "a divergence at row 0 still copies the rest verbatim",
    );

    // The case a two-case divergence rule drops: a `w < 0` row for a key that is
    // neither stored nor previously inserted appends nothing at all, so it must
    // cut the run even though it emits no retraction. Returning it verbatim would
    // store a negative-weight base-table row.
    let phantom = make_batch_opk(&schema, &[(&key(40), 1, 1), (&key(99), -1, 0), (&key(41), 1, 2)]);
    let eff = enforce_unique_pk(&pt, &schema, phantom);
    assert_eq!(
        oracle(&eff),
        vec![(key(40), 1, 1), (key(41), 1, 2)],
        "a retraction of an absent key is dropped, not passed through",
    );
}
