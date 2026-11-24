use super::*;

/// `validate` refuses what a `TABLE_TAB` row can hold but the table cannot: a
/// distribution prefix longer than the PK, a compressed stream, and a SERIAL
/// table that is a stream or whose PK is not its one generated column.
#[test]
fn table_props_validate_refuses_what_the_pk_cannot_carry() {
    let keyed = |prefix_len| TableProps {
        distribution: TableDistribution::Keyed { prefix_len },
        ..TableProps::default()
    };
    assert_eq!(keyed(3).validate(3), Ok(()));
    assert!(keyed(4).validate(3).unwrap_err().contains("exceeds PK column count"));
    let serial = TableProps { serial: true, ..TableProps::default() };
    assert_eq!(serial.validate(1), Ok(()));
    assert!(serial.validate(2).unwrap_err().contains("one SERIAL column"));
    let stream = TableProps { stream: true, ..serial };
    assert!(stream.validate(1).unwrap_err().contains("a stream cannot be SERIAL"));
    let compressed = TableProps { compress: true, ..stream };
    assert!(
        compressed
            .validate(1)
            .unwrap_err()
            .contains("a stream cannot be compressed")
    );
}

/// `cluster_by` gives a keyed distribution the prefix, and refuses one on a
/// replicated table unless it is `0`, no prefix at all.
#[test]
fn cluster_by_takes_a_prefix_only_on_a_keyed_distribution() {
    let keyed = TableDistribution::Keyed { prefix_len: 3 };
    assert_eq!(keyed.cluster_by(1), Ok(TableDistribution::Keyed { prefix_len: 1 }));
    assert_eq!(keyed.cluster_by(0), Ok(TableDistribution::default()));
    assert_eq!(
        TableDistribution::Replicated.cluster_by(0),
        Ok(TableDistribution::Replicated)
    );
    assert!(
        TableDistribution::Replicated
            .cluster_by(2)
            .unwrap_err()
            .contains("(k=2)")
    );
}

/// Each budget alone names its view class; both at once are refused.
#[test]
fn from_budgets_names_one_view_class() {
    let n = NonZeroU64::new(7);
    assert_eq!(ViewBudget::from_budgets(None, None), Ok(ViewBudget::Plain));
    assert_eq!(
        ViewBudget::from_budgets(n, None),
        Ok(ViewBudget::Bounded { capacity_bytes: n.unwrap() })
    );
    assert_eq!(
        ViewBudget::from_budgets(None, n),
        Ok(ViewBudget::Fed { delta_bytes: n.unwrap() })
    );
    assert!(
        ViewBudget::from_budgets(n, n)
            .unwrap_err()
            .contains("cannot carry a delta feed")
    );
}
