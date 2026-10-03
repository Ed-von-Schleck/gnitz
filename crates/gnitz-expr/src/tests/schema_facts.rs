//! The one [`SchemaFacts`] derivation, checked against absolute answers.

use crate::test_support::TestSchema;
use crate::{ColumnLocator, ColumnTable, SchemaFacts};
use gnitz_wire::TypeCode;

/// One case: a column table as `(type_code, nullable)`, and its PK list in
/// PK-LIST order.
type Case = (&'static [(TypeCode, bool)], &'static [u32]);

const CASES: &[Case] = &[
    // Single unsigned PK at column 0; U64/F64/STRING/U128 payload, one nullable.
    (
        &[
            (TypeCode::U64, false),
            (TypeCode::U64, false),
            (TypeCode::F64, true),
            (TypeCode::String, false),
            (TypeCode::U128, false),
        ],
        &[0],
    ),
    // Single signed PK NOT at column 0 — the payload slots renumber around it,
    // so the `ci - 1` closed form does not hold.
    (
        &[
            (TypeCode::String, true),
            (TypeCode::I64, false),
            (TypeCode::U64, false),
            (TypeCode::F32, true),
        ],
        &[1],
    ),
    // Compound two-column PK (signed + unsigned, mixed widths).
    (
        &[
            (TypeCode::I32, false),
            (TypeCode::U16, false),
            (TypeCode::F64, false),
            (TypeCode::Blob, true),
        ],
        &[0, 1],
    ),
    // PK-list order reverses column order, with a column in between: OPK
    // offsets follow the PK list.
    (
        &[
            (TypeCode::U32, false),
            (TypeCode::String, true),
            (TypeCode::F64, false),
            (TypeCode::I64, false),
        ],
        &[3, 0],
    ),
    // Every fixed width 1/2/4/8 as a PK column, plus a 16-byte payload column.
    (
        &[
            (TypeCode::I8, false),
            (TypeCode::U16, false),
            (TypeCode::I32, false),
            (TypeCode::U64, false),
            (TypeCode::I128, true),
            (TypeCode::UUID, false),
        ],
        &[0, 1, 2, 3],
    ),
    // Every fixed width as a payload column, all nullable.
    (
        &[
            (TypeCode::U64, false),
            (TypeCode::U8, true),
            (TypeCode::I16, true),
            (TypeCode::U32, true),
            (TypeCode::I64, true),
            (TypeCode::U128, true),
        ],
        &[0],
    ),
    // PK-only: no payload columns at all.
    (&[(TypeCode::U32, false)], &[0]),
];

#[test]
fn schema_facts_match_the_column_table() {
    for &(cols, pk) in CASES {
        let s = TestSchema::new(cols, pk);
        let ctx = format!("cols {cols:?}, pk {pk:?}");

        assert_eq!(s.pk_cols(), pk, "{ctx}: pk_cols()");
        assert_eq!(s.num_columns(), cols.len(), "{ctx}: num_columns()");
        assert_eq!(s.num_payload_cols(), cols.len() - pk.len(), "{ctx}: num_payload_cols()");
        let want_stride: usize = pk.iter().map(|&p| cols[p as usize].0.wire_stride()).sum();
        assert_eq!(s.pk_stride(), want_stride, "{ctx}: pk_stride()");

        // Expected OPK byte offset per PK column: the running sum in PK-list order.
        let mut pk_off = vec![0usize; cols.len()];
        let mut running = 0usize;
        for &p in pk {
            pk_off[p as usize] = running;
            running += cols[p as usize].0.wire_stride();
        }

        // Expected payload slots: non-PK columns numbered left to right.
        let (mut want_nullable, mut want_not_null) = (0u64, 0u64);
        let mut next_slot = 0usize;
        for (ci, &(want_tc, nullable)) in cols.iter().enumerate() {
            let want_pk = pk.contains(&(ci as u32));
            assert_eq!(s.is_pk_col(ci), want_pk, "{ctx}: is_pk_col({ci})");

            let loc = s.locate(ci);
            assert_eq!(loc.type_code(), want_tc, "{ctx}: locate({ci}).type_code()");
            assert_eq!(loc.size(), want_tc.wire_stride(), "{ctx}: locate({ci}).size()");
            match loc {
                ColumnLocator::Pk { byte_off, .. } => {
                    assert!(want_pk, "{ctx}: locate({ci}) is Pk for a payload column");
                    assert_eq!(byte_off as usize, pk_off[ci], "{ctx}: locate({ci}) OPK byte offset");
                    assert_eq!(s.payload_slot(ci), None, "{ctx}: payload_slot({ci})");
                }
                ColumnLocator::Payload { slot, .. } => {
                    assert!(!want_pk, "{ctx}: locate({ci}) is Payload for a PK column");
                    assert_eq!(slot as usize, next_slot, "{ctx}: locate({ci}) payload slot");
                    assert_eq!(s.payload_slot(ci), Some(next_slot), "{ctx}: payload_slot({ci})");
                    assert_eq!(s.payload_col_idx(next_slot), ci, "{ctx}: payload_col_idx({next_slot})");
                    *if nullable {
                        &mut want_nullable
                    } else {
                        &mut want_not_null
                    } |= 1u64 << next_slot;
                    next_slot += 1;
                }
            }
        }

        assert_eq!(s.try_locate(s.num_columns()), None, "{ctx}: try_locate(num_columns())");
        // An index that truncates onto a PK column as a `u32` is still out of range.
        assert!(!s.is_pk_col(pk[0] as usize + (1 << 32)), "{ctx}: is_pk_col past u32");
        assert_eq!(
            s.payload_slot(s.num_columns()),
            None,
            "{ctx}: payload_slot(num_columns())"
        );
        assert_eq!(
            s.nullable_payload_slots(),
            want_nullable,
            "{ctx}: nullable_payload_slots()"
        );
        assert_eq!(
            s.not_null_payload_slots(),
            // Every bit past the last payload slot is one no batch may set.
            want_not_null | !gnitz_wire::low_bits_mask(next_slot),
            "{ctx}: not_null_payload_slots()"
        );
    }
}
