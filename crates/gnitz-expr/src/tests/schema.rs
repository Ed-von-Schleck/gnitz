//! The descriptor's layout facts, checked against absolute answers.

use crate::{
    decode_schema_block, encode_schema_block, ColumnLocator, ReduceOutKey, ReduceOutSlot, SchemaColumn,
    SchemaDescriptor, SchemaRefusal,
};
use gnitz_wire::schema_block::SchemaBlockCol;
use gnitz_wire::{ColType, TypeCode, MAX_COLUMNS};

fn col(tc: TypeCode) -> SchemaColumn {
    SchemaColumn::new(tc, false)
}

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

/// Per column: its PK membership, locator and payload slot. Per schema: the
/// regions lie back to back at their columns' widths, an arena below eight rows
/// holds exactly its rows, one of eight or more is the fewest rows from there
/// that start each region 8-aligned, and the schema record round-trips.
#[test]
fn layout_facts_match_the_column_table() {
    for &(cols, pk) in CASES {
        let schema_cols: Vec<SchemaColumn> = cols
            .iter()
            .map(|&(tc, nullable)| SchemaColumn::new(tc, nullable))
            .collect();
        let s = SchemaDescriptor::new(&schema_cols, pk);
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
        let mut want_nullable = 0u64;
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
                    want_nullable |= u64::from(nullable) << next_slot;
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
        let want_strings = cols
            .iter()
            .enumerate()
            .filter_map(|(ci, &(tc, _))| s.payload_slot(ci).filter(|_| tc.is_german_string()))
            .fold(0u64, |m, slot| m | 1 << slot);
        assert_eq!(s.string_payload_slots(), want_strings, "{ctx}: string_payload_slots()");

        // The PK, weight and null regions, then one per payload column.
        let widths: Vec<usize> = [want_stride, 8, 8]
            .into_iter()
            .chain(
                (0..cols.len())
                    .filter(|&ci| !pk.contains(&(ci as u32)))
                    .map(|ci| cols[ci].0.wire_stride()),
            )
            .collect();
        let strides: Vec<usize> = (0..s.num_fixed_regions()).map(|r| s.region_stride(r)).collect();
        assert_eq!(strides, widths, "{ctx}: region strides");
        for rows in [0, 1, 7, 8, 9, 100] {
            let aligned = |cap: usize| (0..s.num_fixed_regions()).all(|r| s.region_start(r, cap).is_multiple_of(8));
            let want = match rows {
                0..8 => rows,
                _ => (rows..rows + 8)
                    .find(|&cap| aligned(cap))
                    .expect("eight rows align any stride"),
            };
            assert_eq!(s.arena_rows(rows), want, "{ctx}: {rows} rows");
        }
        assert_eq!(decode_schema_block(&encode_schema_block(&s)).unwrap(), s, "{ctx}");
    }
}

/// A schema is a trailing append of another iff every earlier column keeps its
/// position, type and PK membership.
#[test]
fn trailing_append_keeps_every_earlier_column() {
    use TypeCode::{String, I64, U64};
    let prev = SchemaDescriptor::new(&[col(U64), col(I64)], &[0]);
    let cases: [(&[SchemaColumn], &[u32], bool, &str); 5] = [
        (&[col(U64), col(I64)], &[0], true, "unchanged"),
        (&[col(U64), col(I64), col(String)], &[0], true, "a column appended"),
        (&[col(U64), col(U64)], &[0], false, "a column retyped"),
        (&[col(U64), col(I64)], &[1], false, "the PK moved"),
        (&[col(U64)], &[0], false, "a column dropped"),
    ];
    for (cols, pk, want, what) in cases {
        assert_eq!(
            SchemaDescriptor::new(cols, pk).is_trailing_append_of(&prev),
            want,
            "{what}"
        );
    }
}

/// `covers_pk` asks containment, not equality: any order, and any extra column.
#[test]
fn covers_pk_accepts_any_superset_of_the_pk_columns() {
    let compound = SchemaDescriptor::new(&[col(TypeCode::U64); 4], &[1, 2]);
    for (cols, want) in [
        (&[1, 2][..], true),
        (&[2, 1], true),
        (&[2, 3, 1], true),
        (&[1], false),
        (&[0, 3], false),
    ] {
        assert_eq!(compound.covers_pk(cols), want, "{cols:?}");
    }
}

/// `try_new` refuses a column count past `MAX_COLUMNS`, counted over the whole
/// list, and forwards the PK rule with each column's own nullability; the decode
/// of a schema record carrying either shape refuses it too. The PK rule's own
/// cases are `gnitz-wire`'s.
#[test]
fn admission_refuses_what_the_constructor_aborts_on() {
    let k = col(TypeCode::U64);
    let cases = [
        ("nullable PK column", vec![SchemaColumn::new(TypeCode::U64, true)]),
        ("column count past MAX_COLUMNS", vec![k; MAX_COLUMNS + 2]),
    ];
    for (what, cols) in &cases {
        assert!(SchemaDescriptor::try_new(cols, &[0]).is_err(), "try_new: {what}");
        let record = gnitz_wire::schema_block::encode(
            cols.iter().map(|c| SchemaBlockCol {
                ty: ColType::of(c.type_code),
                nullable: c.nullable,
                hidden: false,
                name: b"k",
            }),
            &[0],
        );
        assert!(decode_schema_block(&record).is_err(), "decode: {what}");
    }
    assert_eq!(
        SchemaDescriptor::try_new(&[k; MAX_COLUMNS + 2], &[0]),
        Err(SchemaRefusal::TooManyColumns(MAX_COLUMNS + 2))
    );
    assert!(SchemaDescriptor::try_new(&[k, k], &[1, 0]).is_ok());
}

/// A schema record's arity prefix carries no redundancy of its own, so every
/// flip in it must either be refused or change the descriptor.
#[test]
fn no_flip_in_a_schema_records_arity_prefix_is_silently_inert() {
    let cols = [
        col(TypeCode::U64),
        col(TypeCode::I64),
        SchemaColumn::new(TypeCode::I64, true),
        SchemaColumn::new(TypeCode::F64, true),
    ];
    let reference = SchemaDescriptor::new(&cols, &[0]);
    let mut buf = encode_schema_block(&reference);
    for byte in 0..4 + 1 + reference.pk_cols().len() {
        for bit in 0..8 {
            buf[byte] ^= 1 << bit;
            if let Ok(decoded) = decode_schema_block(&buf) {
                assert_ne!(decoded, reference, "prefix byte {byte} bit {bit} changed nothing");
            }
            buf[byte] ^= 1 << bit;
        }
    }
}

/// The reduce output key: the source PK list keys on itself, and so does a
/// single NOT NULL PK-eligible column (eligibility is `TypeCode::is_pk_eligible`'s,
/// swept there); anything else — no column, a nullable, float or string column,
/// a partial, reordered or wider set — folds into a synthetic key. Each schema
/// has three columns, the PK ones `U64 NOT NULL` and the rest of the case's type.
#[test]
#[allow(clippy::type_complexity)]
fn reduce_out_key_picks_the_output_key() {
    use ReduceOutKey::*;
    use TypeCode as T;
    // (pk, group, (non-PK type code, nullable), key)
    let rows: &[(&[u32], &[u32], (TypeCode, bool), ReduceOutKey)] = &[
        (&[0], &[], (T::U64, false), SyntheticFold),
        (&[0], &[1], (T::U64, true), SyntheticFold),
        (&[0], &[1], (T::U64, false), Natural),
        (&[0], &[1], (T::F64, false), SyntheticFold),
        (&[0], &[1], (T::String, false), SyntheticFold),
        (&[0], &[0], (T::String, true), Natural),
        (&[0, 1], &[0, 1], (T::String, true), Natural),
        (&[0, 1], &[1, 0], (T::String, true), SyntheticFold),
        (&[0, 1], &[0], (T::String, true), Natural),
        (&[0, 1], &[0, 1, 2], (T::U64, false), SyntheticFold),
        (&[0, 1], &[1, 2], (T::U64, false), SyntheticFold),
    ];
    for &(pk, group, (tc, nullable), want) in rows {
        let cols: Vec<SchemaColumn> = (0..3)
            .map(|c| match pk.contains(&c) {
                true => col(T::U64),
                false => SchemaColumn::new(tc, nullable),
            })
            .collect();
        let schema = SchemaDescriptor::new(&cols, pk);
        assert_eq!(schema.reduce_out_key(group), want, "{pk:?} {group:?} {tc:?} {nullable}");
    }
}

#[test]
fn reduce_out_layout_is_the_key_region_then_the_unspelled_row() {
    use ReduceOutSlot::{Carried, Key, SyntheticKey};
    let schema = |c2_nullable: bool, pk: &[u32]| {
        let u64_col = |nullable| SchemaColumn::new(TypeCode::U64, nullable);
        SchemaDescriptor::new(
            &[col(TypeCode::U64), u64_col(true), u64_col(c2_nullable), u64_col(true)],
            pk,
        )
    };
    // A nullable column takes the fold key, which spells no input column, so the whole row
    // rides behind it — group columns included, since the synthetic key is not one of them.
    let fold = schema(true, &[0]);
    assert_eq!(
        fold.reduce_out_layout(&[2], 0..4),
        vec![SyntheticKey, Carried(0), Carried(1), Carried(2), Carried(3)]
    );
    // A fold's row is its group set, a repeated column carried once per occurrence.
    assert_eq!(
        fold.reduce_out_layout(&[1, 1], [1, 1]),
        vec![SyntheticKey, Carried(1), Carried(1)]
    );
    // A natural key column is in the PK region, so it is not carried again.
    assert_eq!(
        schema(false, &[0]).reduce_out_layout(&[2], 0..4),
        vec![Key(2), Carried(0), Carried(1), Carried(3)]
    );
    // The PK list keys on every PK column.
    assert_eq!(
        schema(false, &[0, 2]).reduce_out_layout(&[0, 2], 0..4),
        vec![Key(0), Key(2), Carried(1), Carried(3)]
    );
}

/// A layout digest moves with the column count, how many columns the PK holds,
/// a type code and the order of two regions, and not with column numbering.
#[test]
fn layout_digest_separates_every_region_axis() {
    use TypeCode::{String as Str, I64, U64};
    let digest = |tcs: &[TypeCode], pk: &[u32]| {
        let cols: Vec<SchemaColumn> = tcs.iter().map(|&tc| col(tc)).collect();
        SchemaDescriptor::new(&cols, pk).layout_digest()
    };
    let base = digest(&[U64, I64], &[0]);
    assert_ne!(base, digest(&[U64, I64, I64], &[0]), "column count");
    assert_ne!(base, digest(&[U64], &[0]), "column count");
    assert_ne!(base, digest(&[U64, I64], &[0, 1]), "PK arity");
    assert_ne!(base, digest(&[U64, Str], &[0]), "type code");
    assert_ne!(base, digest(&[I64, U64], &[0]), "region order");
    assert_eq!(base, digest(&[I64, U64], &[1]), "column numbering");
}
