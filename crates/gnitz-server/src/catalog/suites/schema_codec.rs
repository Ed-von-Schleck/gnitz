//! The meta-schema record as this catalog builds and decodes it, and the client
//! halves the engine is paired with: the layout digest and the DDL bundle.
//!
//! It lives on this side because nothing links this crate, so the client is the
//! half that can be pulled in — as a dev-dependency.

use crate::catalog::sys_tables::encode_record;
use crate::test_support::{arb_schema, cols_of};

use gnitz_wire::schema_block::check_same_types;
use gnitz_wire::{ColumnDef, MAX_PK_COLUMNS};
use gnitz_zset::repr::{Batch, MemBatch};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor, decode_schema_block};
use proptest::prelude::*;

proptest! {
    /// Catalog encoder → catalog decoder.
    #[test]
    fn schema_roundtrip_catalog_codec(schema in arb_schema(MAX_PK_COLUMNS)) {
        let record = encode_record(schema.pk_cols(), &cols_of(&schema));
        prop_assert_eq!(decode_schema_block(&record).unwrap(), schema);
    }

    /// A record the catalog admits for a relation — names and hidden flags free
    /// to differ — decodes to the relation's own schema.
    #[test]
    fn an_admitted_record_decodes_as_itself(schema in arb_schema(MAX_PK_COLUMNS), hidden in any::<bool>()) {
        let mut defs: Vec<ColumnDef> = cols_of(&schema).into_iter().map(|c| c.def).collect();
        for c in &mut defs {
            c.name = format!("renamed_{}", c.name).into();
            c.is_hidden = hidden;
        }
        let frame = gnitz_wire::schema_block::encode(defs.iter().map(ColumnDef::block_col), schema.pk_cols());
        prop_assert_eq!(
            check_same_types(&frame, &encode_record(schema.pk_cols(), &cols_of(&schema))),
            Ok(())
        );
        prop_assert_eq!(decode_schema_block(&frame).unwrap(), schema);
    }
}

proptest! {
    /// Two layouts share a digest iff their regions have the same types — probed
    /// against an unrelated schema and against `a` with every payload column's
    /// nullability flipped.
    #[test]
    fn layout_digest_agrees_with_the_region_types(
        a in arb_schema(gnitz_wire::PK_LIST_MAX_COLS),
        b in arb_schema(gnitz_wire::PK_LIST_MAX_COLS),
    ) {
        let flipped: Vec<SchemaColumn> = a.columns()
            .iter()
            .enumerate()
            .map(|(i, c)| SchemaColumn::new(c.type_code, !a.pk_cols().contains(&(i as u32)) && !c.nullable))
            .collect();
        let twin = SchemaDescriptor::new(&flipped, a.pk_cols());
        for other in [&b, &twin] {
            prop_assert_eq!(a.same_region_types(other), a.layout_digest() == other.layout_digest());
        }
    }
}

/// A DDL bundle the client builds, as the engine reads it: each family's block,
/// written through the client's sink, decodes against the catalog's own schema
/// for that family to the rows the client wrote, weights included — STRING and
/// BLOB cells past the inline bytes, an empty BLOB and the pair-keyed family's
/// two key words among them.
///
/// The frame layout itself is `gnitz_wire::txn_frame`'s, round-tripped in that
/// crate.
#[test]
fn a_client_bundle_decodes_to_the_rows_it_wrote() {
    use gnitz_core::{BatchAppender, ZSetBatch};
    use gnitz_wire::txn_frame::decode_items;
    use gnitz_wire::{CircuitRow, ColTabRow, IdxTabRow, SchemaTabRow, SeqTabRow, SysRow, TableTabRow, ViewTabRow};

    fn block<R: SysRow>(rows: &[(R, i64)]) -> (u64, ZSetBatch) {
        let mut b = ZSetBatch::new(gnitz_core::sys_schema(R::FAMILY));
        let mut sink = BatchAppender::new(&mut b);
        for (row, weight) in rows {
            row.write(&mut sink, *weight);
        }
        (R::FAMILY, b)
    }
    fn read_back<'b, R, E>(
        batch: &'b Batch,
        read: impl Fn(&'b Batch, usize) -> Result<R, E>,
    ) -> Result<Vec<(R, i64)>, E> {
        (0..batch.len())
            .map(|i| Ok((read(batch, i)?, batch.get_weight(i))))
            .collect()
    }

    let schemas = [(
        SchemaTabRow {
            schema_id: 7,
            name: "a_long_schema_name_past_twelve".into(),
        },
        1,
    )];
    let tables = [(
        TableTabRow {
            table_id: 9,
            schema_id: 7,
            name: "a_long_table_name_past_twelve".into(),
            pk_col_idx: gnitz_wire::PkColList::from_slice(&[1, 3]),
            flags: gnitz_wire::TableProps {
                serial: true,
                distribution: gnitz_wire::TableDistribution::Replicated,
                ..Default::default()
            },
        },
        -1,
    )];
    let views = [(
        ViewTabRow {
            view_id: 11,
            schema_id: 7,
            name: "v".into(),
            pk_col_idx: gnitz_wire::PkColList::from_slice(&[0]),
            capacity_bytes: 4 << 20,
            delta_bytes: 32 << 20,
            owner_view_id: 10,
            pk_repeats: true,
            compress: false,
            definition: Default::default(),
        },
        1,
    )];
    let column = |col_idx, name: &'static str| ColTabRow {
        owner_id: 0x1122,
        col_idx,
        name: name.into(),
        type_code: 8,
        is_nullable: true,
        fk_table_id: 12,
        fk_col_idx: 2,
        fk_on_delete: 1,
        is_hidden: true,
        scale: 4,
    };
    let columns = [
        (column(0xAABB, "c"), 1),
        (column(0xAABC, "a_column_name_past_twelve"), 1),
    ];
    let indexes = [(
        IdxTabRow {
            index_id: 70,
            owner_id: 16,
            source_col_idx: gnitz_wire::PkColList::from_slice(&[2, 1]),
            name: "i".into(),
            is_unique: true,
        },
        1,
    )];
    let sequences = [(SeqTabRow { seq_id: 3, next_val: u64::MAX }, 1)];
    let identity = crate::test_support::identity_circuit(16, gnitz_wire::ReadBound::None).encode();
    let circuits = [
        (
            CircuitRow {
                view_id: 11,
                circuit: identity.as_slice().into(),
            },
            1,
        ),
        (CircuitRow { view_id: 12, circuit: (&[][..]).into() }, 1),
    ];

    let bundle = [
        block(&schemas),
        block(&tables),
        block(&views),
        block(&columns),
        block(&indexes),
        block(&sequences),
        block(&circuits),
    ];
    let payload = gnitz_core::encode_ddl_txn(&bundle);
    let ctrl = gnitz_wire::peek_control_block(&payload).expect("control header");
    let items = decode_items(&payload, ctrl.body, gnitz_wire::ClientVerb::DdlTxn).expect("decode_items");
    let ids: Vec<u64> = items.iter().map(|item| item.hdr.target_id).collect();
    assert_eq!(ids, bundle.iter().map(|(id, _)| *id).collect::<Vec<_>>());
    let b: Vec<Batch> = items
        .iter()
        .map(|item| {
            let schema = crate::catalog::SysFamily::from_id(item.hdr.target_id).unwrap().schema();
            MemBatch::of_wal_block(&payload[item.data.clone().unwrap()], schema)
                .unwrap()
                .to_batch()
        })
        .collect();

    assert_eq!(read_back(&b[0], SchemaTabRow::read), Ok(schemas.to_vec()));
    assert_eq!(read_back(&b[1], TableTabRow::read), Ok(tables.to_vec()));
    assert_eq!(read_back(&b[2], ViewTabRow::read), Ok(views.to_vec()));
    assert_eq!(read_back(&b[3], ColTabRow::read), Ok(columns.to_vec()));
    assert_eq!(read_back(&b[4], IdxTabRow::read), Ok(indexes.to_vec()));
    assert_eq!(read_back(&b[5], SeqTabRow::read), Ok(sequences.to_vec()));
    assert_eq!(read_back(&b[6], CircuitRow::read), Ok(circuits.to_vec()));
}
