//! The meta-schema record as this catalog builds and decodes it, and the client
//! halves the engine is paired with: the layout digest and the DDL bundle.
//!
//! It lives on this side because nothing links this crate, so the client is the
//! half that can be pulled in — as a dev-dependency.

use crate::catalog::cache::encode_record;
use crate::catalog::CatalogColumn;
use crate::test_support::arb_schema;
use gnitz_expr::{ColumnTable, SchemaFacts};
use gnitz_wire::schema_block::check_same_types;
use gnitz_wire::{ColumnDef, TypeCode, MAX_PK_COLUMNS};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::{decode_schema_block, SchemaColumn, SchemaDescriptor};
use proptest::prelude::*;

/// The catalog's column records for `schema`, named `c{i}`.
fn catalog_defs(schema: &SchemaDescriptor) -> Vec<CatalogColumn> {
    column_defs(schema).map(|def| CatalogColumn { def, fk: None }).collect()
}

/// The client's schema for `schema`, named `c{i}`.
fn client_schema(schema: &SchemaDescriptor) -> gnitz_core::Schema {
    gnitz_core::Schema {
        columns: column_defs(schema).collect(),
        pk_cols: schema.pk_cols().to_vec(),
    }
}

fn column_defs(schema: &SchemaDescriptor) -> impl Iterator<Item = ColumnDef> + '_ {
    schema.columns[..schema.num_columns()]
        .iter()
        .enumerate()
        .map(|(i, c)| ColumnDef::new(format!("c{i}"), c.type_code, c.nullable))
}

proptest! {
    /// Catalog encoder → catalog decoder.
    #[test]
    fn schema_roundtrip_catalog_codec(schema in arb_schema(MAX_PK_COLUMNS)) {
        let record = encode_record(schema.pk_cols(), &catalog_defs(&schema));
        prop_assert_eq!(decode_schema_block(&record).unwrap(), schema);
    }

    /// A record the catalog admits for a relation — names and hidden flags free
    /// to differ — decodes to the relation's own schema.
    #[test]
    fn an_admitted_record_decodes_as_itself(schema in arb_schema(MAX_PK_COLUMNS), hidden in any::<bool>()) {
        let mut client = client_schema(&schema);
        for c in &mut client.columns {
            c.name = format!("renamed_{}", c.name);
            c.is_hidden = hidden;
        }
        let frame = client.to_block();
        prop_assert_eq!(
            check_same_types(&frame, &encode_record(schema.pk_cols(), &catalog_defs(&schema))),
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
        let flipped: Vec<SchemaColumn> = a.columns[..a.num_columns()]
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

/// Client `encode_ddl_txn` → server `decode_items` → `Batch::decode_from_wal_block`
/// against each family's own schema, for 1-, 2- and 3-family bundles.
///
/// The frame layout itself is `gnitz_wire::txn_frame`'s, round-tripped in that
/// crate, and both ends derive their system schemas from `gnitz_wire::SYS_FAMILIES`
/// rather than hand-keeping them. What is only covered here is the pairing:
/// a client-built WAL block per family, written by the shared row writers the
/// client uses, decoded by the engine against the catalog's own schema for that
/// family — with STRING columns and compound PKs.
#[test]
fn ddl_txn_roundtrip_client_to_server() {
    use gnitz_core::sys_schema;
    use gnitz_core::{BatchAppender, ZSetBatch};
    use gnitz_wire::sys_rows::{CircuitRow, ColTabRow, IdxTabRow, SysRow, TableTabRow, ViewTabRow};
    use gnitz_wire::txn_frame::decode_items;
    use gnitz_wire::{CIRCUIT_TAB, COL_TAB, IDX_TAB, TABLE_TAB, VIEW_TAB};

    // A COL_TAB batch for `oid` with `n` U64 columns.
    let col_batch = |oid: u64, n: usize| -> ZSetBatch {
        let s = sys_schema(COL_TAB);
        let mut b = ZSetBatch::new(s);
        let mut a = BatchAppender::new(&mut b);
        for i in 0..n {
            let col = ColumnDef::new(format!("c{i}"), TypeCode::U64, false);
            let row = ColTabRow {
                owner_id: oid,
                col_idx: i as u64,
                col: &col,
                fk: None,
            };
            row.write(&mut a, 1);
        }
        b
    };
    let table_batch = |tid: u64, weight: i64| -> ZSetBatch {
        let s = sys_schema(TABLE_TAB);
        let mut b = ZSetBatch::new(s);
        let row = TableTabRow {
            table_id: tid,
            schema_id: 3,
            name: "t",
            pk: gnitz_wire::PkColList::from_slice(&[0]),
            props: gnitz_wire::TableProps::default(),
        };
        row.write(&mut BatchAppender::new(&mut b), weight);
        b
    };
    let idx_batch = |idx_id: u64, owner: u64| -> ZSetBatch {
        let s = sys_schema(IDX_TAB);
        let mut b = ZSetBatch::new(s);
        let row = IdxTabRow {
            index_id: idx_id,
            owner_id: owner,
            cols: gnitz_wire::PkColList::from_slice(&[1]),
            name: "idx_t_b",
            is_unique: true,
        };
        row.write(&mut BatchAppender::new(&mut b), 1);
        b
    };

    // Verify a bundle roundtrips: family count, order (tid), and per-row weight
    // and key bytes.
    let verify = |families: &[(u64, ZSetBatch)]| {
        let payload = gnitz_core::encode_ddl_txn(families);
        let ctrl = gnitz_wire::control::peek_control_block(&payload).expect("control header");
        let decoded = decode_items(&payload[ctrl.body], gnitz_wire::ClientVerb::DdlTxn).expect("decode_items");
        assert_eq!(decoded.len(), families.len(), "family count");
        for (fi, ((exp_tid, exp_batch), (frame, item))) in families.iter().zip(&decoded).enumerate() {
            let got_tid = &item.hdr.target_id;
            let slice = &frame[item.data.clone().expect("a DDL_TXN item carries a block")];
            assert_eq!(*got_tid, *exp_tid, "family {fi} tid/order");
            let schema = crate::catalog::SysFamily::from_id(*got_tid)
                .expect("bundle family id must be a system family")
                .schema();
            let batch = Batch::decode_from_wal_block(slice, schema).expect("decode family batch");
            assert_eq!(batch.len(), exp_batch.len(), "row count tid {got_tid}");
            for i in 0..batch.len() {
                assert_eq!(
                    batch.get_weight(i),
                    exp_batch.weights[i],
                    "weight row {i} tid {got_tid}"
                );
                assert_eq!(
                    batch.get_pk_bytes(i),
                    exp_batch.pks.get_bytes(i),
                    "pk row {i} tid {got_tid}"
                );
            }
        }
    };

    // 1-family: DROP TABLE (one TABLE_TAB -1).
    verify(&[(TABLE_TAB, table_batch(16, -1))]);

    // 2-family: CREATE TABLE (COL_TAB + TABLE_TAB).
    verify(&[(COL_TAB, col_batch(17, 2)), (TABLE_TAB, table_batch(17, 1))]);

    // 3-family: CREATE TABLE + inline UNIQUE index (COL_TAB + TABLE_TAB + IDX_TAB).
    verify(&[
        (COL_TAB, col_batch(18, 2)),
        (TABLE_TAB, table_batch(18, 1)),
        (IDX_TAB, idx_batch(100, 18)),
    ]);

    // 3-family: CREATE VIEW (COL_TAB + CIRCUIT_TAB + VIEW_TAB).
    let vid: u64 = 20;
    let src: u64 = 16;
    let circuit = {
        let s = sys_schema(CIRCUIT_TAB);
        let mut b = ZSetBatch::new(s);
        let identity = crate::test_support::identity_circuit(src, gnitz_wire::ReadBound::None);
        CircuitRow { view_id: vid, circuit: &identity }.write(&mut BatchAppender::new(&mut b), 1);
        b
    };
    let view = {
        let s = sys_schema(VIEW_TAB);
        let mut b = ZSetBatch::new(s);
        let row = ViewTabRow {
            view_id: vid,
            schema_id: 3,
            name: "v",
            pk: gnitz_wire::PkColList::from_slice(&[0]),
            props: gnitz_wire::ViewProps::default(),
            owner_view_id: 0,
            pk_repeats: false,
        };
        row.write(&mut BatchAppender::new(&mut b), 1);
        b
    };
    verify(&[(COL_TAB, col_batch(vid, 1)), (CIRCUIT_TAB, circuit), (VIEW_TAB, view)]);
}
