//! The meta-schema record as this catalog builds and decodes it, and the client
//! halves the engine is paired with: the layout digest and the DDL bundle.
//!
//! It lives on this side because nothing links this crate, so the client is the
//! half that can be pulled in — as a dev-dependency.

use crate::catalog::cache::CatalogRecord;
use crate::catalog::CatalogColumn;
use crate::test_support::arb_type_code;
use gnitz_expr::{ColumnTable, SchemaFacts};
use gnitz_store::schema::{decode_schema_block, SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::Batch;
use gnitz_wire::{ColumnDef, TypeCode, MAX_PK_COLUMNS};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

/// One generated schema — per column its type and nullability, and the PK list
/// in declared order — from which every builder below is fed, so each describes
/// the same columns.
type Cols = (Vec<TypeCode>, Vec<bool>, Vec<u32>);

/// `max_pk` bounds the generated PK arity: the engine's schemas run to
/// `MAX_PK_COLUMNS` (5, the secondary-index schema width), but the persisted
/// client codec caps at `PK_LIST_MAX_COLS` (4) — tests that go through the
/// client (`Schema::from_block` → `Schema::validate`) must stay within it.
fn arb_schema(max_pk: usize) -> impl Strategy<Value = Cols> {
    // n_cols ≥ 1, so `1..=n_cols.min(max_pk)` is never empty.
    (1usize..=8)
        .prop_flat_map(move |n_cols| {
            (
                Just(n_cols),
                vec(arb_type_code(), n_cols), // column types
                vec(any::<bool>(), n_cols),   // nullability
                vec(any::<u32>(), n_cols),    // permutation weights
                1usize..=n_cols.min(max_pk),  // PK arity
            )
        })
        .prop_map(|(n_cols, types, nullables, weights, k)| {
            // PK index set = first `k` columns ordered by their weight.
            // The order is the "declared" PK order the encoder must preserve.
            let mut idx: Vec<u32> = (0..n_cols as u32).collect();
            idx.sort_by_key(|&i| weights[i as usize]);
            let pk: Vec<u32> = idx[..k].to_vec();
            // PK columns must be PK-eligible and non-nullable; remap ineligible
            // draws to U64 so every builder accepts the schema.
            let is_pk = |i: usize| pk.contains(&(i as u32));
            let types = (0..n_cols)
                .map(|i| {
                    if is_pk(i) && !types[i].is_pk_eligible() {
                        TypeCode::U64
                    } else {
                        types[i]
                    }
                })
                .collect();
            let nullables = (0..n_cols).map(|i| !is_pk(i) && nullables[i]).collect();
            (types, nullables, pk)
        })
}

fn descriptor((types, nullables, pk): &Cols) -> SchemaDescriptor {
    let cols: Vec<SchemaColumn> = types
        .iter()
        .zip(nullables)
        .map(|(&tc, &nullable)| SchemaColumn::new(tc, nullable))
        .collect();
    SchemaDescriptor::new(&cols, pk)
}

/// The catalog's column records, named `c{i}`.
fn catalog_defs((types, nullables, _): &Cols) -> Vec<CatalogColumn> {
    types
        .iter()
        .zip(nullables)
        .enumerate()
        .map(|(i, (&tc, &nullable))| ColumnDef::new(format!("c{i}"), tc, nullable).into())
        .collect()
}

/// The client's schema, named `c{i}`.
fn client_schema((types, nullables, pk): &Cols) -> gnitz_core::Schema {
    let columns = types
        .iter()
        .zip(nullables)
        .enumerate()
        .map(|(i, (&tc, &nullable))| ColumnDef::new(format!("c{i}"), tc, nullable))
        .collect();
    gnitz_core::Schema { columns, pk_cols: pk.clone() }
}

fn assert_descriptor_eq(a: &SchemaDescriptor, b: &SchemaDescriptor) -> Result<(), TestCaseError> {
    prop_assert_eq!(
        a.pk_cols(),
        b.pk_cols(),
        "pk_indices (declared order) changed on round-trip"
    );
    prop_assert_eq!(a.num_columns(), b.num_columns(), "column count changed on round-trip");
    for i in 0..a.num_columns() {
        prop_assert_eq!(
            a.columns[i].type_code,
            b.columns[i].type_code,
            "type_code at col {} changed",
            i
        );
        prop_assert_eq!(
            a.columns[i].nullable,
            b.columns[i].nullable,
            "nullable at col {} changed",
            i
        );
    }
    Ok(())
}

proptest! {
    /// Catalog encoder → catalog decoder.
    #[test]
    fn schema_roundtrip_catalog_codec(cols in arb_schema(MAX_PK_COLUMNS)) {
        let wire = CatalogRecord::new(&cols.2, &catalog_defs(&cols)).bytes;
        let decoded = decode_schema_block(&wire)
            .expect("decode must succeed for any valid schema");
        assert_descriptor_eq(&descriptor(&cols), &decoded)?;
    }

    /// A record the catalog admits for a relation — names and hidden flags free
    /// to differ — decodes to exactly what decoding it would give.
    #[test]
    fn an_admitted_record_decodes_as_itself(cols in arb_schema(MAX_PK_COLUMNS), hidden in any::<bool>()) {
        let catalog = CatalogRecord::new(&cols.2, &catalog_defs(&cols));
        let mut client = client_schema(&cols);
        for c in &mut client.columns {
            c.name = format!("renamed_{}", c.name);
            c.is_hidden = hidden;
        }
        let frame = client.to_block();
        prop_assert_eq!(catalog.decode_of(&frame).unwrap(), decode_schema_block(&frame).unwrap());
    }
}

proptest! {
    /// Two layouts share a digest iff `same_layout` holds — probed against an
    /// unrelated schema and against `a` with every payload column's nullability
    /// flipped.
    #[test]
    fn layout_digest_agrees_with_same_layout(
        a in arb_schema(gnitz_wire::PK_LIST_MAX_COLS),
        b in arb_schema(gnitz_wire::PK_LIST_MAX_COLS),
    ) {
        let (a, b) = (descriptor(&a), descriptor(&b));
        let flipped: Vec<SchemaColumn> = a.columns[..a.num_columns()]
            .iter()
            .enumerate()
            .map(|(i, c)| SchemaColumn::new(c.type_code, !a.pk_cols().contains(&(i as u32)) && !c.nullable))
            .collect();
        let twin = SchemaDescriptor::new(&flipped, a.pk_cols());
        for other in [&b, &twin] {
            prop_assert_eq!(a.same_layout(other), a.layout_digest() == other.layout_digest());
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
    use gnitz_wire::sys_rows::{
        write_circuit_node_row, write_col_tab_row, write_idx_tab_row, write_table_tab_row, write_view_tab_row,
        CircuitNodeRow, ColTabRow, IdxTabRow, TableTabRow, ViewTabRow,
    };
    use gnitz_wire::txn_frame::decode_items;
    use gnitz_wire::{CIRCUIT_NODES_TAB, COL_TAB, IDX_TAB, TABLE_TAB, VIEW_TAB};

    // A COL_TAB batch for `oid` with `n` U64 columns.
    let col_batch = |oid: u64, n: usize| -> ZSetBatch {
        let s = sys_schema(COL_TAB);
        let mut b = ZSetBatch::new(s);
        let mut a = BatchAppender::new(&mut b, s);
        for i in 0..n {
            let col = ColumnDef::new(format!("c{i}"), TypeCode::U64, false);
            let row = ColTabRow {
                owner_id: oid,
                col_idx: i as u64,
                col: &col,
                fk_table_id: 0,
                fk_col_idx: 0,
            };
            write_col_tab_row(&mut a, &row, 1);
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
            pk_col_idx: 0,
            flags: 0,
        };
        write_table_tab_row(&mut BatchAppender::new(&mut b, s), &row, weight);
        b
    };
    let idx_batch = |idx_id: u64, owner: u64| -> ZSetBatch {
        let s = sys_schema(IDX_TAB);
        let mut b = ZSetBatch::new(s);
        let row = IdxTabRow {
            index_id: idx_id,
            owner_id: owner,
            source_col_idx: gnitz_wire::pack_pk_cols(&[1]),
            name: "idx_t_b",
            flags: 1, // unique, not internal
        };
        write_idx_tab_row(&mut BatchAppender::new(&mut b, s), &row, 1);
        b
    };

    // Verify a bundle roundtrips: family count, order (tid), per-row weight,
    // and — where `check_pk` says so — the PK. It is off for the compound-PK
    // families, whose two scalar forms differ (the engine reads the widened key
    // high-half-first, the client low-half-first) over identical wire bytes.
    let verify = |families: &[(u64, ZSetBatch)], check_pk: &[bool]| {
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
                if check_pk[fi] {
                    assert_eq!(
                        batch.get_pk(i),
                        exp_batch.pks.get(gnitz_core::sys_schema(*exp_tid), i),
                        "pk row {i} tid {got_tid}"
                    );
                }
            }
        }
    };

    // 1-family: DROP TABLE (one TABLE_TAB -1).
    verify(&[(TABLE_TAB, table_batch(16, -1))], &[true]);

    // 2-family: CREATE TABLE (COL_TAB + TABLE_TAB).
    verify(
        &[(COL_TAB, col_batch(17, 2)), (TABLE_TAB, table_batch(17, 1))],
        &[false, true],
    );

    // 3-family: CREATE TABLE + inline UNIQUE index (COL_TAB + TABLE_TAB + IDX_TAB).
    verify(
        &[
            (COL_TAB, col_batch(18, 2)),
            (TABLE_TAB, table_batch(18, 1)),
            (IDX_TAB, idx_batch(100, 18)),
        ],
        &[false, true, true],
    );

    // 3-family: CREATE VIEW (COL_TAB + circuit nodes + VIEW_TAB).
    let vid: u64 = 20;
    let src: u64 = 16;
    let nodes = {
        let s = sys_schema(CIRCUIT_NODES_TAB);
        let mut b = ZSetBatch::new(s);
        let mut a = BatchAppender::new(&mut b, s);
        // ScanDelta(src), unwired and parameterless.
        let scan = CircuitNodeRow {
            view_id: vid,
            node_id: 1,
            opcode: gnitz_wire::Opcode::ScanDelta.as_wire(),
            source_table: Some(src),
            inputs: [None, None],
            params: None,
        };
        write_circuit_node_row(&mut a, &scan, 1);
        // Integrate, fed by node 1.
        let sink = CircuitNodeRow {
            node_id: 2,
            opcode: gnitz_wire::Opcode::IntegrateSink.as_wire(),
            source_table: None,
            inputs: [Some(1), None],
            ..scan
        };
        write_circuit_node_row(&mut a, &sink, 1);
        b
    };
    let view = {
        let s = sys_schema(VIEW_TAB);
        let mut b = ZSetBatch::new(s);
        let row = ViewTabRow {
            view_id: vid,
            schema_id: 3,
            name: "v",
            pk_col_idx: 0,
            props: gnitz_wire::ViewProps::default(),
            owner_view_id: 0,
            pk_repeats: false,
        };
        write_view_tab_row(&mut BatchAppender::new(&mut b, s), &row, 1);
        b
    };
    verify(
        &[
            (COL_TAB, col_batch(vid, 1)),
            (CIRCUIT_NODES_TAB, nodes),
            (VIEW_TAB, view),
        ],
        // Skip the pk check on the two compound-PK families.
        &[false, false, true],
    );
}
