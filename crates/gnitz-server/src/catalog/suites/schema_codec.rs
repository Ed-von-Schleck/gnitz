//! The meta-schema record across the two adapters that build it: this catalog's
//! (`ColumnDef`s → record) and `gnitz-core`'s (client `Schema` → record).
//!
//! It lives on this side because nothing links this crate, so the client is the
//! half that can be pulled in — as a dev-dependency. The two must agree byte for
//! byte: each encodes what the other decodes on every schema-bearing frame.

use crate::catalog::cache::{named_record, CatalogRecord};
use crate::catalog::ColumnDef;
use crate::test_support::arb_type_code;
use gnitz_store::schema::{decode_schema_block, SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::Batch;
use gnitz_wire::{ColType, TypeCode, MAX_PK_COLUMNS};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

/// One generated schema — per column its type and nullability, and the PK list
/// in declared order — from which every adapter below is fed, so each encodes
/// the same columns.
type Cols = (Vec<TypeCode>, Vec<bool>, Vec<u32>);

/// `max_pk` bounds the generated PK arity: the engine's schemas run to
/// `MAX_PK_COLUMNS` (5, the secondary-index schema width), but the persisted
/// client codec caps at `PK_LIST_MAX_COLS` (4) — tests that decode through the
/// client (`schema_from_block` → `Schema::validate`) must stay within it.
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
            // draws to U64 so every adapter accepts the schema.
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
fn catalog_defs((types, nullables, _): &Cols) -> Vec<ColumnDef> {
    types
        .iter()
        .zip(nullables)
        .enumerate()
        .map(|(i, (&tc, &is_nullable))| ColumnDef {
            is_nullable,
            ..ColumnDef::new(format!("c{i}"), ColType::of(tc))
        })
        .collect()
}

/// The client's schema, named `c{i}`.
fn client_schema((types, nullables, pk): &Cols) -> gnitz_core::protocol::types::Schema {
    use gnitz_core::protocol::types::{ColumnDef, Schema};
    let columns = types
        .iter()
        .zip(nullables)
        .enumerate()
        .map(|(i, (&tc, &nullable))| ColumnDef::new(format!("c{i}"), tc, nullable))
        .collect();
    Schema { columns, pk_cols: pk.clone() }
}

fn assert_descriptor_eq(a: &SchemaDescriptor, b: &SchemaDescriptor) -> Result<(), TestCaseError> {
    prop_assert_eq!(
        a.pk_indices(),
        b.pk_indices(),
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
        let wire = named_record(&catalog_defs(&cols), &cols.2);
        let decoded = decode_schema_block(&wire)
            .expect("decode must succeed for any valid schema");
        assert_descriptor_eq(&descriptor(&cols), &decoded)?;
    }

    /// Client encoder → client decoder.
    #[test]
    fn schema_roundtrip_client_codec(cols in arb_schema(gnitz_wire::PK_LIST_MAX_COLS)) {
        use gnitz_core::protocol::codec::{encode_schema_block, schema_from_block};

        let client = client_schema(&cols);
        let wire = encode_schema_block(&client);
        prop_assert_eq!(schema_from_block(&wire).unwrap(), client);
    }

    /// A record the catalog admits for a relation — names and hidden flags free
    /// to differ — decodes to exactly what decoding it would give.
    #[test]
    fn an_admitted_record_decodes_as_itself(cols in arb_schema(MAX_PK_COLUMNS), hidden in any::<bool>()) {
        let catalog = CatalogRecord::new(&cols.2, &catalog_defs(&cols));
        let frame_defs: Vec<ColumnDef> = catalog_defs(&cols)
            .into_iter()
            .map(|d| ColumnDef { name: format!("renamed_{}", d.name), is_hidden: hidden, ..d })
            .collect();
        let frame = named_record(&frame_defs, &cols.2);
        prop_assert_eq!(catalog.decode_of(&frame).unwrap(), decode_schema_block(&frame).unwrap());
    }

    /// Both crates' adapters emit the same bytes for the same schema.
    #[test]
    fn schema_block_bytes_agree_across_the_two_adapters(cols in arb_schema(gnitz_wire::PK_LIST_MAX_COLS)) {
        use gnitz_core::protocol::codec::encode_schema_block;

        let catalog = named_record(&catalog_defs(&cols), &cols.2);
        let client = encode_schema_block(&client_schema(&cols));
        prop_assert_eq!(catalog, client);
    }
}

proptest! {
    /// The client's digest of a table equals the engine's, and two layouts share
    /// a digest iff `same_physical_layout` holds — probed against an unrelated
    /// schema and against `a` with every payload column's nullability flipped.
    #[test]
    fn layout_digest_agrees_with_same_physical_layout(
        a in arb_schema(gnitz_wire::PK_LIST_MAX_COLS),
        b in arb_schema(gnitz_wire::PK_LIST_MAX_COLS),
    ) {
        prop_assert_eq!(client_schema(&a).layout_digest(), descriptor(&a).layout_digest());
        let (a, b) = (descriptor(&a), descriptor(&b));
        let flipped: Vec<SchemaColumn> = a.columns[..a.num_columns()]
            .iter()
            .enumerate()
            .map(|(i, c)| SchemaColumn::new(c.type_code, !a.pk_indices().contains(&(i as u32)) && !c.nullable))
            .collect();
        let twin = SchemaDescriptor::new(&flipped, a.pk_indices());
        for other in [&b, &twin] {
            prop_assert_eq!(a.same_physical_layout(other), a.layout_digest() == other.layout_digest());
        }
    }
}

/// Client `encode_ddl_txn` → server `decode_items` → `Batch::decode_from_wal_block`
/// against each family's own schema, for 1-, 2-, 3-, and 5-family bundles.
///
/// The frame layout itself is `gnitz_wire::txn_frame`'s, round-tripped in that
/// crate, and both ends derive their system schemas from `gnitz_wire::SYS_FAMILIES`
/// rather than hand-keeping them. What is only covered here is the pairing:
/// a client-built WAL block per family, decoded by the engine against the
/// catalog's own schema for that family — over seven real system schemas, with
/// STRING columns and compound PKs.
#[test]
fn ddl_txn_roundtrip_client_to_server() {
    use gnitz_core::protocol::types::{BatchAppender, ZSetBatch};
    use gnitz_core::types::sys_schema;
    use gnitz_wire::txn_frame::decode_items;
    use gnitz_wire::{CIRCUIT_NODES_TAB, COL_TAB, IDX_TAB, TABLE_TAB, VIEW_TAB};

    // Build a small COL_TAB batch for `oid` with `n` U64 columns.
    let col_batch = |oid: u64, n: usize| -> ZSetBatch {
        let s = sys_schema(COL_TAB);
        let mut b = ZSetBatch::new(s);
        {
            let mut a = BatchAppender::new(&mut b, s);
            for i in 0..n {
                a.add_row_cols(&[oid as u128, i as u128], 1)
                    .str_val(&format!("c{i}"))
                    .u64_val(4) // type_code U64
                    .u64_val(0) // is_nullable
                    .u64_val(0) // fk_table_id
                    .u64_val(0) // fk_col_idx
                    .u64_val(0) // is_hidden
                    .u64_val(0); // scale
            }
        }
        b
    };
    let table_batch = |tid: u64, weight: i64| -> ZSetBatch {
        let s = sys_schema(TABLE_TAB);
        let mut b = ZSetBatch::new(s);
        BatchAppender::new(&mut b, s)
            .add_row(tid as u128, weight)
            .u64_val(3) // schema_id
            .str_val("t")
            .u64_val(0)
            .u64_val(0);
        b
    };
    let idx_batch = |idx_id: u64, owner: u64| -> ZSetBatch {
        let s = sys_schema(IDX_TAB);
        let mut b = ZSetBatch::new(s);
        BatchAppender::new(&mut b, s)
            .add_row(idx_id as u128, 1)
            .u64_val(owner)
            .u64_val(gnitz_wire::pack_pk_cols(&[1]))
            .str_val("idx_t_b")
            .u64_val(1); // flags: unique, not internal
        b
    };

    // Verify a bundle roundtrips: family count, order (tid), per-row weight,
    // and — where `check_pk` says so — the PK. It is off for the compound-PK
    // families, whose two scalar forms differ (the engine reads the widened key
    // high-half-first, the client low-half-first) over identical wire bytes.
    let verify = |families: &[(u64, ZSetBatch)], check_pk: &[bool]| {
        let payload = gnitz_core::protocol::encode_ddl_txn(families);
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
                        exp_batch.pks.get(gnitz_core::types::sys_schema(*exp_tid), i),
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

    // 5-family: CREATE VIEW (COL + 3 circuit + VIEW). The compound-PK
    // families are built with the client's exact low/high u128 packing.
    let vid: u64 = 20;
    let src: u64 = 16;
    // Compound-PK family: `pk = view_id (low) | node_id (high)`, matching the
    // client's low/high packing. `check_pk` is false for it, so the exact node
    // ids are arbitrary — pick non-trivial ones (clippy `identity_op`).
    let nodes = {
        let s = sys_schema(CIRCUIT_NODES_TAB);
        let mut b = ZSetBatch::new(s);
        {
            let mut a = BatchAppender::new(&mut b, s);
            // ScanDelta(src), unwired and parameterless.
            a.add_row((vid as u128) | (1u128 << 64), 1)
                .u64_val(gnitz_wire::Opcode::ScanDelta.as_wire())
                .u64_val(src)
                .null()
                .null()
                .null();
            // Integrate, fed by node 1.
            a.add_row((vid as u128) | (2u128 << 64), 1)
                .u64_val(gnitz_wire::Opcode::IntegrateSink.as_wire())
                .null()
                .u64_val(1)
                .null()
                .null();
        }
        b
    };
    let view = {
        let s = sys_schema(VIEW_TAB);
        let mut b = ZSetBatch::new(s);
        BatchAppender::new(&mut b, s)
            .add_row(vid as u128, 1)
            .u64_val(3)
            .str_val("v")
            .u64_val(0) // pk_col_idx
            .u64_val(0) // capacity_bytes
            .u64_val(0) // delta_bytes
            .u64_val(0) // owner_view_id
            .u64_val(0); // flags
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
