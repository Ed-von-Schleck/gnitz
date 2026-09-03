//! The meta-schema block across the two adapters that build it: this catalog's
//! (`SchemaDescriptor` → block) and `gnitz-core`'s (client `Schema` → block).
//!
//! It lives on this side because nothing links this crate, so the client is the
//! half that can be pulled in — as a dev-dependency. A divergence between the
//! two adapters is a live bug in `gnitz-mirror`, whose read path does exactly
//! this conversion: `descriptor_of` encodes a client `Schema` to a block and
//! decodes it back as a `SchemaDescriptor`.

use crate::catalog::schema_block::encode_named_schema_block;
use crate::test_support::{arb_type_code, named_col_defs};
use gnitz_store::schema::{decode_schema_block, SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::Batch;
use gnitz_wire::{is_pk_eligible, type_code, MAX_PK_COLUMNS};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

/// `max_pk` bounds the generated PK arity: the catalog codec supports up to
/// `MAX_PK_COLUMNS` (5, the secondary-index schema width), but the persisted
/// client codec caps at `PK_LIST_MAX_COLS` (4) — tests that decode through the
/// client (`batch_to_schema` → `Schema::validate_parts`) must stay within it.
fn arb_schema(max_pk: usize) -> impl Strategy<Value = SchemaDescriptor> {
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
            let pk_indices: Vec<u32> = idx[..k].to_vec();

            let cols: Vec<SchemaColumn> = (0..n_cols)
                .map(|i| {
                    let is_pk = pk_indices.contains(&(i as u32));
                    // PK columns must be PK-eligible and non-nullable; remap
                    // ineligible draws to U64 so `new()` accepts the schema.
                    let tc = if is_pk && !is_pk_eligible(types[i]) {
                        type_code::U64
                    } else {
                        types[i]
                    };
                    let nullable = if is_pk { 0 } else { nullables[i] as u8 };
                    SchemaColumn::new(tc, nullable)
                })
                .collect();

            SchemaDescriptor::new(&cols, &pk_indices)
        })
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

/// SchemaDescriptor → owned client Schema, with synthetic `c{i}` names.
fn descriptor_to_client_schema(sd: &SchemaDescriptor) -> gnitz_core::protocol::types::Schema {
    use gnitz_core::protocol::types::{ColumnDef, Schema, TypeCode};
    let columns = (0..sd.num_columns())
        .map(|i| {
            let col = &sd.columns[i];
            // arb_schema only emits valid codes, so unwrap is total.
            ColumnDef::new(
                format!("c{i}"),
                TypeCode::try_from_u8(col.type_code).unwrap(),
                col.nullable != 0,
            )
        })
        .collect();
    let pk_cols = sd.pk_indices().iter().map(|&i| i as usize).collect();
    Schema { columns, pk_cols }
}

/// `c0..c{n-1}` — the same names `descriptor_to_client_schema` gives the client
/// side, so the two adapters are fed one schema and not two.
fn synthetic_names(sd: &SchemaDescriptor) -> Vec<String> {
    (0..sd.num_columns()).map(|i| format!("c{i}")).collect()
}

proptest! {
    /// Catalog encoder → catalog decoder.
    #[test]
    fn schema_roundtrip_catalog_codec(original in arb_schema(MAX_PK_COLUMNS)) {
        let original = &original;
        let wire = encode_named_schema_block(original, &named_col_defs(&synthetic_names(original)), 0);
        let decoded = decode_schema_block(&wire, true)
            .expect("decode must succeed for any valid schema");
        assert_descriptor_eq(original, &decoded)?;
    }

    /// Client encoder → client decoder.
    #[test]
    fn schema_roundtrip_client_codec(original in arb_schema(gnitz_wire::PK_LIST_MAX_COLS)) {
        use gnitz_core::protocol::codec::{encode_schema_block, schema_from_block};

        let client = descriptor_to_client_schema(&original);
        let wire = encode_schema_block(&client, 0);
        prop_assert_eq!(schema_from_block(&wire).unwrap(), client);
    }

    /// The two crates' adapters must emit the **same bytes** for the same
    /// schema — the property that makes either side's block decodable by the
    /// other for reasons stronger than "both round-trip", and the one thing
    /// pinning `pack_col_meta_flags`' argument order across them. With each
    /// side's own round-trip above, the two cross-codec directions follow.
    #[test]
    fn schema_block_bytes_agree_across_the_two_adapters(original in arb_schema(gnitz_wire::PK_LIST_MAX_COLS)) {
        use gnitz_core::protocol::codec::encode_schema_block;

        let original = &original;
        let catalog = encode_named_schema_block(original, &named_col_defs(&synthetic_names(original)), 7);
        let client = encode_schema_block(&descriptor_to_client_schema(original), 7);
        prop_assert_eq!(catalog, client);
    }
}

/// Client `encode_ddl_txn` → server `decode_ddl_txn` → `Batch::decode_from_wal_block`
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
    use gnitz_wire::txn_frame::decode_ddl_txn;
    use gnitz_wire::{
        COL_TAB, IDX_TAB, TABLE_TAB, VIEW_TAB, {CIRCUIT_EDGES_TAB, CIRCUIT_NODES_TAB, CIRCUIT_NODE_COLUMNS_TAB},
    };

    // Build a small COL_TAB batch for `oid` with `n` U64 columns.
    let col_batch = |oid: u64, kind: u64, n: usize| -> ZSetBatch {
        let s = sys_schema(COL_TAB);
        let mut b = ZSetBatch::new(s);
        {
            let mut a = BatchAppender::new(&mut b, s);
            for i in 0..n {
                a.add_row(gnitz_wire::pack_col_id(oid, i as u64).unwrap() as u128, 1)
                    .u64_val(oid)
                    .u64_val(kind)
                    .u64_val(i as u64)
                    .str_val(&format!("c{i}"))
                    .u64_val(4) // type_code U64
                    .u64_val(0) // is_nullable
                    .u64_val(0) // fk_table_id
                    .u64_val(0) // fk_col_idx
                    .u64_val(0) // is_serial
                    .u64_val(0); // is_hidden
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
    // and — for single-PK families — the PK. `check_pk` skips the compound-PK
    // circuit families whose engine `get_pk` returns the packed narrow key
    // rather than the client's low/high u128 layout.
    let verify = |families: &[(u64, ZSetBatch)], check_pk: &[bool]| {
        let payload = gnitz_core::protocol::encode_ddl_txn(0xABCD, families);
        let decoded = decode_ddl_txn(&payload).expect("decode_ddl_txn");
        assert_eq!(decoded.len(), families.len(), "family count");
        for (fi, ((exp_tid, exp_batch), (got_tid, slice))) in families.iter().zip(&decoded).enumerate() {
            assert_eq!(*got_tid, *exp_tid as u32, "family {fi} tid/order");
            let schema = crate::catalog::SysFamily::from_id(*got_tid as i64)
                .expect("bundle family id must be a system family")
                .schema();
            let (batch, _) = Batch::decode_from_wal_block(slice, &schema, false).expect("decode family batch");
            assert_eq!(batch.len(), exp_batch.len(), "row count tid {got_tid}");
            for i in 0..batch.len() {
                assert_eq!(
                    batch.get_weight(i),
                    exp_batch.weights[i],
                    "weight row {i} tid {got_tid}"
                );
                if check_pk[fi] {
                    assert_eq!(batch.get_pk(i), exp_batch.pks.get(i), "pk row {i} tid {got_tid}");
                }
            }
        }
    };

    // 1-family: DROP TABLE (one TABLE_TAB -1).
    verify(&[(TABLE_TAB, table_batch(16, -1))], &[true]);

    // 2-family: CREATE TABLE (COL_TAB + TABLE_TAB).
    verify(
        &[(COL_TAB, col_batch(17, 0, 2)), (TABLE_TAB, table_batch(17, 1))],
        &[true, true],
    );

    // 3-family: CREATE TABLE + inline UNIQUE index (COL_TAB + TABLE_TAB + IDX_TAB).
    verify(
        &[
            (COL_TAB, col_batch(18, 0, 2)),
            (TABLE_TAB, table_batch(18, 1)),
            (IDX_TAB, idx_batch(100, 18)),
        ],
        &[true, true, true],
    );

    // 5-family: CREATE VIEW (COL + 3 circuit + VIEW). The compound-PK
    // families are built with the client's exact low/high u128 packing.
    let vid: u64 = 20;
    let src: u64 = 16;
    // Compound-PK families: `pk = view_id (low) | sub (high)`, matching the
    // client's low/high packing. `check_pk` is false for these, so the exact
    // sub values are arbitrary — pick non-trivial ones (clippy `identity_op`).
    let nodes = {
        let s = sys_schema(CIRCUIT_NODES_TAB);
        let mut b = ZSetBatch::new(s);
        {
            let mut a = BatchAppender::new(&mut b, s);
            a.add_row((vid as u128) | (1u128 << 64), 1)
                .u64_val(1)
                .u64_val(0)
                .u64_val(src)
                .null();
            a.add_row((vid as u128) | (2u128 << 64), 1)
                .u64_val(2)
                .u64_val(1)
                .null()
                .null();
        }
        b
    };
    let edges = {
        let s = sys_schema(CIRCUIT_EDGES_TAB);
        let mut b = ZSetBatch::new(s);
        let sub = (2u128 << 8) | 1u128; // (dst_node, dst_port)
        BatchAppender::new(&mut b, s)
            .add_row((vid as u128) | (sub << 64), 1)
            .u64_val(2)
            .u64_val(1)
            .u64_val(1);
        b
    };
    let node_cols = {
        let s = sys_schema(CIRCUIT_NODE_COLUMNS_TAB);
        let mut b = ZSetBatch::new(s);
        let sub = (1u128 << 24) | (2u128 << 16) | 3u128; // (node_id, kind, position)
        BatchAppender::new(&mut b, s)
            .add_row((vid as u128) | (sub << 64), 1)
            .u64_val(1)
            .u64_val(2)
            .u64_val(3)
            .u64_val(4)
            .u64_val(5);
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
            .u64_val(0); // owner_view_id
        b
    };
    verify(
        &[
            (COL_TAB, col_batch(vid, 1, 1)),
            (CIRCUIT_NODES_TAB, nodes),
            (CIRCUIT_EDGES_TAB, edges),
            (CIRCUIT_NODE_COLUMNS_TAB, node_cols),
            (VIEW_TAB, view),
        ],
        // Skip pk check on the compound-PK circuit families.
        &[true, false, false, false, true],
    );
}
