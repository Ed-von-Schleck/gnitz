//! The name rules and the per-PK family contract — the two halves of the
//! precheck that are a property of a row's shape, independent of what the row
//! means. Each rule of [`CatalogEngine::check_family_contract`] is driven on its
//! own, and on a family whose declared facts make it the rule that fires.

use super::*;
use crate::test_support::{col_def, scratch_dir};
use gnitz_store::schema::type_code;
use gnitz_wire::sys_rows::{write_circuit_node_row, write_idx_tab_row, write_schema_tab_row};
use gnitz_wire::sys_rows::{CircuitNodeRow, IdxTabRow, SchemaTabRow};
use std::fs;

// ── The name rules ──────────────────────────────────────────────────────────

#[test]
fn an_unstorable_name_is_rejected_and_a_leading_underscore_is_not() {
    for (name, fragment) in [
        ("", "cannot be empty"),
        ("a b", "invalid characters"),
        ("a.b", "invalid characters"),
        ("a/b", "invalid characters"),
        ("MixedCase", "not canonical"),
    ] {
        let err = reject_unstorable_name(name, "table").expect_err("{name} must be refused");
        assert!(err.contains(fragment), "name {name:?} gave: {err}");
    }
    // The engine's rule is deliberately weaker than the client's identifier
    // policy: it must accept the internal names the planner and the FK hook
    // mint, all of which lead with `_`.
    reject_unstorable_name("_foo", "table").unwrap();
    reject_unstorable_name("_seg4096", "view").unwrap();
    reject_unstorable_name("_fk_16_1", "index").unwrap();
}

#[test]
fn only_an_ascii_uppercase_byte_makes_a_name_non_canonical() {
    reject_non_canonical("a_b_9", "schema").unwrap();
    reject_non_canonical("_", "schema").unwrap();
    assert!(reject_non_canonical("aB", "schema").is_err());
}

// ── Contract fixtures ───────────────────────────────────────────────────────

/// A SCHEMA_TAB batch of `(schema_id, name, weight)` rows.
fn schema_batch(rows: &[(i64, &str, i64)]) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Schema.schema());
    for &(schema_id, name, weight) in rows {
        write_schema_tab_row(
            &mut bb,
            &SchemaTabRow {
                schema_id: schema_id as u64,
                name,
            },
            weight,
        );
    }
    bb.finish()
}

/// An IDX_TAB batch of `(index_id, weight)` rows over one owner and column.
fn idx_batch(rows: &[(i64, i64)]) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Index.schema());
    for &(index_id, weight) in rows {
        write_idx_tab_row(
            &mut bb,
            &IdxTabRow {
                index_id: index_id as u64,
                owner_id: 16,
                source_col_idx: gnitz_wire::pack_pk_cols(&[1]),
                name: "ix",
                flags: 0,
            },
            weight,
        );
    }
    bb.finish()
}

/// A CIRCUIT_NODES batch of `(view_id, node_id, weight)` rows.
fn circuit_batch(rows: &[(i64, u64, i64)]) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::CircuitNodes.schema());
    for &(view_id, node_id, weight) in rows {
        write_circuit_node_row(
            &mut bb,
            &CircuitNodeRow {
                view_id: view_id as u64,
                node_id,
                opcode: gnitz_wire::Opcode::Integrate.as_wire(),
                source_table: None,
                inputs: [None; 2],
                params: None,
            },
            weight,
        );
    }
    bb.finish()
}

/// A one-row SEQ_TAB batch.
fn seq_batch(seq_id: i64, value: u64, weight: i64) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Sequence.schema());
    bb.begin_row(seq_id as u128, weight);
    bb.put_u64(value);
    bb.end_row();
    bb.finish()
}

/// A TABLE_TAB row reproducing what `create_table` writes for `tid` — so a `-1`
/// built from it satisfies the CAS and a `+1` differs only where the test says.
fn table_row(bb: &mut BatchBuilder, tid: i64, sid: i64, name: &str, weight: i64) {
    push_table_tab_row(
        bb,
        tid,
        sid,
        name,
        pack_pk_cols(&[0]),
        gnitz_wire::TableProps::default().pack(),
        weight,
    );
}

fn open(name: &str) -> (CatalogEngine, String) {
    let dir = scratch_dir("catalog", name);
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    (engine, dir)
}

fn contract_err(engine: &CatalogEngine, family: SysFamily, batch: &Batch) -> String {
    match engine.check_family_contract(family, batch) {
        Ok(_) => panic!("the contract must reject this batch"),
        Err(e) => e,
    }
}

// ── Rules 1 and 2: the weight of every row ──────────────────────────────────

#[test]
fn a_zero_weight_row_is_not_a_zset_element() {
    let (engine, dir) = open("precheck_zero_weight");
    let err = contract_err(&engine, SysFamily::Schema, &schema_batch(&[(20, "s", 0)]));
    assert!(err.contains("zero-weight"), "{err}");
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// `retract_key_range` and `retract_pk_list` emit a hard `-1` after gating on
/// the live weight, so a system row left above 1 is under-retracted by `w - 1`
/// and leaves a permanent live ghost.
#[test]
fn a_system_row_may_only_be_written_at_weight_one() {
    let (engine, dir) = open("precheck_weight_one");
    for w in [2i64, -2, i64::MIN] {
        let err = contract_err(&engine, SysFamily::Schema, &schema_batch(&[(20, "s", w)]));
        assert!(err.contains(&format!("at weight {w}")), "w={w}: {err}");
    }
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Rule 3: per-PK multiplicity ─────────────────────────────────────────────

/// A pair-capable family takes at most one row per sign.
#[test]
fn a_repeated_sign_on_one_pk_is_rejected_for_a_pair_capable_family() {
    let (mut engine, dir) = open("precheck_repeat_sign");
    let cols = vec![col_def("id", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    table_row(&mut bb, tid, PUBLIC_SCHEMA_ID, "a", 1);
    table_row(&mut bb, tid, PUBLIC_SCHEMA_ID, "b", 1);
    let err = contract_err(&engine, SysFamily::Table, &bb.finish());
    assert!(err.contains("more than one row for table"), "{err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A family declaring no pair mask takes at most one row per PK, whatever the
/// signs: neither SCHEMA_TAB nor IDX_TAB has a rename surface.
#[test]
fn a_rewrite_pair_is_rejected_for_a_family_with_no_pair_mask() {
    let (engine, dir) = open("precheck_no_pair");
    let err = contract_err(
        &engine,
        SysFamily::Schema,
        &schema_batch(&[(20, "s", -1), (20, "s2", 1)]),
    );
    assert!(err.contains("more than one row for schema"), "{err}");
    let err = contract_err(&engine, SysFamily::Index, &idx_batch(&[(7, -1), (7, 1)]));
    assert!(err.contains("more than one row for index"), "{err}");
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A duplicate `(view_id, node_id)` makes `load_circuit`'s node insert
/// last-writer-wins in cursor order, so rule 3 is the circuit family's whole
/// batch-local contract.
#[test]
fn a_duplicate_circuit_key_is_rejected_and_a_distinct_one_is_not() {
    let (engine, dir) = open("precheck_circuit_dup");
    let err = contract_err(
        &engine,
        SysFamily::CircuitNodes,
        &circuit_batch(&[(20, 0, 1), (20, 0, 1)]),
    );
    assert!(err.contains("more than one row for circuit row"), "{err}");
    assert!(err.contains("view 20 node 0"), "the message names both halves: {err}");

    // Two nodes of one view are distinct keys, and the circuit family takes no
    // live-row probe — that their view exists is the bundle guard's rule.
    engine
        .check_family_contract(SysFamily::CircuitNodes, &circuit_batch(&[(20, 0, 1), (20, 1, 1)]))
        .unwrap();
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Rule 4: the id range ────────────────────────────────────────────────────

/// The floor is a property of the id space, not of the mutation's shape, so it
/// covers every sign and every family that declares one.
#[test]
fn an_id_below_a_familys_first_user_id_is_rejected_whatever_its_sign() {
    let (engine, dir) = open("precheck_id_floor");

    for w in [1i64, -1] {
        let err = contract_err(
            &engine,
            SysFamily::Schema,
            &schema_batch(&[(SYSTEM_SCHEMA_ID, "_system", w)]),
        );
        assert!(err.contains("a system schema"), "w={w}: {err}");
    }

    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    table_row(&mut bb, IDX_TAB_ID, SYSTEM_SCHEMA_ID, "_indices", 1);
    let err = contract_err(&engine, SysFamily::Table, &bb.finish());
    assert!(err.contains("a system table"), "{err}");

    let err = contract_err(&engine, SysFamily::Index, &idx_batch(&[(0, 1)]));
    assert!(err.contains("a system index"), "{err}");

    // A user SERIAL sequence is keyed by its table id; the catalog's own
    // counters live below that floor.
    let err = contract_err(&engine, SysFamily::Sequence, &seq_batch(SEQ_ID_TABLES, 99, 1));
    assert!(err.contains("a system sequence"), "{err}");

    // COL_TAB packs the owner into its PK, so its floor is the packed word —
    // and the message renders both halves rather than that word.
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    push_col_tab_row(
        &mut bb,
        IDX_TAB_ID,
        OWNER_KIND_TABLE,
        0,
        &col_def("id", type_code::U64),
        1,
    );
    let err = contract_err(&engine, SysFamily::Column, &bb.finish());
    assert!(err.contains("a system column"), "{err}");
    assert!(err.contains(&format!("column 0 of owner {IDX_TAB_ID}")), "{err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// The ceiling runs where an id ENTERS a catalog namespace. Index needs it as
/// much as the relation families: `hook_index_register` raises the index-id
/// counter off the ingested row, and `allocate_index_ids` carries no assertion.
#[test]
fn an_id_at_or_above_a_familys_ceiling_is_rejected() {
    let (engine, dir) = open("precheck_id_ceiling");
    let ceiling = sys_tables::RELATION_ID_CEILING;

    for id in [ceiling, ceiling + 4096] {
        let mut bb = BatchBuilder::new(SysFamily::Table.schema());
        table_row(&mut bb, id, PUBLIC_SCHEMA_ID, "t", 1);
        let err = contract_err(&engine, SysFamily::Table, &bb.finish());
        assert!(err.contains("id ceiling"), "table {id}: {err}");

        let err = contract_err(&engine, SysFamily::Index, &idx_batch(&[(id, 1)]));
        assert!(err.contains("id ceiling"), "index {id}: {err}");
    }
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Rule 5: the CAS and the per-PK net bound ────────────────────────────────

#[test]
fn a_retraction_needs_a_live_row_that_content_equals_it() {
    let (mut engine, dir) = open("precheck_cas");
    engine
        .ingest_to_family(SCHEMA_TAB_ID, &schema_batch(&[(20, "live", 1)]))
        .unwrap();

    let err = contract_err(&engine, SysFamily::Schema, &schema_batch(&[(21, "absent", -1)]));
    assert!(err.contains("no longer exists"), "{err}");

    let err = contract_err(&engine, SysFamily::Schema, &schema_batch(&[(20, "stale", -1)]));
    assert!(err.contains("differs from the current one"), "{err}");

    engine
        .check_family_contract(SysFamily::Schema, &schema_batch(&[(20, "live", -1)]))
        .unwrap();
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// The sys stores run no `enforce_unique_pk`, so this bound is the only thing
/// stopping a duplicate live head or a persistent negative ghost.
#[test]
fn a_write_may_not_leave_a_pk_outside_net_weight_zero_or_one() {
    let (mut engine, dir) = open("precheck_net");
    engine
        .ingest_to_family(SCHEMA_TAB_ID, &schema_batch(&[(20, "live", 1)]))
        .unwrap();

    let err = contract_err(&engine, SysFamily::Schema, &schema_batch(&[(20, "live", 1)]));
    assert!(err.contains("net weight 2"), "{err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Rule 6: what a rewrite pair may change ──────────────────────────────────

#[test]
fn a_rewrite_pair_may_change_only_the_fields_its_family_declares() {
    let (mut engine, dir) = open("precheck_pair_mask");
    let cols = vec![col_def("id", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    // A rename is the whole of TABLE_TAB's mask, so it passes.
    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    table_row(&mut bb, tid, PUBLIC_SCHEMA_ID, "t", -1);
    table_row(&mut bb, tid, PUBLIC_SCHEMA_ID, "t2", 1);
    engine.check_family_contract(SysFamily::Table, &bb.finish()).unwrap();

    // Re-homing the relation into another schema is not.
    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    table_row(&mut bb, tid, PUBLIC_SCHEMA_ID, "t", -1);
    table_row(&mut bb, tid, SYSTEM_SCHEMA_ID, "t", 1);
    let err = contract_err(&engine, SysFamily::Table, &bb.finish());
    assert!(err.contains("changes a field it may not"), "{err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
