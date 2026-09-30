//! The name rules and [`check_batch_shape`], each rule driven on a family whose
//! declared facts make it the one that fires.

use super::*;
use crate::test_support::{col_def, idx_tab_batch, push_table_tab_row, push_view_tab_row, schema_tab_batch};
use gnitz_wire::sys_rows::{write_circuit_node_row, CircuitNodeRow};
use gnitz_wire::{pack_pk_cols, IndexProps, TypeCode};

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
        let err = reject_unstorable_name(name, "table").unwrap_err();
        assert!(err.contains(fragment), "name {name:?} gave: {err}");
    }
    for name in ["_", "_foo", "_seg4096", "_fk_16_1", "a_b_9"] {
        reject_unstorable_name(name, "table").unwrap();
    }
}

// ── Fixtures ────────────────────────────────────────────────────────────────

/// A CIRCUIT_NODES batch of `(view_id, node_id, weight)` rows.
fn circuit_batch(rows: &[(u64, u64, i64)]) -> Batch {
    let mut bb = BatchBuilder::new(*SysFamily::CircuitNodes.schema());
    for &(view_id, node_id, weight) in rows {
        write_circuit_node_row(
            &mut bb,
            &CircuitNodeRow {
                view_id,
                node_id,
                opcode: gnitz_wire::Opcode::IntegrateSink.as_wire(),
                source_table: None,
                inputs: [None; 2],
                params: None,
            },
            weight,
        );
    }
    bb.finish()
}

/// A TABLE_TAB batch of `(table_id, schema_id, name, weight)` rows.
fn table_batch(rows: &[(u64, u64, &str, i64)]) -> Batch {
    let mut bb = BatchBuilder::new(*SysFamily::Table.schema());
    for &(tid, sid, name, weight) in rows {
        push_table_tab_row(
            &mut bb,
            tid,
            sid,
            name,
            pack_pk_cols(&[0]),
            gnitz_wire::TableProps::default().pack(),
            weight,
        );
    }
    bb.finish()
}

fn index_batch(index_id: u64, weight: i64) -> Batch {
    idx_tab_batch(index_id, 16, pack_pk_cols(&[1]), "ix", IndexProps::default(), weight)
}

fn shape_err(family: SysFamily, batch: &Batch) -> String {
    check_batch_shape(family, batch)
        .err()
        .expect("the shape rules must reject this batch")
}

// ── Row weights ─────────────────────────────────────────────────────────────

#[test]
fn a_system_row_may_only_be_written_at_weight_one() {
    check_batch_shape(SysFamily::Schema, &schema_tab_batch(&[(20, "s", 1)])).unwrap();
    for w in [0i64, 2, -2, i64::MIN] {
        shape_err(SysFamily::Schema, &schema_tab_batch(&[(20, "s", w)]));
    }
}

// ── Per-PK multiplicity ─────────────────────────────────────────────────────

#[test]
fn a_repeated_sign_on_one_pk_is_rejected() {
    for (family, batch) in [
        (
            SysFamily::Table,
            table_batch(&[(20, PUBLIC_SCHEMA_ID, "a", 1), (20, PUBLIC_SCHEMA_ID, "b", 1)]),
        ),
        (SysFamily::CircuitNodes, circuit_batch(&[(20, 0, 1), (20, 0, 1)])),
    ] {
        let err = shape_err(family, &batch);
        assert!(err.contains("more than one row"), "{family:?}: {err}");
    }
}

#[test]
fn a_row_retracted_only_with_its_owner_refuses_an_unpaired_retraction() {
    let err = shape_err(SysFamily::CircuitNodes, &circuit_batch(&[(20, 0, -1)]));
    assert!(err.contains("retracted only with its owner"), "{err}");

    let mut bb = BatchBuilder::new(*SysFamily::Column.schema());
    col_def("v", TypeCode::U64).write_col_tab_row(&mut bb, 300, 1, -1);
    let err = shape_err(SysFamily::Column, &bb.finish());
    assert!(err.contains("retracted only with its owner"), "{err}");
    assert!(err.contains("column 1 of owner 300"), "{err}");
}

// ── The id range ────────────────────────────────────────────────────────────

#[test]
fn an_id_below_a_familys_first_user_id_is_rejected_whatever_its_sign() {
    for w in [1i64, -1] {
        let err = shape_err(
            SysFamily::Schema,
            &schema_tab_batch(&[(SYSTEM_SCHEMA_ID, "_system", w)]),
        );
        assert!(err.contains("a system schema"), "w={w}: {err}");
    }

    let err = shape_err(
        SysFamily::Table,
        &table_batch(&[(gnitz_wire::IDX_TAB, SYSTEM_SCHEMA_ID, "_indices", 1)]),
    );
    assert!(err.contains("a system table"), "{err}");

    let err = shape_err(SysFamily::Index, &index_batch(0, 1));
    assert!(err.contains("a system index"), "{err}");

    let mut bb = BatchBuilder::new(*SysFamily::Sequence.schema());
    bb.begin_row(SEQ_ID_NEXT_ID as u128, 1);
    bb.put_u64(99);
    bb.end_row();
    let err = shape_err(SysFamily::Sequence, &bb.finish());
    assert!(err.contains("a system sequence"), "{err}");

    let mut bb = BatchBuilder::new(*SysFamily::Column.schema());
    col_def("id", TypeCode::U64).write_col_tab_row(&mut bb, gnitz_wire::IDX_TAB, 0, 1);
    let err = shape_err(SysFamily::Column, &bb.finish());
    assert!(err.contains("a system column"), "{err}");
    assert!(
        err.contains(&format!("column 0 of owner {}", gnitz_wire::IDX_TAB)),
        "{err}"
    );
}

#[test]
fn an_id_at_or_above_a_familys_ceiling_is_rejected() {
    let ceiling = gnitz_wire::CATALOG_ID_CEILING;
    check_batch_shape(SysFamily::Schema, &schema_tab_batch(&[(ceiling - 1, "s", 1)])).unwrap();

    let mut view = BatchBuilder::new(*SysFamily::View.schema());
    push_view_tab_row(&mut view, 1, ceiling, "v", 0, 0, 0);
    for (family, batch) in [
        (SysFamily::Schema, schema_tab_batch(&[(ceiling, "s", 1)])),
        (SysFamily::Table, table_batch(&[(ceiling, PUBLIC_SCHEMA_ID, "t", 1)])),
        (SysFamily::View, view.finish()),
        (SysFamily::Index, index_batch(ceiling, 1)),
    ] {
        let err = shape_err(family, &batch);
        assert!(err.contains("id ceiling"), "{family:?}: {err}");
    }
}

// ── What a rewrite pair may change ──────────────────────────────────────────

#[test]
fn a_rewrite_pair_may_change_only_the_fields_its_family_declares() {
    check_batch_shape(
        SysFamily::Table,
        &table_batch(&[(20, PUBLIC_SCHEMA_ID, "t", -1), (20, PUBLIC_SCHEMA_ID, "t2", 1)]),
    )
    .unwrap();

    let err = shape_err(
        SysFamily::Table,
        &table_batch(&[(20, PUBLIC_SCHEMA_ID, "t", -1), (20, SYSTEM_SCHEMA_ID, "t", 1)]),
    );
    assert!(err.contains("changes a field it may not"), "{err}");

    let mut index_pair = index_batch(20, -1);
    index_pair.append_batch(&index_batch(20, 1));
    for (family, batch) in [
        (SysFamily::Schema, schema_tab_batch(&[(20, "s", -1), (20, "s", 1)])),
        (SysFamily::Index, index_pair),
    ] {
        let err = shape_err(family, &batch);
        assert!(err.contains("admits no rewrite pair"), "{family:?}: {err}");
    }
}

// ── Bundle rules ────────────────────────────────────────────────────────────

/// A circuit `+1` under a view its bundle does not create would inject nodes
/// into a running view's circuit, or make its source's dependents permanently
/// true.
#[test]
fn a_circuit_row_must_name_a_view_its_bundle_creates() {
    let rows = circuit_batch(&[(20, 0, 1)]);
    let err = check_circuit_rows(&rows, &[]).unwrap_err();
    assert!(err.contains("view 20, which this transaction does not create"), "{err}");
    check_circuit_rows(&rows, &[20]).unwrap();
}
