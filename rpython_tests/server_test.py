# server_test.py
#
# Proves the "Everything is a Batch" protocol: every operation — DDL and DML
# alike — is a batch push to a table ID.  System table IDs trigger catalog
# effects (create schema, create table, compile view); user table IDs trigger
# data ingestion and the reactive DAG.  Scans are empty pushes.

import sys
import os

from rpython.rlib import rposix
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core import types, batch
from gnitz.core.batch import RowBuilder, ArenaZSetBatch
from gnitz.core.errors import LayoutError
from gnitz.catalog import engine, system_tables as sys_tab
from gnitz.catalog.metadata import ensure_dir
from gnitz.server.executor import ServerExecutor
from rpython_tests.helpers.jit_stub import ensure_jit_reachable


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def log(msg):
    os.write(1, msg + "\n")

def fail(msg):
    os.write(2, "CRITICAL FAILURE: " + msg + "\n")
    raise Exception(msg)

def assert_true(condition, msg):
    if not condition:
        fail(msg)

def assert_equal_i(expected, actual, msg):
    if expected != actual:
        fail(msg + " (Expected " + str(expected) + ", got " + str(actual) + ")")

def assert_equal_i64(expected, actual, msg):
    if expected != actual:
        fail(msg + " (i64 mismatch)")

def _recursive_delete(path):
    if not os.path.exists(path):
        return
    if os.path.isdir(path):
        items = os.listdir(path)
        for item in items:
            _recursive_delete(path + "/" + item)
        try:
            rposix.rmdir(path)
        except OSError:
            pass
    else:
        try:
            os.unlink(path)
        except OSError:
            pass

def cleanup(path):
    if os.path.exists(path):
        _recursive_delete(path)


# -- System-table batch builders (the "client library") --------------------

def _advance_seq(srv, seq_id, old_val, new_val):
    """Retract old HWM, insert new HWM for a sequence."""
    s = srv.engine.sys.sequences.schema
    b = ArenaZSetBatch(s)
    sys_tab.SeqTab.retract(b, s, seq_id, old_val)
    sys_tab.SeqTab.append(b, s, seq_id, new_val)
    srv.handle_push(sys_tab.SeqTab.ID, b)
    b.free()


def push_create_schema(srv, schema_name):
    """Create a schema by upserting into SchemaTab."""
    sid = srv.engine.registry.allocate_schema_id()
    s = srv.engine.sys.schemas.schema
    b = ArenaZSetBatch(s)
    sys_tab.SchemaTab.append(b, s, sid, schema_name)
    srv.handle_push(sys_tab.SchemaTab.ID, b)
    b.free()
    _advance_seq(srv, sys_tab.SEQ_ID_SCHEMAS, sid - 1, sid)
    return sid


def push_create_table(srv, schema_name, table_name, col_defs, pk_col_idx):
    """
    Create a table by upserting columns into ColTab, then a table record
    into TableTab.  The TableEffectHook fires on the second push, reads
    columns, and materialises a PersistentTable in the registry.
    """
    tid = srv.engine.registry.allocate_table_id()
    sid = srv.engine.registry.get_schema_id(schema_name)
    base_dir = srv.engine.base_dir
    directory = base_dir + "/" + schema_name + "/" + table_name + "_" + str(tid)

    # 1. Push column records
    cs = srv.engine.sys.columns.schema
    cb = ArenaZSetBatch(cs)
    for i in range(len(col_defs)):
        col = col_defs[i]
        sys_tab.ColTab.append(
            cb, cs, tid, sys_tab.OWNER_KIND_TABLE, i,
            col.name, col.field_type.code, int(col.is_nullable),
            col.fk_table_id, col.fk_col_idx,
        )
    srv.handle_push(sys_tab.ColTab.ID, cb)
    cb.free()

    # 2. Push table record — hook fires here
    ts = srv.engine.sys.tables.schema
    tb = ArenaZSetBatch(ts)
    sys_tab.TableTab.append(tb, ts, tid, sid, table_name, directory, pk_col_idx, 0, 0)
    srv.handle_push(sys_tab.TableTab.ID, tb)
    tb.free()

    _advance_seq(srv, sys_tab.SEQ_ID_TABLES, tid - 1, tid)
    return tid


def push_create_view(srv, schema_name, view_name, graph):
    """
    Create a view by pushing circuit tables + deps + columns + view record.
    The ViewEffectHook fires on the final push.
    """
    vid = srv.engine.registry.allocate_table_id()
    sid = srv.engine.registry.get_schema_id(schema_name)
    base_dir = srv.engine.base_dir
    directory = base_dir + "/" + schema_name + "/view_" + view_name + "_" + str(vid)

    # 1. Column records for the view output
    cs = srv.engine.sys.columns.schema
    cb = ArenaZSetBatch(cs)
    for i in range(len(graph.output_col_defs)):
        name, type_code = graph.output_col_defs[i]
        sys_tab.ColTab.append(
            cb, cs, vid, sys_tab.OWNER_KIND_VIEW, i,
            name, type_code, 0, 0, 0,
        )
    srv.handle_push(sys_tab.ColTab.ID, cb)
    cb.free()

    # 2. View dependency records
    ds = srv.engine.sys.view_deps.schema
    db = ArenaZSetBatch(ds)
    for dep_tid in graph.dependencies:
        sys_tab.DepTab.append(db, ds, vid, 0, dep_tid)
    if db.length() > 0:
        srv.handle_push(sys_tab.DepTab.ID, db)
    db.free()

    # 3. Circuit graph (5 tables)
    ns = srv.engine.sys.circuit_nodes.schema
    nb = ArenaZSetBatch(ns)
    for node_id, opcode in graph.nodes:
        sys_tab.CircuitNodesTab.append(nb, ns, vid, node_id, opcode)
    srv.handle_push(sys_tab.CircuitNodesTab.ID, nb)
    nb.free()

    es = srv.engine.sys.circuit_edges.schema
    eb = ArenaZSetBatch(es)
    for edge_id, src, dst, port in graph.edges:
        sys_tab.CircuitEdgesTab.append(eb, es, vid, edge_id, src, dst, port)
    srv.handle_push(sys_tab.CircuitEdgesTab.ID, eb)
    eb.free()

    ss = srv.engine.sys.circuit_sources.schema
    sb = ArenaZSetBatch(ss)
    for node_id, table_id in graph.sources:
        sys_tab.CircuitSourcesTab.append(sb, ss, vid, node_id, table_id)
    srv.handle_push(sys_tab.CircuitSourcesTab.ID, sb)
    sb.free()

    ps = srv.engine.sys.circuit_params.schema
    pb = ArenaZSetBatch(ps)
    for node_id, slot, value in graph.params:
        sys_tab.CircuitParamsTab.append(pb, ps, vid, node_id, slot, value)
    if pb.length() > 0:
        srv.handle_push(sys_tab.CircuitParamsTab.ID, pb)
    pb.free()

    gs = srv.engine.sys.circuit_group_cols.schema
    gb = ArenaZSetBatch(gs)
    for node_id, col_idx in graph.group_cols:
        sys_tab.CircuitGroupColsTab.append(gb, gs, vid, node_id, col_idx)
    if gb.length() > 0:
        srv.handle_push(sys_tab.CircuitGroupColsTab.ID, gb)
    gb.free()

    # 4. View record — hook fires here, compiles the graph
    vs = srv.engine.sys.views.schema
    vb = ArenaZSetBatch(vs)
    sys_tab.ViewTab.append(vb, vs, vid, sid, view_name, "", directory, 0)
    srv.handle_push(sys_tab.ViewTab.ID, vb)
    vb.free()

    _advance_seq(srv, sys_tab.SEQ_ID_TABLES, vid - 1, vid)
    return vid


def _add_int_row(rb, pk, vals, weight=1):
    rb.begin(r_uint64(pk), r_uint64(0), r_int64(weight))
    for v in vals:
        rb.put_int(r_int64(v))
    rb.commit()


# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------

def test_create_schema_via_push(srv):
    """DDL as DML: create a schema by pushing into SchemaTab."""
    log("[SERVER] Testing create_schema via push...")
    push_create_schema(srv, "myapp")
    assert_true(srv.engine.registry.has_schema("myapp"), "schema not created")
    log("  PASSED")


def test_create_table_via_push(srv):
    """DDL as DML: create a table by pushing columns + table record."""
    log("[SERVER] Testing create_table via push...")
    col_defs = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    tid = push_create_table(srv, "myapp", "users", col_defs, 0)
    assert_true(srv.engine.registry.has_id(tid), "table not registered")
    family = srv.engine.registry.get_by_id(tid)
    assert_equal_i(2, len(family.schema.columns), "column count")
    log("  PASSED")
    return tid


def test_upsert_and_scan(srv, table_id):
    """DML: insert rows via push, then scan to read them back."""
    log("[SERVER] Testing upsert + scan...")
    family = srv.engine.registry.get_by_id(table_id)
    schema = family.schema

    # Push 3 rows
    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    _add_int_row(rb, 1, [100])
    _add_int_row(rb, 2, [200])
    _add_int_row(rb, 3, [300])
    srv.handle_push(table_id, b)
    b.free()

    # Scan (empty push)
    result = srv.handle_push(table_id, None)
    assert_true(result is not None, "scan returned None")
    assert_equal_i(3, result.length(), "scan row count")

    # Verify values
    acc = result.get_accessor(0)
    result.bind_accessor(0, acc)
    assert_equal_i64(r_int64(100), acc.get_int_signed(1), "row 0 val")
    result.bind_accessor(1, acc)
    assert_equal_i64(r_int64(200), acc.get_int_signed(1), "row 1 val")
    result.bind_accessor(2, acc)
    assert_equal_i64(r_int64(300), acc.get_int_signed(1), "row 2 val")
    result.free()
    log("  PASSED")


def test_delete_via_push(srv, table_id):
    """DML: delete a row by pushing weight=-1, then scan to verify."""
    log("[SERVER] Testing delete via negative-weight push...")
    family = srv.engine.registry.get_by_id(table_id)
    schema = family.schema

    # Delete pk=2 with weight=-1
    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    _add_int_row(rb, 2, [200], weight=-1)
    srv.handle_push(table_id, b)
    b.free()

    # Scan — should have 2 rows (pk=1 and pk=3)
    result = srv.handle_push(table_id, None)
    assert_true(result is not None, "scan returned None after delete")
    assert_equal_i(2, result.length(), "scan row count after delete")
    result.free()
    log("  PASSED")


def test_scan_system_table(srv):
    """Scan a system table — schemas should include _system, public, myapp."""
    log("[SERVER] Testing scan of system table...")
    result = srv.handle_push(sys_tab.SchemaTab.ID, None)
    assert_true(result is not None, "schema scan returned None")
    assert_true(result.length() >= 3, "expected at least 3 schemas")
    result.free()
    log("  PASSED")


def test_reactive_view_via_push(srv, source_table_id):
    """
    Full reactive pipeline via pure pushes:
    1. Create a view (filter: val > 150)
    2. Push data to the source table
    3. Scan the view to see materialized results
    """
    log("[SERVER] Testing reactive view via push...")

    from gnitz.dbsp.expr import ExprBuilder
    from rpython_tests.helpers.circuit_builder import CircuitBuilder

    # Build expression: col1 > 150
    eb = ExprBuilder()
    col1 = eb.load_col_int(1)
    const = eb.load_const_int(150)
    result_reg = eb.cmp_gt(col1, const)
    prog = eb.build(result_reg)

    # Build circuit graph
    builder = CircuitBuilder(view_id=0, primary_source_id=source_table_id)
    src = builder.input_delta()
    flt = builder.filter(src, expr_code=prog.code_as_ints(),
                         expr_num_regs=prog.num_regs,
                         expr_result_reg=prog.result_reg)
    builder.sink(flt, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)

    # Create view entirely via pushes
    vid = push_create_view(srv, "myapp", "big_vals", graph)

    # Verify view exists
    assert_true(srv.engine.registry.has_id(vid), "view not registered")
    plan = srv.engine.program_cache.get_program(vid)
    assert_true(plan is not None, "view plan not compiled")

    # Push new data to source table — reactive DAG fires
    family = srv.engine.registry.get_by_id(source_table_id)
    b = ArenaZSetBatch(family.schema)
    rb = RowBuilder(family.schema, b)
    _add_int_row(rb, 10, [50])    # filtered out (50 <= 150)
    _add_int_row(rb, 11, [250])   # passes (250 > 150)
    _add_int_row(rb, 12, [500])   # passes (500 > 150)
    srv.handle_push(source_table_id, b)
    b.free()

    # Scan the view — should have 3 passing rows:
    # pk=3 (val=300) backfilled from existing data + pk=11 (val=250) + pk=12 (val=500)
    view_result = srv.handle_push(vid, None)
    assert_true(view_result is not None, "view scan returned None")
    assert_equal_i(3, view_result.length(), "view should have 3 rows")
    view_result.free()

    log("  PASSED")


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------

def entry_point(argv):
    ensure_jit_reachable()
    base_dir = "server_test_data"
    cleanup(base_dir)
    ensure_dir(base_dir)

    try:
        db = engine.open_engine(base_dir)
        srv = ServerExecutor(db)

        test_create_schema_via_push(srv)
        tid = test_create_table_via_push(srv)
        test_upsert_and_scan(srv, tid)
        test_delete_via_push(srv, tid)
        test_scan_system_table(srv)
        test_reactive_view_via_push(srv, tid)

        db.close()
        log("\nALL SERVER TESTS PASSED")
    except Exception as e:
        os.write(2, "FAILURE\n")
        return 1
    finally:
        cleanup(base_dir)

    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
