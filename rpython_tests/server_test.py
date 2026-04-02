# server_test.py
#
# Proves the "Everything is a Batch" protocol: every operation — DDL and DML
# alike — is a batch push to a table ID.  System table IDs trigger catalog
# effects (create schema, create table, compile view); user table IDs trigger
# data ingestion and the reactive DAG.  Scans are empty pushes.

import sys
import os

from rpython.rlib import rsocket
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, batch, errors
from gnitz.core import strings as string_logic
from gnitz.core.batch import RowBuilder, ArenaZSetBatch
from gnitz.core.errors import LayoutError
from gnitz.storage import buffer, mmap_posix, wal_columnar
from gnitz.server import ipc, ipc_ffi
from gnitz.catalog import engine, system_tables as sys_tab
from gnitz.catalog.metadata import ensure_dir
from gnitz.server.executor import ServerExecutor
from rpython_tests.helpers.jit_stub import ensure_jit_reachable
from rpython_tests.helpers.assertions import (
    fail, assert_true, assert_equal_i, assert_equal_i64,
)
from rpython_tests.helpers.fs import cleanup


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def log(msg):
    os.write(1, msg + "\n")


def make_test_schema():
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="id"),
        types.ColumnDefinition(types.TYPE_STRING, name="data"),
    ]
    return types.TableSchema(cols, 0)


def make_int_only_schema():
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
        types.ColumnDefinition(types.TYPE_U32, name="flag"),
    ]
    return types.TableSchema(cols, 0)


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
# IPC Tests
# ------------------------------------------------------------------------------

def test_unowned_buffer_lifecycle():
    """Verifies that unowned buffers inhibit growth and respect physical limits."""
    os.write(1, "[IPC] Testing Unowned Buffer Lifecycle...\n")

    size = 128
    raw_mem = lltype.malloc(rffi.CCHARP.TO, size, flavor="raw")

    try:
        buf = buffer.Buffer.from_external_ptr(raw_mem, size)
        assert_true(not buf.is_owned, "Buffer should be unowned")
        assert_true(not buf.growable, "Unowned buffer should not be growable")

        ptr = buf.alloc(64, alignment=1)
        assert_equal_i(
            rffi.cast(lltype.Signed, raw_mem),
            rffi.cast(lltype.Signed, ptr),
            "Pointer mismatch",
        )

        raised = False
        try:
            buf.alloc(128)
        except errors.MemTableFullError:
            raised = True
        assert_true(raised, "Unowned buffer failed to block overflow")

        buf.free()
        assert_true(
            buf.base_ptr == lltype.nullptr(rffi.CCHARP.TO),
            "Base ptr not neutralized",
        )

        raw_mem[0] = "A"
        assert_true(
            raw_mem[0] == "A", "Underlying memory corrupted after buffer free"
        )

    finally:
        lltype.free(raw_mem, flavor="raw")


def test_ipc_framed_transport():
    """Verifies the SOCK_STREAM length-prefixed framed transport layer."""
    os.write(1, "[IPC] Testing Framed Transport (SOCK_STREAM)...\n")

    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_STREAM)

    try:
        # Build a wire payload with _encode_wire
        schema = make_test_schema()
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=1)
        rb = RowBuilder(schema, zbatch)
        rb.begin(r_uint64(1), r_uint64(0), r_int64(1))
        rb.put_string("framed_test")
        rb.commit()

        ipc.send_framed(s1.fd, 42, zbatch, status=0, error_msg="")
        payload = ipc.recv_framed(s2.fd)

        assert_equal_i(0, payload.status, "Status mismatch")
        assert_true(payload.batch is not None, "Batch should be present")
        assert_equal_i(1, payload.batch.length(), "Batch count mismatch")

        payload.close()
        zbatch.free()
    finally:
        s1.close()
        s2.close()


def test_meta_schema_roundtrip():
    """Verifies schema -> batch -> schema roundtrip."""
    os.write(1, "[IPC] Testing Meta-Schema Roundtrip...\n")

    schema = make_test_schema()
    schema_batch = ipc.schema_to_batch(schema)

    assert_equal_i(2, schema_batch.length(), "Schema batch should have 2 rows")

    reconstructed = ipc.batch_to_schema(schema_batch)

    assert_equal_i(
        len(schema.columns),
        len(reconstructed.columns),
        "Column count mismatch",
    )
    assert_equal_i(schema.pk_index, reconstructed.pk_index, "PK index mismatch")

    for i in range(len(schema.columns)):
        assert_equal_i(
            schema.columns[i].field_type.code,
            reconstructed.columns[i].field_type.code,
            "Type code mismatch at col %d" % i,
        )

    schema_batch.free()


def test_ipc_roundtrip():
    """Verifies full serialize + receive with mixed columns (ints + strings)."""
    os.write(1, "[IPC] Testing IPC Roundtrip...\n")

    schema = make_test_schema()
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_STREAM)

    try:
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=10)
        rb = RowBuilder(schema, zbatch)
        rb.begin(r_uint64(42), r_uint64(0), r_int64(1))
        rb.put_string("Hello Zero-Copy World")
        rb.commit()
        rb.begin(r_uint64(99), r_uint64(0), r_int64(1))
        rb.put_string("Short")
        rb.commit()

        ipc.send_framed(s1.fd, 42, zbatch, status=0, error_msg="")

        payload = ipc.recv_framed(s2.fd)

        assert_equal_i(0, payload.status, "Status mismatch")
        assert_true(payload.schema is not None, "Schema should be present")
        assert_true(payload.batch is not None, "Batch should be present")
        assert_equal_i(2, payload.batch.length(), "Batch count mismatch")

        # Verify schema
        assert_equal_i(
            len(schema.columns),
            len(payload.schema.columns),
            "Schema column count mismatch",
        )
        assert_equal_i(
            schema.columns[0].field_type.code,
            payload.schema.columns[0].field_type.code,
            "Schema col 0 type mismatch",
        )
        assert_equal_i(
            schema.columns[1].field_type.code,
            payload.schema.columns[1].field_type.code,
            "Schema col 1 type mismatch",
        )

        # Verify data
        rec_batch = payload.batch
        acc = rec_batch.get_accessor(0)
        length, prefix, sptr, hptr, py_s = acc.get_str_struct(1)
        s = string_logic.resolve_string(sptr, hptr, py_s)
        assert_true(s == "Hello Zero-Copy World", "String corruption row 0")

        acc = rec_batch.get_accessor(1)
        length, prefix, sptr, hptr, py_s = acc.get_str_struct(1)
        s = string_logic.resolve_string(sptr, hptr, py_s)
        assert_true(s == "Short", "String corruption row 1")

        payload.close()
        zbatch.free()
    finally:
        s1.close()
        s2.close()


def test_int_only_roundtrip():
    """Verifies roundtrip with integer-only schema (no strings)."""
    os.write(1, "[IPC] Testing Int-Only Roundtrip...\n")

    schema = make_int_only_schema()
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_STREAM)

    try:
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=10)
        rb = RowBuilder(schema, zbatch)
        rb.begin(r_uint64(1), r_uint64(0), r_int64(1))
        rb.put_int(r_int64(42))
        rb.put_int(r_int64(7))
        rb.commit()
        rb.begin(r_uint64(2), r_uint64(0), r_int64(1))
        rb.put_int(r_int64(-100))
        rb.put_int(r_int64(255))
        rb.commit()

        ipc.send_framed(s1.fd, 10, zbatch, status=0)

        payload = ipc.recv_framed(s2.fd)

        assert_equal_i(0, payload.status, "Status mismatch")
        assert_true(payload.batch is not None, "Batch should be present")
        assert_equal_i(2, payload.batch.length(), "Row count mismatch")

        acc = payload.batch.get_accessor(0)
        assert_equal_i(42, intmask(r_uint64(acc.get_int(1))), "Row 0 col 1 mismatch")

        acc = payload.batch.get_accessor(1)
        val = rffi.cast(rffi.LONGLONG, acc.get_int(1))
        assert_equal_i(-100, intmask(val), "Row 1 col 1 mismatch")

        payload.close()
        zbatch.free()
    finally:
        s1.close()
        s2.close()


def test_error_path():
    """Verifies that error responses work."""
    os.write(1, "[IPC] Testing Error Path...\n")

    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_STREAM)

    try:
        ipc.send_framed_error(s1.fd, "Fatal VM Crash")

        payload = ipc.recv_framed(s2.fd)
        assert_equal_i(1, payload.status, "Error status mismatch")
        assert_true(
            payload.error_msg == "Fatal VM Crash", "Error string mismatch"
        )
        assert_true(
            payload.batch is None, "Error payload should have no batch"
        )
        assert_true(
            payload.schema is None, "Error payload should have no schema"
        )

        payload.close()
    finally:
        s1.close()
        s2.close()


def test_scan_request():
    """Verifies empty push (scan request) has no schema or data."""
    os.write(1, "[IPC] Testing Scan Request...\n")

    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_STREAM)

    try:
        # Send empty push (scan request): no data, no schema
        ipc.send_framed(s1.fd, 42, None, status=0)

        payload = ipc.recv_framed(s2.fd)
        assert_equal_i(0, payload.status, "Status mismatch")
        assert_true(payload.batch is None, "Scan request should have no batch")
        assert_equal_i(42, payload.target_id, "Target ID mismatch")

        payload.close()
    finally:
        s1.close()
        s2.close()


def test_string_roundtrip():
    """Verifies IPC string roundtrip with various lengths."""
    os.write(1, "[IPC] Testing String Roundtrip...\n")

    schema = make_test_schema()
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_STREAM)

    try:
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=10)
        rb = RowBuilder(schema, zbatch)

        test_strings = [
            "",         # empty
            "Hi",       # 2 bytes (short, fits in prefix)
            "Test",     # 4 bytes (exact prefix length)
            "Hello World!",  # 12 bytes (SHORT_STRING_THRESHOLD)
            "13 bytes str!",  # 13 bytes (just over threshold)
            "A" * 100,  # 100 bytes (long string in blob)
        ]

        for i in range(len(test_strings)):
            rb.begin(r_uint64(i + 1), r_uint64(0), r_int64(1))
            rb.put_string(test_strings[i])
            rb.commit()

        ipc.send_framed(s1.fd, 42, zbatch, status=0)

        payload = ipc.recv_framed(s2.fd)
        assert_equal_i(len(test_strings), payload.batch.length(), "Row count mismatch")

        for i in range(len(test_strings)):
            acc = payload.batch.get_accessor(i)
            length, prefix, sptr, hptr, py_s = acc.get_str_struct(1)
            s = string_logic.resolve_string(sptr, hptr, py_s)
            assert_true(
                s == test_strings[i],
                "String mismatch at row %d: '%s' != '%s'" % (i, s, test_strings[i]),
            )

        payload.close()
        zbatch.free()
    finally:
        s1.close()
        s2.close()


def test_null_strings():
    """Verifies NULL string encoding roundtrip."""
    os.write(1, "[IPC] Testing V2 NULL String Encoding...\n")

    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_STRING, is_nullable=True, name="data"),
    ]
    schema = types.TableSchema(cols, 0)
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_STREAM)

    try:
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=10)
        rb = RowBuilder(schema, zbatch)

        # Row with non-null string
        rb.begin(r_uint64(1), r_uint64(0), r_int64(1))
        rb.put_string("not null")
        rb.commit()

        # Row with null string
        rb.begin(r_uint64(2), r_uint64(0), r_int64(1))
        rb.put_null()
        rb.commit()

        ipc.send_framed(s1.fd, 42, zbatch, status=0)

        payload = ipc.recv_framed(s2.fd)
        assert_equal_i(2, payload.batch.length(), "Row count mismatch")

        # Row 0: non-null
        acc = payload.batch.get_accessor(0)
        assert_true(not acc.is_null(1), "Row 0 should not be null")
        length, prefix, sptr, hptr, py_s = acc.get_str_struct(1)
        s = string_logic.resolve_string(sptr, hptr, py_s)
        assert_true(s == "not null", "Row 0 string mismatch")

        # Row 1: null
        acc = payload.batch.get_accessor(1)
        assert_true(acc.is_null(1), "Row 1 should be null")

        payload.close()
        zbatch.free()
    finally:
        s1.close()
        s2.close()


def test_multi_string_column_roundtrip():
    """Exercises multi-string-column roundtrip."""
    os.write(1, "[IPC] Testing V2 Multi-String Column Roundtrip...\n")

    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_STRING, name="name"),
        types.ColumnDefinition(types.TYPE_STRING, name="description"),
    ]
    schema = types.TableSchema(cols, 0)
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_STREAM)

    try:
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=4)
        rb = RowBuilder(schema, zbatch)

        # Four rows with varying string lengths in both columns.
        # Long strings (> 12 chars) exercise the blob allocator.
        names = ["Alice", "Bob", "Charlie Davidson", "D" * 60]
        descs = ["Short desc", "X" * 50, "Medium length description", "tiny"]

        for i in range(4):
            rb.begin(r_uint64(i + 1), r_uint64(0), r_int64(1))
            rb.put_string(names[i])
            rb.put_string(descs[i])
            rb.commit()

        ipc.send_framed(s1.fd, 77, zbatch, status=0)

        payload = ipc.recv_framed(s2.fd)
        assert_equal_i(0, payload.status, "Status mismatch")
        assert_true(payload.batch is not None, "Batch should be present")
        assert_equal_i(4, payload.batch.length(), "Row count mismatch")

        for i in range(4):
            acc = payload.batch.get_accessor(i)

            length, prefix, sptr, hptr, py_s = acc.get_str_struct(1)
            got_name = string_logic.resolve_string(sptr, hptr, py_s)
            assert_true(
                got_name == names[i],
                "name mismatch row %d: '%s' != '%s'" % (i, got_name, names[i]),
            )

            length, prefix, sptr, hptr, py_s = acc.get_str_struct(2)
            got_desc = string_logic.resolve_string(sptr, hptr, py_s)
            assert_true(
                got_desc == descs[i],
                "desc mismatch row %d: '%s' != '%s'" % (i, got_desc, descs[i]),
            )

        payload.close()
        zbatch.free()
    finally:
        s1.close()
        s2.close()


def test_long_column_name_roundtrip():
    """Column names > 12 chars go to the blob arena (German String threshold).
    The schema block may then end at a non-16-aligned offset, exercising the
    schema->data block transition in encode_batch_append."""
    os.write(1, "[IPC] Testing long column name roundtrip...\n")

    schema = types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U64,    name="primary_key_col"),   # 15 chars
            types.ColumnDefinition(types.TYPE_I64,    name="measurement_value"), # 17 chars
            types.ColumnDefinition(types.TYPE_STRING, name="description_text"),  # 16 chars
        ],
        pk_index=0,
    )
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_STREAM)
    try:
        b = batch.ArenaZSetBatch(schema, initial_capacity=2)
        rb = RowBuilder(schema, b)
        rb.begin(r_uint64(1), r_uint64(0), r_int64(1))
        rb.put_int(r_int64(42))
        rb.put_string("hello world!")
        rb.commit()
        rb.begin(r_uint64(2), r_uint64(0), r_int64(1))
        rb.put_int(r_int64(99))
        rb.put_string("this is a longer string value")
        rb.commit()

        ipc.send_framed(s1.fd, 7, b, status=0)

        payload = ipc.recv_framed(s2.fd)
        assert_equal_i(0, payload.status, "Status mismatch")
        assert_true(payload.batch is not None, "Batch should be present")
        assert_equal_i(2, payload.batch.length(), "Row count mismatch")

        acc = payload.batch.get_accessor(0)
        assert_equal_i(42, intmask(r_uint64(acc.get_int(1))), "Row 0 col 1 mismatch")

        acc = payload.batch.get_accessor(1)
        length, prefix, sptr, hptr, py_s = acc.get_str_struct(2)
        s = string_logic.resolve_string(sptr, hptr, py_s)
        assert_true(s == "this is a longer string value", "Row 1 string mismatch")

        payload.close()
        b.free()
    finally:
        s1.close()
        s2.close()


def test_schema_mismatch():
    """Verifies that schema validation rejects mismatched schemas."""
    os.write(1, "[IPC] Testing V2 Schema Mismatch Detection...\n")

    from gnitz.server.executor import _validate_schema_match

    schema_a = make_test_schema()
    schema_b = make_int_only_schema()

    raised = False
    try:
        _validate_schema_match(schema_a, schema_b)
    except errors.StorageError:
        raised = True
    assert_true(raised, "Schema validation should reject mismatched schemas")

    # Same schema should pass
    schema_c = make_test_schema()
    _validate_schema_match(schema_a, schema_c)


def test_control_schema_roundtrip():
    """Verifies CONTROL_SCHEMA encode/decode roundtrip preserves all fields."""
    os.write(1, "[IPC] Testing control schema roundtrip...\n")

    # --- Case 1: OK message, empty error string ---
    ctrl_batch = ipc._encode_control_batch(
        target_id=42, client_id=7,
        flags=r_uint64(ipc.FLAG_PUSH),
        seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0,
        status=ipc.STATUS_OK, error_msg="",
    )
    assert_equal_i(1, ctrl_batch.length(), "Control batch should have 1 row")

    buf = buffer.Buffer(0)
    wal_columnar.encode_batch_to_buffer(
        buf, ipc.CONTROL_SCHEMA, r_uint64(0), ipc.IPC_CONTROL_TID, ctrl_batch
    )
    ctrl_batch.free()

    decoded = wal_columnar.decode_batch_from_ptr(
        buf.base_ptr, buf.offset, ipc.CONTROL_SCHEMA
    )
    p = ipc.IPCPayload()
    ipc._decode_control_batch(decoded, p)
    decoded.free()
    buf.free()

    assert_equal_i(42, p.target_id, "target_id mismatch")
    assert_equal_i(7, p.client_id, "client_id mismatch")
    assert_equal_i(ipc.STATUS_OK, p.status, "status mismatch")
    assert_true(p.error_msg == "", "error_msg should be empty")
    assert_true(
        p.flags == r_uint64(ipc.FLAG_PUSH),
        "flags mismatch",
    )

    # --- Case 2: ERROR message with non-null error string ---
    ctrl_batch2 = ipc._encode_control_batch(
        target_id=0, client_id=0,
        flags=r_uint64(0),
        seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0,
        status=ipc.STATUS_ERROR, error_msg="something went wrong",
    )
    buf2 = buffer.Buffer(0)
    wal_columnar.encode_batch_to_buffer(
        buf2, ipc.CONTROL_SCHEMA, r_uint64(0), ipc.IPC_CONTROL_TID, ctrl_batch2
    )
    ctrl_batch2.free()

    decoded2 = wal_columnar.decode_batch_from_ptr(
        buf2.base_ptr, buf2.offset, ipc.CONTROL_SCHEMA
    )
    p2 = ipc.IPCPayload()
    ipc._decode_control_batch(decoded2, p2)
    decoded2.free()
    buf2.free()

    assert_equal_i(ipc.STATUS_ERROR, p2.status, "status mismatch")
    assert_true(
        p2.error_msg == "something went wrong",
        "error_msg mismatch: '%s'" % p2.error_msg,
    )


# ------------------------------------------------------------------------------
# Server Tests
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

    from gnitz.core.opcodes import (
        EXPR_LOAD_COL_INT, EXPR_LOAD_CONST, EXPR_CMP_GT,
    )
    from rpython_tests.helpers.circuit_builder import CircuitBuilder

    # Build expression bytecode: col1 > 150
    # reg 0 = col[1], reg 1 = 150, reg 2 = reg0 > reg1
    expr_code = [
        EXPR_LOAD_COL_INT, 0, 1, 0,   # dst=0, col_idx=1
        EXPR_LOAD_CONST,   1, 150, 0,  # dst=1, lo=150, hi=0
        EXPR_CMP_GT,       2, 0, 1,    # dst=2, src1=0, src2=1
    ]
    expr_num_regs = 3
    expr_result_reg = 2

    # Build circuit graph
    builder = CircuitBuilder(view_id=0, primary_source_id=source_table_id)
    src = builder.input_delta()
    flt = builder.filter(src, expr_code=expr_code,
                         expr_num_regs=expr_num_regs,
                         expr_result_reg=expr_result_reg)
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
        # IPC tests (no engine needed)
        test_unowned_buffer_lifecycle()
        test_ipc_framed_transport()
        test_meta_schema_roundtrip()
        test_ipc_roundtrip()
        test_int_only_roundtrip()
        test_error_path()
        test_scan_request()
        test_string_roundtrip()
        test_null_strings()
        test_multi_string_column_roundtrip()
        test_long_column_name_roundtrip()
        test_schema_mismatch()
        test_control_schema_roundtrip()

        # Server tests (need engine)
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
