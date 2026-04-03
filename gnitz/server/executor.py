# gnitz/server/executor.py

import os
import time
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint64, r_int64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.server import ipc, ipc_ffi, rust_ffi
from gnitz.core import errors
from gnitz.storage.owned_batch import ArenaZSetBatch
from gnitz.core.keys import promote_to_index_key
from gnitz.catalog import system_tables as sys
from gnitz.catalog.engine import catalog_advance_sequence
from gnitz.storage import engine_ffi
from gnitz import log


def _raise_catalog_error(context):
    """Raise LayoutError with the last catalog FFI error message."""
    out_len = lltype.malloc(rffi.UINTP.TO, 1, flavor="raw")
    try:
        err_ptr = engine_ffi._catalog_last_error(out_len)
        elen = intmask(out_len[0])
        if elen > 0 and err_ptr:
            msg = rffi.charpsize2str(rffi.cast(rffi.CCHARP, err_ptr), elen)
        else:
            msg = context
    finally:
        lltype.free(out_len, flavor="raw")
    raise errors.LayoutError(msg)


def _catalog_get_schema(engine, tid):
    """Get schema for a table/view via catalog FFI, with real column names."""
    schema_buf = lltype.malloc(rffi.CCHARP.TO, engine_ffi.SCHEMA_DESC_SIZE, flavor="raw")
    try:
        rc = engine_ffi._catalog_get_schema_desc(
            engine._handle, rffi.cast(rffi.LONGLONG, tid),
            rffi.cast(rffi.VOIDP, schema_buf))
        if intmask(rc) < 0:
            raise errors.StorageError("catalog_get_schema_desc failed for tid=%d" % tid)
        schema = engine_ffi.unpack_schema(schema_buf)
    finally:
        lltype.free(schema_buf, flavor="raw")
    # Fetch real column names from catalog
    name_buf = lltype.malloc(rffi.CCHARP.TO, 256, flavor="raw")
    try:
        for ci in range(len(schema.columns)):
            nlen = intmask(engine_ffi._catalog_get_col_name(
                engine._handle, rffi.cast(rffi.LONGLONG, tid),
                rffi.cast(rffi.UINT, ci),
                rffi.cast(rffi.VOIDP, name_buf), rffi.cast(rffi.UINT, 256)))
            if nlen > 0:
                schema.columns[ci].name = rffi.charpsize2str(name_buf, nlen)
    finally:
        lltype.free(name_buf, flavor="raw")
    return schema


def _catalog_has_id(engine, tid):
    """Check if a table/view id exists in the catalog."""
    return intmask(engine_ffi._catalog_has_id(
        engine._handle, rffi.cast(rffi.LONGLONG, tid))) != 0


def _build_check_batch(schema, lo_list, hi_list):
    """Build a batch of (pk_lo, pk_hi, weight=1, null payloads) for PK existence checks."""
    batch = ArenaZSetBatch(schema, initial_capacity=len(lo_list))
    builder = sys.RowBuilder(schema, batch)
    for k in range(len(lo_list)):
        builder.begin(lo_list[k], hi_list[k], r_int64(1))
        for _pi in range(schema.n_payload):
            builder.put_null()
        builder.commit()
    return batch


def _validate_unique_distributed(cat_handle, target_id, batch, dispatcher, engine):
    """Cross-epoch, cross-partition unique index check.

    The Rust _catalog_validate_unique_indices only catches within-batch
    duplicates and checks the master's (empty) index tables.  This function
    broadcasts each new index key to all workers and raises LayoutError if any
    worker already holds that key.

    Called from the two multi-worker push paths in dispatch_push, before
    fan_out_push / pending.add.
    """
    n_circuits = intmask(engine_ffi._catalog_get_index_circuit_count(
        cat_handle, rffi.cast(rffi.LONGLONG, target_id)))
    if n_circuits == 0:
        return

    out_col = lltype.malloc(rffi.UINTP.TO, 1, flavor="raw")
    out_uniq = lltype.malloc(rffi.INTP.TO, 1, flavor="raw")
    out_tc = lltype.malloc(rffi.UCHARP.TO, 1, flavor="raw")
    idx_schema_buf = lltype.malloc(rffi.CCHARP.TO, engine_ffi.SCHEMA_DESC_SIZE, flavor="raw")
    try:
        # Quick check: any unique circuit?
        has_unique = False
        for ci in range(n_circuits):
            rc = engine_ffi._catalog_get_index_circuit_info(
                cat_handle, rffi.cast(rffi.LONGLONG, target_id),
                rffi.cast(rffi.UINT, ci), out_col, out_uniq, out_tc)
            if intmask(rc) >= 0 and intmask(out_uniq[0]) != 0:
                has_unique = True
                break
        if not has_unique:
            return

        source_schema = _catalog_get_schema(engine, target_id)
        n = batch.length()

        # Find which PKs already exist so we can skip UPSERT rows (the worker
        # handles retract+reinsert; re-inserting the same value is not a
        # violation).
        existing_pks = {}
        pk_lo_list = newlist_hint(n)
        pk_hi_list = newlist_hint(n)
        for i in range(n):
            if batch.get_weight(i) <= r_int64(0):
                continue
            pk_lo_list.append(r_uint64(batch._read_pk_lo(i)))
            pk_hi_list.append(r_uint64(batch._read_pk_hi(i)))
        if len(pk_lo_list) > 0:
            pk_batch = _build_check_batch(source_schema, pk_lo_list, pk_hi_list)
            existing_pks = dispatcher.check_pk_existence(
                target_id, pk_batch, source_schema)
            pk_batch.free()

        # Check each unique index circuit
        for ci in range(n_circuits):
            rc = engine_ffi._catalog_get_index_circuit_info(
                cat_handle, rffi.cast(rffi.LONGLONG, target_id),
                rffi.cast(rffi.UINT, ci), out_col, out_uniq, out_tc)
            if intmask(rc) < 0 or intmask(out_uniq[0]) == 0:
                continue
            source_col_idx = intmask(out_col[0])

            rc2 = engine_ffi._catalog_get_index_circuit_schema(
                cat_handle, rffi.cast(rffi.LONGLONG, target_id),
                rffi.cast(rffi.UINT, ci),
                rffi.cast(rffi.VOIDP, idx_schema_buf))
            if intmask(rc2) < 0:
                continue
            idx_schema = engine_ffi.unpack_schema(idx_schema_buf)
            col_type = source_schema.columns[source_col_idx].field_type

            # Collect unique positive-weight index keys from the batch
            check_lo = newlist_hint(n)
            check_hi = newlist_hint(n)
            seen_dict = {}  # {int: {int: True}} for O(1) dedup
            acc = batch.get_accessor(0)
            for i in range(n):
                if batch.get_weight(i) <= r_int64(0):
                    continue
                batch.bind_accessor(i, acc)
                if acc.is_null(source_col_idx):
                    continue
                pk_lo_i = intmask(r_uint64(batch._read_pk_lo(i)))
                pk_hi_i = intmask(r_uint64(batch._read_pk_hi(i)))
                if pk_lo_i in existing_pks and pk_hi_i in existing_pks[pk_lo_i]:
                    continue
                idx_key = promote_to_index_key(acc, source_col_idx, col_type)
                lo = r_uint64(intmask(idx_key))
                hi = r_uint64(intmask(idx_key >> 64))
                lo_int = intmask(lo)
                hi_int = intmask(hi)
                if lo_int in seen_dict and hi_int in seen_dict[lo_int]:
                    raise errors.LayoutError(
                        "Unique index violation on column '%s': duplicate in batch"
                        % source_schema.columns[source_col_idx].name)
                if lo_int not in seen_dict:
                    seen_dict[lo_int] = {}
                seen_dict[lo_int][hi_int] = True
                check_lo.append(lo)
                check_hi.append(hi)

            if len(check_lo) == 0:
                continue

            chk_batch = _build_check_batch(idx_schema, check_lo, check_hi)
            any_exists = dispatcher.check_pk_exists_broadcast(
                target_id, source_col_idx, chk_batch, idx_schema)
            chk_batch.free()

            if any_exists:
                raise errors.LayoutError(
                    "Unique index violation on column '%s'"
                    % source_schema.columns[source_col_idx].name)
    finally:
        lltype.free(out_col, flavor="raw")
        lltype.free(out_uniq, flavor="raw")
        lltype.free(out_tc, flavor="raw")
        lltype.free(idx_schema_buf, flavor="raw")


MAX_PENDING_ROWS    = 100000
TICK_COALESCE_ROWS  = 10000   # min rows before firing a tick
TICK_DEADLINE_MS    = 20      # max ms to wait before firing regardless

STATUS_OK = 0
STATUS_ERROR = 1

# Tick state machine
_TICK_IDLE = 0
_TICK_ACTIVE = 1


class DeferredRequest(object):
    """A scan/seek request deferred while an async tick is in-flight."""

    def __init__(self, fd, client_id, target_id, flags,
                 seek_pk_lo, seek_pk_hi, seek_col_idx):
        self.fd = fd
        self.client_id = client_id
        self.target_id = target_id
        self.flags = flags
        self.seek_pk_lo = seek_pk_lo
        self.seek_pk_hi = seek_pk_hi
        self.seek_col_idx = seek_col_idx


def _sort_pending_by_tid(fds, cids, tids, batches, schemas):
    """Insertion sort parallel lists by target_id. RPython-safe."""
    for i in range(1, len(tids)):
        key_tid = tids[i]
        key_fd = fds[i]
        key_cid = cids[i]
        key_batch = batches[i]
        key_schema = schemas[i]
        j = i - 1
        while j >= 0 and tids[j] > key_tid:
            tids[j + 1] = tids[j]
            fds[j + 1] = fds[j]
            cids[j + 1] = cids[j]
            batches[j + 1] = batches[j]
            schemas[j + 1] = schemas[j]
            j -= 1
        tids[j + 1] = key_tid
        fds[j + 1] = key_fd
        cids[j + 1] = key_cid
        batches[j + 1] = key_batch
        schemas[j + 1] = key_schema


def evaluate_dag(engine, initial_source_id, initial_delta):
    """
    Module-level topological DAG evaluator (single-worker only).

    Delegates entirely to Rust DagEngine via a single FFI call.
    Multi-worker DAG evaluation is handled by Rust worker.rs.
    """
    delta_clone = initial_delta.clone()
    engine_ffi._dag_evaluate(
        engine.dag_handle, initial_source_id, delta_clone._handle)
    # Ownership transferred — prevent double-free
    delta_clone._handle = lltype.nullptr(rffi.VOIDP.TO)
    delta_clone.free()


def _validate_schema_match(wire_schema, expected_schema):
    """Validates that a wire schema matches the expected schema."""
    wire_cols = wire_schema.columns
    exp_cols = expected_schema.columns
    if len(wire_cols) != len(exp_cols):
        raise errors.StorageError(
            "Schema mismatch: expected %d columns, got %d"
            % (len(exp_cols), len(wire_cols))
        )
    if wire_schema.pk_index != expected_schema.pk_index:
        raise errors.StorageError(
            "Schema mismatch: expected pk_index=%d, got %d"
            % (expected_schema.pk_index, wire_schema.pk_index)
        )
    for i in range(len(wire_cols)):
        if wire_cols[i].field_type.code != exp_cols[i].field_type.code:
            raise errors.StorageError(
                "Schema mismatch at column %d: expected type %d, got %d"
                % (i, exp_cols[i].field_type.code, wire_cols[i].field_type.code)
            )


class PendingBatch(object):
    """Accumulates bufferable pushes for batch flush to workers."""

    def __init__(self):
        self.fds = newlist_hint(16)
        self.cids = newlist_hint(16)
        self.tids = newlist_hint(16)
        self.batches = newlist_hint(16)
        self.schemas = newlist_hint(16)
        self.row_count = 0

    def add(self, fd, cid, tid, batch, schema):
        self.fds.append(fd)
        self.cids.append(cid)
        self.tids.append(tid)
        self.batches.append(batch)
        self.schemas.append(schema)
        self.row_count += batch.length()

    def is_empty(self):
        return len(self.fds) == 0

    def clear(self):
        del self.fds[:]
        del self.cids[:]
        del self.tids[:]
        del self.batches[:]
        del self.schemas[:]
        self.row_count = 0


class ServerExecutor(object):
    """
    Coordinates IPC sessions and the Reactive DAG.
    Uses topological ranking to ensure glitch-free incremental evaluation.
    """

    _immutable_fields_ = ["engine", "dag_handle", "dispatcher"]

    def __init__(self, engine, dispatcher=None):
        self.engine = engine
        self.dag_handle = engine.dag_handle
        self.dispatcher = dispatcher

        # Deferred tick tracking (multi-worker only)
        self._tick_tids    = newlist_hint(8)  # [int] table_ids with pending ticks
        self._tick_schemas = {}               # tid -> schema
        self._tick_rows    = {}               # tid -> int (rows accumulated since last tick)
        self._t_last_push = 0.0             # wall-clock time of most recent buffered push

        # LSN tracking
        self._ingest_lsn        = 0  # global monotonic counter; increments per flush batch
        self._last_tick_lsn     = 0  # ingest_lsn at time of last _fire_pending_ticks()
        self._last_response_lsn = 0  # side-channel from handle_push -> response dispatch

        # Schema cache (tid -> TableSchema with column names)
        self._schema_cache = {}

        # Transport handle (set by run_socket_server)
        self._transport = lltype.nullptr(rffi.VOIDP.TO)

        # Non-blocking tick state machine
        self._tick_state = _TICK_IDLE
        self._tick_queue_tids = newlist_hint(8)
        self._tick_queue_schemas = {}             # tid -> Schema
        self._deferred_requests = newlist_hint(8) # [DeferredRequest]

    def run_socket_server(self, socket_path):
        server_fd = ipc_ffi.server_create(socket_path)
        if server_fd < 0:
            raise errors.StorageError("Failed to create server socket")

        transport = rust_ffi.create(256)
        self._transport = transport
        try:
            rust_ffi.accept(transport, server_fd)
            self._run_server(transport)
        finally:
            rust_ffi.destroy(transport)
            self._transport = lltype.nullptr(rffi.VOIDP.TO)

    # -- Transport event loop ------------------------------------------------

    def _run_server(self, transport):
        out_fds = lltype.malloc(rffi.INTP.TO, 256, flavor='raw')
        out_ptrs = lltype.malloc(rffi.VOIDPP.TO, 256, flavor='raw')
        out_lens = lltype.malloc(rffi.UINTP.TO, 256, flavor='raw')
        pending = PendingBatch()

        try:
            while True:
                jit.promote(self.engine)

                if self.dispatcher is not None:
                    crashed = self.dispatcher.check_workers()
                    if crashed >= 0:
                        log_path = (
                            self.engine.base_dir
                            + "/worker_"
                            + str(crashed)
                            + ".log"
                        )
                        os.write(
                            2,
                            "Worker %d crashed (log: %s), shutting down\n"
                            % (crashed, log_path),
                        )
                        self.dispatcher.shutdown_workers()
                        return

                if self._tick_state == _TICK_ACTIVE:
                    timeout_ms = 1  # short poll: yield CPU to workers
                else:
                    timeout_ms = 500
                    if len(self._tick_tids) > 0 and self._t_last_push > 0.0:
                        elapsed_ms = int(
                            (time.time() - self._t_last_push) * 1000.0)
                        remaining = TICK_DEADLINE_MS - elapsed_ms
                        if remaining < timeout_ms:
                            timeout_ms = remaining if remaining > 0 else 0

                n = rust_ffi.poll(
                    transport, timeout_ms,
                    out_fds, out_ptrs, out_lens, 256)

                for i in range(n):
                    fd = intmask(out_fds[i])
                    ptr = out_ptrs[i]
                    length = intmask(out_lens[i])
                    try:
                        self._dispatch_message(
                            transport, fd, ptr, length, pending)
                        if pending.row_count >= MAX_PENDING_ROWS:
                            if self._tick_state != _TICK_ACTIVE:
                                self._flush_pending_pushes(
                                    transport, pending.fds, pending.cids,
                                    pending.tids, pending.batches,
                                    pending.schemas,
                                )
                                pending.clear()
                    finally:
                        rust_ffi.free_recv(transport, ptr)

                if not pending.is_empty():
                    if self._tick_state != _TICK_ACTIVE:
                        self._flush_pending_pushes(
                            transport, pending.fds, pending.cids,
                            pending.tids, pending.batches,
                            pending.schemas,
                        )
                        pending.clear()

                if self._tick_state == _TICK_ACTIVE:
                    self._poll_tick_active(transport, pending)
                else:
                    self._check_and_fire_pending_ticks_async()
        finally:
            lltype.free(out_fds, flavor='raw')
            lltype.free(out_ptrs, flavor='raw')
            lltype.free(out_lens, flavor='raw')

    def _dispatch_message(self, transport, fd, ptr, length, pending):
        """Parse and dispatch a complete message."""
        payload = None
        try:
            payload = ipc._parse_from_ptr(
                rffi.cast(rffi.CCHARP, ptr), length)
            # NOTE: do NOT set payload.raw_buf — Rust owns the recv buffer.
            # It is freed via rust_ffi.free_recv in the caller's finally block.
            self._dispatch_payload(
                transport, fd, payload, pending)
        except errors.GnitzError as ge:
            cid = 0
            tid = 0
            if payload is not None:
                cid = intmask(payload.client_id)
                tid = intmask(payload.target_id)
            try:
                self._send_error_response(
                    transport, fd, ge.msg, target_id=tid,
                    client_id=cid)
            except errors.GnitzError:
                rust_ffi.close_fd(transport, fd)
        except Exception as e:
            os.write(2, "MASTER EXCEPTION: " + str(e) + "\n")
            rust_ffi.close_fd(transport, fd)

    def _dispatch_payload(self, transport, fd, payload, pending):
        """Core message dispatch. Returns rows buffered (0 = immediate)."""
        client_id = intmask(payload.client_id)
        target_id = intmask(payload.target_id)

        # ID allocation — immediate response, no barrier needed
        if target_id == 0:
            if payload.flags & ipc.FLAG_ALLOCATE_TABLE_ID:
                new_id = intmask(engine_ffi._catalog_allocate_table_id(
                    self.engine._handle))
                catalog_advance_sequence(
                    self.engine._handle, sys.SEQ_ID_TABLES, new_id - 1, new_id)
                self._send_response(
                    transport, fd, new_id, None, STATUS_OK, "", client_id)
                return
            if payload.flags & ipc.FLAG_ALLOCATE_SCHEMA_ID:
                new_id = intmask(engine_ffi._catalog_allocate_schema_id(
                    self.engine._handle))
                catalog_advance_sequence(
                    self.engine._handle, sys.SEQ_ID_SCHEMAS, new_id - 1, new_id)
                self._send_response(
                    transport, fd, new_id, None, STATUS_OK, "", client_id)
                return
            if payload.flags & ipc.FLAG_ALLOCATE_INDEX_ID:
                new_id = intmask(engine_ffi._catalog_allocate_index_id(
                    self.engine._handle))
                catalog_advance_sequence(
                    self.engine._handle, sys.SEQ_ID_INDICES, new_id - 1, new_id)
                self._send_response(
                    transport, fd, new_id, None, STATUS_OK, "", client_id)
                return

        # During TICK_ACTIVE, defer reads that need consistent view state
        if self._tick_state == _TICK_ACTIVE:
            if payload.flags & ipc.FLAG_SEEK_BY_INDEX:
                self._deferred_requests.append(DeferredRequest(
                    fd, client_id, target_id,
                    intmask(payload.flags),
                    intmask(payload.seek_pk_lo),
                    intmask(payload.seek_pk_hi),
                    intmask(payload.seek_col_idx)))
                return
            if payload.flags & ipc.FLAG_SEEK:
                self._deferred_requests.append(DeferredRequest(
                    fd, client_id, target_id,
                    intmask(payload.flags),
                    intmask(payload.seek_pk_lo),
                    intmask(payload.seek_pk_hi), 0))
                return
            # Scans (empty-batch push to user table with dispatcher)
            if (self.dispatcher is not None
                    and target_id >= sys.FIRST_USER_TABLE_ID
                    and (payload.batch is None
                         or payload.batch.length() == 0)):
                self._deferred_requests.append(DeferredRequest(
                    fd, client_id, target_id, 0, 0, 0, 0))
                return

        # Index seek — flush pushes for this table + fire ticks
        if payload.flags & ipc.FLAG_SEEK_BY_INDEX:
            self._flush_pending_for_tid(
                transport, target_id, pending)
            self._fire_pending_ticks()
            col_idx = intmask(payload.seek_col_idx)
            schema = _catalog_get_schema(self.engine, target_id)
            if self.dispatcher is not None and target_id >= sys.FIRST_USER_TABLE_ID:
                result = self.dispatcher.fan_out_seek_by_index(
                    target_id, col_idx,
                    intmask(payload.seek_pk_lo),
                    intmask(payload.seek_pk_hi), schema)
            else:
                result = self._seek_by_index(
                    target_id, col_idx,
                    payload.seek_pk_lo, payload.seek_pk_hi)
            resp_schema = result._schema if result is not None else schema
            self._send_response(
                transport, fd, target_id, result, STATUS_OK, "",
                client_id, schema=resp_schema)
            if result is not None:
                result.free()
            return

        # PK seek — flush pushes for this table (no tick needed)
        if payload.flags & ipc.FLAG_SEEK:
            self._flush_pending_for_tid(
                transport, target_id, pending)
            schema = _catalog_get_schema(self.engine, target_id)
            if self.dispatcher is not None and target_id >= sys.FIRST_USER_TABLE_ID:
                result = self.dispatcher.fan_out_seek(
                    target_id,
                    intmask(payload.seek_pk_lo),
                    intmask(payload.seek_pk_hi), schema)
            else:
                result = self._seek_family(
                    target_id, payload.seek_pk_lo, payload.seek_pk_hi)
            resp_schema = result._schema if result is not None else schema
            self._send_response(
                transport, fd, target_id, result, STATUS_OK, "",
                client_id, schema=resp_schema)
            if result is not None:
                result.free()
            return

        target_schema = None
        has_batch = payload.batch is not None
        if has_batch:
            target_schema = _catalog_get_schema(self.engine, target_id)
            if payload.schema is not None:
                _validate_schema_match(payload.schema, target_schema)

        # Bufferable: multi-worker user-table DML with data
        if (
            self.dispatcher is not None
            and target_id >= sys.FIRST_USER_TABLE_ID
            and has_batch
            and payload.batch.length() > 0
        ):
            cat_h = self.engine._handle
            rc = engine_ffi._catalog_validate_fk_inline(
                cat_h, rffi.cast(rffi.LONGLONG, target_id),
                payload.batch._handle)
            if intmask(rc) < 0:
                _raise_catalog_error("FK validation failed")
            rc = engine_ffi._catalog_validate_unique_indices(
                cat_h, rffi.cast(rffi.LONGLONG, target_id),
                payload.batch._handle)
            if intmask(rc) < 0:
                _raise_catalog_error("Unique index violation")
            _validate_unique_distributed(
                cat_h, target_id, payload.batch, self.dispatcher, self.engine)
            cloned = payload.batch.clone()
            pending.add(fd, client_id, target_id, cloned, target_schema)
            return

        # Non-bufferable: flush pushes for this table, then process inline
        self._flush_pending_for_tid(transport, target_id, pending)

        result = self.handle_push(target_id, payload.batch)
        lsn = self._last_response_lsn
        self._last_response_lsn = 0

        if (
            self.dispatcher is not None
            and target_id < sys.FIRST_USER_TABLE_ID
            and has_batch
            and payload.batch.length() > 0
        ):
            if target_schema is None:
                target_schema = _catalog_get_schema(self.engine, target_id)
            self.dispatcher.broadcast_ddl(
                target_id, payload.batch, target_schema)

        resp_schema = None
        if result is not None:
            resp_schema = result._schema
        elif target_schema is not None:
            resp_schema = target_schema
        else:
            resp_schema = _catalog_get_schema(self.engine, target_id)
        self._send_response(
            transport, fd, target_id, result, STATUS_OK, "",
            client_id, schema=resp_schema, seek_pk_lo=lsn)
        if result is not None:
            result.free()

    def _send_response(self, transport, fd, target_id, result, status,
                       error_msg, client_id, schema=None, flags=0,
                       seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0):
        """Encode and send a response via the Rust transport."""
        eff_schema = schema
        if eff_schema is None and result is not None:
            eff_schema = result._schema
        eff_flags = r_uint64(flags)
        wire_buf = ipc._encode_wire(
            target_id, client_id, result, eff_schema, eff_flags,
            seek_pk_lo, seek_pk_hi, seek_col_idx, status, error_msg)
        rust_ffi.send(
            transport, fd,
            wire_buf.base_ptr, wire_buf.offset)
        wire_buf.free()

    def _send_error_response(self, transport, fd, error_msg,
                             target_id=0, client_id=0):
        """Send an error response via the Rust transport."""
        self._send_response(
            transport, fd, target_id, None,
            STATUS_ERROR, error_msg, client_id)

    # -- Unified Protocol ---------------------------------------------------

    def handle_push(self, target_id, in_batch):
        """
        Unified protocol handler.  Every operation is a batch push.

        - non-empty batch  -> UPSERT: ingest into target, trigger hooks & DAG
        - empty/None batch -> SCAN:   return snapshot of target's current state

        Works identically for system tables (DDL via hooks) and user tables
        (DML + reactive cascade).  Returns a response batch (scan) or None.

        When a dispatcher is active, user-table operations are fanned out
        to worker processes. System tables stay local to master.
        """
        if self.dispatcher is not None and target_id >= sys.FIRST_USER_TABLE_ID:
            schema = _catalog_get_schema(self.engine, target_id)
            if in_batch is not None and in_batch.length() > 0:
                cat_h = self.engine._handle
                rc = engine_ffi._catalog_validate_fk_inline(
                    cat_h, rffi.cast(rffi.LONGLONG, target_id),
                    in_batch._handle)
                if intmask(rc) < 0:
                    _raise_catalog_error("FK validation failed")
                rc = engine_ffi._catalog_validate_unique_indices(
                    cat_h, rffi.cast(rffi.LONGLONG, target_id),
                    in_batch._handle)
                if intmask(rc) < 0:
                    _raise_catalog_error("Unique index violation")
                _validate_unique_distributed(
                    cat_h, target_id, in_batch, self.dispatcher, self.engine)
                self.dispatcher.fan_out_push(target_id, in_batch, schema)
                return None  # (non-socket path; LSN not tracked here)
            else:
                self._fire_pending_ticks()               # ensure views are current
                self._last_response_lsn = self._last_tick_lsn
                return self.dispatcher.fan_out_scan(target_id, schema)

        if in_batch is not None and in_batch.length() > 0:
            cat_h = self.engine._handle
            if target_id >= sys.FIRST_USER_TABLE_ID:
                # User table: FK validation + ingest + flush + DAG cascade
                # in one combined call (ensures unique_pk effective batch
                # propagates correctly to views).
                rc = engine_ffi._catalog_validate_fk_inline(
                    cat_h,
                    rffi.cast(rffi.LONGLONG, target_id),
                    in_batch._handle)
                if intmask(rc) < 0:
                    _raise_catalog_error("FK validation failed for tid=%d" % target_id)
                rc = engine_ffi._catalog_validate_unique_indices(
                    cat_h,
                    rffi.cast(rffi.LONGLONG, target_id),
                    in_batch._handle)
                if intmask(rc) < 0:
                    _raise_catalog_error("Unique index violation for tid=%d" % target_id)
                # Transfer ownership of in_batch to Rust (no clone needed).
                rc = engine_ffi._catalog_push_and_evaluate(
                    cat_h,
                    rffi.cast(rffi.LONGLONG, target_id),
                    in_batch._handle)
                in_batch._handle = lltype.nullptr(rffi.VOIDP.TO)
                if intmask(rc) < 0:
                    _raise_catalog_error("push_and_evaluate failed for tid=%d" % target_id)
            else:
                # System table: ingest + flush + DAG (no unique_pk concern)
                batch_clone = in_batch.clone()
                rc = engine_ffi._catalog_ingest(
                    cat_h,
                    rffi.cast(rffi.LONGLONG, target_id),
                    batch_clone._handle)
                batch_clone._handle = lltype.nullptr(rffi.VOIDP.TO)
                batch_clone.free()
                if intmask(rc) < 0:
                    _raise_catalog_error("Ingest failed for tid=%d" % target_id)
                engine_ffi._catalog_flush(
                    cat_h,
                    rffi.cast(rffi.LONGLONG, target_id))
                self._evaluate_dag(target_id, in_batch)
            self._ingest_lsn += 1
            self._last_tick_lsn = self._ingest_lsn
            self._last_response_lsn = self._ingest_lsn
            return None
        else:
            self._last_response_lsn = self._last_tick_lsn
            return self._scan_family(target_id)

    def _get_cached_schema(self, target_id):
        """Get schema with caching to avoid repeated FFI column name lookups."""
        if target_id in self._schema_cache:
            return self._schema_cache[target_id]
        schema = _catalog_get_schema(self.engine, target_id)
        self._schema_cache[target_id] = schema
        return schema

    def _scan_family(self, target_id):
        """Build a batch containing every positive-weight row in the target."""
        schema = self._get_cached_schema(target_id)
        result_handle = engine_ffi._catalog_scan(
            self.engine._handle,
            rffi.cast(rffi.LONGLONG, target_id))
        if not result_handle:
            return ArenaZSetBatch(schema)
        return ArenaZSetBatch._wrap_handle(schema, result_handle, True, True)

    def _seek_family(self, target_id, pk_lo_raw, pk_hi_raw):
        """Point lookup: returns a one-row ArenaZSetBatch or None."""
        result_handle = engine_ffi._catalog_seek(
            self.engine._handle,
            rffi.cast(rffi.LONGLONG, target_id),
            rffi.cast(rffi.ULONGLONG, pk_lo_raw),
            rffi.cast(rffi.ULONGLONG, pk_hi_raw))
        if not result_handle:
            return None
        schema = self._get_cached_schema(target_id)
        return ArenaZSetBatch._wrap_handle(schema, result_handle, True, True)

    def _seek_by_index(self, table_id, col_idx, key_lo, key_hi):
        """Index-assisted point lookup via catalog FFI. Returns single-row batch or None."""
        result_handle = engine_ffi._catalog_seek_by_index(
            self.engine._handle,
            rffi.cast(rffi.LONGLONG, table_id),
            rffi.cast(rffi.UINT, col_idx),
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi))
        if not result_handle:
            return None
        schema = self._get_cached_schema(table_id)
        return ArenaZSetBatch._wrap_handle(schema, result_handle, True, True)

    def _flush_pending_for_tid(self, transport, target_id, pending):
        """Flush only pending pushes for a specific target_id."""
        if pending.is_empty():
            return
        flush_fds = newlist_hint(4)
        flush_cids = newlist_hint(4)
        flush_tids = newlist_hint(4)
        flush_batches = newlist_hint(4)
        flush_schemas = newlist_hint(4)
        keep = 0
        for i in range(len(pending.fds)):
            if pending.tids[i] == target_id:
                flush_fds.append(pending.fds[i])
                flush_cids.append(pending.cids[i])
                flush_tids.append(pending.tids[i])
                flush_batches.append(pending.batches[i])
                flush_schemas.append(pending.schemas[i])
            else:
                pending.fds[keep] = pending.fds[i]
                pending.cids[keep] = pending.cids[i]
                pending.tids[keep] = pending.tids[i]
                pending.batches[keep] = pending.batches[i]
                pending.schemas[keep] = pending.schemas[i]
                keep += 1
        # Trim lists to kept entries
        while len(pending.fds) > keep:
            del pending.fds[-1]
            del pending.cids[-1]
            del pending.tids[-1]
            del pending.batches[-1]
            del pending.schemas[-1]
        pending.row_count = 0
        for i in range(len(pending.batches)):
            pending.row_count += pending.batches[i].length()
        if len(flush_fds) > 0:
            self._flush_pending_pushes(
                transport, flush_fds, flush_cids, flush_tids,
                flush_batches, flush_schemas)

    def _flush_pending_pushes(self, transport, p_fds, p_cids, p_tids,
                              p_batches, p_schemas):
        """Merge same-target pushes and fan out once per target."""
        n = len(p_fds)
        if n == 0:
            return

        _sort_pending_by_tid(p_fds, p_cids, p_tids, p_batches, p_schemas)

        run_start = 0
        while run_start < n:
            target_id = p_tids[run_start]
            schema = p_schemas[run_start]
            merged = p_batches[run_start]
            run_end = run_start + 1
            while run_end < n and p_tids[run_end] == target_id:
                merged.append_batch(p_batches[run_end])
                p_batches[run_end].free()
                run_end += 1

            err_msg = ""
            try:
                self.dispatcher.fan_out_ingest(target_id, merged, schema)
            except errors.GnitzError as ge:
                err_msg = ge.msg

            n_rows = merged.length()
            merged.free()

            if len(err_msg) == 0:
                self._ingest_lsn += 1
                current_lsn = self._ingest_lsn
                if target_id not in self._tick_schemas:
                    self._tick_tids.append(target_id)
                    self._tick_schemas[target_id] = schema
                    self._tick_rows[target_id] = 0
                self._t_last_push = time.time()
                self._tick_rows[target_id] = self._tick_rows[target_id] + n_rows
            else:
                current_lsn = 0

            for k in range(run_start, run_end):
                try:
                    if len(err_msg) > 0:
                        self._send_error_response(
                            transport, p_fds[k], err_msg,
                            target_id=target_id, client_id=p_cids[k])
                    else:
                        self._send_response(
                            transport, p_fds[k], target_id, None,
                            STATUS_OK, "",
                            p_cids[k], schema=schema,
                            seek_pk_lo=current_lsn)
                except Exception:
                    rust_ffi.close_fd(transport, p_fds[k])

            run_start = run_end

    def _check_and_fire_pending_ticks(self):
        """Fire pending ticks if coalesce threshold or deadline reached."""
        if len(self._tick_tids) == 0:
            return
        for tid in self._tick_tids:
            if self._tick_rows[tid] >= TICK_COALESCE_ROWS:
                self._fire_pending_ticks()
                return
        if self._t_last_push > 0.0:
            elapsed_ms = int((time.time() - self._t_last_push) * 1000.0)
            if elapsed_ms >= TICK_DEADLINE_MS:
                self._fire_pending_ticks()

    def _fire_pending_ticks(self):
        """Fire all pending ticks unconditionally."""
        if len(self._tick_tids) == 0:
            return
        tids = self._tick_tids
        self._tick_tids = newlist_hint(8)
        for tid in tids:
            schema = self._tick_schemas[tid]
            del self._tick_schemas[tid]
            del self._tick_rows[tid]
            # Table may have been dropped between ingest and tick; skip if gone.
            if not _catalog_has_id(self.engine, tid):
                continue
            try:
                self.dispatcher.fan_out_tick(tid, schema)
            except errors.GnitzError as ge:
                log.warn("fan_out_tick error tid=%d: %s" % (tid, ge.msg))
        self._last_tick_lsn = self._ingest_lsn
        self._t_last_push = 0.0

    def _evaluate_dag(self, initial_source_id, initial_delta):
        evaluate_dag(self.engine, initial_source_id, initial_delta)

    # -- Non-blocking tick state machine ------------------------------------

    def _check_and_fire_pending_ticks_async(self):
        """Check tick deadline/threshold; transition to TICK_ACTIVE if ready.

        Falls through to the synchronous path in single-process mode.
        """
        if self.dispatcher is None:
            self._check_and_fire_pending_ticks()
            return
        if len(self._tick_tids) == 0:
            return
        should_fire = False
        for tid in self._tick_tids:
            if self._tick_rows[tid] >= TICK_COALESCE_ROWS:
                should_fire = True
                break
        if not should_fire and self._t_last_push > 0.0:
            elapsed_ms = int((time.time() - self._t_last_push) * 1000.0)
            if elapsed_ms >= TICK_DEADLINE_MS:
                should_fire = True
        if not should_fire:
            return

        # Move pending ticks to the async queue
        self._tick_queue_tids = self._tick_tids
        self._tick_queue_schemas = self._tick_schemas
        self._tick_tids = newlist_hint(8)
        self._tick_schemas = {}
        self._tick_rows.clear()
        self._t_last_push = 0.0

        self._start_next_async_tick()

    def _start_next_async_tick(self):
        """Pop the next queued tid and start its async tick."""
        if len(self._tick_queue_tids) == 0:
            return
        tid = self._tick_queue_tids[0]
        del self._tick_queue_tids[0]
        schema = self._tick_queue_schemas.get(tid, None)
        if tid in self._tick_queue_schemas:
            del self._tick_queue_schemas[tid]

        if schema is None or not _catalog_has_id(self.engine, tid):
            # Table dropped or schema missing; skip to next
            if len(self._tick_queue_tids) > 0:
                self._start_next_async_tick()
            return

        self._tick_state = _TICK_ACTIVE
        try:
            self.dispatcher.start_tick_async(tid, schema)
        except errors.GnitzError as ge:
            log.warn("start_tick_async error tid=%d: %s" % (tid, ge.msg))
            self._tick_state = _TICK_IDLE
            if len(self._tick_queue_tids) > 0:
                self._start_next_async_tick()

    def _poll_tick_active(self, transport, pending):
        """Non-blocking progress on the current async tick."""
        try:
            done = self.dispatcher.poll_tick_progress()
        except errors.GnitzError as ge:
            log.warn("poll_tick_progress error: %s" % ge.msg)
            done = True

        if not done:
            return

        log.debug("async tick complete")
        self._last_tick_lsn = self._ingest_lsn

        # More tids queued?
        if len(self._tick_queue_tids) > 0:
            self._start_next_async_tick()
            return

        # All ticks complete — transition to IDLE
        self._tick_state = _TICK_IDLE

        # Flush pushes accumulated during the tick
        if not pending.is_empty():
            self._flush_pending_pushes(
                transport, pending.fds, pending.cids,
                pending.tids, pending.batches,
                pending.schemas,
            )
            pending.clear()

        # Process deferred scan/seek requests
        self._process_deferred_requests(transport)

    def _process_deferred_requests(self, transport):
        """Replay scan/seek requests that were deferred during TICK_ACTIVE."""
        deferred = self._deferred_requests
        self._deferred_requests = newlist_hint(8)

        for i in range(len(deferred)):
            req = deferred[i]
            try:
                if req.flags & ipc.FLAG_SEEK_BY_INDEX:
                    schema = _catalog_get_schema(
                        self.engine, req.target_id)
                    if (self.dispatcher is not None
                            and req.target_id >= sys.FIRST_USER_TABLE_ID):
                        result = self.dispatcher.fan_out_seek_by_index(
                            req.target_id, req.seek_col_idx,
                            req.seek_pk_lo, req.seek_pk_hi, schema)
                    else:
                        result = self._seek_by_index(
                            req.target_id, req.seek_col_idx,
                            req.seek_pk_lo, req.seek_pk_hi)
                    resp_schema = (result._schema
                                   if result is not None else schema)
                    self._send_response(
                        transport, req.fd, req.target_id, result,
                        STATUS_OK, "", req.client_id, schema=resp_schema)
                    if result is not None:
                        result.free()
                elif req.flags & ipc.FLAG_SEEK:
                    schema = _catalog_get_schema(
                        self.engine, req.target_id)
                    if (self.dispatcher is not None
                            and req.target_id >= sys.FIRST_USER_TABLE_ID):
                        result = self.dispatcher.fan_out_seek(
                            req.target_id, req.seek_pk_lo,
                            req.seek_pk_hi, schema)
                    else:
                        result = self._seek_family(
                            req.target_id, req.seek_pk_lo, req.seek_pk_hi)
                    resp_schema = (result._schema
                                   if result is not None else schema)
                    self._send_response(
                        transport, req.fd, req.target_id, result,
                        STATUS_OK, "", req.client_id, schema=resp_schema)
                    if result is not None:
                        result.free()
                else:
                    # Deferred scan (empty-batch push)
                    self._last_response_lsn = self._last_tick_lsn
                    schema = _catalog_get_schema(
                        self.engine, req.target_id)
                    if self.dispatcher is not None:
                        result = self.dispatcher.fan_out_scan(
                            req.target_id, schema)
                    else:
                        result = self._scan_family(req.target_id)
                    resp_schema = (result._schema
                                   if result is not None else schema)
                    self._send_response(
                        transport, req.fd, req.target_id, result,
                        STATUS_OK, "", req.client_id, schema=resp_schema,
                        seek_pk_lo=self._last_response_lsn)
                    if result is not None:
                        result.free()
            except errors.GnitzError as ge:
                try:
                    self._send_error_response(
                        transport, req.fd, ge.msg,
                        target_id=req.target_id, client_id=req.client_id)
                except errors.GnitzError:
                    rust_ffi.close_fd(transport, req.fd)
            except Exception:
                rust_ffi.close_fd(transport, req.fd)
