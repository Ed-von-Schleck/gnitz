# gnitz/server/executor.py

import os
import time
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.server import ipc, ipc_ffi, rust_ffi
from gnitz.core import errors
from gnitz.core.batch import ArenaZSetBatch, BatchWriter
from gnitz.core.types import TYPE_U128
from gnitz.catalog import system_tables as sys
from gnitz.catalog.registry import (
    ingest_to_family, validate_fk_inline, validate_fk_distributed,
    validate_unique_indices_distributed,
)
from gnitz.dbsp import ops
from gnitz import log


def _sort_pending(pending):
    """Insertion sort by depth (index 0). RPython-safe."""
    for i in range(1, len(pending)):
        item = pending[i]
        j = i - 1
        while j >= 0 and pending[j][0] > item[0]:
            pending[j + 1] = pending[j]
            j -= 1
        pending[j + 1] = item


def _sort_pending_desc(pending):
    """Insertion sort by depth DESCENDING (deeper left, shallowest at tail). RPython-safe."""
    for i in range(1, len(pending)):
        item = pending[i]
        j = i - 1
        while j >= 0 and pending[j][0] < item[0]:
            pending[j + 1] = pending[j]
            j -= 1
        pending[j + 1] = item

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


def _pending_key(view_id, source_id):
    """Encode (view_id, source_id) as a single int for dict keying."""
    return (view_id << 32) | (source_id & 0xFFFFFFFF)


def evaluate_dag(engine, initial_source_id, initial_delta,
                 exchange_handler=None):
    """
    Module-level topological DAG evaluator.
    Uses a depth-sorted approach to ensure each view evaluates exactly once
    per epoch, even in diamond dependency graphs.

    Called by both the single-process ServerExecutor and multi-worker
    WorkerProcess paths. When exchange_handler is provided, plans with
    exchange_post_plan will perform IPC exchange mid-circuit.

    Pending entries are 4-tuples: (depth, view_id, source_id, batch).
    source_id tracks which source table triggered the delta, enabling
    multi-input circuits to bind to the correct register.
    """
    registry = engine.registry
    program_cache = engine.program_cache

    dep_map = program_cache.get_dep_map(registry)

    # pending: list of (depth, view_id, source_id, batch), sorted DESCENDING by depth
    # so the shallowest (min-depth) node is always at the tail for O(1) pop.
    first_layer = dep_map.get(initial_source_id, [])
    pending = []
    pending_pos = {}   # _pending_key(view_id, source_id) -> index in pending
    for v_id in first_layer:
        d = registry.get_depth(v_id)
        pending.append((d, v_id, initial_source_id, initial_delta.clone()))
    _sort_pending_desc(pending)
    for pi in range(len(pending)):
        pk = _pending_key(pending[pi][1], pending[pi][2])
        pending_pos[pk] = pi

    while len(pending) > 0:
        depth, target_view_id, source_id, incoming_delta = pending[-1]   # O(1) tail = min-depth
        del pending[-1]                                                   # O(1)
        del pending_pos[_pending_key(target_view_id, source_id)]

        plan = program_cache.get_program(target_view_id)
        if plan is None:
            log.warn("evaluate_dag: no plan for view_id=%d, skipping" % target_view_id)
            incoming_delta.free()
            continue

        if plan.exchange_post_plan is not None and exchange_handler is not None:
            # Multi-worker: pre-plan -> (optional IPC exchange) -> post-plan
            pre_result = plan.execute_epoch(incoming_delta, source_id=source_id)
            incoming_delta.free()
            if pre_result is None:
                pre_result = ArenaZSetBatch(plan.out_schema)

            if plan.skip_exchange:
                # Co-partitioned: master pre-sent the batch; stash hit in do_exchange,
                # or skip entirely (worker already has correct partition).
                out_delta = plan.exchange_post_plan.execute_epoch(pre_result)
                pre_result.free()
            else:
                exchanged = exchange_handler.do_exchange(
                    target_view_id, pre_result
                )
                pre_result.free()
                out_delta = plan.exchange_post_plan.execute_epoch(exchanged)
                exchanged.free()
        elif plan.exchange_post_plan is not None:
            # Single-process: pre-plan -> post-plan (no exchange needed)
            pre_result = plan.execute_epoch(incoming_delta, source_id=source_id)
            incoming_delta.free()
            if pre_result is None:
                pre_result = ArenaZSetBatch(plan.out_schema)
            out_delta = plan.exchange_post_plan.execute_epoch(pre_result)
            pre_result.free()
        elif (plan.join_shard_map is not None
              and source_id in plan.join_shard_map
              and exchange_handler is not None):
            # Multi-worker join: exchange raw delta by join key column
            # BEFORE running the circuit, so all rows with the same join
            # key land on the same worker (co-partitioning).
            copart = (plan.co_partitioned_join_sources is not None
                      and source_id in plan.co_partitioned_join_sources)
            if copart:
                # Shard col == source PK: push routing already co-partitions data.
                out_delta = plan.execute_epoch(incoming_delta, source_id=source_id)
                incoming_delta.free()
            else:
                exchanged = exchange_handler.do_exchange(
                    target_view_id, incoming_delta,
                    source_id=source_id,
                )
                incoming_delta.free()
                out_delta = plan.execute_epoch(exchanged, source_id=source_id)
                exchanged.free()
        else:
            out_delta = plan.execute_epoch(incoming_delta, source_id=source_id)
            incoming_delta.free()

        has_output = out_delta is not None and out_delta.length() > 0
        if has_output:
            view_family = registry.get_by_id(target_view_id)
            ingest_to_family(view_family, out_delta)
            view_family.store.flush()

        # In multi-worker mode, downstream views must always be queued
        # even with empty deltas, so every worker participates in
        # exchange barriers. Skipping would deadlock the master.
        if has_output or exchange_handler is not None:
            dependents = dep_map.get(target_view_id, [])
            for dep_id in dependents:
                dep_pk = _pending_key(dep_id, target_view_id)
                if dep_pk in pending_pos:                        # O(1) lookup
                    if has_output:
                        found = pending_pos[dep_pk]
                        existing_d, existing_id, existing_sid, existing_batch = pending[found]
                        merged = ArenaZSetBatch(existing_batch._schema)
                        writer = BatchWriter(merged)
                        ops.op_union(existing_batch, out_delta, writer)
                        existing_batch.free()
                        pending[found] = (existing_d, existing_id, existing_sid, merged)
                        # pending_pos[dep_pk] unchanged — in-place update
                else:
                    d = registry.get_depth(dep_id)
                    if has_output:
                        pending.append((d, dep_id, target_view_id, out_delta.clone()))
                    else:
                        dep_family = registry.get_by_id(dep_id)
                        pending.append((d, dep_id, target_view_id, ArenaZSetBatch(dep_family.schema)))
                    _sort_pending_desc(pending)
                    # All positions may have shifted — full rebuild
                    pending_pos.clear()
                    for pi in range(len(pending)):
                        ppk = _pending_key(pending[pi][1], pending[pi][2])
                        pending_pos[ppk] = pi

        if has_output:
            out_delta.free()


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

    _immutable_fields_ = ["engine", "program_cache", "dispatcher"]

    def __init__(self, engine, dispatcher=None):
        self.engine = engine
        self.program_cache = engine.program_cache
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
                    timeout_ms = 0  # stay responsive during async tick
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
                new_id = self.engine.registry.allocate_table_id()
                self.engine._advance_sequence(
                    sys.SEQ_ID_TABLES, new_id - 1, new_id)
                self._send_response(
                    transport, fd, new_id, None, STATUS_OK, "", client_id)
                return
            if payload.flags & ipc.FLAG_ALLOCATE_SCHEMA_ID:
                new_id = self.engine.registry.allocate_schema_id()
                self.engine._advance_sequence(
                    sys.SEQ_ID_SCHEMAS, new_id - 1, new_id)
                self._send_response(
                    transport, fd, new_id, None, STATUS_OK, "", client_id)
                return
            if payload.flags & ipc.FLAG_ALLOCATE_INDEX_ID:
                new_id = self.engine.registry.allocate_index_id()
                self.engine._advance_sequence(
                    sys.SEQ_ID_INDICES, new_id - 1, new_id)
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
            schema = self.engine.registry.get_by_id(target_id).schema
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
            schema = self.engine.registry.get_by_id(target_id).schema
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

        family = None
        if payload.batch is not None:
            family = self.engine.registry.get_by_id(target_id)
            if payload.schema is not None:
                _validate_schema_match(payload.schema, family.schema)

        # Bufferable: multi-worker user-table DML with data
        if (
            self.dispatcher is not None
            and target_id >= sys.FIRST_USER_TABLE_ID
            and family is not None
            and payload.batch.length() > 0
        ):
            validate_fk_distributed(
                family, payload.batch, self.dispatcher)
            validate_unique_indices_distributed(
                family, payload.batch, self.dispatcher)
            cloned = payload.batch.clone()
            pending.add(fd, client_id, target_id, cloned, family.schema)
            return

        # Non-bufferable: flush pushes for this table, then process inline
        self._flush_pending_for_tid(transport, target_id, pending)

        result = self.handle_push(target_id, payload.batch)
        lsn = self._last_response_lsn
        self._last_response_lsn = 0

        if (
            self.dispatcher is not None
            and target_id < sys.FIRST_USER_TABLE_ID
            and family is not None
            and payload.batch.length() > 0
        ):
            self.dispatcher.broadcast_ddl(
                target_id, payload.batch, family.schema)

        resp_schema = None
        if result is not None:
            resp_schema = result._schema
        elif family is not None:
            resp_schema = family.schema
        else:
            resp_schema = self.engine.registry.get_by_id(target_id).schema
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
            family = self.engine.registry.get_by_id(target_id)
            schema = family.schema
            if in_batch is not None and in_batch.length() > 0:
                validate_fk_distributed(family, in_batch, self.dispatcher)
                validate_unique_indices_distributed(family, in_batch, self.dispatcher)
                self.dispatcher.fan_out_push(target_id, in_batch, schema)
                return None  # (non-socket path; LSN not tracked here)
            else:
                self._fire_pending_ticks()               # ensure views are current
                self._last_response_lsn = self._last_tick_lsn
                return self.dispatcher.fan_out_scan(target_id, schema)

        if in_batch is not None and in_batch.length() > 0:
            family = self.engine.registry.get_by_id(target_id)
            validate_fk_inline(family, in_batch)
            effective = ingest_to_family(family, in_batch)
            family.store.flush()
            self._evaluate_dag(target_id, effective)
            if effective is not in_batch:
                effective.free()
            self._ingest_lsn += 1
            self._last_tick_lsn = self._ingest_lsn
            self._last_response_lsn = self._ingest_lsn
            return None
        else:
            self._last_response_lsn = self._last_tick_lsn
            return self._scan_family(target_id)

    def _scan_family(self, target_id):
        """Build a batch containing every positive-weight row in the target."""
        family = self.engine.registry.get_by_id(target_id)
        schema = family.schema
        result = ArenaZSetBatch(schema)
        cursor = family.store.create_cursor()
        try:
            while cursor.is_valid():
                w = cursor.weight()
                if w > r_int64(0):
                    acc = cursor.get_accessor()
                    result.append_from_accessor(cursor.key_lo(), cursor.key_hi(), w, acc)
                cursor.advance()
        finally:
            cursor.close()
        return result

    def _seek_family(self, target_id, pk_lo_raw, pk_hi_raw):
        """Point lookup: returns a one-row ArenaZSetBatch or None."""
        family = self.engine.registry.get_by_id(target_id)
        cursor = family.store.create_cursor()
        try:
            cursor.seek(r_uint64(pk_lo_raw), r_uint64(pk_hi_raw))
            if not cursor.is_valid():
                return None
            if cursor.key_lo() != r_uint64(pk_lo_raw) or cursor.key_hi() != r_uint64(pk_hi_raw):
                return None
            w = cursor.weight()
            if w <= r_int64(0):
                return None
            result = ArenaZSetBatch(family.schema, initial_capacity=1)
            acc = cursor.get_accessor()
            result.append_from_accessor(r_uint64(pk_lo_raw), r_uint64(pk_hi_raw), w, acc)
            return result
        finally:
            cursor.close()

    def _seek_by_index(self, table_id, col_idx, key_lo, key_hi):
        """Index-assisted point lookup. Returns single-row batch or None."""
        family = self.engine.registry.get_by_id(table_id)
        circuit = None
        for c in family.index_circuits:
            if c.source_col_idx == col_idx:
                circuit = c
                break
        if circuit is None:
            return None   # no index on this column

        idx_cursor = circuit.table.create_cursor()
        try:
            idx_cursor.seek(r_uint64(key_lo), r_uint64(key_hi))
            if not idx_cursor.is_valid() or idx_cursor.key_lo() != r_uint64(key_lo) or idx_cursor.key_hi() != r_uint64(key_hi):
                return None
            # source_pk is at col 1 in the 2-col index schema.
            # Type dispatch needed: source_pk_type is U64 or U128.
            if circuit.source_pk_type.code == TYPE_U128.code:
                source_pk_lo = idx_cursor.get_accessor().get_u128_lo(1)
                source_pk_hi = idx_cursor.get_accessor().get_u128_hi(1)
            else:
                source_pk_lo = r_uint64(intmask(idx_cursor.get_accessor().get_int(1)))
                source_pk_hi = r_uint64(0)
            src_lo = intmask(source_pk_lo)
            src_hi = intmask(source_pk_hi)
        finally:
            idx_cursor.close()
        return self._seek_family(table_id, src_lo, src_hi)

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
            if not self.engine.registry.has_id(tid):
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

        if schema is None or not self.engine.registry.has_id(tid):
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
                    schema = self.engine.registry.get_by_id(
                        req.target_id).schema
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
                    schema = self.engine.registry.get_by_id(
                        req.target_id).schema
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
                    schema = self.engine.registry.get_by_id(
                        req.target_id).schema
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
