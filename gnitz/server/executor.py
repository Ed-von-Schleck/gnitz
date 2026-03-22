# gnitz/server/executor.py

import os
import time
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.server import ipc, ipc_ffi, uring_ffi
from gnitz.core import errors
from gnitz.storage.buffer import c_memmove
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

# io_uring user_data encoding: tag (8 bits) | fd (56 bits)
_TAG_ACCEPT = r_uint64(0x01) << 56
_TAG_RECV = r_uint64(0x02) << 56
_TAG_SEND = r_uint64(0x03) << 56
_TAG_MASK = r_uint64(0xFF) << 56
_FD_MASK = r_uint64(0x00FFFFFFFFFFFFFF)

# Per-connection state machine states
_CONN_WAITING_HEADER = 0
_CONN_WAITING_PAYLOAD = 1
_CONN_SENDING = 2


def _make_udata(tag, fd):
    return tag | r_uint64(fd)


def _udata_tag(udata):
    return udata & _TAG_MASK


def _udata_fd(udata):
    return intmask(udata & _FD_MASK)


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

        # Connection Registries
        self.active_fds = newlist_hint(16)
        self.client_fds = {}  # int(fd) -> 1 (set of known client fds)
        self.fd_to_client = {}  # int(fd) -> int(client_id)
        self.client_to_fd = {}  # int(client_id) -> int(fd)

        # Poll-cache: rebuilt only when active_fds changes (Fix N2)
        self._poll_fds    = newlist_hint(8)
        self._poll_events = newlist_hint(8)
        self._poll_dirty  = True

        # Deferred tick tracking (multi-worker only)
        self._tick_tids    = newlist_hint(8)  # [int] table_ids with pending ticks
        self._tick_schemas = {}               # tid -> schema
        self._tick_rows    = {}               # tid -> int (rows accumulated since last tick)
        self._t_first_tick = 0.0             # wall-clock time of oldest pending tick

        # Per-connection SOCK_STREAM header buffers
        self._client_hdr_bufs = {}   # int(fd) -> rffi.CCHARP (4 raw bytes)
        self._client_hdr_pos  = {}   # int(fd) -> rffi.INTP (1-element raw array)

        # LSN tracking
        self._ingest_lsn        = 0  # global monotonic counter; increments per flush batch
        self._last_tick_lsn     = 0  # ingest_lsn at time of last _fire_pending_ticks()
        self._last_response_lsn = 0  # side-channel from handle_push → _drain_client

        # io_uring state (unused in poll path)
        self._use_uring = False
        self._ring = lltype.nullptr(rffi.VOIDP.TO)
        self._conn_state = {}       # fd -> int (_CONN_WAITING_*)
        self._conn_hdr_buf = {}     # fd -> rffi.CCHARP (4 bytes, raw)
        self._conn_hdr_pos = {}     # fd -> int (bytes of header received)
        self._conn_payload_buf = {} # fd -> rffi.CCHARP (payload, raw)
        self._conn_payload_len = {} # fd -> int (total expected)
        self._conn_payload_pos = {} # fd -> int (bytes received)
        self._send_bufs = {}        # fd -> rffi.CCHARP (framed wire data, raw)
        self._send_lens = {}        # fd -> int (total bytes)
        self._send_pos = {}         # fd -> int (bytes sent)

    def run_socket_server(self, socket_path):
        server_fd = ipc_ffi.server_create(socket_path)
        if server_fd < 0:
            raise errors.StorageError("Failed to create server socket")
        self.active_fds.append(server_fd)

        ring = lltype.nullptr(rffi.VOIDP.TO)
        if os.environ.get("GNITZ_FORCE_POLL") != "1":
            try:
                ring = uring_ffi.uring_create(256)
            except errors.StorageError:
                pass

        if ring:
            self._use_uring = True
            self._ring = ring
            try:
                self._run_uring_server(ring, server_fd)
            finally:
                uring_ffi.uring_destroy(ring)
                self._use_uring = False
                self._ring = lltype.nullptr(rffi.VOIDP.TO)
        else:
            self._run_poll_server(server_fd)

    def _run_poll_server(self, server_fd):
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

            if self._poll_dirty:
                count = len(self.active_fds)
                self._poll_fds = newlist_hint(count)
                self._poll_events = newlist_hint(count)
                for i in range(count):
                    self._poll_fds.append(self.active_fds[i])
                    self._poll_events.append(ipc_ffi.POLLIN | ipc_ffi.POLLERR | ipc_ffi.POLLHUP)
                self._poll_dirty = False
            fds = self._poll_fds
            timeout_ms = 500
            if len(self._tick_tids) > 0 and self._t_first_tick > 0.0:
                elapsed_ms = int((time.time() - self._t_first_tick) * 1000.0)
                remaining  = TICK_DEADLINE_MS - elapsed_ms
                if remaining < timeout_ms:
                    timeout_ms = remaining if remaining > 0 else 0
            revents = ipc_ffi.poll(self._poll_fds, self._poll_events, timeout_ms)

            pending_fds = newlist_hint(16)
            pending_cids = newlist_hint(16)
            pending_tids = newlist_hint(16)
            pending_batches = newlist_hint(16)
            pending_schemas = newlist_hint(16)
            pending_row_count = 0

            # Pass 1: cleanup disconnected clients before accepting new ones.
            for i in range(len(fds)):
                fd = fds[i]
                rev = revents[i]
                if rev == 0:
                    continue
                if rev & (ipc_ffi.POLLERR | ipc_ffi.POLLHUP | ipc_ffi.POLLNVAL):
                    if fd != server_fd:
                        self._cleanup_client(fd)

            # Pass 2: accept new connections and drain existing clients.
            for i in range(len(fds)):
                fd = fds[i]
                rev = revents[i]
                if rev == 0:
                    continue
                if rev & (ipc_ffi.POLLERR | ipc_ffi.POLLHUP | ipc_ffi.POLLNVAL):
                    continue
                if rev & ipc_ffi.POLLIN:
                    if fd == server_fd:
                        while True:
                            c_fd = ipc_ffi.server_accept(server_fd)
                            if c_fd < 0:
                                break
                            self.active_fds.append(c_fd)
                            self.client_fds[c_fd] = 1
                            hdr_buf = lltype.malloc(rffi.CCHARP.TO, 4, flavor='raw')
                            hdr_pos = lltype.malloc(rffi.INTP.TO, 1, flavor='raw')
                            hdr_pos[0] = rffi.cast(rffi.INT, 0)
                            self._client_hdr_bufs[c_fd] = hdr_buf
                            self._client_hdr_pos[c_fd] = hdr_pos
                            self._poll_dirty = True
                    else:
                        while True:
                            rows = self._drain_client(
                                fd, pending_fds, pending_cids,
                                pending_tids, pending_batches,
                                pending_schemas,
                            )
                            if rows < 0:
                                break
                            pending_row_count += rows
                            if pending_row_count >= MAX_PENDING_ROWS:
                                self._flush_pending_pushes(
                                    pending_fds, pending_cids,
                                    pending_tids, pending_batches,
                                    pending_schemas,
                                )
                                pending_fds = newlist_hint(16)
                                pending_cids = newlist_hint(16)
                                pending_tids = newlist_hint(16)
                                pending_batches = newlist_hint(16)
                                pending_schemas = newlist_hint(16)
                                pending_row_count = 0

            if len(pending_fds) > 0:
                self._flush_pending_pushes(
                    pending_fds, pending_cids,
                    pending_tids, pending_batches,
                    pending_schemas,
                )

            self._check_and_fire_pending_ticks()

    # -- io_uring event loop ------------------------------------------------

    def _run_uring_server(self, ring, server_fd):
        uring_ffi.uring_prep_accept(
            ring, server_fd, _make_udata(_TAG_ACCEPT, 0))

        pending_fds = newlist_hint(16)
        pending_cids = newlist_hint(16)
        pending_tids = newlist_hint(16)
        pending_batches = newlist_hint(16)
        pending_schemas = newlist_hint(16)
        pending_row_count = 0

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

            timeout_ms = 500
            if len(self._tick_tids) > 0 and self._t_first_tick > 0.0:
                elapsed_ms = int((time.time() - self._t_first_tick) * 1000.0)
                remaining = TICK_DEADLINE_MS - elapsed_ms
                if remaining < timeout_ms:
                    timeout_ms = remaining if remaining > 0 else 0

            uring_ffi.uring_submit_and_wait_timeout(ring, 1, timeout_ms)
            count, udata_list, res_list, flags_list = uring_ffi.uring_drain(
                ring, 256)

            # Pass 1: cleanup disconnects/errors (fd reuse safety)
            for i in range(count):
                tag = _udata_tag(udata_list[i])
                if tag == _TAG_ACCEPT:
                    continue
                if res_list[i] <= 0:
                    fd = _udata_fd(udata_list[i])
                    if fd in self._conn_state:
                        self._cleanup_client(fd)

            # Pass 2: process successful completions
            for i in range(count):
                tag = _udata_tag(udata_list[i])
                res = res_list[i]
                cqe_flags = flags_list[i]

                if tag == _TAG_ACCEPT:
                    self._handle_accept_cqe(ring, server_fd, res, cqe_flags)
                elif tag == _TAG_RECV:
                    fd = _udata_fd(udata_list[i])
                    if fd not in self._conn_state:
                        continue
                    if res <= 0:
                        continue
                    rows = self._handle_recv_cqe(
                        ring, fd, res,
                        pending_fds, pending_cids, pending_tids,
                        pending_batches, pending_schemas,
                    )
                    if rows > 0:
                        pending_row_count += rows
                        if pending_row_count >= MAX_PENDING_ROWS:
                            self._flush_pending_pushes(
                                pending_fds, pending_cids,
                                pending_tids, pending_batches,
                                pending_schemas,
                            )
                            pending_fds = newlist_hint(16)
                            pending_cids = newlist_hint(16)
                            pending_tids = newlist_hint(16)
                            pending_batches = newlist_hint(16)
                            pending_schemas = newlist_hint(16)
                            pending_row_count = 0
                elif tag == _TAG_SEND:
                    fd = _udata_fd(udata_list[i])
                    if fd not in self._conn_state:
                        continue
                    if res <= 0:
                        continue
                    self._handle_send_cqe(ring, fd, res)

            if len(pending_fds) > 0:
                self._flush_pending_pushes(
                    pending_fds, pending_cids,
                    pending_tids, pending_batches,
                    pending_schemas,
                )
                pending_fds = newlist_hint(16)
                pending_cids = newlist_hint(16)
                pending_tids = newlist_hint(16)
                pending_batches = newlist_hint(16)
                pending_schemas = newlist_hint(16)
                pending_row_count = 0

            self._check_and_fire_pending_ticks()

    def _handle_accept_cqe(self, ring, server_fd, res, cqe_flags):
        if res < 0:
            if not (cqe_flags & uring_ffi.CQE_F_MORE):
                uring_ffi.uring_prep_accept(
                    ring, server_fd, _make_udata(_TAG_ACCEPT, 0))
            return
        c_fd = res
        self.active_fds.append(c_fd)
        self.client_fds[c_fd] = 1
        self._poll_dirty = True

        hdr_buf = lltype.malloc(rffi.CCHARP.TO, 4, flavor='raw')
        self._conn_state[c_fd] = _CONN_WAITING_HEADER
        self._conn_hdr_buf[c_fd] = hdr_buf
        self._conn_hdr_pos[c_fd] = 0

        uring_ffi.uring_prep_recv(
            ring, c_fd, hdr_buf, 4, _make_udata(_TAG_RECV, c_fd))

        if not (cqe_flags & uring_ffi.CQE_F_MORE):
            uring_ffi.uring_prep_accept(
                ring, server_fd, _make_udata(_TAG_ACCEPT, 0))

    def _handle_recv_cqe(self, ring, fd, res,
                         p_fds, p_cids, p_tids, p_batches, p_schemas):
        state = self._conn_state.get(fd, -1)
        if state == _CONN_WAITING_HEADER:
            self._conn_hdr_pos[fd] = self._conn_hdr_pos[fd] + res
            if self._conn_hdr_pos[fd] < 4:
                pos = self._conn_hdr_pos[fd]
                hdr_buf = self._conn_hdr_buf[fd]
                uring_ffi.uring_prep_recv(
                    ring, fd, rffi.ptradd(hdr_buf, pos), 4 - pos,
                    _make_udata(_TAG_RECV, fd))
                return 0

            hdr_buf = self._conn_hdr_buf[fd]
            payload_len = ipc._decode_u32_le(hdr_buf)

            if payload_len == 0:
                self._cleanup_client(fd)
                return -1

            payload_buf = lltype.malloc(
                rffi.CCHARP.TO, payload_len, flavor='raw')
            self._conn_state[fd] = _CONN_WAITING_PAYLOAD
            self._conn_payload_buf[fd] = payload_buf
            self._conn_payload_len[fd] = payload_len
            self._conn_payload_pos[fd] = 0

            uring_ffi.uring_prep_recv(
                ring, fd, payload_buf, payload_len,
                _make_udata(_TAG_RECV, fd))
            return 0

        elif state == _CONN_WAITING_PAYLOAD:
            self._conn_payload_pos[fd] = self._conn_payload_pos[fd] + res
            if self._conn_payload_pos[fd] < self._conn_payload_len[fd]:
                pos = self._conn_payload_pos[fd]
                payload_buf = self._conn_payload_buf[fd]
                remaining = self._conn_payload_len[fd] - pos
                uring_ffi.uring_prep_recv(
                    ring, fd, rffi.ptradd(payload_buf, pos), remaining,
                    _make_udata(_TAG_RECV, fd))
                return 0

            payload_buf = self._conn_payload_buf[fd]
            payload_len = self._conn_payload_len[fd]
            # Transfer ownership: remove from dict before passing to
            # _process_complete_message (which frees via IPCPayload.close)
            del self._conn_payload_buf[fd]
            del self._conn_payload_len[fd]
            del self._conn_payload_pos[fd]
            return self._process_complete_message(
                ring, fd, payload_buf, payload_len,
                p_fds, p_cids, p_tids, p_batches, p_schemas)

        return -1

    def _process_complete_message(self, ring, fd, raw_ptr, raw_len,
                                  p_fds, p_cids, p_tids, p_batches,
                                  p_schemas):
        payload = None
        rows_buffered = 0
        try:
            payload = ipc._parse_from_ptr(raw_ptr, raw_len)
            payload.raw_buf = raw_ptr
            payload.raw_buf_size = raw_len
            rows_buffered = self._dispatch_payload(
                fd, payload, p_fds, p_cids, p_tids, p_batches, p_schemas)
        except errors.ClientDisconnectedError:
            self._cleanup_client(fd)
            return -1
        except errors.GnitzError as ge:
            err_msg = ge.msg
            tid = intmask(payload.target_id) if payload else 0
            cid = intmask(payload.client_id) if payload else 0
            try:
                self._send_error_response(fd, err_msg, target_id=tid,
                                          client_id=cid)
            except errors.GnitzError:
                self._cleanup_client(fd)
                return -1
        except Exception as e:
            os.write(2, "MASTER EXCEPTION: " + str(e) + "\n")
            self._cleanup_client(fd)
            return -1
        finally:
            if payload is not None:
                payload.close()
            else:
                lltype.free(raw_ptr, flavor='raw')
        return rows_buffered

    def _dispatch_payload(self, fd, payload, p_fds, p_cids, p_tids,
                          p_batches, p_schemas):
        """Core message dispatch. Returns rows buffered (0 = immediate)."""
        client_id = intmask(payload.client_id)
        target_id = intmask(payload.target_id)

        # ID allocation — immediate response
        if target_id == 0:
            if payload.flags & ipc.FLAG_ALLOCATE_TABLE_ID:
                new_id = self.engine.registry.allocate_table_id()
                self.engine._advance_sequence(
                    sys.SEQ_ID_TABLES, new_id - 1, new_id)
                self._send_response(
                    fd, new_id, None, STATUS_OK, "", client_id)
                return 0
            if payload.flags & ipc.FLAG_ALLOCATE_SCHEMA_ID:
                new_id = self.engine.registry.allocate_schema_id()
                self.engine._advance_sequence(
                    sys.SEQ_ID_SCHEMAS, new_id - 1, new_id)
                self._send_response(
                    fd, new_id, None, STATUS_OK, "", client_id)
                return 0
            if payload.flags & ipc.FLAG_ALLOCATE_INDEX_ID:
                new_id = self.engine.registry.allocate_index_id()
                self.engine._advance_sequence(
                    sys.SEQ_ID_INDICES, new_id - 1, new_id)
                self._send_response(
                    fd, new_id, None, STATUS_OK, "", client_id)
                return 0

        if client_id > 0:
            self.fd_to_client[fd] = client_id
            self.client_to_fd[client_id] = fd

        # Index seek
        if payload.flags & ipc.FLAG_SEEK_BY_INDEX:
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
                fd, target_id, result, STATUS_OK, "",
                client_id, schema=resp_schema)
            if result is not None:
                result.free()
            return 0

        # PK seek
        if payload.flags & ipc.FLAG_SEEK:
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
                fd, target_id, result, STATUS_OK, "",
                client_id, schema=resp_schema)
            if result is not None:
                result.free()
            return 0

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
            p_fds.append(fd)
            p_cids.append(client_id)
            p_tids.append(target_id)
            p_batches.append(cloned)
            p_schemas.append(family.schema)
            return cloned.length()

        # Non-bufferable: process inline
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
            fd, target_id, result, STATUS_OK, "",
            client_id, schema=resp_schema, seek_pk_lo=lsn)
        if result is not None:
            result.free()
        return 0

    def _send_response(self, fd, target_id, result, status, error_msg,
                       client_id, schema=None, flags=0,
                       seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0):
        """Send a response via the active I/O backend (uring or poll)."""
        if self._use_uring:
            self._queue_async_send(
                self._ring, fd, target_id, result, status, error_msg,
                client_id, schema=schema, flags=flags,
                seek_pk_lo=seek_pk_lo, seek_pk_hi=seek_pk_hi,
                seek_col_idx=seek_col_idx)
        else:
            ipc.send_framed(
                fd, target_id, result, status=status, error_msg=error_msg,
                client_id=client_id, schema=schema, flags=flags,
                seek_pk_lo=seek_pk_lo, seek_pk_hi=seek_pk_hi,
                seek_col_idx=seek_col_idx)

    def _send_error_response(self, fd, error_msg, target_id=0, client_id=0):
        """Send an error response via the active I/O backend."""
        if self._use_uring:
            self._queue_async_send(
                self._ring, fd, target_id, None,
                STATUS_ERROR, error_msg, client_id)
        else:
            ipc.send_framed_error(
                fd, error_msg, target_id=target_id, client_id=client_id)

    def _queue_async_send(self, ring, fd, target_id, result, status,
                          error_msg, client_id, schema=None, flags=0,
                          seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0):
        eff_schema = schema
        if eff_schema is None and result is not None:
            eff_schema = result._schema
        eff_flags = r_uint64(flags)
        wire_buf = ipc._encode_wire(
            target_id, client_id, result, eff_schema, eff_flags,
            seek_pk_lo, seek_pk_hi, seek_col_idx, status, error_msg)
        payload_len = wire_buf.offset
        total = 4 + payload_len
        send_buf = lltype.malloc(rffi.CCHARP.TO, total, flavor='raw')
        send_buf[0] = chr(payload_len & 0xFF)
        send_buf[1] = chr((payload_len >> 8) & 0xFF)
        send_buf[2] = chr((payload_len >> 16) & 0xFF)
        send_buf[3] = chr((payload_len >> 24) & 0xFF)
        c_memmove(
            rffi.cast(rffi.VOIDP, rffi.ptradd(send_buf, 4)),
            rffi.cast(rffi.VOIDP, wire_buf.base_ptr),
            rffi.cast(rffi.SIZE_T, payload_len))
        wire_buf.free()

        self._send_bufs[fd] = send_buf
        self._send_lens[fd] = total
        self._send_pos[fd] = 0
        self._conn_state[fd] = _CONN_SENDING

        uring_ffi.uring_prep_send(
            ring, fd, send_buf, total, _make_udata(_TAG_SEND, fd))

    def _handle_send_cqe(self, ring, fd, res):
        self._send_pos[fd] = self._send_pos[fd] + res
        if self._send_pos[fd] < self._send_lens[fd]:
            pos = self._send_pos[fd]
            send_buf = self._send_bufs[fd]
            remaining = self._send_lens[fd] - pos
            uring_ffi.uring_prep_send(
                ring, fd, rffi.ptradd(send_buf, pos), remaining,
                _make_udata(_TAG_SEND, fd))
            return
        lltype.free(self._send_bufs[fd], flavor='raw')
        del self._send_bufs[fd]
        del self._send_lens[fd]
        del self._send_pos[fd]

        self._conn_state[fd] = _CONN_WAITING_HEADER
        self._conn_hdr_pos[fd] = 0
        hdr_buf = self._conn_hdr_buf[fd]
        uring_ffi.uring_prep_recv(
            ring, fd, hdr_buf, 4, _make_udata(_TAG_RECV, fd))

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

    # -- Socket Layer (delegates to handle_push) ---------------------------

    def _drain_client(self, fd, p_fds, p_cids, p_tids, p_batches, p_schemas):
        """Receive one IPC message. Buffer multi-worker user-table pushes;
        process everything else inline. Returns rows buffered, or -1 (EAGAIN)."""
        payload = None
        rows_buffered = 0
        try:
            payload = ipc.try_recv_framed(
                fd, self._client_hdr_bufs[fd], self._client_hdr_pos[fd])
            if payload is None:
                return -1  # EAGAIN
            rows_buffered = self._dispatch_payload(
                fd, payload, p_fds, p_cids, p_tids, p_batches, p_schemas)
        except errors.ClientDisconnectedError:
            self._cleanup_client(fd)
            return -1
        except errors.GnitzError as ge:
            err_msg = ge.msg
            tid = intmask(payload.target_id) if payload else 0
            cid = intmask(payload.client_id) if payload else 0
            try:
                self._send_error_response(fd, err_msg, target_id=tid,
                                          client_id=cid)
            except errors.GnitzError:
                self._cleanup_client(fd)
                return -1
        except Exception as e:
            os.write(2, "MASTER EXCEPTION: " + str(e) + "\n")
            self._cleanup_client(fd)
            return -1
        finally:
            if payload is not None:
                payload.close()
        return rows_buffered

    def _flush_pending_pushes(self, p_fds, p_cids, p_tids, p_batches,
                              p_schemas):
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
                    if self._t_first_tick == 0.0:
                        self._t_first_tick = time.time()
                self._tick_rows[target_id] = self._tick_rows[target_id] + n_rows
            else:
                current_lsn = 0

            for k in range(run_start, run_end):
                try:
                    if len(err_msg) > 0:
                        self._send_error_response(
                            p_fds[k], err_msg,
                            target_id=target_id, client_id=p_cids[k])
                    else:
                        self._send_response(
                            p_fds[k], target_id, None, STATUS_OK, "",
                            p_cids[k], schema=schema,
                            seek_pk_lo=current_lsn)
                except Exception:
                    self._cleanup_client(p_fds[k])

            run_start = run_end

    def _check_and_fire_pending_ticks(self):
        """Fire pending ticks if coalesce threshold or deadline reached."""
        if len(self._tick_tids) == 0:
            return
        for tid in self._tick_tids:
            if self._tick_rows[tid] >= TICK_COALESCE_ROWS:
                self._fire_pending_ticks()
                return
        if self._t_first_tick > 0.0:
            elapsed_ms = int((time.time() - self._t_first_tick) * 1000.0)
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
        self._t_first_tick = 0.0

    def _evaluate_dag(self, initial_source_id, initial_delta):
        evaluate_dag(self.engine, initial_source_id, initial_delta)

    def _cleanup_client(self, fd):
        # io_uring state (no-op in poll path)
        if fd in self._conn_hdr_buf:
            lltype.free(self._conn_hdr_buf[fd], flavor='raw')
            del self._conn_hdr_buf[fd]
        if fd in self._conn_state:
            del self._conn_state[fd]
        if fd in self._conn_hdr_pos:
            del self._conn_hdr_pos[fd]
        if fd in self._conn_payload_buf:
            lltype.free(self._conn_payload_buf[fd], flavor='raw')
            del self._conn_payload_buf[fd]
        if fd in self._conn_payload_len:
            del self._conn_payload_len[fd]
        if fd in self._conn_payload_pos:
            del self._conn_payload_pos[fd]
        if fd in self._send_bufs:
            lltype.free(self._send_bufs[fd], flavor='raw')
            del self._send_bufs[fd]
        if fd in self._send_lens:
            del self._send_lens[fd]
        if fd in self._send_pos:
            del self._send_pos[fd]

        # Poll state (no-op in uring path)
        if fd in self._client_hdr_bufs:
            lltype.free(self._client_hdr_bufs[fd], flavor='raw')
            del self._client_hdr_bufs[fd]
        if fd in self._client_hdr_pos:
            lltype.free(self._client_hdr_pos[fd], flavor='raw')
            del self._client_hdr_pos[fd]

        # Common: close fd, remove from registries
        if fd in self.client_fds:
            try:
                os.close(intmask(fd))
            except OSError:
                pass
            del self.client_fds[fd]

        if fd in self.fd_to_client:
            client_id = self.fd_to_client[fd]
            if client_id in self.client_to_fd:
                del self.client_to_fd[client_id]
            del self.fd_to_client[fd]

        for i in range(len(self.active_fds)):
            if self.active_fds[i] == fd:
                last = len(self.active_fds) - 1
                self.active_fds[i] = self.active_fds[last]
                del self.active_fds[last]
                break
        self._poll_dirty = True
