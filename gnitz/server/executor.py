# gnitz/server/executor.py

import os
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz.server import ipc, ipc_ffi
from gnitz.core import errors
from gnitz.core.batch import ZSetBatch, BatchWriter
from gnitz.catalog import system_tables as sys
from gnitz.catalog.registry import ingest_to_family, validate_fk_distributed
from gnitz.dbsp import ops
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128


def _sort_pending(pending):
    """Insertion sort by depth (index 0). RPython-safe."""
    for i in range(1, len(pending)):
        item = pending[i]
        j = i - 1
        while j >= 0 and pending[j][0] > item[0]:
            pending[j + 1] = pending[j]
            j -= 1
        pending[j + 1] = item

MAX_PENDING_ROWS = 100000

STATUS_OK = 0
STATUS_ERROR = 1


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


def evaluate_dag(engine, initial_source_id, initial_delta,
                 exchange_handler=None):
    """
    Module-level topological DAG evaluator.
    Uses a depth-sorted approach to ensure each view evaluates exactly once
    per epoch, even in diamond dependency graphs.

    Called by both the single-process ServerExecutor and multi-worker
    WorkerProcess paths. When exchange_handler is provided, plans with
    exchange_post_plan will perform IPC exchange mid-circuit.
    """
    registry = engine.registry
    program_cache = engine.program_cache

    # Build dep_map once: source_id -> [view_id, ...]
    dep_map = {}
    if registry.has_id(sys.DepTab.ID):
        deps_family = registry.get_by_id(sys.DepTab.ID)
        cursor = deps_family.store.create_cursor()
        try:
            while cursor.is_valid():
                if cursor.weight() > r_int64(0):
                    acc = cursor.get_accessor()
                    v_id    = intmask(r_uint64(acc.get_int(sys.DepTab.COL_VIEW_ID)))
                    dep_tid = intmask(r_uint64(acc.get_int(sys.DepTab.COL_DEP_TABLE_ID)))
                    dep_vid = intmask(r_uint64(acc.get_int(sys.DepTab.COL_DEP_VIEW_ID)))
                    if dep_tid > 0:
                        if dep_tid not in dep_map:
                            dep_map[dep_tid] = []
                        if v_id not in dep_map[dep_tid]:
                            dep_map[dep_tid].append(v_id)
                    if dep_vid > 0:
                        if dep_vid not in dep_map:
                            dep_map[dep_vid] = []
                        if v_id not in dep_map[dep_vid]:
                            dep_map[dep_vid].append(v_id)
                cursor.advance()
        finally:
            cursor.close()

    # pending: list of (depth, view_id, batch), sorted ascending by depth
    first_layer = dep_map.get(initial_source_id, [])
    pending = []
    for v_id in first_layer:
        d = registry.get_depth(v_id)
        pending.append((d, v_id, initial_delta.clone()))
    _sort_pending(pending)

    while len(pending) > 0:
        depth, target_view_id, incoming_delta = pending[0]
        del pending[0]

        plan = program_cache.get_program(target_view_id)
        if plan is None:
            incoming_delta.free()
            continue

        if plan.exchange_post_plan is not None and exchange_handler is not None:
            # Multi-worker: pre-plan -> IPC exchange -> post-plan
            pre_result = plan.execute_epoch(incoming_delta)
            incoming_delta.free()
            if pre_result is None:
                pre_result = ZSetBatch(plan.out_schema)

            exchanged = exchange_handler.do_exchange(
                target_view_id, pre_result, plan.exchange_shard_cols
            )
            pre_result.free()

            out_delta = plan.exchange_post_plan.execute_epoch(exchanged)
            exchanged.free()
        elif plan.exchange_post_plan is not None:
            # Single-process: pre-plan -> post-plan (no exchange needed)
            pre_result = plan.execute_epoch(incoming_delta)
            incoming_delta.free()
            if pre_result is None:
                pre_result = ZSetBatch(plan.out_schema)
            out_delta = plan.exchange_post_plan.execute_epoch(pre_result)
            pre_result.free()
        else:
            out_delta = plan.execute_epoch(incoming_delta)
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
                found = -1
                for pi in range(len(pending)):
                    if pending[pi][1] == dep_id:
                        found = pi
                        break
                if found >= 0:
                    if has_output:
                        existing_d, existing_id, existing_batch = pending[found]
                        merged = ZSetBatch(existing_batch._schema)
                        writer = BatchWriter(merged)
                        ops.op_union(existing_batch, out_delta, writer)
                        existing_batch.free()
                        pending[found] = (existing_d, existing_id, merged)
                else:
                    d = registry.get_depth(dep_id)
                    if has_output:
                        pending.append((d, dep_id, out_delta.clone()))
                    else:
                        dep_family = registry.get_by_id(dep_id)
                        pending.append((d, dep_id, ZSetBatch(dep_family.schema)))
                    _sort_pending(pending)

        if out_delta is not None and out_delta.length() > 0:
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

    def run_socket_server(self, socket_path):
        server_fd = ipc_ffi.server_create(socket_path)
        if server_fd < 0:
            raise errors.StorageError("Failed to create server socket")
        self.active_fds.append(server_fd)

        while True:
            jit.promote(self.engine)

            # Check for worker crashes (non-blocking)
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

            count = len(self.active_fds)
            fds = newlist_hint(count)
            events = newlist_hint(count)

            for i in range(count):
                fds.append(self.active_fds[i])
                events.append(ipc_ffi.POLLIN | ipc_ffi.POLLERR | ipc_ffi.POLLHUP)

            revents = ipc_ffi.poll(fds, events, 500)

            # Phase 1: Drain all ready FDs
            pending_fds = newlist_hint(16)
            pending_cids = newlist_hint(16)
            pending_tids = newlist_hint(16)
            pending_batches = newlist_hint(16)
            pending_schemas = newlist_hint(16)
            pending_row_count = 0

            # Pass 1: cleanup disconnected clients before accepting new ones.
            # Without this, if a client disconnects and a new client connects
            # in the same poll() call, server_fd is processed first (new fd
            # accepted), then the old POLLHUP fires _cleanup_client on the
            # same fd number, killing the newly accepted connection.
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
                    continue  # Already handled in pass 1
                if rev & ipc_ffi.POLLIN:
                    if fd == server_fd:
                        while True:
                            c_fd = ipc_ffi.server_accept(server_fd)
                            if c_fd < 0:
                                break
                            self.active_fds.append(c_fd)
                            self.client_fds[c_fd] = 1
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

            # Phase 2: Flush remaining buffered pushes
            if len(pending_fds) > 0:
                self._flush_pending_pushes(
                    pending_fds, pending_cids,
                    pending_tids, pending_batches,
                    pending_schemas,
                )

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
                self.dispatcher.fan_out_push(target_id, in_batch, schema)
                # No _evaluate_dag here — workers handle it
                return None
            else:
                return self.dispatcher.fan_out_scan(target_id, schema)

        if in_batch is not None and in_batch.length() > 0:
            family = self.engine.registry.get_by_id(target_id)
            effective = ingest_to_family(family, in_batch)
            family.store.flush()
            self._evaluate_dag(target_id, effective)
            if effective is not in_batch:
                effective.free()
            return None
        else:
            return self._scan_family(target_id)

    def _scan_family(self, target_id):
        """Build a batch containing every positive-weight row in the target."""
        family = self.engine.registry.get_by_id(target_id)
        schema = family.schema
        result = ZSetBatch(schema)
        cursor = family.store.create_cursor()
        try:
            while cursor.is_valid():
                w = cursor.weight()
                if w > r_int64(0):
                    pk = cursor.key()
                    acc = cursor.get_accessor()
                    result.append_from_accessor(r_uint128(pk), w, acc)
                cursor.advance()
        finally:
            cursor.close()
        return result

    # -- Socket Layer (delegates to handle_push) ---------------------------

    def _drain_client(self, fd, p_fds, p_cids, p_tids, p_batches, p_schemas):
        """Receive one IPC message. Buffer multi-worker user-table pushes;
        process everything else inline. Returns rows buffered, or -1 (EAGAIN)."""
        payload = None
        rows_buffered = 0
        try:
            payload = ipc.try_receive_payload(fd)
            if payload is None:
                return -1  # EAGAIN — no message ready on this FD
            client_id = intmask(payload.client_id)
            target_id = intmask(payload.target_id)

            # ID allocation — immediate response
            if target_id == 0:
                if payload.flags & ipc.FLAG_ALLOCATE_TABLE_ID:
                    new_id = self.engine.registry.allocate_table_id()
                    self.engine._advance_sequence(
                        sys.SEQ_ID_TABLES, new_id - 1, new_id
                    )
                    ipc.send_batch(fd, new_id, None, STATUS_OK, "", client_id)
                    return 0
                if payload.flags & ipc.FLAG_ALLOCATE_SCHEMA_ID:
                    new_id = self.engine.registry.allocate_schema_id()
                    self.engine._advance_sequence(
                        sys.SEQ_ID_SCHEMAS, new_id - 1, new_id
                    )
                    ipc.send_batch(fd, new_id, None, STATUS_OK, "", client_id)
                    return 0

            if client_id > 0:
                self.fd_to_client[fd] = client_id
                self.client_to_fd[client_id] = fd

            # Schema validation (if pushing data)
            if payload.batch is not None and payload.schema is not None:
                family = self.engine.registry.get_by_id(target_id)
                _validate_schema_match(payload.schema, family.schema)

            # Bufferable: multi-worker user-table DML with data
            if (
                self.dispatcher is not None
                and target_id >= sys.FIRST_USER_TABLE_ID
                and payload.batch is not None
                and payload.batch.length() > 0
            ):
                family = self.engine.registry.get_by_id(target_id)
                validate_fk_distributed(
                    family, payload.batch, self.dispatcher
                )
                cloned = payload.batch.clone()
                p_fds.append(fd)
                p_cids.append(client_id)
                p_tids.append(target_id)
                p_batches.append(cloned)
                p_schemas.append(family.schema)
                rows_buffered = cloned.length()
                return rows_buffered

            # Non-bufferable: process inline
            result = self.handle_push(target_id, payload.batch)

            # Broadcast system-table deltas to workers for DDL sync
            if (
                self.dispatcher is not None
                and target_id < sys.FIRST_USER_TABLE_ID
                and payload.batch is not None
                and payload.batch.length() > 0
            ):
                family = self.engine.registry.get_by_id(target_id)
                self.dispatcher.broadcast_ddl(
                    target_id, payload.batch, family.schema
                )

            # Response always includes schema
            resp_schema = None
            if result is not None:
                resp_schema = result._schema
            else:
                family = self.engine.registry.get_by_id(target_id)
                resp_schema = family.schema
            ipc.send_batch(
                fd, target_id, result, STATUS_OK, "",
                client_id, schema=resp_schema,
            )
            if result is not None:
                result.free()

        except errors.ClientDisconnectedError:
            # Peer closed before or during message read — normal, no response needed.
            self._cleanup_client(fd)
            return -1
        except errors.GnitzError as ge:
            err_msg = ge.msg
            tid = intmask(payload.target_id) if payload else 0
            cid = intmask(payload.client_id) if payload else 0
            try:
                ipc.send_error(fd, err_msg, target_id=tid, client_id=cid)
            except errors.GnitzError:
                # Socket disconnected while sending error — clean up and stop
                # draining this fd.
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
                self.dispatcher.fan_out_push(target_id, merged, schema)
            except errors.GnitzError as ge:
                err_msg = ge.msg

            merged.free()

            for k in range(run_start, run_end):
                try:
                    if len(err_msg) > 0:
                        ipc.send_error(
                            p_fds[k], err_msg,
                            target_id=target_id, client_id=p_cids[k],
                        )
                    else:
                        ipc.send_batch(
                            p_fds[k], target_id, None, STATUS_OK, "",
                            p_cids[k], schema=schema,
                        )
                except Exception:
                    self._cleanup_client(p_fds[k])

            run_start = run_end

    def _evaluate_dag(self, initial_source_id, initial_delta):
        evaluate_dag(self.engine, initial_source_id, initial_delta)

    def _cleanup_client(self, fd):
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

        new_fds = newlist_hint(len(self.active_fds))
        for active_fd in self.active_fds:
            if active_fd != fd:
                new_fds.append(active_fd)
        self.active_fds = new_fds
