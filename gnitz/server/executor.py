# gnitz/server/executor.py

import os
from rpython.rlib import rsocket, jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz.server import ipc, ipc_ffi
from gnitz.core import errors
from gnitz.core.batch import ZSetBatch
from gnitz.vm import runtime, interpreter
from gnitz.catalog.system_tables import SYS_TABLE_VIEW_DEPS, SYS_TABLE_SUBSCRIPTIONS

# Assuming program_cache is implemented as part of Phase 4
from gnitz.catalog.program_cache import ProgramCache

STATUS_OK = 0
STATUS_ERROR = 1


class _CascadeItem(object):
    """Container for the DAG evaluation queue."""
    def __init__(self, target_id, batch, depth, owns_batch):
        self.target_id = target_id
        self.batch = batch
        self.depth = depth
        self.owns_batch = owns_batch


class ServerExecutor(object):
    """
    Coordinates the lifecycle of a Zero-Copy IPC session and the Reactive DAG.
    Routes incoming batches to Base Tables, and cascades changes through Views.
    """

    _immutable_fields_ = ["engine", "program_cache"]

    def __init__(self, engine):
        self.engine = engine
        self.program_cache = engine.program_cache

        # Connection Registries
        self.active_fds = newlist_hint(16)
        self.client_sockets = {}    # int(fd) -> RSocket
        self.fd_to_client = {}      # int(fd) -> int(client_id)
        self.client_to_fd = {}      # int(client_id) -> int(fd)

    def run_socket_server(self, socket_path):
        """
        Listens on a Unix Domain Socket using an event-driven poll loop.
        """
        if os.path.exists(socket_path):
            try:
                os.unlink(socket_path)
            except OSError:
                pass

        server_sock = rsocket.RSocket(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)
        server_sock.bind(rsocket.UnixAddress(socket_path))
        server_sock.listen(1024)
        server_sock.setblocking(False)

        server_fd = server_sock.fd
        self.active_fds.append(server_fd)

        # Keep reference to prevent GC from closing the server socket
        self.client_sockets[server_fd] = server_sock

        while True:
            jit.promote(self.engine)

            # Prepare fast C-arrays for the multiplexer
            count = len(self.active_fds)
            fds = newlist_hint(count)
            events = newlist_hint(count)

            for i in range(count):
                fds.append(self.active_fds[i])
                events.append(ipc_ffi.POLLIN | ipc_ffi.POLLERR | ipc_ffi.POLLHUP)

            # Poll with a 500ms timeout to allow safe thread interruption
            revents = ipc_ffi.poll(fds, events, 500)

            for i in range(len(fds)):
                fd = fds[i]
                rev = revents[i]

                if rev == 0:
                    continue

                # 1. Handle Disconnects and Errors
                if rev & (ipc_ffi.POLLERR | ipc_ffi.POLLHUP | ipc_ffi.POLLNVAL):
                    if fd != server_fd:
                        self._cleanup_client(fd)
                    continue

                # 2. Handle Reads
                if rev & ipc_ffi.POLLIN:
                    if fd == server_fd:
                        # Accept all pending connections
                        try:
                            while True:
                                client_sock, _ = server_sock.accept()
                                client_sock.setblocking(False)
                                c_fd = client_sock.fd
                                self.active_fds.append(c_fd)
                                self.client_sockets[c_fd] = client_sock
                        except rsocket.SocketError:
                            # EWOULDBLOCK reached
                            pass
                    else:
                        self._handle_client_data(fd)

    def _handle_client_data(self, fd):
        """
        Processes a single incoming IPC batch.
        """
        payload = None
        try:
            payload = ipc.receive_payload(fd, self.engine.registry)

            client_id = intmask(payload.client_id)
            target_id = intmask(payload.target_id)

            # Authenticate / Register Client ID
            if client_id > 0:
                self.fd_to_client[fd] = client_id
                self.client_to_fd[client_id] = fd

            if payload.batch is not None and payload.batch.length() > 0:
                family = self.engine.registry.get_by_id(target_id)

                # Ingest performs: FK validation -> WAL write -> Index projection.
                family.ingest_batch(payload.batch)
                family.flush()

                # ACK Success
                ipc.send_batch(fd, target_id, None, STATUS_OK, "", client_id)

                # Trigger the Reactive Dataflow Cascade
                self._evaluate_dag(target_id, payload.batch)
            else:
                # Empty batch acts as a Ping/ACK or Subscription command
                ipc.send_batch(fd, target_id, None, STATUS_OK, "", client_id)

        except errors.GnitzError as ge:
            err_msg = ge.msg if hasattr(ge, 'msg') else str(ge)
            tid = intmask(payload.target_id) if payload else 0
            cid = intmask(payload.client_id) if payload else 0
            ipc.send_error(fd, err_msg, target_id=tid, client_id=cid)
        except Exception:
            # Unrecoverable transport error (e.g., connection reset by peer)
            self._cleanup_client(fd)
        finally:
            if payload is not None:
                payload.close()

    def _cleanup_client(self, fd):
        """
        Gracefully tears down a socket and generates an algebraic retraction
        for the disconnected client in the `_subscriptions` Z-Set.
        """
        client_id = 0
        if fd in self.fd_to_client:
            client_id = self.fd_to_client[fd]
            del self.fd_to_client[fd]
            if client_id in self.client_to_fd:
                del self.client_to_fd[client_id]

        # Convert disconnect into Z-Set Algebra
        if client_id > 0 and self.engine.registry.has_id(SYS_TABLE_SUBSCRIPTIONS):
            sys_subs = self.engine.registry.get_by_id(SYS_TABLE_SUBSCRIPTIONS)
            cursor = sys_subs.create_cursor()
            retract_batch = ZSetBatch(sys_subs.schema)

            try:
                while cursor.is_valid():
                    acc = cursor.get_accessor()
                    # sub_id is PK. col 0 = view_id, col 1 = client_id
                    c_id = intmask(acc.get_int(1))
                    weight = cursor.weight()
                    if c_id == client_id and weight > r_int64(0):
                        # Construct exact negation
                        retract_batch.append_from_accessor(
                            cursor.key(), r_int64(-intmask(weight)), acc
                        )
                    cursor.advance()

                if retract_batch.length() > 0:
                    sys_subs.ingest_batch(retract_batch)
                    sys_subs.flush()
                    # A disconnect could cascade if views count active subscribers!
                    self._evaluate_dag(SYS_TABLE_SUBSCRIPTIONS, retract_batch)
            finally:
                cursor.close()
                retract_batch.free()

        # Physical Socket Teardown
        if fd in self.active_fds:
            self.active_fds.remove(fd)
        if fd in self.client_sockets:
            sock = self.client_sockets[fd]
            del self.client_sockets[fd]
            sock.close()

    def _evaluate_dag(self, initial_target_id, initial_delta):
        """
        The Topological Evaluator. Iteratively computes dependent views.

        This is the heart of the Reactive Dataflow system. When a base table
        is updated, this function triggers a cascade through the dependency graph,
        evaluating views and broadcasting results to subscribers.
        """
        # initial_delta is typically provided by the caller (the ingestion handler)
        # and is cleaned up there. owns_batch=False prevents double-frees.
        queue = newlist_hint(8)
        queue.append(_CascadeItem(initial_target_id, initial_delta, 0, False))

        while len(queue) > 0:
            item = queue.pop(0)
            curr_id = item.target_id
            delta = item.batch
            depth = item.depth

            # Safety breaker for cycles or overly complex DAGs
            if depth > 64:
                if item.owns_batch:
                    delta.free()
                continue

            # Identify downstream views depending on the changed entity
            dependent_views = self._get_dependent_views(curr_id)
            for i in range(len(dependent_views)):
                view_id = dependent_views[i]

                # Fetch the pre-compiled ExecutablePlan
                plan = self.program_cache.get_program(view_id)
                if plan is None:
                    continue

                # REUSE the register file stored in the plan.
                # It contains the stateful cursors (Scan/Join) bound to the catalog.
                reg_file = plan.reg_file

                # --- 1. Bind Input (Register 0) ---
                reg_in = reg_file.registers[0]
                if reg_in is not None:
                    reg_in.batch = delta

                # --- 2. Clear all non-input delta registers for a clean tick ---
                # Register 0 is the input and was just aliased above; skipping it
                # avoids wiping the incoming delta and avoids the None-batch
                # hazard left by the alias cleanup at the end of the previous tick.
                # All other DeltaRegisters are guaranteed to have a live batch
                # (created in DeltaRegister.__init__ and never set to None).
                for reg_idx in range(1, len(reg_file.registers)):
                    reg = reg_file.registers[reg_idx]
                    if reg is not None and reg.is_delta():
                        reg.batch.clear()

                # --- 3. Virtual Machine Execution ---
                # We create a transient Context but reuse the stored Program array.
                context = runtime.ExecutionContext(reg_file)
                interpreter_obj = interpreter.DBSPInterpreter(plan.program)
                interpreter_obj.resume(context)

                # --- 4. Process Cascade and Broadcast ---
                reg_out = reg_file.registers[1]
                if reg_out is not None:
                    b = reg_out.batch
                    if b is not None and b.length() > 0:
                        # CLONE is mandatory:
                        # 1. Physical: reg_out.batch.clear() will be called next tick.
                        # 2. Logic: Downstream views need an immutable snapshot.
                        out_delta_cloned = b.clone()

                        # O(1) Broadcast: Send the FD to all subscribers
                        self._broadcast_delta(view_id, out_delta_cloned)

                        # Recursive Step: Push the new delta into the queue
                        # owns_batch=True ensures this clone is freed after use.
                        queue.append(_CascadeItem(view_id, out_delta_cloned, depth + 1, True))

                # --- 5. Break Input Alias ---
                # Sever the reference to the external delta so that if the batch
                # is freed by its owner (owns_batch=True on the queue item),
                # register 0 does not hold a dangling pointer into freed arenas.
                # The next tick's step 1 will rebind it before any reads occur.
                if reg_in is not None:
                    reg_in.batch = None

            # If the current delta was a clone (from an upstream view),
            # its lifecycle in this cascade is complete.
            if item.owns_batch:
                delta.free()

    def _get_dependent_views(self, target_id):
        """
        Queries `_system._view_deps` to find downstream dependents.
        """
        if not self.engine.registry.has_id(SYS_TABLE_VIEW_DEPS):
            return []

        deps_table = self.engine.registry.get_by_id(SYS_TABLE_VIEW_DEPS)
        cursor = deps_table.create_cursor()
        res = newlist_hint(4)

        try:
            while cursor.is_valid():
                acc = cursor.get_accessor()
                v_id = intmask(acc.get_int(0))
                d_view_id = intmask(acc.get_int(1))
                d_table_id = intmask(acc.get_int(2))

                if (d_view_id == target_id or d_table_id == target_id) and cursor.weight() > r_int64(0):
                    if v_id not in res:
                        res.append(v_id)
                cursor.advance()
        finally:
            cursor.close()

        return res

    def _broadcast_delta(self, view_id, out_delta):
        """
        O(1) Fan-out: Serializes the update once and pushes the FD to all subscribers.
        """
        if not self.engine.registry.has_id(SYS_TABLE_SUBSCRIPTIONS):
            return

        subs_table = self.engine.registry.get_by_id(SYS_TABLE_SUBSCRIPTIONS)
        cursor = subs_table.create_cursor()
        target_fds = newlist_hint(8)

        try:
            while cursor.is_valid():
                acc = cursor.get_accessor()
                v_id = intmask(acc.get_int(0))

                if v_id == view_id and cursor.weight() > r_int64(0):
                    c_id = intmask(acc.get_int(1))
                    if c_id in self.client_to_fd:
                        target_fds.append(self.client_to_fd[c_id])
                cursor.advance()
        finally:
            cursor.close()

        if len(target_fds) > 0:
            memfd = -1
            try:
                # Serialize ONCE
                memfd = ipc.serialize_to_memfd(view_id, zbatch=out_delta)
                for fd in target_fds:
                    res = ipc_ffi.send_fd(fd, memfd)
                    if res < 0:
                        # Schedule dead sockets for cleanup in the next poll tick
                        self._cleanup_client(fd)
            finally:
                if memfd >= 0:
                    os.close(memfd)


def run_standalone_node(socket_path, engine):
    """
    Entry point to start a local database node.
    """
    executor = ServerExecutor(engine)
    executor.run_socket_server(socket_path)
