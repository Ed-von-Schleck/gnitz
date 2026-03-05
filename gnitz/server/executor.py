# gnitz/server/executor.py

import os
from rpython.rlib import rsocket, jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz.server import ipc, ipc_ffi
from gnitz.core import errors
from gnitz.core.batch import ZSetBatch
from gnitz.catalog import system_tables as sys
from gnitz.catalog.registry import ingest_to_family
from gnitz.dbsp import ops


def _sort_pending(pending):
    """Insertion sort by depth (index 0). RPython-safe."""
    for i in range(1, len(pending)):
        item = pending[i]
        j = i - 1
        while j >= 0 and pending[j][0] > item[0]:
            pending[j + 1] = pending[j]
            j -= 1
        pending[j + 1] = item

STATUS_OK = 0
STATUS_ERROR = 1


class ServerExecutor(object):
    """
    Coordinates IPC sessions and the Reactive DAG.
    Uses topological ranking to ensure glitch-free incremental evaluation.
    """

    _immutable_fields_ = ["engine", "program_cache"]

    def __init__(self, engine):
        self.engine = engine
        self.program_cache = engine.program_cache

        # Connection Registries
        self.active_fds = newlist_hint(16)
        self.client_sockets = {}  # int(fd) -> RSocket
        self.fd_to_client = {}  # int(fd) -> int(client_id)
        self.client_to_fd = {}  # int(client_id) -> int(fd)

    def run_socket_server(self, socket_path):
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
        self.client_sockets[server_fd] = server_sock

        while True:
            jit.promote(self.engine)
            count = len(self.active_fds)
            fds = newlist_hint(count)
            events = newlist_hint(count)

            for i in range(count):
                fds.append(self.active_fds[i])
                events.append(ipc_ffi.POLLIN | ipc_ffi.POLLERR | ipc_ffi.POLLHUP)

            revents = ipc_ffi.poll(fds, events, 500)

            for i in range(len(fds)):
                fd = fds[i]
                rev = revents[i]
                if rev == 0:
                    continue

                if rev & (ipc_ffi.POLLERR | ipc_ffi.POLLHUP | ipc_ffi.POLLNVAL):
                    if fd != server_fd:
                        self._cleanup_client(fd)
                    continue

                if rev & ipc_ffi.POLLIN:
                    if fd == server_fd:
                        try:
                            while True:
                                client_sock, _ = server_sock.accept()
                                client_sock.setblocking(False)
                                c_fd = client_sock.fd
                                self.active_fds.append(c_fd)
                                self.client_sockets[c_fd] = client_sock
                        except rsocket.SocketError:
                            pass
                    else:
                        self._handle_client_data(fd)

    def _handle_client_data(self, fd):
        payload = None
        try:
            payload = ipc.receive_payload(fd, self.engine.registry)
            client_id = intmask(payload.client_id)
            target_id = intmask(payload.target_id)

            if client_id > 0:
                self.fd_to_client[fd] = client_id
                self.client_to_fd[client_id] = fd

            if payload.batch is not None and payload.batch.length() > 0:
                family = self.engine.registry.get_by_id(target_id)

                # Use the catalog ingestion pipeline for FK and Index enforcement
                ingest_to_family(family, payload.batch)
                family.store.flush()

                ipc.send_batch(fd, target_id, None, STATUS_OK, "", client_id)

                # Trigger the Reactive Cascade
                self._evaluate_dag(target_id, payload.batch)
            else:
                ipc.send_batch(fd, target_id, None, STATUS_OK, "", client_id)

        except errors.GnitzError as ge:
            err_msg = ge.msg if hasattr(ge, "msg") else str(ge)
            tid = intmask(payload.target_id) if payload else 0
            cid = intmask(payload.client_id) if payload else 0
            ipc.send_error(fd, err_msg, target_id=tid, client_id=cid)
        except Exception:
            self._cleanup_client(fd)
        finally:
            if payload is not None:
                payload.close()

    def _evaluate_dag(self, initial_source_id, initial_delta):
        """
        Topological Evaluator.
        Uses a depth-sorted approach to ensure each view evaluates exactly once
        per epoch, even in diamond dependency graphs.
        """
        # Build dep_map once: source_id -> [view_id, ...]
        dep_map = {}
        if self.engine.registry.has_id(sys.DepTab.ID):
            deps_family = self.engine.registry.get_by_id(sys.DepTab.ID)
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
            d = self.engine.registry.get_depth(v_id)
            pending.append((d, v_id, initial_delta.clone()))
        _sort_pending(pending)

        while len(pending) > 0:
            depth, target_view_id, incoming_delta = pending[0]
            del pending[0]

            plan = self.program_cache.get_program(target_view_id)
            if plan is None:
                incoming_delta.free()
                continue

            out_delta = plan.execute_epoch(incoming_delta)
            incoming_delta.free()

            if out_delta is not None and out_delta.length() > 0:
                self._broadcast_delta(target_view_id, out_delta)

                dependents = dep_map.get(target_view_id, [])
                for dep_id in dependents:
                    found = -1
                    for pi in range(len(pending)):
                        if pending[pi][1] == dep_id:
                            found = pi
                            break
                    if found >= 0:
                        existing_d, existing_id, existing_batch = pending[found]
                        merged = ZSetBatch(existing_batch._schema)
                        ops.op_union(existing_batch, out_delta, merged)
                        existing_batch.free()
                        pending[found] = (existing_d, existing_id, merged)
                    else:
                        d = self.engine.registry.get_depth(dep_id)
                        pending.append((d, dep_id, out_delta.clone()))
                        _sort_pending(pending)

                out_delta.free()

    def _broadcast_delta(self, view_id, out_delta):
        pass

    def _cleanup_client(self, fd):
        if fd in self.client_sockets:
            sock = self.client_sockets[fd]
            try:
                sock.close()
            except rsocket.SocketError:
                pass
            del self.client_sockets[fd]
        
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
