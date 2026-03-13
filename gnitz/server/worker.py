# gnitz/server/worker.py
#
# Worker process: owns a subset of partitions for every user table,
# receives push/scan requests from the master via a socketpair.

import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz import log
from gnitz.server import ipc, ipc_ffi
from gnitz.core import errors
from gnitz.core.batch import ZSetBatch
from gnitz.catalog import system_tables as sys
from gnitz.catalog.registry import ingest_to_family, _enforce_unique_pk
from gnitz.server.executor import evaluate_dag


class WorkerExchangeHandler(object):
    """Sends pre-exchange output to master, receives repartitioned input."""

    def __init__(self, master_fd):
        self.master_fd = master_fd

    def do_exchange(self, view_id, batch, shard_cols):
        schema = batch._schema
        ipc.send_batch(
            self.master_fd, view_id, batch,
            schema=schema, flags=ipc.FLAG_EXCHANGE,
        )
        payload = ipc.receive_payload(self.master_fd)
        if payload.batch is not None and payload.batch.length() > 0:
            result = payload.batch.clone()
        else:
            result = ZSetBatch(schema)
        payload.close()
        return result


class WorkerProcess(object):
    """
    A child process that owns partitions [part_start, part_end) of every
    user table. Communicates with the master over a single socketpair fd.
    """

    _immutable_fields_ = ["worker_id", "master_fd", "engine", "part_start", "part_end"]

    def __init__(self, worker_id, master_fd, engine, part_start, part_end):
        self.worker_id = worker_id
        self.master_fd = master_fd
        self.engine = engine
        self.part_start = part_start
        self.part_end = part_end
        self.exchange_handler = WorkerExchangeHandler(master_fd)

    def run(self):
        """Main poll loop. Blocks until master disconnects or sends shutdown."""
        fds = [self.master_fd]
        events = [ipc_ffi.POLLIN | ipc_ffi.POLLHUP]

        while True:
            revents = ipc_ffi.poll(fds, events, 1000)
            if len(revents) == 0:
                continue
            rev = revents[0]
            if rev & (ipc_ffi.POLLHUP | ipc_ffi.POLLERR | ipc_ffi.POLLNVAL):
                self._shutdown()
                return
            if rev & ipc_ffi.POLLIN:
                should_exit = self._handle_request()
                if should_exit:
                    return

    def _handle_request(self):
        """Receive one request, dispatch, respond. Returns True on shutdown."""
        payload = None
        try:
            payload = ipc.receive_payload(self.master_fd)
            target_id = intmask(payload.target_id)
            flags = intmask(payload.flags)

            if flags & ipc.FLAG_SHUTDOWN:
                self._shutdown()
                return True

            if flags & ipc.FLAG_DDL_SYNC:
                self._handle_ddl_sync(target_id, payload.batch)
                ipc.send_batch(self.master_fd, target_id, None)
                return False

            if flags & ipc.FLAG_HAS_PK:
                self._handle_has_pk(target_id, payload.batch)
                return False

            if flags & ipc.FLAG_PUSH:
                batch = payload.batch
                if batch is not None and batch.length() > 0:
                    self._handle_push(target_id, batch)
                else:
                    # Empty push — still need to participate in exchange barriers
                    family = self.engine.registry.get_by_id(target_id)
                    empty = ZSetBatch(family.schema)
                    evaluate_dag(self.engine, target_id, empty,
                                 exchange_handler=self.exchange_handler)
                    empty.free()
                ipc.send_batch(self.master_fd, target_id, None)
            else:
                result = self._handle_scan(target_id)
                schema = self.engine.registry.get_by_id(target_id).schema
                ipc.send_batch(
                    self.master_fd, target_id, result, schema=schema
                )
                if result is not None:
                    result.free()
            return False

        except errors.GnitzError as ge:
            ipc.send_error(self.master_fd, ge.msg)
            return False
        except OSError as oe:
            os.write(2,
                "W" + str(self.worker_id) + " OSError errno=" + str(oe.errno) + "\n"
            )
            self._shutdown()
            return True
        except Exception as e:
            os.write(2,
                "W" + str(self.worker_id) + " unhandled: " + str(e) + "\n"
            )
            self._shutdown()
            return True
        finally:
            if payload is not None:
                payload.close()

    def _handle_push(self, target_id, batch):
        """Ingest batch into the local partition store, flush, and evaluate DAG."""
        family = self.engine.registry.get_by_id(target_id)
        if family.unique_pk:
            effective = _enforce_unique_pk(family, batch)
            family.store.ingest_batch(effective)
            family.store.flush()
            evaluate_dag(self.engine, target_id, effective,
                         exchange_handler=self.exchange_handler)
            effective.free()
        else:
            family.store.ingest_batch(batch)
            family.store.flush()
            evaluate_dag(self.engine, target_id, batch,
                         exchange_handler=self.exchange_handler)
        log.debug(
            "W" + str(self.worker_id)
            + " push tid=" + str(target_id)
            + " rows=" + str(batch.length())
        )

    def _handle_has_pk(self, target_id, batch):
        """Check PK existence for FK validation. Responds with weights 1/0."""
        family = self.engine.registry.get_by_id(target_id)
        schema = family.schema
        result = ZSetBatch(schema)
        n = batch.length() if batch is not None else 0
        for i in range(n):
            pk = batch.get_pk(i)
            exists = family.store.has_pk(pk)
            w = r_int64(1) if exists else r_int64(0)
            result._direct_append_row(batch, i, w)
        ipc.send_batch(self.master_fd, target_id, result, schema=schema)
        result.free()

    def _handle_scan(self, target_id):
        """Scan all local partitions for target, return result batch."""
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
        log.debug(
            "W" + str(self.worker_id)
            + " scan tid=" + str(target_id)
            + " rows=" + str(result.length())
        )
        return result

    def _handle_ddl_sync(self, target_id, batch):
        """Replay a system-table delta to keep the local registry in sync.

        Workers do NOT write to system table WALs (master owns them).
        We update the in-memory memtable directly and fire the hooks
        so that new user-table families get created in the registry.
        The registry's active_part_start/end ensure hooks only create
        partitions owned by this worker.
        """
        if batch is None or batch.length() == 0:
            return

        family = self.engine.registry.get_by_id(target_id)
        family.store.ingest_batch_memonly(batch)

        for h_idx in range(len(family.post_ingest_hooks)):
            family.post_ingest_hooks[h_idx].on_delta(batch)

        log.debug(
            "W" + str(self.worker_id)
            + " ddl_sync tid=" + str(target_id)
            + " rows=" + str(batch.length())
        )

    def _shutdown(self):
        """Flush all families and exit."""
        for family in self.engine.registry.iter_families():
            if family.table_id >= sys.FIRST_USER_TABLE_ID:
                family.store.flush()
        os._exit(0)
