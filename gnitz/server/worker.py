# gnitz/server/worker.py
#
# Worker process: owns a subset of partitions for every user table,
# receives push/scan requests from the master via a socketpair.

import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz import log
from gnitz.server import ipc, ipc_ffi
from gnitz.core import errors
from gnitz.storage import mmap_posix
from gnitz.core.batch import ArenaZSetBatch
from gnitz.catalog import system_tables as sys
from gnitz.catalog.registry import ingest_to_family
from gnitz.core.types import TYPE_U128
from gnitz.server.executor import evaluate_dag


class WorkerExchangeHandler(object):
    """Sends pre-exchange output to master, receives repartitioned input."""

    def __init__(self, master_fd):
        self.master_fd = master_fd
        self._stash = {}   # view_id (int) -> ArenaZSetBatch pre-sent by master

    def stash_preloaded(self, view_id, batch):
        """Store a pre-sent exchange batch so do_exchange can return it directly."""
        if view_id in self._stash:
            self._stash[view_id].free()
        self._stash[view_id] = batch

    def do_exchange(self, view_id, batch, source_id=0):
        # Co-partitioned / trivial pre-plan: master already sent this batch.
        if view_id in self._stash:
            result = self._stash[view_id]
            del self._stash[view_id]
            return result
        schema = batch._schema
        ipc.send_batch(
            self.master_fd, view_id, batch,
            schema=schema, flags=ipc.FLAG_EXCHANGE,
            seek_pk_lo=source_id,
        )
        payload = ipc.receive_payload(self.master_fd)
        if payload.batch is not None and payload.batch.length() > 0:
            result = payload.batch.clone()
        else:
            result = ArenaZSetBatch(schema)
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
        self.pending_deltas = {}   # table_id (int) -> ArenaZSetBatch

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
            flags = intmask(payload.flags)

            # Drain all preloaded exchange frames sent before the actual push.
            while flags & ipc.FLAG_PRELOADED_EXCHANGE:
                vid = intmask(payload.target_id)
                if payload.batch is not None and payload.batch.length() > 0:
                    stashed = payload.batch.clone()
                else:
                    stashed = ArenaZSetBatch(payload.schema)
                self.exchange_handler.stash_preloaded(vid, stashed)
                payload.close()
                payload = None
                payload = ipc.receive_payload(self.master_fd)
                flags = intmask(payload.flags)

            target_id = intmask(payload.target_id)

            if flags & ipc.FLAG_SHUTDOWN:
                self._shutdown()
                return True

            if flags & ipc.FLAG_DDL_SYNC:
                self._handle_ddl_sync(target_id, payload.batch)
                return False

            if flags & ipc.FLAG_BACKFILL:
                source_tid = target_id
                view_id = intmask(payload.seek_pk_lo)
                self._handle_backfill(source_tid, view_id)
                ipc.send_batch(self.master_fd, source_tid, None)
                return False

            if flags & ipc.FLAG_HAS_PK:
                self._handle_has_pk(target_id, payload.batch, intmask(payload.seek_col_idx))
                return False

            if flags & ipc.FLAG_PUSH:
                batch = payload.batch
                if batch is not None and batch.length() > 0:
                    self._handle_push(target_id, batch)
                ipc.send_batch(self.master_fd, target_id, None)
            elif flags & ipc.FLAG_TICK:
                self._handle_tick(target_id)
                ipc.send_batch(self.master_fd, target_id, None)
            elif flags & ipc.FLAG_SEEK_BY_INDEX:
                result = self._handle_seek_by_index(
                    target_id, intmask(payload.seek_col_idx),
                    intmask(payload.seek_pk_lo), intmask(payload.seek_pk_hi),
                )
                schema = self.engine.registry.get_by_id(target_id).schema
                ipc.send_batch(self.master_fd, target_id, result, schema=schema)
                if result is not None:
                    result.free()
            elif flags & ipc.FLAG_SEEK:
                result = self._handle_seek(
                    target_id,
                    intmask(payload.seek_pk_lo), intmask(payload.seek_pk_hi),
                )
                schema = self.engine.registry.get_by_id(target_id).schema
                ipc.send_batch(self.master_fd, target_id, result, schema=schema)
                if result is not None:
                    result.free()
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
        except mmap_posix.MMapError:
            ipc.send_error(self.master_fd, "I/O write error")
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
        """Ingest batch into the local partition store and flush.
        System tables (DDL) evaluate immediately; user tables accumulate until FLAG_TICK."""
        family = self.engine.registry.get_by_id(target_id)
        # FK validation was done by the master before fan_out_ingest.
        # ingest_to_family is partition-local ZSet algebra — correct for workers.
        effective = ingest_to_family(family, batch)
        family.store.flush()
        if target_id < sys.FIRST_USER_TABLE_ID:
            # System tables must fire DDL hooks (TableEffectHook) immediately so
            # that newly created tables are registered before user pushes arrive.
            evaluate_dag(self.engine, target_id, effective,
                         exchange_handler=self.exchange_handler)
            if effective is not batch:
                effective.free()
        else:
            if target_id in self.pending_deltas:
                self.pending_deltas[target_id].append_batch(effective)
                if effective is not batch:
                    effective.free()
            else:
                if effective is not batch:
                    self.pending_deltas[target_id] = effective   # take ownership
                else:
                    self.pending_deltas[target_id] = effective.clone()  # must clone
        log.debug(
            "W" + str(self.worker_id)
            + " push tid=" + str(target_id)
            + " rows=" + str(batch.length())
        )

    def _handle_tick(self, target_id):
        """Pop accumulated delta and run evaluate_dag."""
        if target_id in self.pending_deltas:
            delta = self.pending_deltas[target_id]
            del self.pending_deltas[target_id]
        else:
            if not self.engine.registry.has_id(target_id):
                # Table was dropped before tick arrived; nothing to evaluate.
                return
            family = self.engine.registry.get_by_id(target_id)
            delta = ArenaZSetBatch(family.schema)
        evaluate_dag(self.engine, target_id, delta,
                     exchange_handler=self.exchange_handler)
        delta.free()

    def _handle_has_pk(self, target_id, batch, col_hint):
        """Check PK existence for FK / unique-index validation. Responds with weights 1/0.

        col_hint == 0: FK check against the main table store.
        col_hint > 0:  unique index check on column (col_hint - 1).
        """
        family = self.engine.registry.get_by_id(target_id)
        if col_hint > 0:
            col_idx = col_hint - 1
            store = None
            schema = None
            for ic in range(len(family.index_circuits)):
                circuit = family.index_circuits[ic]
                if circuit.source_col_idx == col_idx and circuit.is_unique:
                    store = circuit.table
                    schema = circuit.table.get_schema()
                    break
            if store is None:
                raise errors.LayoutError(
                    "No unique index on column %d for table %d"
                    % (col_idx, target_id)
                )
        else:
            store = family.store
            schema = family.schema

        result = ArenaZSetBatch(schema)
        n = batch.length() if batch is not None else 0
        for i in range(n):
            exists = store.has_pk(batch.get_pk_lo(i), batch.get_pk_hi(i))
            w = r_int64(1) if exists else r_int64(0)
            result._direct_append_row(batch, i, w)
        ipc.send_batch(self.master_fd, target_id, result, schema=schema)
        result.free()

    def _handle_scan(self, target_id):
        """Scan all local partitions for target, return result batch."""
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
        log.debug(
            "W" + str(self.worker_id)
            + " scan tid=" + str(target_id)
            + " rows=" + str(result.length())
        )
        return result

    def _handle_seek(self, target_id, pk_lo_raw, pk_hi_raw):
        """Point lookup by primary key on this worker's local partitions."""
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

    def _handle_seek_by_index(self, target_id, col_idx, key_lo, key_hi):
        """Index-assisted lookup on this worker's local index circuits."""
        family = self.engine.registry.get_by_id(target_id)
        circuit = None
        for c in family.index_circuits:
            if c.source_col_idx == col_idx:
                circuit = c
                break
        if circuit is None:
            return None

        idx_cursor = circuit.table.create_cursor()
        try:
            idx_cursor.seek(r_uint64(key_lo), r_uint64(key_hi))
            if not idx_cursor.is_valid() or idx_cursor.key_lo() != r_uint64(key_lo) or idx_cursor.key_hi() != r_uint64(key_hi):
                return None
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
        return self._handle_seek(target_id, src_lo, src_hi)

    def _handle_backfill(self, source_tid, view_id):
        """Scan local partition of source, evaluate DAG (with exchange) for the view."""
        if not self.engine.registry.has_id(source_tid):
            return
        family = self.engine.registry.get_by_id(source_tid)
        schema = family.schema
        local_batch = ArenaZSetBatch(schema)
        cursor = family.store.create_cursor()
        try:
            while cursor.is_valid():
                w = cursor.weight()
                if w > r_int64(0):
                    acc = cursor.get_accessor()
                    local_batch.append_from_accessor(cursor.key_lo(), cursor.key_hi(), w, acc)
                cursor.advance()
        finally:
            cursor.close()
        evaluate_dag(self.engine, source_tid, local_batch,
                     exchange_handler=self.exchange_handler)
        local_batch.free()

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
