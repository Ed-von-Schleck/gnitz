# gnitz/server/worker.py
#
# Worker process: owns a subset of partitions for every user table.
# Receives requests from the master via the shared append-only log (SAL),
# sends responses via a per-worker W2M shared region.

import os
from rpython.rlib.rarithmetic import r_int64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz import log
from gnitz.server import ipc
from gnitz.storage import engine_ffi
from gnitz.core import errors
from gnitz.storage import mmap_posix
from gnitz.storage.owned_batch import ArenaZSetBatch
from gnitz.catalog import system_tables as sys
from gnitz.server.executor import evaluate_dag, _catalog_get_schema, _catalog_has_id


class WorkerExchangeHandler(object):
    """Sends pre-exchange output to master via W2M, receives relay via SAL."""

    def __init__(self, worker_process):
        self.wp = worker_process
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

        # Non-trivial: send via W2M, block waiting for relay via SAL.
        schema = batch._schema
        ipc.write_to_w2m(
            self.wp.w2m_region, view_id, batch, schema,
            ipc.FLAG_EXCHANGE, source_id, 0, 0, 0, "",
        )
        engine_ffi.eventfd_signal(self.wp.w2m_efd)

        while True:
            engine_ffi.eventfd_wait(self.wp.m2w_efd, 30000)
            size = intmask(ipc.atomic_load_u64(
                rffi.ptradd(self.wp.sal_ptr, self.wp.read_cursor)))
            if size == 0:
                continue
            # Epoch fence: skip stale data from previous epoch
            hdr_off = self.wp.read_cursor + 8
            epoch = intmask(ipc._read_u32_raw(
                self.wp.sal_ptr, hdr_off + 28))
            if epoch != self.wp._expected_epoch:
                continue
            msg = ipc.read_worker_message(
                self.wp.sal_ptr, self.wp.read_cursor, self.wp.worker_id)
            self.wp.read_cursor += msg.advance
            if msg.payload is not None and msg.payload.batch is not None:
                return msg.payload.batch.clone()
            return ArenaZSetBatch(schema)


class WorkerProcess(object):
    """
    A child process that owns partitions [part_start, part_end) of every
    user table. Reads requests from the SAL (master→worker), writes
    responses to a W2M region (worker→master).
    """

    _immutable_fields_ = ["worker_id", "master_pid", "engine",
                          "part_start", "part_end",
                          "sal_ptr", "m2w_efd", "w2m_region", "w2m_efd"]

    def __init__(self, worker_id, master_pid, engine, part_start, part_end,
                 sal_ptr, m2w_efd, w2m_region, w2m_efd):
        self.worker_id = worker_id
        self.master_pid = master_pid
        self.engine = engine
        self.part_start = part_start
        self.part_end = part_end
        self.sal_ptr = sal_ptr         # inherited mmap (read from master)
        self.m2w_efd = m2w_efd         # eventfd: master signals this worker
        self.w2m_region = w2m_region   # W2MRegion: this worker writes responses
        self.w2m_efd = w2m_efd         # eventfd: this worker signals master
        self.exchange_handler = WorkerExchangeHandler(self)
        self.pending_deltas = {}       # table_id (int) -> ArenaZSetBatch
        self.read_cursor = 0           # SAL read position (worker-local)
        self._expected_epoch = 1       # epoch for checkpoint fencing

    def run(self):
        """Main event loop. Waits for SAL signals or master crash."""
        # Signal master that recovery is complete and we are ready
        self._send_ack(0)
        while True:
            ready = engine_ffi.eventfd_wait(self.m2w_efd, 1000)
            if ready == 0:
                # Timeout — check for master crash via getppid
                if os.getppid() != self.master_pid:
                    self._shutdown()
                    return
                continue
            if ready < 0:
                continue

            should_exit = self._drain_sal()
            if should_exit:
                return

    def _drain_sal(self):
        """Process all pending SAL message groups. Returns True on shutdown."""
        while True:
            if self.read_cursor + 8 >= ipc.SAL_MMAP_SIZE:
                break
            size = intmask(ipc.atomic_load_u64(
                rffi.ptradd(self.sal_ptr, self.read_cursor)))
            if size == 0:
                break
            # Epoch fence: skip stale groups from previous epoch
            hdr_off = self.read_cursor + 8
            epoch = intmask(ipc._read_u32_raw(self.sal_ptr, hdr_off + 28))
            if epoch != self._expected_epoch:
                break
            msg = ipc.read_worker_message(
                self.sal_ptr, self.read_cursor, self.worker_id)
            self.read_cursor += msg.advance
            should_exit = self._dispatch_message(msg)
            if should_exit:
                return True
        return False

    def _dispatch_message(self, msg):
        """Dispatch a single SAL message. Returns True on shutdown."""
        flags = msg.flags
        target_id = msg.target_id
        payload = msg.payload  # IPCPayload or None

        # Skip messages not targeted at this worker (unicast to another worker)
        if payload is None and msg.advance > 0:
            return False

        try:
            # Drain preloaded exchange groups before processing push
            while flags & ipc.FLAG_PRELOADED_EXCHANGE:
                vid = target_id
                if payload is not None and payload.batch is not None and payload.batch.length() > 0:
                    stashed = payload.batch.clone()
                elif payload is not None and payload.schema is not None:
                    stashed = ArenaZSetBatch(payload.schema)
                else:
                    stashed = None
                if stashed is not None:
                    self.exchange_handler.stash_preloaded(vid, stashed)
                # Read next SAL group
                if self.read_cursor + 8 >= ipc.SAL_MMAP_SIZE:
                    return False
                size = intmask(ipc.read_u64_raw(
                    self.sal_ptr, self.read_cursor))
                if size == 0:
                    return False
                msg = ipc.read_worker_message(
                    self.sal_ptr, self.read_cursor, self.worker_id)
                self.read_cursor += msg.advance
                flags = msg.flags
                target_id = msg.target_id
                payload = msg.payload

            if flags & ipc.FLAG_SHUTDOWN:
                self._shutdown()
                return True

            if flags & ipc.FLAG_FLUSH:
                self._handle_flush_all()
                # Reset BEFORE sending ACK (ordering invariant)
                self.read_cursor = 0
                self._expected_epoch += 1
                self._send_ack(0, flags=ipc.FLAG_CHECKPOINT)
                return False

            if flags & ipc.FLAG_DDL_SYNC:
                batch = payload.batch if payload is not None else None
                self._handle_ddl_sync(target_id, batch)
                return False

            if flags & ipc.FLAG_BACKFILL:
                source_tid = target_id
                view_id = intmask(payload.seek_pk_lo) if payload is not None else 0
                self._handle_backfill(source_tid, view_id)
                self._send_ack(source_tid)
                return False

            if flags & ipc.FLAG_HAS_PK:
                batch = payload.batch if payload is not None else None
                col_hint = intmask(payload.seek_col_idx) if payload is not None else 0
                self._handle_has_pk(target_id, batch, col_hint)
                return False

            if flags & ipc.FLAG_PUSH:
                batch = payload.batch if payload is not None else None
                if batch is not None and batch.length() > 0:
                    self._handle_push(target_id, batch)
                self._send_ack(target_id)
                return False

            if flags & ipc.FLAG_TICK:
                self._handle_tick(target_id)
                self._send_ack(target_id)
                return False

            if flags & ipc.FLAG_SEEK_BY_INDEX:
                result = self._handle_seek_by_index(
                    target_id,
                    intmask(payload.seek_col_idx) if payload is not None else 0,
                    intmask(payload.seek_pk_lo) if payload is not None else 0,
                    intmask(payload.seek_pk_hi) if payload is not None else 0,
                )
                schema = _catalog_get_schema(self.engine, target_id)
                self._send_response(target_id, result, schema)
                if result is not None:
                    result.free()
                return False

            if flags & ipc.FLAG_SEEK:
                result = self._handle_seek(
                    target_id,
                    intmask(payload.seek_pk_lo) if payload is not None else 0,
                    intmask(payload.seek_pk_hi) if payload is not None else 0,
                )
                schema = _catalog_get_schema(self.engine, target_id)
                self._send_response(target_id, result, schema)
                if result is not None:
                    result.free()
                return False

            # Default: scan
            result = self._handle_scan(target_id)
            schema = _catalog_get_schema(self.engine, target_id)
            self._send_response(target_id, result, schema)
            if result is not None:
                result.free()
            return False

        except errors.GnitzError as ge:
            self._send_error(ge.msg)
            return False
        except mmap_posix.MMapError:
            self._send_error("I/O write error")
            return False
        except OSError as oe:
            os.write(2,
                "W" + str(self.worker_id) + " OSError errno="
                + str(oe.errno) + " strerror="
                + os.strerror(oe.errno) + "\n")
            self._shutdown()
            return True
        except Exception as e:
            os.write(2,
                "W" + str(self.worker_id) + " unhandled: " + str(e) + "\n")
            self._shutdown()
            return True

    # -----------------------------------------------------------------------
    # W2M response helpers
    # -----------------------------------------------------------------------

    def _send_ack(self, target_id, flags=0):
        ipc.write_to_w2m(self.w2m_region, target_id, None, None, flags,
                         0, 0, 0, 0, "")
        engine_ffi.eventfd_signal(self.w2m_efd)

    def _send_response(self, target_id, result, schema):
        ipc.write_to_w2m(self.w2m_region, target_id, result, schema, 0,
                         0, 0, 0, 0, "")
        engine_ffi.eventfd_signal(self.w2m_efd)

    def _send_error(self, error_msg):
        ipc.write_to_w2m(self.w2m_region, 0, None, None, 0,
                         0, 0, 0, ipc.STATUS_ERROR, error_msg)
        engine_ffi.eventfd_signal(self.w2m_efd)

    # -----------------------------------------------------------------------
    # Request handlers (unchanged from socket transport)
    # -----------------------------------------------------------------------

    def _handle_push(self, target_id, batch):
        """Ingest batch into the local partition store and flush.
        System tables (DDL) evaluate immediately; user tables accumulate until FLAG_TICK."""
        cat_h = self.engine._handle
        tid_ll = rffi.cast(rffi.LONGLONG, target_id)
        if target_id < sys.FIRST_USER_TABLE_ID:
            # System table: ingest via catalog FFI (takes ownership of clone)
            batch_clone = batch.clone()
            rc = engine_ffi._catalog_ingest(cat_h, tid_ll, batch_clone._handle)
            batch_clone._handle = lltype.nullptr(rffi.VOIDP.TO)
            batch_clone._freed = True
            if intmask(rc) < 0:
                raise errors.StorageError("catalog_ingest failed for tid=%d" % target_id)
            engine_ffi._catalog_flush(cat_h, tid_ll)
            evaluate_dag(self.engine, target_id, batch,
                         exchange_handler=self.exchange_handler)
        else:
            # User table: ingest returning effective delta (takes ownership of clone)
            batch_clone = batch.clone()
            eff_handle = engine_ffi._catalog_ingest_effective(
                cat_h, tid_ll, batch_clone._handle)
            batch_clone._handle = lltype.nullptr(rffi.VOIDP.TO)
            batch_clone._freed = True
            if not eff_handle:
                raise errors.StorageError(
                    "catalog_ingest_effective failed for tid=%d" % target_id)
            engine_ffi._catalog_flush(cat_h, tid_ll)
            schema = _catalog_get_schema(self.engine, target_id)
            is_sorted = intmask(engine_ffi._batch_is_sorted(eff_handle)) != 0
            is_cons = intmask(engine_ffi._batch_is_consolidated(eff_handle)) != 0
            effective = ArenaZSetBatch._wrap_handle(schema, eff_handle, is_sorted, is_cons)
            if target_id in self.pending_deltas:
                self.pending_deltas[target_id].append_batch(effective)
                effective.free()
            else:
                self.pending_deltas[target_id] = effective
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
            if not _catalog_has_id(self.engine, target_id):
                return
            schema = _catalog_get_schema(self.engine, target_id)
            delta = ArenaZSetBatch(schema)
        evaluate_dag(self.engine, target_id, delta,
                     exchange_handler=self.exchange_handler)
        delta.free()

    def _handle_has_pk(self, target_id, batch, col_hint):
        """Check PK existence for FK / unique-index validation."""
        cat_h = self.engine._handle
        tid_ll = rffi.cast(rffi.LONGLONG, target_id)
        if col_hint > 0:
            col_idx = col_hint - 1
            index_handle = engine_ffi._catalog_get_index_store(
                cat_h, tid_ll, rffi.cast(rffi.UINT, col_idx))
            if not index_handle:
                raise errors.LayoutError(
                    "No unique index on column %d for table %d"
                    % (col_idx, target_id))
            # Use the check batch's schema (idx_schema) for the response —
            # master only reads weights; using source_schema here would cause a
            # schema-mismatch panic in _direct_append_row (idx_schema has
            # fewer payload columns than source_schema).
            schema = batch._schema if batch is not None else _catalog_get_schema(self.engine, target_id)
            result = ArenaZSetBatch(schema)
            n = batch.length() if batch is not None else 0
            for i in range(n):
                exists = intmask(engine_ffi._table_has_pk(
                    index_handle,
                    rffi.cast(rffi.ULONGLONG, batch.get_pk_lo(i)),
                    rffi.cast(rffi.ULONGLONG, batch.get_pk_hi(i)))) != 0
                w = r_int64(1) if exists else r_int64(0)
                result._direct_append_row(batch, i, w)
        else:
            ptable_handle = engine_ffi._catalog_get_ptable_handle(cat_h, tid_ll)
            schema = _catalog_get_schema(self.engine, target_id)
            result = ArenaZSetBatch(schema)
            n = batch.length() if batch is not None else 0
            for i in range(n):
                if ptable_handle:
                    exists = intmask(engine_ffi._ptable_has_pk(
                        ptable_handle,
                        rffi.cast(rffi.ULONGLONG, batch.get_pk_lo(i)),
                        rffi.cast(rffi.ULONGLONG, batch.get_pk_hi(i)))) != 0
                else:
                    exists = False
                w = r_int64(1) if exists else r_int64(0)
                result._direct_append_row(batch, i, w)
        self._send_response(target_id, result, schema)
        result.free()

    def _handle_scan(self, target_id):
        """Scan all local partitions for target, return result batch."""
        schema = _catalog_get_schema(self.engine, target_id)
        result_handle = engine_ffi._catalog_scan(
            self.engine._handle,
            rffi.cast(rffi.LONGLONG, target_id))
        if not result_handle:
            return ArenaZSetBatch(schema)
        return ArenaZSetBatch._wrap_handle(schema, result_handle, True, True)

    def _handle_seek(self, target_id, pk_lo_raw, pk_hi_raw):
        """Point lookup by primary key on this worker's local partitions."""
        result_handle = engine_ffi._catalog_seek(
            self.engine._handle,
            rffi.cast(rffi.LONGLONG, target_id),
            rffi.cast(rffi.ULONGLONG, pk_lo_raw),
            rffi.cast(rffi.ULONGLONG, pk_hi_raw))
        if not result_handle:
            return None
        schema = _catalog_get_schema(self.engine, target_id)
        return ArenaZSetBatch._wrap_handle(schema, result_handle, True, True)

    def _handle_seek_by_index(self, target_id, col_idx, key_lo, key_hi):
        """Index-assisted lookup on this worker's local index circuits."""
        result_handle = engine_ffi._catalog_seek_by_index(
            self.engine._handle,
            rffi.cast(rffi.LONGLONG, target_id),
            rffi.cast(rffi.UINT, col_idx),
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi))
        if not result_handle:
            return None
        schema = _catalog_get_schema(self.engine, target_id)
        return ArenaZSetBatch._wrap_handle(schema, result_handle, True, True)

    def _handle_backfill(self, source_tid, view_id):
        """Scan local partition of source, evaluate DAG for the view."""
        if not _catalog_has_id(self.engine, source_tid):
            return
        schema = _catalog_get_schema(self.engine, source_tid)
        result_handle = engine_ffi._catalog_scan(
            self.engine._handle,
            rffi.cast(rffi.LONGLONG, source_tid))
        if not result_handle:
            local_batch = ArenaZSetBatch(schema)
        else:
            local_batch = ArenaZSetBatch._wrap_handle(
                schema, result_handle, True, True)
        evaluate_dag(self.engine, source_tid, local_batch,
                     exchange_handler=self.exchange_handler)
        local_batch.free()

    def _handle_ddl_sync(self, target_id, batch):
        """Replay a system-table delta to keep the local catalog in sync."""
        if batch is None or batch.length() == 0:
            return
        n_rows = batch.length()
        batch_clone = batch.clone()
        rc = engine_ffi._catalog_ddl_sync(
            self.engine._handle,
            rffi.cast(rffi.LONGLONG, target_id),
            batch_clone._handle)
        batch_clone._handle = lltype.nullptr(rffi.VOIDP.TO)
        batch_clone._freed = True
        if intmask(rc) < 0:
            raise errors.StorageError(
                "catalog_ddl_sync failed for tid=%d" % target_id)
        log.debug(
            "W" + str(self.worker_id)
            + " ddl_sync tid=" + str(target_id)
            + " rows=" + str(n_rows)
        )

    def _handle_flush_all(self):
        """Flush all user-table families to ensure memtable data is durable."""
        MAX_TABLES = 256
        id_buf = lltype.malloc(rffi.LONGLONGP.TO, MAX_TABLES, flavor="raw")
        try:
            n = intmask(engine_ffi._catalog_iter_user_table_ids(
                self.engine._handle, id_buf, rffi.cast(rffi.UINT, MAX_TABLES)))
            for i in range(n):
                tid = intmask(id_buf[i])
                engine_ffi._catalog_flush(
                    self.engine._handle,
                    rffi.cast(rffi.LONGLONG, tid))
        finally:
            lltype.free(id_buf, flavor="raw")

    def _shutdown(self):
        """Flush all families and exit."""
        self._handle_flush_all()
        os._exit(0)
