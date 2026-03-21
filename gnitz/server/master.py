# gnitz/server/master.py
#
# Master-side dispatcher: fans out push/scan operations to worker processes
# via the shared append-only log (SAL) and collects responses via per-worker
# W2M regions. Eventfds provide cross-process signaling.

import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz import log
from gnitz.server import ipc, ipc_ffi, eventfd_ffi
from gnitz.storage import mmap_posix
from gnitz.core import errors
from gnitz.core.batch import ArenaZSetBatch
from gnitz.dbsp.ops.exchange import (
    repartition_batch, worker_for_pk, relay_scatter, PartitionRouter,
    multi_scatter,
)

WNOHANG = 1


class MasterDispatcher(object):
    """
    Fans out push/scan operations to N workers via a single SAL file.
    Collects ACKs/responses synchronously via per-worker W2M regions.
    """

    _immutable_fields_ = ["num_workers", "worker_fds[*]", "assignment",
                          "sal", "m2w_efds[*]", "w2m_efds[*]"]

    def __init__(self, num_workers, worker_fds, worker_pids, assignment,
                 program_cache, sal, w2m_regions, m2w_efds, w2m_efds):
        self.num_workers = num_workers
        self.worker_fds = worker_fds       # kept for crash detection (POLLHUP)
        self.worker_pids = worker_pids
        self.assignment = assignment
        self.program_cache = program_cache
        self.router = PartitionRouter(assignment)
        self.sal = sal                     # SharedAppendLog
        self.w2m_regions = w2m_regions     # list[W2MRegion]
        self.m2w_efds = m2w_efds           # list[int] — master→worker eventfds
        self.w2m_efds = w2m_efds           # list[int] — worker→master eventfds

    # -----------------------------------------------------------------------
    # Core send/receive helpers
    # -----------------------------------------------------------------------

    def _write_group(self, target_id, flags, worker_batches, schema,
                     seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0):
        """Encode per-worker data and write one SAL message group.

        Does NOT fdatasync or signal. worker_batches[w] is ArenaZSetBatch
        or None. Schema is broadcast to all workers that have data.
        """
        wire_bufs = [None] * self.num_workers
        for w in range(self.num_workers):
            wb = worker_batches[w]
            sch = schema if wb is not None else schema
            wire_bufs[w] = ipc._encode_wire(
                target_id, 0, wb, sch, flags,
                seek_pk_lo, seek_pk_hi, seek_col_idx, 0, "")

        lsn = self.sal.lsn_counter
        self.sal.lsn_counter += 1
        ipc.write_message_group(self.sal, target_id, lsn, flags,
                                wire_bufs, self.num_workers)

        for w in range(self.num_workers):
            if wire_bufs[w] is not None:
                wire_bufs[w].free()

    def _sync_and_signal_all(self):
        """fdatasync the SAL, then signal all workers via eventfd."""
        mmap_posix.fdatasync_c(self.sal.fd)
        for w in range(self.num_workers):
            eventfd_ffi.eventfd_signal(self.m2w_efds[w])

    def _sync_and_signal_one(self, worker):
        """fdatasync the SAL, then signal one worker via eventfd."""
        mmap_posix.fdatasync_c(self.sal.fd)
        eventfd_ffi.eventfd_signal(self.m2w_efds[worker])

    def _send_to_workers(self, target_id, flags, worker_batches, schema,
                         seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0):
        """Write message group + fdatasync + signal all workers."""
        self._write_group(target_id, flags, worker_batches, schema,
                          seek_pk_lo, seek_pk_hi, seek_col_idx)
        self._sync_and_signal_all()

    def _reset_w2m_cursors(self):
        """Reset all workers' W2M write cursors after master has read all responses."""
        for w in range(self.num_workers):
            self.w2m_regions[w].set_write_cursor(ipc.W2M_HEADER_SIZE)

    def _collect_acks(self):
        """Collect one ACK from each worker. Raises on error."""
        remaining = self.num_workers
        collected = [False] * self.num_workers
        w2m_rcs = [ipc.W2M_HEADER_SIZE] * self.num_workers

        while remaining > 0:
            eventfd_ffi.eventfd_wait_any(self.w2m_efds, 1000)
            for w in range(self.num_workers):
                if collected[w]:
                    continue
                wc = self.w2m_regions[w].get_write_cursor()
                if w2m_rcs[w] < wc:
                    payload, w2m_rcs[w] = ipc.read_from_w2m(
                        self.w2m_regions[w], w2m_rcs[w])
                    if payload.status != 0:
                        err = payload.error_msg
                        raise errors.StorageError(
                            "Worker %d error: %s" % (w, err))
                    collected[w] = True
                    remaining -= 1

        self._reset_w2m_cursors()

    def _collect_acks_and_relay(self, schema):
        """Collect ACKs from all workers, relaying exchange messages."""
        remaining = self.num_workers
        collected = [False] * self.num_workers
        w2m_rcs = [ipc.W2M_HEADER_SIZE] * self.num_workers

        exchange_payloads = {}
        exchange_counts = {}
        exchange_schemas = {}
        exchange_source_ids = {}

        while remaining > 0:
            eventfd_ffi.eventfd_wait_any(self.w2m_efds, 1000)
            for w in range(self.num_workers):
                if collected[w]:
                    continue
                wc = self.w2m_regions[w].get_write_cursor()
                while w2m_rcs[w] < wc:
                    payload, w2m_rcs[w] = ipc.read_from_w2m(
                        self.w2m_regions[w], w2m_rcs[w])
                    if payload.flags & ipc.FLAG_EXCHANGE:
                        vid = intmask(payload.target_id)
                        ex_source_id = intmask(payload.seek_pk_lo)
                        if vid not in exchange_payloads:
                            exchange_payloads[vid] = [None] * self.num_workers
                            exchange_counts[vid] = 0
                        exchange_payloads[vid][w] = payload
                        if payload.schema is not None:
                            exchange_schemas[vid] = payload.schema
                        if ex_source_id > 0:
                            exchange_source_ids[vid] = ex_source_id
                        exchange_counts[vid] += 1
                        if exchange_counts[vid] == self.num_workers:
                            ex_schema = exchange_schemas.get(vid, schema)
                            src_id = exchange_source_ids.get(vid, 0)
                            self._relay_exchange(
                                vid, exchange_payloads[vid],
                                ex_schema, src_id)
                            del exchange_payloads[vid]
                            del exchange_counts[vid]
                            if vid in exchange_schemas:
                                del exchange_schemas[vid]
                            if vid in exchange_source_ids:
                                del exchange_source_ids[vid]
                        break  # re-check write_cursor after relay
                    else:
                        if payload.status != 0:
                            err = payload.error_msg
                            raise errors.StorageError(
                                "Worker %d error: %s" % (w, err))
                        collected[w] = True
                        remaining -= 1
                        break

        self._reset_w2m_cursors()

    def _collect_responses(self, schema):
        """Collect data responses from all workers. Returns concatenated batch."""
        result = ArenaZSetBatch(schema)
        remaining = self.num_workers
        collected = [False] * self.num_workers
        w2m_rcs = [ipc.W2M_HEADER_SIZE] * self.num_workers

        while remaining > 0:
            eventfd_ffi.eventfd_wait_any(self.w2m_efds, 1000)
            for w in range(self.num_workers):
                if collected[w]:
                    continue
                wc = self.w2m_regions[w].get_write_cursor()
                if w2m_rcs[w] < wc:
                    payload, w2m_rcs[w] = ipc.read_from_w2m(
                        self.w2m_regions[w], w2m_rcs[w])
                    if payload.status != 0:
                        err = payload.error_msg
                        raise errors.StorageError(
                            "Worker %d error: %s" % (w, err))
                    if (payload.batch is not None
                            and payload.batch.length() > 0):
                        result.append_batch(payload.batch)
                    collected[w] = True
                    remaining -= 1

        self._reset_w2m_cursors()
        return result

    def _collect_one(self, worker):
        """Collect a single response from one worker. Returns IPCPayload."""
        w2m_rc = ipc.W2M_HEADER_SIZE
        while True:
            eventfd_ffi.eventfd_wait(self.w2m_efds[worker], 1000)
            wc = self.w2m_regions[worker].get_write_cursor()
            if w2m_rc < wc:
                payload, w2m_rc = ipc.read_from_w2m(
                    self.w2m_regions[worker], w2m_rc)
                self.w2m_regions[worker].set_write_cursor(
                    ipc.W2M_HEADER_SIZE)
                return payload

    # -----------------------------------------------------------------------
    # Fan-out operations
    # -----------------------------------------------------------------------

    def fan_out_ingest(self, target_id, batch, schema):
        """Split batch by worker partition, send FLAG_PUSH, collect ACKs.
        Workers only ingest — no evaluate_dag, no exchange relay."""
        sub_batches = repartition_batch(
            batch, [schema.pk_index], self.num_workers, self.assignment)
        self._send_to_workers(target_id, ipc.FLAG_PUSH, sub_batches, schema)
        for w in range(self.num_workers):
            sb = sub_batches[w]
            if sb is not None:
                sb.free()
        self._collect_acks()
        log.debug(
            "fan_out_ingest tid=" + str(target_id)
            + " rows=" + str(batch.length()))

    def fan_out_tick(self, target_id, schema):
        """Send FLAG_TICK to all workers; handle exchange relay; collect ACKs."""
        worker_batches = [None] * self.num_workers
        self._send_to_workers(target_id, ipc.FLAG_TICK, worker_batches, schema)
        self._collect_acks_and_relay(schema)
        log.debug("fan_out_tick tid=" + str(target_id))

    def fan_out_push(self, target_id, batch, schema):
        """Split batch by worker partition, send sub-batches, collect ACKs.
        Handles mid-circuit exchange relay when workers send FLAG_EXCHANGE."""
        n = batch.length()
        preloadable = self.program_cache.get_preloadable_views(target_id)
        family = self.program_cache.registry.get_by_id(target_id)

        if len(preloadable) > 0:
            has_retraction = False
            for i in range(n):
                if batch.get_weight(i) < r_int64(0):
                    has_retraction = True
                    break
            if has_retraction:
                preloadable = newlist_hint(0)

        if len(preloadable) == 0:
            sub_batches = repartition_batch(
                batch, [schema.pk_index], self.num_workers, self.assignment)
            for ci in range(len(family.index_circuits)):
                circuit = family.index_circuits[ci]
                if circuit.is_unique:
                    col_idx = circuit.source_col_idx
                    for w in range(self.num_workers):
                        sb = sub_batches[w]
                        if sb is not None and sb.length() > 0:
                            self.router.record_routing(
                                sb, target_id, col_idx, w)
            self._send_to_workers(
                target_id, ipc.FLAG_PUSH, sub_batches, schema)
            for w in range(self.num_workers):
                sb = sub_batches[w]
                if sb is not None:
                    sb.free()
        else:
            col_specs = newlist_hint(len(preloadable) + 1)
            col_specs.append([schema.pk_index])
            for i in range(len(preloadable)):
                col_specs.append(preloadable[i][1])
            all_batches = multi_scatter(
                batch, col_specs, self.num_workers, self.assignment)
            pk_batches = all_batches[0]
            for ci in range(len(family.index_circuits)):
                circuit = family.index_circuits[ci]
                if circuit.is_unique:
                    col_idx = circuit.source_col_idx
                    for w in range(self.num_workers):
                        sb = pk_batches[w]
                        if sb is not None and sb.length() > 0:
                            self.router.record_routing(
                                sb, target_id, col_idx, w)
            # Write preload groups (no sync yet)
            for si in range(len(preloadable)):
                vid = preloadable[si][0]
                preload_batches = all_batches[si + 1]
                self._write_group(vid, ipc.FLAG_PRELOADED_EXCHANGE,
                                  preload_batches, schema)
                for w in range(self.num_workers):
                    pb = preload_batches[w]
                    if pb is not None:
                        pb.free()
            # Write main push group
            self._write_group(
                target_id, ipc.FLAG_PUSH, pk_batches, schema)
            for w in range(self.num_workers):
                sb = pk_batches[w]
                if sb is not None:
                    sb.free()
            # Single fdatasync + signal for all groups
            self._sync_and_signal_all()

        self._collect_acks_and_relay(schema)
        log.debug(
            "fan_out_push tid=" + str(target_id) + " rows=" + str(n))

    def _relay_exchange(self, view_id, payloads, schema, source_id=0):
        """Repartition collected exchange payloads and relay to workers."""
        shard_cols = []
        if source_id > 0:
            shard_cols = self.program_cache.get_join_shard_cols(
                view_id, source_id)
        if len(shard_cols) == 0:
            shard_cols = self.program_cache.get_shard_cols(view_id)
        sources = [None] * self.num_workers
        for i in range(len(payloads)):
            p = payloads[i]
            if p is not None:
                sources[i] = p.batch
        dest_batches = relay_scatter(
            sources, shard_cols, self.num_workers, self.assignment)
        # Close exchange payloads (no-op for W2M-backed)
        for p in payloads:
            if p is not None:
                p.close()
        # Send relay response via SAL
        self._send_to_workers(view_id, 0, dest_batches, schema)
        for w in range(self.num_workers):
            if dest_batches[w] is not None:
                dest_batches[w].free()

    def fan_out_backfill(self, view_id, source_id, source_schema):
        """Broadcast FLAG_BACKFILL; handle exchange relay; collect ACKs."""
        worker_batches = [None] * self.num_workers
        self._send_to_workers(
            source_id, ipc.FLAG_BACKFILL, worker_batches, source_schema,
            seek_pk_lo=view_id)
        self._collect_acks_and_relay(source_schema)

    def fan_out_scan(self, target_id, schema):
        """Send scan to all workers, concatenate result batches."""
        worker_batches = [None] * self.num_workers
        self._send_to_workers(target_id, 0, worker_batches, schema)
        result = self._collect_responses(schema)
        log.debug(
            "fan_out_scan tid=" + str(target_id)
            + " result_rows=" + str(result.length()))
        return result

    def fan_out_seek(self, target_id, pk_lo, pk_hi, schema):
        """Route PK seek to the single worker that owns the partition."""
        worker = worker_for_pk(pk_lo, pk_hi, self.assignment)
        worker_batches = [None] * self.num_workers
        self._write_group(
            target_id, ipc.FLAG_SEEK, worker_batches, schema,
            seek_pk_lo=pk_lo, seek_pk_hi=pk_hi)
        self._sync_and_signal_one(worker)

        payload = self._collect_one(worker)
        if payload.status != 0:
            err = payload.error_msg
            raise errors.StorageError(
                "Worker %d seek error: %s" % (worker, err))
        result = None
        if payload.batch is not None and payload.batch.length() > 0:
            result = ArenaZSetBatch(schema)
            result.append_batch(payload.batch)
        return result

    def fan_out_seek_by_index(self, target_id, col_idx, key_lo, key_hi,
                              schema):
        """Index seek: unicast on cache hit, broadcast on cache miss."""
        cached_worker = self.router.worker_for_index_key(
            target_id, col_idx, key_lo, key_hi)
        if cached_worker >= 0:
            worker_batches = [None] * self.num_workers
            self._write_group(
                target_id, ipc.FLAG_SEEK_BY_INDEX, worker_batches, schema,
                seek_col_idx=col_idx, seek_pk_lo=key_lo, seek_pk_hi=key_hi)
            self._sync_and_signal_one(cached_worker)
            payload = self._collect_one(cached_worker)
            if payload.status != 0:
                err = payload.error_msg
                raise errors.StorageError(
                    "Worker %d seek_by_index error: %s"
                    % (cached_worker, err))
            result = None
            if payload.batch is not None and payload.batch.length() > 0:
                result = ArenaZSetBatch(schema)
                result.append_batch(payload.batch)
            return result

        # Cache miss: broadcast
        worker_batches = [None] * self.num_workers
        self._send_to_workers(
            target_id, ipc.FLAG_SEEK_BY_INDEX, worker_batches, schema,
            seek_col_idx=col_idx, seek_pk_lo=key_lo, seek_pk_hi=key_hi)
        result = None
        remaining = self.num_workers
        collected = [False] * self.num_workers
        w2m_rcs = [ipc.W2M_HEADER_SIZE] * self.num_workers
        while remaining > 0:
            eventfd_ffi.eventfd_wait_any(self.w2m_efds, 1000)
            for w in range(self.num_workers):
                if collected[w]:
                    continue
                wc = self.w2m_regions[w].get_write_cursor()
                if w2m_rcs[w] < wc:
                    payload, w2m_rcs[w] = ipc.read_from_w2m(
                        self.w2m_regions[w], w2m_rcs[w])
                    if payload.status != 0:
                        err = payload.error_msg
                        raise errors.StorageError(
                            "Worker %d seek_by_index error: %s" % (w, err))
                    if (result is None and payload.batch is not None
                            and payload.batch.length() > 0):
                        result = ArenaZSetBatch(schema)
                        result.append_batch(payload.batch)
                    collected[w] = True
                    remaining -= 1
        return result

    def broadcast_ddl(self, target_id, batch, schema):
        """Send a system-table delta to all workers for registry sync."""
        worker_batches = [None] * self.num_workers
        for w in range(self.num_workers):
            worker_batches[w] = batch  # same batch for all (not freed here)
        self._send_to_workers(
            target_id, ipc.FLAG_DDL_SYNC, worker_batches, schema)
        log.debug(
            "broadcast_ddl tid=" + str(target_id)
            + " rows=" + str(batch.length()))

    def check_fk_batch(self, target_id, check_batch, schema):
        """Check PK existence across workers. Returns True if any key missing."""
        sub_batches = repartition_batch(
            check_batch, [schema.pk_index], self.num_workers, self.assignment)
        self._send_to_workers(
            target_id, ipc.FLAG_HAS_PK, sub_batches, schema)
        for w in range(self.num_workers):
            sb = sub_batches[w]
            if sb is not None:
                sb.free()

        any_missing = False
        remaining = self.num_workers
        collected = [False] * self.num_workers
        w2m_rcs = [ipc.W2M_HEADER_SIZE] * self.num_workers
        while remaining > 0:
            eventfd_ffi.eventfd_wait_any(self.w2m_efds, 1000)
            for w in range(self.num_workers):
                if collected[w]:
                    continue
                wc = self.w2m_regions[w].get_write_cursor()
                if w2m_rcs[w] < wc:
                    payload, w2m_rcs[w] = ipc.read_from_w2m(
                        self.w2m_regions[w], w2m_rcs[w])
                    if payload.status != 0:
                        err = payload.error_msg
                        raise errors.StorageError(
                            "Worker %d has_pk error: %s" % (w, err))
                    if payload.batch is not None:
                        for j in range(payload.batch.length()):
                            if payload.batch.get_weight(j) == r_int64(0):
                                any_missing = True
                    collected[w] = True
                    remaining -= 1
        return any_missing

    def check_pk_exists_broadcast(self, owner_table_id, source_col_idx,
                                  check_batch, schema):
        """Broadcast PK existence check. Returns True if any key exists."""
        col_hint = source_col_idx + 1
        worker_batches = [None] * self.num_workers
        for w in range(self.num_workers):
            worker_batches[w] = check_batch
        self._send_to_workers(
            owner_table_id, ipc.FLAG_HAS_PK, worker_batches, schema,
            seek_col_idx=col_hint)

        any_exists = False
        remaining = self.num_workers
        collected = [False] * self.num_workers
        w2m_rcs = [ipc.W2M_HEADER_SIZE] * self.num_workers
        while remaining > 0:
            eventfd_ffi.eventfd_wait_any(self.w2m_efds, 1000)
            for w in range(self.num_workers):
                if collected[w]:
                    continue
                wc = self.w2m_regions[w].get_write_cursor()
                if w2m_rcs[w] < wc:
                    payload, w2m_rcs[w] = ipc.read_from_w2m(
                        self.w2m_regions[w], w2m_rcs[w])
                    if payload.status != 0:
                        err = payload.error_msg
                        raise errors.StorageError(
                            "Worker %d has_pk error: %s" % (w, err))
                    if payload.batch is not None:
                        for j in range(payload.batch.length()):
                            if payload.batch.get_weight(j) == r_int64(1):
                                any_exists = True
                    collected[w] = True
                    remaining -= 1
        return any_exists

    def check_pk_existence(self, target_id, check_batch, schema):
        """Split-route PK check. Returns {lo: {hi: 1}} dict of existing PKs."""
        sub_batches = repartition_batch(
            check_batch, [schema.pk_index], self.num_workers, self.assignment)
        self._send_to_workers(
            target_id, ipc.FLAG_HAS_PK, sub_batches, schema)
        for w in range(self.num_workers):
            sb = sub_batches[w]
            if sb is not None:
                sb.free()

        existing = {}
        remaining = self.num_workers
        collected = [False] * self.num_workers
        w2m_rcs = [ipc.W2M_HEADER_SIZE] * self.num_workers
        while remaining > 0:
            eventfd_ffi.eventfd_wait_any(self.w2m_efds, 1000)
            for w in range(self.num_workers):
                if collected[w]:
                    continue
                wc = self.w2m_regions[w].get_write_cursor()
                if w2m_rcs[w] < wc:
                    payload, w2m_rcs[w] = ipc.read_from_w2m(
                        self.w2m_regions[w], w2m_rcs[w])
                    if payload.status != 0:
                        err = payload.error_msg
                        raise errors.StorageError(
                            "Worker %d has_pk error: %s" % (w, err))
                    if payload.batch is not None:
                        for j in range(payload.batch.length()):
                            if payload.batch.get_weight(j) == r_int64(1):
                                lo = intmask(r_uint64(
                                    payload.batch._read_pk_lo(j)))
                                hi = intmask(r_uint64(
                                    payload.batch._read_pk_hi(j)))
                                if lo not in existing:
                                    existing[lo] = {}
                                existing[lo][hi] = 1
                    collected[w] = True
                    remaining -= 1
        return existing

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    def check_workers(self):
        """Non-blocking check for crashed workers. Returns -1 if all OK."""
        for w in range(self.num_workers):
            pid = self.worker_pids[w]
            if pid <= 0:
                continue
            try:
                rpid, status = os.waitpid(pid, WNOHANG)
            except OSError:
                return w
            if rpid > 0:
                return w
        return -1

    def shutdown_workers(self):
        """Send shutdown to all workers and wait for them to exit."""
        try:
            worker_batches = [None] * self.num_workers
            self._send_to_workers(
                0, ipc.FLAG_SHUTDOWN, worker_batches, None)
        except errors.GnitzError:
            pass
        for w in range(self.num_workers):
            pid = self.worker_pids[w]
            if pid > 0:
                try:
                    os.waitpid(pid, 0)
                except OSError:
                    pass
            try:
                os.close(intmask(self.worker_fds[w]))
            except OSError:
                pass
