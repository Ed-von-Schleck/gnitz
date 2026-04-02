# gnitz/server/master.py
#
# Master-side dispatcher: fans out push/scan operations to worker processes
# via the shared append-only log (SAL) and collects responses via per-worker
# W2M regions. Eventfds provide cross-process signaling.

import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi

from gnitz import log
from gnitz.server import ipc, ipc_ffi
from gnitz.storage import engine_ffi
from gnitz.storage import mmap_posix
from gnitz.core import errors
from gnitz.storage.owned_batch import ArenaZSetBatch
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

    _immutable_fields_ = ["num_workers", "assignment",
                          "sal", "m2w_efds[*]", "w2m_efds[*]"]

    def __init__(self, num_workers, worker_pids, assignment,
                 dag_handle, registry, sal, w2m_regions, m2w_efds, w2m_efds):
        self.num_workers = num_workers
        self.worker_pids = worker_pids
        self.assignment = assignment
        self.dag_handle = dag_handle
        self.registry = registry
        self.router = PartitionRouter(assignment)
        self.sal = sal                     # SharedAppendLog
        self.w2m_regions = w2m_regions     # list[W2MRegion]
        self.m2w_efds = m2w_efds           # list[int] — master→worker eventfds
        self.w2m_efds = w2m_efds           # list[int] — worker→master eventfds
        self._checkpoint_threshold = (sal.mmap_size * 3) >> 2  # 75%

        # Async tick state (non-blocking tick processing)
        self._async_remaining = 0
        self._async_collected = [False] * num_workers
        self._async_w2m_rcs = [ipc.W2M_HEADER_SIZE] * num_workers
        self._async_exchange_payloads = {}     # vid -> [IPCPayload|None] * N
        self._async_exchange_counts = {}       # vid -> int
        self._async_exchange_schemas = {}      # vid -> Schema
        self._async_exchange_source_ids = {}   # vid -> int
        self._async_schema = None              # Schema for current tick
        self._async_active = False

    # -----------------------------------------------------------------------
    # Core send/receive helpers
    # -----------------------------------------------------------------------

    def _write_group(self, target_id, flags, worker_batches, schema,
                     seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0,
                     unicast_worker=-1):
        """Encode per-worker data and write one SAL message group.

        Does NOT fdatasync or signal. worker_batches[w] is ArenaZSetBatch
        or None. When unicast_worker >= 0, only that worker gets wire
        data; others see offset/size 0 and skip on read.
        """
        wire_bufs = [None] * self.num_workers
        for w in range(self.num_workers):
            if unicast_worker >= 0 and w != unicast_worker:
                continue
            wb = worker_batches[w]
            wire_bufs[w] = ipc._encode_wire(
                target_id, 0, wb, schema, flags,
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
            engine_ffi.eventfd_signal(self.m2w_efds[w])

    def _sync_and_signal_one(self, worker):
        """fdatasync the SAL, then signal one worker via eventfd."""
        mmap_posix.fdatasync_c(self.sal.fd)
        engine_ffi.eventfd_signal(self.m2w_efds[worker])

    def _write_unicast(self, worker, target_id, flags, schema,
                       seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0):
        """Write a SAL message targeted at a single worker."""
        worker_batches = [None] * self.num_workers
        self._write_group(target_id, flags, worker_batches, schema,
                          seek_pk_lo=seek_pk_lo, seek_pk_hi=seek_pk_hi,
                          seek_col_idx=seek_col_idx,
                          unicast_worker=worker)

    def _send_to_workers(self, target_id, flags, worker_batches, schema,
                         seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0):
        """Write message group + fdatasync + signal all workers."""
        self._write_group(target_id, flags, worker_batches, schema,
                          seek_pk_lo, seek_pk_hi, seek_col_idx)
        self._sync_and_signal_all()

    def _send_broadcast(self, target_id, flags, batch, schema,
                        seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0):
        """Encode batch once and broadcast identical data to all workers."""
        wire_buf = ipc._encode_wire(
            target_id, 0, batch, schema, flags,
            seek_pk_lo, seek_pk_hi, seek_col_idx, 0, "")
        wire_bufs = [None] * self.num_workers
        for w in range(self.num_workers):
            wire_bufs[w] = wire_buf
        lsn = self.sal.lsn_counter
        self.sal.lsn_counter += 1
        ipc.write_message_group(self.sal, target_id, lsn, flags,
                                wire_bufs, self.num_workers)
        wire_buf.free()
        self._sync_and_signal_all()

    def _reset_w2m_cursors(self):
        """Reset all workers' W2M write cursors after master has read all responses."""
        for w in range(self.num_workers):
            self.w2m_regions[w].set_write_cursor(ipc.W2M_HEADER_SIZE)

    def _wait_all_workers(self):
        """Wait for one response from each worker. Returns list[IPCPayload].

        ALWAYS resets W2M cursors before returning, even on exception.
        """
        results = [None] * self.num_workers
        remaining = self.num_workers
        collected = [False] * self.num_workers
        w2m_rcs = [ipc.W2M_HEADER_SIZE] * self.num_workers
        try:
            while remaining > 0:
                engine_ffi.eventfd_wait_any(self.w2m_efds, 1000)
                for w in range(self.num_workers):
                    if collected[w]:
                        continue
                    wc = self.w2m_regions[w].get_write_cursor()
                    if w2m_rcs[w] < wc:
                        payload, w2m_rcs[w] = ipc.read_from_w2m(
                            self.w2m_regions[w], w2m_rcs[w])
                        if payload.status != 0:
                            raise errors.StorageError(
                                "Worker %d error: %s"
                                % (w, payload.error_msg))
                        results[w] = payload
                        collected[w] = True
                        remaining -= 1
        finally:
            self._reset_w2m_cursors()
        return results

    def _collect_acks(self):
        """Collect one ACK from each worker. Raises on error."""
        self._wait_all_workers()

    def _collect_acks_and_relay(self, schema):
        """Collect ACKs from all workers, relaying exchange messages."""
        remaining = self.num_workers
        collected = [False] * self.num_workers
        w2m_rcs = [ipc.W2M_HEADER_SIZE] * self.num_workers

        exchange_payloads = {}
        exchange_counts = {}
        exchange_schemas = {}
        exchange_source_ids = {}

        try:
            while remaining > 0:
                engine_ffi.eventfd_wait_any(self.w2m_efds, 1000)
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
                                exchange_payloads[vid] = (
                                    [None] * self.num_workers)
                                exchange_counts[vid] = 0
                            exchange_payloads[vid][w] = payload
                            if payload.schema is not None:
                                exchange_schemas[vid] = payload.schema
                            if ex_source_id > 0:
                                exchange_source_ids[vid] = ex_source_id
                            exchange_counts[vid] += 1
                            if exchange_counts[vid] == self.num_workers:
                                ex_schema = exchange_schemas.get(
                                    vid, schema)
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
                                raise errors.StorageError(
                                    "Worker %d error: %s"
                                    % (w, payload.error_msg))
                            collected[w] = True
                            remaining -= 1
                            break
        finally:
            self._reset_w2m_cursors()

    def _collect_responses(self, schema):
        """Collect data responses from all workers. Returns concatenated batch."""
        results = self._wait_all_workers()
        result = ArenaZSetBatch(schema)
        for w in range(self.num_workers):
            payload = results[w]
            if (payload is not None and payload.batch is not None
                    and payload.batch.length() > 0):
                result.append_batch(payload.batch)
        return result

    def _collect_one(self, worker):
        """Collect a single response from one worker. Returns IPCPayload."""
        w2m_rc = ipc.W2M_HEADER_SIZE
        try:
            while True:
                engine_ffi.eventfd_wait(self.w2m_efds[worker], 1000)
                wc = self.w2m_regions[worker].get_write_cursor()
                if w2m_rc < wc:
                    payload, w2m_rc = ipc.read_from_w2m(
                        self.w2m_regions[worker], w2m_rc)
                    return payload
        finally:
            self.w2m_regions[worker].set_write_cursor(
                ipc.W2M_HEADER_SIZE)

    # -----------------------------------------------------------------------
    # SAL Checkpoint
    # -----------------------------------------------------------------------

    def _maybe_checkpoint(self):
        """Proactive checkpoint when SAL fill exceeds 75%."""
        if self.sal.write_cursor < self._checkpoint_threshold:
            return
        self._do_checkpoint()

    def _do_checkpoint(self):
        """Flush all workers, collect checkpoint ACKs, reset SAL."""
        # Step 1: Ask all workers to flush
        worker_batches = [None] * self.num_workers
        self._send_to_workers(0, ipc.FLAG_FLUSH, worker_batches, None)

        # Step 2: Collect checkpoint ACKs from all workers
        remaining = self.num_workers
        collected = [False] * self.num_workers
        w2m_rcs = [ipc.W2M_HEADER_SIZE] * self.num_workers
        try:
            while remaining > 0:
                engine_ffi.eventfd_wait_any(self.w2m_efds, 1000)
                for w in range(self.num_workers):
                    if collected[w]:
                        continue
                    wc = self.w2m_regions[w].get_write_cursor()
                    if w2m_rcs[w] < wc:
                        payload, w2m_rcs[w] = ipc.read_from_w2m(
                            self.w2m_regions[w], w2m_rcs[w])
                        if payload.status != 0:
                            raise errors.StorageError(
                                "Worker %d checkpoint error: %s"
                                % (w, payload.error_msg))
                        collected[w] = True
                        remaining -= 1
        finally:
            self._reset_w2m_cursors()

        # Step 3: All workers flushed. Reset SAL.
        self.sal.epoch += 1
        self.sal.write_cursor = 0
        # Zero sentinel at offset 0 (release fence — visible to workers)
        ipc.atomic_store_u64(
            self.sal.ptr, rffi.cast(rffi.ULONGLONG, 0))
        log.info("SAL checkpoint epoch=" + str(self.sal.epoch))

    # -----------------------------------------------------------------------
    # Fan-out operations
    # -----------------------------------------------------------------------

    def fan_out_ingest(self, target_id, batch, schema):
        """Split batch by worker partition, send FLAG_PUSH, collect ACKs.
        Workers only ingest — no evaluate_dag, no exchange relay."""
        self._maybe_checkpoint()
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
        self._maybe_checkpoint()
        worker_batches = [None] * self.num_workers
        self._send_to_workers(target_id, ipc.FLAG_TICK, worker_batches, schema)
        self._collect_acks_and_relay(schema)
        log.debug("fan_out_tick tid=" + str(target_id))

    # -----------------------------------------------------------------------
    # Async (non-blocking) tick API
    # -----------------------------------------------------------------------

    def start_tick_async(self, target_id, schema):
        """Write FLAG_TICK to SAL, signal workers, init async tracking state.

        Does NOT block.  Caller must poll poll_tick_progress() until True.
        """
        self._maybe_checkpoint()
        worker_batches = [None] * self.num_workers
        self._send_to_workers(target_id, ipc.FLAG_TICK, worker_batches, schema)

        self._async_remaining = self.num_workers
        for w in range(self.num_workers):
            self._async_collected[w] = False
            self._async_w2m_rcs[w] = ipc.W2M_HEADER_SIZE
        self._async_exchange_payloads.clear()
        self._async_exchange_counts.clear()
        self._async_exchange_schemas.clear()
        self._async_exchange_source_ids.clear()
        self._async_schema = schema
        self._async_active = True

    def poll_tick_progress(self):
        """Non-blocking progress check for an async tick.

        Returns True when the tick is complete (all workers sent final ACK).
        Handles exchange relay inline when all workers have reported for a
        given view_id.  Raises StorageError on worker error.
        """
        if not self._async_active:
            return True

        # Short timeout: yield CPU to workers while still polling client I/O
        engine_ffi.eventfd_wait_any(self.w2m_efds, 1)

        for w in range(self.num_workers):
            if self._async_collected[w]:
                continue
            wc = self.w2m_regions[w].get_write_cursor()
            while self._async_w2m_rcs[w] < wc:
                payload, self._async_w2m_rcs[w] = ipc.read_from_w2m(
                    self.w2m_regions[w], self._async_w2m_rcs[w])
                if payload.flags & ipc.FLAG_EXCHANGE:
                    vid = intmask(payload.target_id)
                    ex_source_id = intmask(payload.seek_pk_lo)
                    if vid not in self._async_exchange_payloads:
                        self._async_exchange_payloads[vid] = (
                            [None] * self.num_workers)
                        self._async_exchange_counts[vid] = 0
                    self._async_exchange_payloads[vid][w] = payload
                    if payload.schema is not None:
                        self._async_exchange_schemas[vid] = payload.schema
                    if ex_source_id > 0:
                        self._async_exchange_source_ids[vid] = ex_source_id
                    self._async_exchange_counts[vid] += 1
                    if self._async_exchange_counts[vid] == self.num_workers:
                        ex_schema = self._async_exchange_schemas.get(
                            vid, self._async_schema)
                        src_id = self._async_exchange_source_ids.get(vid, 0)
                        self._relay_exchange(
                            vid, self._async_exchange_payloads[vid],
                            ex_schema, src_id)
                        del self._async_exchange_payloads[vid]
                        del self._async_exchange_counts[vid]
                        if vid in self._async_exchange_schemas:
                            del self._async_exchange_schemas[vid]
                        if vid in self._async_exchange_source_ids:
                            del self._async_exchange_source_ids[vid]
                    break  # re-check write_cursor after relay
                else:
                    if payload.status != 0:
                        self._finish_async_tick()
                        raise errors.StorageError(
                            "Worker %d error: %s"
                            % (w, payload.error_msg))
                    self._async_collected[w] = True
                    self._async_remaining -= 1
                    break

        if self._async_remaining == 0:
            self._finish_async_tick()
            return True
        return False

    def _finish_async_tick(self):
        """Clean up async tick state.  Resets W2M cursors."""
        self._reset_w2m_cursors()
        self._async_active = False
        self._async_schema = None

    # -----------------------------------------------------------------------

    def fan_out_push(self, target_id, batch, schema):
        """Split batch by worker partition, send sub-batches, collect ACKs.
        Handles mid-circuit exchange relay when workers send FLAG_EXCHANGE."""
        self._maybe_checkpoint()
        n = batch.length()
        # Get preloadable views from DagEngine
        pv_vids = rffi.lltype.malloc(rffi.LONGLONGP.TO, 64, flavor="raw")
        pv_cols = rffi.lltype.malloc(rffi.INTP.TO, 64, flavor="raw")
        try:
            n_pv = engine_ffi._dag_get_preloadable_views(
                self.dag_handle, target_id, pv_vids, pv_cols, 64)
            preloadable = newlist_hint(intmask(n_pv))
            for pi in range(intmask(n_pv)):
                preloadable.append(
                    (intmask(pv_vids[pi]), [intmask(pv_cols[pi])]))
        finally:
            rffi.lltype.free(pv_vids, flavor="raw")
            rffi.lltype.free(pv_cols, flavor="raw")
        family = self.registry.get_by_id(target_id)

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
        # Guard: error if SAL space is critically low during relay
        remaining = self.sal.mmap_size - self.sal.write_cursor
        if remaining < (self.sal.mmap_size >> 3):  # < 12.5% (128 MB)
            raise errors.StorageError(
                "SAL space exhausted during exchange relay (%d bytes left)"
                % remaining)
        shard_cols = []
        sc_buf = rffi.lltype.malloc(rffi.INTP.TO, 8, flavor="raw")
        try:
            if source_id > 0:
                n_sc = engine_ffi._dag_get_join_shard_cols(
                    self.dag_handle, view_id, source_id, sc_buf, 8)
                for sci in range(intmask(n_sc)):
                    shard_cols.append(intmask(sc_buf[sci]))
            if len(shard_cols) == 0:
                n_sc = engine_ffi._dag_get_shard_cols(
                    self.dag_handle, view_id, sc_buf, 8)
                for sci in range(intmask(n_sc)):
                    shard_cols.append(intmask(sc_buf[sci]))
        finally:
            rffi.lltype.free(sc_buf, flavor="raw")
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
        self._maybe_checkpoint()
        worker_batches = [None] * self.num_workers
        self._send_to_workers(
            source_id, ipc.FLAG_BACKFILL, worker_batches, source_schema,
            seek_pk_lo=view_id)
        self._collect_acks_and_relay(source_schema)

    def fan_out_scan(self, target_id, schema):
        """Send scan to all workers, concatenate result batches."""
        self._maybe_checkpoint()
        worker_batches = [None] * self.num_workers
        self._send_to_workers(target_id, 0, worker_batches, schema)
        result = self._collect_responses(schema)
        log.debug(
            "fan_out_scan tid=" + str(target_id)
            + " result_rows=" + str(result.length()))
        return result

    def fan_out_seek(self, target_id, pk_lo, pk_hi, schema):
        """Route PK seek to the single worker that owns the partition."""
        self._maybe_checkpoint()
        worker = worker_for_pk(pk_lo, pk_hi, self.assignment)
        self._write_unicast(
            worker, target_id, ipc.FLAG_SEEK, schema,
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
        self._maybe_checkpoint()
        cached_worker = self.router.worker_for_index_key(
            target_id, col_idx, key_lo, key_hi)
        if cached_worker >= 0:
            self._write_unicast(
                cached_worker, target_id, ipc.FLAG_SEEK_BY_INDEX, schema,
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
        results = self._wait_all_workers()
        result = None
        for w in range(self.num_workers):
            payload = results[w]
            if (result is None and payload is not None
                    and payload.batch is not None
                    and payload.batch.length() > 0):
                result = ArenaZSetBatch(schema)
                result.append_batch(payload.batch)
        return result

    def broadcast_ddl(self, target_id, batch, schema):
        """Send a system-table delta to all workers for registry sync."""
        self._maybe_checkpoint()
        self._send_broadcast(target_id, ipc.FLAG_DDL_SYNC, batch, schema)
        log.debug(
            "broadcast_ddl tid=" + str(target_id)
            + " rows=" + str(batch.length()))

    def check_fk_batch(self, target_id, check_batch, schema):
        """Check PK existence across workers. Returns True if any key missing."""
        self._maybe_checkpoint()
        sub_batches = repartition_batch(
            check_batch, [schema.pk_index], self.num_workers, self.assignment)
        self._send_to_workers(
            target_id, ipc.FLAG_HAS_PK, sub_batches, schema)
        for w in range(self.num_workers):
            sb = sub_batches[w]
            if sb is not None:
                sb.free()

        results = self._wait_all_workers()
        any_missing = False
        for w in range(self.num_workers):
            payload = results[w]
            if payload is not None and payload.batch is not None:
                for j in range(payload.batch.length()):
                    if payload.batch.get_weight(j) == r_int64(0):
                        any_missing = True
        return any_missing

    def check_pk_exists_broadcast(self, owner_table_id, source_col_idx,
                                  check_batch, schema):
        """Broadcast PK existence check. Returns True if any key exists."""
        self._maybe_checkpoint()
        col_hint = source_col_idx + 1
        self._send_broadcast(
            owner_table_id, ipc.FLAG_HAS_PK, check_batch, schema,
            seek_col_idx=col_hint)

        results = self._wait_all_workers()
        any_exists = False
        for w in range(self.num_workers):
            payload = results[w]
            if payload is not None and payload.batch is not None:
                for j in range(payload.batch.length()):
                    if payload.batch.get_weight(j) == r_int64(1):
                        any_exists = True
        return any_exists

    def check_pk_existence(self, target_id, check_batch, schema):
        """Split-route PK check. Returns {lo: {hi: 1}} dict of existing PKs."""
        self._maybe_checkpoint()
        sub_batches = repartition_batch(
            check_batch, [schema.pk_index], self.num_workers, self.assignment)
        self._send_to_workers(
            target_id, ipc.FLAG_HAS_PK, sub_batches, schema)
        for w in range(self.num_workers):
            sb = sub_batches[w]
            if sb is not None:
                sb.free()

        results = self._wait_all_workers()
        existing = {}
        for w in range(self.num_workers):
            payload = results[w]
            if payload is not None and payload.batch is not None:
                for j in range(payload.batch.length()):
                    if payload.batch.get_weight(j) == r_int64(1):
                        lo = intmask(r_uint64(
                            payload.batch._read_pk_lo(j)))
                        hi = intmask(r_uint64(
                            payload.batch._read_pk_hi(j)))
                        if lo not in existing:
                            existing[lo] = {}
                        existing[lo][hi] = 1
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
