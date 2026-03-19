# gnitz/server/master.py
#
# Master-side dispatcher: fans out push/scan operations to worker processes
# and collects responses.

import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz import log
from gnitz.server import ipc, ipc_ffi
from gnitz.core import errors
from gnitz.core.batch import ArenaZSetBatch
from gnitz.dbsp.ops.exchange import repartition_batch, worker_for_pk, relay_scatter, PartitionRouter, multi_scatter

WNOHANG = 1


class MasterDispatcher(object):
    """
    Fans out push/scan operations to N workers over socketpair fds.
    Collects ACKs/responses synchronously.
    """

    _immutable_fields_ = ["num_workers", "worker_fds[*]", "assignment"]

    def __init__(self, num_workers, worker_fds, worker_pids, assignment, program_cache):
        self.num_workers = num_workers
        self.worker_fds = worker_fds
        self.worker_pids = worker_pids
        self.assignment = assignment
        self.program_cache = program_cache
        self.router = PartitionRouter(assignment)

    def fan_out_push(self, target_id, batch, schema):
        """Split batch by worker partition, send sub-batches, collect ACKs.
        Handles mid-circuit exchange relay when workers send FLAG_EXCHANGE.
        For trivial non-co-partitioned views, pre-sends repartitioned batches
        as FLAG_PRELOADED_EXCHANGE before the push to eliminate the round-trip."""
        n = batch.length()
        preloadable = self.program_cache.get_preloadable_views(target_id)
        family = self.program_cache.registry.get_by_id(target_id)

        # INVARIANT: FLAG_PRELOADED_EXCHANGE requires all routing-column values in the
        # push batch to be correctly filled at routing time.  Rust's delete() zeros
        # non-PK columns in DELETE rows (only the PK is set; all other fields are 0).
        # Pre-routing by those zeroed values sends preload batches to the wrong workers.
        # Guard: any batch with weight<0 rows must skip the preload path entirely and
        # fall back to FLAG_EXCHANGE so ingest_to_family recovers actual stored values
        # before the delta is re-routed.
        if len(preloadable) > 0:
            has_retraction = False
            for i in range(n):
                if batch.get_weight(i) < r_int64(0):
                    has_retraction = True
                    break
            if has_retraction:
                preloadable = newlist_hint(0)

        if len(preloadable) == 0:
            # No trivial pre-plans: existing single-scatter path
            sub_batches = repartition_batch(
                batch, [schema.pk_index], self.num_workers, self.assignment
            )
            # Record index routing for unique secondary indexes on this table.
            for ci in range(len(family.index_circuits)):
                circuit = family.index_circuits[ci]
                if circuit.is_unique:
                    col_idx = circuit.source_col_idx
                    for w in range(self.num_workers):
                        sb = sub_batches[w]
                        if sb is not None and sb.length() > 0:
                            self.router.record_routing(sb, target_id, col_idx, w)
            # Send to ALL workers (even empty batches) so they all participate
            # in potential exchange barriers.
            for w in range(self.num_workers):
                sb = sub_batches[w]
                ipc.send_batch(
                    self.worker_fds[w], target_id, sb, schema=schema,
                    flags=ipc.FLAG_PUSH,
                )
                if sb is not None:
                    sb.free()
        else:
            # Combined multi-scatter: PK routing + all preloadable shard routings.
            # all_batches[0] = PK-partitioned sub-batches (for push + index routing).
            # all_batches[1..] = shard-partitioned sub-batches for each preloadable view.
            col_specs = newlist_hint(len(preloadable) + 1)
            col_specs.append([schema.pk_index])
            for i in range(len(preloadable)):
                col_specs.append(preloadable[i][1])
            all_batches = multi_scatter(batch, col_specs, self.num_workers, self.assignment)
            pk_batches = all_batches[0]
            # Record index routing for unique secondary indexes on this table.
            for ci in range(len(family.index_circuits)):
                circuit = family.index_circuits[ci]
                if circuit.is_unique:
                    col_idx = circuit.source_col_idx
                    for w in range(self.num_workers):
                        sb = pk_batches[w]
                        if sb is not None and sb.length() > 0:
                            self.router.record_routing(sb, target_id, col_idx, w)
            # Send preloads first, then push, for each worker.
            for w in range(self.num_workers):
                for si in range(len(preloadable)):
                    vid = preloadable[si][0]
                    preload_sb = all_batches[si + 1][w]
                    ipc.send_batch(
                        self.worker_fds[w], vid, preload_sb,
                        schema=schema, flags=ipc.FLAG_PRELOADED_EXCHANGE,
                    )
                    if preload_sb is not None:
                        preload_sb.free()
                sb = pk_batches[w]
                ipc.send_batch(
                    self.worker_fds[w], target_id, sb,
                    schema=schema, flags=ipc.FLAG_PUSH,
                )
                if sb is not None:
                    sb.free()

        # Message loop: handle both ACKs and exchange relays.
        # Build poll lists once; swap-remove on ACK (O(1) per worker).
        poll_fds    = newlist_hint(self.num_workers)
        poll_events = newlist_hint(self.num_workers)
        poll_wids   = newlist_hint(self.num_workers)
        for w in range(self.num_workers):
            poll_fds.append(self.worker_fds[w])
            poll_events.append(ipc_ffi.POLLIN | ipc_ffi.POLLERR | ipc_ffi.POLLHUP)
            poll_wids.append(w)

        exchange_payloads = {}   # view_id -> [IPCPayload | None] * num_workers
        exchange_counts = {}     # view_id -> int
        exchange_schemas = {}    # view_id -> schema
        exchange_source_ids = {} # view_id -> source_id (for join exchange)

        while len(poll_fds) > 0:
            revents = ipc_ffi.poll(poll_fds, poll_events, 10)
            pi = len(poll_fds) - 1
            while pi >= 0:
                if revents[pi] & (ipc_ffi.POLLIN | ipc_ffi.POLLERR | ipc_ffi.POLLHUP):
                    w = poll_wids[pi]
                    payload = ipc.receive_payload(self.worker_fds[w])
                    if payload.flags & ipc.FLAG_EXCHANGE:
                        vid = intmask(payload.target_id)
                        ex_source_id = intmask(payload.seek_pk_lo)
                        if vid not in exchange_payloads:
                            exchange_payloads[vid] = [None] * self.num_workers
                            exchange_counts[vid] = 0
                        exchange_payloads[vid][w] = payload          # store; do NOT close
                        if payload.schema is not None:
                            exchange_schemas[vid] = payload.schema
                        if ex_source_id > 0:
                            exchange_source_ids[vid] = ex_source_id
                        exchange_counts[vid] += 1
                        # payload.close() is deferred to _relay_exchange

                        if exchange_counts[vid] == self.num_workers:
                            ex_schema = exchange_schemas.get(vid, schema)
                            src_id = exchange_source_ids.get(vid, 0)
                            self._relay_exchange(vid, exchange_payloads[vid], ex_schema, src_id)
                            del exchange_payloads[vid]
                            del exchange_counts[vid]
                            if vid in exchange_schemas:
                                del exchange_schemas[vid]
                            if vid in exchange_source_ids:
                                del exchange_source_ids[vid]
                            # payloads closed inside _relay_exchange
                    else:
                        # Normal ACK — swap-remove this worker from the poll set
                        if payload.status != 0:
                            err = payload.error_msg
                            payload.close()
                            raise errors.StorageError(
                                "Worker %d push error: %s" % (w, err)
                            )
                        payload.close()
                        last = len(poll_fds) - 1
                        if pi != last:
                            poll_fds[pi]    = poll_fds[last]
                            poll_events[pi] = poll_events[last]
                            poll_wids[pi]   = poll_wids[last]
                        poll_fds.pop()
                        poll_events.pop()
                        poll_wids.pop()
                pi -= 1

        log.debug(
            "fan_out_push tid=" + str(target_id)
            + " rows=" + str(n)
        )

    def _relay_exchange(self, view_id, payloads, schema, source_id=0):
        """Repartition collected exchange payloads and relay to workers."""
        shard_cols = []
        if source_id > 0:
            shard_cols = self.program_cache.get_join_shard_cols(view_id, source_id)
        if len(shard_cols) == 0:
            shard_cols = self.program_cache.get_shard_cols(view_id)
        sources = [None] * self.num_workers
        for i in range(len(payloads)):
            p = payloads[i]
            if p is not None:
                sources[i] = p.batch
        dest_batches = relay_scatter(sources, shard_cols, self.num_workers, self.assignment)
        for p in payloads:
            if p is not None:
                p.close()
        for w in range(self.num_workers):
            ipc.send_batch(
                self.worker_fds[w], view_id, dest_batches[w],
                schema=schema,
            )
            if dest_batches[w] is not None:
                dest_batches[w].free()

    def fan_out_backfill(self, view_id, source_id, source_schema):
        """Broadcast FLAG_BACKFILL; handle exchange relay; collect ACKs."""
        for w in range(self.num_workers):
            ipc.send_batch(
                self.worker_fds[w], source_id, None,
                schema=source_schema, flags=ipc.FLAG_BACKFILL,
                seek_pk_lo=view_id,
            )

        poll_fds = newlist_hint(self.num_workers)
        poll_events = newlist_hint(self.num_workers)
        poll_wids = newlist_hint(self.num_workers)
        for w in range(self.num_workers):
            poll_fds.append(self.worker_fds[w])
            poll_events.append(ipc_ffi.POLLIN | ipc_ffi.POLLERR | ipc_ffi.POLLHUP)
            poll_wids.append(w)

        exchange_payloads = {}
        exchange_counts = {}
        exchange_schemas = {}
        exchange_source_ids = {}

        while len(poll_fds) > 0:
            revents = ipc_ffi.poll(poll_fds, poll_events, 10)
            pi = len(poll_fds) - 1
            while pi >= 0:
                if revents[pi] & (ipc_ffi.POLLIN | ipc_ffi.POLLERR | ipc_ffi.POLLHUP):
                    w = poll_wids[pi]
                    payload = ipc.receive_payload(self.worker_fds[w])
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
                            ex_schema = exchange_schemas.get(vid, source_schema)
                            src_id = exchange_source_ids.get(vid, 0)
                            self._relay_exchange(vid, exchange_payloads[vid], ex_schema, src_id)
                            del exchange_payloads[vid]
                            del exchange_counts[vid]
                            if vid in exchange_schemas:
                                del exchange_schemas[vid]
                            if vid in exchange_source_ids:
                                del exchange_source_ids[vid]
                    else:
                        # ACK — swap-remove this worker from the poll set
                        payload.close()
                        last = len(poll_fds) - 1
                        if pi != last:
                            poll_fds[pi]    = poll_fds[last]
                            poll_events[pi] = poll_events[last]
                            poll_wids[pi]   = poll_wids[last]
                        poll_fds.pop()
                        poll_events.pop()
                        poll_wids.pop()
                pi -= 1

    def fan_out_scan(self, target_id, schema):
        """Send scan to all workers, concatenate result batches."""
        for w in range(self.num_workers):
            ipc.send_batch(self.worker_fds[w], target_id, None, schema=schema)

        result = ArenaZSetBatch(schema)
        scan_fds    = newlist_hint(self.num_workers)
        scan_events = newlist_hint(self.num_workers)
        scan_wids   = newlist_hint(self.num_workers)
        for w in range(self.num_workers):
            scan_fds.append(self.worker_fds[w])
            scan_events.append(ipc_ffi.POLLIN | ipc_ffi.POLLERR | ipc_ffi.POLLHUP)
            scan_wids.append(w)

        while len(scan_fds) > 0:
            revents = ipc_ffi.poll(scan_fds, scan_events, 10)
            pi = len(scan_fds) - 1
            while pi >= 0:
                if revents[pi] & (ipc_ffi.POLLIN | ipc_ffi.POLLERR | ipc_ffi.POLLHUP):
                    w = scan_wids[pi]
                    payload = ipc.receive_payload(self.worker_fds[w])
                    if payload.status != 0:
                        err = payload.error_msg
                        payload.close()
                        raise errors.StorageError(
                            "Worker %d scan error: %s" % (w, err)
                        )
                    if payload.batch is not None and payload.batch.length() > 0:
                        result.append_batch(payload.batch)
                    payload.close()
                    last = len(scan_fds) - 1
                    if pi != last:
                        scan_fds[pi]    = scan_fds[last]
                        scan_events[pi] = scan_events[last]
                        scan_wids[pi]   = scan_wids[last]
                    scan_fds.pop()
                    scan_events.pop()
                    scan_wids.pop()
                pi -= 1

        log.debug(
            "fan_out_scan tid=" + str(target_id)
            + " result_rows=" + str(result.length())
        )
        return result

    def fan_out_seek(self, target_id, pk_lo, pk_hi, schema):
        """Route PK seek to the single worker that owns the relevant partition."""
        worker = worker_for_pk(pk_lo, pk_hi, self.assignment)
        ipc.send_batch(
            self.worker_fds[worker], target_id, None, schema=schema,
            flags=ipc.FLAG_SEEK, seek_pk_lo=pk_lo, seek_pk_hi=pk_hi,
        )
        payload = ipc.receive_payload(self.worker_fds[worker])
        if payload.status != 0:
            err = payload.error_msg
            payload.close()
            raise errors.StorageError("Worker %d seek error: %s" % (worker, err))
        result = None
        if payload.batch is not None and payload.batch.length() > 0:
            result = ArenaZSetBatch(schema)
            result.append_batch(payload.batch)
        payload.close()
        return result

    def fan_out_seek_by_index(self, target_id, col_idx, key_lo, key_hi, schema):
        """Broadcast index seek to all workers; return first non-empty result.
        On cache hit, unicasts to the single owning worker instead."""
        cached_worker = self.router.worker_for_index_key(target_id, col_idx, key_lo, key_hi)
        if cached_worker >= 0:
            ipc.send_batch(
                self.worker_fds[cached_worker], target_id, None, schema=schema,
                flags=ipc.FLAG_SEEK_BY_INDEX,
                seek_col_idx=col_idx, seek_pk_lo=key_lo, seek_pk_hi=key_hi,
            )
            payload = ipc.receive_payload(self.worker_fds[cached_worker])
            if payload.status != 0:
                err = payload.error_msg
                payload.close()
                raise errors.StorageError(
                    "Worker %d seek_by_index error: %s" % (cached_worker, err)
                )
            result = None
            if payload.batch is not None and payload.batch.length() > 0:
                result = ArenaZSetBatch(schema)
                result.append_batch(payload.batch)
            payload.close()
            return result
        # Cache miss: broadcast to all workers
        for w in range(self.num_workers):
            ipc.send_batch(
                self.worker_fds[w], target_id, None, schema=schema,
                flags=ipc.FLAG_SEEK_BY_INDEX,
                seek_col_idx=col_idx, seek_pk_lo=key_lo, seek_pk_hi=key_hi,
            )
        result = None
        for w in range(self.num_workers):
            payload = ipc.receive_payload(self.worker_fds[w])
            if payload.status != 0:
                err = payload.error_msg
                payload.close()
                raise errors.StorageError(
                    "Worker %d seek_by_index error: %s" % (w, err)
                )
            if result is None and payload.batch is not None and payload.batch.length() > 0:
                result = ArenaZSetBatch(schema)
                result.append_batch(payload.batch)
            payload.close()
        return result

    def broadcast_ddl(self, target_id, batch, schema):
        """Send a system-table delta to all workers for registry sync."""
        ipc.broadcast_batch(self.worker_fds, target_id, batch,
                            schema=schema, flags=ipc.FLAG_DDL_SYNC)
        log.debug(
            "broadcast_ddl tid=" + str(target_id)
            + " rows=" + str(batch.length())
        )

    def check_fk_batch(self, target_id, check_batch, schema):
        """Check PK existence across workers. Returns True if any key is missing."""
        sub_batches = repartition_batch(check_batch, [schema.pk_index], self.num_workers, self.assignment)

        # Send to ALL workers (even empty sub-batches) so the request/response
        # pattern is always symmetric and cannot deadlock.
        for w in range(self.num_workers):
            sb = sub_batches[w]
            ipc.send_batch(
                self.worker_fds[w], target_id, sb, schema=schema,
                flags=ipc.FLAG_HAS_PK,
            )
            if sb is not None:
                sb.free()

        # Collect responses from ALL workers.
        any_missing = False
        for w in range(self.num_workers):
            payload = ipc.receive_payload(self.worker_fds[w])
            if payload.status != 0:
                err = payload.error_msg
                payload.close()
                raise errors.StorageError(
                    "Worker %d has_pk error: %s" % (w, err)
                )
            if payload.batch is not None:
                for j in range(payload.batch.length()):
                    if payload.batch.get_weight(j) == r_int64(0):
                        any_missing = True
            payload.close()

        return any_missing

    def check_pk_exists_broadcast(self, owner_table_id, source_col_idx, check_batch, schema):
        """Broadcast PK existence check to all workers. Returns True if any key exists.

        Used for unique index enforcement: index stores are partitioned by main-table PK
        (not index key), so we must ask ALL workers.
        """
        col_hint = source_col_idx + 1
        for w in range(self.num_workers):
            ipc.send_batch(
                self.worker_fds[w], owner_table_id, check_batch, schema=schema,
                flags=ipc.FLAG_HAS_PK, seek_col_idx=col_hint,
            )
        any_exists = False
        for w in range(self.num_workers):
            payload = ipc.receive_payload(self.worker_fds[w])
            if payload.status != 0:
                err = payload.error_msg
                payload.close()
                raise errors.StorageError(
                    "Worker %d has_pk error: %s" % (w, err)
                )
            if payload.batch is not None:
                for j in range(payload.batch.length()):
                    if payload.batch.get_weight(j) == r_int64(1):
                        any_exists = True
            payload.close()
        return any_exists

    def check_pk_existence(self, target_id, check_batch, schema):
        """Split-route PK check. Returns {lo: {hi: 1}} dict of existing PKs.

        Used to identify UPSERT rows before unique index enforcement.
        """
        sub_batches = repartition_batch(check_batch, [schema.pk_index], self.num_workers, self.assignment)
        for w in range(self.num_workers):
            sb = sub_batches[w]
            ipc.send_batch(
                self.worker_fds[w], target_id, sb, schema=schema,
                flags=ipc.FLAG_HAS_PK,
            )
            if sb is not None:
                sb.free()
        existing = {}
        for w in range(self.num_workers):
            payload = ipc.receive_payload(self.worker_fds[w])
            if payload.status != 0:
                err = payload.error_msg
                payload.close()
                raise errors.StorageError(
                    "Worker %d has_pk error: %s" % (w, err)
                )
            if payload.batch is not None:
                for j in range(payload.batch.length()):
                    if payload.batch.get_weight(j) == r_int64(1):
                        lo = intmask(r_uint64(payload.batch._read_pk_lo(j)))
                        hi = intmask(r_uint64(payload.batch._read_pk_hi(j)))
                        if lo not in existing:
                            existing[lo] = {}
                        existing[lo][hi] = 1
            payload.close()
        return existing

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
        for w in range(self.num_workers):
            try:
                ipc.send_batch(
                    self.worker_fds[w], 0, None, flags=ipc.FLAG_SHUTDOWN
                )
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
