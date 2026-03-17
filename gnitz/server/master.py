# gnitz/server/master.py
#
# Master-side dispatcher: fans out push/scan operations to worker processes
# and collects responses.

import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz import log
from gnitz.server import ipc, ipc_ffi
from gnitz.core import errors
from gnitz.core.batch import ArenaZSetBatch
from gnitz.storage.partitioned_table import _partition_for_key
from gnitz.dbsp.ops.exchange import repartition_batch

WNOHANG = 1


class PartitionAssignment(object):
    """Maps 256 partitions to N workers. Worker w owns [start, end)."""

    _immutable_fields_ = ["num_workers", "starts[*]", "ends[*]",
                          "_partition_to_worker[*]"]

    def __init__(self, num_workers):
        self.num_workers = num_workers
        chunk = 256 // num_workers
        starts = [0] * num_workers
        ends = [0] * num_workers
        for w in range(num_workers):
            starts[w] = w * chunk
            if w == num_workers - 1:
                ends[w] = 256
            else:
                ends[w] = (w + 1) * chunk
        self.starts = starts
        self.ends = ends
        # Precompute O(1) lookup table
        lut = [0] * 256
        for w in range(num_workers):
            for p in range(starts[w], ends[w]):
                lut[p] = w
        self._partition_to_worker = lut

    def worker_for_partition(self, partition_idx):
        """Returns the worker ID that owns the given partition."""
        return self._partition_to_worker[partition_idx]

    def range_for_worker(self, worker_id):
        """Returns (start, end) partition range for the given worker."""
        return self.starts[worker_id], self.ends[worker_id]


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

    def _split_batch_by_pk(self, batch, schema):
        """Split batch into per-worker sub-batches by PK hash."""
        sub_batches = [None] * self.num_workers
        for i in range(batch.length()):
            pk_lo = r_uint64(batch._read_pk_lo(i))
            pk_hi = r_uint64(batch._read_pk_hi(i))
            p = _partition_for_key(pk_lo, pk_hi)
            w = self.assignment.worker_for_partition(p)
            if sub_batches[w] is None:
                sub_batches[w] = ArenaZSetBatch(schema)
            sub_batches[w]._direct_append_row(batch, i, batch.get_weight(i))
        return sub_batches

    def fan_out_push(self, target_id, batch, schema):
        """Split batch by worker partition, send sub-batches, collect ACKs.
        Handles mid-circuit exchange relay when workers send FLAG_EXCHANGE."""
        n = batch.length()
        sub_batches = self._split_batch_by_pk(batch, schema)

        # Send to ALL workers (even empty batches) so they all participate
        # in potential exchange barriers
        for w in range(self.num_workers):
            sb = sub_batches[w]
            ipc.send_batch(
                self.worker_fds[w], target_id, sb, schema=schema,
                flags=ipc.FLAG_PUSH,
            )
            if sb is not None:
                sb.free()

        # Message loop: handle both ACKs and exchange relays
        acked = [False] * self.num_workers
        num_acked = 0
        exchange_buffers = {}   # view_id -> [batch_per_worker]
        exchange_counts = {}    # view_id -> int
        exchange_schemas = {}   # view_id -> schema
        exchange_source_ids = {}  # view_id -> source_id (for join exchange)

        while num_acked < self.num_workers:
            poll_fds    = newlist_hint(self.num_workers)
            poll_events = newlist_hint(self.num_workers)
            poll_wids   = newlist_hint(self.num_workers)
            for w in range(self.num_workers):
                if not acked[w]:
                    poll_fds.append(self.worker_fds[w])
                    poll_events.append(ipc_ffi.POLLIN | ipc_ffi.POLLERR | ipc_ffi.POLLHUP)
                    poll_wids.append(w)

            revents = ipc_ffi.poll(poll_fds, poll_events, 10)

            for pi in range(len(poll_fds)):
                if revents[pi] & (ipc_ffi.POLLIN | ipc_ffi.POLLERR | ipc_ffi.POLLHUP):
                    w = poll_wids[pi]
                    payload = ipc.receive_payload(self.worker_fds[w])
                    if payload.flags & ipc.FLAG_EXCHANGE:
                        vid = intmask(payload.target_id)
                        ex_source_id = intmask(payload.seek_pk_lo)
                        if vid not in exchange_buffers:
                            exchange_buffers[vid] = [None] * self.num_workers
                            exchange_counts[vid] = 0
                        if payload.batch is not None and payload.batch.length() > 0:
                            exchange_buffers[vid][w] = payload.batch.clone()
                        if payload.schema is not None:
                            exchange_schemas[vid] = payload.schema
                        if ex_source_id > 0:
                            exchange_source_ids[vid] = ex_source_id
                        exchange_counts[vid] += 1
                        payload.close()

                        if exchange_counts[vid] == self.num_workers:
                            ex_schema = exchange_schemas.get(vid, schema)
                            src_id = exchange_source_ids.get(vid, 0)
                            self._relay_exchange(vid, exchange_buffers[vid], ex_schema, src_id)
                            del exchange_buffers[vid]
                            del exchange_counts[vid]
                            if vid in exchange_schemas:
                                del exchange_schemas[vid]
                            if vid in exchange_source_ids:
                                del exchange_source_ids[vid]
                    else:
                        # Normal ACK
                        if payload.status != 0:
                            err = payload.error_msg
                            payload.close()
                            raise errors.StorageError(
                                "Worker %d push error: %s" % (w, err)
                            )
                        payload.close()
                        acked[w] = True
                        num_acked += 1

        log.debug(
            "fan_out_push tid=" + str(target_id)
            + " rows=" + str(n)
        )

    def _relay_exchange(self, view_id, worker_batches, schema, source_id=0):
        """Repartition collected exchange batches and send to workers."""
        # Merge all worker batches into one
        merged = ArenaZSetBatch(schema)
        for w in range(self.num_workers):
            if worker_batches[w] is not None:
                merged.append_batch(worker_batches[w])
                worker_batches[w].free()

        # Determine shard columns: join-specific first, then circuit-level
        shard_cols = []
        if source_id > 0:
            shard_cols = self.program_cache.get_join_shard_cols(view_id, source_id)
        if len(shard_cols) == 0:
            shard_cols = self.program_cache.get_shard_cols(view_id)
        if len(shard_cols) > 0:
            dest_batches = repartition_batch(
                merged, shard_cols, self.num_workers, self.assignment
            )
        else:
            dest_batches = self._split_batch_by_pk(merged, schema)
        merged.free()

        for w in range(self.num_workers):
            ipc.send_batch(
                self.worker_fds[w], view_id, dest_batches[w],
                schema=schema,
            )
            if dest_batches[w] is not None:
                dest_batches[w].free()

    def fan_out_scan(self, target_id, schema):
        """Send scan to all workers, concatenate result batches."""
        for w in range(self.num_workers):
            ipc.send_batch(self.worker_fds[w], target_id, None, schema=schema)

        result = ArenaZSetBatch(schema)
        for w in range(self.num_workers):
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

        log.debug(
            "fan_out_scan tid=" + str(target_id)
            + " result_rows=" + str(result.length())
        )
        return result

    def fan_out_seek(self, target_id, pk_lo, pk_hi, schema):
        """Route PK seek to the single worker that owns the relevant partition."""
        partition = _partition_for_key(r_uint64(pk_lo), r_uint64(pk_hi))
        worker = self.assignment.worker_for_partition(partition)
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
        """Broadcast index seek to all workers; return first non-empty result."""
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
        for w in range(self.num_workers):
            ipc.send_batch(
                self.worker_fds[w], target_id, batch,
                schema=schema, flags=ipc.FLAG_DDL_SYNC,
            )
        for w in range(self.num_workers):
            payload = ipc.receive_payload(self.worker_fds[w])
            if payload.status != 0:
                err = payload.error_msg
                payload.close()
                raise errors.StorageError(
                    "Worker %d DDL sync error: %s" % (w, err)
                )
            payload.close()

        log.debug(
            "broadcast_ddl tid=" + str(target_id)
            + " rows=" + str(batch.length())
        )

    def check_fk_batch(self, target_id, check_batch, schema):
        """Check PK existence across workers. Returns True if any key is missing."""
        sub_batches = self._split_batch_by_pk(check_batch, schema)

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
