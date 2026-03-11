# gnitz/server/master.py
#
# Master-side dispatcher: fans out push/scan operations to worker processes
# and collects responses.

import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz import log
from gnitz.server import ipc
from gnitz.core import errors
from gnitz.core.batch import ArenaZSetBatch, ZSetBatch
from gnitz.storage.partitioned_table import _partition_for_key

WNOHANG = 1


class PartitionAssignment(object):
    """Maps 256 partitions to N workers. Worker w owns [start, end)."""

    _immutable_fields_ = ["num_workers", "starts[*]", "ends[*]"]

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

    def worker_for_partition(self, partition_idx):
        """Returns the worker ID that owns the given partition."""
        for w in range(self.num_workers):
            if partition_idx >= self.starts[w] and partition_idx < self.ends[w]:
                return w
        return self.num_workers - 1

    def range_for_worker(self, worker_id):
        """Returns (start, end) partition range for the given worker."""
        return self.starts[worker_id], self.ends[worker_id]


class MasterDispatcher(object):
    """
    Fans out push/scan operations to N workers over socketpair fds.
    Collects ACKs/responses synchronously.
    """

    _immutable_fields_ = ["num_workers", "worker_fds[*]", "assignment"]

    def __init__(self, num_workers, worker_fds, worker_pids, assignment):
        self.num_workers = num_workers
        self.worker_fds = worker_fds
        self.worker_pids = worker_pids
        self.assignment = assignment

    def fan_out_push(self, target_id, batch, schema):
        """Split batch by worker partition, send sub-batches, collect ACKs."""
        n = batch.length()
        sub_batches = [None] * self.num_workers

        for i in range(n):
            pk_lo = r_uint64(batch._read_pk_lo(i))
            pk_hi = r_uint64(batch._read_pk_hi(i))
            p = _partition_for_key(pk_lo, pk_hi)
            w = self.assignment.worker_for_partition(p)
            if sub_batches[w] is None:
                sub_batches[w] = ArenaZSetBatch(schema)
            sub_batches[w]._direct_append_row(batch, i, batch.get_weight(i))

        sent_workers = newlist_hint(self.num_workers)
        for w in range(self.num_workers):
            sb = sub_batches[w]
            if sb is not None:
                ipc.send_batch(
                    self.worker_fds[w], target_id, sb, schema=schema
                )
                sb.free()
                sent_workers.append(w)

        for i in range(len(sent_workers)):
            w = sent_workers[i]
            payload = ipc.receive_payload(self.worker_fds[w])
            if payload.status != 0:
                err = payload.error_msg
                payload.close()
                raise errors.StorageError(
                    "Worker %d push error: %s" % (w, err)
                )
            payload.close()

        log.debug(
            "fan_out_push tid=" + str(target_id)
            + " rows=" + str(n)
            + " workers=" + str(len(sent_workers))
        )

    def fan_out_scan(self, target_id, schema):
        """Send scan to all workers, concatenate result batches."""
        for w in range(self.num_workers):
            ipc.send_batch(self.worker_fds[w], target_id, None, schema=schema)

        result = ZSetBatch(schema)
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
