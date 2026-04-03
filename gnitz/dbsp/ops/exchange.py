# gnitz/dbsp/ops/exchange.py
#
# Partition assignment for multi-worker mode.
# All repartition/scatter operators and the routing cache have moved to Rust
# (master.rs / ops/exchange.rs).


class PartitionAssignment(object):
    """Maps 256 partitions to N workers. Worker w owns [start, end).

    The partition→worker mapping is computed via Rust (same hash as the
    repartition operators).  Only `num_workers` is stored; all queries are
    O(1) arithmetic.
    """

    _immutable_fields_ = ["num_workers"]

    def __init__(self, num_workers):
        self.num_workers = num_workers

    def worker_for_partition(self, partition_idx):
        """Returns the worker ID that owns the given partition."""
        chunk = 256 // self.num_workers
        w = partition_idx // chunk
        if w >= self.num_workers:
            w = self.num_workers - 1
        return w

    def range_for_worker(self, worker_id):
        """Returns (start, end) partition range for the given worker."""
        chunk = 256 // self.num_workers
        start = worker_id * chunk
        if worker_id == self.num_workers - 1:
            end = 256
        else:
            end = (worker_id + 1) * chunk
        return start, end
