# gnitz/server — Multi-process server architecture
#
# Architecture Contracts
# =====================
# Violations produce silent data loss or file-lock errors that are
# difficult to debug. Maintain these invariants when modifying this package.
#
# 1. WAL Ownership
#    Each WAL file is owned by exactly one process (exclusive flock() on open).
#    - System-table WALs: owned by master. Workers must never call
#      catalog_ingest or catalog_flush on system tables via the WAL path.
#      Use ddl_sync / raw_store_ingest (memonly) instead.
#    - User-table WALs: owned by the worker holding that partition.
#      Master creates no user-table partitions (active partition range = empty).
#    - Symptom: StorageError("WAL file is locked by another process")
#
# 2. Partition Range
#    CatalogEngine active partitions must be set BEFORE any DDL fires that
#    creates user tables.
#    - After os.fork(), each child sets the range before processing DDL.
#    - Master sets (0, 0) — creates no user-table partitions.
#    - Symptom: multiple processes create the same partition dirs, flock() conflicts.
#
# 3. DDL Sync Ordering
#    Master must not hold user-table partition WAL locks when broadcasting DDL.
#    The master's active partition range is empty immediately after fork.
#    Workers receive DDL and create partitions within their assigned range.
#
# 4. Error Propagation in IPC
#    Every receive_payload() response must check payload.status. A non-zero
#    status means the remote encountered an error. fan_out_push, fan_out_scan,
#    and broadcast_ddl all raise StorageError with the worker's error message.
#    Silently ignoring error responses was the original bug that masked the
#    WAL-lock issue — master saw empty scan results instead of error messages.
