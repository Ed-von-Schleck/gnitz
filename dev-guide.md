# GnitzDB Developer Guide

## Multi-worker invariant checklist

When adding a new server-side constraint, validation rule, or invariant:

- [ ] Is it enforced in `engine.py` / `registry.py` for the master DDL path?
- [ ] Is it in `validate_fk_inline` (single-process) or `validate_fk_distributed`
      (multi-worker)? FK and partition-global constraints belong at the request layer,
      NOT inside `ingest_to_family`.
- [ ] Does it require distributed coordination (like FK validation)?
      If so, extend `validate_fk_distributed` / `check_fk_batch`.
- [ ] Does it need to fire on every worker even for empty batches
      (e.g. exchange barrier participation)?
- [ ] Any change touching `worker.py`, `registry.py` ingest path, or `ipc.py`:
      run `make e2e`.
- [ ] Is there a multi-worker E2E test in `rust_client/gnitz-py/tests/test_workers.py`
      covering it?
- [ ] Is there a unit test in `master_worker_test.py` covering the IPC path?
- [ ] Does the feature use `hash_row_by_columns` for exchange routing across
      tables where the same logical value may have different column types
      (e.g., PK column = U64 vs regular column = I64)? The hash must produce
      identical partitions regardless of type for co-partitioning correctness.

## Running E2E tests

**Always run the E2E suite with multiple workers.** Single-worker mode skips
exchange/fanout paths and will miss bugs in distributed execution.

```bash
cd rust_client/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/ -v --tb=short
```

The Makefile `e2e` target already sets `GNITZ_WORKERS=4`. Never run
`uv run pytest tests/` without the `GNITZ_WORKERS` variable set.

## Debug logging during development

To enable verbose logging during E2E tests:

```bash
GNITZ_WORKERS=4 GNITZ_LOG_LEVEL=debug uv run pytest tests/test_joins.py -x
```

Master log: `~/git/gnitz/tmp/server_debug.log`
Worker logs (always preserved from last run): `~/git/gnitz/tmp/last_worker_N.log`

Log format: `<epoch>.<ms> <process> <level> <message>`
- Process tags: `M` (master), `W0`-`WN` (workers)

To add temporary debug logging in RPython code, use `log.debug()` —
never raw `os.write(2, ...)`. This ensures timestamps, process identity,
and level-gating. Remove debug calls before committing.

To watch all worker logs live:

```bash
tail -f ~/git/gnitz/tmp/gnitz_*/worker_*.log
```
