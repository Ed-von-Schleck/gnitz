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

## Running E2E tests

**Always run the E2E suite with multiple workers.** Single-worker mode skips
exchange/fanout paths and will miss bugs in distributed execution.

```bash
cd rust_client/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/ -v --tb=short
```

The Makefile `e2e` target already sets `GNITZ_WORKERS=4`. Never run
`uv run pytest tests/` without the `GNITZ_WORKERS` variable set.

## Debugging workers

Worker stdout/stderr goes to `<DATA_DIR>/worker_N.log`, not the terminal.
The server prints the log paths at startup.
To watch all worker logs live:

```bash
tail -f ~/git/gnitz/tmp/gnitz_*/worker_*.log
```
