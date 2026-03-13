# GnitzDB Developer Guide

## Multi-worker invariant checklist

When adding a new server-side constraint, validation rule, or invariant:

- [ ] Is it enforced in `engine.py` / `registry.py` for the master DDL path?
- [ ] Is it enforced in `worker.py` `_handle_push` for the worker DML path?
- [ ] Does it require distributed coordination (like FK validation)?
      If so, extend `validate_fk_distributed` / `check_fk_batch`.
- [ ] Does it need to fire on every worker even for empty batches
      (e.g. exchange barrier participation)?
- [ ] Is there a multi-worker E2E test in `test_workers.py` covering it?
- [ ] Is there a unit test in `master_worker_test.py` covering the IPC path?

## Running E2E tests

**Always run the E2E suite with multiple workers.** Single-worker mode skips
exchange/fanout paths and will miss bugs in distributed execution.

```bash
cd py_client && GNITZ_WORKERS=4 uv run pytest tests/ -x --tb=short
```

The Makefile `e2e` target already sets `GNITZ_WORKERS=4`. Never run
`uv run pytest tests/` without the `GNITZ_WORKERS` variable set.
