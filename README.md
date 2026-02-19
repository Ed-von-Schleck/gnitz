# GnitzDB

An incremental, persistent Z-Set database engine written in RPython.

Implements the [DBSP](https://arxiv.org/abs/2203.16684) computation model: queries are compiled into streaming circuits that process only deltas, not full re-evaluations. The storage layer is columnar and memory-mapped, with a skip-list MemTable, write-ahead log, and an N-way merge compactor.

## Status

Early implementation. The core storage engine (MemTable, WAL, shards, compaction, manifest) is complete and tested. The VM (filter, map, reduce, join, distinct) is implemented but not yet fully exercised end-to-end. The architecture has four layers with strict unidirectional imports:

```
core  ←  backend  ←  storage
              ↑
             vm
```

`backend/` is a narrow interface layer (~3 files) that decouples the VM from the columnar storage implementation, making alternative backends possible without touching VM code.

## Requirements

PyPy2 and a checkout of the [PyPy source tree](https://pypy.org/download.html) for the RPython toolchain.

```bash
export PYPY_PATH=../pypy
export PYTHONPATH=.:$PYPY_PATH
pypy2 -m unittest discover tests/
```
