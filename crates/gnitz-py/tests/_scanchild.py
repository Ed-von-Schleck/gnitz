"""A full scan of a `KV` relation in an interpreter of its own, for a test that
stops the reader mid-reply (see `_childproc`):
`python -m _scanchild <target> <relation>`.

Prints `go` just before it sends the scan, then the row count it read.
"""
import sys

import gnitz
from _schemas import KV

if __name__ == "__main__":
    target, name = sys.argv[1:]
    with gnitz.connect(target) as c:
        tid, _ = c.resolve_table(name)
        print("go", flush=True)
        print(len(c.scan(tid, KV)), flush=True)
