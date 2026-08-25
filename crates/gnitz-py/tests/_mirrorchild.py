"""The mirror bodies that have to run in an interpreter of their own.

Real module rather than a source string passed to `python -c`: these are sixty
lines of assertions, and inside a literal they are invisible to linting and
formatting and a typo surfaces only as a missing sentinel.

Each case reads `MIRROR_TARGET`, `MIRROR_DIR` and `MIRROR_SCHEMA` from its
environment and prints `SENTINEL` once it has run every assertion. **The parent
asserts on the sentinel**, never on the exit code alone: a child that dies before
its assertions still exits 0.
"""
import os
import sys
import time

import gnitz

SENTINEL = "MIRROR-CHILD-OK"
READY = "MIRROR-CHILD-READY"


def _env():
    return os.environ["MIRROR_DIR"], os.environ["MIRROR_TARGET"], os.environ["MIRROR_SCHEMA"]


def poison():
    """An armed ingest seam poisons the handle, and the process lives on."""
    base, target, sn = _env()
    m = gnitz.Mirror(base, target)
    try:
        m.mirror_view(sn, "f")
        raise SystemExit("the armed seam must fail the bootstrap ingest")
    except gnitz.GnitzMirrorPoisonedError as e:
        assert isinstance(e, gnitz.GnitzError), "the poison class must stay catchable as GnitzError"
    assert m.poisoned is not None, "the handle reports what poisoned it"

    # Every call that touches a copy is refused with the same class — including
    # the two that reach the copy through the SQL layer, whose own error channel
    # would otherwise flatten the poison into a plain GnitzError.
    for call in (lambda: m.poll(),
                 lambda: m.checkpoint(),
                 lambda: m.forget_view(0),
                 lambda: m.scan(0),
                 lambda: m.execute_sql("SELECT * FROM f", schema_name=sn)):
        try:
            call()
            raise SystemExit("a poisoned handle must refuse this call")
        except gnitz.GnitzMirrorPoisonedError:
            pass

    # A poisoned handle can still be diagnosed and released; a close() that
    # raised would hold its data directory for the life of the interpreter.
    # Reopening `base` itself is what proves the lock came back.
    assert m.mirrors(0) is False
    m.close()
    gnitz.Mirror(base, target).close()


def crash():
    """Poll past a checkpoint, then block so the parent can SIGKILL us.

    The rounds applied after the checkpoint are what the kill loses, and what
    the parent's reopen must recover from the feed rather than double.
    """
    from _feedviews import _churn

    base, target, sn = _env()
    client = gnitz.connect(target)
    m = gnitz.Mirror(base, target)
    m.mirror_view(sn, "f")
    client.execute_sql("SELECT COUNT(*) AS n FROM f", schema_name=sn)
    m.poll()
    m.checkpoint()

    _churn(client, sn, 61, 120)
    client.execute_sql("SELECT COUNT(*) AS n FROM f", schema_name=sn)
    m.poll()

    print(READY, flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    {"poison": poison, "crash": crash}[sys.argv[1]]()
    print(SENTINEL, flush=True)
