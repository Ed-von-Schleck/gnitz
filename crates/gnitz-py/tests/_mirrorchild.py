"""The mirroring-client bodies that have to run in an interpreter of their own.

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
from _feedviews import _rows

SENTINEL = "MIRROR-CHILD-OK"
READY = "MIRROR-CHILD-READY"


def _env():
    return os.environ["MIRROR_DIR"], os.environ["MIRROR_TARGET"], os.environ["MIRROR_SCHEMA"]


def poison():
    """An armed ingest seam poisons the copy, and the process lives on."""
    base, target, sn = _env()
    m = gnitz.connect(target)
    m.mirror_at(base)
    try:
        m.mirror_view(sn, "f")
        raise SystemExit("the armed seam must fail the bootstrap ingest")
    except gnitz.GnitzMirrorPoisonedError as e:
        assert isinstance(e, gnitz.GnitzError), "the poison class must stay catchable as GnitzError"
    assert m.mirror_poisoned is not None, "the client reports what poisoned its copy"

    # Every call that touches a copy is refused with the same class.
    for call in (lambda: m.poll(),
                 lambda: m.checkpoint(),
                 lambda: m.forget_view(0)):
        try:
            call()
            raise SystemExit("a poisoned copy must refuse this call")
        except gnitz.GnitzMirrorPoisonedError:
            pass

    # The bootstrap never finished, so this view has no valid copy — and a read
    # of one is *delegated*, not refused. That is the whole point of gating on
    # what the copy holds rather than on whether a store is attached: a poisoned
    # copy must not take the connection's own reads down with it.
    [vid] = m.mirrored_ids()
    assert m.mirrors(vid) is False
    assert _rows(m.execute_sql("SELECT * FROM f", schema_name=sn)), (
        "a read the copy cannot answer is delegated, poisoned store or not"
    )
    assert len(m.scan(vid)) > 0, "and so is a scan of it"
    assert _rows(m.execute_sql("SELECT * FROM t", schema_name=sn))

    # `close_mirror` is the only way out of a poison, and it keeps the
    # connection: the copy goes, the directory is released, and the client can
    # attach again. Reopening `base` from a second client is what proves the lock
    # came back.
    m.close_mirror()
    assert m.mirror_poisoned is None, "the poison went with the store"
    second = gnitz.connect(target)
    second.mirror_at(base)
    second.close()
    m.mirror_at(base)
    m.close()


def panic():
    """A panic inside the guarded apply poisons a copy that is still gated in,
    and every read that would have come off it is refused — including the one
    that arrives through the SQL layer, whose own error channel would otherwise
    flatten the poison into a plain GnitzError."""
    from _feedviews import _churn

    base, target, sn = _env()
    m = gnitz.connect(target)
    m.mirror_at(base)
    vid = m.mirror_view(sn, "f").view_id
    assert m.mirrors(vid), "the bootstrap succeeds; the seam fires on a poll"

    _churn(m, sn, 61, 120)
    m.execute_sql("SELECT COUNT(*) AS n FROM f", schema_name=sn)
    try:
        m.poll()
        raise SystemExit("the armed seam must panic inside the guarded apply")
    except BaseException as e:  # pyo3 raises PanicException, a BaseException
        assert "panic" in type(e).__name__.lower(), f"unexpected {type(e).__name__}: {e}"
    assert m.mirror_poisoned is not None, "the guard poisons before the unwind continues"

    # The copy is still gated in, so these are reads it would have answered.
    assert m.mirrors(vid)
    for call in (lambda: m.execute_sql("SELECT * FROM f", schema_name=sn),
                 lambda: m.scan(vid)):
        try:
            call()
            raise SystemExit("a poisoned copy must refuse the reads it would answer")
        except gnitz.GnitzMirrorPoisonedError:
            pass

    # And a relation the copy does not hold is untouched.
    assert _rows(m.execute_sql("SELECT * FROM t", schema_name=sn))
    m.close_mirror()
    m.close()


def crash():
    """Poll past a checkpoint, then block so the parent can SIGKILL us.

    The rounds applied after the checkpoint are what the kill loses, and what
    the parent's reopen must recover from the feed rather than double.
    """
    from _feedviews import _churn

    base, target, sn = _env()
    m = gnitz.connect(target)
    m.mirror_at(base)
    m.mirror_view(sn, "f")
    m.execute_sql("SELECT COUNT(*) AS n FROM f", schema_name=sn)
    m.poll()
    m.checkpoint()

    _churn(m, sn, 61, 120)
    m.execute_sql("SELECT COUNT(*) AS n FROM f", schema_name=sn)
    m.poll()

    print(READY, flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    {"poison": poison, "panic": panic, "crash": crash}[sys.argv[1]]()
    print(SENTINEL, flush=True)
