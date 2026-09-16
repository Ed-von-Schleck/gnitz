"""The mirroring-client bodies that have to run in an interpreter of their own.

Real module rather than a source string passed to `python -c`: these are sixty
lines of assertions, and inside a literal they are invisible to linting and
formatting and a typo surfaces only as a missing sentinel.

Each case reads `MIRROR_TARGET`, `MIRROR_DIR` and `MIRROR_SCHEMA` from its
environment; a failed assertion, a `SystemExit` with a message and a Rust abort
all exit non-zero.
"""
import os
import sys
import time

import gnitz
from _read import rows

READY = "MIRROR-CHILD-READY"


def _env():
    return os.environ["MIRROR_DIR"], os.environ["MIRROR_TARGET"], os.environ["MIRROR_SCHEMA"]


def erase():
    """An armed ingest seam erases the copy it was applying to; the store and the
    process both live on."""
    base, target, sn = _env()
    m = gnitz.connect(target)
    m.mirror_at(base)
    try:
        m.mirror_view(sn, "f")
        raise SystemExit("the armed seam must fail the bootstrap ingest")
    except gnitz.GnitzMirrorPoisonedError:
        raise SystemExit("one copy's fault must not poison the store")
    except gnitz.GnitzError:
        pass
    assert m.mirror_poisoned is None, "the store stays usable"

    # Intact, so the calls that span the store still answer.
    m.poll()
    m.checkpoint()

    # The bootstrap never finished, so this view has no valid copy, and a read of
    # one is delegated rather than refused.
    [vid] = m.mirrored_ids()
    assert m.mirrors(vid) is False
    assert rows(m, sn, "SELECT * FROM f"), "a read the copy cannot answer is delegated"
    assert len(m.scan(vid)) > 0, "and so is a scan of it"
    assert rows(m, sn, "SELECT * FROM t")

    # The copy goes, the directory is released, and the client can attach again;
    # reopening `base` from a second client checks the lock came back.
    m.close_mirror()
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
    assert rows(m, sn, "SELECT * FROM t")
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
    {"erase": erase, "panic": panic, "crash": crash}[sys.argv[1]]()
