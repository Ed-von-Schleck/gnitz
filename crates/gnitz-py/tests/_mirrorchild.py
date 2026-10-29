"""The mirroring-client bodies that have to run in an interpreter of their own
(see `_childproc`): `python -m _mirrorchild <case> <copy dir> <target> <schema>`.

A failed assertion, a `SystemExit` with a message and a Rust abort all exit
non-zero.
"""
import sys

import gnitz
from _feedviews import agree, churn
from _read import bag, scanned


def erase(base, target, sn):
    """An armed ingest seam erases the copy it was applying to; the store and the
    process both live on."""
    m, plain = gnitz.connect(target, schema=sn), gnitz.connect(target, schema=sn)
    m.mirror_at(base)
    try:
        m.mirror_view("f")
        raise SystemExit("the armed seam must fail the bootstrap ingest")
    except gnitz.GnitzMirrorPoisonedError:
        raise SystemExit("one copy's fault must not poison the store")
    except gnitz.GnitzError:
        pass
    assert m.mirror_poisoned is None, "the store stays usable"

    # Intact, so the calls that span the store still answer.
    m.sync()
    m.checkpoint()

    # The bootstrap never finished, so this view has no valid copy, and a read of
    # one is delegated rather than refused.
    [vid] = m.mirrored_ids()
    assert m.cursor(vid) is None
    agree(m, plain, "SELECT * FROM f", local=False)
    assert bag(scanned(m, "f")) == bag(scanned(plain, "f")), "and so is a scan of it"
    agree(m, plain, "SELECT * FROM t", local=False)

    # The copy goes, the directory is released, and the client can attach again;
    # reopening `base` from a second client checks the lock came back.
    m.close_mirror()
    plain.mirror_at(base)
    plain.close()
    m.mirror_at(base)
    m.close()


def panic(base, target, sn):
    """A panic inside the apply poisons a copy that is still gated in,
    and every read that would have come off it is refused — including the one
    that arrives through the SQL layer, whose own error channel would otherwise
    flatten the poison into a plain GnitzError."""
    m, plain = gnitz.connect(target, schema=sn), gnitz.connect(target, schema=sn)
    m.mirror_at(base)
    vid = m.mirror_view("f").view_id
    f_schema = m.resolve_table("f")[1]
    assert m.cursor(vid) is not None, "the bootstrap succeeds; the seam fires on a poll"

    churn(m, 61, 120)
    try:
        m.sync()
        raise SystemExit("the armed seam must panic inside the apply")
    except BaseException as e:  # pyo3 raises PanicException, a BaseException
        assert "panic" in type(e).__name__.lower(), f"unexpected {type(e).__name__}: {e}"
    assert m.mirror_poisoned is not None, "the unwind through the store's lock poisons it"

    # The copy is still gated in, so these are reads it would have answered.
    assert m.cursor(vid) is not None
    for call in (lambda: m.execute_sql("SELECT * FROM f"),
                 lambda: m.scan(vid, f_schema)):
        try:
            call()
            raise SystemExit("a poisoned copy must refuse the reads it would answer")
        except gnitz.GnitzMirrorPoisonedError:
            pass

    # And a relation the copy does not hold is untouched.
    agree(m, plain, "SELECT * FROM t", local=False)
    m.close_mirror()
    m.close()


if __name__ == "__main__":
    {"erase": erase, "panic": panic}[sys.argv[1]](*sys.argv[2:])
