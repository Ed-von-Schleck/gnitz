"""Locating the expression-register cap instead of pinning it, and the one
predicate that decides whether a rejection is usable.

The cap is a constant that may move, so a test finds the boundary rather than
naming it. What must not move is the *message*: a rejection has to name the limit
the author can act on. A leaked `TooManyRegs(66)` names an internal enum and
tells them nothing, which is why it is asserted against here rather than
tolerated as "some error".
"""

import pytest


def conjunct_ladder(col: str, n: int) -> str:
    """`n` conjuncts, each true exactly for `col = 1` and none sharing an
    instruction the builder could fold: `col * k < k + 1` over distinct odd `k`.

    Folding is what makes a naive ladder useless for reaching the cap — with a
    shared subexpression the builder spends one register for the whole chain.
    """
    return " AND ".join(f"{col} * {2 * i + 1} < {2 * i + 2}" for i in range(n))


def names_a_cap(exc) -> bool:
    """The rejection names the register limit or the column limit — the two caps
    a definition can cross — and does not leak the enum that carries it."""
    msg = str(exc)
    assert "TooManyRegs" not in msg, f"internal enum leaked to the client: {msg}"
    return "registers" in msg or "MAX_COLUMNS" in msg


def first_rejected(run, ns):
    """The first `n` in `ns` at which `run(n)` is rejected for a cap, every
    earlier one having been served. The rejection must name the limit."""
    for n in ns:
        try:
            run(n)
        except Exception as e:
            assert names_a_cap(e), f"n={n}: the message must name the limit, got: {e}"
            assert n > ns.start, f"n={n}: the smallest shape must be servable"
            return n
    pytest.fail(f"no n in {ns} crossed the register cap")
