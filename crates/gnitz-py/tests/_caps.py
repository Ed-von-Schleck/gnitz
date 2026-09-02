"""Locating the expression-register cap instead of pinning it."""

import pytest


def first_rejected(run, ns):
    """The first `n` in `ns` at which `run(n)` is rejected for the register cap,
    every earlier one having been served. The rejection must name the limit."""
    for n in ns:
        try:
            run(n)
        except Exception as e:
            assert "64" in str(e), f"n={n}: the message must name the limit, got: {e}"
            assert n > ns.start, f"n={n}: the smallest shape must be servable"
            return n
    pytest.fail(f"no n in {ns} crossed the register cap")
