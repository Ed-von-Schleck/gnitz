"""Unique name fragments for the objects a test creates.

Every test in the suite runs against one shared server, so a schema or table
name has to be unique across the whole session. A random draw is a birthday bet
the suite loses: ~1900 tests over a six-digit space collide about half the time,
and the loser fails with `Schema already exists`. A process-wide counter cannot
collide at all.
"""

import itertools

_COUNTER = itertools.count(100_000)


def uid() -> str:
    """A six-digit name fragment no other test in this process has used."""
    return str(next(_COUNTER))
