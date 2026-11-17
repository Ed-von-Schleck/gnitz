"""Pytest fixtures over a gnitz-server of the test session's own. Enable them
in a ``conftest.py``::

    pytest_plugins = ["gnitz.testing"]

- ``gnitz_server``: a `gnitz.LocalServer` the session shares, its data removed
  when the session ends. It runs the server `gnitz.find_server` finds, which
  ``pip install "gnitz[server]"`` provides.
- ``gnitz_target``: that server's target, what `gnitz.connect` takes.
- ``gnitz_client``: a connection whose unqualified names resolve in a schema of
  the test's own, dropped with everything in it when the test ends.

`fresh_schema` is what ``gnitz_client`` is built on, for fixtures of a
suite's own.
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Iterator

import pytest

import gnitz

__all__ = ["fresh_schema", "gnitz_client", "gnitz_server", "gnitz_target"]


@pytest.fixture(scope="session")
def gnitz_server() -> Iterator[gnitz.LocalServer]:
    with gnitz.local_server() as server:
        yield server


@pytest.fixture(scope="session")
def gnitz_target(gnitz_server: gnitz.LocalServer) -> str:
    return gnitz_server.target


@contextlib.contextmanager
def fresh_schema(target: str, name: str) -> Iterator[gnitz.GnitzClient]:
    """A connection to `target` whose unqualified names resolve in a new
    schema `name`, dropped with everything in it on exit.

    The drop is ``DROP SCHEMA ... CASCADE``: one atomic DDL bundle that
    retracts every view, then every table, then the schema row, so a test
    never tears its own objects down. The drop is not wrapped: a schema that
    refuses to drop is a finding, and swallowing it leaks the objects into the
    catalog every later test shares.
    """
    with gnitz.connect(target, schema=name) as conn:
        conn.execute_sql(f'CREATE SCHEMA "{name}"')
        yield conn
        conn.execute_sql(f'DROP SCHEMA "{name}" CASCADE')


@pytest.fixture
def gnitz_client(gnitz_target: str) -> Iterator[gnitz.GnitzClient]:
    with fresh_schema(gnitz_target, f"test_{uuid.uuid4().hex[:12]}") as conn:
        yield conn
