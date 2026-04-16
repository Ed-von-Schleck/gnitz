"""Per-test fixtures for micro benchmarks: schema isolation, bench_timer."""

from __future__ import annotations

import itertools
import os

import pytest

from helpers.datagen import SCALES
from helpers.timing import BenchTimer, record_result

_counter = itertools.count()


@pytest.fixture
def schema_name(client):
    # Leaking state across the session-scoped server silently breaks later
    # tests with stale tick rows, so let drop_schema raise instead of
    # swallowing: server-side cascade already handles non-empty schemas.
    sn = f"bench_{next(_counter)}_{os.getpid()}"
    client.create_schema(sn)
    yield sn
    client.drop_schema(sn)


@pytest.fixture
def bench_timer(request, scale_mode):
    category = request.node.module.__name__.rsplit(".", 1)[-1]
    timer = BenchTimer(request.node.name, f"micro/{category}")
    yield timer
    record_result(timer.result())


@pytest.fixture
def scale(scale_mode):
    return SCALES[scale_mode]
