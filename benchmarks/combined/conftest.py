"""Per-test fixtures for combined benchmarks."""

from __future__ import annotations

import itertools
import os

import pytest
import gnitz

from helpers.datagen import SCALES
from helpers.timing import BenchTimer, record_result

_counter = itertools.count()


@pytest.fixture
def client(socket_path):
    with gnitz.connect(socket_path) as conn:
        yield conn


@pytest.fixture
def schema_name(client):
    sn = f"comb_{next(_counter)}_{os.getpid()}"
    client.create_schema(sn)
    yield sn
    try:
        client.drop_schema(sn)
    except Exception:
        pass


@pytest.fixture
def bench_timer(request, scale_mode):
    category = request.node.module.__name__.rsplit(".", 1)[-1]
    timer = BenchTimer(request.node.name, f"combined/{category}")
    yield timer
    record_result(timer.result())


@pytest.fixture
def scale(scale_mode):
    return SCALES[scale_mode]
