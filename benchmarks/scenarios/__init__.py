"""Every scenario `bench.py` runs, in the order a full run takes them."""

from . import client, relational, shapes, workloads

ALL = [*shapes.SCENARIOS, *relational.SCENARIOS, *workloads.SCENARIOS, *client.SCENARIOS]
BY_NAME = {s.name: s for s in ALL}
assert len(BY_NAME) == len(ALL), "scenario names are unique"
