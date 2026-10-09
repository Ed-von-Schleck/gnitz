"""The benchmark harness. It boots servers the way the E2E suite does, so that
suite's server helper is on the path for every module here."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "crates/gnitz-py/tests"))
