"""Running a module of this directory in a fresh interpreter.

For what cannot be staged inside the process running the assertions: a fault
seam is a per-process latch, so arming one in the pytest interpreter would
contaminate every sibling test; and a syscall count or an exit status is a
property of a whole process.

The child is a real module (`python -m <module> <case> <args...>`) rather than a
source string: inside a literal its assertions are invisible to linting and
formatting, and a typo surfaces only as a missing sentinel.
"""
import os
import subprocess
import sys

from _paths import REPO_ROOT

_TESTS_DIR = str(REPO_ROOT / "crates" / "gnitz-py" / "tests")


def run(module, *args, prefix=(), extra_env=None, rc=0, timeout=180):
    """Run the child to completion under `prefix` — a command it runs inside,
    such as a tracer — assert it exited `rc`, and return its stdout and stderr.

    Its environment is this process's, with the helpers beside it importable,
    plus `extra_env`."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([_TESTS_DIR, env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    env.update(extra_env or {})
    done = subprocess.run(
        [*prefix, sys.executable, "-m", module, *map(str, args)],
        capture_output=True, text=True, env=env, timeout=timeout,
    )
    assert done.returncode == rc, (
        f"child exited {done.returncode}, not {rc}\nstdout:\n{done.stdout}\nstderr:\n{done.stderr}")
    return done.stdout, done.stderr
