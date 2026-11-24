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


def _argv(module, args, prefix=()):
    return [*prefix, sys.executable, "-m", module, *map(str, args)]


def _env(extra_env):
    """This process's environment, with the helpers beside it importable, plus
    `extra_env`."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([_TESTS_DIR, env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    env.update(extra_env or {})
    return env


def run(module, *args, prefix=(), extra_env=None, rc=0, timeout=180):
    """Run the child to completion under `prefix` — a command it runs inside,
    such as a tracer — assert it exited `rc`, and return its stdout and stderr."""
    done = subprocess.run(
        _argv(module, args, prefix), capture_output=True, text=True, env=_env(extra_env),
        timeout=timeout,
    )
    assert done.returncode == rc, (
        f"child exited {done.returncode}, not {rc}\nstdout:\n{done.stdout}\nstderr:\n{done.stderr}")
    return done.stdout, done.stderr


def start(module, *args):
    """Start the child with its stdout piped, for a test that acts on it while
    it runs — reads a line it prints, signals it — and return the `Popen`."""
    return subprocess.Popen(_argv(module, args), stdout=subprocess.PIPE, text=True, env=_env(None))
