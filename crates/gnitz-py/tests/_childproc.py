"""Running a module of this directory in a fresh interpreter.

For what cannot be staged inside the process running the assertions: a fault
seam is a per-process latch, so arming one in the pytest interpreter would
contaminate every sibling test; a host crash means `SIGKILL` with no destructor;
and a syscall count or an exit status is a property of a whole process.

The child is a real module (`python -m <module> <case> <args...>`) rather than a
source string: inside a literal its assertions are invisible to linting and
formatting, and a typo surfaces only as a missing sentinel.
"""
import os
import subprocess
import sys
import time

from _paths import REPO_ROOT

_TESTS_DIR = str(REPO_ROOT / "crates" / "gnitz-py" / "tests")

# What a child prints once it has reached the state its parent is waiting for.
READY = "CHILD-READY"


def argv(module, *args):
    return [sys.executable, "-m", module, *map(str, args)]


def env(extra=None):
    """This process's environment, with the child able to import the helpers
    beside it, plus `extra`."""
    e = dict(os.environ)
    e["PYTHONPATH"] = os.pathsep.join([_TESTS_DIR, e.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    e.update(extra or {})
    return e


def spawn(module, *args, extra_env=None):
    """Start the child and hand back the live process.

    For a child that prints `READY` and then blocks: the parent reads that line
    while the child is still running, and sends the signal itself.
    """
    return subprocess.Popen(
        argv(module, *args),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=env(extra_env),
    )


def run(module, *args, extra_env=None, timeout=180):
    """Run the child to completion, asserting it exited cleanly."""
    p = spawn(module, *args, extra_env=extra_env)
    out, err = p.communicate(timeout=timeout)
    assert p.returncode == 0, f"child exited {p.returncode}\nstdout:\n{out}\nstderr:\n{err}"


def wait_for_ready(proc, timeout=180):
    """Block until the child prints `READY`.

    Raises if the child exits first — its stderr is in the message, since a child
    that died has already said why.
    """
    lines = []
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = proc.stdout.readline()
        if line == "":
            raise AssertionError(
                f"child exited before printing {READY!r}\n"
                f"stdout:\n{''.join(lines)}\nstderr:\n{proc.stderr.read()}"
            )
        lines.append(line)
        if READY in line:
            return
    raise AssertionError(f"child never printed {READY!r}\nstdout so far:\n{''.join(lines)}")
