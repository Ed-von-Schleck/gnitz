"""Running one of `_mirrorchild.py`'s cases in a fresh interpreter.

Two cases need one and no more. A fault seam is a per-process latch, so arming
one in the pytest interpreter would contaminate every sibling test; and a host
crash means `SIGKILL` with no destructor and therefore no exit checkpoint, which
cannot be staged inside the process running the assertions.

The child reads `MIRROR_TARGET`, `MIRROR_DIR` and `MIRROR_SCHEMA` out of its
environment, plus whatever else the caller passes. **The assertion is always on
the sentinel**, never on the exit code alone: a child that dies before its
assertions still exits 0.
"""
import os
import subprocess
import sys
import time

from _mirrorchild import READY, SENTINEL
from _paths import REPO_ROOT

_TESTS_DIR = str(REPO_ROOT / "crates" / "gnitz-py" / "tests")


def _child_env(base_dir, target, schema, env):
    e = dict(os.environ)
    # So the child can import `_mirrorchild` and the helpers beside it.
    e["PYTHONPATH"] = os.pathsep.join([_TESTS_DIR, e.get("PYTHONPATH", "")]).rstrip(os.pathsep)
    e["MIRROR_TARGET"] = target
    e["MIRROR_DIR"] = base_dir
    e["MIRROR_SCHEMA"] = schema
    e.update(env or {})
    return e


def spawn(case, base_dir, target, schema, env=None):
    """Start `_mirrorchild.<case>` in a fresh interpreter and hand back the live
    process.

    For a child that prints `READY` and then blocks: the parent reads that line
    while the child is still running, and sends the signal itself.
    """
    return subprocess.Popen(
        [sys.executable, "-m", "_mirrorchild", case],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=_child_env(base_dir, target, schema, env),
    )


def run(case, base_dir, target, schema, env=None, timeout=180):
    """Run `_mirrorchild.<case>` to completion, asserting it reached its
    sentinel."""
    p = spawn(case, base_dir, target, schema, env)
    out, err = p.communicate(timeout=timeout)
    assert SENTINEL in out, f"child did not reach its sentinel (rc={p.returncode})\nstdout:\n{out}\nstderr:\n{err}"
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
