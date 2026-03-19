# rpython_tests/helpers/jit_stub.py
#
# Stub JitDriver for test binaries compiled with --opt=jit.
#
# RPython's warmspot pass requires at least one jit_merge_point to be
# reachable from the translated entry point.  Production code satisfies this
# via gnitz/vm/interpreter.py.  Test binaries that do not exercise the VM
# would otherwise fail with "no jit_merge_point found!".
#
# Solution: a zero-iteration stub loop that is statically reachable but never
# executes at runtime (n starts at 0, loop condition n > 0 is immediately
# false).  Each test entry_point calls ensure_jit_reachable() once.
#
# The server binary is completely unaffected: gnitz/server/main.py does not
# import from rpython_tests.

from rpython.rlib import jit

_stub_driver = jit.JitDriver(greens=[], reds=['n'])

def _stub_loop(n):
    # n is a parameter, so the flowspace cannot constant-fold the condition
    # away — the loop body stays in the flow graph and warmspot finds the
    # merge point.  At runtime n is always 0, so the body never executes.
    while n > 0:
        _stub_driver.jit_merge_point(n=n)
        n -= 1

def ensure_jit_reachable():
    """Satisfy warmspot's merge-point scan.  The loop body never executes."""
    _stub_loop(0)
