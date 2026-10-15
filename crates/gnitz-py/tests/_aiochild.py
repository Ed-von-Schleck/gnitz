"""The `gnitz.aio` bodies whose subject is a whole process — its syscall bill,
its exit status — and so run in an interpreter of their own (see `_childproc`):
`python -m _aiochild <case> <target> <args...>`.
"""
import asyncio
import sys

from gnitz import aio
from _schemas import SCHEMAS, kv


def syscalls(target, tid, n, mode):
    """`n` single-row pushes into `tid`, awaited one at a time (`loop`) or
    gathered."""
    tid, n = int(tid), int(n)

    def batch(pk):
        return kv((pk, pk))

    async def main():
        async with aio.connect(target) as conn:
            # One warm-up push, so the measured region is the same shape for every
            # mode and for the n=0 baseline.
            await conn.push(tid, batch(0))
            if mode == "loop":
                for i in range(n):
                    await conn.push(tid, batch(i + 1))
            elif n:
                await asyncio.gather(*[conn.push(tid, batch(i + 1)) for i in range(n)])

    asyncio.run(main())


def shutdown(target):
    """An exception escapes `asyncio.run` with work in flight."""
    async def main():
        conn = await aio.connect(target)
        conn.scan(*SCHEMAS)
        conn.scan(*SCHEMAS)
        raise RuntimeError("boom")

    asyncio.run(main())


if __name__ == "__main__":
    {"syscalls": syscalls, "shutdown": shutdown}[sys.argv[1]](*sys.argv[2:])
