"""Write `seccomp.json`: Docker's default seccomp profile plus the io_uring
syscalls the server runs on.

Docker's default profile is the reference the other runtimes' defaults follow,
and since Docker 25 it denies `io_uring_setup`, `io_uring_enter` and
`io_uring_register`, which the server cannot boot without. Every other rule is
upstream's, so the server runs exactly as confined as any default container
except for io_uring.

The upstream file is pinned by tag and digest; bump both together.
"""

import hashlib
import json
import sys
import urllib.request
from pathlib import Path

UPSTREAM_TAG = "seccomp/v0.2.4"  # what Docker 29.9 vendors
UPSTREAM_URL = f"https://raw.githubusercontent.com/moby/profiles/{UPSTREAM_TAG}/seccomp/default.json"
UPSTREAM_SHA256 = "785b2429264afba4d594320337cb17f144f3c7d51585f9805eef72e28f4f9334"

IO_URING = {
    "names": ["io_uring_setup", "io_uring_enter", "io_uring_register"],
    "action": "SCMP_ACT_ALLOW",
    "comment": "gnitz-server: the master's reactor and the store's batched fsync run on io_uring",
}


def main() -> None:
    with urllib.request.urlopen(UPSTREAM_URL, timeout=30) as resp:
        raw = resp.read()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != UPSTREAM_SHA256:
        sys.exit(f"{UPSTREAM_URL}: sha256 {digest}, pinned {UPSTREAM_SHA256}")
    profile = json.loads(raw)
    if any(name in IO_URING["names"] for rule in profile["syscalls"] for name in rule["names"]):
        sys.exit(f"{UPSTREAM_TAG} already names an io_uring syscall: review it before regenerating")
    profile["syscalls"].insert(0, IO_URING)
    out = Path(__file__).with_name("seccomp.json")
    out.write_text(json.dumps(profile, indent="\t") + "\n")
    print(f"wrote {out} from {UPSTREAM_TAG}")


if __name__ == "__main__":
    main()
