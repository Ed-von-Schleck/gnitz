"""Smoke-test the server image the way it is deployed.

    smoke.py <podman|docker> <image>

Boots the image under `container/seccomp.json` with a CPU quota, a named
volume, `--workers=auto` and an authenticated TLS listener, queries a view
over the published port, stops it, boots it again on the same volume and reads
the view back. Then boots it under the runtime's default seccomp profile and
expects the refusal to name its remedy. Every container and volume it makes is
removed, pass or fail.

Run through `make image-test`, which builds the image and the client first.
"""

import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import gnitz

SECCOMP = Path(__file__).with_name("seccomp.json")
PORT = 7543
CPUS = 3
ROWS = 1000


def run(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run([ENGINE, *args], capture_output=True, text=True, check=check)


def wait_ready(name: str, timeout: float = 60.0) -> str:
    """The container's log once it says ready; fails if it exits first."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        logs = run("logs", name).stderr
        if gnitz._native.READY_LINE in logs:
            return logs
        if run("inspect", "--format", "{{.State.Running}}", name).stdout.strip() != "true":
            sys.exit(f"{name} exited before it was ready:\n{logs}")
        time.sleep(0.2)
    sys.exit(f"{name} not ready after {timeout}s:\n{run('logs', name).stderr}")


def serve(name: str, volume: str) -> str:
    """Boot the server, and return the TLS target the host dials."""
    run(
        "run", "--detach", "--name", name,
        "--security-opt", f"seccomp={SECCOMP}",
        f"--cpus={CPUS}",
        "--volume", f"{volume}:/data",
        "--publish", f"127.0.0.1::{PORT}",
        IMAGE,
        "--workers=auto", "--log-level=normal",
        f"--tls-listen=0.0.0.0:{PORT}", "--tls-name=localhost", "--auth-tokens=/data/tokens",
        "/data", "/data/gnitz.sock",
    )
    logs = wait_ready(name)
    # A quota of 3 CPUs below the host's: unpinned, and 2 workers beside the master.
    for line in ("workers: 2 (auto)", "affinity: not applied (a CPU quota of 3"):
        assert line in logs, f"{line!r} not in:\n{logs}"
    host_port = run("port", name, f"{PORT}/tcp").stdout.split()[0].rsplit(":", 1)[1]
    return f"127.0.0.1:{host_port}"


def stop(name: str) -> None:
    run("stop", "--time", "60", name)
    code = run("inspect", "--format", "{{.State.ExitCode}}", name).stdout.strip()
    assert code == "0", f"{name} stopped with exit code {code}:\n{run('logs', name).stderr}"
    run("rm", name)


def sums(client) -> dict[int, tuple[int, int]]:
    """The view's rows as `k -> (total, weight)`."""
    [result] = client.execute_sql("SELECT k, total FROM s")
    return {row.k: (row.total, row._weight) for row in result["rows"]}


def main() -> None:
    tag = uuid.uuid4().hex[:8]
    volume, name = f"gnitz-smoke-{tag}", f"gnitz-smoke-{tag}"
    want = {k: (sum(i for i in range(ROWS) if i % 3 == k), 1) for k in range(3)}
    run("volume", "create", volume)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            # The token is minted by the image itself, into the volume.
            token = run("run", "--rm", "--volume", f"{volume}:/data", IMAGE, "--new-token", "/data/tokens", "smoke")
            (Path(tmp) / "token").write_text(token.stdout.strip() + "\n")

            def connect(target: str):
                return gnitz.connect(f"tls://{target}?ca={tmp}/ca.pem&token_file={tmp}/token")

            target = serve(name, volume)
            run("cp", f"{name}:/data/tls_cert.pem", f"{tmp}/ca.pem")
            with connect(target) as client:
                client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT, v BIGINT)")
                client.execute_sql("CREATE VIEW s AS SELECT k, SUM(v) AS total FROM t GROUP BY k")
                client.execute_sql("INSERT INTO t VALUES " + ",".join(f"({i},{i % 3},{i})" for i in range(ROWS)))
                assert sums(client) == want, sums(client)
            stop(name)

            # The volume carries the database across a restart.
            with connect(serve(name, volume)) as client:
                assert sums(client) == want, sums(client)
            stop(name)

        # The runtime's own profile denies io_uring, and the refusal says what to do.
        refused = run("run", "--name", name, "--volume", f"{volume}:/data", IMAGE, check=False)
        run("rm", name)
        assert refused.returncode == 1, f"exit {refused.returncode}:\n{refused.stderr}"
        assert "seccomp profile that also allows io_uring_setup" in refused.stderr, refused.stderr
    finally:
        run("rm", "--force", name, check=False)
        run("volume", "rm", "--force", volume, check=False)
    print(f"{IMAGE}: ok")


if __name__ == "__main__":
    ENGINE, IMAGE = sys.argv[1:]
    main()
