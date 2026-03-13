"""Session-scoped server fixture and per-test client fixture."""

import os
import subprocess
import tempfile
import time
import shutil

import pytest

from gnitz_client import GnitzClient


@pytest.fixture(scope="session")
def server():
    """Start gnitz-server-c, yield the socket path, kill on teardown."""
    tmpdir = tempfile.mkdtemp(prefix="gnitz_e2e_")
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")

    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.join(os.path.dirname(__file__), "..", "..", "gnitz-server-c"),
    )
    binary = os.path.abspath(binary)

    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")

    cmd = [binary, data_dir, sock_path]
    num_workers = os.environ.get("GNITZ_WORKERS", "")
    if num_workers:
        cmd.append(f"--workers={num_workers}")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for socket to appear
    for _ in range(100):  # 10s timeout
        if os.path.exists(sock_path):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        stdout, stderr = proc.communicate()
        raise RuntimeError(
            f"Server failed to start.\nstdout: {stdout.decode()}\nstderr: {stderr.decode()}"
        )

    yield sock_path

    # Check if server died early
    early_exit = proc.poll()
    if early_exit is not None:
        stdout, stderr = proc.communicate()
        print(f"\n[server died early with code {early_exit}]")
        print(f"[stdout]\n{stdout.decode(errors='replace')}")
        print(f"[stderr]\n{stderr.decode(errors='replace')}")
    proc.kill()
    proc.wait()
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def client(server):
    """Per-test client connection."""
    c = GnitzClient(server)
    yield c
    c.close()


@pytest.fixture(scope="module")
def isolated_server():
    """Per-module server instance — a crash is isolated to this module."""
    tmpdir = tempfile.mkdtemp(prefix="gnitz_e2e_")
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")

    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.join(os.path.dirname(__file__), "..", "..", "gnitz-server-c"),
    )
    binary = os.path.abspath(binary)

    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")

    cmd = [binary, data_dir, sock_path]
    num_workers = os.environ.get("GNITZ_WORKERS", "")
    if num_workers:
        cmd.append(f"--workers={num_workers}")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    for _ in range(100):  # 10s timeout
        if os.path.exists(sock_path):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        stdout, stderr = proc.communicate()
        raise RuntimeError(
            f"Server failed to start.\nstdout: {stdout.decode()}\nstderr: {stderr.decode()}"
        )

    yield sock_path

    early_exit = proc.poll()
    if early_exit is not None:
        stdout, stderr = proc.communicate()
        print(f"\n[server died early with code {early_exit}]")
        print(f"[stdout]\n{stdout.decode(errors='replace')}")
        print(f"[stderr]\n{stderr.decode(errors='replace')}")
    proc.kill()
    proc.wait()
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def iclient(isolated_server):
    """Per-test client on the module-isolated server."""
    c = GnitzClient(isolated_server)
    yield c
    c.close()
