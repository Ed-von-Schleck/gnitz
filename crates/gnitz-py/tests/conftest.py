import os, subprocess, tempfile, time, shutil, signal
import pytest
import gnitz


def _resolve_server_binary():
    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__),
                                     "../../../gnitz-server")),
    )
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    return binary


def start_server_proc(data_dir, sock_path, workers=None, extra_env=None):
    """Launch a fresh server in its own process group; wait for socket."""
    binary = _resolve_server_binary()
    cmd = [binary, data_dir, sock_path]
    if workers:
        cmd += [f"--workers={workers}"]
    env = None
    if extra_env:
        env = os.environ.copy()
        env.update(extra_env)
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True, env=env,
    )
    for _ in range(100):
        if os.path.exists(sock_path):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        proc.communicate()
        raise RuntimeError("Server did not start")
    return proc


def stop_server_proc(proc):
    """SIGKILL the entire process group, then reap."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


@pytest.fixture(scope="session")
def server():
    tmpdir = tempfile.mkdtemp(dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_py_")
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__),
                                     "../../../gnitz-server")),
    )
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    cmd = [binary, data_dir, sock_path]
    if w := os.environ.get("GNITZ_WORKERS"):
        cmd += [f"--workers={w}"]
    if ll := os.environ.get("GNITZ_LOG_LEVEL"):
        cmd += [f"--log-level={ll}"]
    stderr_path = os.path.expanduser("~/git/gnitz/tmp/server_debug.log")
    stderr_f = open(stderr_path, "w")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=stderr_f)
    for _ in range(100):
        if os.path.exists(sock_path):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        proc.communicate()
        raise RuntimeError("Server did not start")
    yield sock_path
    proc.kill()
    proc.wait()
    stderr_f.close()
    stable_dir = os.path.expanduser("~/git/gnitz/tmp")
    for wlog in ("worker_0.log", "worker_1.log", "worker_2.log", "worker_3.log"):
        src = os.path.join(data_dir, wlog)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(stable_dir, "last_" + wlog))
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def client(server):
    with gnitz.connect(server) as conn:
        yield conn
