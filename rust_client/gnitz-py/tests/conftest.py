import os, subprocess, tempfile, time, shutil
import pytest
import gnitz


@pytest.fixture(scope="session")
def server():
    tmpdir = tempfile.mkdtemp(dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_py_")
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__),
                                     "../../../gnitz-server-c")),
    )
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    cmd = [binary, data_dir, sock_path]
    if w := os.environ.get("GNITZ_WORKERS"):
        cmd += [f"--workers={w}"]
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
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def client(server):
    with gnitz.connect(server) as conn:
        yield conn
