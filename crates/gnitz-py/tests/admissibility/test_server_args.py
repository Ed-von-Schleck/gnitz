"""The server refuses what it cannot serve before anything exists.

The refusals themselves are unit-tested (`gnitz-server/src/tests/main.rs`, and
the TLS ones beside `TlsArgs::resolve`). What only a spawn can show is that each
ends the process as a usage error with no data directory created, and that an
environment knob the server cannot read stops the boot instead of running at a
default nobody asked for.
"""

import os
import subprocess

import pytest

from _serverproc import server_binary, server_env

EX_USAGE = 64


def _refused(server, *args, **kw):
    """Boot with `args` appended, which argv refuses. Returns the boot's output."""
    assert server.start_expecting_exit(extra_args=args, **kw) == EX_USAGE
    assert not os.path.exists(server.data_dir), "a refused argv creates nothing"
    return server.log_text()


def test_out_of_range_workers_is_a_usage_error(own_server):
    assert "--workers" in _refused(own_server, workers=10_000)


def test_an_unknown_flag_is_not_a_positional(own_server):
    assert "--workerz=4" in _refused(own_server, "--workerz=4")


def test_a_space_separated_flag_value_is_refused(own_server):
    assert '"--workers"' in _refused(own_server, "--workers", "4")


def test_a_third_positional_is_refused(own_server):
    assert "/third" in _refused(own_server, "/third")


def test_a_public_bind_without_client_authentication_is_refused(own_server):
    assert "--allow-unauthenticated" in _refused(own_server, "--tls-listen=0.0.0.0:0")


def test_one_public_address_is_refused_the_dev_certificate(own_server):
    out = _refused(own_server, "--tls-listen=192.0.2.1:0", "--allow-unauthenticated")
    assert "--tls-cert" in out


def test_an_unreadable_pem_file_is_refused(own_server):
    out = _refused(own_server, "--tls-cert=/nonexistent/cert.pem", "--tls-key=/nonexistent/key.pem")
    assert "/nonexistent/cert.pem" in out


@pytest.mark.parametrize("flag", ["--help", "-h"])
def test_help_goes_to_stdout(flag):
    out = subprocess.run([server_binary(), flag], capture_output=True, text=True,
                         env=server_env(), timeout=30)
    assert out.returncode == 0
    assert out.stdout.startswith("gnitz-server —")
    assert out.stderr == ""


@pytest.mark.parametrize("var,value", [
    ("GNITZ_SAL_BYTES", "1g"),
    ("GNITZ_CHECKPOINT_BYTES", "64k"),
    ("GNITZ_MESH_OUTBOX_BYTES", "x"),
    ("GNITZ_INBOUND_MEM_BYTES", "-1"),
    ("GNITZ_MAX_CONNS", "0"),
    ("GNITZ_HELLO_TIMEOUT_MS", "5s"),
    ("GNITZ_CLIENT_SEND_TIMEOUT_MS", "5s"),
    ("GNITZ_REPLY_FRAME_BUDGET", "16KiB"),
    ("GNITZ_RAM_TIER_BYTES", "4 MB"),
    ("GNITZ_SCAN_CHUNK_ROWS", "3.0"),
    ("GNITZ_ADHOC_GROUP_CAP", "none"),
    ("GNITZ_KEY_SPANS_SPILL_BYTES", "0"),
    ("GNITZ_CPU_AFFINITY", "off"),
    ("GNITZ_DISABLE_IO_URING", "flase"),
    ("GNITZ_LOG_LEVEL", "verbos"),
])
def test_an_unreadable_environment_knob_stops_the_boot(own_server, var, value):
    assert own_server.start_expecting_exit(extra_env={var: value}) != 0
    # A knob only a worker reads is refused in that worker's log.
    assert var in own_server.log_text() or var in "".join(own_server.worker_log_texts())
