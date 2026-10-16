"""The server refuses what it cannot serve before anything exists.

The refusals themselves, and what each names, are unit-tested
(`gnitz-server/src/tests/main.rs`, and the TLS ones beside `TlsArgs::resolve`).
What only a spawn can show is that a refusal ends the process as a usage error
with no data directory created — from the argv parser and from the TLS
resolution that runs after it — and that an environment knob the server cannot
read stops the boot instead of running at a default nobody asked for.
"""

import os
import subprocess

import pytest

from _serverproc import server_binary, server_env

EX_USAGE = 64


@pytest.mark.parametrize("args,named", [
    (("--workerz=4",), "--workerz=4"),
    (("--tls-listen=0.0.0.0:0",), "--allow-unauthenticated"),
], ids=["argv", "tls"])
def test_a_refused_command_line_is_a_usage_error_that_creates_nothing(own_server, args, named):
    assert own_server.start_expecting_exit(named, extra_args=args) == EX_USAGE
    assert not os.path.exists(own_server.data_dir)


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
    ("GNITZ_CASCADE_BYTES", "1MB"),
    ("GNITZ_FK_PRESENCE_KEYS", "-1"),
    ("GNITZ_PUSH_QUEUE_BYTES", "8M"),
    ("GNITZ_PATIENT_TICK_GAP_MS", "10ms"),
    ("GNITZ_TICK_COALESCE_ROWS", "10k"),
])
def test_an_unreadable_environment_knob_stops_the_boot(own_server, var, value):
    # A knob only a worker reads is refused in that worker's log.
    own_server.start_expecting_exit(var, extra_env={var: value})
