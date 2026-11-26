# Configuration

```text
gnitz-server [OPTIONS] <data_dir> <socket_path>
gnitz-server --new-token <token_file> <name>
gnitz-server --disk-usage <data_dir>
```

`gnitz-server --help` describes every option. Options are written
`--flag=VALUE`; an unknown option, or a value that does not parse, stops the
server before it creates anything.

## Options

| Option | Default | |
|---|---|---|
| `--workers=N\|auto` | `1` | Worker processes; see [Deployment](deployment.md#workers). |
| `--log-level=LEVEL` | `quiet` | `quiet`, `normal` or `verbose`. |
| `--socket-mode=MODE` | `600` | Permission bits of the Unix socket. |
| `--tls-listen=IP:PORT` | none | Also accept TLS clients on this address. |
| `--tls-cert=PEM`, `--tls-key=PEM` | minted | The server's certificate and key. |
| `--tls-name=NAME,…` | none | Names a minted certificate covers besides loopback. |
| `--auth-tokens=FILE` | none | Require an access token on TLS connections. |
| `--tls-client-ca=PEM` | none | Require client certificates signed by this CA. |
| `--allow-unauthenticated` | off | Permit a non-loopback TLS bind without authentication. |
| `--backup-dir=DIR` | none | Where [`\backup`](backup.md#backups) writes; without it backups are refused. |

[Security](security.md) covers the TLS options and
[Backup and upgrades](backup.md) the backup directory.

## Environment

A variable that is set but cannot be read stops the server; unset takes the
default.

| Variable | |
|---|---|
| `GNITZ_LOG_LEVEL` | As `--log-level`; the option wins. |
| `GNITZ_MAX_CONNS` | Open client connections, all transports together (default 256). |
| `GNITZ_HELLO_TIMEOUT_MS` | Time a connection may take to identify itself, TLS handshake included. |
| `GNITZ_CPU_AFFINITY` | `0` stops pinning processes to CPUs; set it when servers share a host without their own CPU sets. |
| `GNITZ_CHECKPOINT_BYTES` | Log size that triggers a checkpoint (default 75% of the log). |

Clients read `GNITZ_TARGET` (the server to connect to) and `GNITZ_TOKEN` (the
access token).

## The data directory

Everything the server keeps lives in `<data_dir>`: the write-ahead log, the
stored relations, the minted TLS certificate and the workers' logs.
`gnitz-server --disk-usage <data_dir>` prints what it holds on disk, by
relation and store; it reads only the files, so it runs beside a live server.

## Exit codes

| Code | |
|---|---|
| 0 | Clean shutdown. |
| 1 | Boot failure. |
| 2 | A worker process died while serving. |
| 64 | The command line was refused; nothing was created. |
| 65 | The data directory is of another release's storage format; nothing in it was changed. See [upgrades](backup.md#upgrading). |
| 134 | Fail-stop abort (128 + `SIGABRT`). |

Exit codes 64 and 65 repeat on every restart with the same arguments, so a
service manager should not restart on them; the systemd unit does not.
