# Deployment

A server is one process tree on one host: a master and a number of worker
processes, a data directory, a Unix socket, and optionally a TLS listener.

```sh
gnitz-server --workers=auto /var/lib/gnitz /run/gnitz/gnitz.sock
```

A client and a server must be of the same release; a client that dials another
release's server is refused, naming both. Pin versions together.

## Host requirements

| Requirement | Why | If it is missing, the boot says |
|---|---|---|
| Linux ≥ 6.7 | The server's event loop needs io_uring's `IORING_OP_FUTEX_WAIT` | `kernel … lacks io_uring IORING_OP_FUTEX_WAIT` |
| io_uring allowed | `kernel.io_uring_disabled` must be `0` (or `1` with the server in `kernel.io_uring_group`), and a seccomp filter must allow `io_uring_setup`, `io_uring_enter` and `io_uring_register` | `kernel.io_uring_disabled=…`, or `a seccomp filter denies it …` |
| x86-64 with AVX2, or aarch64 | The release binaries target x86-64-v3 | the binary dies on an illegal instruction |

## Workers

`--workers=N` runs `N` worker processes; `--workers=auto` runs one per physical
core the server may use, less one for the master, and pins each to its core.
Under a CPU quota narrower than the cores, such as a container's `--cpus`, it
runs one per whole CPU of the quota, less one for the master, unpinned. The
log names the count and placement at boot.

Changing the worker count is safe: a boot at a different count first
redistributes the stored data onto the new count.

## Stopping

`SIGTERM` or `SIGINT` drains, checkpoints and exits 0. Give the service manager
a stop timeout longer than the largest checkpoint you expect — two minutes is a
reasonable start. A `SIGKILL` or a crash loses no acknowledged write to a
table; the next boot replays the log since the last checkpoint.

## systemd

The release archive carries `gnitz-server.service`:

```sh
sudo install -m 0755 gnitz-server /usr/local/bin/
sudo install -m 0644 gnitz-server.service /etc/systemd/system/
sudo useradd --system --no-create-home --shell /usr/sbin/nologin gnitz
sudo systemctl enable --now gnitz-server
```

The data lives in `/var/lib/gnitz`. Clients connect over
`/run/gnitz/gnitz.sock`, which members of the `gnitz` group may use
(`sudo usermod -aG gnitz <user>`). For clients over the network, add a TLS
listener to `ExecStart` as described in [Security](security.md).
`systemctl reload gnitz-server` reloads certificates and tokens.

## Containers

Each release publishes `ghcr.io/ed-von-schleck/gnitz-server` for amd64 and
arm64, tagged with its version, and attaches the seccomp profile it runs under.
Pin the version tag rather than `latest`, since clients must match it.

The default seccomp profile of Docker, podman, containerd and CRI-O denies
io_uring, so the server runs under `seccomp.json`: Docker's default profile
with the three io_uring calls allowed, otherwise as confined as any container.
It needs no capability and no `--privileged`.

```sh
curl -LO https://github.com/Ed-von-Schleck/gnitz/releases/latest/download/seccomp.json
docker volume create gnitz
# mint an access token into the volume; it is printed once
docker run --rm -v gnitz:/data ghcr.io/ed-von-schleck/gnitz-server --new-token /data/tokens app

docker run -d --name gnitz \
    --security-opt seccomp=seccomp.json \
    --stop-timeout 120 \
    -v gnitz:/data -p 7543:7543 \
    ghcr.io/ed-von-schleck/gnitz-server --workers=auto \
        --tls-listen=0.0.0.0:7543 --tls-name=db.example.com --auth-tokens=/data/tokens \
        /data /data/gnitz.sock
```

`podman run` takes the same flags. Rootless podman forwards a client that
dials `localhost` into the container over IPv6, which a listener on
`0.0.0.0` does not accept: under podman, listen on `--tls-listen=[::]:7543`,
or have clients dial `127.0.0.1`. Without arguments the image runs
`--workers=auto /data /data/gnitz.sock`: a Unix socket in the volume and no
network listener.

- **Data.** Mount a volume at `/data`; never use the container's writable layer.
- **Logs.** The master logs to stderr (`docker logs`); each worker to
  `/data/worker_<N>.log`.
- **Unix socket.** The server runs as the image's `nonroot` user (65532), and
  the [socket](security.md#the-unix-socket) admits only its owner. A client
  container sharing the volume can dial `/data/gnitz.sock` when it runs as that
  user (`--user 65532`), or in its group (`--user <uid>:65532`) when the server
  is started with `--socket-mode=660`. For a client on the host, run the server
  as the host user (`--user "$(id -u):$(id -g)"`, plus `--userns=keep-id` under
  rootless podman) on a host directory mounted at `/data`.
- **Memory.** The image sets `GLIBC_TUNABLES=glibc.malloc.hugetlb=1` so the heap
  uses transparent huge pages where the host allows them; `-e GLIBC_TUNABLES=`
  turns it off.

### Docker Compose

```yaml
services:
  gnitz:
    image: ghcr.io/ed-von-schleck/gnitz-server
    command: [--workers=auto, --tls-listen=0.0.0.0:7543, --tls-name=localhost,
              --auth-tokens=/data/tokens, /data, /data/gnitz.sock]
    security_opt: [seccomp=./seccomp.json]
    stop_grace_period: 2m
    ports: ["7543:7543"]
    volumes: [gnitz:/data]
volumes:
  gnitz:
```

The server refuses to start without the token file, so mint a token into the
volume first:

```sh
docker compose run --rm gnitz --new-token /data/tokens app
docker compose up -d
```
