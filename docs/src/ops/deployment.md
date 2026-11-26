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
| io_uring allowed | `kernel.io_uring_disabled` must be `0`, and a seccomp filter must allow `io_uring_setup`, `io_uring_enter` and `io_uring_register` | `kernel.io_uring_disabled=…`, or `a seccomp filter denies it …` |
| x86-64 with AVX2, or aarch64 | The release binaries target x86-64-v3 | the binary dies on an illegal instruction |
| A local filesystem for the data | The write-ahead log is a memory-mapped file that every write `fdatasync`s | — |

## Workers

`--workers=N` runs `N` worker processes; `--workers=auto` runs one per physical
core the server may use, less one for the master, and pins each to its core.
Under a CPU quota narrower than the cores (a container's `--cpus`, a Kubernetes
`limits.cpu` without exclusive cores) it runs one per whole CPU of the quota,
unpinned. The log names the count and placement at boot.

Changing the worker count is safe: a boot at a different count first
redistributes the stored data onto the new count.

## Stopping

`SIGTERM` or `SIGINT` drains, checkpoints and exits 0. Give the service manager
a stop timeout longer than the largest checkpoint you expect — two minutes is a
reasonable start. A `SIGKILL` or a crash loses nothing that was acknowledged;
the next boot replays the log since the last checkpoint.

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

`podman run` takes the same flags. Without arguments the image runs
`--workers=auto /data /data/gnitz.sock`: a Unix socket in the volume and no
network listener.

- **Data.** Mount a volume at `/data`; never use the container's writable layer.
- **Logs.** The master logs to stderr (`docker logs`); each worker to
  `/data/worker_<N>.log`.
- **Unix socket.** A client container sharing the volume can dial
  `/data/gnitz.sock` if the [socket's mode](security.md#the-unix-socket) lets
  its user in: run it as the server's user (`--user`, plus `--userns=keep-id`
  under rootless podman), or start the server with `--socket-mode=660`.
- **Memory.** The image sets `GLIBC_TUNABLES=glibc.malloc.hugetlb=1` so the heap
  uses transparent huge pages where the host allows them; `-e GLIBC_TUNABLES=`
  turns it off.

Docker Desktop 4.42 blocks io_uring whatever the profile; later versions do
not. gVisor does not support the io_uring the server needs, so the server does
not run under it.

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

### Kubernetes

Put `seccomp.json` on every node under the kubelet's seccomp root
(`/var/lib/kubelet/seccomp/`) and reference it explicitly, so the server's
confinement does not depend on how a node is configured. A managed node image
may set `kernel.io_uring_disabled`; check the node pool before scheduling.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: gnitz }
spec:
  serviceName: gnitz
  selector: { matchLabels: { app: gnitz } }
  template:
    metadata: { labels: { app: gnitz } }
    spec:
      terminationGracePeriodSeconds: 120
      securityContext:
        seccompProfile: { type: Localhost, localhostProfile: gnitz-seccomp.json }
      containers:
        - name: gnitz
          image: ghcr.io/ed-von-schleck/gnitz-server
          args: [--workers=auto, --tls-listen=0.0.0.0:7543, --tls-name=gnitz.default.svc,
                 --auth-tokens=/data/tokens, /data, /data/gnitz.sock]
          ports: [{ containerPort: 7543 }]
          readinessProbe: { tcpSocket: { port: 7543 } }
          resources:
            requests: { cpu: "4", memory: 8Gi }
            limits: { cpu: "4", memory: 8Gi }
          volumeMounts: [{ name: data, mountPath: /data }]
  volumeClaimTemplates:
    - metadata: { name: data }
      spec: { accessModes: [ReadWriteOnce], resources: { requests: { storage: 50Gi } } }
```

## macOS and Windows

The server runs on Linux only. On macOS run it in a container, or in a Linux
VM such as [Lima](https://lima-vm.io); on Windows, inside WSL 2.
