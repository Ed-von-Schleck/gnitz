# The gnitz server image

Each release publishes the image for amd64 and arm64, tagged with its version and,
unless it is a prerelease, `latest`, and attaches the seccomp profile it runs under:

```bash
docker pull ghcr.io/ed-von-schleck/gnitz-server
curl -LO https://github.com/Ed-von-Schleck/gnitz/releases/latest/download/seccomp.json
```

A client and a server must be of the same release, so a deployment pins the
version tag rather than `latest`. To build the image from a checkout instead:

```bash
make image        # build gnitz-server:<git describe> and gnitz-server:latest
make image-test   # boot it the way it is deployed, query it, restart it, and see it refused without the profile
```

`make image` uses podman if it is installed, else docker (`CONTAINER=docker` picks one).
The image is the release server on distroless (`gcr.io/distroless/cc-debian13`, glibc
and nothing else), running as the unprivileged user `nonroot` (uid 65532).

## What the host must give it

The server runs on io_uring. A container shares the host's kernel, so the image
cannot bring what the host lacks:

| Requirement | Why | If it is missing, the boot says |
|---|---|---|
| Linux ≥ 6.7 | The master's event loop waits on `IORING_OP_FUTEX_WAIT` | `kernel … lacks io_uring IORING_OP_FUTEX_WAIT` |
| A seccomp profile that allows `io_uring_setup`, `io_uring_enter`, `io_uring_register` | The default profile of Docker (since 25), podman, containerd and CRI-O denies them | `a seccomp filter denies it …` |
| `kernel.io_uring_disabled = 0` | `1` admits only `CAP_SYS_ADMIN` and `kernel.io_uring_group`; `2` admits nobody | `kernel.io_uring_disabled=…` |
| An x86-64-v3 (AVX2) CPU | The release build targets it | — the binary dies on an illegal instruction |

gVisor (GKE Sandbox) runs io_uring off by default and only partially when it is on,
so the server does not run there. A VM-isolated runtime (Kata, Firecracker) needs the
guest kernel to be 6.7 or later.

The server needs no capability, no `--privileged`, no `--shm-size` and no
`--ulimit memlock`.

## `seccomp.json`

[`seccomp.json`](seccomp.json) is Docker's default profile with one rule added, which
allows the three io_uring syscalls. Every other rule is upstream's, so the server
runs as confined as any default container apart from io_uring. Run it under that
profile, not `seccomp=unconfined`.

`make container-seccomp` regenerates it from the upstream file, pinned by tag and
digest in [`seccomp.py`](seccomp.py). To move to a new upstream tag, bump both values.

## Running it

```bash
docker volume create gnitz
# An access token, minted by the image into the volume; the token is printed once.
docker run --rm -v gnitz:/data ghcr.io/ed-von-schleck/gnitz-server --new-token /data/tokens app

docker run -d --name gnitz \
    --security-opt seccomp=seccomp.json \
    --stop-timeout 120 \
    -v gnitz:/data -p 7543:7543 \
    ghcr.io/ed-von-schleck/gnitz-server --workers=auto \
        --tls-listen=0.0.0.0:7543 --tls-name=db.example.com --auth-tokens=/data/tokens \
        /data /data/gnitz.sock
```

`podman run` takes the same flags. With no arguments the image runs
`--workers=auto /data /data/gnitz.sock`: a Unix socket in the volume and no TCP
listener.

- **Data.** Mount a volume at `/data`, never the container's own writable layer: the
  WAL is a preallocated, memory-mapped file that every write `fdatasync`s.
- **Network.** TCP is TLS only. Binding `0.0.0.0` counts as a non-loopback bind, so it
  needs `--auth-tokens` or `--tls-client-ca`. Give it a certificate (`--tls-cert`,
  `--tls-key`), or name what clients dial with `--tls-name`: without either, the minted
  certificate covers only loopback, which no client outside the container dials. The
  minted certificate is kept in the volume as `/data/tls_cert.pem`, and a client trusts
  it with `tls://host:7543?ca=<copy of it>&token_file=<file holding the token>`.
- **Unix socket.** A client in another container that shares the volume can dial
  `/data/gnitz.sock` directly. A client on the host also needs write permission on the
  socket file, so run the container as the client's user (`--user "$(id -u):$(id -g)"`,
  plus `--userns=keep-id` under rootless podman).
- **Stopping.** `SIGTERM` drains, checkpoints and exits 0. Give it a stop timeout
  longer than the largest checkpoint you expect; the default (10 s for Docker, 30 s for
  Kubernetes) may be too short. A `SIGKILL` loses nothing that was acknowledged, but the
  next boot replays the WAL tail. `docker kill -s HUP` reloads the TLS certificate, the
  client CA and the token file.
- **Logs.** The master logs to stderr, which is what `docker logs` shows. Each worker
  logs to `/data/worker_<N>.log`.

### Memory

The image sets `GLIBC_TUNABLES=glibc.malloc.hugetlb=1`, so glibc's allocator backs its
heap with transparent huge pages. The server's kernel time goes mostly into page
faults as its heap grows, and this setting makes those faults much less frequent, at
the cost of slightly more resident memory. It takes effect where the host's
`/sys/kernel/mm/transparent_hugepage/enabled` is `madvise`, the default on most
distributions. Under `never` it does nothing, and under `always` the heap already
gets huge pages. Pass `-e GLIBC_TUNABLES=` to turn it off.

### Workers and CPUs

`--workers=auto` sizes the server from what the container is given:

- **A cpuset and no narrower quota** (`--cpuset-cpus`, or a Kubernetes Guaranteed pod
  with integer CPUs under the static CPU manager): one worker per physical core, less
  one core for the master. Each worker is pinned to its own core.
- **A CPU quota narrower than the cpuset** (`--cpus`, or Kubernetes `limits.cpu`
  without exclusive cores): one worker per whole CPU of the quota, less one for the
  master. Nothing is pinned, because the cores are shared with other containers.

The log names the count and the placement (`workers: N (auto)`, `affinity: …`). If a
boot finds a different worker count than the last one, it redistributes the stored
data onto the new count before it serves, so changing a container's CPUs costs that
one relay at its next start. Pass `--workers=N` to keep the count fixed.

## Docker Compose

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

## Kubernetes

A pod with no `seccompProfile` runs unconfined unless the kubelet has
`--seccomp-default` set. Pin the profile explicitly so the server's confinement does
not depend on how a node is configured. Put `seccomp.json` on every node under the
kubelet's seccomp root (`/var/lib/kubelet/seccomp/`), by node provisioning, a
DaemonSet or the Security Profiles Operator, and reference it:

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

The node must meet the host requirements above. A managed node image may set
`kernel.io_uring_disabled`, so check the node pool's image before you schedule the
server on it.
