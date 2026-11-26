# Security

A server accepts clients on two transports: its Unix socket, always, and a TLS
1.3 listener, when started with `--tls-listen`. Every client that gets in may
run any statement; there are no roles or per-relation permissions.

## The Unix socket

The socket asks for no token; filesystem permissions decide who may connect.
It is created owner-only (`--socket-mode=600`). `--socket-mode=660` admits the
socket's group, which is how the systemd unit lets the `gnitz` group in.

## TLS

```sh
gnitz-server --tls-listen=0.0.0.0:7543 --tls-name=db.example.com \
    --auth-tokens=/var/lib/gnitz/tokens /var/lib/gnitz /run/gnitz/gnitz.sock
```

Network clients speak TLS only. The server's certificate is either

- **your own**: `--tls-cert=fullchain.pem --tls-key=privkey.pem`, for example
  from Let's Encrypt; or
- **minted by the server**: without `--tls-cert`, it writes a self-signed
  certificate covering loopback and every `--tls-name` to
  `<data_dir>/tls_cert.pem` and keeps it across restarts. Clients trust it by
  a copy of that file. Delete `tls_cert.pem` and `tls_key.pem` to mint a new
  one.

A bind to a non-loopback address must authenticate clients: it needs
`--auth-tokens`, `--tls-client-ca`, or the explicit escape hatch
`--allow-unauthenticated`.

## Access tokens

```sh
gnitz-server --new-token /var/lib/gnitz/tokens app
```

mints a token named `app`, appends its SHA-256 to the token file and prints the
token once; the server keeps only the hash. Start the server with
`--auth-tokens=<file>`, and every TLS connection must present one of the
tokens, loopback included.

A client gives its token in a file named in the target, or in `GNITZ_TOKEN`:

```sh
gnitz "tls://db.example.com:7543?ca=gnitz-ca.pem&token_file=app.token"
GNITZ_TOKEN=… gnitz tls://db.example.com:7543
```

To revoke a token, delete its line from the file and reload the server.

## Client certificates

`--tls-client-ca=ca.pem` requires every TLS client to present a certificate
signed by that CA, given in the target as `cert=client.pem&key=client.key`.
Use a CA dedicated to gnitz clients: any certificate it signs gets in. There is
no revocation list; rotating the CA revokes every client at once. With
`--auth-tokens` too, a client needs both.

## Reloading

`SIGHUP` (`systemctl reload`, `docker kill -s HUP`) rereads the certificate and
key, the client CA and the token file, for connections accepted from then on.
A reload that fails keeps what is served and logs why. A minted certificate is
read only at boot.

## Target syntax

A client target is either a Unix socket path or `tls://HOST:PORT`, optionally
followed by `?` and `&`-separated `ca=PATH`, `cert=PATH&key=PATH` and
`token_file=PATH`. Without `ca=`, the server's certificate is checked against
the public web roots.
