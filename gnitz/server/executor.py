# gnitz/server/executor.py

import os
from rpython.rlib import rsocket, jit
from gnitz.server import ipc
from gnitz.core import errors

STATUS_OK = 0
STATUS_ERROR = 1


class ServerExecutor(object):
    """
    Coordinates the lifecycle of a Zero-Copy IPC session.
    
    Hardened to use SOCK_SEQPACKET for reliable message-oriented FD passing
    and deterministic resource cleanup.
    """

    _immutable_fields_ = ["schema", "vm_processor"]

    def __init__(self, schema, vm_processor):
        """
        schema: The TableSchema defining the layout for this executor.
        vm_processor: An object that executes logic on a batch.
                      Signature: process(input_batch) -> (output_batch, status, msg)
        """
        self.schema = schema
        self.vm_processor = vm_processor

    def run_socket_server(self, socket_path):
        """
        Listens on a Unix Domain Socket using SOCK_SEQPACKET.
        
        SOCK_SEQPACKET provides the reliability of SOCK_STREAM but preserves
        message boundaries, which is critical for the 1:1 mapping of dummy 
        payload bytes to file descriptors in SCM_RIGHTS.
        """
        if os.path.exists(socket_path):
            try:
                os.unlink(socket_path)
            except OSError:
                pass

        # Switch to SOCK_SEQPACKET for discrete message boundaries.
        server_sock = rsocket.RSocket(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)
        try:
            server_sock.bind(rsocket.UnixAddress(socket_path))
            server_sock.listen(5)

            while True:
                client_conn, _ = server_sock.accept()
                try:
                    self.handle_connection(client_conn.fd)
                except Exception:
                    # Generic catch for unexpected connection-level failures.
                    pass
                finally:
                    client_conn.close()
        finally:
            server_sock.close()

    @jit.unroll_safe
    def handle_connection(self, client_fd):
        """
        The main request-response loop for a single IPC client.
        """
        while True:
            payload = None
            try:
                # 1. Receive and Reconstruct (Zero-Copy Receive)
                # Hardened receive_payload validates bounds and magic numbers.
                try:
                    payload = ipc.receive_payload(client_fd, self.schema)
                except errors.StorageError:
                    # Likely a socket closure, magic mismatch, or protocol violation.
                    break
                except Exception:
                    break

                # 2. Execute VM Logic
                try:
                    out_batch, status, msg = self.vm_processor.process(payload.batch)

                    # 3. Serialize and Send Result (Zero-Copy Send)
                    # This performs a single copy into a new memfd.
                    ipc.send_batch(client_fd, out_batch, status, msg)

                except Exception as vm_err:
                    # 4. Error Path (Metadata-only response)
                    # Uses ipc.send_error to avoid dummy batch allocation (Case 6).
                    ipc.send_error(client_fd, str(vm_err))

            finally:
                # 5. Deterministic Cleanup (CRITICAL)
                # munmap and close(fd) occur immediately, even on VM failure.
                if payload is not None:
                    payload.close()


class SimpleEchoProcessor(object):
    """
    Example VM processor that just returns the input batch.
    """

    def process(self, input_batch):
        return input_batch, STATUS_OK, ""


def run_standalone_server(socket_path, schema):
    """
    Entry point to start a local zero-copy database node.
    """
    processor = SimpleEchoProcessor()
    executor = ServerExecutor(schema, processor)
    executor.run_socket_server(socket_path)
