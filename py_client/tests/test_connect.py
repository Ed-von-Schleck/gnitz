"""Test basic connect/disconnect lifecycle."""


def test_connect_and_close(client):
    """Verify socket is valid after connection."""
    assert client.sock is not None
    assert client.sock.fileno() >= 0


def test_reconnect(server):
    """Connect, close, reconnect."""
    from gnitz_client import GnitzClient

    c1 = GnitzClient(server)
    assert c1.sock.fileno() >= 0
    c1.close()

    c2 = GnitzClient(server)
    assert c2.sock.fileno() >= 0
    c2.close()
