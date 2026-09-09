import gnitz


def test_connect_and_close(client):
    assert client is not None


def test_context_manager(server):
    with gnitz.GnitzClient(server) as c:
        sid = c.create_schema("ctx_test")
        assert sid > 0
        c.drop_schema("ctx_test")


def test_close_idempotent(client):
    client.close()
    client.close()  # second call must not raise


def test_reconnect(server):
    """Close a connection and open a new one; new connection must be functional."""
    conn = gnitz.connect(server)
    conn.close()
    conn2 = gnitz.connect(server)
    try:
        # Verify conn2 works by scanning a system table (always exists)
        result = conn2.scan(1)
        assert result is not None
    finally:
        conn2.close()
