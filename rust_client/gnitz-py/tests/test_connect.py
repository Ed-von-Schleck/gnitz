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
