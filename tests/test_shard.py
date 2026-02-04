import os
import pytest
from gnitz.storage.writer import ShardWriter
from gnitz.storage.shard import ShardView

def test_end_to_end_shard_io():
    filename = "test_io.db"
    try:
        # 1. Write
        sw = ShardWriter()
        sw.add_entry("key1", "value1", 10)
        sw.add_entry("key2", "value2", -5)
        sw.finalize(filename)

        # 2. Read
        view = ShardView(filename)
        assert view.count == 2
        
        # Check Weights
        assert view.get_weight(0) == 10
        assert view.get_weight(1) == -5
        
        # Check Strings
        assert view.materialize_key(0) == "key1"
        assert view.materialize_value(0) == "value1"
        assert view.materialize_key(1) == "key2"
        assert view.materialize_value(1) == "value2"
        
        view.close()
    finally:
        if os.path.exists(filename):
            os.unlink(filename)
