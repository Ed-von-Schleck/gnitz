# rpython_tests/helpers/fs.py
#
# Shared filesystem cleanup helpers for RPython test binaries.

import os

from rpython.rlib import rposix


def cleanup_dir(path):
    """Remove directory tree using os.path API."""
    if not os.path.exists(path):
        return
    for item in os.listdir(path):
        p = os.path.join(path, item)
        if os.path.isdir(p):
            cleanup_dir(p)
        else:
            os.unlink(p)
    os.rmdir(path)


def _recursive_delete(path):
    """Remove directory tree using rposix for robustness."""
    if not os.path.exists(path):
        return
    if os.path.isdir(path):
        items = os.listdir(path)
        for item in items:
            _recursive_delete(path + "/" + item)
        try:
            rposix.rmdir(path)
        except OSError:
            pass
    else:
        try:
            os.unlink(path)
        except OSError:
            pass


def cleanup(path):
    """Remove path if it exists, using _recursive_delete."""
    if os.path.exists(path):
        _recursive_delete(path)
