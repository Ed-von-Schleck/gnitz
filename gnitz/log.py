# gnitz/log.py
#
# Structured logging for GnitzDB.
#
# Three levels: QUIET (default), NORMAL, DEBUG.
# error/warn always emit; info requires NORMAL; debug requires DEBUG.
# All output goes to stderr. Timestamps are epoch seconds + milliseconds.

import os
import time

QUIET = 0
NORMAL = 1
DEBUG = 2
VERBOSE = DEBUG  # backward-compat alias


class _LogState(object):
    def __init__(self):
        self.level = QUIET
        self.process_tag = "M"


_state = _LogState()


def init(level):
    _state.level = level


def set_process_tag(tag):
    _state.process_tag = tag


def parse_level(s):
    if s == "quiet":
        return QUIET
    elif s == "normal":
        return NORMAL
    elif s == "debug" or s == "verbose":
        return DEBUG
    os.write(2, "Unknown log level '" + s + "', defaulting to quiet\n")
    return QUIET


def is_info():
    return _state.level >= NORMAL


def is_debug():
    return _state.level >= VERBOSE


def error(msg):
    _emit("ERROR", msg)


def warn(msg):
    _emit("WARN", msg)


def info(msg):
    if _state.level >= NORMAL:
        _emit("INFO", msg)


def debug(msg):
    if _state.level >= VERBOSE:
        _emit("DEBUG", msg)


def _pad3(n):
    if n < 10:
        return "00" + str(n)
    elif n < 100:
        return "0" + str(n)
    return str(n)


def _emit(tag, msg):
    t = time.time()
    secs = int(t)
    millis = int((t - float(secs)) * 1000.0)
    os.write(
        2, str(secs) + "." + _pad3(millis)
        + " " + _state.process_tag
        + " " + tag + " " + msg + "\n"
    )
