#!/usr/bin/env python3
"""Concatenate files into one bundle with LLM-friendly boundary markers.

Emits Gemini-style plain-text delimiters (`--- path ---`), so an LLM served the
bundle as a single file can still tell the originals apart. Paths are recorded
relative to the current directory, or absolute where the relative form would
climb out of it. Originals are never modified.

A boundary marker is not escaped in the file bodies, so an input line that
itself reads `--- x ---` is indistinguishable from one.

Without -o the bundle is written to stdout.
"""

import argparse
import contextlib
import os
import sys
import tempfile


def display_path(p):
    """`p` relative to the current directory, or absolute when the relative
    form would escape it — `../../etc/hosts` names the file by where the
    bundler happened to run."""
    absolute = os.path.abspath(p)
    rel = os.path.relpath(absolute)
    return absolute if rel.split(os.sep)[0] == os.pardir else rel


def read_text(path):
    """The file's text, and whether undecodable bytes were replaced. A non-UTF-8
    input is still bundled — U+FFFD is the right default for a tool feeding an
    LLM a single blob — but not silently: the caller names the file on stderr."""
    with open(path, "rb") as f:
        raw = f.read()
    try:
        return raw.decode("utf-8"), False
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace"), True


def default_file_mode():
    """The mode `open(path, "w")` would create — which `mkstemp`'s 0600 is not."""
    mask = os.umask(0o077)
    os.umask(mask)
    return 0o666 & ~mask


def write_atomically(path, write_body):
    """Run `write_body(f)` into a temp file beside `path`, then rename over it, so
    a write that dies partway leaves `path` as it was."""
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(path)),
                               prefix=".bundle-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            write_body(f)
        os.chmod(tmp, default_file_mode())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def bundle(entries, out):
    for i, (rel, text) in enumerate(entries):
        if i:
            out.write("\n")
        out.write(f"--- {rel} ---\n")
        out.write(text)
        if not text.endswith("\n"):
            out.write("\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="+", metavar="FILE", help="files to concatenate")
    ap.add_argument("-o", "--output", help="output file (default: stdout)")
    args = ap.parse_args()

    # Refuse to replace a file with a bundle of itself. `realpath` rather than
    # `samefile`: the output need not exist yet.
    if args.output:
        out_real = os.path.realpath(args.output)
        clashing = [f for f in args.files if os.path.realpath(f) == out_real]
        if clashing:
            sys.exit(f"error: output {args.output} is also an input: " + ", ".join(clashing))

    # Read everything before writing anything: a read that fails halfway would
    # otherwise leave a truncated bundle behind. Every bad path is named by the
    # errno the open raised — a missing file, a directory, a permission error —
    # rather than by a pre-pass that would call a FIFO or `/dev/stdin` "not a
    # file".
    contents = []
    mangled = []
    try:
        for f in args.files:
            text, replaced = read_text(f)
            if replaced:
                mangled.append(f)
            contents.append((display_path(f), text))
    except OSError as e:
        sys.exit(f"error: cannot read {e.filename}: {e.strerror}")

    for f in mangled:
        print(f"warning: {f} is not valid UTF-8; undecodable bytes replaced with U+FFFD",
              file=sys.stderr)

    if args.output:
        try:
            write_atomically(args.output, lambda f: bundle(contents, f))
        except OSError as e:
            sys.exit(f"error: cannot write {args.output}: {e.strerror}")
        print(f"wrote {len(contents)} files to {args.output}", file=sys.stderr)
    else:
        bundle(contents, sys.stdout)


if __name__ == "__main__":
    main()
