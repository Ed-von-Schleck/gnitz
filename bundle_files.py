#!/usr/bin/env python3
"""Concatenate files into one bundle with LLM-friendly boundary markers.

Emits Gemini-style plain-text delimiters (`--- path ---`), the format Google's
long-context tooling uses, so an LLM served the bundle as a single file can
still tell the originals apart. Paths are recorded relative to the current
directory, or absolute where the relative form would climb out of it.
Originals are never modified.

A boundary marker is not escaped in the file bodies, so an input line that
itself reads `--- x ---` is indistinguishable from one.

Usage:
    bundle_files.py FILE [FILE ...] [-o OUTPUT]

Without -o the bundle is written to stdout.
"""

import argparse
import os
import sys
from pathlib import Path


def display_path(p):
    """`p` relative to the current directory, or absolute when the relative
    form would escape it — `../../etc/hosts` names the file by where the
    bundler happened to run, and across Windows drives `relpath` raises."""
    absolute = os.path.abspath(p)
    try:
        rel = os.path.relpath(absolute)
    except ValueError:
        return absolute
    return absolute if rel.split(os.sep)[0] == os.pardir else rel


def bundle(paths, out):
    """Write the already-read `(display path, text)` pairs as one bundle."""
    for i, (rel, text) in enumerate(paths):
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

    missing = [f for f in args.files if not Path(f).is_file()]
    if missing:
        sys.exit("error: not a file: " + ", ".join(missing))

    # Opening the output truncates it, so an output that is also an input would
    # be destroyed before it could be read. `realpath` rather than `samefile`:
    # the output need not exist yet.
    if args.output:
        out_real = os.path.realpath(args.output)
        clashing = [f for f in args.files if os.path.realpath(f) == out_real]
        if clashing:
            sys.exit(f"error: output {args.output} is also an input: " + ", ".join(clashing))

    # Read everything before writing anything: a read that fails halfway would
    # otherwise leave a truncated bundle behind. The `is_file` pass above names
    # every bad path at once; this catches what it cannot predict — a permission
    # error, or a file that moved between the two.
    try:
        contents = [(display_path(f), Path(f).read_text(encoding="utf-8", errors="replace"))
                    for f in args.files]
    except OSError as e:
        sys.exit(f"error: cannot read {e.filename}: {e.strerror}")

    if args.output:
        with open(args.output, "w", encoding="utf-8") as out:
            bundle(contents, out)
        print(f"wrote {len(contents)} files to {args.output}", file=sys.stderr)
    else:
        bundle(contents, sys.stdout)


if __name__ == "__main__":
    main()
