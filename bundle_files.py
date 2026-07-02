#!/usr/bin/env python3
"""Concatenate files into one bundle with LLM-friendly boundary markers.

Emits Gemini-style plain-text delimiters (`--- path ---`), the format Google's
long-context tooling uses, so an LLM served the bundle as a single file can
still tell the originals apart. Paths are recorded relative to the current
directory. Originals are never modified.

Usage:
    bundle_files.py FILE [FILE ...] [-o OUTPUT]

Without -o the bundle is written to stdout.
"""

import argparse
import os
import sys
from pathlib import Path


def bundle(paths, out):
    for i, p in enumerate(paths):
        rel = os.path.relpath(p)
        text = Path(p).read_text(encoding="utf-8", errors="replace")
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

    if args.output:
        with open(args.output, "w", encoding="utf-8") as out:
            bundle(args.files, out)
        print(f"wrote {len(args.files)} files to {args.output}", file=sys.stderr)
    else:
        bundle(args.files, sys.stdout)


if __name__ == "__main__":
    main()
