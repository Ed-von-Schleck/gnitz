"""perf record / perf stat wrappers + flamegraph generation."""

from __future__ import annotations

import os
import re
import shutil
import signal
import subprocess
from pathlib import Path


class PerfRecorder:
    """perf record -F 99 -g -p <pid> -o <output_dir>/perf.data"""

    def __init__(self, pid: int, output_dir: Path | str):
        self._pid = pid
        self._output_dir = Path(output_dir)
        self._proc: subprocess.Popen | None = None

    def start(self) -> None:
        perf_data = self._output_dir / "perf.data"
        try:
            self._proc = subprocess.Popen(
                ["perf", "record", "-F", "99", "-g", "-k", "1", "-e", "cycles",
                 "-p", str(self._pid), "-o", str(perf_data)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except (FileNotFoundError, PermissionError) as e:
            print(f"[perf] Could not start perf record: {e}")
            print("[perf] Check that 'perf' is installed and "
                  "kernel.perf_event_paranoid allows profiling.")
            self._proc = None

    def stop(self) -> None:
        if self._proc is None:
            return
        self._proc.send_signal(signal.SIGINT)
        try:
            self._proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait()

    def flamegraph(self) -> Path | None:
        """Generate flamegraph SVG from perf.data.

        Tries inferno (Rust) first, falls back to FlameGraph perl scripts.
        Returns path to SVG or None on failure.
        """
        perf_data = self._output_dir / "perf.data"
        if not perf_data.exists():
            return None

        svg_path = self._output_dir / "flamegraph.svg"

        # Try inferno (Rust toolchain)
        if shutil.which("inferno-collapse-perf") and shutil.which("inferno-flamegraph"):
            try:
                script = subprocess.run(
                    ["perf", "script", "-i", str(perf_data)],
                    capture_output=True, text=True, timeout=60,
                )
                collapse = subprocess.run(
                    ["inferno-collapse-perf"],
                    input=script.stdout, capture_output=True, text=True, timeout=60,
                )
                with open(svg_path, "w") as f:
                    subprocess.run(
                        ["inferno-flamegraph"],
                        input=collapse.stdout, stdout=f, timeout=60,
                    )
                print(f"[perf] Flamegraph written to {svg_path}")
                return svg_path
            except Exception as e:
                print(f"[perf] inferno flamegraph failed: {e}")

        # Fallback: FlameGraph perl scripts
        for tool in ("stackcollapse-perf.pl", "flamegraph.pl"):
            if not shutil.which(tool):
                print(f"[perf] {tool} not found. Install inferno or FlameGraph.")
                return None
        try:
            script = subprocess.run(
                ["perf", "script", "-i", str(perf_data)],
                capture_output=True, text=True, timeout=60,
            )
            collapse = subprocess.run(
                ["stackcollapse-perf.pl"],
                input=script.stdout, capture_output=True, text=True, timeout=60,
            )
            with open(svg_path, "w") as f:
                subprocess.run(
                    ["flamegraph.pl"],
                    input=collapse.stdout, stdout=f, timeout=60,
                )
            print(f"[perf] Flamegraph written to {svg_path}")
            return svg_path
        except Exception as e:
            print(f"[perf] FlameGraph generation failed: {e}")
            return None


class PerfStat:
    """perf stat -p <pid> -e cycles,instructions,cache-references,..."""

    EVENTS = "cycles,instructions,cache-references,cache-misses,branch-misses"

    def __init__(self, pid: int):
        self._pid = pid
        self._proc: subprocess.Popen | None = None

    def start(self) -> None:
        try:
            self._proc = subprocess.Popen(
                ["perf", "stat", "-p", str(self._pid),
                 "-e", self.EVENTS],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
        except (FileNotFoundError, PermissionError) as e:
            print(f"[perf] Could not start perf stat: {e}")
            print("[perf] Check that 'perf' is installed and "
                  "kernel.perf_event_paranoid allows profiling.")
            self._proc = None

    def stop(self) -> dict[str, int]:
        """Stop perf stat and parse output. Returns {event_name: count}."""
        if self._proc is None:
            return {}
        self._proc.send_signal(signal.SIGINT)
        try:
            _, stderr = self._proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            _, stderr = self._proc.communicate()

        counters = {}
        for line in stderr.splitlines():
            line = line.strip()
            # Format: "1,234,567      cycles"
            m = re.match(r"^([\d,]+)\s+(\S+)", line)
            if m:
                count = int(m.group(1).replace(",", ""))
                event = m.group(2)
                counters[event] = count
        return counters
