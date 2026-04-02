#!/usr/bin/env python3
"""Report whether the native sigilo shared library is available."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import atlas._sigilo as _sigilo


def main() -> int:
    if _sigilo.available():
        print("C extension available")
        print(f"  ping():           {_sigilo.ping()}")
        print(f"  render_version(): {_sigilo.render_version()}")
        print(f"  library path:     {_sigilo.library_path()}")
        return 0
    print(f"C extension not available: {_sigilo.load_error()}")
    print("  Python fallback renderer is active.")
    print("  Build with: make build-c")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
