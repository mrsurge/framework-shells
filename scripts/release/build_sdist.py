from __future__ import annotations

import argparse
from pathlib import Path
import sys

from common import DIST_DIR, ROOT, newest_added_path, run, snapshot_paths


def build_sdist(*, out_dir: Path | None = None) -> Path:
    dist_dir = (out_dir or DIST_DIR).resolve()
    dist_dir.mkdir(parents=True, exist_ok=True)
    before = snapshot_paths(dist_dir, "*.tar.gz")
    run(
        [sys.executable, "setup.py", "sdist", "--dist-dir", str(dist_dir)],
        cwd=ROOT,
    )
    after = snapshot_paths(dist_dir, "*.tar.gz")
    return newest_added_path(before, after)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a source distribution")
    parser.add_argument("--out-dir", help="Output directory for the sdist")
    args = parser.parse_args()
    sdist_path = build_sdist(out_dir=Path(args.out_dir).resolve() if args.out_dir else None)
    print(sdist_path)


if __name__ == "__main__":
    main()
