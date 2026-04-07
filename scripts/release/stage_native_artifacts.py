from __future__ import annotations

import argparse
from pathlib import Path

from common import copy_repo_to_staging, log, stage_broker_binary


def stage_native_artifacts(
    *,
    staging_dir: Path,
    broker_path: Path | None = None,
) -> Path:
    staged_root = copy_repo_to_staging(staging_dir)
    if broker_path is not None:
        staged_broker = stage_broker_binary(staged_root, broker_path)
        log(f"staged broker: {staged_broker}")
    log(f"staging tree ready: {staged_root}")
    return staged_root


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a staged packaging tree and inject native artifacts")
    parser.add_argument("staging_dir", help="Destination for the staged source tree")
    parser.add_argument("--broker-path", help="Path to a built broker binary to stage")
    args = parser.parse_args()
    stage_native_artifacts(
        staging_dir=Path(args.staging_dir).resolve(),
        broker_path=Path(args.broker_path).resolve() if args.broker_path else None,
    )


if __name__ == "__main__":
    main()
