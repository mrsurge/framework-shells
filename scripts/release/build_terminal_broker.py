from __future__ import annotations

import argparse
from pathlib import Path

from common import ROOT, TERMINAL_BROKER_MANIFEST, log, run, rust_host_target, terminal_broker_binary_path


def build_terminal_broker(*, target: str | None = None, profile: str = "release") -> Path:
    resolved_target = target or rust_host_target()
    cargo_args = [
        "cargo",
        "build",
        f"--{profile}",
        "--manifest-path",
        str(TERMINAL_BROKER_MANIFEST),
        "--target",
        resolved_target,
    ]
    run(cargo_args, cwd=ROOT)
    broker_path = terminal_broker_binary_path(target=resolved_target, profile=profile)
    if not broker_path.is_file():
        raise FileNotFoundError(f"Built broker binary not found: {broker_path}")
    log(f"built broker: {broker_path}")
    return broker_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the native terminal stream broker")
    parser.add_argument("--target", help="Rust target triple. Defaults to current rust host target.")
    parser.add_argument(
        "--profile",
        choices=("release", "debug"),
        default="release",
        help="Cargo profile to build",
    )
    args = parser.parse_args()
    path = build_terminal_broker(target=args.target, profile=args.profile)
    print(path)


if __name__ == "__main__":
    main()
