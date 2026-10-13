"""Explicitly vendor/check the canonical standalone Ferrous supervisor crate."""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FILES = (
    "Cargo.toml", "README.md", "src/lib.rs", "src/policy.rs", "src/session.rs",
    "src/service.rs", "src/client.rs", "src/replay.rs", "src/attachment.rs", "src/bin/fws-supervisor.rs", "tests/framing.rs",
    "tests/session.rs", "tests/service.rs", "tests/client.rs", "tests/attachment_cases.json", "examples/framing_peer.rs",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("--source", type=Path, default=ROOT / ".external/ferrous-framework/crates/fws-supervisor")
    mode = parser.add_mutually_exclusive_group(required=True)
    _ = mode.add_argument("--check", action="store_true")
    _ = mode.add_argument("--write", action="store_true")
    args = parser.parse_args()
    source = Path(str(args.source))
    destination = ROOT / "native/fws_supervisor"
    for name in FILES:
        if not (source / name).is_file():
            parser.error(f"missing canonical source: {source / name}")
    # Reject new source files until the explicit vendoring inventory is updated.
    found = {path.relative_to(source).as_posix() for path in source.rglob("*")
             if path.is_file() and not set(path.relative_to(source).parts).intersection({"target", ".git"})
             and path.name != "Cargo.lock"}
    if found != set(FILES):
        parser.error(f"canonical inventory changed: {sorted(found.symmetric_difference(FILES))}")
    failures: list[str] = []
    if destination.exists():
        extra = {path.relative_to(destination).as_posix() for path in destination.rglob("*")
                 if path.is_file() and "target" not in path.relative_to(destination).parts
                 and path.name != "Cargo.lock"} - set(FILES)
        if extra:
            parser.error(f"unexpected vendored files (preserved): {sorted(extra)}")
    for name in FILES:
        target = destination / name
        if args.write:
            target.parent.mkdir(parents=True, exist_ok=True)
            _ = shutil.copyfile(source / name, target)
        if not target.is_file() or target.read_bytes() != (source / name).read_bytes():
            failures.append(name)
    if failures:
        print("Supervisor source drift: " + ", ".join(failures))
        return 1
    print("Supervisor source snapshot matches canonical crate")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
