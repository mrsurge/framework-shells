from __future__ import annotations

import argparse
from pathlib import Path

from build_sdist import build_sdist
from build_wheel import build_wheel
from common import DIST_DIR, default_plat_name_for_target, log, rust_host_target
from smoke_test_wheel import smoke_test_wheel


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the full broker-first distribution set")
    parser.add_argument("--out-dir", help="Directory for built artifacts")
    parser.add_argument("--native-target", help="Rust target triple for the native broker wheel")
    parser.add_argument("--plat-name", help="Wheel platform tag for the native broker wheel")
    parser.add_argument(
        "--profile",
        choices=("release", "debug"),
        default="release",
        help="Cargo profile for native broker builds",
    )
    parser.add_argument(
        "--skip-smoke",
        action="store_true",
        help="Skip installed-wheel smoke tests",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir).resolve() if args.out_dir else DIST_DIR
    native_target = args.native_target or rust_host_target()
    native_plat_name = args.plat_name or default_plat_name_for_target(native_target)

    sdist_path = build_sdist(out_dir=out_dir)
    log(f"built sdist: {sdist_path}")

    pure_wheel = build_wheel(out_dir=out_dir)
    log(f"built pure wheel: {pure_wheel}")
    if not args.skip_smoke:
        smoke_test_wheel(pure_wheel, expect_native_broker=False)
        log("pure wheel smoke test passed")

    native_wheel = build_wheel(
        out_dir=out_dir,
        native_broker=True,
        target=native_target,
        plat_name=native_plat_name,
        profile=args.profile,
    )
    log(f"built native broker wheel: {native_wheel}")
    if not args.skip_smoke:
        if native_target != rust_host_target():
            raise SystemExit(
                "Smoke testing a foreign-target native wheel is not supported on this host; "
                "rerun with --skip-smoke or build the host target"
            )
        smoke_test_wheel(native_wheel, expect_native_broker=True)
        log("native broker wheel smoke test passed")

    print("\nArtifacts:")
    print(f"  sdist: {sdist_path}")
    print(f"  pure wheel: {pure_wheel}")
    print(f"  native wheel: {native_wheel}")


if __name__ == "__main__":
    main()
