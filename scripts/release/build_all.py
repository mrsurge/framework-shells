from __future__ import annotations

import argparse
from pathlib import Path

from build_sdist import build_sdist
from build_wheel import build_wheel
from common import DEFAULT_NATIVE_WHEEL_TARGETS, DIST_DIR, default_plat_name_for_target, log, rust_host_target
from smoke_test_wheel import smoke_test_wheel


def _dedupe_targets(targets: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for target in targets:
        if target in seen:
            continue
        seen.add(target)
        ordered.append(target)
    return ordered


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the full broker-first distribution set")
    parser.add_argument("--out-dir", help="Directory for built artifacts")
    parser.add_argument(
        "--native-target",
        action="append",
        default=[],
        help="Rust target triple for a native broker wheel. May be passed multiple times.",
    )
    parser.add_argument(
        "--matrix",
        choices=("host", "default"),
        default="host",
        help=(
            "Native wheel target selection. "
            "'host' builds only the current rust host target. "
            "'default' builds the supported Unix-family matrix."
        ),
    )
    parser.add_argument(
        "--plat-name",
        help="Wheel platform tag for the native broker wheel. Only valid when exactly one native target is selected.",
    )
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
    host_target = rust_host_target()
    requested_targets = _dedupe_targets(list(args.native_target))
    if requested_targets:
        native_targets = requested_targets
    elif args.matrix == "default":
        native_targets = list(DEFAULT_NATIVE_WHEEL_TARGETS)
    else:
        native_targets = [host_target]

    if args.plat_name and len(native_targets) != 1:
        raise SystemExit("--plat-name can only be used when building exactly one native target")

    sdist_path = build_sdist(out_dir=out_dir)
    log(f"built sdist: {sdist_path}")

    pure_wheel = build_wheel(out_dir=out_dir)
    log(f"built pure wheel: {pure_wheel}")
    if not args.skip_smoke:
        smoke_test_wheel(pure_wheel, expect_native_broker=False)
        log("pure wheel smoke test passed")

    native_wheels: list[tuple[str, Path]] = []
    for target in native_targets:
        plat_name = args.plat_name if len(native_targets) == 1 else default_plat_name_for_target(target)
        native_wheel = build_wheel(
            out_dir=out_dir,
            native_broker=True,
            target=target,
            plat_name=plat_name,
            profile=args.profile,
        )
        native_wheels.append((target, native_wheel))
        log(f"built native broker wheel [{target}]: {native_wheel}")
        if args.skip_smoke:
            continue
        if target != host_target:
            log(
                "skipping native wheel smoke test for foreign target "
                f"{target}; smoke testing is only supported on host target {host_target}"
            )
            continue
        smoke_test_wheel(native_wheel, expect_native_broker=True)
        log(f"native broker wheel smoke test passed [{target}]")

    print("\nArtifacts:")
    print(f"  sdist: {sdist_path}")
    print(f"  pure wheel: {pure_wheel}")
    for target, native_wheel in native_wheels:
        print(f"  native wheel [{target}]: {native_wheel}")


if __name__ == "__main__":
    main()
