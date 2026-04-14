from __future__ import annotations

import argparse
from pathlib import Path
import sys

from build_terminal_broker import build_terminal_broker
from common import (
    DIST_DIR,
    copy_repo_to_staging,
    default_plat_name_for_target,
    fresh_temp_dir,
    newest_added_path,
    normalize_plat_name,
    run,
    rust_host_target,
    snapshot_paths,
    stage_broker_binary,
)


def build_wheel(
    *,
    out_dir: Path | None = None,
    native_broker: bool = False,
    broker_path: Path | None = None,
    target: str | None = None,
    plat_name: str | None = None,
    profile: str = "release",
    staging_dir: Path | None = None,
    keep_staging: bool = False,
) -> Path:
    dist_dir = (out_dir or DIST_DIR).resolve()
    dist_dir.mkdir(parents=True, exist_ok=True)
    before = snapshot_paths(dist_dir, "*.whl")

    temp_ctx = None
    if staging_dir is None:
        temp_ctx = fresh_temp_dir("fws-wheel-stage-")
        stage_root = Path(temp_ctx.name) / "source"
    else:
        stage_root = staging_dir.resolve()

    try:
        copy_repo_to_staging(stage_root)

        resolved_target = target or rust_host_target()
        packaged_broker = native_broker or broker_path is not None
        build_env: dict[str, str] = {}
        if packaged_broker:
            resolved_broker_path = broker_path
            if resolved_broker_path is None:
                resolved_broker_path = build_terminal_broker(
                    target=resolved_target,
                    profile=profile,
                )
            stage_broker_binary(stage_root, resolved_broker_path)
            build_env["FRAMEWORK_SHELLS_INSTALL_MODE"] = "python-only"
            build_env["FRAMEWORK_SHELLS_PIPE_PUMP_MODE"] = (
                "build" if resolved_target == rust_host_target() else "python-only"
            )
        else:
            build_env["FRAMEWORK_SHELLS_INSTALL_MODE"] = "python-only"
            build_env["FRAMEWORK_SHELLS_PIPE_PUMP_MODE"] = "python-only"

        wheel_args = [sys.executable, "setup.py", "bdist_wheel", "--dist-dir", str(dist_dir)]
        if packaged_broker:
            resolved_plat_name = plat_name or default_plat_name_for_target(resolved_target)
            if not resolved_plat_name:
                raise ValueError(
                    f"No default wheel platform tag is known for target {resolved_target!r}; "
                    "pass --plat-name explicitly"
                )
            wheel_args.extend(["--plat-name", normalize_plat_name(resolved_plat_name)])

        run(wheel_args, cwd=stage_root, env=build_env)
        after = snapshot_paths(dist_dir, "*.whl")
        return newest_added_path(before, after)
    finally:
        if temp_ctx is not None and not keep_staging:
            temp_ctx.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a wheel from a staged source tree")
    parser.add_argument("--out-dir", help="Output directory for the wheel")
    parser.add_argument(
        "--native-broker",
        action="store_true",
        help="Bundle the native terminal broker into the wheel",
    )
    parser.add_argument("--broker-path", help="Path to an already-built broker binary")
    parser.add_argument("--target", help="Rust target triple for the native broker build")
    parser.add_argument("--plat-name", help="Wheel platform tag (for native wheels)")
    parser.add_argument(
        "--profile",
        choices=("release", "debug"),
        default="release",
        help="Cargo profile for broker builds",
    )
    parser.add_argument("--staging-dir", help="Persistent staging directory to use")
    parser.add_argument(
        "--keep-staging",
        action="store_true",
        help="Keep the temporary staging directory after the build",
    )
    args = parser.parse_args()

    wheel_path = build_wheel(
        out_dir=Path(args.out_dir).resolve() if args.out_dir else None,
        native_broker=args.native_broker,
        broker_path=Path(args.broker_path).resolve() if args.broker_path else None,
        target=args.target,
        plat_name=args.plat_name,
        profile=args.profile,
        staging_dir=Path(args.staging_dir).resolve() if args.staging_dir else None,
        keep_staging=args.keep_staging,
    )
    print(wheel_path)


if __name__ == "__main__":
    main()
