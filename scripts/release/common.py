from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sysconfig
import tempfile
from typing import Iterable


ROOT = Path(__file__).resolve().parents[2]
DIST_DIR = ROOT / "dist"
TERMINAL_BROKER_BIN_NAME = "fws-terminal-stream-broker"
TERMINAL_BROKER_MANIFEST = ROOT / "native" / "fws_terminal_stream_broker" / "Cargo.toml"
TERMINAL_BROKER_PACKAGE_PATH = Path("framework_shells") / "bin" / TERMINAL_BROKER_BIN_NAME

_SUGGESTED_PLAT_NAMES: dict[str, str] = {
    "aarch64-linux-android": "android_24_arm64_v8a",
    "x86_64-unknown-linux-gnu": "linux_x86_64",
    "aarch64-unknown-linux-gnu": "linux_aarch64",
    "x86_64-apple-darwin": "macosx_10_9_x86_64",
    "aarch64-apple-darwin": "macosx_11_0_arm64",
}


def log(message: str) -> None:
    print(f"[release] {message}")


def run(
    cmd: Iterable[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    argv = [str(part) for part in cmd]
    log("$ " + " ".join(argv))
    resolved_env = None
    if env is not None:
        resolved_env = os.environ.copy()
        resolved_env.update(env)
    return subprocess.run(
        argv,
        cwd=str(cwd) if cwd else None,
        env=resolved_env,
        text=True,
        check=True,
    )


def normalize_plat_name(raw: str) -> str:
    return str(raw).strip().replace("-", "_").replace(".", "_")


def current_wheel_plat_name() -> str:
    return normalize_plat_name(sysconfig.get_platform())


def rust_host_target() -> str:
    output = subprocess.check_output(["rustc", "-vV"], text=True)
    for line in output.splitlines():
        if line.startswith("host:"):
            return line.split(":", 1)[1].strip()
    raise RuntimeError("Unable to determine rust host target")


def default_plat_name_for_target(target: str) -> str | None:
    host_target = rust_host_target()
    if target == host_target:
        return current_wheel_plat_name()
    return _SUGGESTED_PLAT_NAMES.get(target)


def terminal_broker_binary_path(*, target: str, profile: str) -> Path:
    return (
        ROOT
        / "native"
        / "fws_terminal_stream_broker"
        / "target"
        / target
        / profile
        / TERMINAL_BROKER_BIN_NAME
    )


def ensure_executable(path: Path) -> None:
    mode = path.stat().st_mode
    path.chmod(mode | 0o111)


def stage_broker_binary(staging_root: Path, broker_binary: Path) -> Path:
    if not broker_binary.is_file():
        raise FileNotFoundError(f"Broker binary not found: {broker_binary}")
    destination = staging_root / TERMINAL_BROKER_PACKAGE_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(broker_binary, destination)
    ensure_executable(destination)
    return destination


def snapshot_paths(directory: Path, pattern: str) -> set[Path]:
    if not directory.exists():
        return set()
    return {path.resolve() for path in directory.glob(pattern)}


def newest_added_path(before: set[Path], after: set[Path]) -> Path:
    added = sorted(after - before, key=lambda path: path.stat().st_mtime)
    if not added:
        raise RuntimeError("No new artifact was created")
    return added[-1]


def _repo_copy_ignore(dirname: str, names: list[str]) -> set[str]:
    ignored: set[str] = set()
    for name in names:
        if name in {
            ".git",
            ".venv",
            "env",
            "venv",
            "dist",
            "build",
            "__pycache__",
            ".pytest_cache",
            ".mypy_cache",
            ".ruff_cache",
            ".repo_memory.md",
        }:
            ignored.add(name)
            continue
        if name.endswith(".egg-info"):
            ignored.add(name)
            continue
        if name == "target":
            ignored.add(name)
            continue
    return ignored


def copy_repo_to_staging(destination: Path) -> Path:
    if destination.exists():
        shutil.rmtree(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(ROOT, destination, ignore=_repo_copy_ignore)
    return destination


def fresh_temp_dir(prefix: str) -> tempfile.TemporaryDirectory[str]:
    return tempfile.TemporaryDirectory(prefix=prefix)


def venv_python_path(venv_dir: Path) -> Path:
    return venv_dir / "bin" / "python"
