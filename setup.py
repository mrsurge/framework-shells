from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

from setuptools import setup

try:
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
except Exception:  # pragma: no cover - wheel is expected in normal builds
    _bdist_wheel = None


ROOT = Path(__file__).resolve().parent
BROKER_RELATIVE_PATH = Path("framework_shells/bin/fws-terminal-stream-broker")
BROKER_SOURCE_MANIFEST = ROOT / "native" / "fws_terminal_stream_broker" / "Cargo.toml"
BROKER_SOURCE_BINARY = ROOT / "native" / "fws_terminal_stream_broker" / "target" / "release" / "fws-terminal-stream-broker"
INSTALL_MODE_ENV = "FRAMEWORK_SHELLS_INSTALL_MODE"
INSTALL_MODE_ALIAS_ENV = "FWS_INSTALL_MODE"
PYTHON_ONLY_ENV = "FRAMEWORK_SHELLS_PYTHON_ONLY"
BUILD_NATIVE_ENV = "FRAMEWORK_SHELLS_BUILD_NATIVE"

_prepared_native_broker = False
_staged_native_broker = False
_native_broker_preexisted = False


def _has_bundled_native_broker() -> bool:
    path = ROOT / BROKER_RELATIVE_PATH
    return path.is_file()


def _log(message: str) -> None:
    print(f"[framework_shells build] {message}")


def _env_truthy(name: str) -> bool:
    value = (os.environ.get(name) or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _install_mode() -> str:
    raw = (os.environ.get(INSTALL_MODE_ENV) or os.environ.get(INSTALL_MODE_ALIAS_ENV) or "").strip().lower()
    if not raw:
        if _env_truthy(PYTHON_ONLY_ENV):
            return "python-only"
        if _env_truthy(BUILD_NATIVE_ENV):
            return "build"
        return "auto"
    normalized = raw.replace("_", "-")
    if normalized in {"auto", "build", "native", "force", "required", "python", "python-only", "no-build", "skip"}:
        return normalized
    raise RuntimeError(
        f"Unsupported {INSTALL_MODE_ENV}={raw!r}. "
        "Use one of: auto, build, python-only."
    )


def _requires_native_build(mode: str) -> bool:
    return mode in {"build", "native", "force", "required"}


def _skips_native_build(mode: str) -> bool:
    return mode in {"python", "python-only", "no-build", "skip"}


def _ensure_executable(path: Path) -> None:
    mode = path.stat().st_mode
    path.chmod(mode | 0o111)


def _build_and_stage_native_broker() -> None:
    global _staged_native_broker
    global _native_broker_preexisted

    mode = _install_mode()
    if _has_bundled_native_broker():
        _native_broker_preexisted = True
        return
    if _skips_native_build(mode):
        _log(f"skipping native terminal broker build ({INSTALL_MODE_ENV}={mode})")
        return
    if not BROKER_SOURCE_MANIFEST.is_file():
        if _requires_native_build(mode):
            raise FileNotFoundError(f"Missing broker Cargo manifest: {BROKER_SOURCE_MANIFEST}")
        _log("native terminal broker source is unavailable; continuing with a pure-Python wheel")
        return

    try:
        subprocess.run(
            ["cargo", "build", "--manifest-path", str(BROKER_SOURCE_MANIFEST), "--release"],
            cwd=str(ROOT),
            check=True,
            text=True,
        )
        if not BROKER_SOURCE_BINARY.is_file():
            raise FileNotFoundError(f"Built broker binary not found: {BROKER_SOURCE_BINARY}")
        destination = ROOT / BROKER_RELATIVE_PATH
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(BROKER_SOURCE_BINARY, destination)
        _ensure_executable(destination)
        _staged_native_broker = True
        _log(f"bundled native terminal broker from source build: {destination}")
    except Exception as exc:
        if _requires_native_build(mode):
            raise RuntimeError(f"Failed to build native terminal broker: {exc}") from exc
        _log(f"native terminal broker build failed; continuing with a pure-Python wheel: {exc}")


def _prepare_native_broker() -> None:
    global _prepared_native_broker
    if _prepared_native_broker:
        return
    _prepared_native_broker = True
    _build_and_stage_native_broker()


def _cleanup_staged_native_broker() -> None:
    if not _staged_native_broker or _native_broker_preexisted:
        return
    destination = ROOT / BROKER_RELATIVE_PATH
    try:
        if destination.exists():
            destination.unlink()
        if destination.parent.exists() and not any(destination.parent.iterdir()):
            destination.parent.rmdir()
    except Exception:
        pass


cmdclass: dict[str, type[object]] = {}


if _bdist_wheel is not None:

    class bdist_wheel(_bdist_wheel):
        def finalize_options(self) -> None:
            _prepare_native_broker()
            super().finalize_options()
            if _has_bundled_native_broker():
                self.root_is_pure = False

        def run(self) -> None:
            _prepare_native_broker()
            try:
                super().run()
            finally:
                _cleanup_staged_native_broker()

        def get_tag(self) -> tuple[str, str, str]:
            python_tag, abi_tag, plat_tag = super().get_tag()
            if not _has_bundled_native_broker():
                return python_tag, abi_tag, plat_tag
            resolved_plat = self.plat_name or plat_tag
            return ("py3", "none", resolved_plat)


    cmdclass["bdist_wheel"] = bdist_wheel


setup(cmdclass=cmdclass)
