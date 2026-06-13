from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys
import sysconfig
from typing import cast

from setuptools import Command, setup
try:
    from setuptools.command.build_py import build_py as _build_py
except Exception:  # pragma: no cover - setuptools should provide this in normal builds
    _build_py = None
try:
    from setuptools.command.develop import develop as _develop
except Exception:  # pragma: no cover - legacy editable installs may not be available
    _develop = None
try:
    from setuptools.command.egg_info import egg_info as _egg_info
except Exception:  # pragma: no cover - setuptools should provide this in normal builds
    _egg_info = None
try:
    from setuptools.command.editable_wheel import editable_wheel as _editable_wheel
except Exception:  # pragma: no cover - older setuptools may not provide PEP 660 helper
    _editable_wheel = None
try:
    from setuptools.command.dist_info import dist_info as _dist_info
except Exception:  # pragma: no cover - older setuptools may not provide this helper
    _dist_info = None

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
PIPE_PUMP_MODE_ENV = "FRAMEWORK_SHELLS_PIPE_PUMP_MODE"
PIPE_PUMP_MODE_ALIAS_ENV = "FWS_PIPE_PUMP_MODE"
PYTHON_ONLY_ENV = "FRAMEWORK_SHELLS_PYTHON_ONLY"
BUILD_NATIVE_ENV = "FRAMEWORK_SHELLS_BUILD_NATIVE"
PIPE_PUMP_MODULE_NAME = "fws_pipe_pump"
PIPE_PUMP_PACKAGE_DIR = Path("framework_shells")
PIPE_PUMP_SOURCE_MANIFEST = ROOT / "native" / "fws_pipe_pump" / "Cargo.toml"

_prepared_native_broker = False
_staged_native_broker = False
_native_broker_preexisted = False
_prepared_pipe_pump = False
_staged_pipe_pump = False
_pipe_pump_preexisted = False


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


def _normalize_wheel_tag(value: str) -> str:
    return value.strip().replace("-", "_").replace(".", "_")


def _normalize_mode(raw: str) -> str:
    return raw.replace("_", "-")


def _is_free_threaded_python() -> bool:
    return bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


def _python_extension_suffix() -> str:
    suffix = sysconfig.get_config_var("EXT_SUFFIX")
    if isinstance(suffix, str) and suffix:
        return suffix
    return ".pyd" if os.name == "nt" else ".so"


def _pipe_pump_package_path() -> Path:
    if _is_free_threaded_python():
        return PIPE_PUMP_PACKAGE_DIR / f"{PIPE_PUMP_MODULE_NAME}{_python_extension_suffix()}"
    return PIPE_PUMP_PACKAGE_DIR / f"{PIPE_PUMP_MODULE_NAME}.so"


def _pipe_pump_package_artifacts() -> list[Path]:
    package_dir = ROOT / PIPE_PUMP_PACKAGE_DIR
    if not package_dir.is_dir():
        return []
    return _pipe_pump_artifacts_in(package_dir)


def _pipe_pump_artifacts_in(package_dir: Path) -> list[Path]:
    patterns = (
        f"{PIPE_PUMP_MODULE_NAME}*.so",
        f"{PIPE_PUMP_MODULE_NAME}*.pyd",
        f"{PIPE_PUMP_MODULE_NAME}*.dylib",
        f"{PIPE_PUMP_MODULE_NAME}*.dll",
    )
    seen: set[Path] = set()
    artifacts: list[Path] = []
    for pattern in patterns:
        for path in package_dir.glob(pattern):
            if path in seen or not path.is_file():
                continue
            seen.add(path)
            artifacts.append(path)
    return artifacts


def _pipe_pump_build_artifacts() -> list[Path]:
    artifacts: list[Path] = []
    for package_dir in ROOT.glob("build/lib*/framework_shells"):
        artifacts.extend(_pipe_pump_artifacts_in(package_dir))
    return artifacts


def _pipe_pump_wheel_tag(platform_tag: str) -> tuple[str, str, str]:
    resolved_plat = _normalize_wheel_tag(platform_tag)
    if not _is_free_threaded_python():
        return ("cp39", "abi3", resolved_plat)

    version = sys.version_info
    python_tag = f"cp{version.major}{version.minor}"
    soabi = sysconfig.get_config_var("SOABI")
    abi_tag = ""
    if isinstance(soabi, str) and soabi:
        first = soabi.split("-", 1)[0]
        if first.startswith("cpython-"):
            abi_tag = "cp" + first.removeprefix("cpython-")
    if not abi_tag:
        abi_tag = f"{python_tag}t"
    return (python_tag, _normalize_wheel_tag(abi_tag), resolved_plat)


def _build_and_stage_native_broker() -> None:
    global _staged_native_broker
    global _native_broker_preexisted

    mode = _install_mode()
    destination = ROOT / BROKER_RELATIVE_PATH
    if destination.is_file():
        _native_broker_preexisted = True
    if _skips_native_build(mode):
        _log(f"skipping native terminal broker build ({INSTALL_MODE_ENV}={mode})")
        return
    if not BROKER_SOURCE_MANIFEST.is_file():
        if _requires_native_build(mode):
            raise FileNotFoundError(f"Missing broker Cargo manifest: {BROKER_SOURCE_MANIFEST}")
        _log("native terminal broker source is unavailable; continuing with a pure-Python wheel")
        return

    try:
        if destination.exists():
            destination.unlink()
        subprocess.run(
            ["cargo", "build", "--manifest-path", str(BROKER_SOURCE_MANIFEST), "--release"],
            cwd=str(ROOT),
            check=True,
            text=True,
        )
        if not BROKER_SOURCE_BINARY.is_file():
            raise FileNotFoundError(f"Built broker binary not found: {BROKER_SOURCE_BINARY}")
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


def _pipe_pump_mode() -> str:
    raw = (os.environ.get(PIPE_PUMP_MODE_ENV) or os.environ.get(PIPE_PUMP_MODE_ALIAS_ENV) or "").strip().lower()
    if not raw:
        return _install_mode()
    normalized = _normalize_mode(raw)
    if normalized in {"auto", "build", "native", "force", "required", "python", "python-only", "no-build", "skip"}:
        return normalized
    raise RuntimeError(
        f"Unsupported {PIPE_PUMP_MODE_ENV}={raw!r}. "
        "Use one of: auto, build, python-only."
    )


def _pipe_pump_artifact_candidates() -> list[Path]:
    build_dir = ROOT / "native" / "fws_pipe_pump" / "target" / "release"
    return [
        build_dir / f"lib{PIPE_PUMP_MODULE_NAME}.so",
        build_dir / f"lib{PIPE_PUMP_MODULE_NAME}.dylib",
        build_dir / f"{PIPE_PUMP_MODULE_NAME}.dll",
        build_dir / f"{PIPE_PUMP_MODULE_NAME}.so",
    ]


def _build_and_stage_pipe_pump() -> None:
    global _staged_pipe_pump
    global _pipe_pump_preexisted

    existing_artifacts = _pipe_pump_package_artifacts()
    if existing_artifacts:
        _pipe_pump_preexisted = True

    mode = _pipe_pump_mode()
    if _skips_native_build(mode):
        _log(f"skipping native pipe extension build ({PIPE_PUMP_MODE_ENV}={mode})")
        return
    if not PIPE_PUMP_SOURCE_MANIFEST.is_file():
        if _requires_native_build(mode):
            raise FileNotFoundError(f"Missing native pipe Cargo manifest: {PIPE_PUMP_SOURCE_MANIFEST}")
        _log("native pipe extension source is unavailable; continuing with a pure-Python wheel")
        return

    try:
        for artifact_path in [*existing_artifacts, *_pipe_pump_build_artifacts()]:
            artifact_path.unlink()
        command = ["cargo", "build", "--manifest-path", str(PIPE_PUMP_SOURCE_MANIFEST), "--release"]
        if _is_free_threaded_python():
            command.append("--no-default-features")
        subprocess.run(command, cwd=str(ROOT), check=True, text=True)
        artifact = next((path for path in _pipe_pump_artifact_candidates() if path.is_file()), None)
        if artifact is None:
            raise FileNotFoundError("Built native pipe artifact not found")
        destination = ROOT / _pipe_pump_package_path()
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(artifact, destination)
        _ensure_executable(destination)
        _staged_pipe_pump = True
        _log(f"bundled native pipe extension from source build: {destination}")
    except Exception as exc:
        if _requires_native_build(mode):
            raise RuntimeError(f"Failed to build native pipe extension: {exc}") from exc
        _log(f"native pipe extension build failed; continuing with a pure-Python wheel: {exc}")


def _prepare_pipe_pump() -> None:
    global _prepared_pipe_pump
    if _prepared_pipe_pump:
        return
    _prepared_pipe_pump = True
    _build_and_stage_pipe_pump()


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


def _cleanup_staged_pipe_pump() -> None:
    if not _staged_pipe_pump or _pipe_pump_preexisted:
        return
    try:
        for destination in _pipe_pump_package_artifacts():
            destination.unlink()
    except Exception:
        pass


def _has_bundled_pipe_pump() -> bool:
    return bool(_pipe_pump_package_artifacts())


def _prepare_native_artifacts() -> None:
    _prepare_native_broker()
    _prepare_pipe_pump()


def _cleanup_staged_native_artifacts() -> None:
    _cleanup_staged_native_broker()
    _cleanup_staged_pipe_pump()


cmdclass: dict[str, object] = {}


if _build_py is not None:

    class build_py(_build_py):
        def run(self) -> None:
            _prepare_native_artifacts()
            super().run()


    cmdclass["build_py"] = build_py


if _develop is not None:

    class develop(_develop):
        def run(self) -> None:
            _prepare_native_artifacts()
            super().run()


    cmdclass["develop"] = develop


if _egg_info is not None:

    class egg_info(_egg_info):
        def run(self) -> None:
            _prepare_native_artifacts()
            super().run()


    cmdclass["egg_info"] = egg_info


if _dist_info is not None:

    class dist_info(_dist_info):
        def run(self) -> None:
            _prepare_native_artifacts()
            super().run()


    cmdclass["dist_info"] = dist_info


if _editable_wheel is not None:

    class editable_wheel(_editable_wheel):
        def run(self) -> None:
            _prepare_native_artifacts()
            super().run()


    cmdclass["editable_wheel"] = editable_wheel


if _bdist_wheel is not None:

    class bdist_wheel(_bdist_wheel):
        def finalize_options(self) -> None:
            _prepare_native_artifacts()
            super().finalize_options()
            if _has_bundled_native_broker():
                self.root_is_pure = False
                if self.plat_name:
                    self.plat_name = _normalize_wheel_tag(str(self.plat_name))
            elif _has_bundled_pipe_pump():
                self.root_is_pure = False
                if self.plat_name:
                    self.plat_name = _normalize_wheel_tag(str(self.plat_name))

        def run(self) -> None:
            _prepare_native_artifacts()
            try:
                super().run()
            finally:
                _cleanup_staged_native_artifacts()

        def get_tag(self) -> tuple[str, str, str]:
            python_tag, abi_tag, plat_tag = super().get_tag()
            if not _has_bundled_native_broker() and not _has_bundled_pipe_pump():
                return python_tag, abi_tag, plat_tag
            resolved_plat = _normalize_wheel_tag(str(self.plat_name or plat_tag))
            if _has_bundled_pipe_pump():
                return _pipe_pump_wheel_tag(resolved_plat)
            return ("py3", "none", resolved_plat)


    cmdclass["bdist_wheel"] = bdist_wheel


setup(cmdclass=cast(dict[str, type[Command]], cmdclass))
