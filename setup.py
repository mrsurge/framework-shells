from __future__ import annotations

from pathlib import Path

from setuptools import setup

try:
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
except Exception:  # pragma: no cover - wheel is expected in normal builds
    _bdist_wheel = None


ROOT = Path(__file__).resolve().parent
BROKER_RELATIVE_PATH = Path("framework_shells/bin/fws-terminal-stream-broker")


def _has_bundled_native_broker() -> bool:
    path = ROOT / BROKER_RELATIVE_PATH
    return path.is_file()


cmdclass: dict[str, type[object]] = {}


if _bdist_wheel is not None:

    class bdist_wheel(_bdist_wheel):
        def finalize_options(self) -> None:
            super().finalize_options()
            if _has_bundled_native_broker():
                self.root_is_pure = False

        def get_tag(self) -> tuple[str, str, str]:
            python_tag, abi_tag, plat_tag = super().get_tag()
            if not _has_bundled_native_broker():
                return python_tag, abi_tag, plat_tag
            resolved_plat = self.plat_name or plat_tag
            return ("py3", "none", resolved_plat)


    cmdclass["bdist_wheel"] = bdist_wheel


setup(cmdclass=cmdclass)
