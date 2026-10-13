#!/bin/sh
# Rebuild upstream source; never relabel an existing Linux/experimental ABI wheel.
set -eu

PYTHON=${PYTHON:-python}
OUT=${OUT:-.external/uvloop-te2}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
unset PYTHONPATH PYTHONHOME
"$PYTHON" -I -c 'import sys, sysconfig; assert sys.platform == "android"; assert sys.version_info[:2] == (3, 14); assert not sysconfig.get_config_var("Py_GIL_DISABLED"); assert sysconfig.get_platform() == "android-24-arm64_v8a"'

URL=https://files.pythonhosted.org/packages/fa/42/02c739ce85fb2ee8d99212c61417da8140c6b87e9d97c430bea520d76044/uvloop-0.23.0.tar.gz
SHA=28d160f51ab4da3b187063652e643dea6831072add4adc1e6d62afbe73b6be27
ARCHIVE="$OUT/uvloop-0.23.0.tar.gz"
if [ ! -f "$ARCHIVE" ]; then
    curl --fail --location --silent --show-error "$URL" -o "$ARCHIVE"
fi
printf '%s  %s\n' "$SHA" "$ARCHIVE" | sha256sum -c -

# Each build gets fresh source; previous sources and compiled artifacts survive.
SOURCE=$(mktemp -d "$OUT/source-XXXXXXXX")
tar -xzf "$ARCHIVE" -C "$SOURCE"
SOURCE="$SOURCE/uvloop-0.23.0"
sed -i 's/^name = "uvloop"$/name = "uvloop-te2"/; s/^description = .*/description = "TE2 Termux build of upstream uvloop (Android arm64, CPython 3.14)"/; s/^requires-python = .*/requires-python = ">=3.14,<3.15"/' "$SOURCE/pyproject.toml"
sed -i 's/^    version=VERSION,$/    version=VERSION + ".post1",/' "$SOURCE/setup.py"
cat > "$SOURCE/TE2_BUILD_NOTICE.rst" <<'EOF'
uvloop-te2: Termux distribution
========================================

This is an independently packaged TE2 Termux build of MagicStack uvloop 0.23.0,
not an official upstream release. The distribution is uvloop-te2 0.23.0.post1;
the Python package remains ``uvloop`` and reports upstream version 0.23.0.
Do not install it alongside the upstream uvloop distribution in the same venv.

Initial wheel: regular CPython 3.14, Android API 24+, arm64-v8a, built in Termux.
Other architectures and free-threaded Python are not covered by this artifact.
The wheel uses Termux's libpython3.14.so, libandroid-support.so and library
prefix; it is not a generic Android Python distribution wheel.
The bundled libuv is built statically. Upstream source behavior is unchanged;
only distribution metadata and this notice are added. Original authorship and
MIT/Apache license notices are retained, including libuv's license notice.

Source: https://github.com/MagicStack/uvloop
Build recipe: scripts/release/build_uvloop_te2.sh in
https://github.com/mrsurge/framework-shells
Source archive SHA256:
28d160f51ab4da3b187063652e643dea6831072add4adc1e6d62afbe73b6be27

EOF
cat "$SOURCE/README.rst" >> "$SOURCE/TE2_BUILD_NOTICE.rst"
sed -i 's/^readme = "README.rst"$/readme = "TE2_BUILD_NOTICE.rst"/' "$SOURCE/pyproject.toml"
# The wheel contains libuv object code, so ship its notice as well as uvloop's.
cp "$SOURCE/vendor/libuv/LICENSE" "$SOURCE/LICENSE-LIBUV"
mkdir -p "$OUT/dist"
"$PYTHON" -I -m build --wheel --no-isolation --outdir "$OUT/dist" "$SOURCE"
printf '\nSource retained at %s\n' "$SOURCE"
sha256sum "$OUT"/dist/uvloop_te2-0.23.0.post1-*.whl
