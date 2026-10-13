# FastAPI Removal And Native Python Packaging

## Priority And Scope

Release blocker for the next TE2 release, requested 2026-10-13. Remove the
FastAPI dependency tree from Python FWS without changing shell semantics or the
Python/Ferrous HTTP, Socket.IO, dashboard and peer contracts. Existing strict
typing is achieved, not a prerequisite to repeat. mypy/mypyc compatibility and
compiled execution are separate acceptance checks.

Completed approval: plan/tracker and isolated uvloop-te2 build, validation and
PyPI upload. Subsequent approval adds optional loop selection at FWS-owned CLI
and Python bridge thread startup, documentation and isolated tests. FastAPI
migration remains pending. No TE2 edits, repository commit/push, live restart or
existing-environment installation. Preserve uncommitted UDS replay/attachment
work and existing native artifacts/caches.

## Source Findings

- FWS directly requires FastAPI in pyproject.toml. API adapters live in
  framework_shells/api/fastapi_router.py, fws_ui.py and websocket.py.
- socketio_backend.py imports shared actions/state helpers from fws_ui.py,
  coupling the Socket.IO lane to FastAPI despite separate transport semantics.
- The historical Python Ferrous bridge host also constructs a FastAPI app.
  Modern native Ferrous uses Axum/Socketioxide/Tokio and needs no such removal.
- No FWS-owned Pydantic models were found in the initial import survey.
- TE2's app/libs/te2_asgi shares typed ASGI body/response helpers with msgspec
  and Starlette primitives, not a new server or application route framework.
  Use that design as reference; standalone FWS must never import TE2 internals.
- TE2 still separately lists FastAPI and contains legacy worker/runtime bridge
  imports. FWS removal alone does not prove TE2's dependency closure is clean.
- FWS runs on the caller's event loop. Its old native-pipe benchmark defaults
  to uvloop; recent UDS benchmarks used asyncio. Installing uvloop and selecting
  it are distinct. Do not replace a caller's loop policy on import.

## Implementation Tracker

- [x] Establish priority, scope and source-level dependency inventory.
- [x] Record strict typing as the existing baseline.
- [ ] Characterize routes, status/error bodies, query/body coercion, auth,
  redirects/root paths, file responses, WebSockets and host lifespan behavior.
- [ ] Extract shared actions/state/auth from framework-specific adapters.
- [ ] Replace FastAPI routing/injection with explicit typed ASGI adapters and
  established Starlette primitives. Preserve body bounds/disconnect handling;
  never retry mutations or issue a second response after streaming has begun.
- [ ] Replace host mounting/construction with an explicit standalone ASGI API;
  inventory and coordinate downstream imports before removing old exports.
- [ ] Remove FastAPI from FWS dependencies and validate a fresh installation
  without FastAPI/Pydantic. Check the complete resolved dependency closure.
- [ ] Run existing strict checks, add actual mypy compatibility checks and
  compile an explicit eligible module list with mypyc in an isolated build.
- [ ] Compare source and compiled import/lifecycle/transport behavior. Do not
  equate typecheck success, Rust extensions or importability with mypyc parity.
- [ ] Verify Python/Ferrous dashboard, peer control and raw pipe/log behavior;
  no protocol changes, polling, lifecycle rewrite or log hot-path changes.
- [ ] Coordinate TE2 release integration and separately authorize live testing.

## uvloop-te2 Packaging

Publish an explicitly identified Termux distribution, not an impersonation of
the upstream project. Distribution name: uvloop-te2; Python import: uvloop.
Preserve upstream license notices, version provenance and build instructions.
Do not install uvloop and uvloop-te2 together: they own the same import files.

Initial target selected by user: ~/test-projects/newven, regular CPython 3.14.6,
Android API 24+, arm64-v8a, GIL enabled. It already imports upstream uvloop
0.23.0 with the cp314-cp314-android_24_arm64_v8a tag. The experimental .314t
interpreter is explicitly out of scope. Do not use UV or retag an incompatible
Linux/free-threaded binary. No other Android ABI/Python minor is claimed.

Proposed dependency routing, pending successful publication and integration:

```toml
"uvloop-te2; sys_platform == 'android'"
"uvloop; sys_platform == 'linux'"
```

These are OS markers, not CPU guesses. Android arm64 and Linux arm64 are not
interchangeable. Linux's upstream installer chooses the compatible manylinux
wheel; manylinux is not a dependency environment marker. Only enable supported
Python/architecture ranges once artifacts or a tested source build exist.
macOS and unsupported-platform policy need an explicit integration decision.

- [x] Inspect target interpreter and installed uvloop provenance/tags.
- [x] Create isolated reproducible build and retain source/build artifacts.
- [x] Check metadata, upstream notices, ELF dependencies and Android ABI tags.
- [x] Install only in a disposable venv; test uvloop selection, fd readiness,
  subprocess stdio, UDS I/O and cancellation/cleanup.
- [x] Upload validated artifact through configured PyPI credentials without
  printing credentials; verify remote file hashes and index install.
- [ ] Add tested platform dependency markers in a later implementation slice.
- [ ] Verify actual host loop selection and native pipe activation under uvloop.

## Optional Loop Selection Tracker

- [x] Add an independent FWS helper matching TE2's optional module selection.
  Missing top-level uvloop uses asyncio; nested-import/ABI failures propagate.
- [x] Support FRAMEWORK_SHELLS_DISABLE_UVLOOP (1/true/yes/on), with no global
  policy changes on import and no changes to host-owned running loops.
- [x] Use the helper for the CLI and both historical Python bridge threads.
  Factory failure is reported through bridge startup readiness/error handling.
- [x] Use asyncio.Runner on Python 3.11+ and explicit equivalent cleanup for
  the advertised Python 3.9/3.10 boundary. Test both cleanup paths on 3.14.
- [x] Test selection, CLI dispatch, thread ownership, visible startup failure,
  task/async-generator/executor cleanup and nested-run rejection.
- [x] Verify real uvloop-te2 fd/subprocess behavior and native_pipe_testing
  binary exchange/raw-log equality under selected uvloop and disabled asyncio.
- [ ] Run on actual Python 3.9/3.10 interpreters; their cleanup path has only
  been exercised on the current interpreter.
- [ ] Verify deployed hosts after an approved install/restart.

The optional selector does not install uvloop. Platform dependency markers
remain pending; FastAPI removal remains the next release priority.

Validation: 12 focused tests pass with the published wheel and cached native
pipe extension; system-interpreter regression run passes 13 tests with two
optional-artifact skips. Changed modules/tests pass focused strict basedpyright,
and the helper passes mypy --strict. Whole-repo basedpyright reports two existing
reportPrivateUsage errors in tests/test_ferrous_record_compat.py (lines 56/60).
The native-pipe fixture explicitly closes its paused asyncio transport and waits
for exit metadata completion; production pipe cleanup behavior is untouched.

## Release Gates

FastAPI removal is urgent and independent of completing experimental UDS
survival integration. Do not merge or revert that work to accomplish removal.
No publication until wheel validation passes; record exact artifacts, hashes
and test outcomes below. A PyPI 404 is not proof that a project name can be
registered. Do not change an existing project's ownership/settings.

## Results

Planning and wheel publication complete; runtime removal remains pending.
Build tools and source artifacts stay in
ignored .external/uvloop-te2. The reproducible recipe verifies upstream's sdist
SHA256 and creates a fresh retained source directory per run:

```sh
PYTHON="$PWD/.external/uvloop-te2/build-env/bin/python" \
  sh scripts/release/build_uvloop_te2.sh
```

The build interpreter must have build, setuptools, wheel and Cython installed.
This wheel targets Termux's Python/library layout; an Android wheel tag alone
does not guarantee compatibility with other Android Python distributions.
Validate in a fresh venv without the upstream uvloop distribution using
scripts/release/smoke_uvloop_te2.py, inspect ELF dependencies/license payloads,
and run twine check before any upload. Existing newven packages are not replaced.

Published 2026-10-13:

- Package: https://pypi.org/project/uvloop-te2/0.23.0.post1/
- File: uvloop_te2-0.23.0.post1-cp314-cp314-android_24_arm64_v8a.whl
- SHA256: c5f7fd97109ce9804bb769a31913b3caafd157d35c207cd633db3081d28dda65
- Upstream source: uvloop 0.23.0, archive SHA256 pinned in build recipe.
- Only packaging name/version/description/notices changed; import remains
  uvloop and its reported upstream version remains 0.23.0.
- Upstream base/pipe/Unix-socket/runner suites: 147 cases, 144 passed, 3 skipped.
  Suites include both uvloop and asyncio comparison cases, not 144 uvloop-only
  tests. These are selected upstream suites, not the full upstream test suite.
- Six repeated custom transport/loop-close smokes passed, with ResourceWarning
  treated as error, plus isolated pip check and strict Twine metadata checks.
- Native extension is Android AArch64, statically includes libuv and retains
  MIT/Apache/libuv notices. Dynamic dependencies: Termux libpython3.14.so,
  libandroid-support.so, libc.so and libdl.so; Termux RUNPATH is intentional.
- Initial Twine check required process-local LD_PRELOAD of Termux libpython
  for the publisher's nh3 dependency. No installed runtime environment changed.
  A description underline error was fixed before publication; the rebuilt
  wheel's native binary was verified identical to the upstream-tested binary.
- PyPI's published SHA256 matches the local wheel. The index-selected Android
  wheel was installed into the disposable validation venv and smoke-tested.
- Build/logs/receipt retained under .external/uvloop-te2; existing caches and
  user-owned native artifacts retained. No FWS dependency markers/runtime
  migration, TE2 changes, live restarts, repository versions/commits/pushes.
