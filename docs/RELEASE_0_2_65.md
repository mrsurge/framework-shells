# FWS 0.2.65 release tracker

## Approved scope

Publish the FastAPI-free upstream source as 0.2.65 (intentional jump from
0.0.64), with Linux x86-64 and Android aarch64 native wheels and a source archive.
Verify native terminal broker and pipe-pump inclusion/import/behavior plus package
metadata before publication. Preserve existing caches/dirty device checkouts and
running services. Commit/push/tag the FWS release; pin TE2/ALS only after publication.

## Progress

- [x] Synced local main to upstream 583273b, without local changes to overwrite.
- [x] Confirmed PyPI still publishes FastAPI-dependent 0.0.64; no 0.0.65 release.
- [x] User selected version 0.2.65 and approved builds/validation/publication.
- [x] Update Python project version. Native crate versions are independent.
- [x] Linux native wheels (ordinary ABI3 and CPython 3.14 free-threaded), metadata,
  auditwheel compatibility and isolated installed native terminal smoke pass.
- [x] Android native wheel built on Pixel CPython 3.14.6; isolated installed native
  terminal smoke passes. Dirty Pixel source checkout untouched.
- [x] Platform-neutral sdist, without bundled platform-specific binaries.
- [x] FastAPI-removal/optional-loop tests: 20 passed, two optional-loop skips,
  eight subtests. Metadata for all four artifacts excludes FastAPI/schema deps.
- [ ] Commit/push/tag and publish only the validated artifacts; verify PyPI hashes.
- [ ] Update TE2/ALS source pins and release-builder default; resolve fresh closure.

## Device/source isolation

Pixel's existing checkout is feature/uds-shells at 4f033f2, with locally changed
native broker/pipe-pump artifacts. Do not switch or overwrite it. Build the new
version from a separate staged copy, retaining its existing source and running
processes. No app installation or shared framework restart is authorized here.

## Native provenance and compatibility

Candidate Linux source builds succeeded, but auditwheel identified glibc 2.39
requirements from the current host. Do not publish these narrower candidates.
Both native crate trees, including Cargo.lock, are identical to the 0.0.64 tag.
Final Linux wheels therefore reuse the unchanged native payloads from the old
manylinux_2_28 wheels, after matching their full wheel SHA-256 to PyPI metadata.
Auditwheel verifies both final wheels still support manylinux_2_28; installed
smokes validate both interpreter modes. This is deliberate native reuse, not a
claim that the fresh host-built binaries support glibc 2.28.

Publish only dist/0.2.65/publish/: ABI3 Linux, cp314t Linux, ABI3 Android and the
platform-neutral sdist. Raw Linux/Android source-build sdists/candidates in the
parent output directory are not release inputs. Build/smoke logs remain in the
parent TE2 .codex-scratch and the isolated Pixel fws-release-0.2.65 staging root.
