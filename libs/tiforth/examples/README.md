# TiForth consumer examples (dependency matrix)

This folder contains **standalone consumer projects** demonstrating how an application can use
TiForth with different dependency strategies:

- App uses Arrow APIs directly or not
- Arrow comes from **system** (a local install prefix) or is **bundled** (built from source)
- Arrow is linked **shared** or **static**
- TiForth comes from **system** (a local install prefix) or is **bundled** (built from source)
- TiForth is linked **shared** or **static**

Notes:

- TiForth always depends on Arrow (core + compute). “No Arrow” means the app does not build Arrow
  arrays / call Arrow compute APIs itself (but it still uses `arrow::Result` via TiForth’s public API).
- “System” means `find_package(...)` from a prefix in `CMAKE_PREFIX_PATH`. For “system Arrow”, do
  **not** install Arrow globally; install into a local prefix and treat it as “system”.

## Example list

All projects live under `libs/tiforth/examples/projects/`:

- `noarrow__tiforth-system-shared`
- `noarrow__tiforth-system-static`
- `noarrow__tiforth-bundled-shared`
- `noarrow__tiforth-bundled-static`

- `arrow-system-shared__tiforth-system-shared`
- `arrow-system-shared__tiforth-system-static`
- `arrow-system-static__tiforth-system-shared`
- `arrow-system-static__tiforth-system-static`

- `arrow-system-shared__tiforth-bundled-shared`
- `arrow-system-shared__tiforth-bundled-static`
- `arrow-system-static__tiforth-bundled-shared`
- `arrow-system-static__tiforth-bundled-static`

- `arrow-bundled-shared__tiforth-bundled-shared`
- `arrow-bundled-shared__tiforth-bundled-static`
- `arrow-bundled-static__tiforth-bundled-shared`
- `arrow-bundled-static__tiforth-bundled-static`

Combinations intentionally not provided:

- `arrow-bundled-*__tiforth-system-*`: a `find_package(tiforth)` consumer must also satisfy
  TiForth’s `find_dependency(Arrow)` / `find_dependency(ArrowCompute)` at configure time, so Arrow
  must be discoverable as a “system” package (local install prefix is OK). Bundling a second Arrow
  inside the app build would risk loading two different Arrow libraries in one process.

## Build quickstart

### 1) Prepare local “system” prefixes (Arrow + TiForth)

Use `libs/tiforth/examples/scripts/bootstrap_local_system_deps.sh` to build/install:

- `libs/tiforth/examples/_install/arrow` (Arrow core + compute; shared + static targets)
- `libs/tiforth/examples/_install/tiforth-<shared|static>-arrow-<shared|static>` (4 TiForth variants)

To avoid FetchContent downloads, pass a local Arrow source checkout:

```sh
FETCHCONTENT_SOURCE_DIR_ARROW=/path/to/arrow-src ./libs/tiforth/examples/scripts/bootstrap_local_system_deps.sh
```

### 2) Configure/build an example

From the repo root:

```sh
cmake -S libs/tiforth/examples/projects/<example> -B /tmp/<example>-build \
  -DCMAKE_PREFIX_PATH="$(pwd)/libs/tiforth/examples/_install/arrow;$(pwd)/libs/tiforth/examples/_install/tiforth-shared-arrow-shared"
cmake --build /tmp/<example>-build
```

Pick the matching TiForth install prefix:

- `...__tiforth-system-shared`: `.../_install/tiforth-shared-arrow-<shared|static>`
- `...__tiforth-system-static`: `.../_install/tiforth-static-arrow-<shared|static>`
- `arrow-system-static__...`: prefer `...-arrow-static` to keep linkage consistent

For bundled Arrow examples (`arrow-bundled-*`), CMake may download Arrow (22.0.0) via FetchContent.
To avoid network downloads, set `FETCHCONTENT_SOURCE_DIR_ARROW` to a local Arrow source checkout.
