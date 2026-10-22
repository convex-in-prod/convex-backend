#!/usr/bin/env python3
# /// script
# requires-python = "~=3.13.0"
# dependencies = []
# ///

import random
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from pathlib import Path

times: dict[str, float] = {}


def run(cmd: str | list[str]) -> None:
    try:
        subprocess.run(
            cmd,
            shell=True,
            check=True,
            text=True,
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as e:
        print("error while running", cmd)
        print(e)
        print(e.output)
        raise e


def log_duration(func: Callable[[], None]) -> Callable[[], None]:
    @wraps(func)
    def timed() -> None:
        t0 = time.time()
        func()
        duration = time.time() - t0
        times[func.__name__] = duration

    return timed


TEMP_DIR = Path("tmpDist")


def provide_temp_dir(func: Callable[[Path], None]) -> Callable[[], None]:
    @wraps(func)
    def provided() -> None:
        return func(TEMP_DIR)

    return provided


@log_duration
@provide_temp_dir
def build_public_types(temp_dir: Path) -> None:
    run(f"tsrs --outDir {temp_dir / 'esm-types'}")
    # Both declaration builds use the same module settings. Only the package
    # markers differ; equal directory depths also preserve declaration-map paths.
    shutil.copytree(temp_dir / "esm-types", temp_dir / "cjs-types")
    (temp_dir / "esm-types/package.json").write_text('{"type": "module"}')
    (temp_dir / "cjs-types/package.json").write_text('{"type": "commonjs"}')


@log_duration
@provide_temp_dir
def build_internal_types(temp_dir: Path) -> None:
    run(f"tsrs --stripInternal false --outDir {temp_dir / 'internal-esm-types'}")
    shutil.copytree(temp_dir / "internal-esm-types", temp_dir / "internal-cjs-types")
    (temp_dir / "internal-esm-types/package.json").write_text('{"type": "module"}')
    (temp_dir / "internal-cjs-types/package.json").write_text('{"type": "commonjs"}')


@log_duration
@provide_temp_dir
def build_cjs_and_esm(temp_dir: Path) -> None:
    (temp_dir / Path("esm")).mkdir(parents=True, exist_ok=True)
    (temp_dir / Path("cjs")).mkdir(parents=True, exist_ok=True)
    (temp_dir / Path("esm/package.json")).write_text('{"type": "module"}')
    (temp_dir / Path("cjs/package.json")).write_text('{"type": "commonjs"}')
    run(f"node scripts/build.cjs esm tempDir={temp_dir}")
    run(f"node scripts/build.cjs cjs tempDir={temp_dir}")
    run(f"node scripts/node-browser.mjs tempDir={temp_dir}")


@log_duration
@provide_temp_dir
def build_browser_script_tag(temp_dir: Path) -> None:
    run(f"node scripts/build.cjs browser-script-tag tempDir={temp_dir}")


@log_duration
@provide_temp_dir
def build_react_script_tag(temp_dir: Path) -> None:
    run(f"node scripts/build.cjs react-script-tag tempDir={temp_dir}")


@log_duration
@provide_temp_dir
def build_standalone_cli(temp_dir) -> None:
    run(f"node scripts/build.cjs standalone-cli tempDir={temp_dir}")


def main() -> None:
    t0 = time.time()

    global TEMP_DIR
    TEMP_DIR = Path("tmpDist" + str(random.random())[2:])
    temp_to_delete = Path("tmpDist" + str(random.random())[2:])

    pool = ThreadPoolExecutor(max_workers=20)

    children = []
    # Types are slower, run them first
    children.append(pool.submit(build_public_types))
    children.append(pool.submit(build_internal_types))
    children.append(pool.submit(build_cjs_and_esm))
    children.append(pool.submit(build_browser_script_tag))
    children.append(pool.submit(build_react_script_tag))
    children.append(pool.submit(build_standalone_cli))

    for child in as_completed(children):
        try:
            child.result()
        except subprocess.CalledProcessError:
            # Skip the stacktrace - not really useful in output
            sys.exit(1)

    # Quickly swap these directories.
    try:
        shutil.move("dist", temp_to_delete)
    except FileNotFoundError:
        pass
    shutil.move(TEMP_DIR, "dist")
    shutil.rmtree(temp_to_delete, ignore_errors=True)

    for name in sorted(times.keys(), key=lambda task: times[task]):
        print(f"{round(times[name], 3):2.2f}s {name}")

    print(f"{time.time() - t0:2.2f}s total")


if __name__ == "__main__":
    main()
