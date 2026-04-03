"""Helpers to build or clean the vendored libatlas_sigilo shared library."""

# Copyright (c) 2026 Erick Andrade Busato
# SPDX-License-Identifier: MIT

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from shlex import split as shell_split


@dataclass(slots=True)
class BuildResult:
    """Outcome of a native sigilo build attempt."""

    success: bool
    backend: str
    library_path: str
    message: str


def repo_root() -> Path:
    """Return the repository root from the build helper location."""
    return Path(__file__).resolve().parents[2]


def package_dir(root: Path | None = None) -> Path:
    base = root or repo_root()
    return base / "atlas"


def c_dir(root: Path | None = None) -> Path:
    base = root or repo_root()
    return base / "atlas" / "_c"


def build_dir(root: Path | None = None) -> Path:
    base = root or repo_root()
    return base / "build" / "atlas_sigilo"


def legacy_build_dir(root: Path | None = None) -> Path:
    return c_dir(root) / "build"


def library_filename() -> str:
    if sys.platform == "win32":
        return "libatlas_sigilo.dll"
    if sys.platform == "darwin":
        return "libatlas_sigilo.dylib"
    return "libatlas_sigilo.so"


def library_path(root: Path | None = None) -> Path:
    return package_dir(root) / library_filename()


def _macos_deployment_target() -> str | None:
    if sys.platform != "darwin":
        return None
    explicit = os.environ.get("MACOSX_DEPLOYMENT_TARGET")
    if explicit:
        return explicit
    if "arm64" in _macos_target_architectures():
        return "11.0"
    return "10.13"


def _normalize_macos_architecture(name: str) -> tuple[str, ...]:
    normalized = name.strip().lower()
    if normalized in {"x86_64", "amd64", "x64"}:
        return ("x86_64",)
    if normalized in {"arm64", "aarch64"}:
        return ("arm64",)
    if normalized == "universal2":
        return ("x86_64", "arm64")
    return ()


def _dedupe(items: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return tuple(ordered)


def _normalize_macos_architectures(names: list[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for name in names:
        normalized.extend(_normalize_macos_architecture(name))
    return _dedupe(normalized)


def _archflags_architectures(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    tokens = shell_split(value)
    architectures: list[str] = []
    for index, token in enumerate(tokens[:-1]):
        if token == "-arch":
            architectures.append(tokens[index + 1])
    return _normalize_macos_architectures(architectures)


def _platform_tag_architectures(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    lowered = value.lower()
    if "universal2" in lowered:
        return ("x86_64", "arm64")
    for candidate in ("x86_64", "arm64", "aarch64", "amd64", "x64"):
        if candidate in lowered:
            return _normalize_macos_architectures([candidate])
    return ()


def _macos_target_architectures() -> tuple[str, ...]:
    explicit = os.environ.get("CMAKE_OSX_ARCHITECTURES")
    if explicit:
        return _normalize_macos_architectures(explicit.replace(";", " ").split())
    archflags = _archflags_architectures(os.environ.get("ARCHFLAGS"))
    if archflags:
        return archflags
    for key in ("_PYTHON_HOST_PLATFORM", "PLAT"):
        platform_archs = _platform_tag_architectures(os.environ.get(key))
        if platform_archs:
            return platform_archs
    return _normalize_macos_architectures([platform.machine() or ""])


def _macos_archflags() -> str:
    return " ".join(f"-arch {arch}" for arch in _macos_target_architectures())


def _append_flags(existing: str | None, addition: str) -> str:
    if not existing:
        return addition
    if addition in existing:
        return existing
    return f"{existing} {addition}"


def build_sigilo_library(
    *,
    repo_root: Path | None = None,
    prefer_cmake: bool = True,
) -> BuildResult:
    """Build the native shared library with CMake when available or Makefile otherwise."""
    root = repo_root or globals()["repo_root"]()
    if prefer_cmake and shutil.which("cmake") is not None:
        cmake_result = _build_with_cmake(root)
        if cmake_result.success:
            return cmake_result
    if shutil.which("make") is not None:
        return _build_with_make(root)
    return BuildResult(
        success=False,
        backend="none",
        library_path=str(library_path(root)),
        message="neither cmake nor make is available",
    )


def clean_sigilo_library(*, repo_root: Path | None = None) -> None:
    """Remove generated shared-library artifacts and intermediate build directories."""
    root = repo_root or globals()["repo_root"]()
    artifact = library_path(root)
    if artifact.exists():
        artifact.unlink()
    c_build_dir = build_dir(root)
    if c_build_dir.exists():
        shutil.rmtree(c_build_dir)
    stale_source_build_dir = legacy_build_dir(root)
    if stale_source_build_dir.exists():
        shutil.rmtree(stale_source_build_dir)
    for candidate in (c_dir(root) / library_filename(),):
        if candidate.exists():
            candidate.unlink()
    if shutil.which("make") is not None:
        subprocess.run(["make", "-C", str(c_dir(root)), "clean"], check=False)


def _build_with_cmake(root: Path) -> BuildResult:
    native_build_dir = build_dir(root)
    native_build_dir.mkdir(parents=True, exist_ok=True)
    pkg_dir = package_dir(root)
    configure = [
        "cmake",
        "-S",
        str(c_dir(root)),
        "-B",
        str(native_build_dir),
        "-DCMAKE_BUILD_TYPE=Release",
        f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={pkg_dir}",
    ]
    if platform.system() == "Windows":
        configure.extend(["-G", "Visual Studio 17 2022", "-A", "x64"])
    if platform.system() == "Darwin":
        configure.append(f"-DCMAKE_OSX_ARCHITECTURES={';'.join(_macos_target_architectures())}")
        deployment_target = _macos_deployment_target()
        if deployment_target is not None:
            configure.append(f"-DCMAKE_OSX_DEPLOYMENT_TARGET={deployment_target}")
    build_env = os.environ.copy()
    deployment_target = _macos_deployment_target()
    if deployment_target is not None:
        build_env["MACOSX_DEPLOYMENT_TARGET"] = deployment_target
    if platform.system() == "Darwin":
        build_env["CMAKE_OSX_ARCHITECTURES"] = ";".join(_macos_target_architectures())
        archflags = _macos_archflags()
        if archflags:
            build_env["ARCHFLAGS"] = archflags
            build_env["CFLAGS"] = _append_flags(build_env.get("CFLAGS"), archflags)
            build_env["LDFLAGS"] = _append_flags(build_env.get("LDFLAGS"), archflags)
    configure_result = subprocess.run(
        configure,
        capture_output=True,
        text=True,
        check=False,
        env=build_env,
    )
    if configure_result.returncode != 0:
        return BuildResult(
            success=False,
            backend="cmake",
            library_path=str(library_path(root)),
            message=configure_result.stderr.strip() or "cmake configure failed",
        )
    build = [
        "cmake",
        "--build",
        str(native_build_dir),
        "--config",
        "Release",
    ]
    build_result = subprocess.run(build, capture_output=True, text=True, check=False, env=build_env)
    lib_path = _locate_library(root)
    if build_result.returncode != 0 or lib_path is None:
        return BuildResult(
            success=False,
            backend="cmake",
            library_path=str(library_path(root)),
            message=build_result.stderr.strip() or "cmake build failed",
        )
    return BuildResult(
        success=True,
        backend="cmake",
        library_path=str(lib_path),
        message="built with cmake",
    )


def _build_with_make(root: Path) -> BuildResult:
    build_env = os.environ.copy()
    deployment_target = _macos_deployment_target()
    if deployment_target is not None:
        build_env["MACOSX_DEPLOYMENT_TARGET"] = deployment_target
    if platform.system() == "Darwin":
        archflags = _macos_archflags()
        if archflags:
            build_env["ARCHFLAGS"] = archflags
            build_env["ATLAS_SIGILO_ARCHFLAGS"] = archflags
    result = subprocess.run(
        ["make", "-C", str(c_dir(root)), "clean", "all"],
        capture_output=True,
        text=True,
        check=False,
        env=build_env,
    )
    lib_path = _locate_library(root)
    if result.returncode != 0 or lib_path is None:
        return BuildResult(
            success=False,
            backend="make",
            library_path=str(library_path(root)),
            message=result.stderr.strip() or "make build failed",
        )
    return BuildResult(
        success=True,
        backend="make",
        library_path=str(lib_path),
        message="built with make",
    )


def _locate_library(root: Path) -> Path | None:
    candidates = [
        package_dir(root) / library_filename(),
        c_dir(root) / library_filename(),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    for search_root in (build_dir(root), legacy_build_dir(root)):
        if search_root.exists():
            for candidate in search_root.rglob(library_filename()):
                if candidate.exists():
                    shutil.copy2(candidate, package_dir(root) / candidate.name)
                    return package_dir(root) / candidate.name
    return None


def _main(argv: list[str]) -> int:
    action = argv[1] if len(argv) > 1 else "build"
    prefer_cmake = "--prefer-make" not in argv
    if action == "build":
        result = build_sigilo_library(prefer_cmake=prefer_cmake)
        if result.success:
            print(f"[atlas] Native sigilo library built via {result.backend}: {result.library_path}")
            return 0
        print(f"[atlas] Native sigilo build failed via {result.backend}: {result.message}", file=sys.stderr)
        return 1
    if action == "clean":
        clean_sigilo_library()
        print("[atlas] Native sigilo artifacts removed.")
        return 0
    print(f"Unsupported action: {action}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv))
